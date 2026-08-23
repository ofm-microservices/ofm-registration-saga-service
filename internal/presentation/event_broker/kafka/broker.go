package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
	"registration-saga-service/config"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
)

type broker struct {
	brokers           []string
	group, deadLetter string
	mu                sync.Mutex
	readers           []*kafka.Reader
}

// NewBroker constructs the Kafka broker used by registration-saga-service.
func NewBroker(cfg config.KafkaConfig) (eventbroker.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter"}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, Balancer: &kafka.Hash{}}
	defer w.Close()
	return w.WriteMessages(ctx, kafka.Message{Key: eventKey(payload), Value: payload, Headers: kafkaHeaders(ctx)})
}

func eventKey(payload []byte) []byte { key := sha256.Sum256(payload); return key[:] }

func kafkaHeaders(ctx context.Context) []kafka.Header {
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eventbroker.MessageHandler) error {
	return b.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: subject}, handler)
}

func (b *broker) RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
	group := strings.TrimSpace(b.group) + "-" + strings.TrimSpace(cfg.Subject)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: cfg.Subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan kafka.Message, 32)
	errs := make(chan error, 1)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case msg := <-jobs:
					attempts := 0
					handlerErr := resilience.Retry(workerCtx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
						attempts = attempt
						return handler(kafkaprop.Context(attemptCtx, msg.Headers), cfg.Subject, msg.Value)
					})
					if handlerErr != nil {
						payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: cfg.Subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", handlerErr), Error: handlerErr.Error(), FailedAt: time.Now().UTC()})
						if marshalErr != nil {
							select {
							case errs <- marshalErr:
							default:
							}
							cancel()
							return
						}
						writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
						writeCtx, writeCancel := context.WithTimeout(workerCtx, 5*time.Second)
						dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
						writeCancel()
						_ = writer.Close()
						if dlqErr != nil {
							select {
							case errs <- dlqErr:
							default:
							}
							cancel()
							return
						}
						sharedmetrics.IncKafkaDLQ(b.deadLetter)
					}
					if commitErr := r.CommitMessages(workerCtx, msg); commitErr != nil {
						select {
						case errs <- commitErr:
						default:
						}
						cancel()
						return
					}
				}
			}
		}()
	}
	defer func() { close(jobs); workers.Wait() }()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case workerErr := <-errs:
				return workerErr
			default:
				return err
			}
		}
		select {
		case jobs <- msg:
		case workerErr := <-errs:
			return workerErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
