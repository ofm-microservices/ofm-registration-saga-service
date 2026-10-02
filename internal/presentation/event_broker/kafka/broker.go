package kafka

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
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
	migrationMapper   MigrationEventMapper
	mu                sync.Mutex
	readers           []*kafka.Reader
}

// NewBroker constructs the Kafka broker used by registration-saga-service.
func NewBroker(cfg config.KafkaConfig) (eventbroker.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter", migrationMapper: NewMigrationEventMapper()}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	if strings.HasPrefix(subject, "migration.registration.") {
		enveloped, err = b.migrationMapper.Map(ctx, subject, payload)
		if err != nil {
			return err
		}
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, Balancer: &kafka.Hash{}, BatchSize: 100, BatchTimeout: 50 * time.Millisecond, AllowAutoTopicCreation: true}
	defer w.Close()
	transportkafka.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Key: eventKey(enveloped), Value: enveloped, Headers: kafkaHeaders(ctx)})
}

// PublishRealtime publishes a client-visible notification to the shared
// realtime fanout subject without exposing that subject to application code.
func (b *broker) PublishRealtime(ctx context.Context, payload []byte) error {
	return b.Publish(ctx, "realtime", payload)
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
	if strings.TrimSpace(cfg.GroupID) != "" {
		group = strings.TrimSpace(cfg.GroupID)
	}
	if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
		group = "registration-saga-service-recovery"
	}
	go func() {
		_ = (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: group, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx)
	}()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: cfg.Subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6, MaxWait: 50 * time.Millisecond})
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
					attempts := retryAttempt(msg.Headers)
					payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
					var handlerErr error
					if unwrapErr != nil {
						handlerErr = unwrapErr
					} else {
						transportkafka.Consumed(cfg.Subject, msg.Partition, msg.Offset, attempts, payload)
						if strings.HasPrefix(cfg.Subject, "migration.recovery.commands.") {
							payload = msg.Value
						}
						handlerErr = handler(kafkaprop.Context(workerCtx, msg.Headers), cfg.Subject, payload)
					}
					if handlerErr != nil {
						var permanent resilience.PermanentError
						if !errors.As(handlerErr, &permanent) && attempts < resilience.DefaultRetryPolicy.MaxAttempts {
							writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(group), WriteTimeout: 5 * time.Second}
							queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(workerCtx, msg, cfg.Subject, attempts+1, handlerErr)
							_ = writer.Close()
							if queueErr != nil {
								select {
								case errs <- queueErr:
								default:
								}
								cancel()
								return
							}
							continue
						}
						payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: cfg.Subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", handlerErr), Error: handlerErr.Error(), FailedAt: time.Now().UTC()})
						if marshalErr != nil {
							select {
							case errs <- marshalErr:
							default:
							}
							cancel()
							return
						}
						writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second, AllowAutoTopicCreation: true}
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

func retryAttempt(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == "x-ofm-retry-attempt" {
			if n, err := strconv.Atoi(string(h.Value)); err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
