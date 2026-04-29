package nats

import (
	"context"
	"encoding/json"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
)

type resultSubscriber struct {
	broker  eventbroker.EventBroker
	service app.RegistrationService
	cfg     config.NATSConfig
	log     logging.Logger
}

// NewResultSubscriber constructs the consumer that listens for downstream
// registration step results.
func NewResultSubscriber(
	broker eventbroker.EventBroker,
	service app.RegistrationService,
	cfg config.NATSConfig,
	log logging.Logger,
) (ResultSubscriber, error) {
	if broker == nil {
		return nil, ErrNilBroker
	}
	if service == nil {
		return nil, ErrNilRegistrationService
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &resultSubscriber{
		broker:  broker,
		service: service,
		cfg:     cfg,
		log:     log.With(logging.String("module", "result-subscriber")),
	}, nil
}

// Subscribe starts all result consumers required by the first registration
// slice.
func (s *resultSubscriber) Subscribe(ctx context.Context) error {
	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.UserEventsStream,
		Subject:    s.cfg.UserCreateResultSubject,
		Durable:    s.cfg.UserCreateResultDurable,
		BatchSize:  s.cfg.ResultBatchSize,
		MaxWait:    s.cfg.ResultMaxWait,
		Workers:    s.cfg.ResultWorkers,
		QueueSize:  s.cfg.ResultQueueSize,
		AckWait:    s.cfg.ResultAckWait,
		MaxDeliver: s.cfg.ResultMaxDeliver,
		Adaptive:   BuildAdaptiveConfig(s.cfg),
	}, func(ctx context.Context, _ string, payload []byte) error {
		var result userCreateResult
		if err := json.Unmarshal(payload, &result); err != nil {
			return WrapUnmarshalUserCreateResultError(err)
		}
		return s.service.HandleUserCreateResult(ctx, app.UserCreateResult(result))
	}); err != nil {
		return err
	}

	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.AuthEventsStream,
		Subject:    s.cfg.AuthCreatePendingResultSubject,
		Durable:    s.cfg.AuthCreatePendingResultDurable,
		BatchSize:  s.cfg.ResultBatchSize,
		MaxWait:    s.cfg.ResultMaxWait,
		Workers:    s.cfg.ResultWorkers,
		QueueSize:  s.cfg.ResultQueueSize,
		AckWait:    s.cfg.ResultAckWait,
		MaxDeliver: s.cfg.ResultMaxDeliver,
		Adaptive:   BuildAdaptiveConfig(s.cfg),
	}, func(ctx context.Context, _ string, payload []byte) error {
		var result authCreatePendingResult
		if err := json.Unmarshal(payload, &result); err != nil {
			return WrapUnmarshalAuthCreatePendingResultError(err)
		}
		return s.service.HandleAuthCreatePendingResult(ctx, app.AuthCreatePendingResult(result))
	}); err != nil {
		return err
	}

	if err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{
		Stream:     s.cfg.MailEventsStream,
		Subject:    s.cfg.MailSendResultSubject,
		Durable:    s.cfg.MailSendResultDurable,
		BatchSize:  s.cfg.ResultBatchSize,
		MaxWait:    s.cfg.ResultMaxWait,
		Workers:    s.cfg.ResultWorkers,
		QueueSize:  s.cfg.ResultQueueSize,
		AckWait:    s.cfg.ResultAckWait,
		MaxDeliver: s.cfg.ResultMaxDeliver,
		Adaptive:   BuildAdaptiveConfig(s.cfg),
	}, func(ctx context.Context, _ string, payload []byte) error {
		var result mailSendResult
		if err := json.Unmarshal(payload, &result); err != nil {
			return WrapUnmarshalMailSendResultError(err)
		}
		return s.service.HandleMailSendResult(ctx, app.MailSendResult(result))
	}); err != nil {
		return err
	}

	s.log.Info("registration result pull consumers ready")
	return nil
}

// BuildAdaptiveConfig projects NATS config values into a generic adaptive pull
// consumer config.
func BuildAdaptiveConfig(cfg config.NATSConfig) config.PullAdaptiveConfig {
	return config.PullAdaptiveConfig{
		Enabled:         cfg.ResultAdaptiveEnabled,
		CheckInterval:   cfg.ResultAdaptiveCheckInterval,
		MediumPending:   cfg.ResultAdaptiveMediumPending,
		HighPending:     cfg.ResultAdaptiveHighPending,
		LowBatchSize:    cfg.ResultAdaptiveLowBatchSize,
		LowMaxWait:      cfg.ResultAdaptiveLowMaxWait,
		MediumBatchSize: cfg.ResultAdaptiveMediumBatchSize,
		MediumMaxWait:   cfg.ResultAdaptiveMediumMaxWait,
		HighBatchSize:   cfg.ResultAdaptiveHighBatchSize,
		HighMaxWait:     cfg.ResultAdaptiveHighMaxWait,
	}
}
