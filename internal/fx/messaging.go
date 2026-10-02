package appfx

import (
	"context"
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
	broker "registration-saga-service/internal/presentation/event_broker/kafka"

	"go.uber.org/fx"
)

// MessagingModule wires JetStream bootstrap and the runtime event broker.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBrokerWithStore),
)

// InvokeEnsureStream is retained as a compatibility no-op; Kafka topics are
// provisioned by the broker/runtime rather than JetStream bootstrap.
func InvokeEnsureStream(*config.Config, logging.Logger) error { return nil }

// ProvideEventBroker constructs the Kafka-backed broker and closes it during FX
// shutdown.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (eventbroker.EventBroker, error) {
	return provideEventBroker(lc, cfg, lg, nil)
}

// ProvideEventBrokerWithStore wires Kafka with durable PostgreSQL event claims.
func ProvideEventBrokerWithStore(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger, db *sqlx.DB) (eventbroker.EventBroker, error) {
	return provideEventBroker(lc, cfg, lg, &sqlEventStore{db: db})
}

type sqlEventStore struct{ db *sqlx.DB }

func (s *sqlEventStore) Claim(ctx context.Context, event idempotency.Event) (bool, error) {
	return idempotency.ClaimDB(ctx, s.db, event)
}
func (s *sqlEventStore) Release(ctx context.Context, eventID string) error {
	return idempotency.Release(ctx, s.db, eventID)
}

func provideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger, store idempotency.Store) (eventbroker.EventBroker, error) {
	eventBroker, err := broker.NewBroker(cfg.Kafka)
	if err != nil {
		lg.Error("connect kafka failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			eventBroker.Close()
			return nil
		},
	})

	realtime, _ := eventBroker.(interface {
		PublishRealtime(context.Context, []byte) error
	})
	return &eventBrokerAdapter{broker: eventBroker, realtime: realtime, store: store}, nil
}

type eventBrokerAdapter struct {
	broker   eventbroker.EventBroker
	realtime interface {
		PublishRealtime(context.Context, []byte) error
	}
	store idempotency.Store
}

func (a *eventBrokerAdapter) Publish(ctx context.Context, subject string, payload []byte) error {
	return a.broker.Publish(ctx, subject, payload)
}
func (a *eventBrokerAdapter) Subscribe(ctx context.Context, subject string, handler eventbroker.MessageHandler) error {
	return a.broker.Subscribe(ctx, subject, a.wrap(handler))
}
func (a *eventBrokerAdapter) RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
	return a.broker.RunPullConsumer(ctx, cfg, a.wrap(handler))
}
func (a *eventBrokerAdapter) Close() { a.broker.Close() }

// PublishRealtime preserves the optional client-notification capability of
// the concrete Kafka broker while keeping the application dependent on the
// transport-agnostic EventBroker contract.
func (a *eventBrokerAdapter) PublishRealtime(ctx context.Context, payload []byte) error {
	if a.realtime == nil {
		return errors.New("realtime publisher is not configured")
	}
	return a.realtime.PublishRealtime(ctx, payload)
}
func (a *eventBrokerAdapter) wrap(handler eventbroker.MessageHandler) eventbroker.MessageHandler {
	return func(ctx context.Context, subject string, payload []byte) error {
		if a.store == nil {
			return handler(ctx, subject, payload)
		}
		event := idempotency.DecodeOrFingerprint(subject, payload)
		claimed, err := a.store.Claim(ctx, event)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
		if err := handler(ctx, subject, payload); err != nil {
			_ = a.store.Release(ctx, event.EventID)
			return err
		}
		return nil
	}
}
