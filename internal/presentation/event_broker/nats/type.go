package nats

import (
	"context"
	"registration-saga-service/config"
	eventbroker "registration-saga-service/internal/presentation/event_broker"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
)

// ResultSubscriber consumes result subjects that drive the registration saga
// state machine forward.
type ResultSubscriber interface {
	Subscribe(ctx context.Context) error
}

// PullConsumerConfigValidator validates pull-consumer runtime configuration
// before the broker touches JetStream state.
type PullConsumerConfigValidator interface {
	Validate(cfg config.PullConsumerConfig) error
}

// PullConsumerRuntime represents one configured JetStream pull-consumer
// runtime.
type PullConsumerRuntime interface {
	Start(ctx context.Context)
}

// PullConsumerRuntimeFactory builds the runtime used by the broker after the
// config is validated.
type PullConsumerRuntimeFactory interface {
	Create(
		nc *nats.Conn,
		log logging.Logger,
		cfg config.PullConsumerConfig,
		handler eventbroker.MessageHandler,
	) (PullConsumerRuntime, error)
}
