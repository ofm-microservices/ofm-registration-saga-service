package eventbroker

import (
	"context"
	"registration-saga-service/config"
)

// MessageHandler processes one broker message and returns an error if it should
// be retried or surfaced to the caller.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// EventBroker abstracts message publication and subscription so the
// application layer stays transport-agnostic.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	RunPullConsumer(ctx context.Context, cfg config.PullConsumerConfig, handler MessageHandler) error
	Close()
}
