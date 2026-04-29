package nats

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyNATSURL                 = errors.New("nats url is empty")
	ErrNilLogger                    = errors.New("logger is nil")
	ErrNilBroker                    = errors.New("event broker is nil")
	ErrNilRegistrationService       = errors.New("registration service is nil")
	ErrEmptyStreamName              = errors.New("stream name is empty")
	ErrEmptySubject                 = errors.New("subject is empty")
	ErrEmptyDurableName             = errors.New("durable name is empty")
	ErrInvalidBatchSize             = errors.New("batch size must be greater than zero")
	ErrInvalidMaxWait               = errors.New("max wait must be greater than zero")
	ErrInvalidWorkerCount           = errors.New("worker count must be greater than zero")
	ErrInvalidQueueSize             = errors.New("queue size must be greater than zero")
	ErrInvalidAckWait               = errors.New("ack wait must be greater than zero")
	ErrInvalidMaxDeliver            = errors.New("max deliver must be greater than zero")
	ErrInvalidAdaptiveCheckInterval = errors.New("adaptive check interval must be greater than zero")
	ErrInvalidAdaptiveThresholds    = errors.New("adaptive thresholds must satisfy: high > medium >= 0")
	ErrInvalidAdaptivePlan          = errors.New("adaptive plan batch size and max wait must be greater than zero")
)

// WrapConnectToNATSError annotates low-level NATS connection failures.
func WrapConnectToNATSError(err error) error {
	return fmt.Errorf("connect to nats: %w", err)
}

// WrapPublishToNATSError annotates broker publish failures.
func WrapPublishToNATSError(subject string, err error) error {
	return fmt.Errorf("publish to nats (%s): %w", subject, err)
}

// WrapSubscribeToNATSError annotates push-subscribe failures.
func WrapSubscribeToNATSError(subject string, err error) error {
	return fmt.Errorf("subscribe to nats (%s): %w", subject, err)
}

// WrapFlushNATSPublisherError annotates publisher flush failures.
func WrapFlushNATSPublisherError(err error) error {
	return fmt.Errorf("flush nats publisher: %w", err)
}

// WrapInitJetStreamContextError annotates JetStream initialization failures.
func WrapInitJetStreamContextError(err error) error {
	return fmt.Errorf("init jetstream context: %w", err)
}

// WrapEnsureConsumerError annotates add-or-update pull-consumer failures.
func WrapEnsureConsumerError(streamName, durableName string, addErr, updateErr error) error {
	return fmt.Errorf("ensure consumer %q in stream %q: add err=%v, update err=%w", durableName, streamName, addErr, updateErr)
}

// WrapCreatePullSubscriberError annotates pull-subscription creation failures.
func WrapCreatePullSubscriberError(subject, durable string, err error) error {
	return fmt.Errorf("create pull subscriber subject=%q durable=%q: %w", subject, durable, err)
}

// WrapUnmarshalUserCreateResultError annotates user result decode failures.
func WrapUnmarshalUserCreateResultError(err error) error {
	return fmt.Errorf("unmarshal user create result: %w", err)
}

// WrapUnmarshalAuthCreatePendingResultError annotates auth result decode
// failures.
func WrapUnmarshalAuthCreatePendingResultError(err error) error {
	return fmt.Errorf("unmarshal auth create pending registration result: %w", err)
}

// WrapUnmarshalMailSendResultError annotates mail result decode failures.
func WrapUnmarshalMailSendResultError(err error) error {
	return fmt.Errorf("unmarshal mail send result: %w", err)
}
