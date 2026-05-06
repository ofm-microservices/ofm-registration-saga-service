package nats

import (
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	"time"

	"github.com/nats-io/nats.go"
)

// EnsureStream ensures the streams required by the registration saga exist with
// the expected subject bindings.
func EnsureStream(cfg config.NATSConfig, log logging.Logger) error {
	if log == nil {
		return ErrNilLogger
	}

	lg := log.With(logging.String("module", "jetstream-bootstrap"))
	lg.Info("ensuring jetstream streams", logging.String("stream", cfg.RegistrationEventsStream))

	nc, err := Connect(cfg)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return WrapInitJetStreamContextError(err)
	}

	streamCfg := &nats.StreamConfig{
		Name:      cfg.RegistrationEventsStream,
		Subjects:  []string{cfg.RegistrationCodeSentSubject, cfg.RegistrationCompletedSubject, cfg.RegistrationFailedSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
		MaxAge:    7 * 24 * time.Hour,
	}

	if _, err := js.AddStream(streamCfg); err != nil {
		if _, updateErr := js.UpdateStream(streamCfg); updateErr != nil {
			return WrapEnsureStreamError(streamCfg.Name, err, updateErr)
		}
	}

	userEventsStream := &nats.StreamConfig{
		Name:      cfg.UserEventsStream,
		Subjects:  []string{cfg.UserCreateResultSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
		MaxAge:    7 * 24 * time.Hour,
	}

	if _, err := js.AddStream(userEventsStream); err != nil {
		if _, updateErr := js.UpdateStream(userEventsStream); updateErr != nil {
			return WrapEnsureStreamError(userEventsStream.Name, err, updateErr)
		}
	}

	authEventsStream := &nats.StreamConfig{
		Name:      cfg.AuthEventsStream,
		Subjects:  []string{cfg.AuthCreatePendingResultSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
		MaxAge:    7 * 24 * time.Hour,
	}

	if _, err := js.AddStream(authEventsStream); err != nil {
		if _, updateErr := js.UpdateStream(authEventsStream); updateErr != nil {
			return WrapEnsureStreamError(authEventsStream.Name, err, updateErr)
		}
	}

	mailEventsStream := &nats.StreamConfig{
		Name:      cfg.MailEventsStream,
		Subjects:  []string{cfg.MailSendResultSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
		MaxAge:    7 * 24 * time.Hour,
	}

	if _, err := js.AddStream(mailEventsStream); err != nil {
		if _, updateErr := js.UpdateStream(mailEventsStream); updateErr != nil {
			return WrapEnsureStreamError(mailEventsStream.Name, err, updateErr)
		}
	}

	lg.Info("jetstream streams ensured",
		logging.String("registration_events_stream", streamCfg.Name),
		logging.String("user_events_stream", userEventsStream.Name),
		logging.String("auth_events_stream", authEventsStream.Name),
		logging.String("mail_events_stream", mailEventsStream.Name),
	)
	return nil
}

// Connect establishes a raw NATS connection for broker and bootstrap code.
func Connect(cfg config.NATSConfig) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("registration-saga-service"),
		nats.MaxReconnects(-1),
	}

	if cfg.User != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, WrapConnectToNATSError(err)
	}

	return nc, nil
}
