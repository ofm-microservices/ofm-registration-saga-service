package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
)

type userCreateResult struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}
type authCreateResult struct {
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}
type mailSendResult struct {
	SessionID     string `json:"session_id"`
	ClientID      string `json:"client_id"`
	UserID        string `json:"user_id"`
	RequestID     string `json:"request_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	MessageType   string `json:"message_type"`
	To            string `json:"to"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	Timestamp     string `json:"timestamp"`
}

// ResultSubscriber consumes registration result events from Kafka.
type ResultSubscriber interface{ Subscribe(context.Context) error }

type resultSubscriber struct {
	broker  eventbroker.EventBroker
	service app.RegistrationService
	cfg     config.KafkaConfig
	log     logging.Logger
}

// NewResultSubscriber constructs the Kafka result consumer for the saga.
func NewResultSubscriber(b eventbroker.EventBroker, s app.RegistrationService, c config.KafkaConfig, l logging.Logger) (ResultSubscriber, error) {
	if b == nil || s == nil || l == nil {
		return nil, errors.New("invalid result subscriber dependency")
	}
	return &resultSubscriber{broker: b, service: s, cfg: c, log: l.With(logging.String("module", "result-subscriber"))}, nil
}

func (s *resultSubscriber) Subscribe(ctx context.Context) error {
	topics := []struct {
		topic   string
		handler eventbroker.MessageHandler
	}{{s.cfg.UserResultTopic, s.user}, {s.cfg.AuthResultTopic, s.auth}, {s.cfg.MailResultTopic, s.mail}}
	for _, t := range topics {
		go func(t struct {
			topic   string
			handler eventbroker.MessageHandler
		}) {
			backoff := time.Second
			for ctx.Err() == nil {
				err := s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: t.topic}, t.handler)
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					s.log.Error("registration result Kafka consumer stopped; retrying",
						logging.String("topic", t.topic),
						logging.String("backoff", backoff.String()),
						logging.Err(err))
				}
				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if backoff < 30*time.Second {
					backoff *= 2
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
				}
			}
		}(t)
	}
	s.log.Info("registration result Kafka consumers ready")
	return nil
}
func (s *resultSubscriber) user(ctx context.Context, _ string, p []byte) error {
	var r userCreateResult
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	err := s.service.HandleUserCreateResult(ctx, app.UserCreateResult(r))
	if err != nil {
		s.log.Error("user result handler failed", logging.String("session_id", r.SessionID), logging.Err(err))
	} else {
		s.log.Info("user result handled", logging.String("session_id", r.SessionID), logging.String("status", r.Status))
	}
	return err
}
func (s *resultSubscriber) auth(ctx context.Context, _ string, p []byte) error {
	var r authCreateResult
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	err := s.service.HandleAuthCreatePendingResult(ctx, app.AuthCreatePendingResult(r))
	if err != nil {
		s.log.Error("auth result handler failed", logging.String("session_id", r.SessionID), logging.Err(err))
	} else {
		s.log.Info("auth result handled", logging.String("session_id", r.SessionID), logging.String("status", r.Status))
	}
	return err
}
func (s *resultSubscriber) mail(ctx context.Context, _ string, p []byte) error {
	var r mailSendResult
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	err := s.service.HandleMailSendResult(ctx, app.MailSendResult(r))
	if err != nil {
		s.log.Error("mail result handler failed", logging.String("session_id", r.SessionID), logging.Err(err))
	} else {
		s.log.Info("mail result handled", logging.String("session_id", r.SessionID), logging.String("status", r.Status))
	}
	return err
}
