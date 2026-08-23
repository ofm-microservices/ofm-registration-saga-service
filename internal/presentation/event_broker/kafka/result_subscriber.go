package kafka

import (
	"context"
	"encoding/json"
	"errors"

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
		}) { _ = s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: t.topic}, t.handler) }(t)
	}
	s.log.Info("registration result Kafka consumers ready")
	return nil
}
func (s *resultSubscriber) user(ctx context.Context, _ string, p []byte) error {
	var r userCreateResult
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	return s.service.HandleUserCreateResult(ctx, app.UserCreateResult(r))
}
func (s *resultSubscriber) auth(ctx context.Context, _ string, p []byte) error {
	var r authCreateResult
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	return s.service.HandleAuthCreatePendingResult(ctx, app.AuthCreatePendingResult(r))
}
func (s *resultSubscriber) mail(ctx context.Context, _ string, p []byte) error {
	var r mailSendResult
	if err := json.Unmarshal(p, &r); err != nil {
		return err
	}
	return s.service.HandleMailSendResult(ctx, app.MailSendResult(r))
}
