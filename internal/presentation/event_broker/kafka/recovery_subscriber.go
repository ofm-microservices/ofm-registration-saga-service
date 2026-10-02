package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"
	"registration-saga-service/internal/domain"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
)

// RecoverySubscriber applies registration fallback commands through the saga application service.
type RecoverySubscriber interface{ Subscribe(context.Context) error }
type recoverySubscriber struct {
	broker eventbroker.EventBroker
	svc    app.RegistrationService
	cfg    config.KafkaConfig
	log    logging.Logger
}

// NewRecoverySubscriber constructs the registration recovery Kafka adapter.
func NewRecoverySubscriber(b eventbroker.EventBroker, svc app.RegistrationService, cfg config.KafkaConfig, log logging.Logger) (RecoverySubscriber, error) {
	if b == nil || svc == nil || log == nil {
		return nil, errors.New("invalid registration recovery subscriber dependency")
	}
	return &recoverySubscriber{broker: b, svc: svc, cfg: cfg, log: log.With(logging.String("module", "kafka-registration-recovery-subscriber"))}, nil
}
func (s *recoverySubscriber) Subscribe(ctx context.Context) error {
	return s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: s.cfg.RecoveryTopic, GroupID: s.cfg.RecoveryGroup}, s.handle)
}
func (s *recoverySubscriber) handle(ctx context.Context, _ string, raw []byte) error {
	var cmd events.Envelope
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return fmt.Errorf("decode registration recovery command: %w", err)
	}
	if !strings.EqualFold(cmd.AggregateType, "registration") {
		return fmt.Errorf("unsupported registration recovery aggregate_type=%q", cmd.AggregateType)
	}
	var p struct {
		SessionID string `json:"session_id"`
		ClientID  string `json:"client_id"`
		UserID    string `json:"user_id"`
		Email     string `json:"email"`
		Username  string `json:"username"`
		Password  string `json:"password"`
		FirstName string `json:"first_name"`
		Surname   string `json:"surname"`
	}
	if err := json.Unmarshal(cmd.Payload, &p); err != nil {
		return err
	}
	if p.ClientID == "" {
		p.ClientID = cmd.AggregateID
	}
	for field, value := range map[string]string{"session_id": p.SessionID, "client_id": p.ClientID, "user_id": p.UserID} {
		if strings.TrimSpace(value) == "" {
			return resilience.Permanent(fmt.Errorf("registration recovery command %s is missing %s", cmd.CommandID, field))
		}
	}
	result, err := s.svc.Start(ctx, domain.StartRegistrationParams{SessionID: p.SessionID, ClientID: p.ClientID, UserID: p.UserID, Email: p.Email, Username: p.Username, Password: p.Password, FirstName: p.FirstName, Surname: p.Surname})
	if err != nil {
		return err
	}
	body, e := json.Marshal(events.Envelope{EventID: cmd.EventID + ".completed", CommandID: cmd.CommandID, CorrelationID: cmd.CorrelationID, CausationID: cmd.EventID, IdempotencyKey: cmd.IdempotencyKey, TestRunID: cmd.TestRunID, EventType: "migration.recovery.completed", Operation: cmd.Operation, SchemaVersion: 1, AggregateType: "registration", AggregateID: result.SessionID, SourceService: "registration-saga-service-recovery", OccurredAt: time.Now().UTC(), Payload: mustJSON(result)})
	if e != nil {
		return e
	}
	return s.broker.Publish(context.WithoutCancel(ctx), s.cfg.RecoveryCompletedTopic, body)
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
