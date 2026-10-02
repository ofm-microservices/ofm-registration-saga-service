package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ofm-microservices/ofm-common/pkg/events"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
)

// MigrationEventMapper builds canonical migration envelopes for registration
// events published directly to Kafka rather than through a database outbox.
type MigrationEventMapper interface {
	Map(ctx context.Context, subject string, payload []byte) ([]byte, error)
}

type migrationEventMapper struct{}

type registrationPayload struct {
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
	UserID    string `json:"user_id"`
}

// NewMigrationEventMapper constructs the registration migration translator.
func NewMigrationEventMapper() MigrationEventMapper { return migrationEventMapper{} }

func (migrationEventMapper) Map(ctx context.Context, subject string, payload []byte) ([]byte, error) {
	var body registrationPayload
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("decode registration migration payload: %w", err)
	}
	if body.SessionID == "" || body.UserID == "" {
		return nil, fmt.Errorf("registration migration payload is missing session_id or user_id")
	}
	envelope, err := events.New(subject, payload)
	if err != nil {
		return nil, err
	}
	metadata := requestmetadata.FromContext(ctx)
	envelope.AggregateType = "registration"
	envelope.AggregateID = body.SessionID
	envelope.SessionID = body.SessionID
	envelope.SourceService = "registration-saga-service"
	envelope.Operation = subject
	envelope.CorrelationID = metadata.CorrelationID
	envelope.TestRunID = metadata.TestRunID
	return envelope.Marshal()
}
