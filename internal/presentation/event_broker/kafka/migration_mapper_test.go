package kafka

import (
	"context"
	"encoding/json"
	"testing"

	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
)

func TestMigrationEventMapperAddsProjectionIdentity(t *testing.T) {
	ctx := requestmetadata.WithValues(context.Background(), requestmetadata.Values{
		CorrelationID: "correlation-1",
		TestRunID:     "run-1",
	})
	payload := []byte(`{"session_id":"session-1","client_id":"client-1","user_id":"user-1","status":"completed"}`)

	encoded, err := NewMigrationEventMapper().Map(ctx, "migration.registration.completed", payload)
	if err != nil {
		t.Fatalf("map migration event: %v", err)
	}

	var envelope struct {
		EventType     string `json:"event_type"`
		AggregateType string `json:"aggregate_type"`
		AggregateID   string `json:"aggregate_id"`
		SessionID     string `json:"session_id"`
		SourceService string `json:"source_service"`
		CorrelationID string `json:"correlation_id"`
		TestRunID     string `json:"test_run_id"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode mapped event: %v", err)
	}
	if envelope.EventType != "migration.registration.completed" || envelope.AggregateType != "registration" || envelope.AggregateID != "session-1" || envelope.SessionID != "session-1" || envelope.SourceService != "registration-saga-service" {
		t.Fatalf("unexpected migration envelope: %+v", envelope)
	}
	if envelope.CorrelationID != "correlation-1" || envelope.TestRunID != "run-1" {
		t.Fatalf("metadata was not propagated: %+v", envelope)
	}
}

func TestMigrationEventMapperRejectsIncompletePayload(t *testing.T) {
	_, err := NewMigrationEventMapper().Map(context.Background(), "migration.registration.completed", []byte(`{"user_id":"user-1"}`))
	if err == nil {
		t.Fatal("expected incomplete registration payload error")
	}
}
