package scylla

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
)

type eventStore struct{ session *gocql.Session }

// NewEventStore creates the Scylla-backed durable claim store for registration projections.
func NewEventStore(session *gocql.Session) idempotency.Store { return &eventStore{session: session} }

func (s *eventStore) Claim(ctx context.Context, event idempotency.Event) (bool, error) {
	values := map[string]interface{}{}
	applied, err := s.session.Query(`INSERT INTO processed_events(event_id,event_type,source_service,aggregate_type,aggregate_id,aggregate_version,processed_at) VALUES (?,?,?,?,?,?,toTimestamp(now())) IF NOT EXISTS`, event.EventID, event.EventType, event.SourceService, event.AggregateType, event.AggregateID, event.AggregateVersion).WithContext(ctx).MapScanCAS(values)
	return applied, err
}

func (s *eventStore) Release(ctx context.Context, eventID string) error {
	return s.session.Query(`DELETE FROM processed_events WHERE event_id = ?`, eventID).WithContext(ctx).Exec()
}
