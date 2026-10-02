CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    operation text NOT NULL,
    schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_events_occurred_at_idx
    ON outbox_events (occurred_at);

CREATE OR REPLACE FUNCTION emit_registration_outbox_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload)
        VALUES ('registration', OLD.session_id, 'registration-saga-service.registration.changed', 'deactivated', to_jsonb(OLD));
        RETURN OLD;
    END IF;
    INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload)
    VALUES (
        'registration',
        NEW.session_id,
        'registration-saga-service.registration.changed',
        CASE TG_OP WHEN 'INSERT' THEN 'created' ELSE 'updated' END,
        to_jsonb(NEW)
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS registration_saga_sessions_outbox_event ON registration_saga_sessions;
CREATE TRIGGER registration_saga_sessions_outbox_event
AFTER INSERT OR UPDATE OR DELETE ON registration_saga_sessions
FOR EACH ROW EXECUTE FUNCTION emit_registration_outbox_event();

DROP TRIGGER IF EXISTS registration_saga_steps_outbox_event ON registration_saga_steps;
CREATE TRIGGER registration_saga_steps_outbox_event
AFTER INSERT OR UPDATE OR DELETE ON registration_saga_steps
FOR EACH ROW EXECUTE FUNCTION emit_registration_outbox_event();

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload, occurred_at)
SELECT 'registration', session_id, 'registration-saga-service.registration.changed', 'created', to_jsonb(registration_saga_sessions), created_at
FROM registration_saga_sessions;

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload, occurred_at)
SELECT 'registration', session_id, 'registration-saga-service.registration.changed', 'created', to_jsonb(registration_saga_steps), created_at
FROM registration_saga_steps;
