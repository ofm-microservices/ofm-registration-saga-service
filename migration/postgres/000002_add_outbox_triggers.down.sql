DROP TRIGGER IF EXISTS registration_saga_sessions_outbox_event ON registration_saga_sessions;
DROP TRIGGER IF EXISTS registration_saga_steps_outbox_event ON registration_saga_steps;
DROP FUNCTION IF EXISTS emit_registration_outbox_event();
DROP TABLE IF EXISTS outbox_events;
