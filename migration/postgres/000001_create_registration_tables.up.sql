CREATE TABLE IF NOT EXISTS registration_saga_sessions (session_id text PRIMARY KEY,client_id text NOT NULL,user_id text NOT NULL,email text NOT NULL,username text NOT NULL,status text NOT NULL,created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS registration_sessions_email_idx ON registration_saga_sessions(email);
CREATE UNIQUE INDEX IF NOT EXISTS registration_sessions_username_idx ON registration_saga_sessions(username);
CREATE TABLE IF NOT EXISTS registration_saga_steps (session_id text NOT NULL,step_key text NOT NULL,status text NOT NULL,created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,PRIMARY KEY(session_id,step_key));
CREATE TABLE IF NOT EXISTS processed_events (event_id text PRIMARY KEY,event_type text NOT NULL,source_service text NOT NULL,aggregate_type text NOT NULL,aggregate_id text NOT NULL,aggregate_version bigint NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());
