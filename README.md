# OFM Registration Saga Service

## Purpose

`ofm-registration-saga-service` owns registration orchestration state.
It is not the source of truth for auth credentials or user profile data. Its
job is to:

- create registration sessions
- persist step state
- fan out the first registration commands
- consume result events from downstream services
- move the registration session through its workflow

Current implemented slice:

- start registration
- create saga session and step rows
- publish `saga.user.create`
- publish `saga.auth.create_pending_registration`
- wait for user/auth results
- react to `mail.send.result`
- emit `registration.code.sent`

## Run

Local process:

```bash
cp .env.example .env
set -a && source .env && set +a && go run ./cmd/registration-saga-service
```

Docker stack from the shared infra repo:

```bash
cd ../ofm-infra
just infra-up
```

## Environment

```env
APP_ENV=local
LOG_LEVEL=info

GRPC_HOST=0.0.0.0
GRPC_PORT=9090

SCYLLA_HOSTS=127.0.0.1
SCYLLA_PORT=9042
SCYLLA_KEYSPACE=registration_saga_service
SCYLLA_USERNAME=admin
SCYLLA_PASSWORD=admin
SCYLLA_CONSISTENCY=quorum
SCYLLA_CONNECT_TIMEOUT=10s
SCYLLA_MAX_WAIT_SCHEMA_AGREEMENT=30s
SCYLLA_RETRY_ATTEMPTS=20
SCYLLA_RETRY_BACKOFF=2s

NATS_URL=nats://127.0.0.1:4222
NATS_USER=
NATS_PASSWORD=
NATS_STREAM_REGISTRATION_EVENTS=REGISTRATION_EVENTS
NATS_STREAM_USER_EVENTS=USER_EVENTS
NATS_STREAM_AUTH_EVENTS=AUTH_EVENTS
NATS_STREAM_MAIL_EVENTS=MAIL_EVENTS
NATS_SUBJECT_REGISTRATION_CODE_SENT=registration.code.sent
NATS_SUBJECT_SAGA_CREATE_USER=saga.user.create
NATS_SUBJECT_SAGA_CREATE_USER_RESULT=saga.user.create.result
NATS_SUBJECT_SAGA_CREATE_PENDING_AUTH=saga.auth.create_pending_registration
NATS_SUBJECT_SAGA_CREATE_PENDING_AUTH_RESULT=saga.auth.create_pending_registration.result
NATS_SUBJECT_MAIL_SEND_RESULT=mail.send.result
NATS_DURABLE_SAGA_CREATE_USER_RESULT=registration_saga_user_create_result
NATS_DURABLE_SAGA_CREATE_PENDING_AUTH_RESULT=registration_saga_auth_create_pending_result
NATS_DURABLE_MAIL_SEND_RESULT=registration_saga_mail_send_result
NATS_RESULT_BATCH_SIZE=32
NATS_RESULT_MAX_WAIT=10ms
NATS_RESULT_WORKERS=8
NATS_RESULT_QUEUE_SIZE=500
NATS_RESULT_ACK_WAIT=30s
NATS_RESULT_MAX_DELIVER=5
NATS_RESULT_ADAPTIVE_ENABLED=false
NATS_RESULT_ADAPTIVE_CHECK_INTERVAL=2s
NATS_RESULT_ADAPTIVE_MEDIUM_PENDING=200
NATS_RESULT_ADAPTIVE_HIGH_PENDING=1000
NATS_RESULT_ADAPTIVE_LOW_BATCH_SIZE=8
NATS_RESULT_ADAPTIVE_LOW_MAX_WAIT=25ms
NATS_RESULT_ADAPTIVE_MEDIUM_BATCH_SIZE=32
NATS_RESULT_ADAPTIVE_MEDIUM_MAX_WAIT=10ms
NATS_RESULT_ADAPTIVE_HIGH_BATCH_SIZE=128
NATS_RESULT_ADAPTIVE_HIGH_MAX_WAIT=2ms
```

## Technologies

Core runtime:

- Go
- ScyllaDB for session and step persistence
- gRPC for internal registration start requests from `api-gateway`
- NATS JetStream for orchestration commands and results
- bcrypt for password hashing before auth-service handoff
- Uber Fx for wiring
- Zap for structured logging

Main libraries from `go.mod`:

- `github.com/gocql/gocql`
- `google.golang.org/grpc`
- `github.com/nats-io/nats.go`
- `github.com/google/uuid`
- `golang.org/x/crypto`
- `go.uber.org/fx`
- `go.uber.org/zap`

## Architecture Notes

- `internal/domain` defines session and step concepts
- `internal/application` owns orchestration logic
- `internal/infra/scylla` owns persistence adapters
- `internal/presentation/grpc` exposes the start-registration API
- `internal/presentation/event_broker/nats` consumes downstream results
- `pkg/storage/scylla` owns connection and schema bootstrap

This service should stay narrow. It owns workflow state, not long-term user or
auth business data.
