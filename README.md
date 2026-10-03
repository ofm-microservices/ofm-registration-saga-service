# OFM Registration Saga Service

## Purpose

The Registration Saga Service owns registration workflow state and orchestration. It coordinates user, auth, and mail services but does not own credentials, profiles, or email delivery. Status: active.

## Flow and boundaries

The gateway starts registration through the saga API. The saga persists its session and steps, publishes user/auth commands through NATS, consumes result events, and emits registration progress such as code-sent and completed. Each downstream service remains the owner of its own data.

PostgreSQL stores saga state and outbox/processed-event records. Debezium and Kafka carry committed projection events where configured. The saga advances a step only after the required result is durably processed.

## Configuration

.env.example groups are DB_*, MIGRATIONS_*, gRPC listener/clients, NATS subjects and durable consumers, Kafka/CDC, retry/timeouts, and observability. Database values select workflow storage; NATS values select commands/results; timeouts control waiting and recovery behavior.

## Local development

    cp .env.example .env
    go run ./cmd/registration-saga-service
    go test ./...

## Build and operations

Dockerfile builds ofm/registration-saga-service:<tag>. Helm and ofm-infra provide deployment and dependencies. Diagnose saga steps, result events, outbox records, Kafka lag, and WebSocket notifications.

