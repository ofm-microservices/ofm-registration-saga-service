FROM golang:1.25.5 AS builder

WORKDIR /src

COPY ofm-common /src/ofm-common
COPY ofm-registration-saga-service /src/ofm-registration-saga-service

WORKDIR /src/ofm-registration-saga-service

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/registration-saga-service ./cmd/registration-saga-service

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app \
    && chown app:app /app \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/registration-saga-service /app/registration-saga-service

RUN chown -R app:app /app

USER app:app

CMD ["/app/registration-saga-service"]
