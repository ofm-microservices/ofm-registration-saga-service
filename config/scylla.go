package config

import "time"

// ScyllaConfig holds the connection and schema-management settings for the
// saga state store.
type ScyllaConfig struct {
	Hosts                  []string      `env:"SCYLLA_HOSTS" envSeparator:"," envDefault:"127.0.0.1"`
	Port                   int           `env:"SCYLLA_PORT" envDefault:"9042"`
	Keyspace               string        `env:"SCYLLA_KEYSPACE" envDefault:"registration_saga_service"`
	Username               string        `env:"SCYLLA_USERNAME"`
	Password               string        `env:"SCYLLA_PASSWORD"`
	Consistency            string        `env:"SCYLLA_CONSISTENCY" envDefault:"quorum"`
	ConnectTimeout         time.Duration `env:"SCYLLA_CONNECT_TIMEOUT" envDefault:"10s"`
	MaxWaitSchemaAgreement time.Duration `env:"SCYLLA_MAX_WAIT_SCHEMA_AGREEMENT" envDefault:"30s"`
	RetryAttempts          int           `env:"SCYLLA_RETRY_ATTEMPTS" envDefault:"20"`
	RetryBackoff           time.Duration `env:"SCYLLA_RETRY_BACKOFF" envDefault:"2s"`
}
