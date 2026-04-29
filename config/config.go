package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config is the root runtime configuration for registration-saga-service.
type Config struct {
	App         AppConfig
	GRPC        GRPCConfig
	NATS        NATSConfig
	Scylla      ScyllaConfig
	AuthService AuthServiceConfig
	UserService UserServiceConfig
}

// Load reads environment variables into Config and applies defaults declared on
// the individual config fields.
func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, WrapParseEnvConfigError(err)
	}

	return cfg, nil
}
