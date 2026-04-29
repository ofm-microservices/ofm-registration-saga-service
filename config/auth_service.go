package config

// AuthServiceConfig defines how the saga reaches auth-service query RPCs.
type AuthServiceConfig struct {
	Address string `env:"AUTH_SERVICE_ADDRESS" envDefault:"127.0.0.1:9091"`
}
