package config

// UserServiceConfig defines how the saga reaches user-service query RPCs.
type UserServiceConfig struct {
	Address string `env:"USER_SERVICE_ADDRESS" envDefault:"127.0.0.1:9502"`
}
