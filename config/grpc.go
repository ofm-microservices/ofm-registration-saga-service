package config

// GRPCConfig configures the internal gRPC entrypoint used by api-gateway.
type GRPCConfig struct {
	Host string `env:"GRPC_HOST" envDefault:"0.0.0.0"`
	Port int    `env:"GRPC_PORT" envDefault:"9500"`
}
