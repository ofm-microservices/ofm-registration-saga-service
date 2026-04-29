package appfx

import (
	"registration-saga-service/config"

	"go.uber.org/fx"
)

// ConfigModule provides parsed runtime configuration.
var ConfigModule = fx.Options(
	fx.Provide(ProvideConfig),
)

// ProvideConfig loads the service configuration from environment variables.
func ProvideConfig() (*config.Config, error) {
	return config.Load()
}
