package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/config"

	"go.uber.org/fx"
)

// LoggerModule provides the structured logger used across the service.
var LoggerModule = fx.Options(
	fx.Provide(ProvideLogger),
)

// ProvideLogger builds the service logger from runtime configuration.
func ProvideLogger(cfg *config.Config) (logging.Logger, error) {
	return logging.New("registration-saga-service", cfg.App.Env, cfg.App.LogLevel)
}
