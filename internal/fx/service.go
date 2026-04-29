package appfx

import (
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"

	"go.uber.org/fx"
)

// ServiceModule provides the application service.
var ServiceModule = fx.Options(
	fx.Provide(ProvideRegistrationService),
)

// ProvideRegistrationService constructs the registration application service.
func ProvideRegistrationService(
	sessions app.SessionRepository,
	steps app.StepRepository,
	broker app.EventBroker,
	emailChecker app.EmailAvailabilityChecker,
	usernameChecker app.UsernameAvailabilityChecker,
	cfg *config.Config,
	lg logging.Logger,
) (app.RegistrationService, error) {
	return app.New(sessions, steps, broker, emailChecker, usernameChecker, cfg.NATS, lg)
}
