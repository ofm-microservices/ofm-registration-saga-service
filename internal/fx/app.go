package appfx

import (
	"github.com/ofm-microseervices/ofm-common/pkg/logging"

	"go.uber.org/fx"
)

// AppModule wires application startup hooks that do not expose reusable
// dependencies.
var AppModule = fx.Options(
	fx.Invoke(InvokeStartLog),
)

// InvokeStartLog emits the process start log entry once FX wiring completes.
func InvokeStartLog(lg logging.Logger) {
	lg.Info("starting registration-saga-service")
}
