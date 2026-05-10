package main

import (
	appfx "registration-saga-service/internal/fx"

	"go.uber.org/fx"
)

var newApp = fx.New
var runApp = (*fx.App).Run

func main() {
	runApp(newApp(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.MetricsModule,
		appfx.AppModule,
		appfx.StorageModule,
		appfx.MessagingModule,
		appfx.RepoModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	))
}
