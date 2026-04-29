package main

import (
	appfx "registration-saga-service/internal/fx"

	"go.uber.org/fx"
)

func main() {
	app := fx.New(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.AppModule,
		appfx.StorageModule,
		appfx.MessagingModule,
		appfx.RepoModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	)
	app.Run()
}
