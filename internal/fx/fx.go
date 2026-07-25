package appfx

import "go.uber.org/fx"

// Module is the compatibility bundle of all FX modules used by the service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	TracingModule,
	MetricsModule,
	AppModule,
	StorageModule,
	MessagingModule,
	RepoModule,
	ServiceModule,
	PresentationModule,
)
