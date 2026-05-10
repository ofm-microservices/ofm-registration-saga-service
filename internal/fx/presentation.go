package appfx

import (
	"context"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	app "registration-saga-service/internal/application"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
	events "registration-saga-service/internal/presentation/event_broker/nats"
	grpcserver "registration-saga-service/internal/presentation/grpc"

	"go.uber.org/fx"
)

// PresentationModule wires the transport adapters used by the service.
var PresentationModule = fx.Options(
	fx.Provide(
		ProvideAuthAvailabilityChecker,
		ProvideUserAvailabilityChecker,
		ProvideResultSubscriber,
		ProvideGRPCServer,
	),
	fx.Invoke(
		InvokeSubscribeResults,
		InvokeRunGRPCServer,
	),
)

// ProvideResultSubscriber constructs the result-consumer adapter for saga
// progress events.
func ProvideResultSubscriber(
	broker eventbroker.EventBroker,
	service app.RegistrationService,
	cfg *config.Config,
	lg logging.Logger,
) (events.ResultSubscriber, error) {
	return events.NewResultSubscriber(broker, service, cfg.NATS, lg)
}

// ProvideAuthAvailabilityChecker constructs the gRPC client used to query
// auth-service for email ownership.
func ProvideAuthAvailabilityChecker(
	lc fx.Lifecycle,
	cfg *config.Config,
	lg logging.Logger,
) (app.EmailAvailabilityChecker, error) {
	client, err := grpcserver.NewAuthClient(cfg.AuthService.Address, lg)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})

	return client, nil
}

// ProvideUserAvailabilityChecker constructs the gRPC client used to query
// user-service for username ownership.
func ProvideUserAvailabilityChecker(
	lc fx.Lifecycle,
	cfg *config.Config,
	lg logging.Logger,
) (app.UsernameAvailabilityChecker, error) {
	client, err := grpcserver.NewUserClient(cfg.UserService.Address, lg)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})

	return client, nil
}

// ProvideGRPCServer constructs the internal gRPC server used by api-gateway.
func ProvideGRPCServer(
	service app.RegistrationService,
	cfg *config.Config,
	lg logging.Logger,
) (grpcserver.Server, error) {
	return grpcserver.NewServer(service, cfg.GRPC, lg)
}

// InvokeSubscribeResults starts background NATS consumers for saga result
// subjects.
func InvokeSubscribeResults(
	lc fx.Lifecycle,
	subscriber events.ResultSubscriber,
	cfg *config.Config,
	lg logging.Logger,
) {
	var cancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel

			if err := subscriber.Subscribe(runCtx); err != nil {
				lg.Error("subscribe to registration results failed", logging.Err(err))
				cancel()
				return err
			}

			lg.Info("registration-saga-service subscriptions initialized", logging.String("env", cfg.App.Env))
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

// InvokeRunGRPCServer starts and gracefully stops the gRPC server with the FX
// lifecycle.
func InvokeRunGRPCServer(lc fx.Lifecycle, srv grpcserver.Server) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := srv.Start(); err != nil {
					panic(err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}
