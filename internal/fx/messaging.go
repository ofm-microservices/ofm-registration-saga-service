package appfx

import (
	"context"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
	broker "registration-saga-service/internal/presentation/event_broker/nats"
	natsbootstrap "registration-saga-service/pkg/messaging/nats"

	"go.uber.org/fx"
)

// MessagingModule wires JetStream bootstrap and the runtime event broker.
var MessagingModule = fx.Options(
	fx.Invoke(InvokeEnsureStream),
	fx.Provide(ProvideEventBroker),
)

// InvokeEnsureStream ensures all streams required by the saga exist before the
// service begins consuming or publishing messages.
func InvokeEnsureStream(cfg *config.Config, lg logging.Logger) error {
	if err := natsbootstrap.EnsureStream(cfg.NATS, lg); err != nil {
		lg.Error("bootstrap jetstream resources failed", logging.Err(err))
		return err
	}
	return nil
}

// ProvideEventBroker constructs the NATS-backed broker and closes it during FX
// shutdown.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (eventbroker.EventBroker, error) {
	eventBroker, err := broker.NewBroker(cfg.NATS, lg)
	if err != nil {
		lg.Error("connect nats failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			eventBroker.Close()
			return nil
		},
	})

	return eventBroker, nil
}
