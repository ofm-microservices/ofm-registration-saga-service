package appfx

import (
	"context"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	scyllastore "registration-saga-service/pkg/storage/scylla"

	"github.com/gocql/gocql"
	"go.uber.org/fx"
)

// StorageModule provides the primary Scylla session used by saga persistence.
var StorageModule = fx.Options(
	fx.Provide(ProvideScyllaSession),
)

// ProvideScyllaSession connects to Scylla, ensures the schema exists, and
// closes the session on shutdown.
func ProvideScyllaSession(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (*gocql.Session, error) {
	session, err := scyllastore.ConnectAndEnsureSchema(cfg.Scylla, lg)
	if err != nil {
		lg.Error("connect scylla failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			session.Close()
			return nil
		},
	})

	return session, nil
}
