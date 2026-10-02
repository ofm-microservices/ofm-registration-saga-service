package appfx

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"registration-saga-service/config"
	pkgpostgres "registration-saga-service/pkg/storage/postgres"
)

// StorageModule provides PostgreSQL storage for the registration saga.
var StorageModule = fx.Options(fx.Provide(ProvidePostgresDB))

// ProvidePostgresDB opens the registration PostgreSQL pool after migrations.
func ProvidePostgresDB(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (*sqlx.DB, error) {
	if err := pkgpostgres.RunMigrations(cfg.DB); err != nil {
		return nil, err
	}
	db, err := pkgpostgres.Open(cfg.DB)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return db.Close() }})
	lg.Info("PostgreSQL connected")
	return db, nil
}
