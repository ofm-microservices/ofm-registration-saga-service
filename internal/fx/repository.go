package appfx

import (
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"registration-saga-service/internal/domain"
	pgrepo "registration-saga-service/internal/infra/postgres"
)

// RepoModule provides PostgreSQL-backed registration repositories.
var RepoModule = fx.Options(fx.Provide(ProvideSessionRepositoryPostgres), fx.Provide(ProvideStepRepositoryPostgres))

// ProvideSessionRepositoryPostgres constructs the PostgreSQL session repository.
func ProvideSessionRepositoryPostgres(db *sqlx.DB, lg logging.Logger) (domain.SessionRepository, error) {
	return pgrepo.NewSessionRepository(db, lg)
}

// ProvideStepRepositoryPostgres constructs the PostgreSQL step repository.
func ProvideStepRepositoryPostgres(db *sqlx.DB, lg logging.Logger) (domain.StepRepository, error) {
	return pgrepo.NewStepRepository(db, lg)
}
