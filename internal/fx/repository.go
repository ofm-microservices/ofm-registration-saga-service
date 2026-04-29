package appfx

import (
	"registration-saga-service/internal/domain"
	scyllarepo "registration-saga-service/internal/infra/scylla"

	"github.com/gocql/gocql"
	"go.uber.org/fx"
)

// RepoModule provides concrete persistence adapters behind the repository
// interfaces used by the application layer.
var RepoModule = fx.Options(
	fx.Provide(
		ProvideSessionRepository,
		ProvideStepRepository,
	),
)

// ProvideSessionRepository constructs the Scylla-backed session repository.
func ProvideSessionRepository(db *gocql.Session) (domain.SessionRepository, error) {
	return scyllarepo.NewSessionRepository(db)
}

// ProvideStepRepository constructs the Scylla-backed step repository.
func ProvideStepRepository(db *gocql.Session) (domain.StepRepository, error) {
	return scyllarepo.NewStepRepository(db)
}
