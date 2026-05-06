package grpc

import (
	"context"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	registrationv1 "github.com/ofm-microseervices/ofm-common/proto/registration/v1"
	app "registration-saga-service/internal/application"
	"registration-saga-service/internal/domain"
)

// RegistrationService aliases the application service exposed over gRPC.
type RegistrationService = app.RegistrationService

// Logger aliases the shared logger contract used by the gRPC adapter.
type Logger = logging.Logger

// Server exposes the registration start use case over gRPC.
type Server interface {
	Start() error
	Shutdown(ctx context.Context) error
}

// RegistrationMapper translates between the shared registration gRPC contract
// and the registration application boundary.
type RegistrationMapper interface {
	ToStartParams(req *registrationv1.StartRegistrationRequest) domain.StartRegistrationParams
	ToStartResponse(result *domain.StartRegistrationResult) *registrationv1.StartRegistrationResponse
	ToVerifyEmailParams(req *registrationv1.VerifyEmailRequest) domain.VerifyEmailParams
	ToVerifyEmailResponse(result *domain.VerifyEmailResult) *registrationv1.VerifyEmailResponse
	ToRegistrationStatusResponse(result *domain.RegistrationStatus) *registrationv1.GetRegistrationStatusResponse
	ToStartError(err error) error
}
