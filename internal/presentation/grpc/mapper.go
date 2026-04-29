package grpc

import (
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	registrationv1 "github.com/ofm-microseervices/ofm-common/proto/registration/v1"
	"registration-saga-service/internal/domain"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registrationMapper struct {
	log logging.Logger
}

func newRegistrationMapper(log logging.Logger) RegistrationMapper {
	return &registrationMapper{log: log}
}

func (m *registrationMapper) ToStartParams(req *registrationv1.StartRegistrationRequest) domain.StartRegistrationParams {
	return domain.StartRegistrationParams{
		ClientID:  req.GetClientId(),
		Email:     req.GetEmail(),
		Username:  req.GetUsername(),
		Password:  req.GetPassword(),
		FirstName: req.GetFirstName(),
		Surname:   req.GetSurname(),
	}
}

func (m *registrationMapper) ToStartResponse(result *domain.StartRegistrationResult) *registrationv1.StartRegistrationResponse {
	return &registrationv1.StartRegistrationResponse{
		SessionId:     result.SessionID,
		ClientId:      result.ClientID,
		UserId:        result.UserID,
		Status:        result.Status,
		ConflictState: result.ConflictState,
		UsernameTaken: result.UsernameTaken,
		EmailTaken:    result.EmailTaken,
	}
}

func (m *registrationMapper) ToStartError(err error) error {
	switch err {
	case domain.ErrInvalidEmail:
		return status.Error(codes.InvalidArgument, "invalid email")
	case domain.ErrInvalidUsername:
		return status.Error(codes.InvalidArgument, "invalid username")
	case domain.ErrInvalidPassword:
		return status.Error(codes.InvalidArgument, "invalid password")
	default:
		m.log.Error("start registration failed", logging.Err(err))
		return status.Error(codes.Internal, "internal server error")
	}
}
