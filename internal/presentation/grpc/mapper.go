package grpc

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	registrationv1 "github.com/ofm-microservices/ofm-common/proto/registration/v1"
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

func (m *registrationMapper) ToVerifyEmailParams(req *registrationv1.VerifyEmailRequest) domain.VerifyEmailParams {
	return domain.VerifyEmailParams{
		SessionID: req.GetSessionId(),
		ClientID:  req.GetClientId(),
		Code:      req.GetCode(),
	}
}

func (m *registrationMapper) ToVerifyEmailResponse(result *domain.VerifyEmailResult) *registrationv1.VerifyEmailResponse {
	return &registrationv1.VerifyEmailResponse{
		SessionId: result.SessionID,
		ClientId:  result.ClientID,
		Status:    result.Status,
	}
}

func (m *registrationMapper) ToRegistrationStatusResponse(result *domain.RegistrationStatus) *registrationv1.GetRegistrationStatusResponse {
	return &registrationv1.GetRegistrationStatusResponse{
		SessionId: result.SessionID,
		ClientId:  result.ClientID,
		UserId:    result.UserID,
		Status:    result.Status,
	}
}

func (m *registrationMapper) ToStartError(err error) error {
	switch err {
	case domain.ErrInvalidSessionID:
		return status.Error(codes.InvalidArgument, "invalid session id")
	case domain.ErrInvalidClientID:
		return status.Error(codes.InvalidArgument, "invalid client id")
	case domain.ErrInvalidVerificationCode:
		return status.Error(codes.InvalidArgument, "invalid verification code")
	case domain.ErrInvalidEmail:
		return status.Error(codes.InvalidArgument, "invalid email")
	case domain.ErrInvalidUsername:
		return status.Error(codes.InvalidArgument, "invalid username")
	case domain.ErrInvalidPassword:
		return status.Error(codes.InvalidArgument, "invalid password")
	case domain.ErrSessionNotFound:
		return status.Error(codes.NotFound, "registration session not found")
	case domain.ErrClientMismatch:
		return status.Error(codes.PermissionDenied, "registration client mismatch")
	case domain.ErrInvalidStatus:
		return status.Error(codes.FailedPrecondition, "registration is not ready for this operation")
	default:
		m.log.Error("registration request failed", logging.Err(err))
		return status.Error(codes.Internal, "internal server error")
	}
}
