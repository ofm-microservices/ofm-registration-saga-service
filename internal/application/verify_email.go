package service

import (
	"context"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/internal/domain"
)

// VerifyEmail accepts an email verification command and continues the saga
// asynchronously.
func (s *registrationService) VerifyEmail(ctx context.Context, params domain.VerifyEmailParams) (*domain.VerifyEmailResult, error) {
	sessionID := strings.TrimSpace(params.SessionID)
	clientID := strings.TrimSpace(params.ClientID)
	code := strings.TrimSpace(params.Code)
	if sessionID == "" {
		return nil, domain.ErrInvalidSessionID
	}
	if clientID == "" {
		return nil, domain.ErrInvalidClientID
	}
	if code == "" {
		return nil, domain.ErrInvalidVerificationCode
	}

	session, err := s.sessions.GetByID(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if session.ClientID != clientID {
		return nil, domain.ErrClientMismatch
	}
	if session.Status == domain.SessionStatusCompleted {
		return &domain.VerifyEmailResult{SessionID: session.SessionID, ClientID: session.ClientID, Status: session.Status}, nil
	}
	if session.Status != domain.SessionStatusCodeSent {
		return nil, domain.ErrInvalidStatus
	}

	if _, err := s.steps.Create(ctx, domain.Step{SessionID: session.SessionID, StepKey: domain.StepKeyVerifyEmailCode, Status: domain.StepStatusInProgress}); err != nil {
		return nil, err
	}
	if _, err := s.steps.Create(ctx, domain.Step{SessionID: session.SessionID, StepKey: domain.StepKeyActivateUser, Status: domain.StepStatusPending}); err != nil {
		return nil, err
	}
	if err := s.sessions.UpdateStatus(ctx, session.SessionID, domain.SessionStatusVerifyingEmail); err != nil {
		return nil, err
	}

	go s.completeEmailVerification(context.Background(), *session, code)

	return &domain.VerifyEmailResult{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		Status:    domain.SessionStatusVerifyingEmail,
	}, nil
}

// GetRegistrationStatus returns the current state for the api-gateway token
// completion endpoint.
func (s *registrationService) GetRegistrationStatus(ctx context.Context, sessionID, clientID string) (*domain.RegistrationStatus, error) {
	sessionID = strings.TrimSpace(sessionID)
	clientID = strings.TrimSpace(clientID)
	if sessionID == "" {
		return nil, domain.ErrInvalidSessionID
	}
	if clientID == "" {
		return nil, domain.ErrInvalidClientID
	}

	session, err := s.sessions.GetByID(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if session.ClientID != clientID {
		return nil, domain.ErrClientMismatch
	}
	if session.Status == domain.SessionStatusCompleted {
		claimed, err := s.sessions.ClaimCompleted(ctx, session.SessionID)
		if err != nil {
			return nil, err
		}
		if claimed {
			session.Status = domain.SessionStatusCompleted
		} else {
			session.Status = domain.SessionStatusTokensClaimed
		}
	}

	return &domain.RegistrationStatus{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		UserID:    session.UserID,
		Status:    session.Status,
	}, nil
}

func (s *registrationService) completeEmailVerification(ctx context.Context, session domain.Session, code string) {
	if err := s.emailChecker.VerifyRegistrationEmail(ctx, session.UserID, code); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyVerifyEmailCode, "verify email failed", err)
		return
	}
	if err := s.steps.UpdateStatus(ctx, session.SessionID, domain.StepKeyVerifyEmailCode, domain.StepStatusCompleted); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyVerifyEmailCode, "update verify email step failed", err)
		return
	}

	if err := s.sessions.UpdateStatus(ctx, session.SessionID, domain.SessionStatusActivatingUser); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyActivateUser, "update session activation status failed", err)
		return
	}
	if err := s.steps.UpdateStatus(ctx, session.SessionID, domain.StepKeyActivateUser, domain.StepStatusInProgress); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyActivateUser, "update activate user step failed", err)
		return
	}
	if err := s.usernameChecker.ActivateUser(ctx, session.UserID); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyActivateUser, "activate user failed", err)
		return
	}
	if err := s.steps.UpdateStatus(ctx, session.SessionID, domain.StepKeyActivateUser, domain.StepStatusCompleted); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyActivateUser, "complete activate user step failed", err)
		return
	}
	if err := s.sessions.UpdateStatus(ctx, session.SessionID, domain.SessionStatusCompleted); err != nil {
		s.failEmailVerification(ctx, session, domain.StepKeyActivateUser, "complete registration session failed", err)
		return
	}

	payload, err := s.mapr.ToRegistrationCompletedEventPayload(session)
	if err != nil {
		s.log.Error("build registration completed event failed", logging.String("session_id", session.SessionID), logging.Err(err))
		return
	}
	if err := s.broker.Publish(ctx, s.cfg.RegistrationCompletedSubject, payload); err != nil {
		s.log.Error("publish registration completed event failed", logging.String("session_id", session.SessionID), logging.Err(err))
	}
}

func (s *registrationService) failEmailVerification(ctx context.Context, session domain.Session, stepKey, message string, err error) {
	s.log.Error(message, logging.String("session_id", session.SessionID), logging.Err(err))
	_ = s.steps.UpdateStatus(ctx, session.SessionID, stepKey, domain.StepStatusFailed)
	_ = s.sessions.UpdateStatus(ctx, session.SessionID, domain.SessionStatusFailed)
	_ = s.usernameChecker.DeactivateUser(ctx, session.UserID)
	_ = s.emailChecker.DeactivateRegistrationAuth(ctx, session.UserID)

	payload, mapErr := s.mapr.ToRegistrationFailedEventPayload(session, err.Error())
	if mapErr != nil {
		s.log.Error("build registration failed event failed", logging.String("session_id", session.SessionID), logging.Err(mapErr))
		return
	}
	if pubErr := s.broker.Publish(ctx, s.cfg.RegistrationFailedSubject, payload); pubErr != nil {
		s.log.Error("publish registration failed event failed", logging.String("session_id", session.SessionID), logging.Err(pubErr))
	}
}
