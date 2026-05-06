package service

import (
	"context"
	"registration-saga-service/internal/domain"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"golang.org/x/crypto/bcrypt"
)

func hashStartPassword(password string) (string, error) {
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", ErrHashPassword
	}

	return string(passwordHash), nil
}

func (s *registrationService) dispatchStartCommands(session domain.Session, input startInput, passwordHash string) {
	runCtx := context.Background()

	if err := s.markStartInProgress(runCtx, session.SessionID); err != nil {
		return
	}
	if err := s.publishUserCreateCommand(runCtx, session, input.firstName, input.surname); err != nil {
		s.failStartStep(runCtx, session.SessionID, domain.StepKeyUserCreateProfile, "publish user create command failed", err)
		return
	}
	if err := s.publishAuthCreateCommand(runCtx, session, input.email, passwordHash); err != nil {
		s.failStartStep(runCtx, session.SessionID, domain.StepKeyAuthCreatePending, "publish auth create command failed", err)
	}
}

func (s *registrationService) markStartInProgress(ctx context.Context, sessionID string) error {
	if err := s.sessions.UpdateStatus(ctx, sessionID, domain.SessionStatusInProgress); err != nil {
		s.log.Error("update session status failed", logging.String("session_id", sessionID), logging.Err(err))
		return err
	}
	if err := s.steps.UpdateStatus(ctx, sessionID, domain.StepKeyUserCreateProfile, domain.StepStatusInProgress); err != nil {
		s.log.Error("update user step status failed", logging.String("session_id", sessionID), logging.Err(err))
		return err
	}
	if err := s.steps.UpdateStatus(ctx, sessionID, domain.StepKeyAuthCreatePending, domain.StepStatusInProgress); err != nil {
		s.log.Error("update auth step status failed", logging.String("session_id", sessionID), logging.Err(err))
		return err
	}

	return nil
}

func (s *registrationService) publishUserCreateCommand(ctx context.Context, session domain.Session, firstName, surname string) error {
	userPayload, err := s.mapr.ToUserCreateCommandPayload(session, firstName, surname)
	if err != nil {
		return err
	}

	return s.broker.Publish(ctx, s.cfg.UserCreateSubject, userPayload)
}

func (s *registrationService) publishAuthCreateCommand(ctx context.Context, session domain.Session, email, passwordHash string) error {
	authPayload, err := s.mapr.ToAuthCreatePendingCommandPayload(session, email, passwordHash)
	if err != nil {
		return err
	}

	return s.broker.Publish(ctx, s.cfg.AuthCreatePendingSubject, authPayload)
}

func (s *registrationService) failStartStep(ctx context.Context, sessionID, stepKey, message string, err error) {
	s.log.Error(message, logging.String("session_id", sessionID), logging.Err(err))
	_ = s.steps.UpdateStatus(ctx, sessionID, stepKey, domain.StepStatusFailed)
	_ = s.sessions.UpdateStatus(ctx, sessionID, domain.SessionStatusFailed)
}
