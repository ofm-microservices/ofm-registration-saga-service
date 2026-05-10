package service

import (
	"context"
	"net/mail"
	"strings"

	"github.com/google/uuid"
	"registration-saga-service/internal/domain"
)

type startInput struct {
	email     string
	username  string
	password  string
	clientID  string
	firstName string
	surname   string
}

func normalizeStartInput(params domain.StartRegistrationParams) (startInput, error) {
	input := startInput{
		email:     strings.TrimSpace(params.Email),
		username:  strings.TrimSpace(params.Username),
		password:  strings.TrimSpace(params.Password),
		clientID:  strings.TrimSpace(params.ClientID),
		firstName: strings.TrimSpace(params.FirstName),
		surname:   strings.TrimSpace(params.Surname),
	}

	if _, err := mail.ParseAddress(input.email); err != nil {
		return startInput{}, domain.ErrInvalidEmail
	}
	if input.username == "" {
		return startInput{}, domain.ErrInvalidUsername
	}
	if len(input.password) < 8 {
		return startInput{}, domain.ErrInvalidPassword
	}
	if input.clientID == "" {
		input.clientID = uuid.Must(uuid.NewV7()).String()
	}

	return input, nil
}

func (s *registrationService) lookupStartConflict(ctx context.Context, email, username string) (*domain.StartRegistrationResult, error) {
	incompleteConflict, err := s.lookupIncompleteConflict(ctx, email, username)
	if err != nil {
		return nil, err
	}
	if incompleteConflict != nil {
		return incompleteConflict, nil
	}

	return s.lookupCompletedConflict(ctx, email, username)
}

func (s *registrationService) createStartSession(ctx context.Context, input startInput) (domain.Session, error) {
	session := domain.Session{
		SessionID: uuid.Must(uuid.NewV7()).String(),
		ClientID:  input.clientID,
		UserID:    uuid.Must(uuid.NewV7()).String(),
		Email:     input.email,
		Username:  input.username,
		Status:    domain.SessionStatusStarted,
	}

	created, err := s.sessions.Create(ctx, session)
	if err != nil {
		return domain.Session{}, err
	}

	return *created, nil
}

func (s *registrationService) createStartSteps(ctx context.Context, sessionID string) error {
	steps := []domain.Step{
		{SessionID: sessionID, StepKey: domain.StepKeyUserCreateProfile, Status: domain.StepStatusPending},
		{SessionID: sessionID, StepKey: domain.StepKeyAuthCreatePending, Status: domain.StepStatusPending},
		{SessionID: sessionID, StepKey: domain.StepKeyMailSendVerificationCode, Status: domain.StepStatusPending},
	}

	for _, step := range steps {
		if _, err := s.steps.Create(ctx, step); err != nil {
			return err
		}
	}

	return nil
}
