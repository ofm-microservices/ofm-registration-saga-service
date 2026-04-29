package service

import (
	"encoding/json"
	"registration-saga-service/internal/domain"
	"time"
)

type registrationMessageMapper struct{}

func newRegistrationMessageMapper() RegistrationMessageMapper {
	return &registrationMessageMapper{}
}

func (m *registrationMessageMapper) ToUserCreateCommandPayload(session domain.Session, firstName, surname string) ([]byte, error) {
	payload, err := json.Marshal(UserCreateCommand{
		SessionID: session.SessionID,
		UserID:    session.UserID,
		Username:  session.Username,
		FirstName: firstName,
		LastName:  surname,
	})
	if err != nil {
		return nil, WrapPublishUserCreateCommandError(err)
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToAuthCreatePendingCommandPayload(session domain.Session, email, passwordHash string) ([]byte, error) {
	payload, err := json.Marshal(AuthCreatePendingCommand{
		SessionID:    session.SessionID,
		ClientID:     session.ClientID,
		UserID:       session.UserID,
		Email:        email,
		PasswordHash: passwordHash,
	})
	if err != nil {
		return nil, WrapPublishAuthCreateCommandError(err)
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToCodeSentEventPayload(result MailSendResult) ([]byte, error) {
	payload, err := json.Marshal(struct {
		SessionID string `json:"session_id"`
		ClientID  string `json:"client_id"`
		UserID    string `json:"user_id"`
		Status    string `json:"status"`
		Timestamp string `json:"timestamp"`
	}{
		SessionID: result.SessionID,
		ClientID:  result.ClientID,
		UserID:    result.UserID,
		Status:    domain.SessionStatusCodeSent,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, WrapPublishCodeSentEventError(err)
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToIncompleteConflictResult(emailSession, usernameSession *domain.Session) *domain.StartRegistrationResult {
	emailTaken := emailSession != nil && emailSession.Status != domain.SessionStatusFailed && emailSession.Status != domain.SessionStatusCompleted
	usernameTaken := usernameSession != nil && usernameSession.Status != domain.SessionStatusFailed && usernameSession.Status != domain.SessionStatusCompleted
	if !emailTaken && !usernameTaken {
		return nil
	}

	return &domain.StartRegistrationResult{
		Status:        "conflict",
		ConflictState: domain.AvailabilityStateIncomplete,
		UsernameTaken: usernameTaken,
		EmailTaken:    emailTaken,
	}
}

func (m *registrationMessageMapper) ToCompletedConflictResult(emailTaken, usernameTaken bool) *domain.StartRegistrationResult {
	if !emailTaken && !usernameTaken {
		return nil
	}

	return &domain.StartRegistrationResult{
		Status:        "conflict",
		ConflictState: domain.AvailabilityStateCompleted,
		UsernameTaken: usernameTaken,
		EmailTaken:    emailTaken,
	}
}
