package service

import (
	"encoding/json"
	"registration-saga-service/internal/domain"
	"time"
)

type registrationMessageMapper struct{}

var jsonMarshal = json.Marshal

func newRegistrationMessageMapper() RegistrationMessageMapper {
	return &registrationMessageMapper{}
}

func (m *registrationMessageMapper) ToUserCreateCommandPayload(session domain.Session, firstName, surname string) ([]byte, error) {
	payload, err := jsonMarshal(UserCreateCommand{
		SessionID: session.SessionID,
		UserID:    session.UserID,
		Username:  session.Username,
		FirstName: firstName,
		LastName:  surname,
	})
	if err != nil {
		return nil, ErrPublishUserCreateCommand
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToAuthCreatePendingCommandPayload(session domain.Session, email, passwordHash string) ([]byte, error) {
	payload, err := jsonMarshal(AuthCreatePendingCommand{
		SessionID:    session.SessionID,
		ClientID:     session.ClientID,
		UserID:       session.UserID,
		Email:        email,
		Username:     session.Username,
		PasswordHash: passwordHash,
	})
	if err != nil {
		return nil, ErrPublishAuthCreateCommand
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToCodeSentEventPayload(result MailSendResult) ([]byte, error) {
	payload, err := jsonMarshal(struct {
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
		return nil, ErrPublishCodeSentEvent
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToRegistrationCompletedEventPayload(session domain.Session) ([]byte, error) {
	payload, err := jsonMarshal(struct {
		SessionID string `json:"session_id"`
		ClientID  string `json:"client_id"`
		UserID    string `json:"user_id"`
		Status    string `json:"status"`
		Timestamp string `json:"timestamp"`
	}{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		UserID:    session.UserID,
		Status:    domain.SessionStatusCompleted,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, ErrPublishRegistrationCompletedEvent
	}

	return payload, nil
}

func (m *registrationMessageMapper) ToRegistrationFailedEventPayload(session domain.Session, reason string) ([]byte, error) {
	payload, err := jsonMarshal(struct {
		SessionID string `json:"session_id"`
		ClientID  string `json:"client_id"`
		UserID    string `json:"user_id"`
		Status    string `json:"status"`
		Error     string `json:"error"`
		Timestamp string `json:"timestamp"`
	}{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		UserID:    session.UserID,
		Status:    domain.SessionStatusFailed,
		Error:     reason,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, ErrPublishRegistrationFailedEvent
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
