package service

import (
	"context"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"registration-saga-service/internal/domain"
	eventbroker "registration-saga-service/internal/presentation/event_broker"
)

// RegistrationService owns the registration workflow for the first saga slice
// and its result-driven transitions.
type RegistrationService interface {
	// Start creates the saga session and dispatches the first registration slice.
	Start(ctx context.Context, params domain.StartRegistrationParams) (*domain.StartRegistrationResult, error)
	// HandleUserCreateResult advances the saga with the user-service result.
	HandleUserCreateResult(ctx context.Context, result UserCreateResult) error
	// HandleAuthCreatePendingResult advances the saga with the auth-service result.
	HandleAuthCreatePendingResult(ctx context.Context, result AuthCreatePendingResult) error
	// HandleMailSendResult finalizes the first slice after mail delivery.
	HandleMailSendResult(ctx context.Context, result MailSendResult) error
}

// SessionRepository aliases the persistence contract used for saga sessions.
type SessionRepository = domain.SessionRepository

// StepRepository aliases the persistence contract used for saga steps.
type StepRepository = domain.StepRepository

// EventBroker aliases the transport contract used by the application layer.
type EventBroker = eventbroker.EventBroker

// Logger aliases the shared structured logger used by the application layer.
type Logger = logging.Logger

// EmailAvailabilityChecker reports whether auth-service already owns an email.
type EmailAvailabilityChecker interface {
	ExistsByEmail(ctx context.Context, email string) (bool, error)
}

// UsernameAvailabilityChecker reports whether user-service already owns a
// username.
type UsernameAvailabilityChecker interface {
	ExistsByUsername(ctx context.Context, username string) (bool, error)
}

// RegistrationMessageMapper translates the registration application state into
// outbound command and event payloads.
type RegistrationMessageMapper interface {
	ToUserCreateCommandPayload(session domain.Session, firstName, surname string) ([]byte, error)
	ToAuthCreatePendingCommandPayload(session domain.Session, email, passwordHash string) ([]byte, error)
	ToCodeSentEventPayload(result MailSendResult) ([]byte, error)
	ToIncompleteConflictResult(emailSession, usernameSession *domain.Session) *domain.StartRegistrationResult
	ToCompletedConflictResult(emailTaken, usernameTaken bool) *domain.StartRegistrationResult
}

// UserCreateCommand is the command published to user-service.
type UserCreateCommand struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name,omitempty"`
	LastName  string `json:"last_name,omitempty"`
}

// UserCreateResult is the user-service response consumed by the saga.
type UserCreateResult struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

// AuthCreatePendingCommand is the command published to auth-service for the
// initial registration slice.
type AuthCreatePendingCommand struct {
	SessionID    string `json:"session_id"`
	ClientID     string `json:"client_id"`
	UserID       string `json:"user_id"`
	Email        string `json:"email"`
	PasswordHash string `json:"password_hash"`
}

// AuthCreatePendingResult is the auth-service response consumed by the saga.
type AuthCreatePendingResult struct {
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

// MailSendResult is the mail-service response consumed by the saga.
type MailSendResult struct {
	SessionID     string `json:"session_id"`
	ClientID      string `json:"client_id"`
	UserID        string `json:"user_id"`
	RequestID     string `json:"request_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	MessageType   string `json:"message_type"`
	To            string `json:"to"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	Timestamp     string `json:"timestamp"`
}
