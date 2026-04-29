package domain

import (
	"context"
	"time"
)

const (
	// SessionStatusStarted marks a newly created session before command fanout.
	SessionStatusStarted = "started"
	// SessionStatusInProgress marks a session with at least one in-flight step.
	SessionStatusInProgress = "in_progress"
	// SessionStatusCodeSent marks the first registration slice as successful.
	SessionStatusCodeSent = "code_sent"
	// SessionStatusCompleted marks a fully confirmed registration workflow.
	SessionStatusCompleted = "completed"
	// SessionStatusFailed marks a non-recoverable failure in the current slice.
	SessionStatusFailed = "failed"

	AvailabilityStateNotFound   = "not_found"
	AvailabilityStateIncomplete = "found_incomplete"
	AvailabilityStateCompleted  = "found_completed"

	// StepStatusPending marks a step created but not yet dispatched.
	StepStatusPending = "pending"
	// StepStatusInProgress marks a step that has been dispatched and is awaiting a result.
	StepStatusInProgress = "in_progress"
	// StepStatusCompleted marks a step that finished successfully.
	StepStatusCompleted = "completed"
	// StepStatusFailed marks a step that finished with a failure result.
	StepStatusFailed = "failed"

	// StepKeyUserCreateProfile identifies the user-service profile creation step.
	StepKeyUserCreateProfile = "user.create_profile"
	// StepKeyAuthCreatePending identifies auth credential creation plus code generation.
	StepKeyAuthCreatePending = "auth.create_pending_registration"
	// StepKeyMailSendVerificationCode identifies verification-code email delivery.
	StepKeyMailSendVerificationCode = "mail.send_verification_code"
)

// Session is the persisted write-model snapshot of a registration saga.
type Session struct {
	SessionID string
	ClientID  string
	UserID    string
	Email     string
	Username  string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Step is the persisted execution state of one orchestration step inside a
// registration session.
type Step struct {
	SessionID string
	StepKey   string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// StartRegistrationParams is the input accepted by the registration use case.
type StartRegistrationParams struct {
	ClientID  string
	Email     string
	Username  string
	Password  string
	FirstName string
	Surname   string
}

// StartRegistrationResult is returned immediately after the session and its
// initial orchestration state are created.
type StartRegistrationResult struct {
	SessionID     string
	ClientID      string
	UserID        string
	Status        string
	ConflictState string
	UsernameTaken bool
	EmailTaken    bool
}

// SessionRepository persists registration sessions owned by the saga domain.
type SessionRepository interface {
	Create(ctx context.Context, session Session) (*Session, error)
	GetByID(ctx context.Context, sessionID string) (*Session, error)
	GetByEmail(ctx context.Context, email string) (*Session, error)
	GetByUsername(ctx context.Context, username string) (*Session, error)
	UpdateStatus(ctx context.Context, sessionID, status string) error
}

// StepRepository persists registration step state owned by the saga domain.
type StepRepository interface {
	Create(ctx context.Context, step Step) (*Step, error)
	GetByKey(ctx context.Context, sessionID, stepKey string) (*Step, error)
	ListBySessionID(ctx context.Context, sessionID string) ([]Step, error)
	UpdateStatus(ctx context.Context, sessionID, stepKey, status string) error
}
