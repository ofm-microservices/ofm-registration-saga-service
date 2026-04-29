package model

import "time"

// SessionRow is the Scylla persistence model for registration sessions.
type SessionRow struct {
	SessionID string
	ClientID  string
	UserID    string
	Email     string
	Username  string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// StepRow is the Scylla persistence model for registration steps.
type StepRow struct {
	SessionID string
	StepKey   string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}
