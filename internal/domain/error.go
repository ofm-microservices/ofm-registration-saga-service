package domain

import "errors"

var (
	// ErrInvalidSessionID reports a missing or malformed registration session id.
	ErrInvalidSessionID = errors.New("invalid session id")
	// ErrInvalidClientID reports a missing or malformed client routing id.
	ErrInvalidClientID = errors.New("invalid client id")
	// ErrInvalidUserID reports a missing or malformed user id.
	ErrInvalidUserID = errors.New("invalid user id")
	// ErrInvalidEmail reports an email that cannot be accepted for registration.
	ErrInvalidEmail = errors.New("invalid email")
	// ErrInvalidUsername reports a missing or malformed username.
	ErrInvalidUsername = errors.New("invalid username")
	// ErrInvalidPassword reports a password that does not satisfy minimal policy.
	ErrInvalidPassword = errors.New("invalid password")
	// ErrInvalidStatus reports an unsupported session or step status value.
	ErrInvalidStatus = errors.New("invalid status")
	// ErrInvalidStepKey reports an unknown orchestration step key.
	ErrInvalidStepKey = errors.New("invalid step key")
	// ErrSessionNotFound reports a missing saga session in persistence.
	ErrSessionNotFound = errors.New("registration session not found")
	// ErrStepNotFound reports a missing saga step in persistence.
	ErrStepNotFound = errors.New("registration step not found")
)
