package scylla

import (
	"errors"
	"fmt"
)

// ErrNilLogger reports a missing logger dependency.
var ErrNilLogger = errors.New("logger is nil")

// WrapCreateSessionError annotates session insert failures.
func WrapCreateSessionError(err error) error {
	return fmt.Errorf("create registration session: %w", err)
}

// WrapGetSessionByIDError annotates session lookup-by-id failures.
func WrapGetSessionByIDError(err error) error {
	return fmt.Errorf("get registration session by id: %w", err)
}

// WrapUpdateSessionStatusError annotates session status update failures.
func WrapUpdateSessionStatusError(err error) error {
	return fmt.Errorf("update registration session status: %w", err)
}

// WrapGetSessionByEmailError annotates session lookup-by-email failures.
func WrapGetSessionByEmailError(err error) error {
	return fmt.Errorf("get registration session by email: %w", err)
}

// WrapGetSessionByUsernameError annotates session lookup-by-username failures.
func WrapGetSessionByUsernameError(err error) error {
	return fmt.Errorf("get registration session by username: %w", err)
}

// WrapCreateStepError annotates step insert failures.
func WrapCreateStepError(err error) error {
	return fmt.Errorf("create registration step: %w", err)
}

// WrapGetStepByKeyError annotates step lookup failures.
func WrapGetStepByKeyError(err error) error {
	return fmt.Errorf("get registration step by key: %w", err)
}

// WrapListStepsBySessionIDError annotates step listing failures.
func WrapListStepsBySessionIDError(err error) error {
	return fmt.Errorf("list registration steps by session id: %w", err)
}

// WrapUpdateStepStatusError annotates step status update failures.
func WrapUpdateStepStatusError(err error) error {
	return fmt.Errorf("update registration step status: %w", err)
}
