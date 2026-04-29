package service

import (
	"errors"
	"fmt"
)

var (
	ErrNilSessionRepository = errors.New("session repository is nil")
	ErrNilStepRepository    = errors.New("step repository is nil")
	ErrNilEventBroker       = errors.New("event broker is nil")
	ErrNilLogger            = errors.New("logger is nil")
	ErrNilEmailChecker      = errors.New("email availability checker is nil")
	ErrNilUsernameChecker   = errors.New("username availability checker is nil")
)

// WrapHashPasswordError annotates bcrypt failures while preparing auth-service
// input.
func WrapHashPasswordError(err error) error {
	return fmt.Errorf("hash password: %w", err)
}

// WrapPublishUserCreateCommandError annotates user-service command publish
// failures.
func WrapPublishUserCreateCommandError(err error) error {
	return fmt.Errorf("publish user create command: %w", err)
}

// WrapPublishAuthCreateCommandError annotates auth-service command publish
// failures.
func WrapPublishAuthCreateCommandError(err error) error {
	return fmt.Errorf("publish auth create pending registration command: %w", err)
}

// WrapPublishCodeSentEventError annotates publication failures for the
// user-facing registration progress event.
func WrapPublishCodeSentEventError(err error) error {
	return fmt.Errorf("publish registration code sent event: %w", err)
}
