package service

import (
	"errors"
)

var (
	ErrNilSessionRepository              = errors.New("session repository is nil")
	ErrNilStepRepository                 = errors.New("step repository is nil")
	ErrNilEventBroker                    = errors.New("event broker is nil")
	ErrNilLogger                         = errors.New("logger is nil")
	ErrNilEmailChecker                   = errors.New("email availability checker is nil")
	ErrNilUsernameChecker                = errors.New("username availability checker is nil")
	ErrHashPassword                      = errors.New("hash password failed")
	ErrPublishUserCreateCommand          = errors.New("publish user create command failed")
	ErrPublishAuthCreateCommand          = errors.New("publish auth create pending registration command failed")
	ErrPublishCodeSentEvent              = errors.New("publish registration code sent event failed")
	ErrPublishRegistrationCompletedEvent = errors.New("publish registration completed event failed")
	ErrPublishRegistrationFailedEvent    = errors.New("publish registration failed event failed")
)
