package grpc

import "errors"

var (
	ErrNilRegistrationService = errors.New("registration service is nil")
	ErrNilLogger              = errors.New("logger is nil")
)
