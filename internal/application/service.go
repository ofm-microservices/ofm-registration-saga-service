package service

import (
	"context"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"registration-saga-service/config"
	"registration-saga-service/internal/domain"
)

type registrationService struct {
	sessions        SessionRepository
	steps           StepRepository
	broker          EventBroker
	emailChecker    EmailAvailabilityChecker
	usernameChecker UsernameAvailabilityChecker
	mapr            RegistrationMessageMapper
	cfg             config.NATSConfig
	log             logging.Logger
}

// New constructs the registration application service that owns saga-session
// creation and result-driven state transitions.
func New(
	sessions SessionRepository,
	steps StepRepository,
	broker EventBroker,
	emailChecker EmailAvailabilityChecker,
	usernameChecker UsernameAvailabilityChecker,
	cfg config.NATSConfig,
	log logging.Logger,
) (RegistrationService, error) {
	if sessions == nil {
		return nil, ErrNilSessionRepository
	}
	if steps == nil {
		return nil, ErrNilStepRepository
	}
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if emailChecker == nil {
		return nil, ErrNilEmailChecker
	}
	if usernameChecker == nil {
		return nil, ErrNilUsernameChecker
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &registrationService{
		sessions:        sessions,
		steps:           steps,
		broker:          broker,
		emailChecker:    emailChecker,
		usernameChecker: usernameChecker,
		mapr:            newRegistrationMessageMapper(),
		cfg:             cfg,
		log:             log.With(logging.String("module", "application")),
	}, nil
}

// Start validates the request, persists the initial saga state, and fan-outs
// the first commands required to create user and auth data.
func (s *registrationService) Start(ctx context.Context, params domain.StartRegistrationParams) (*domain.StartRegistrationResult, error) {
	input, err := normalizeStartInput(params)
	if err != nil {
		return nil, err
	}

	conflict, err := s.lookupStartConflict(ctx, input.email, input.username)
	if err != nil {
		return nil, err
	}
	if conflict != nil {
		return conflict, nil
	}

	session, err := s.createStartSession(ctx, input)
	if err != nil {
		return nil, err
	}
	if err := s.createStartSteps(ctx, session.SessionID); err != nil {
		return nil, err
	}

	passwordHash, err := hashStartPassword(input.password)
	if err != nil {
		return nil, err
	}

	go s.dispatchStartCommands(session, input, passwordHash)

	return &domain.StartRegistrationResult{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		UserID:    session.UserID,
		Status:    session.Status,
	}, nil
}

// HandleUserCreateResult records the outcome of the user profile creation step.
func (s *registrationService) HandleUserCreateResult(ctx context.Context, result UserCreateResult) error {
	if result.SessionID == "" {
		return domain.ErrInvalidSessionID
	}

	status := domain.StepStatusCompleted
	sessionStatus := ""
	if result.Status != "success" {
		status = domain.StepStatusFailed
		sessionStatus = domain.SessionStatusFailed
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyUserCreateProfile, status); err != nil {
		return err
	}
	if sessionStatus != "" {
		return s.sessions.UpdateStatus(ctx, result.SessionID, sessionStatus)
	}

	return s.syncSessionStatus(ctx, result.SessionID)
}

// HandleAuthCreatePendingResult records the auth outcome and activates the mail
// step when auth creation succeeds.
func (s *registrationService) HandleAuthCreatePendingResult(ctx context.Context, result AuthCreatePendingResult) error {
	if result.SessionID == "" {
		return domain.ErrInvalidSessionID
	}

	status := domain.StepStatusCompleted
	sessionStatus := ""
	if result.Status != "success" {
		status = domain.StepStatusFailed
		sessionStatus = domain.SessionStatusFailed
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyAuthCreatePending, status); err != nil {
		return err
	}
	if sessionStatus != "" {
		return s.sessions.UpdateStatus(ctx, result.SessionID, sessionStatus)
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyMailSendVerificationCode, domain.StepStatusInProgress); err != nil {
		return err
	}

	return s.syncSessionStatus(ctx, result.SessionID)
}

// HandleMailSendResult records the mail outcome and emits the user-facing
// "code sent" event on success.
func (s *registrationService) HandleMailSendResult(ctx context.Context, result MailSendResult) error {
	if result.SessionID == "" {
		return domain.ErrInvalidSessionID
	}

	status := domain.StepStatusCompleted
	if result.Status != "success" {
		status = domain.StepStatusFailed
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyMailSendVerificationCode, status); err != nil {
		return err
	}
	if result.Status != "success" {
		return s.sessions.UpdateStatus(ctx, result.SessionID, domain.SessionStatusFailed)
	}

	payload, err := s.mapr.ToCodeSentEventPayload(result)
	if err != nil {
		return err
	}

	if err := s.broker.Publish(ctx, s.cfg.RegistrationCodeSentSubject, payload); err != nil {
		return err
	}

	return s.syncSessionStatus(ctx, result.SessionID)
}

func (s *registrationService) syncSessionStatus(ctx context.Context, sessionID string) error {
	steps, err := s.steps.ListBySessionID(ctx, sessionID)
	if err != nil {
		return err
	}

	allCompleted := true
	for _, step := range steps {
		if step.Status == domain.StepStatusFailed {
			return s.sessions.UpdateStatus(ctx, sessionID, domain.SessionStatusFailed)
		}
		if step.Status != domain.StepStatusCompleted {
			allCompleted = false
		}
	}
	if !allCompleted {
		return nil
	}

	return s.sessions.UpdateStatus(ctx, sessionID, domain.SessionStatusCodeSent)
}

func (s *registrationService) lookupIncompleteConflict(ctx context.Context, email, username string) (*domain.StartRegistrationResult, error) {
	emailSession, err := s.sessions.GetByEmail(ctx, email)
	if err != nil && err != domain.ErrSessionNotFound {
		return nil, err
	}
	usernameSession, err := s.sessions.GetByUsername(ctx, username)
	if err != nil && err != domain.ErrSessionNotFound {
		return nil, err
	}

	return s.mapr.ToIncompleteConflictResult(emailSession, usernameSession), nil
}

func (s *registrationService) lookupCompletedConflict(ctx context.Context, email, username string) (*domain.StartRegistrationResult, error) {
	emailTaken, err := s.emailChecker.ExistsByEmail(ctx, email)
	if err != nil {
		return nil, err
	}
	usernameTaken, err := s.usernameChecker.ExistsByUsername(ctx, username)
	if err != nil {
		return nil, err
	}
	return s.mapr.ToCompletedConflictResult(emailTaken, usernameTaken), nil
}
