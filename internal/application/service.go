package service

import (
	"context"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"registration-saga-service/config"
	"registration-saga-service/internal/domain"
	"time"
)

const registrationSagaMetricName = "registration"

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
	started := time.Now()
	status := "success"
	defer func() {
		metrics.Global().ObserveSagaStep(registrationSagaMetricName, "start", status, time.Since(started))
	}()

	input, err := normalizeStartInput(params)
	if err != nil {
		status = "error"
		return nil, err
	}

	conflict, err := s.lookupStartConflict(ctx, input.email, input.username)
	if err != nil {
		status = "error"
		return nil, err
	}
	if conflict != nil {
		metrics.Global().IncSagaFailed(registrationSagaMetricName)
		return conflict, nil
	}

	session, err := s.createStartSession(ctx, input)
	if err != nil {
		status = "error"
		return nil, err
	}
	if err := s.createStartSteps(ctx, session.SessionID); err != nil {
		status = "error"
		return nil, err
	}

	passwordHash, err := hashStartPassword(input.password)
	if err != nil {
		status = "error"
		return nil, err
	}

	go s.dispatchStartCommands(session, input, passwordHash)

	metrics.Global().IncSagaStarted(registrationSagaMetricName)
	metrics.Global().SetSagaActiveSessions(registrationSagaMetricName, 1)
	return &domain.StartRegistrationResult{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		UserID:    session.UserID,
		Status:    session.Status,
	}, nil
}

// HandleUserCreateResult records the outcome of the user profile creation step.
func (s *registrationService) HandleUserCreateResult(ctx context.Context, result UserCreateResult) error {
	started := time.Now()
	metricStatus := "success"
	defer func() {
		metrics.Global().ObserveSagaStep(registrationSagaMetricName, domain.StepKeyUserCreateProfile, metricStatus, time.Since(started))
	}()

	if result.SessionID == "" {
		metricStatus = "error"
		return domain.ErrInvalidSessionID
	}

	stepStatus := domain.StepStatusCompleted
	sessionStatus := ""
	if result.Status != "success" {
		stepStatus = domain.StepStatusFailed
		sessionStatus = domain.SessionStatusFailed
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyUserCreateProfile, stepStatus); err != nil {
		metricStatus = "error"
		return err
	}
	if sessionStatus != "" {
		metricStatus = "error"
		return s.compensateRegistrationFailure(ctx, result.SessionID, result.UserID, result.Error)
	}

	return s.syncSessionStatus(ctx, result.SessionID)
}

// HandleAuthCreatePendingResult records the auth outcome and activates the mail
// step when auth creation succeeds.
func (s *registrationService) HandleAuthCreatePendingResult(ctx context.Context, result AuthCreatePendingResult) error {
	started := time.Now()
	metricStatus := "success"
	defer func() {
		metrics.Global().ObserveSagaStep(registrationSagaMetricName, domain.StepKeyAuthCreatePending, metricStatus, time.Since(started))
	}()

	if result.SessionID == "" {
		metricStatus = "error"
		return domain.ErrInvalidSessionID
	}

	stepStatus := domain.StepStatusCompleted
	sessionStatus := ""
	if result.Status != "success" {
		stepStatus = domain.StepStatusFailed
		sessionStatus = domain.SessionStatusFailed
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyAuthCreatePending, stepStatus); err != nil {
		metricStatus = "error"
		return err
	}
	if sessionStatus != "" {
		metricStatus = "error"
		return s.compensateRegistrationFailure(ctx, result.SessionID, result.UserID, result.Error)
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyMailSendVerificationCode, domain.StepStatusInProgress); err != nil {
		metricStatus = "error"
		return err
	}

	return s.syncSessionStatus(ctx, result.SessionID)
}

// HandleMailSendResult records the mail outcome and emits the user-facing
// "code sent" event on success.
func (s *registrationService) HandleMailSendResult(ctx context.Context, result MailSendResult) error {
	started := time.Now()
	metricStatus := "success"
	defer func() {
		metrics.Global().ObserveSagaStep(registrationSagaMetricName, domain.StepKeyMailSendVerificationCode, metricStatus, time.Since(started))
	}()

	if result.SessionID == "" {
		metricStatus = "error"
		return domain.ErrInvalidSessionID
	}

	stepStatus := domain.StepStatusCompleted
	if result.Status != "success" {
		stepStatus = domain.StepStatusFailed
	}

	if err := s.steps.UpdateStatus(ctx, result.SessionID, domain.StepKeyMailSendVerificationCode, stepStatus); err != nil {
		metricStatus = "error"
		return err
	}
	if result.Status != "success" {
		metricStatus = "error"
		return s.compensateRegistrationFailure(ctx, result.SessionID, result.UserID, result.Error)
	}

	payload, err := s.mapr.ToCodeSentEventPayload(result)
	if err != nil {
		metricStatus = "error"
		return err
	}

	if err := s.broker.Publish(ctx, s.cfg.RegistrationCodeSentSubject, payload); err != nil {
		metricStatus = "error"
		return err
	}

	return s.syncSessionStatus(ctx, result.SessionID)
}

func (s *registrationService) syncSessionStatus(ctx context.Context, sessionID string) error {
	// Result events are consumed serially. Reconcile briefly so a replica can
	// expose the just-written step, but never block the consumer for seconds;
	// the next result event will retry the reconciliation.
	for attempt := 0; attempt < 3; attempt++ {
		steps, err := s.steps.ListBySessionID(ctx, sessionID)
		if err != nil {
			return err
		}

		allCompleted := true
		for _, step := range steps {
			if step.Status == domain.StepStatusFailed {
				return s.compensateRegistrationFailure(ctx, sessionID, "", "registration step failed")
			}
			if step.Status != domain.StepStatusCompleted {
				allCompleted = false
			}
		}
		if allCompleted {
			s.log.Info("registration steps reconciled",
				logging.Operation("registration.saga.reconcile"),
				logging.String("session_id", sessionID),
				logging.Int("attempt", attempt+1),
			)
			break
		}
		if attempt == 2 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := s.sessions.UpdateStatus(ctx, sessionID, domain.SessionStatusCodeSent); err != nil {
		return err
	}
	metrics.Global().IncSagaCompleted(registrationSagaMetricName)
	metrics.Global().SetSagaActiveSessions(registrationSagaMetricName, 0)
	return nil
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

func (s *registrationService) compensateRegistrationFailure(ctx context.Context, sessionID, userID, reason string) error {
	metrics.Global().IncSagaCompensationStarted(registrationSagaMetricName)
	log := logging.WithContext(ctx, s.log)
	started := time.Now()
	session, err := s.sessions.GetByID(ctx, sessionID)
	if err != nil {
		metrics.Global().IncSagaCompensationFailed(registrationSagaMetricName)
		log.Error("compensation session lookup failed",
			logging.Operation("saga.registration.compensate"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		return err
	}
	if userID == "" {
		userID = session.UserID
	}

	if err := s.sessions.UpdateStatus(ctx, sessionID, domain.SessionStatusFailed); err != nil {
		metrics.Global().IncSagaCompensationFailed(registrationSagaMetricName)
		log.Error("compensation session update failed",
			logging.Operation("saga.registration.compensate"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		return err
	}

	if userID != "" {
		if err := s.usernameChecker.DeactivateUser(ctx, userID); err != nil {
			metrics.Global().IncSagaCompensationFailed(registrationSagaMetricName)
			log.Error("compensate user failed",
				logging.Operation("saga.registration.compensate_user"),
				logging.Attempt(1),
				logging.Retryable(true),
				logging.DurationMS(time.Since(started)),
				logging.String("user_id", userID),
				logging.Err(err),
			)
		}
		if err := s.emailChecker.DeactivateRegistrationAuth(ctx, userID); err != nil {
			metrics.Global().IncSagaCompensationFailed(registrationSagaMetricName)
			log.Error("compensate auth failed",
				logging.Operation("saga.registration.compensate_auth"),
				logging.Attempt(1),
				logging.Retryable(true),
				logging.DurationMS(time.Since(started)),
				logging.String("user_id", userID),
				logging.Err(err),
			)
		}
	}

	payload, err := s.mapr.ToRegistrationFailedEventPayload(*session, reason)
	if err != nil {
		log.Error("build registration failed event failed",
			logging.Operation("saga.registration.build_failed_event"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		metrics.Global().IncSagaCompensationFailed(registrationSagaMetricName)
		return nil
	}
	if err := s.broker.Publish(ctx, s.cfg.RegistrationFailedSubject, payload); err != nil {
		log.Error("publish registration failed event failed",
			logging.Operation("saga.registration.publish_failed_event"),
			logging.Attempt(1),
			logging.Retryable(true),
			logging.DurationMS(time.Since(started)),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		metrics.Global().IncSagaCompensationFailed(registrationSagaMetricName)
	}
	metrics.Global().IncSagaFailed(registrationSagaMetricName)
	metrics.Global().IncSagaCompensationCompleted(registrationSagaMetricName)
	metrics.Global().SetSagaActiveSessions(registrationSagaMetricName, 0)

	return nil
}
