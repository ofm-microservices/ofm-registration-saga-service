package scylla

import (
	"context"
	"errors"
	"registration-saga-service/internal/domain"
	"registration-saga-service/internal/infra/scylla/model"
	"time"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type sessionRepository struct {
	db  *gocql.Session
	log logging.Logger
}

type stepRepository struct {
	db  *gocql.Session
	log logging.Logger
}

// NewSessionRepository constructs the Scylla-backed registration session
// repository.
func NewSessionRepository(db *gocql.Session, log logging.Logger) (domain.SessionRepository, error) {
	if db == nil {
		return nil, errors.New("scylla session is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &sessionRepository{db: db, log: log.With(logging.String("module", "scylla-session-repository"))}, nil
}

// NewStepRepository constructs the Scylla-backed registration step repository.
func NewStepRepository(db *gocql.Session, log logging.Logger) (domain.StepRepository, error) {
	if db == nil {
		return nil, errors.New("scylla session is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &stepRepository{db: db, log: log.With(logging.String("module", "scylla-step-repository"))}, nil
}

func (r *sessionRepository) Create(ctx context.Context, session domain.Session) (*domain.Session, error) {
	now := time.Now().UTC()
	row := model.SessionRow{
		SessionID: session.SessionID,
		ClientID:  session.ClientID,
		UserID:    session.UserID,
		Email:     session.Email,
		Username:  session.Username,
		Status:    session.Status,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := r.db.Query(
		insertSessionQuery,
		row.SessionID,
		row.ClientID,
		row.UserID,
		row.Email,
		row.Username,
		row.Status,
		row.CreatedAt,
		row.UpdatedAt,
	).WithContext(ctx).Exec(); err != nil {
		r.log.Error("create registration session failed",
			logging.Operation("db.registration.session.create"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", row.SessionID),
			logging.Err(err),
		)
		return nil, WrapCreateSessionError(err)
	}

	if err := r.db.Query(
		insertSessionByEmailQuery,
		row.Email,
		row.SessionID,
		row.ClientID,
		row.UserID,
		row.Username,
		row.Status,
		row.CreatedAt,
		row.UpdatedAt,
	).WithContext(ctx).Exec(); err != nil {
		r.log.Error("create registration session by email failed",
			logging.Operation("db.registration.session.create_by_email"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", row.SessionID),
			logging.String("email", row.Email),
			logging.Err(err),
		)
		return nil, WrapCreateSessionError(err)
	}

	if err := r.db.Query(
		insertSessionByUsernameQuery,
		row.Username,
		row.SessionID,
		row.ClientID,
		row.UserID,
		row.Email,
		row.Status,
		row.CreatedAt,
		row.UpdatedAt,
	).WithContext(ctx).Exec(); err != nil {
		r.log.Error("create registration session by username failed",
			logging.Operation("db.registration.session.create_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", row.SessionID),
			logging.String("username", row.Username),
			logging.Err(err),
		)
		return nil, WrapCreateSessionError(err)
	}

	return &domain.Session{
		SessionID: row.SessionID,
		ClientID:  row.ClientID,
		UserID:    row.UserID,
		Email:     row.Email,
		Username:  row.Username,
		Status:    row.Status,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *sessionRepository) GetByID(ctx context.Context, sessionID string) (*domain.Session, error) {
	var row model.SessionRow
	if err := r.db.Query(getSessionByIDQuery, sessionID).WithContext(ctx).Consistency(gocql.One).
		Scan(&row.SessionID, &row.ClientID, &row.UserID, &row.Email, &row.Username, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrSessionNotFound
		}
		r.log.Error("get registration session by id failed",
			logging.Operation("db.registration.session.get_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		return nil, WrapGetSessionByIDError(err)
	}

	return &domain.Session{
		SessionID: row.SessionID,
		ClientID:  row.ClientID,
		UserID:    row.UserID,
		Email:     row.Email,
		Username:  row.Username,
		Status:    row.Status,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *sessionRepository) GetByEmail(ctx context.Context, email string) (*domain.Session, error) {
	var row model.SessionRow
	if err := r.db.Query(getSessionByEmailQuery, email).WithContext(ctx).Consistency(gocql.One).
		Scan(&row.SessionID, &row.ClientID, &row.UserID, &row.Email, &row.Username, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrSessionNotFound
		}
		r.log.Error("get registration session by email failed",
			logging.Operation("db.registration.session.get_by_email"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("email", email),
			logging.Err(err),
		)
		return nil, WrapGetSessionByEmailError(err)
	}

	return &domain.Session{
		SessionID: row.SessionID,
		ClientID:  row.ClientID,
		UserID:    row.UserID,
		Email:     row.Email,
		Username:  row.Username,
		Status:    row.Status,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *sessionRepository) GetByUsername(ctx context.Context, username string) (*domain.Session, error) {
	var row model.SessionRow
	if err := r.db.Query(getSessionByUsernameQuery, username).WithContext(ctx).Consistency(gocql.One).
		Scan(&row.SessionID, &row.ClientID, &row.UserID, &row.Email, &row.Username, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrSessionNotFound
		}
		r.log.Error("get registration session by username failed",
			logging.Operation("db.registration.session.get_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("username", username),
			logging.Err(err),
		)
		return nil, WrapGetSessionByUsernameError(err)
	}

	return &domain.Session{
		SessionID: row.SessionID,
		ClientID:  row.ClientID,
		UserID:    row.UserID,
		Email:     row.Email,
		Username:  row.Username,
		Status:    row.Status,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *sessionRepository) UpdateStatus(ctx context.Context, sessionID, status string) error {
	session, err := r.GetByID(ctx, sessionID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	if err := r.db.Query(updateSessionStatusQuery, status, now, sessionID).WithContext(ctx).Exec(); err != nil {
		r.log.Error("update registration session status failed",
			logging.Operation("db.registration.session.update_status"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		return WrapUpdateSessionStatusError(err)
	}
	if err := r.db.Query(updateSessionByEmailStatusQuery, status, now, session.Email).WithContext(ctx).Exec(); err != nil {
		r.log.Error("update registration session by email status failed",
			logging.Operation("db.registration.session.update_by_email_status"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.String("email", session.Email),
			logging.Err(err),
		)
		return WrapUpdateSessionStatusError(err)
	}
	if err := r.db.Query(updateSessionByUsernameStatusQuery, status, now, session.Username).WithContext(ctx).Exec(); err != nil {
		r.log.Error("update registration session by username status failed",
			logging.Operation("db.registration.session.update_by_username_status"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.String("username", session.Username),
			logging.Err(err),
		)
		return WrapUpdateSessionStatusError(err)
	}
	return nil
}

func (r *sessionRepository) ClaimCompleted(ctx context.Context, sessionID string) (bool, error) {
	session, err := r.GetByID(ctx, sessionID)
	if err != nil {
		return false, err
	}

	now := time.Now().UTC()
	var currentStatus string
	applied, err := r.db.Query(
		claimCompletedSessionQuery,
		domain.SessionStatusTokensClaimed,
		now,
		sessionID,
		domain.SessionStatusCompleted,
	).WithContext(ctx).ScanCAS(&currentStatus)
	if err != nil {
		r.log.Error("claim registration session failed",
			logging.Operation("db.registration.session.claim_completed"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.Err(err),
		)
		return false, WrapUpdateSessionStatusError(err)
	}
	if !applied {
		return false, nil
	}

	if err := r.db.Query(updateSessionByEmailStatusQuery, domain.SessionStatusTokensClaimed, now, session.Email).WithContext(ctx).Exec(); err != nil {
		r.log.Error("claim registration session by email failed",
			logging.Operation("db.registration.session.claim_by_email"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.String("email", session.Email),
			logging.Err(err),
		)
		return false, WrapUpdateSessionStatusError(err)
	}
	if err := r.db.Query(updateSessionByUsernameStatusQuery, domain.SessionStatusTokensClaimed, now, session.Username).WithContext(ctx).Exec(); err != nil {
		r.log.Error("claim registration session by username failed",
			logging.Operation("db.registration.session.claim_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("session_id", sessionID),
			logging.String("username", session.Username),
			logging.Err(err),
		)
		return false, WrapUpdateSessionStatusError(err)
	}
	return true, nil
}

func (r *stepRepository) Create(ctx context.Context, step domain.Step) (*domain.Step, error) {
	now := time.Now().UTC()
	row := model.StepRow{
		SessionID: step.SessionID,
		StepKey:   step.StepKey,
		Status:    step.Status,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := r.db.Query(insertStepQuery, row.SessionID, row.StepKey, row.Status, row.CreatedAt, row.UpdatedAt).
		WithContext(ctx).Exec(); err != nil {
		return nil, WrapCreateStepError(err)
	}

	return &domain.Step{
		SessionID: row.SessionID,
		StepKey:   row.StepKey,
		Status:    row.Status,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *stepRepository) GetByKey(ctx context.Context, sessionID, stepKey string) (*domain.Step, error) {
	var row model.StepRow
	if err := r.db.Query(getStepByKeyQuery, sessionID, stepKey).WithContext(ctx).Consistency(gocql.One).
		Scan(&row.SessionID, &row.StepKey, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrStepNotFound
		}
		return nil, WrapGetStepByKeyError(err)
	}

	return &domain.Step{
		SessionID: row.SessionID,
		StepKey:   row.StepKey,
		Status:    row.Status,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}, nil
}

func (r *stepRepository) ListBySessionID(ctx context.Context, sessionID string) ([]domain.Step, error) {
	// Saga completion is derived from all three rows in this partition. A
	// default ONE read can observe one stale replica immediately after the
	// result consumers update the steps and leave the session stuck in progress.
	iter := r.db.Query(listStepsBySessionIDQuery, sessionID).WithContext(ctx).Consistency(gocql.Quorum).Iter()
	defer iter.Close()

	steps := make([]domain.Step, 0, 3)
	var row model.StepRow
	for iter.Scan(&row.SessionID, &row.StepKey, &row.Status, &row.CreatedAt, &row.UpdatedAt) {
		steps = append(steps, domain.Step{
			SessionID: row.SessionID,
			StepKey:   row.StepKey,
			Status:    row.Status,
			CreatedAt: row.CreatedAt,
			UpdatedAt: row.UpdatedAt,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, WrapListStepsBySessionIDError(err)
	}

	return steps, nil
}

func (r *stepRepository) UpdateStatus(ctx context.Context, sessionID, stepKey, status string) error {
	query := updateStepStatusQuery
	if status == domain.StepStatusInProgress {
		query = updateStepInProgressQuery
	}
	if err := r.db.Query(query, status, time.Now().UTC(), sessionID, stepKey).WithContext(ctx).Exec(); err != nil {
		return WrapUpdateStepStatusError(err)
	}
	return nil
}
