package postgres

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"registration-saga-service/internal/domain"
	"time"
)

// NewSessionRepository constructs the PostgreSQL-backed registration session repository.
func NewSessionRepository(db *sqlx.DB, lg logging.Logger) (domain.SessionRepository, error) {
	if db == nil {
		return nil, errors.New("postgres database is nil")
	}
	if lg == nil {
		return nil, errors.New("logger is nil")
	}
	return &sessionRepo{db: db, log: lg}, nil
}

// NewStepRepository constructs the PostgreSQL-backed registration step repository.
func NewStepRepository(db *sqlx.DB, lg logging.Logger) (domain.StepRepository, error) {
	if db == nil {
		return nil, errors.New("postgres database is nil")
	}
	if lg == nil {
		return nil, errors.New("logger is nil")
	}
	return &stepRepo{db: db, log: lg}, nil
}

type sessionRepo struct {
	db  *sqlx.DB
	log logging.Logger
}

// ReconcileStatus serializes aggregate status calculation for one registration
// session. Result consumers update individual steps independently, so this
// transaction lock prevents a stale reconciliation from overwriting a newer
// aggregate status.
func (r *sessionRepo) ReconcileStatus(ctx context.Context, sessionID string) (string, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, sessionID); err != nil {
		return "", err
	}
	var session sessionRow
	if err = tx.GetContext(ctx, &session, `SELECT session_id,client_id,user_id,email,username,status,created_at,updated_at FROM registration_saga_sessions WHERE session_id=$1 FOR UPDATE`, sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", domain.ErrSessionNotFound
		}
		return "", err
	}
	var steps []stepRow
	if err = tx.SelectContext(ctx, &steps, `SELECT session_id,step_key,status,created_at,updated_at FROM registration_saga_steps WHERE session_id=$1 ORDER BY step_key`, sessionID); err != nil {
		return "", err
	}
	status := domain.SessionStatusInProgress
	allCompleted := true
	for _, step := range steps {
		if step.Status == domain.StepStatusFailed {
			status = domain.SessionStatusFailed
			allCompleted = false
			break
		}
		if step.Status != domain.StepStatusCompleted {
			allCompleted = false
		}
	}
	if allCompleted {
		status = domain.SessionStatusCodeSent
	}
	if _, err = tx.ExecContext(ctx, `UPDATE registration_saga_sessions SET status=$1,updated_at=$2 WHERE session_id=$3`, status, time.Now().UTC(), sessionID); err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return status, nil
}

type stepRepo struct {
	db  *sqlx.DB
	log logging.Logger
}
type sessionRow struct {
	SessionID string    `db:"session_id"`
	ClientID  string    `db:"client_id"`
	UserID    string    `db:"user_id"`
	Email     string    `db:"email"`
	Username  string    `db:"username"`
	Status    string    `db:"status"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}
type stepRow struct {
	SessionID string    `db:"session_id"`
	StepKey   string    `db:"step_key"`
	Status    string    `db:"status"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func sm(r sessionRow) *domain.Session {
	return &domain.Session{SessionID: r.SessionID, ClientID: r.ClientID, UserID: r.UserID, Email: r.Email, Username: r.Username, Status: r.Status, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
}
func st(r stepRow) *domain.Step {
	return &domain.Step{SessionID: r.SessionID, StepKey: r.StepKey, Status: r.Status, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
}

const ss = `SELECT session_id,client_id,user_id,email,username,status,created_at,updated_at FROM registration_saga_sessions `

func (r *sessionRepo) Create(ctx context.Context, s domain.Session) (*domain.Session, error) {
	now := time.Now().UTC()
	_, e := r.db.ExecContext(ctx, `INSERT INTO registration_saga_sessions(session_id,client_id,user_id,email,username,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, s.SessionID, s.ClientID, s.UserID, s.Email, s.Username, s.Status, now, now)
	if e != nil {
		return nil, e
	}
	s.CreatedAt = now
	s.UpdatedAt = now
	return &s, nil
}
func (r *sessionRepo) one(ctx context.Context, q string, arg any) (*domain.Session, error) {
	var x sessionRow
	e := r.db.GetContext(ctx, &x, q, arg)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, domain.ErrSessionNotFound
	}
	if e != nil {
		return nil, e
	}
	return sm(x), nil
}
func (r *sessionRepo) GetByID(ctx context.Context, id string) (*domain.Session, error) {
	return r.one(ctx, ss+`WHERE session_id=$1`, id)
}
func (r *sessionRepo) GetByEmail(ctx context.Context, v string) (*domain.Session, error) {
	return r.one(ctx, ss+`WHERE email=$1 ORDER BY created_at DESC LIMIT 1`, v)
}
func (r *sessionRepo) GetByUsername(ctx context.Context, v string) (*domain.Session, error) {
	return r.one(ctx, ss+`WHERE username=$1 ORDER BY created_at DESC LIMIT 1`, v)
}
func (r *sessionRepo) UpdateStatus(ctx context.Context, id, status string) error {
	_, e := r.db.ExecContext(ctx, `UPDATE registration_saga_sessions SET status=$1,updated_at=$2 WHERE session_id=$3`, status, time.Now().UTC(), id)
	return e
}
func (r *sessionRepo) ClaimCompleted(ctx context.Context, id string) (bool, error) {
	tx, e := r.db.BeginTxx(ctx, nil)
	if e != nil {
		return false, e
	}
	defer tx.Rollback()
	var n int
	e = tx.GetContext(ctx, &n, `UPDATE registration_saga_sessions SET status=$1,updated_at=$2 WHERE session_id=$3 AND status=$4 RETURNING 1`, domain.SessionStatusTokensClaimed, time.Now().UTC(), id, domain.SessionStatusCompleted)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if e = tx.Commit(); e != nil {
		return false, e
	}
	return n == 1, nil
}
func (r *stepRepo) Create(ctx context.Context, s domain.Step) (*domain.Step, error) {
	now := time.Now().UTC()
	_, e := r.db.ExecContext(ctx, `INSERT INTO registration_saga_steps(session_id,step_key,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5)`, s.SessionID, s.StepKey, s.Status, now, now)
	if e != nil {
		return nil, e
	}
	s.CreatedAt = now
	s.UpdatedAt = now
	return &s, nil
}
func (r *stepRepo) GetByKey(ctx context.Context, saga, key string) (*domain.Step, error) {
	var x stepRow
	e := r.db.GetContext(ctx, &x, `SELECT session_id,step_key,status,created_at,updated_at FROM registration_saga_steps WHERE session_id=$1 AND step_key=$2`, saga, key)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, domain.ErrStepNotFound
	}
	if e != nil {
		return nil, e
	}
	return st(x), nil
}
func (r *stepRepo) ListBySessionID(ctx context.Context, saga string) ([]domain.Step, error) {
	var xs []stepRow
	if e := r.db.SelectContext(ctx, &xs, `SELECT session_id,step_key,status,created_at,updated_at FROM registration_saga_steps WHERE session_id=$1 ORDER BY step_key`, saga); e != nil {
		return nil, e
	}
	out := make([]domain.Step, 0, len(xs))
	for _, x := range xs {
		out = append(out, *st(x))
	}
	return out, nil
}
func (r *stepRepo) UpdateStatus(ctx context.Context, saga, key, status string) error {
	// A result from another Kafka topic may have already completed this step.
	// Never regress a completed step when the auth result activates the mail
	// step after the mail result arrived first.
	_, e := r.db.ExecContext(ctx, `UPDATE registration_saga_steps SET status=$1,updated_at=$2 WHERE session_id=$3 AND step_key=$4 AND NOT (status=$5 AND $1=$6)`, status, time.Now().UTC(), saga, key, domain.StepStatusCompleted, domain.StepStatusInProgress)
	return e
}
