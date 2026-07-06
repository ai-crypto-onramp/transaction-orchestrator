// Package store provides durable persistence for sagas, transaction steps, and
// outbox events, backed by PostgreSQL via pgxpool.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TxStatus is the state-machine value stored on transactions.status.
type TxStatus string

const (
	TxStatusCreated           TxStatus = "created"
	TxStatusPolicyChecked     TxStatus = "policy_checked"
	TxStatusPaymentCaptured   TxStatus = "payment_captured"
	TxStatusKytScreened       TxStatus = "kyt_screened"
	TxStatusSigned            TxStatus = "signed"
	TxStatusBroadcasted       TxStatus = "broadcasted"
	TxStatusConfirmed         TxStatus = "confirmed"
	TxStatusLedgered          TxStatus = "ledgered"
	TxStatusCompleted         TxStatus = "completed"
	TxStatusFailedCompensated TxStatus = "failed_compensated"
	TxStatusFailed            TxStatus = "failed"
)

// StepStatus is the per-step lifecycle value.
type StepStatus string

const (
	StepStatusPending      StepStatus = "pending"
	StepStatusRunning      StepStatus = "running"
	StepStatusSucceeded    StepStatus = "succeeded"
	StepStatusFailed       StepStatus = "failed"
	StepStatusCompensating StepStatus = "compensating"
	StepStatusCompensated  StepStatus = "compensated"
)

// OutboxStatus is the publication lifecycle of an outbox event.
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "pending"
	OutboxStatusPublished OutboxStatus = "published"
)

// Transaction is the top-level transaction row.
type Transaction struct {
	TxID       uuid.UUID
	UserID     string
	QuoteID    string
	Amount     string
	Asset      string
	Rail       string
	DestAddr   string
	Status     TxStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Version    int64
}

// Step is one row in transaction_steps.
type Step struct {
	TxID          uuid.UUID
	StepName      string
	Status        StepStatus
	Attempt       int
	StartedAt     *time.Time
	FinishedAt    *time.Time
	Error         string
	IdempotencyKey string
}

// SagaState is the durable workflow snapshot.
type SagaState struct {
	TxID           uuid.UUID
	CurrentStep    string
	State          string
	LeaseOwner     string
	LeaseExpiresAt *time.Time
	Payload        []byte
	Version        int64
}

// OutboxEvent is one row in outbox_events.
type OutboxEvent struct {
	EventID     uuid.UUID
	TxID        uuid.UUID
	EventType   string
	Step        string
	Attempt     int
	Payload     []byte
	CreatedAt   time.Time
	PublishedAt *time.Time
	Status      OutboxStatus
	DedupKey    string
}

// Tx is a database transaction scoped to the store's connection pool.
type Tx interface {
	Executor
	Commit(context.Context) error
	Rollback(context.Context) error
}

// Executor is the common subset of pgxpool.Pool / pgx.Tx used by the Store. It
// lets callers pass either a pool or an active transaction to methods that
// accept an Executor.
type Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store is the persistence interface for saga state.
type Store interface {
	// BeginTx starts a serializable DB transaction.
	BeginTx(ctx context.Context) (Tx, error)
	// Pool returns the underlying connection pool (for use outside a tx).
	Pool() *pgxpool.Pool
	// CreateTx inserts a new transactions row.
	CreateTx(ctx context.Context, ex Executor, tx *Transaction) error
	// UpdateStep upserts a transaction_steps row by (tx_id, step_name, attempt).
	UpdateStep(ctx context.Context, ex Executor, step *Step) error
	// LoadSagaState loads the saga_state row for txID.
	LoadSagaState(ctx context.Context, ex Executor, txID uuid.UUID) (*SagaState, error)
	// SaveSagaState updates the saga_state row, optimistically locking on Version.
	SaveSagaState(ctx context.Context, ex Executor, state *SagaState) error
	// AppendOutbox inserts an outbox_events row.
	AppendOutbox(ctx context.Context, ex Executor, event *OutboxEvent) error
	// Close releases the underlying pool.
	Close(ctx context.Context) error
}

// pgStore is the pgxpool-backed implementation of Store.
type pgStore struct {
	pool *pgxpool.Pool
}

// New constructs a Store backed by the given pgxpool.Pool. The pool must be
// constructed by the caller (see Connect).
func New(pool *pgxpool.Pool) Store {
	return &pgStore{pool: pool}
}

// Connect constructs a pgxpool.Pool from dsn and returns a Store over it.
func Connect(ctx context.Context, dsn string) (Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("construct pgx pool: %w", err)
	}
	return &pgStore{pool: pool}, nil
}

func (s *pgStore) Pool() *pgxpool.Pool { return s.pool }

func (s *pgStore) Close(ctx context.Context) error {
	s.pool.Close()
	return nil
}

func (s *pgStore) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return &pgxTx{tx: tx}, nil
}

type pgxTx struct {
	tx pgx.Tx
}

func (t *pgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, args...)
}
func (t *pgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}
func (t *pgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}
func (t *pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

func (s *pgStore) CreateTx(ctx context.Context, ex Executor, tx *Transaction) error {
	if ex == nil {
		ex = s.pool
	}
	const q = `INSERT INTO transactions (tx_id, user_id, quote_id, amount, asset, rail, dest_address, status, created_at, updated_at, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err := ex.Exec(ctx, q,
		tx.TxID, tx.UserID, tx.QuoteID, tx.Amount, tx.Asset, tx.Rail,
		tx.DestAddr, tx.Status, tx.CreatedAt, tx.UpdatedAt, tx.Version,
	)
	if err != nil {
		return fmt.Errorf("create tx: %w", err)
	}
	return nil
}

func (s *pgStore) UpdateStep(ctx context.Context, ex Executor, step *Step) error {
	if ex == nil {
		ex = s.pool
	}
	const q = `INSERT INTO transaction_steps (tx_id, step_name, status, attempt, started_at, finished_at, error, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tx_id, step_name, attempt) DO UPDATE SET
			status = EXCLUDED.status,
			started_at = EXCLUDED.started_at,
			finished_at = EXCLUDED.finished_at,
			error = EXCLUDED.error,
			idempotency_key = EXCLUDED.idempotency_key`
	_, err := ex.Exec(ctx, q,
		step.TxID, step.StepName, step.Status, step.Attempt,
		step.StartedAt, step.FinishedAt, step.Error, step.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("update step: %w", err)
	}
	return nil
}

func (s *pgStore) LoadSagaState(ctx context.Context, ex Executor, txID uuid.UUID) (*SagaState, error) {
	if ex == nil {
		ex = s.pool
	}
	const q = `SELECT tx_id, current_step, state, lease_owner, lease_expires_at, payload, version
		FROM saga_state WHERE tx_id = $1`
	row := ex.QueryRow(ctx, q, txID)
	var st SagaState
	var leaseOwner *string
	err := row.Scan(&st.TxID, &st.CurrentStep, &st.State, &leaseOwner, &st.LeaseExpiresAt, &st.Payload, &st.Version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("load saga state: %w", err)
	}
	if leaseOwner != nil {
		st.LeaseOwner = *leaseOwner
	}
	return &st, nil
}

func (s *pgStore) SaveSagaState(ctx context.Context, ex Executor, state *SagaState) error {
	if ex == nil {
		ex = s.pool
	}
	const q = `UPDATE saga_state SET
		current_step = $1, state = $2, lease_owner = $3, lease_expires_at = $4,
		payload = $5, version = version + 1, updated_at = now()
		WHERE tx_id = $6 AND version = $7`
	tag, err := ex.Exec(ctx, q,
		state.CurrentStep, state.State, nullableString(state.LeaseOwner), state.LeaseExpiresAt,
		state.Payload, state.TxID, state.Version,
	)
	if err != nil {
		return fmt.Errorf("save saga state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	state.Version++
	return nil
}

func (s *pgStore) AppendOutbox(ctx context.Context, ex Executor, event *OutboxEvent) error {
	if ex == nil {
		ex = s.pool
	}
	const q = `INSERT INTO outbox_events (event_id, tx_id, event_type, step, attempt, payload, created_at, published_at, status, dedup_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err := ex.Exec(ctx, q,
		event.EventID, event.TxID, event.EventType, event.Step, event.Attempt,
		event.Payload, event.CreatedAt, event.PublishedAt, event.Status, event.DedupKey,
	)
	if err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}
	return nil
}

// ErrNotFound is returned when a single-row load matches no rows.
var ErrNotFound = errors.New("store: not found")

// ErrVersionConflict is returned by SaveSagaState when the optimistic-version
// guard rejects the update (concurrent modification).
var ErrVersionConflict = errors.New("store: version conflict")

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}