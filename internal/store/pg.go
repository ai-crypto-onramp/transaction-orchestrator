package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/migrations"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore is the Postgres-backed Store implementation, backed by a pgxpool.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore opens a pool against dsn, applies the embedded migrations, and
// returns a ready PgStore.
func NewPgStore(ctx context.Context, dsn string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := migrations.Up(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrations.Up: %w", err)
	}
	return &PgStore{pool: pool}, nil
}

// Close releases the underlying pool.
func (s *PgStore) Close() { s.pool.Close() }

type pgTx struct {
	tx pgx.Tx
}

func (t *pgTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

func (s *PgStore) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: tx}, nil
}

func (s *PgStore) Within(tx Tx) TxStore {
	return &pgTxStore{tx: tx.(*pgTx).tx}
}

func (s *PgStore) RunInTx(ctx context.Context, fn func(TxStore) error) error {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return err
	}
	if err := fn(s.Within(tx)); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// Read-only helpers (executed outside an explicit Tx; pgxpool auto-transaction).

func (s *PgStore) LoadTx(ctx context.Context, txID string) (Transaction, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT tx_id, user_id, quote_id, amount, asset, rail, dest_address, status, created_at, updated_at, version
		FROM transactions WHERE tx_id = $1`, txID)
	var t Transaction
	err := row.Scan(&t.TxID, &t.UserID, &t.QuoteID, &t.Amount, &t.Asset, &t.Rail, &t.DestAddress,
		&t.Status, &t.CreatedAt, &t.UpdatedAt, &t.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transaction{}, ErrNotFound
	}
	return t, err
}

func (s *PgStore) LoadSagaState(ctx context.Context, txID string) (SagaState, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT tx_id, current_step, state, lease_owner, lease_expires_at, payload, version
		FROM saga_state WHERE tx_id = $1`, txID)
	return scanSaga(row.Scan)
}

func (s *PgStore) ListSteps(ctx context.Context, txID string) ([]StepRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tx_id, step_name, status, attempt, started_at, finished_at, error, idempotency_key
		FROM transaction_steps WHERE tx_id = $1
		ORDER BY attempt, step_name`, txID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StepRow{}
	for rows.Next() {
		var r StepRow
		if err := rows.Scan(&r.TxID, &r.StepName, &r.Status, &r.Attempt, &r.StartedAt, &r.FinishedAt, &r.Error, &r.IdempotencyKey); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PgStore) ListOutboxPending(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, tx_id, event_type, step, attempt, payload, created_at, published_at, status, dedup_key
		FROM outbox_events WHERE status = 'pending'
		ORDER BY created_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []OutboxEvent{}
	for rows.Next() {
		var e OutboxEvent
		if err := scanEvent(rows.Scan, &e); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *PgStore) ListInflightSagaIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tx_id FROM saga_state
		WHERE state NOT IN ('completed','failed_compensated','failed')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

type pgTxStore struct {
	tx pgx.Tx
}

func (m *pgTxStore) CreateTx(ctx context.Context, t Transaction, steps []StepRow, saga SagaState, events []OutboxEvent) error {
	if _, err := m.tx.Exec(ctx, `
		INSERT INTO transactions (tx_id, user_id, quote_id, amount, asset, rail, dest_address, status, created_at, updated_at, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		t.TxID, t.UserID, t.QuoteID, t.Amount, t.Asset, t.Rail, t.DestAddress, t.Status,
		t.CreatedAt, t.UpdatedAt, t.Version); err != nil {
		return mapPgErr(err)
	}
	for _, r := range steps {
		if _, err := m.tx.Exec(ctx, `
			INSERT INTO transaction_steps (tx_id, step_name, status, attempt, started_at, finished_at, error, idempotency_key)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			r.TxID, r.StepName, r.Status, r.Attempt, r.StartedAt, r.FinishedAt, r.Error, r.IdempotencyKey); err != nil {
			return mapPgErr(err)
		}
	}
	if _, err := m.tx.Exec(ctx, `
		INSERT INTO saga_state (tx_id, current_step, state, lease_owner, lease_expires_at, payload, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		saga.TxID, saga.CurrentStep, saga.State, saga.LeaseOwner, saga.LeaseExpiresAt, EncodeJSON(saga.Payload), saga.Version); err != nil {
		return mapPgErr(err)
	}
	if err := m.AppendOutbox(ctx, events); err != nil {
		return err
	}
	return nil
}

func (m *pgTxStore) InsertStep(ctx context.Context, row StepRow) error {
	_, err := m.tx.Exec(ctx, `
		INSERT INTO transaction_steps (tx_id, step_name, status, attempt, started_at, finished_at, error, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		row.TxID, row.StepName, row.Status, row.Attempt, row.StartedAt, row.FinishedAt, row.Error, row.IdempotencyKey)
	return mapPgErr(err)
}

func (m *pgTxStore) UpdateStep(ctx context.Context, row StepRow) error {
	ct, err := m.tx.Exec(ctx, `
		UPDATE transaction_steps
		SET status=$2, started_at=$3, finished_at=$4, error=$5
		WHERE tx_id=$1 AND step_name=$6 AND attempt=$7`,
		row.TxID, row.Status, row.StartedAt, row.FinishedAt, row.Error, row.StepName, row.Attempt)
	if err != nil {
		return mapPgErr(err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (m *pgTxStore) LoadStep(ctx context.Context, txID string, step statemachine.Step, attempt int) (StepRow, error) {
	row := m.tx.QueryRow(ctx, `
		SELECT tx_id, step_name, status, attempt, started_at, finished_at, error, idempotency_key
		FROM transaction_steps WHERE tx_id=$1 AND step_name=$2 AND attempt=$3`, txID, step, attempt)
	var r StepRow
	err := row.Scan(&r.TxID, &r.StepName, &r.Status, &r.Attempt, &r.StartedAt, &r.FinishedAt, &r.Error, &r.IdempotencyKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return StepRow{}, ErrNotFound
	}
	return r, err
}

func (m *pgTxStore) UpdateTransactionStatus(ctx context.Context, txID string, status statemachine.State, version int64) error {
	ct, err := m.tx.Exec(ctx, `
		UPDATE transactions SET status=$2, version=version+1, updated_at=NOW()
		WHERE tx_id=$1 AND version=$3`, txID, status, version)
	if err != nil {
		return mapPgErr(err)
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

func (m *pgTxStore) SaveSagaState(ctx context.Context, saga SagaState) error {
	ct, err := m.tx.Exec(ctx, `
		UPDATE saga_state
		SET current_step=$2, state=$3, lease_owner=$4, lease_expires_at=$5, payload=$6, version=version+1
		WHERE tx_id=$1 AND version=$7`,
		saga.TxID, saga.CurrentStep, saga.State, saga.LeaseOwner, saga.LeaseExpiresAt, EncodeJSON(saga.Payload), saga.Version-1)
	if err != nil {
		return mapPgErr(err)
	}
	if ct.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

func (m *pgTxStore) LoadSagaState(ctx context.Context, txID string) (SagaState, error) {
	row := m.tx.QueryRow(ctx, `
		SELECT tx_id, current_step, state, lease_owner, lease_expires_at, payload, version
		FROM saga_state WHERE tx_id=$1`, txID)
	return scanSaga(row.Scan)
}

func (m *pgTxStore) AppendOutbox(ctx context.Context, events []OutboxEvent) error {
	for _, e := range events {
		if e.EventID == "" {
			e.EventID = NewEventID()
		}
		if _, err := m.tx.Exec(ctx, `
			INSERT INTO outbox_events (event_id, tx_id, event_type, step, attempt, payload, created_at, status, dedup_key)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			e.EventID, e.TxID, e.EventType, e.Step, e.Attempt, EncodeJSON(e.Payload), e.CreatedAt, e.Status, e.DedupKey); err != nil {
			return mapPgErr(err)
		}
	}
	return nil
}

func (m *pgTxStore) MarkOutboxPublished(ctx context.Context, eventIDs []string, at time.Time) error {
	for _, id := range eventIDs {
		if _, err := m.tx.Exec(ctx, `
			UPDATE outbox_events SET status='published', published_at=$2 WHERE event_id=$1`, id, at); err != nil {
			return mapPgErr(err)
		}
	}
	return nil
}

// --- helpers -----------------------------------------------------------------

func scanSaga(scan func(...any) error) (SagaState, error) {
	var s SagaState
	var leaseOwner *string
	var leaseExpiry *time.Time
	var payload []byte
	err := scan(&s.TxID, &s.CurrentStep, &s.State, &leaseOwner, &leaseExpiry, &payload, &s.Version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SagaState{}, ErrNotFound
		}
		return SagaState{}, err
	}
	if leaseOwner != nil {
		s.LeaseOwner = *leaseOwner
	}
	s.LeaseExpiresAt = leaseExpiry
	s.Payload = decodePayload(payload)
	return s, nil
}

func scanEvent(scan func(...any) error, e *OutboxEvent) error {
	var publishedAt *time.Time
	var payload []byte
	if err := scan(&e.EventID, &e.TxID, &e.EventType, &e.Step, &e.Attempt, &payload, &e.CreatedAt, &publishedAt, &e.Status, &e.DedupKey); err != nil {
		return err
	}
	e.PublishedAt = publishedAt
	e.Payload = decodePayload(payload)
	return nil
}

func decodePayload(b []byte) map[string]any {
	if len(b) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

func mapPgErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return ErrDuplicate
		case "40001", "40P01": // serialization_failure, deadlock_detected
			return ErrConflict
		}
	}
	return err
}