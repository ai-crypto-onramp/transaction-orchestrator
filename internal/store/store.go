// Package store is the persistence boundary for the orchestrator.
//
// It defines the typed Store interface used by the API and the saga worker,
// plus an in-memory implementation suitable for unit testing and a Postgres
// implementation backed by pgxpool.  All multi-row mutations performed by the
// API / worker occur inside a single Tx, satisfying the outbox atomicity
// requirement.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/google/uuid"
)

// StepStatus is the lifecycle state of a single transaction_steps row.
type StepStatus string

const (
	StepPending      StepStatus = "pending"
	StepRunning      StepStatus = "running"
	StepSucceeded    StepStatus = "succeeded"
	StepFailed       StepStatus = "failed"
	StepCompensating StepStatus = "compensating"
	StepCompensated  StepStatus = "compensated"
)

// OutboxStatus is the lifecycle state of an outbox_events row.
type OutboxStatus string

const (
	OutboxPending   OutboxStatus = "pending"
	OutboxInflight  OutboxStatus = "inflight"
	OutboxPublished OutboxStatus = "published"
)

// Transaction is the top-level tx record.
type Transaction struct {
	TxID        string
	UserID      string
	QuoteID     string
	Amount      string
	Asset       string
	Rail        string
	DestAddress string
	Status      statemachine.State
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Version     int64
}

// StepRow is one row of transaction_steps.
type StepRow struct {
	TxID           string
	StepName       statemachine.Step
	Status         StepStatus
	Attempt        int
	StartedAt      *time.Time
	FinishedAt     *time.Time
	Error          string
	IdempotencyKey string
}

// SagaState is the durable workflow snapshot.
type SagaState struct {
	TxID            string
	CurrentStep     statemachine.Step
	State           statemachine.State
	LeaseOwner      string
	LeaseExpiresAt  *time.Time
	Payload         map[string]any
	Version         int64
}

// OutboxEvent is one row of outbox_events.
type OutboxEvent struct {
	EventID     string
	TxID        string
	EventType   string
	Step        string
	Attempt     int
	Payload     map[string]any
	CreatedAt   time.Time
	PublishedAt *time.Time
	Status      OutboxStatus
	DedupKey    string
}

// Tx is the unit-of-work handle returned by BeginTx.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Store is the persistence boundary.  Methods that mutate multiple rows do so
// atomically inside the provided Tx.
type Store interface {
	// BeginTx starts a serializable unit of work.
	BeginTx(ctx context.Context) (Tx, error)
	// Within returns the Tx-bound view used by mutation methods.
	Within(tx Tx) TxStore
	// RunInTx runs fn inside a fresh transaction, committing on nil and
	// rolling back on error.  Panics are recovered into an error.
	RunInTx(ctx context.Context, fn func(TxStore) error) error

	// Read-only methods (can be called outside a Tx):
	LoadTx(ctx context.Context, txID string) (Transaction, error)
	LoadSagaState(ctx context.Context, txID string) (SagaState, error)
	ListSteps(ctx context.Context, txID string) ([]StepRow, error)
	ListOutboxPending(ctx context.Context, limit int) ([]OutboxEvent, error)
	ListInflightSagaIDs(ctx context.Context) ([]string, error)

	// Maintenance:
	Close()
}

// TxStore is the Tx-bound mutation surface.
type TxStore interface {
	CreateTx(ctx context.Context, t Transaction, steps []StepRow, saga SagaState, events []OutboxEvent) error
	UpdateStep(ctx context.Context, row StepRow) error
	InsertStep(ctx context.Context, row StepRow) error
	UpdateTransactionStatus(ctx context.Context, txID string, status statemachine.State, version int64) error
	SaveSagaState(ctx context.Context, saga SagaState) error
	LoadSagaState(ctx context.Context, txID string) (SagaState, error)
	LoadStep(ctx context.Context, txID string, step statemachine.Step, attempt int) (StepRow, error)
	AppendOutbox(ctx context.Context, events []OutboxEvent) error
	MarkOutboxPublished(ctx context.Context, eventIDs []string, at time.Time) error
	// ClaimOutboxPending selects up to limit pending outbox events and locks
	// them for update (SKIP LOCKED on Postgres) so concurrent relays do not
	// double-publish.  Must be called inside a Tx.
	ClaimOutboxPending(ctx context.Context, limit int) ([]OutboxEvent, error)
}

// ErrNotFound is returned by read methods when the row does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrConflict is returned by optimistic-concurrency updates when the version
// does not match.
var ErrConflict = errors.New("store: version conflict")

// ErrDuplicate is returned when an idempotency-key or dedup-key insert
// collides with an existing row.
var ErrDuplicate = errors.New("store: duplicate")

// IdempotencyKey returns the canonical idempotency key for a step attempt.
func IdempotencyKey(txID string, step statemachine.Step, attempt int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d", txID, step, attempt)))
	return hex.EncodeToString(h[:])
}

// DedupKey returns the canonical outbox dedup key.
func DedupKey(txID, eventType, step string, attempt int) string {
	return fmt.Sprintf("%s|%s|%s|%d", txID, eventType, step, attempt)
}

// EncodeJSON is a small helper used by both implementations.
func EncodeJSON(v map[string]any) []byte {
	if v == nil {
		return []byte("{}")
	}
	b, _ := json.Marshal(v)
	return b
}

// NewEventID returns a fresh event id (uuid v4 string).
func NewEventID() string { return uuid.NewString() }

// --- In-memory implementation ------------------------------------------------

// MemStore is an in-memory Store for unit tests.  It is safe for concurrent
// use.
type MemStore struct {
	mu       sync.Mutex
	txs      map[string]Transaction
	steps    map[string][]StepRow // keyed by tx_id
	sagas    map[string]SagaState
	outbox   []OutboxEvent
}

// NewMemStore builds an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		txs:   make(map[string]Transaction),
		steps: make(map[string][]StepRow),
		sagas: make(map[string]SagaState),
	}
}

func (s *MemStore) BeginTx(ctx context.Context) (Tx, error) { return &memTx{s: s}, nil }

type memTx struct {
	s   *MemStore
	mu  sync.Mutex
	dry bool
}

func (t *memTx) Commit(ctx context.Context) error   { t.mu.Lock(); t.dry = true; t.mu.Unlock(); return nil }
func (t *memTx) Rollback(ctx context.Context) error { return nil }

func (s *MemStore) Within(tx Tx) TxStore {
	return &memTxStore{s: s, tx: tx.(*memTx)}
}

func (s *MemStore) RunInTx(ctx context.Context, fn func(TxStore) error) error {
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

func (s *MemStore) LoadTx(ctx context.Context, txID string) (Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.txs[txID]
	if !ok {
		return Transaction{}, ErrNotFound
	}
	return t, nil
}

func (s *MemStore) LoadSagaState(ctx context.Context, txID string) (SagaState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sg, ok := s.sagas[txID]
	if !ok {
		return SagaState{}, ErrNotFound
	}
	return sg, nil
}

func (s *MemStore) ListSteps(ctx context.Context, txID string) ([]StepRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := append([]StepRow(nil), s.steps[txID]...)
	return rows, nil
}

func (s *MemStore) ListOutboxPending(ctx context.Context, limit int) ([]OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutboxEvent, 0, limit)
	for _, e := range s.outbox {
		if e.Status == OutboxPending {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *MemStore) ListInflightSagaIDs(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []string{}
	for id, sg := range s.sagas {
		if !sg.State.Terminal() {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *MemStore) Close() {}

type memTxStore struct {
	s  *MemStore
	tx *memTx
}

func (m *memTxStore) CreateTx(ctx context.Context, t Transaction, steps []StepRow, saga SagaState, events []OutboxEvent) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	if _, exists := m.s.txs[t.TxID]; exists {
		return ErrDuplicate
	}
	m.s.txs[t.TxID] = t
	m.s.steps[t.TxID] = append([]StepRow(nil), steps...)
	m.s.sagas[t.TxID] = saga
	m.s.outbox = append(m.s.outbox, events...)
	return nil
}

func (m *memTxStore) InsertStep(ctx context.Context, row StepRow) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	for _, r := range m.s.steps[row.TxID] {
		if r.StepName == row.StepName && r.Attempt == row.Attempt {
			return ErrDuplicate
		}
	}
	m.s.steps[row.TxID] = append(m.s.steps[row.TxID], row)
	return nil
}

func (m *memTxStore) UpdateStep(ctx context.Context, row StepRow) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	for i, r := range m.s.steps[row.TxID] {
		if r.StepName == row.StepName && r.Attempt == row.Attempt {
			m.s.steps[row.TxID][i] = row
			return nil
		}
	}
	return ErrNotFound
}

func (m *memTxStore) LoadStep(ctx context.Context, txID string, step statemachine.Step, attempt int) (StepRow, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	for _, r := range m.s.steps[txID] {
		if r.StepName == step && r.Attempt == attempt {
			return r, nil
		}
	}
	return StepRow{}, ErrNotFound
}

func (m *memTxStore) UpdateTransactionStatus(ctx context.Context, txID string, status statemachine.State, version int64) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	t, ok := m.s.txs[txID]
	if !ok {
		return ErrNotFound
	}
	if t.Version != version {
		return ErrConflict
	}
	t.Status = status
	t.Version = version + 1
	t.UpdatedAt = time.Now().UTC()
	m.s.txs[txID] = t
	return nil
}

func (m *memTxStore) SaveSagaState(ctx context.Context, saga SagaState) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	cur, ok := m.s.sagas[saga.TxID]
	if !ok {
		return ErrNotFound
	}
	if cur.Version != saga.Version-1 {
		return ErrConflict
	}
	saga.Version = cur.Version + 1
	m.s.sagas[saga.TxID] = saga
	return nil
}

func (m *memTxStore) LoadSagaState(ctx context.Context, txID string) (SagaState, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	sg, ok := m.s.sagas[txID]
	if !ok {
		return SagaState{}, ErrNotFound
	}
	return sg, nil
}

func (m *memTxStore) AppendOutbox(ctx context.Context, events []OutboxEvent) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	for _, e := range events {
		for _, ex := range m.s.outbox {
			if ex.DedupKey == e.DedupKey {
				return ErrDuplicate
			}
		}
	}
	m.s.outbox = append(m.s.outbox, events...)
	return nil
}

func (m *memTxStore) MarkOutboxPublished(ctx context.Context, eventIDs []string, at time.Time) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	for _, id := range eventIDs {
		for i, e := range m.s.outbox {
			if e.EventID == id {
				e.Status = OutboxPublished
				e.PublishedAt = &at
				m.s.outbox[i] = e
				break
			}
		}
	}
	return nil
}

func (m *memTxStore) ClaimOutboxPending(ctx context.Context, limit int) ([]OutboxEvent, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	out := make([]OutboxEvent, 0, limit)
	for i, e := range m.s.outbox {
		if e.Status == OutboxPending {
			e.Status = OutboxInflight
			m.s.outbox[i] = e
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}