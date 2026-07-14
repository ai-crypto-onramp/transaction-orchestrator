package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// skipIfNoDocker skips t if Docker is unavailable (testcontainers auto-detects
// via the docker socket).  On CI runners Docker is present.
func skipIfNoDocker(t *testing.T, ctx context.Context) {
	t.Helper()
	// Cheap probe: try to create a throwaway container; if it fails we skip.
	// We do the probe once per package via a shared sentinel.
	if testing.Short() {
		t.Skip("skipping pg test in -short mode")
	}
}

func newPgStoreForTest(t *testing.T) *PgStore {
	t.Helper()
	ctx := context.Background()
	skipIfNoDocker(t, ctx)
	pgC, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("to"), tcpostgres.WithUsername("u"), tcpostgres.WithPassword("p"))
	if err != nil {
		t.Skipf("postgres container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(context.Background()) })
	dsn, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("conn string: %v", err)
	}
	ps, err := NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPgStore: %v", err)
	}
	t.Cleanup(ps.Close)
	return ps
}

func TestPgCreateTxAndReads(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	now := time.Now().UTC()
	tx := Transaction{
		TxID: "pg-1", UserID: "u1", QuoteID: "q1", Amount: "100", Asset: "BTC",
		Rail: "card", DestAddress: "0xabc", Status: statemachine.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	var steps []StepRow
	for _, st := range statemachine.StepOrder {
		steps = append(steps, StepRow{
			TxID: "pg-1", StepName: st, Status: StepPending, Attempt: 1,
			IdempotencyKey: IdempotencyKey("pg-1", st, 1),
		})
	}
	sg := SagaState{
		TxID: "pg-1", CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Version: 1, Payload: map[string]any{},
	}
	evts := []OutboxEvent{{
		EventID: NewEventID(), TxID: "pg-1", EventType: "transaction.created",
		Status: OutboxPending, DedupKey: DedupKey("pg-1", "transaction.created", "", 0),
		CreatedAt: now,
	}}
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.CreateTx(ctx, tx, steps, sg, evts)
	}); err != nil {
		t.Fatalf("CreateTx: %v", err)
	}

	got, err := ps.LoadTx(ctx, "pg-1")
	if err != nil {
		t.Fatalf("LoadTx: %v", err)
	}
	if got.Status != statemachine.StateCreated || got.Version != 1 {
		t.Fatalf("unexpected tx: %+v", got)
	}
	if _, err := ps.LoadTx(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	sg2, err := ps.LoadSagaState(ctx, "pg-1")
	if err != nil {
		t.Fatalf("LoadSagaState: %v", err)
	}
	if sg2.State != statemachine.StateCreated {
		t.Fatalf("unexpected saga: %+v", sg2)
	}

	rows, err := ps.ListSteps(ctx, "pg-1")
	if err != nil {
		t.Fatalf("ListSteps: %v", err)
	}
	if len(rows) != 6 {
		t.Fatalf("expected 6 steps, got %d", len(rows))
	}

	pending, err := ps.ListOutboxPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListOutboxPending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}

	inflight, err := ps.ListInflightSagaIDs(ctx)
	if err != nil {
		t.Fatalf("ListInflightSagaIDs: %v", err)
	}
	if len(inflight) != 1 {
		t.Fatalf("expected 1 inflight, got %d", len(inflight))
	}
}

func TestPgUpdateStepAndSaga(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-2")
	now := time.Now().UTC()
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.UpdateStep(ctx, StepRow{
			TxID: "pg-2", StepName: statemachine.StepPolicy, Status: StepSucceeded, Attempt: 1,
			StartedAt: &now, FinishedAt: &now,
		})
	}); err != nil {
		t.Fatalf("UpdateStep: %v", err)
	}
	// InsertStep duplicate should fail.
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.InsertStep(ctx, StepRow{
			TxID: "pg-2", StepName: statemachine.StepPolicy, Status: StepPending, Attempt: 1,
			IdempotencyKey: "k",
		})
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
	// LoadStep.
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		_, err := ts.LoadStep(ctx, "pg-2", statemachine.StepPolicy, 1)
		return err
	}); err != nil {
		t.Fatalf("LoadStep: %v", err)
	}
	// SaveSagaState with correct version.
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		sg, err := ts.LoadSagaState(ctx, "pg-2")
		if err != nil {
			return err
		}
		sg.State = statemachine.StatePolicyChecked
		sg.Version = sg.Version + 1
		return ts.SaveSagaState(ctx, sg)
	}); err != nil {
		t.Fatalf("SaveSagaState: %v", err)
	}
	// UpdateTransactionStatus with wrong version should conflict.
	err = ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.UpdateTransactionStatus(ctx, "pg-2", statemachine.StatePolicyChecked, 999)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestPgClaimAndPublish(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-3")
	// Claim pending events.
	var claimed []OutboxEvent
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		var err error
		claimed, err = ts.ClaimOutboxPending(ctx, 10)
		return err
	}); err != nil {
		t.Fatalf("ClaimOutboxPending: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed, got %d", len(claimed))
	}
	// Mark published.
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.MarkOutboxPublished(ctx, []string{claimed[0].EventID}, time.Now().UTC())
	}); err != nil {
		t.Fatalf("MarkOutboxPublished: %v", err)
	}
	pending, _ := ps.ListOutboxPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after publish, got %d", len(pending))
	}
}

func TestPgAppendOutboxDuplicate(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-4")
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.AppendOutbox(ctx, []OutboxEvent{{
			EventID: NewEventID(), TxID: "pg-4", EventType: "transaction.created",
			Status: OutboxPending, DedupKey: DedupKey("pg-4", "transaction.created", "", 0),
			CreatedAt: time.Now().UTC(),
		}})
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestPgCreateDuplicateTx(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-5")
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.CreateTx(ctx, Transaction{TxID: "pg-5", Status: statemachine.StateCreated, Version: 1},
			nil, SagaState{TxID: "pg-5", Version: 1}, nil)
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestPgSaveSagaStateConflict(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-6")
	// First save with correct version.
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		sg, err := ts.LoadSagaState(ctx, "pg-6")
		if err != nil {
			return err
		}
		sg.State = statemachine.StatePolicyChecked
		sg.Version = sg.Version + 1
		return ts.SaveSagaState(ctx, sg)
	}); err != nil {
		t.Fatalf("SaveSagaState: %v", err)
	}
	// Re-save with the stale version: conflict.
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "pg-6")
		sg.State = statemachine.StatePaymentCaptured
		// Keep the old version so the update targets the prior row.
		return ts.SaveSagaState(ctx, sg)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestPgUpdateStepNotFound(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-7")
	now := time.Now().UTC()
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.UpdateStep(ctx, StepRow{
			TxID: "pg-7", StepName: statemachine.Step("nonexistent"), Status: StepSucceeded, Attempt: 99,
			StartedAt: &now, FinishedAt: &now,
		})
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPgLoadStepNotFound(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-8")
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		_, err := ts.LoadStep(ctx, "pg-8", statemachine.StepPolicy, 99)
		return err
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPgListOutboxPendingEmpty(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-9")
	// Drain the single pending event.
	_ = ps.RunInTx(ctx, func(ts TxStore) error {
		claimed, _ := ts.ClaimOutboxPending(ctx, 10)
		ids := make([]string, 0, len(claimed))
		for _, e := range claimed {
			ids = append(ids, e.EventID)
		}
		return ts.MarkOutboxPublished(ctx, ids, time.Now().UTC())
	})
	pending, err := ps.ListOutboxPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListOutboxPending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending))
	}
}

func TestPgListInflightAfterTerminal(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-10")
	// Move saga to terminal.
	_ = ps.RunInTx(ctx, func(ts TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "pg-10")
		sg.State = statemachine.StateCompleted
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(ctx, sg)
		tx, _ := ts.LoadSagaState(ctx, "pg-10") // touch to keep in tx
		_ = ts.UpdateTransactionStatus(ctx, "pg-10", statemachine.StateCompleted, 1)
		_ = tx
		return nil
	})
	inflight, err := ps.ListInflightSagaIDs(ctx)
	if err != nil {
		t.Fatalf("ListInflightSagaIDs: %v", err)
	}
	for _, id := range inflight {
		if id == "pg-10" {
			t.Fatal("expected pg-10 to not be inflight after terminal")
		}
	}
}

// TestPgBeginTxWithinAndRollback exercises the explicit BeginTx/Within/Rollback
// path on the pg-backed store.
func TestPgBeginTxWithinAndRollback(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-11")
	tx, err := ps.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	ts := ps.Within(tx)
	// LoadSagaState inside the tx.
	if _, err := ts.LoadSagaState(ctx, "pg-11"); err != nil {
		t.Fatalf("LoadSagaState in tx: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	// Commit after rollback should error (already closed).
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("expected commit-after-rollback to error")
	}
}

// TestPgRunInTxRollbackOnError verifies RunInTx rolls back on error.
func TestPgRunInTxRollbackOnError(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-12")
	before, _ := ps.LoadTx(ctx, "pg-12")
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		// Insert a new step attempt row inside the tx.
		now := time.Now().UTC()
		if err := ts.InsertStep(ctx, StepRow{
			TxID: "pg-12", StepName: statemachine.StepPolicy, Status: StepSucceeded, Attempt: 2,
			StartedAt: &now, IdempotencyKey: IdempotencyKey("pg-12", statemachine.StepPolicy, 2),
		}); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	if err == nil || err.Error() != "force rollback" {
		t.Fatalf("unexpected err: %v", err)
	}
	// The new attempt row should not have been committed.
	steps, _ := ps.ListSteps(ctx, "pg-12")
	for _, r := range steps {
		if r.Attempt == 2 {
			t.Fatal("expected attempt 2 to have been rolled back")
		}
	}
	after, _ := ps.LoadTx(ctx, "pg-12")
	if after.Version != before.Version {
		t.Fatalf("tx version changed after rollback: %d -> %d", before.Version, after.Version)
	}
}

// TestPgInsertStepAndLoadStep exercises the InsertStep happy path plus the
// LoadStep not-found path on the pg-backed store.
func TestPgInsertStepAndLoadStep(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-13")
	now := time.Now().UTC()
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.InsertStep(ctx, StepRow{
			TxID: "pg-13", StepName: statemachine.StepPayment, Status: StepRunning, Attempt: 2,
			StartedAt: &now, IdempotencyKey: IdempotencyKey("pg-13", statemachine.StepPayment, 2),
		})
	}); err != nil {
		t.Fatalf("InsertStep: %v", err)
	}
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		row, err := ts.LoadStep(ctx, "pg-13", statemachine.StepPayment, 2)
		if err != nil {
			return err
		}
		if row.Status != StepRunning {
			t.Fatalf("expected running, got %s", row.Status)
		}
		return nil
	}); err != nil {
		t.Fatalf("LoadStep: %v", err)
	}
}

// TestPgClaimEmptyAndMarkEmpty exercises the empty-batch paths for
// ClaimOutboxPending and MarkOutboxPublished.
func TestPgClaimEmptyAndMarkEmpty(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	seedPgTx(t, ps, "pg-14")
	// Claim when there is exactly one pending; drains it.
	var claimed []OutboxEvent
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		var err error
		claimed, err = ts.ClaimOutboxPending(ctx, 10)
		return err
	}); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed, got %d", len(claimed))
	}
	// Claim again -> empty.
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		var err error
		claimed, err = ts.ClaimOutboxPending(ctx, 10)
		return err
	}); err != nil {
		t.Fatalf("Claim2: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected 0 claimed, got %d", len(claimed))
	}
	// MarkOutboxPublished with empty list is a no-op.
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.MarkOutboxPublished(ctx, nil, time.Now().UTC())
	}); err != nil {
		t.Fatalf("MarkOutboxPublished empty: %v", err)
	}
}

// TestPgUpdateTransactionStatusNotFound verifies UpdateTransactionStatus on a
// missing tx returns ErrConflict (0 rows affected).
func TestPgUpdateTransactionStatusMissing(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.UpdateTransactionStatus(ctx, "nope", statemachine.StateCreated, 1)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

// TestPgSaveSagaStateNotFound verifies SaveSagaState on a missing saga returns
// ErrConflict (0 rows affected).
func TestPgSaveSagaStateMissing(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.SaveSagaState(ctx, SagaState{TxID: "nope", Version: 1})
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

// TestPgLoadSagaStateMissing verifies the read-only LoadSagaState returns
// ErrNotFound for a missing saga.
func TestPgLoadSagaStateMissing(t *testing.T) {
	ps := newPgStoreForTest(t)
	ctx := context.Background()
	_, err := ps.LoadSagaState(ctx, "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func seedPgTx(t *testing.T, ps *PgStore, txID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	tx := Transaction{
		TxID: txID, UserID: "u1", QuoteID: "q1", Amount: "100", Asset: "BTC",
		Rail: "card", DestAddress: "0xabc", Status: statemachine.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	var steps []StepRow
	for _, st := range statemachine.StepOrder {
		steps = append(steps, StepRow{
			TxID: txID, StepName: st, Status: StepPending, Attempt: 1,
			IdempotencyKey: IdempotencyKey(txID, st, 1),
		})
	}
	sg := SagaState{
		TxID: txID, CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Version: 1, Payload: map[string]any{},
	}
	evts := []OutboxEvent{{
		EventID: NewEventID(), TxID: txID, EventType: "transaction.created",
		Status: OutboxPending, DedupKey: DedupKey(txID, "transaction.created", "", 0),
		CreatedAt: now,
	}}
	if err := ps.RunInTx(ctx, func(ts TxStore) error {
		return ts.CreateTx(ctx, tx, steps, sg, evts)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}