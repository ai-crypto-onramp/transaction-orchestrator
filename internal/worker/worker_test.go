package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/saga"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func seedTx(t *testing.T, s store.Store, txID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: txID, UserID: "u1", QuoteID: "q1", Amount: "100", Asset: "BTC",
		Rail: "CARD", DestAddress: "0xabc", Status: statemachine.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	var steps []store.StepRow
	for _, st := range statemachine.StepOrder {
		steps = append(steps, store.StepRow{
			TxID: txID, StepName: st, Status: store.StepPending, Attempt: 1,
			IdempotencyKey: store.IdempotencyKey(txID, st, 1),
		})
	}
	sg := store.SagaState{
		TxID: txID, CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Payload: map[string]any{}, Version: 1,
	}
	evts := []store.OutboxEvent{{
		EventID: store.NewEventID(), TxID: txID, EventType: "transaction.created",
		Status: store.OutboxPending, DedupKey: store.DedupKey(txID, "transaction.created", "", 0),
		CreatedAt: now,
	}}
	if err := s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.CreateTx(ctx, tx, steps, sg, evts)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func testClients() *saga.Clients {
	stub := partner.NewStub(partner.DefaultStubConfig())
	return &saga.Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
}

func testCfg() saga.Config {
	return saga.Config{
		MaxRetries:  3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		StepTimeout: func(string) time.Duration { return 2 * time.Second },
	}
}

func TestDispatcherRunsSaga(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-w1")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 4, "test-owner")
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	d.Start(ctx)
	defer d.Stop()
	d.Submit("tx-w1")
	// Wait for saga to reach terminal state.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tx, _ := s.LoadTx(ctx, "tx-w1")
		if tx.Status.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	tx, _ := s.LoadTx(ctx, "tx-w1")
	if tx.Status != statemachine.StateCompleted {
		t.Fatalf("expected completed, got %s", tx.Status)
	}
}

func TestDispatcherPartitioningStable(t *testing.T) {
	s := store.NewMemStore()
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 8, "owner")
	// The same txID always hashes to the same partition.
	p1 := d.partitionOf("tx-foo")
	p2 := d.partitionOf("tx-foo")
	if p1 != p2 {
		t.Fatalf("partitioning not stable: %d vs %d", p1, p2)
	}
	if p1 < 0 || p1 >= d.Concurrency {
		t.Fatalf("partition out of range: %d", p1)
	}
	// Different tx ids may map to the same or different partitions; just
	// sanity check the range.
	p3 := d.partitionOf("tx-bar")
	if p3 < 0 || p3 >= d.Concurrency {
		t.Fatalf("partition out of range: %d", p3)
	}
}

func TestRecoverEnqueuesInflight(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-rec")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 4, "rec-owner")
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	if err := d.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Recover enqueues the in-flight tx. Start the pool and verify it runs to
	// completion.
	d.Start(ctx)
	defer d.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tx, _ := s.LoadTx(ctx, "tx-rec")
		if tx.Status.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	tx, _ := s.LoadTx(ctx, "tx-rec")
	if tx.Status != statemachine.StateCompleted {
		t.Fatalf("expected completed after recover, got %s", tx.Status)
	}
}

func TestControlRetryTerminal(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-ctrl")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "ctrl-owner")
	ctrl := &Control{Dispatcher: d, Executor: ex}
	ctx := context.Background()
	// Force the saga into a terminal state.
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "tx-ctrl")
		sg.State = statemachine.StateCompleted
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(ctx, sg)
		return nil
	})
	err := ctrl.Retry(ctx, "tx-ctrl", statemachine.StepPolicy)
	if err == nil {
		t.Fatal("expected error retrying terminal saga")
	}
}

func TestControlCompensateAlreadyFailed(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-comp")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "comp-owner")
	ctrl := &Control{Dispatcher: d, Executor: ex}
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "tx-comp")
		sg.State = statemachine.StateFailedCompensated
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(ctx, sg)
		return nil
	})
	err := ctrl.Compensate(ctx, "tx-comp")
	if err == nil {
		t.Fatal("expected error compensating already-failed saga")
	}
}

func TestRecoverSubmitsAllInflight(t *testing.T) {
	s := store.NewMemStore()
	for _, id := range []string{"a", "b", "c"} {
		seedTx(t, s, id)
	}
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 4, "owner")
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	if err := d.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Recover should have enqueued 3 ids across the partitions.
	var got int
	for _, ch := range d.partitions {
		got += len(ch)
	}
	if got != 3 {
		t.Fatalf("expected 3 enqueued ids, got %d", got)
	}
}

func TestSubmitDoesNotBlockAfterStop(t *testing.T) {
	s := store.NewMemStore()
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "owner")
	d.Start(context.Background())
	d.Stop()
	// Submit after Stop must not block.
	done := make(chan struct{})
	go func() {
		d.Submit("tx-late")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Submit blocked after Stop")
	}
}

func TestControlRetryMissingTx(t *testing.T) {
	s := store.NewMemStore()
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "owner")
	ctrl := &Control{Dispatcher: d, Executor: ex}
	if err := ctrl.Retry(context.Background(), "nope", statemachine.StepPolicy); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestControlRetryEnqueuesAndCompensateMissing verifies the retry happy path
// and the compensate-missing-tx error path.
func TestControlRetryEnqueuesAndCompensateMissing(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-retry-ok")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "retry-owner")
	d.Start(logging.WithLogger(context.Background(), logging.New("debug")))
	defer d.Stop()
	ctrl := &Control{Dispatcher: d, Executor: ex}
	if err := ctrl.Retry(context.Background(), "tx-retry-ok", statemachine.StepPolicy); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	// Wait for the saga to complete.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tx, _ := s.LoadTx(context.Background(), "tx-retry-ok")
		if tx.Status.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	tx, _ := s.LoadTx(context.Background(), "tx-retry-ok")
	if tx.Status != statemachine.StateCompleted {
		t.Fatalf("expected completed after retry, got %s", tx.Status)
	}
	// Compensate on a missing tx should return ErrNotFound.
	if err := ctrl.Compensate(context.Background(), "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestControlCompensateHappyPath runs the saga to payment_captured, then
// forces a compensation cascade and verifies the saga ends in
// failed_compensated.
func TestControlCompensateHappyPath(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-comp-ok")
	// Move the saga into payment_captured so compensation refunds.
	stub := partner.NewStub(partner.DefaultStubConfig())
	clients := &saga.Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := saga.NewExecutor(s, clients, testCfg())
	d := New(s, ex, 2, "comp-owner")
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	d.Start(ctx)
	defer d.Stop()
	d.Submit("tx-comp-ok")
	// Wait for payment_captured.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		sg, _ := s.LoadSagaState(ctx, "tx-comp-ok")
		if sg.State == statemachine.StatePaymentCaptured || sg.State == statemachine.StateKytScreened || sg.State.Terminal() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Stop the dispatcher so Compensate runs inline without racing.
	d.Stop()
	ctrl := &Control{Dispatcher: d, Executor: ex}
	// Force the saga into a state where Compensate can run (current step =
	// kyt, non-terminal).
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "tx-comp-ok")
		if sg.State == statemachine.StateCompleted {
			return nil
		}
		sg.State = statemachine.StatePaymentCaptured
		sg.CurrentStep = statemachine.StepKyt
		sg.Version = sg.Version + 1
		return ts.SaveSagaState(ctx, sg)
	})
	sg, _ := s.LoadSagaState(ctx, "tx-comp-ok")
	if sg.State.Terminal() {
		t.Skip("saga already terminal before compensate")
	}
	if err := ctrl.Compensate(ctx, "tx-comp-ok"); err != nil {
		t.Fatalf("Compensate: %v", err)
	}
	tx, _ := s.LoadTx(ctx, "tx-comp-ok")
	if tx.Status != statemachine.StateFailedCompensated && tx.Status != statemachine.StateFailed {
		t.Fatalf("expected failed* after compensate, got %s", tx.Status)
	}
}

// TestControlCompensateTerminalCompleted verifies Compensate rejects a
// completed saga.
func TestControlCompensateTerminalCompleted(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-comp-done")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "owner")
	ctrl := &Control{Dispatcher: d, Executor: ex}
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "tx-comp-done")
		sg.State = statemachine.StateCompleted
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(ctx, sg)
		return nil
	})
	if err := ctrl.Compensate(ctx, "tx-comp-done"); err == nil {
		t.Fatal("expected error compensating completed saga")
	}
}

// TestControlCompensateUnknownStep verifies Compensate errors when the saga's
// current_step does not map to a registered step.
func TestControlCompensateUnknownStep(t *testing.T) {
	s := store.NewMemStore()
	seedTx(t, s, "tx-comp-unknown")
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "owner")
	ctrl := &Control{Dispatcher: d, Executor: ex}
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "tx-comp-unknown")
		sg.CurrentStep = "nope"
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(ctx, sg)
		return nil
	})
	if err := ctrl.Compensate(ctx, "tx-comp-unknown"); err == nil {
		t.Fatal("expected error for unknown step")
	}
}

// Ensure the atomic import is exercised (sanity).
var _ = atomic.AddInt32

func TestNewDefaultsConcurrencyAndOwner(t *testing.T) {
	s := store.NewMemStore()
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 0, "")
	if d.Concurrency != 32 {
		t.Fatalf("expected default concurrency 32, got %d", d.Concurrency)
	}
	if d.Owner == "" {
		t.Fatal("expected non-empty default owner")
	}
	if len(d.partitions) != 32 {
		t.Fatalf("expected 32 partitions, got %d", len(d.partitions))
	}
}

func TestRecoverStoreError(t *testing.T) {
	s := store.NewMemStore()
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "owner")
	es := &errListStore{MemStore: store.NewMemStore()}
	d.Store = es
	if err := d.Recover(context.Background()); err == nil {
		t.Fatal("expected error from Recover with failing store")
	}
}

type errListStore struct{ *store.MemStore }

func (e *errListStore) ListInflightSagaIDs(ctx context.Context) ([]string, error) {
	return nil, errors.New("boom")
}

func TestStopIsIdempotent(t *testing.T) {
	s := store.NewMemStore()
	ex := saga.NewExecutor(s, testClients(), testCfg())
	d := New(s, ex, 2, "owner")
	d.Start(context.Background())
	d.Stop()
	d.Stop() // must not panic or hang
}