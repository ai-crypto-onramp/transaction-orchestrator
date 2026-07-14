package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

func seedTx(t *testing.T, s *MemStore, txID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	err := s.RunInTx(ctx, func(ts TxStore) error {
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
		saga := SagaState{
			TxID: txID, CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
			Version: 1, Payload: map[string]any{},
		}
		ev := []OutboxEvent{{
			EventID: NewEventID(), TxID: txID, EventType: "transaction.created",
			Status: OutboxPending, DedupKey: DedupKey(txID, "transaction.created", "", 0),
			CreatedAt: now,
		}}
		return ts.CreateTx(ctx, tx, steps, saga, ev)
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestMemStoreCreateTxAndReads(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	seedTx(t, s, "tx-1")

	tx, err := s.LoadTx(ctx, "tx-1")
	if err != nil {
		t.Fatalf("LoadTx: %v", err)
	}
	if tx.Status != statemachine.StateCreated || tx.Version != 1 {
		t.Fatalf("unexpected tx: %+v", tx)
	}
	if _, err := s.LoadTx(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	steps, err := s.ListSteps(ctx, "tx-1")
	if err != nil {
		t.Fatalf("ListSteps: %v", err)
	}
	if len(steps) != 6 {
		t.Fatalf("expected 6 steps, got %d", len(steps))
	}
	sg, err := s.LoadSagaState(ctx, "tx-1")
	if err != nil {
		t.Fatalf("LoadSagaState: %v", err)
	}
	if sg.State != statemachine.StateCreated || sg.CurrentStep != statemachine.StepPolicy {
		t.Fatalf("unexpected saga: %+v", sg)
	}
	pending, err := s.ListOutboxPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListOutboxPending: %v", err)
	}
	if len(pending) != 1 || pending[0].EventType != "transaction.created" {
		t.Fatalf("unexpected outbox: %+v", pending)
	}
}

func TestMemStoreCreateDuplicate(t *testing.T) {
	s := NewMemStore()
	seedTx(t, s, "dup")
	ctx := context.Background()
	err := s.RunInTx(ctx, func(ts TxStore) error {
		return ts.CreateTx(ctx, Transaction{TxID: "dup", Status: statemachine.StateCreated, Version: 1},
			nil, SagaState{TxID: "dup", Version: 1}, nil)
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestMemStoreSaveSagaStateConflict(t *testing.T) {
	s := NewMemStore()
	seedTx(t, s, "c")
	ctx := context.Background()
	sg, _ := s.LoadSagaState(ctx, "c")
	// Save once with correct version (sg.Version == 1, SaveSagaState expects new.Version == sg.Version+1)
	sg.State = statemachine.StatePolicyChecked
	sg.Version = sg.Version + 1
	if err := s.RunInTx(ctx, func(ts TxStore) error { return ts.SaveSagaState(ctx, sg) }); err != nil {
		t.Fatalf("SaveSagaState: %v", err)
	}
	// Re-save with stale version: should conflict.
	sg.State = statemachine.StatePaymentCaptured
	err := s.RunInTx(ctx, func(ts TxStore) error { return ts.SaveSagaState(ctx, sg) })
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestMemStoreUpdateStepAndInsertDuplicate(t *testing.T) {
	s := NewMemStore()
	seedTx(t, s, "u")
	ctx := context.Background()
	err := s.RunInTx(ctx, func(ts TxStore) error {
		now := time.Now().UTC()
		return ts.UpdateStep(ctx, StepRow{
			TxID: "u", StepName: statemachine.StepPolicy, Status: StepSucceeded, Attempt: 1,
			StartedAt: &now, FinishedAt: &now,
		})
	})
	if err != nil {
		t.Fatalf("UpdateStep: %v", err)
	}
	err = s.RunInTx(ctx, func(ts TxStore) error {
		return ts.InsertStep(ctx, StepRow{
			TxID: "u", StepName: statemachine.StepPolicy, Status: StepPending, Attempt: 1,
			IdempotencyKey: "k",
		})
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
}

func TestMemStoreMarkOutboxPublished(t *testing.T) {
	s := NewMemStore()
	seedTx(t, s, "p")
	ctx := context.Background()
	pending, _ := s.ListOutboxPending(ctx, 10)
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}
	at := time.Now().UTC()
	err := s.RunInTx(ctx, func(ts TxStore) error {
		return ts.MarkOutboxPublished(ctx, []string{pending[0].EventID}, at)
	})
	if err != nil {
		t.Fatalf("MarkOutboxPublished: %v", err)
	}
	pending, _ = s.ListOutboxPending(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after publish, got %d", len(pending))
	}
}

func TestIdempotencyAndDedupKeys(t *testing.T) {
	k1 := IdempotencyKey("tx", statemachine.StepPolicy, 1)
	k2 := IdempotencyKey("tx", statemachine.StepPolicy, 2)
	if k1 == k2 {
		t.Fatal("idempotency keys should differ by attempt")
	}
	dk := DedupKey("tx", "step.succeeded", "policy", 1)
	if dk != "tx|step.succeeded|policy|1" {
		t.Fatalf("unexpected dedup key: %s", dk)
	}
}

func TestMemStoreCloseIsNoop(t *testing.T) {
	s := NewMemStore()
	s.Close()
	// Close is idempotent.
	s.Close()
}

func TestEncodeJSONNilAndEmpty(t *testing.T) {
	if got := EncodeJSON(nil); string(got) != "{}" {
		t.Fatalf("expected {} for nil, got %s", got)
	}
	if got := EncodeJSON(map[string]any{"k": "v"}); string(got) != `{"k":"v"}` {
		t.Fatalf("unexpected json: %s", got)
	}
}

func TestMemStoreListOutboxPendingLimit(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	_ = s.RunInTx(ctx, func(ts TxStore) error {
		var evts []OutboxEvent
		for i := 0; i < 5; i++ {
			evts = append(evts, OutboxEvent{
				EventID: NewEventID(), TxID: "t", EventType: "e",
				Status: OutboxPending, DedupKey: DedupKey("t", "e", "", i),
				CreatedAt: time.Now().UTC(),
			})
		}
		return ts.AppendOutbox(ctx, evts)
	})
	pending, err := s.ListOutboxPending(ctx, 3)
	if err != nil {
		t.Fatalf("ListOutboxPending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending (limit), got %d", len(pending))
	}
}

func TestMemStoreListInflightEmptyAndNonTerminal(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	inflight, err := s.ListInflightSagaIDs(ctx)
	if err != nil || len(inflight) != 0 {
		t.Fatalf("expected 0 inflight, got %d err=%v", len(inflight), err)
	}
	seedTx(t, s, "if-1")
	inflight, _ = s.ListInflightSagaIDs(ctx)
	if len(inflight) != 1 {
		t.Fatalf("expected 1 inflight, got %d", len(inflight))
	}
}

func TestMemStoreBeginTxCommitRollback(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	tx, err := s.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx2, _ := s.BeginTx(ctx)
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestMemStoreWithinReturnsTxStore(t *testing.T) {
	s := NewMemStore()
	ctx := context.Background()
	tx, _ := s.BeginTx(ctx)
	ts := s.Within(tx)
	if ts == nil {
		t.Fatal("Within returned nil")
	}
	_ = tx.Commit(ctx)
}