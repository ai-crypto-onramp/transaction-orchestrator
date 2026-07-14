package saga

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

type fakeBlockchain struct {
	confirmed bool
	calls      int
	err        error
}

func (f *fakeBlockchain) Status(ctx context.Context, txHash string) (broadcastStatus, error) {
	f.calls++
	if f.err != nil {
		return broadcastStatus{}, f.err
	}
	return broadcastStatus{TxHash: txHash, Confirmed: f.confirmed}, nil
}

func seedBroadcastedTx(t *testing.T, s store.Store, txID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: txID, UserID: "u", QuoteID: "q", Amount: "1", Asset: "BTC",
		Rail: "card", DestAddress: "0x", Status: statemachine.StateBroadcasted,
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
		TxID: txID, CurrentStep: statemachine.StepLedger, State: statemachine.StateBroadcasted,
		Version: 1, Payload: map[string]any{"tx_hash": "0xh-" + txID},
	}
	evts := []store.OutboxEvent{{
		EventID: store.NewEventID(), TxID: txID, EventType: "transaction.broadcasted",
		Status: store.OutboxPending, DedupKey: store.DedupKey(txID, "transaction.broadcasted", "", 0),
		CreatedAt: now,
	}}
	if err := s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.CreateTx(ctx, tx, steps, sg, evts)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func TestConfirmPollerAdvancesToConfirmed(t *testing.T) {
	s := store.NewMemStore()
	seedBroadcastedTx(t, s, "tx-poll-1")
	bc := &fakeBlockchain{confirmed: true}
	p := &ConfirmPoller{
		Store: s, Client: bc,
		Interval: 10 * time.Millisecond, MaxWait: 2 * time.Second,
	}
	ctx := logging.WithLogger(context.Background(), logging.New("debug"))
	if err := p.Run(ctx, "tx-poll-1"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sg, _ := s.LoadSagaState(ctx, "tx-poll-1")
	if sg.State != statemachine.StateConfirmed {
		t.Fatalf("expected confirmed, got %s", sg.State)
	}
}

func TestConfirmPollerDeadlineExceeded(t *testing.T) {
	s := store.NewMemStore()
	seedBroadcastedTx(t, s, "tx-poll-2")
	bc := &fakeBlockchain{confirmed: false}
	p := &ConfirmPoller{
		Store: s, Client: bc,
		Interval: 5 * time.Millisecond, MaxWait: 100 * time.Millisecond,
	}
	ctx := context.Background()
	if err := p.Run(ctx, "tx-poll-2"); err == nil {
		t.Fatal("expected deadline error")
	}
}

func TestConfirmPollerMissingTxHash(t *testing.T) {
	s := store.NewMemStore()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: "tx-poll-3", UserID: "u", QuoteID: "q", Amount: "1", Asset: "BTC",
		Rail: "card", DestAddress: "0x", Status: statemachine.StateBroadcasted,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	sg := store.SagaState{
		TxID: "tx-poll-3", CurrentStep: statemachine.StepLedger, State: statemachine.StateBroadcasted,
		Version: 1, Payload: map[string]any{},
	}
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		return ts.CreateTx(context.Background(), tx, nil, sg, nil)
	})
	p := &ConfirmPoller{Store: s, Client: &fakeBlockchain{}, Interval: time.Millisecond, MaxWait: time.Second}
	if err := p.Run(context.Background(), "tx-poll-3"); err == nil {
		t.Fatal("expected missing-tx_hash error")
	}
}

func TestConfirmPollerWrongState(t *testing.T) {
	s := store.NewMemStore()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: "tx-poll-4", UserID: "u", QuoteID: "q", Amount: "1", Asset: "BTC",
		Rail: "card", DestAddress: "0x", Status: statemachine.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	sg := store.SagaState{
		TxID: "tx-poll-4", CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Version: 1, Payload: map[string]any{},
	}
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		return ts.CreateTx(context.Background(), tx, nil, sg, nil)
	})
	p := &ConfirmPoller{Store: s, Client: &fakeBlockchain{}, Interval: time.Millisecond, MaxWait: time.Second}
	if err := p.Run(context.Background(), "tx-poll-4"); err == nil {
		t.Fatal("expected wrong-state error")
	}
}

func TestConfirmPollerAlreadyConfirmed(t *testing.T) {
	s := store.NewMemStore()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: "tx-poll-5", UserID: "u", QuoteID: "q", Amount: "1", Asset: "BTC",
		Rail: "card", DestAddress: "0x", Status: statemachine.StateConfirmed,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	sg := store.SagaState{
		TxID: "tx-poll-5", CurrentStep: statemachine.StepLedger, State: statemachine.StateConfirmed,
		Version: 1, Payload: map[string]any{"tx_hash": "0xh"},
	}
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		return ts.CreateTx(context.Background(), tx, nil, sg, nil)
	})
	p := &ConfirmPoller{Store: s, Client: &fakeBlockchain{}, Interval: time.Millisecond, MaxWait: time.Second}
	if err := p.Run(context.Background(), "tx-poll-5"); err != nil {
		t.Fatalf("expected nil for already-confirmed, got %v", err)
	}
}

func TestConfirmPollerLoadError(t *testing.T) {
	s := store.NewMemStore()
	p := &ConfirmPoller{Store: s, Client: &fakeBlockchain{}, Interval: time.Millisecond, MaxWait: time.Second}
	err := p.Run(context.Background(), "nope")
	if err == nil || !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// flippingBlockchain returns Confirmed=false on the first call and true
// afterwards, exercising the poll-loop success branch.
type flippingBlockchain struct{ calls int }

func (f *flippingBlockchain) Status(ctx context.Context, txHash string) (broadcastStatus, error) {
	f.calls++
	if f.calls > 1 {
		return broadcastStatus{TxHash: txHash, Confirmed: true}, nil
	}
	return broadcastStatus{TxHash: txHash, Confirmed: false}, nil
}

func TestConfirmPollerPollsThenConfirms(t *testing.T) {
	s := store.NewMemStore()
	seedBroadcastedTx(t, s, "tx-flip")
	bc := &flippingBlockchain{}
	p := &ConfirmPoller{Store: s, Client: bc, Interval: 5 * time.Millisecond, MaxWait: 2 * time.Second}
	if err := p.Run(context.Background(), "tx-flip"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if bc.calls < 2 {
		t.Fatalf("expected at least 2 status calls, got %d", bc.calls)
	}
	sg, _ := s.LoadSagaState(context.Background(), "tx-flip")
	if sg.State != statemachine.StateConfirmed {
		t.Fatalf("expected confirmed, got %s", sg.State)
	}
}