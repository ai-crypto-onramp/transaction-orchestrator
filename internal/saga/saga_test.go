package saga

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func testCfg() Config {
	return Config{
		MaxRetries:  3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		StepTimeout: func(string) time.Duration { return 2 * time.Second },
	}
}

func seedCtx(t *testing.T, s store.Store, txID string) store.Transaction {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: txID, UserID: "u1", QuoteID: "q1", Amount: "100", Asset: "BTC",
		Rail: "card", DestAddress: "0xabc", Status: statemachine.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	var steps []store.StepRow
	for _, st := range statemachine.StepOrder {
		steps = append(steps, store.StepRow{
			TxID: txID, StepName: st, Status: store.StepPending, Attempt: 1,
			IdempotencyKey: store.IdempotencyKey(txID, st, 1),
		})
	}
	saga0 := store.SagaState{
		TxID: txID, CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Payload: map[string]any{}, Version: 1,
	}
	evts := []store.OutboxEvent{{
		EventID: store.NewEventID(), TxID: txID, EventType: "transaction.created",
		Status: store.OutboxPending, DedupKey: store.DedupKey(txID, "transaction.created", "", 0),
		CreatedAt: now,
	}}
	if err := s.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.CreateTx(ctx, tx, steps, saga0, evts)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return tx
}

func runWithLog(ctx context.Context) context.Context {
	return logging.WithLogger(ctx, logging.New("debug"))
}

// TestHappyPathFullSaga runs the full six-step happy path with stubs.
func TestHappyPathFullSaga(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-happy")
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())

	ctx := runWithLog(context.Background())
	if err := ex.Run(ctx, "tx-happy", "test"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tx, _ := s.LoadTx(ctx, "tx-happy")
	if tx.Status != statemachine.StateCompleted {
		t.Fatalf("expected completed, got %s", tx.Status)
	}
	sg, _ := s.LoadSagaState(ctx, "tx-happy")
	if sg.State != statemachine.StateCompleted {
		t.Fatalf("expected saga completed, got %s", sg.State)
	}
	if sg.Payload["auth_id"] == "" || sg.Payload["capture_id"] == "" {
		t.Fatalf("expected auth_id and capture_id in payload, got %+v", sg.Payload)
	}
	if sg.Payload["signed_tx_hex"] == "" || sg.Payload["tx_hash"] == "" {
		t.Fatalf("expected signed_tx_hex and tx_hash in payload, got %+v", sg.Payload)
	}
	if sg.Payload["ledger_journal_id"] == "" {
		t.Fatalf("expected ledger_journal_id in payload, got %+v", sg.Payload)
	}
	steps, _ := s.ListSteps(ctx, "tx-happy")
	for _, r := range steps {
		if r.Status != store.StepSucceeded {
			t.Fatalf("step %s should be succeeded, got %s", r.StepName, r.Status)
		}
	}
}

// TestPolicyDeny tests Stage 3 acceptance criteria.
func TestPolicyDeny(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-deny")
	cfg := partner.DefaultStubConfig()
	cfg.PolicyDecision = partner.PolicyDeny
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	if err := ex.Run(ctx, "tx-deny", "test"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tx, _ := s.LoadTx(ctx, "tx-deny")
	if tx.Status != statemachine.StateFailedCompensated {
		t.Fatalf("expected failed_compensated, got %s", tx.Status)
	}
	if stub.AuthorizeCalls > 0 {
		t.Fatalf("payment should not be called on policy deny, got %d calls", stub.AuthorizeCalls)
	}
}

// TestPolicyTransientRetry exercises retry path on transient errors.
func TestPolicyTransientRetry(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-retry")
	cfg := partner.DefaultStubConfig()
	cfg.PolicyError = partner.ErrTransient
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-retry", "test") // Run compensates and returns nil after exhaustion
	if stub.PolicyCalls != testCfg().MaxRetries {
		t.Fatalf("expected %d policy calls, got %d", testCfg().MaxRetries, stub.PolicyCalls)
	}
	tx, _ := s.LoadTx(ctx, "tx-retry")
	if tx.Status != statemachine.StateFailedCompensated {
		t.Fatalf("expected failed_compensated, got %s", tx.Status)
	}
}

// TestPaymentPreCaptureFailure verifies that VoidAuthorization is called on
// pre-capture failure and the saga ends in failed_compensated.
func TestPaymentPreCaptureFailure(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-precap")
	cfg := partner.DefaultStubConfig()
	cfg.CaptureError = errors.New("bank declined") // non-transient
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-precap", "test")
	if stub.AuthorizeCalls != 1 {
		t.Fatalf("expected 1 authorize call, got %d", stub.AuthorizeCalls)
	}
	if stub.VoidCalls != 1 {
		t.Fatalf("expected void to be called from inside Execute, got %d", stub.VoidCalls)
	}
	if stub.RefundCalls != 0 {
		t.Fatalf("expected no refund on pre-capture failure, got %d", stub.RefundCalls)
	}
	tx, _ := s.LoadTx(ctx, "tx-precap")
	if tx.Status != statemachine.StateFailedCompensated {
		t.Fatalf("expected failed_compensated, got %s", tx.Status)
	}
}

// TestPaymentPostCaptureFailure simulates a step that fails *after* capture
// (KYT reject) and verifies Refund is called exactly once.
func TestPaymentPostCaptureFailureRefund(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-postcap")
	cfg := partner.DefaultStubConfig()
	cfg.KytDecision = partner.KytReject
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-postcap", "test")
	if stub.RefundCalls != 1 {
		t.Fatalf("expected exactly 1 refund, got %d", stub.RefundCalls)
	}
	tx, _ := s.LoadTx(ctx, "tx-postcap")
	if tx.Status != statemachine.StateFailedCompensated {
		t.Fatalf("expected failed_compensated, got %s", tx.Status)
	}
	steps, _ := s.ListSteps(ctx, "tx-postcap")
	var paymentRow store.StepRow
	for _, r := range steps {
		if r.StepName == statemachine.StepPayment {
			paymentRow = r
		}
	}
	if paymentRow.Status != store.StepCompensated {
		t.Fatalf("expected payment step compensated, got %s", paymentRow.Status)
	}
}

// TestSignFailureRefundsPayment verifies that a sign failure triggers refund.
func TestSignFailureRefundsPayment(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-signfail")
	cfg := partner.DefaultStubConfig()
	cfg.SignError = errors.New("mpc node down")
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-signfail", "test")
	if stub.RefundCalls != 1 {
		t.Fatalf("expected exactly 1 refund on sign failure, got %d", stub.RefundCalls)
	}
	tx, _ := s.LoadTx(ctx, "tx-signfail")
	if tx.Status != statemachine.StateFailedCompensated {
		t.Fatalf("expected failed_compensated, got %s", tx.Status)
	}
}

// TestBroadcastAuditOnMempool verifies the audit-log path when broadcast is
// in mempool and a downstream step fails triggering compensation.  Per the
// README, broadcast compensation records the tx_hash for monitoring even
// though the on-chain tx cannot be reversed.
func TestBroadcastAuditOnMempool(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-mempool")
	cfg := partner.DefaultStubConfig()
	cfg.BroadcastInMem = true
	// Force a sign failure so the cascade runs BroadcastStep.Compensate,
	// which must record the tx_hash for monitoring.  We seed the saga at
	// the mpc_sign step with tx_hash already set (simulating a re-broadcast
	// after a previous successful broadcast).
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(context.Background(), "tx-mempool")
		sg.State = statemachine.StateKytScreened
		sg.CurrentStep = statemachine.StepMpcSign
		sg.Payload["auth_id"] = "auth-tx-mempool"
		sg.Payload["capture_id"] = "cap-tx-mempool"
		sg.Payload["tx_hash"] = "0xhash-mempool"
		sg.Payload["in_mempool"] = true
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(context.Background(), sg)
		return nil
	})
	cfg.SignError = errors.New("mpc node down")
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-mempool", "test")
	if stub.AuditCalls == 0 {
		t.Fatalf("expected audit records for mempool tx_hash, got 0")
	}
	if stub.RefundCalls != 1 {
		t.Fatalf("expected refund on sign failure, got %d", stub.RefundCalls)
	}
}

// TestLedgerFailureParksFailed verifies ledger post-broadcast failure parks
// in failed (no refund).
func TestLedgerFailureParksFailed(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-ledgerfail")
	cfg := partner.DefaultStubConfig()
	cfg.LedgerError = errors.New("ledger unavailable")
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-ledgerfail", "test")
	if stub.RefundCalls != 0 {
		t.Fatalf("ledger failure should NOT refund, got %d", stub.RefundCalls)
	}
	tx, _ := s.LoadTx(ctx, "tx-ledgerfail")
	if tx.Status != statemachine.StateFailed {
		t.Fatalf("expected failed (reconcile), got %s", tx.Status)
	}
}

// TestIdempotencyKeyPreventsDoubleExecute forces the same attempt to be
// re-run; the second run must be a no-op.
func TestIdempotencyKeyPreventsDoubleExecute(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-idem")
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	// Run policy step manually with attempt 1 twice.
	tx, _ := s.LoadTx(ctx, "tx-idem")
	sg, _ := s.LoadSagaState(ctx, "tx-idem")
	sc := &SagaContext{Tx: tx, Saga: sg, Attempt: 1, Partners: c}
	policy := NewPolicyStep(stub)
	if err := ex.runStepOnce(ctx, policy, sc); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// Reload saga state so the second run sees the persisted update.
	sg2, _ := s.LoadSagaState(ctx, "tx-idem")
	sc2 := &SagaContext{Tx: tx, Saga: sg2, Attempt: 1, Partners: c}
	if err := ex.runStepOnce(ctx, policy, sc2); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if stub.PolicyCalls > 1 {
		t.Fatalf("expected policy to be called once, got %d", stub.PolicyCalls)
	}
}

// TestNonRetriableShortCircuits verifies that a non-retriable error skips
// retries.
func TestNonRetriableShortCircuits(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-nonretr")
	cfg := partner.DefaultStubConfig()
	cfg.PolicyError = NonRetriable(errors.New("hard fail"))
	stub := partner.NewStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-nonretr", "test")
	if stub.PolicyCalls != 1 {
		t.Fatalf("expected 1 policy call on non-retriable, got %d", stub.PolicyCalls)
	}
}

// TestIsNonRetriable covers the helper.
func TestIsNonRetriable(t *testing.T) {
	if !IsNonRetriable(NonRetriable(errors.New("x"))) {
		t.Fatal("wrapped should be non-retriable")
	}
	if !IsNonRetriable(partner.ErrDenied) {
		t.Fatal("ErrDenied should be non-retriable")
	}
	if IsNonRetriable(partner.ErrTransient) {
		t.Fatal("ErrTransient should be retriable")
	}
	if IsNonRetriable(nil) {
		t.Fatal("nil should not be non-retriable")
	}
}

// TestBackoffMonotonic sanity-checks the backoff helper.
func TestBackoffMonotonic(t *testing.T) {
	cfg := Config{BaseBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
	d1 := exBackoff(cfg, 1)
	d2 := exBackoff(cfg, 2)
	if d1 <= 0 || d2 < d1 {
		t.Fatalf("backoff should grow: d1=%v d2=%v", d1, d2)
	}
	d10 := exBackoff(cfg, 10)
	if d10 > cfg.MaxBackoff*2 {
		t.Fatalf("backoff should be capped: %v", d10)
	}
}

// exBackoff is a test-export alias for the executor's backoff.
func exBackoff(c Config, attempt int) time.Duration {
	e := &Executor{Cfg: c}
	return e.backoff(attempt)
}

// TestRunOnTerminalIsNoop ensures re-running a completed tx is a no-op.
func TestRunOnTerminalIsNoop(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-term")
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx := runWithLog(context.Background())
	_ = ex.Run(ctx, "tx-term", "test")
	// Force terminal state, then re-run.
	_ = s.RunInTx(ctx, func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(ctx, "tx-term")
		sg.State = statemachine.StateCompleted
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(ctx, sg)
		return nil
	})
	calls := stub.PolicyCalls
	_ = ex.Run(ctx, "tx-term", "test")
	if stub.PolicyCalls != calls {
		t.Fatalf("re-run on terminal should not call policy; was %d now %d", calls, stub.PolicyCalls)
	}
}

// TestStepErrorMessages asserts that step errors include context.
func TestStepErrorMessages(t *testing.T) {
	_, err := (&PolicyStep{}).Execute(context.Background(), &SagaContext{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
	_, err = (&PaymentStep{}).Execute(context.Background(), &SagaContext{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
	_, err = (&KytStep{}).Execute(context.Background(), &SagaContext{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
	_, err = (&MpcSignStep{}).Execute(context.Background(), &SagaContext{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
	_, err = (&BroadcastStep{}).Execute(context.Background(), &SagaContext{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
	_, err = (&LedgerStep{}).Execute(context.Background(), &SagaContext{})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected not-configured error, got %v", err)
	}
	// Compensate on a nil-client step should also error for payment.
	if err := (&PaymentStep{}).Compensate(context.Background(), &SagaContext{}); err == nil {
		t.Fatal("expected payment compensate error on nil client")
	}
	// BroadcastStep.Compensate with an audit client records an audit event.
	stub := partner.NewStub(partner.DefaultStubConfig())
	bc := NewBroadcastStep(stub)
	if err := bc.Compensate(context.Background(), &SagaContext{
		Tx: store.Transaction{TxID: "tx-audit"},
		Saga: store.SagaState{Payload: map[string]any{"tx_hash": "0xh"}},
		Partners: &Clients{Audit: stub},
	}); err != nil {
		t.Fatalf("broadcast compensate: %v", err)
	}
	if stub.AuditCalls == 0 {
		t.Fatal("expected audit record on broadcast compensate")
	}
	// LedgerStep.Compensate is a no-op.
	if err := (&LedgerStep{client: stub}).Compensate(context.Background(), &SagaContext{}); err != nil {
		t.Fatalf("ledger compensate: %v", err)
	}
}

// TestCompensateCascadeManual exercises the exported CompensateCascade path
// and the saga DefaultConfig helper.
func TestCompensateCascadeManual(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-casc")
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, DefaultConfig())
	ctx := runWithLog(context.Background())
	// Advance to payment_captured so compensation will refund.
	if err := ex.Run(ctx, "tx-casc", "test"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	tx, _ := s.LoadTx(ctx, "tx-casc")
	if tx.Status != statemachine.StateCompleted {
		t.Fatalf("expected completed, got %s", tx.Status)
	}
	// Now manually call CompensateCascade for the ledger step (no-op) to
	// exercise the exported wrapper; it should not error because ledger
	// compensate is a no-op.
	if err := ex.CompensateCascade(ctx, "tx-casc", "test", ex.Steps[5]); err != nil {
		t.Fatalf("CompensateCascade: %v", err)
	}
}

// TestDefaultConfigSanity just exercises DefaultConfig so it is covered.
func TestDefaultConfigSanity(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxRetries != 5 || cfg.BaseBackoff <= 0 || cfg.MaxBackoff <= 0 || cfg.StepTimeout == nil {
		t.Fatalf("unexpected DefaultConfig: %+v", cfg)
	}
}

// heldLease always returns ok=false to exercise the backoff branch in Run.
type heldLease struct{}

func (heldLease) Acquire(ctx context.Context, txID, owner string, ttl time.Duration) (func(), bool, error) {
	return func() {}, false, nil
}

// TestRunLeaseHeldExitsOnContextCancel verifies Run exits when the context is
// cancelled while waiting for a held lease.
func TestRunLeaseHeldExitsOnContextCancel(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-held")
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ex.Lease = heldLease{}
	ctx, cancel := context.WithCancel(runWithLog(context.Background()))
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	if err := ex.Run(ctx, "tx-held", "test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestValidatePathTwoHopAndEqual exercises the two-hop and equal-state branches
// of validatePath.
func TestValidatePathTwoHopAndEqual(t *testing.T) {
	if err := validatePath(statemachine.StateCreated, statemachine.StateCreated, statemachine.StateCreated); err != nil {
		t.Fatalf("equal path should be nil, got %v", err)
	}
	// Two-hop: created -> policy_checked -> payment_captured is legal.
	if err := validatePath(statemachine.StateCreated, statemachine.StatePolicyChecked, statemachine.StatePaymentCaptured); err != nil {
		t.Fatalf("two-hop legal path should be nil, got %v", err)
	}
	// Two-hop with illegal first hop should error.
	if err := validatePath(statemachine.StateCreated, statemachine.StateCompleted, statemachine.StatePaymentCaptured); err == nil {
		t.Fatal("expected illegal first hop to error")
	}
	// Two-hop with illegal second hop should error.
	if err := validatePath(statemachine.StateCreated, statemachine.StatePolicyChecked, statemachine.StateSigned); err == nil {
		t.Fatal("expected illegal second hop to error")
	}
}

// TestStepByNameNotFound exercises the not-found branch of StepByName.
func TestStepByNameNotFound(t *testing.T) {
	ex := &Executor{}
	if ex.StepByName("nope") != nil {
		t.Fatal("expected nil for unknown step")
	}
}

// TestRunNoStepForCurrentStep verifies Run errors when the saga's current_step
// is unknown.
func TestRunNoStepForCurrentStep(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-nostep")
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(context.Background(), "tx-nostep")
		sg.CurrentStep = "nope"
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(context.Background(), sg)
		return nil
	})
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	err := ex.Run(runWithLog(context.Background()), "tx-nostep", "test")
	if err == nil || !strings.Contains(err.Error(), "no step") {
		t.Fatalf("expected no-step error, got %v", err)
	}
}

// TestRunAdvanceToConfirmedMissingTxHash verifies advanceToConfirmed errors
// when tx_hash is missing and the saga cannot complete.
func TestRunAdvanceToConfirmedMissingTxHash(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-nohash")
	// Move saga to broadcasted without tx_hash.
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(context.Background(), "tx-nohash")
		sg.State = statemachine.StateBroadcasted
		sg.CurrentStep = statemachine.StepLedger
		sg.Payload = map[string]any{}
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(context.Background(), sg)
		return nil
	})
	stub := partner.NewStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	_ = ex.Run(runWithLog(context.Background()), "tx-nohash", "test")
	sg, _ := s.LoadSagaState(context.Background(), "tx-nohash")
	if !sg.State.IsFailure() {
		t.Fatalf("expected failure state (could not advance, ledger retries exhausted), got %s", sg.State)
	}
}

// TestRunAdvanceToConfirmedPolled verifies the executor's inline
// advanceToConfirmed path moves the saga to confirmed when the blockchain
// gateway eventually reports the tx confirmed.  We seed the saga at
// broadcasted (with tx_hash) and call advanceToConfirmed directly.
func TestRunAdvanceToConfirmedPolled(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-pollrun")
	bc := &pollingFakeBlockchain{}
	c := &Clients{Blockchain: bc}
	ex := NewExecutor(s, c, testCfg())
	_ = s.RunInTx(context.Background(), func(ts store.TxStore) error {
		sg, _ := ts.LoadSagaState(context.Background(), "tx-pollrun")
		sg.State = statemachine.StateBroadcasted
		sg.CurrentStep = statemachine.StepLedger
		sg.Payload = map[string]any{"tx_hash": "0xhpoll"}
		sg.Version = sg.Version + 1
		_ = ts.SaveSagaState(context.Background(), sg)
		return nil
	})
	ctx := runWithLog(context.Background())
	if err := ex.advanceToConfirmed(ctx, "tx-pollrun"); err != nil {
		t.Fatalf("advanceToConfirmed: %v", err)
	}
	sg, _ := s.LoadSagaState(ctx, "tx-pollrun")
	if sg.State != statemachine.StateConfirmed {
		t.Fatalf("expected confirmed, got %s", sg.State)
	}
}

// pollingFakeBlockchain returns Confirmed=false on the first Status call and
// Confirmed=true afterwards, simulating an eventual on-chain confirmation.
type pollingFakeBlockchain struct{ calls int }

func (p *pollingFakeBlockchain) Broadcast(ctx context.Context, req partner.BroadcastRequest) (partner.BroadcastResponse, error) {
	return partner.BroadcastResponse{TxHash: "0xhpoll", InMempool: true, Confirmed: false}, nil
}
func (p *pollingFakeBlockchain) Status(ctx context.Context, txHash string) (partner.BroadcastResponse, error) {
	p.calls++
	if p.calls > 1 {
		return partner.BroadcastResponse{TxHash: txHash, InMempool: true, Confirmed: true}, nil
	}
	return partner.BroadcastResponse{TxHash: txHash, InMempool: true, Confirmed: false}, nil
}