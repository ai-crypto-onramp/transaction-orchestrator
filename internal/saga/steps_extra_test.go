package saga

import (
	"context"
	"errors"
	"testing"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

// stepStub wraps partner.Stub to allow fine-grained control per step.
func stepStub(cfg partner.StubConfig) *partner.Stub { return partner.NewStub(cfg) }

func sagaCtxWithTx(txID string) *SagaContext {
	return &SagaContext{
		Tx: store.Transaction{
			TxID: txID, UserID: "u", QuoteID: "q", Amount: "1", Asset: "BTC",
			Rail: "CARD", DestAddress: "0xabc",
		},
		Saga: store.SagaState{
			TxID: txID, Version: 1, Payload: map[string]any{},
		},
		Partners: &Clients{},
	}
}

func TestKytStepTransientError(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.KytError = partner.ErrTransient
	stub := stepStub(cfg)
	k := NewKytStep(stub)
	_, err := k.Execute(context.Background(), sagaCtxWithTx("tx-kt"))
	if !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
}

func TestKytStepUnknownDecision(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.KytDecision = partner.KytDecision("bogus")
	stub := stepStub(cfg)
	k := NewKytStep(stub)
	_, err := k.Execute(context.Background(), sagaCtxWithTx("tx-ku"))
	if err == nil || !IsNonRetriable(err) {
		t.Fatalf("expected non-retriable error for unknown decision, got %v", err)
	}
}

func TestKytStepReviewDecision(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.KytDecision = partner.KytReview
	stub := stepStub(cfg)
	k := NewKytStep(stub)
	_, err := k.Execute(context.Background(), sagaCtxWithTx("tx-kr"))
	if err == nil || !IsNonRetriable(err) {
		t.Fatalf("expected non-retriable error for review, got %v", err)
	}
}

func TestKytStepClearDecision(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	k := NewKytStep(stub)
	res, err := k.Execute(context.Background(), sagaCtxWithTx("tx-kc"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.State != statemachine.StateKytScreened {
		t.Fatalf("expected kyt_screened, got %s", res.State)
	}
	if res.PayloadMerge["kyt_reason"] != "stub" {
		t.Fatalf("expected kyt_reason stub, got %#v", res.PayloadMerge)
	}
}

func TestKytStepCompensateIsNoop(t *testing.T) {
	k := NewKytStep(stepStub(partner.DefaultStubConfig()))
	if err := k.Compensate(context.Background(), sagaCtxWithTx("tx-kcomp")); err != nil {
		t.Fatalf("Compensate: %v", err)
	}
}

func TestPaymentStepTransientAuthorize(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.AuthorizeError = partner.ErrTransient
	stub := stepStub(cfg)
	p := NewPaymentStep(stub)
	_, err := p.Execute(context.Background(), sagaCtxWithTx("tx-pt"))
	if !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
}

func TestPaymentStepDeniedAuthorize(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.AuthorizeError = partner.ErrDenied
	stub := stepStub(cfg)
	p := NewPaymentStep(stub)
	_, err := p.Execute(context.Background(), sagaCtxWithTx("tx-pd"))
	if err == nil || !IsNonRetriable(err) {
		t.Fatalf("expected non-retriable, got %v", err)
	}
}

func TestPaymentStepTransientCaptureVoidsAuth(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.CaptureError = partner.ErrTransient
	stub := stepStub(cfg)
	p := NewPaymentStep(stub)
	res, err := p.Execute(context.Background(), sagaCtxWithTx("tx-ptc"))
	if !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
	if res.PayloadMerge["auth_id"] == "" {
		t.Fatal("expected auth_id in payload merge on transient capture")
	}
	if stub.VoidCalls != 1 {
		t.Fatalf("expected void called once, got %d", stub.VoidCalls)
	}
}

func TestPaymentStepSuccessStoresIDs(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	p := NewPaymentStep(stub)
	res, err := p.Execute(context.Background(), sagaCtxWithTx("tx-ps"))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.State != statemachine.StatePaymentCaptured {
		t.Fatalf("expected payment_captured, got %s", res.State)
	}
	if res.PayloadMerge["auth_id"] == "" || res.PayloadMerge["capture_id"] == "" {
		t.Fatalf("expected auth_id and capture_id, got %#v", res.PayloadMerge)
	}
}

func TestPaymentStepCompensateRefundError(t *testing.T) {
	p := NewPaymentStep(&errPayment{refundErr: errors.New("refund fail")})
	sc := sagaCtxWithTx("tx-pcr")
	sc.Saga.Payload["capture_id"] = "cap-1"
	if err := p.Compensate(context.Background(), sc); err == nil {
		t.Fatal("expected refund error, got nil")
	}
}

func TestPaymentStepCompensateVoidError(t *testing.T) {
	p := NewPaymentStep(&errPayment{voidErr: errors.New("void fail")})
	sc := sagaCtxWithTx("tx-pcv")
	sc.Saga.Payload["auth_id"] = "auth-1"
	if err := p.Compensate(context.Background(), sc); err == nil {
		t.Fatal("expected void error, got nil")
	}
}

func TestPaymentStepCompensateNoIDsIsNoop(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	p := NewPaymentStep(stub)
	sc := sagaCtxWithTx("tx-pc0")
	if err := p.Compensate(context.Background(), sc); err != nil {
		t.Fatalf("Compensate with no ids: %v", err)
	}
}

func TestBroadcastStepIdempotentExistingHash(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	b := NewBroadcastStep(stub)
	sc := sagaCtxWithTx("tx-bi")
	sc.Saga.Payload["tx_hash"] = "0xexisting"
	res, err := b.Execute(context.Background(), sc)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.State != statemachine.StateBroadcasted || res.PayloadMerge["tx_hash"] != "0xexisting" {
		t.Fatalf("unexpected idempotent result: %+v", res)
	}
	if stub.BroadcastCalls != 0 {
		t.Fatalf("expected 0 broadcast calls on idempotent path, got %d", stub.BroadcastCalls)
	}
}

func TestBroadcastStepMissingSignedTxHex(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	b := NewBroadcastStep(stub)
	_, err := b.Execute(context.Background(), sagaCtxWithTx("tx-bm"))
	if err == nil || !IsNonRetriable(err) {
		t.Fatalf("expected non-retriable missing signed_tx_hex, got %v", err)
	}
}

func TestBroadcastStepTransientError(t *testing.T) {
	cfg := partner.DefaultStubConfig()
	cfg.BroadcastError = partner.ErrTransient
	stub := stepStub(cfg)
	b := NewBroadcastStep(stub)
	sc := sagaCtxWithTx("tx-bt")
	sc.Saga.Payload["signed_tx_hex"] = "0xsigned"
	_, err := b.Execute(context.Background(), sc)
	if !errors.Is(err, partner.ErrTransient) {
		t.Fatalf("expected ErrTransient, got %v", err)
	}
}

func TestBroadcastStepSuccessMempoolAndConfirmed(t *testing.T) {
	// Confirmed=true path: payload gets confirmed=true.
	cfg := partner.DefaultStubConfig()
	cfg.BroadcastConfirmed = true
	stub := stepStub(cfg)
	b := NewBroadcastStep(stub)
	sc := sagaCtxWithTx("tx-bs")
	sc.Saga.Payload["signed_tx_hex"] = "0xsigned"
	res, err := b.Execute(context.Background(), sc)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.State != statemachine.StateBroadcasted {
		t.Fatalf("expected broadcasted, got %s", res.State)
	}
	if res.PayloadMerge["tx_hash"] == "" || res.PayloadMerge["confirmed"] != true {
		t.Fatalf("expected tx_hash and confirmed, got %#v", res.PayloadMerge)
	}
}

func TestBroadcastStepCompensateNoTxHashNoAudit(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	b := NewBroadcastStep(stub)
	sc := sagaCtxWithTx("tx-bc0")
	sc.Partners = &Clients{Audit: stub}
	if err := b.Compensate(context.Background(), sc); err != nil {
		t.Fatalf("Compensate: %v", err)
	}
	if stub.AuditCalls != 0 {
		t.Fatalf("expected no audit calls without tx_hash, got %d", stub.AuditCalls)
	}
}

func TestCompensateCascadeUnknownStepErrors(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-cu")
	stub := stepStub(partner.DefaultStubConfig())
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	// Use a step with a name not registered in ex.Steps.
	err := ex.CompensateCascade(runWithLog(context.Background()), "tx-cu", "test", &unknownStep{})
	if err == nil {
		t.Fatal("expected error for unregistered step")
	}
}

// unknownStep is a Step whose Name is not in statemachine.StepOrder.
type unknownStep struct{}

func (unknownStep) Name() statemachine.Step { return statemachine.Step("UNKNOWN") }
func (unknownStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	return StepResult{}, nil
}
func (unknownStep) Compensate(ctx context.Context, sc *SagaContext) error { return nil }

func TestMergePayloadOverlaysMergeOnBase(t *testing.T) {
	base := map[string]any{"a": "1", "b": "2"}
	merge := map[string]any{"b": "3", "c": "4"}
	out := mergePayload(base, merge)
	if out["a"] != "1" || out["b"] != "3" || out["c"] != "4" {
		t.Fatalf("unexpected merge: %#v", out)
	}
}

func TestAuditWithNilClientsIsNoop(t *testing.T) {
	ex := &Executor{}
	ex.audit(context.Background(), &SagaContext{}, "step", "before", "after", errors.New("x"))
}

func TestAuditWithErrorString(t *testing.T) {
	stub := stepStub(partner.DefaultStubConfig())
	ex := &Executor{Clients: &Clients{Audit: stub}}
	ex.audit(context.Background(), &SagaContext{Tx: store.Transaction{TxID: "tx-aud"}}, "step", "before", "after", errors.New("boom"))
	if stub.AuditCalls != 1 {
		t.Fatalf("expected 1 audit call, got %d", stub.AuditCalls)
	}
}

func TestRunStepWithRetryContextCancelReturnsCtxErr(t *testing.T) {
	s := store.NewMemStore()
	seedCtx(t, s, "tx-rsrx")
	cfg := partner.DefaultStubConfig()
	cfg.PolicyError = errors.New("fail")
	stub := stepStub(cfg)
	c := &Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	ex := NewExecutor(s, c, testCfg())
	ctx, cancel := context.WithCancel(runWithLog(context.Background()))
	cancel() // cancel before running so backoff path exits immediately
	tx, _ := s.LoadTx(ctx, "tx-rsrx")
	sg, _ := s.LoadSagaState(ctx, "tx-rsrx")
	step := ex.Steps[0]
	err := ex.runStepWithRetry(ctx, step, &SagaContext{Tx: tx, Saga: sg, Attempt: 1, Partners: c})
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestRunLoadTxErrorPropagates(t *testing.T) {
	ex := &Executor{Store: store.NewMemStore(), Clients: &Clients{}}
	ex.Lease = heldLease{}
	if err := ex.Run(context.Background(), "missing", "test"); err == nil {
		t.Fatal("expected error loading missing tx")
	}
}

func TestAdvanceToConfirmedNilBlockchainIsNoop(t *testing.T) {
	s := store.NewMemStore()
	ex := &Executor{Store: s, Clients: &Clients{Blockchain: nil}}
	if err := ex.advanceToConfirmed(context.Background(), "missing"); err != nil {
		t.Fatalf("expected nil with nil blockchain, got %v", err)
	}
}

func TestBackoffDefaultsBaseAndMax(t *testing.T) {
	// Exercise the base<=0 and max<=0 defaulting branches.
	e := &Executor{Cfg: Config{}}
	d := e.backoff(1)
	if d <= 0 {
		t.Fatalf("expected positive backoff with defaults, got %v", d)
	}
}

// errPayment is a partner.Payment implementation that returns configurable
// errors from VoidAuthorization and Refund.
type errPayment struct {
	voidErr   error
	refundErr error
}

func (e *errPayment) Authorize(ctx context.Context, req partner.PaymentAuthorizeRequest) (partner.PaymentAuthorizeResponse, error) {
	return partner.PaymentAuthorizeResponse{AuthID: "a"}, nil
}

func (e *errPayment) Capture(ctx context.Context, req partner.PaymentCaptureRequest) (partner.PaymentCaptureResponse, error) {
	return partner.PaymentCaptureResponse{CaptureID: "c"}, nil
}

func (e *errPayment) VoidAuthorization(ctx context.Context, req partner.PaymentVoidRequest) error {
	return e.voidErr
}

func (e *errPayment) Refund(ctx context.Context, req partner.PaymentRefundRequest) error {
	return e.refundErr
}