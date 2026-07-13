// Package partner defines the client interfaces for the six saga partner
// services, plus an in-memory stub implementation suitable for unit and
// integration tests.
//
// Real gRPC client bindings land later; for now the stub is the production
// default when the *_URL env vars point at "stub://".  Each method takes a
// context so callers can apply per-step timeouts.
package partner

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrDenied is returned by partner methods to signal an explicit policy/kyt
// deny that should not be retried.
var ErrDenied = errors.New("partner: denied")

// ErrTransient is a recoverable error that should be retried.
var ErrTransient = errors.New("partner: transient")

// --- policy-risk-engine ------------------------------------------------------

// PolicyDecision enumerates policy outcomes.
type PolicyDecision string

const (
	PolicyAllow PolicyDecision = "allow"
	PolicyDeny  PolicyDecision = "deny"
)

// PolicyRequest is the input to Policy.Evaluate.
type PolicyRequest struct {
	TxID, UserID, QuoteID, Amount, Asset, Rail, DestAddress string
}

// PolicyResponse carries the decision and a free-form reason.
type PolicyResponse struct {
	Decision PolicyDecision
	Reason   string
}

// Policy is the policy-risk-engine client.
type Policy interface {
	Evaluate(ctx context.Context, req PolicyRequest) (PolicyResponse, error)
}

// --- payment-orchestration ---------------------------------------------------

// PaymentAuthorizeRequest is the input to Authorize.
type PaymentAuthorizeRequest struct {
	TxID, UserID, QuoteID, Amount, Asset, Rail string
}

// PaymentAuthorizeResponse carries the authorization id.
type PaymentAuthorizeResponse struct {
	AuthID string
}

// PaymentCaptureRequest is the input to Capture.
type PaymentCaptureRequest struct {
	TxID, AuthID, Amount, Asset string
}

// PaymentCaptureResponse carries the capture id.
type PaymentCaptureResponse struct {
	CaptureID string
}

// PaymentVoidRequest voids an authorization.
type PaymentVoidRequest struct {
	TxID, AuthID string
}

// PaymentRefundRequest refunds a captured payment.
type PaymentRefundRequest struct {
	TxID, CaptureID, Amount, Asset string
}

// Payment is the payment-orchestration client.
type Payment interface {
	Authorize(ctx context.Context, req PaymentAuthorizeRequest) (PaymentAuthorizeResponse, error)
	Capture(ctx context.Context, req PaymentCaptureRequest) (PaymentCaptureResponse, error)
	VoidAuthorization(ctx context.Context, req PaymentVoidRequest) error
	Refund(ctx context.Context, req PaymentRefundRequest) error
}

// --- aml-kyt-screening -------------------------------------------------------

// KytDecision enumerates KYT outcomes.
type KytDecision string

const (
	KytClear   KytDecision = "clear"
	KytReview  KytDecision = "review"
	KytReject  KytDecision = "reject"
)

// KytRequest is the input to Screen.
type KytRequest struct {
	TxID, UserID, DestAddress, Amount, Asset string
}

// KytResponse carries the decision and a reason.
type KytResponse struct {
	Decision KytDecision
	Reason   string
}

// Kyt is the aml-kyt-screening client.
type Kyt interface {
	Screen(ctx context.Context, req KytRequest) (KytResponse, error)
}

// --- mpc-signing-service -----------------------------------------------------

// MpcSignRequest is the input to Sign.
type MpcSignRequest struct {
	TxID, UnsignedTxHex string
}

// MpcSignResponse carries the signed tx hex.
type MpcSignResponse struct {
	SignedTxHex string
}

// Mpc is the mpc-signing-service client.
type Mpc interface {
	Sign(ctx context.Context, req MpcSignRequest) (MpcSignResponse, error)
}

// --- blockchain-gateway ------------------------------------------------------

// BroadcastRequest is the input to Broadcast.
type BroadcastRequest struct {
	TxID, SignedTxHex string
}

// BroadcastResponse carries the on-chain tx hash.
type BroadcastResponse struct {
	TxHash      string
	InMempool   bool
	Confirmed   bool
}

// Blockchain is the blockchain-gateway client.
type Blockchain interface {
	Broadcast(ctx context.Context, req BroadcastRequest) (BroadcastResponse, error)
	Status(ctx context.Context, txHash string) (BroadcastResponse, error)
}

// --- ledger-accounting -------------------------------------------------------

// LedgerPostRequest is the input to PostDoubleEntry.
type LedgerPostRequest struct {
	TxID, UserID, Amount, Asset, Rail string
}

// LedgerPostResponse carries the journal id.
type LedgerPostResponse struct {
	JournalID string
}

// Ledger is the ledger-accounting client.
type Ledger interface {
	PostDoubleEntry(ctx context.Context, req LedgerPostRequest) (LedgerPostResponse, error)
}

// --- audit-event-log (async) -------------------------------------------------

// AuditEvent is one audit record.
type AuditEvent struct {
	TxID, Step, Actor, Before, After, Err string
	Attempt int
	At      time.Time
}

// Audit is the audit-event-log client.  Calls are best-effort.
type Audit interface {
	Record(ctx context.Context, e AuditEvent) error
}

// --- stub implementation -----------------------------------------------------

// StubConfig controls the behavior of the Stub client.
type StubConfig struct {
	PolicyDecision   PolicyDecision
	PolicyError      error
	KytDecision      KytDecision
	KytError         error
	BroadcastInMem   bool
	BroadcastConfirmed bool
	BroadcastError   error
	SignError        error
	AuthorizeError   error
	CaptureError     error
	LedgerError      error
	// Sleep optionally slows each call (for timeout tests).
	Sleep time.Duration
}

// DefaultStubConfig returns a config that makes every partner succeed.
// Broadcast returns Confirmed=true so the happy-path saga can complete
// synchronously without a separate confirmation poller.
func DefaultStubConfig() StubConfig {
	return StubConfig{
		PolicyDecision:     PolicyAllow,
		KytDecision:        KytClear,
		BroadcastInMem:     true,
		BroadcastConfirmed: true,
	}
}

// Stub is a single in-memory partner client implementing all interfaces.
type Stub struct {
	cfg StubConfig
	mu  sync.Mutex
	// Recorded call counts for assertions in tests.
	PolicyCalls   int
	AuthorizeCalls int
	CaptureCalls   int
	VoidCalls      int
	RefundCalls    int
	KytCalls       int
	SignCalls      int
	BroadcastCalls int
	StatusCalls    int
	LedgerCalls    int
	AuditCalls     int
	// Recorded ids:
	lastAuthID    string
	lastCaptureID string
	lastJournalID string
}

// NewStub returns a Stub with the given config.
func NewStub(cfg StubConfig) *Stub { return &Stub{cfg: cfg} }

func (s *Stub) sleep(ctx context.Context) {
	if s.cfg.Sleep > 0 {
		select {
		case <-time.After(s.cfg.Sleep):
		case <-ctx.Done():
		}
	}
}

// --- policy ------------------------------------------------------------------

func (s *Stub) Evaluate(ctx context.Context, req PolicyRequest) (PolicyResponse, error) {
	s.mu.Lock()
	s.PolicyCalls++
	s.mu.Unlock()
	s.sleep(ctx)
	if s.cfg.PolicyError != nil {
		return PolicyResponse{}, s.cfg.PolicyError
	}
	return PolicyResponse{Decision: s.cfg.PolicyDecision, Reason: "stub"}, nil
}

// --- payment -----------------------------------------------------------------

func (s *Stub) Authorize(ctx context.Context, req PaymentAuthorizeRequest) (PaymentAuthorizeResponse, error) {
	s.mu.Lock()
	s.AuthorizeCalls++
	id := "auth-" + req.TxID
	s.lastAuthID = id
	s.mu.Unlock()
	s.sleep(ctx)
	if s.cfg.AuthorizeError != nil {
		return PaymentAuthorizeResponse{}, s.cfg.AuthorizeError
	}
	return PaymentAuthorizeResponse{AuthID: id}, nil
}

func (s *Stub) Capture(ctx context.Context, req PaymentCaptureRequest) (PaymentCaptureResponse, error) {
	s.mu.Lock()
	s.CaptureCalls++
	id := "cap-" + req.TxID
	s.lastCaptureID = id
	s.mu.Unlock()
	s.sleep(ctx)
	if s.cfg.CaptureError != nil {
		return PaymentCaptureResponse{}, s.cfg.CaptureError
	}
	return PaymentCaptureResponse{CaptureID: id}, nil
}

func (s *Stub) VoidAuthorization(ctx context.Context, req PaymentVoidRequest) error {
	s.mu.Lock()
	s.VoidCalls++
	s.mu.Unlock()
	return nil
}

func (s *Stub) Refund(ctx context.Context, req PaymentRefundRequest) error {
	s.mu.Lock()
	s.RefundCalls++
	s.mu.Unlock()
	return nil
}

// --- kyt ---------------------------------------------------------------------

func (s *Stub) Screen(ctx context.Context, req KytRequest) (KytResponse, error) {
	s.mu.Lock()
	s.KytCalls++
	s.mu.Unlock()
	s.sleep(ctx)
	if s.cfg.KytError != nil {
		return KytResponse{}, s.cfg.KytError
	}
	return KytResponse{Decision: s.cfg.KytDecision, Reason: "stub"}, nil
}

// --- mpc ---------------------------------------------------------------------

func (s *Stub) Sign(ctx context.Context, req MpcSignRequest) (MpcSignResponse, error) {
	s.mu.Lock()
	s.SignCalls++
	s.mu.Unlock()
	s.sleep(ctx)
	if s.cfg.SignError != nil {
		return MpcSignResponse{}, s.cfg.SignError
	}
	return MpcSignResponse{SignedTxHex: "signed-" + req.TxID}, nil
}

// --- blockchain --------------------------------------------------------------

func (s *Stub) Broadcast(ctx context.Context, req BroadcastRequest) (BroadcastResponse, error) {
	s.mu.Lock()
	s.BroadcastCalls++
	s.mu.Unlock()
	s.sleep(ctx)
	if s.cfg.BroadcastError != nil {
		return BroadcastResponse{}, s.cfg.BroadcastError
	}
	return BroadcastResponse{
		TxHash:    "0xhash-" + req.TxID,
		InMempool: s.cfg.BroadcastInMem,
		Confirmed: s.cfg.BroadcastConfirmed,
	}, nil
}

func (s *Stub) Status(ctx context.Context, txHash string) (BroadcastResponse, error) {
	s.mu.Lock()
	s.StatusCalls++
	s.mu.Unlock()
	return BroadcastResponse{TxHash: txHash, InMempool: true, Confirmed: s.cfg.BroadcastConfirmed}, nil
}

// --- ledger ------------------------------------------------------------------

func (s *Stub) PostDoubleEntry(ctx context.Context, req LedgerPostRequest) (LedgerPostResponse, error) {
	s.mu.Lock()
	s.LedgerCalls++
	id := "jrn-" + req.TxID
	s.lastJournalID = id
	s.mu.Unlock()
	if s.cfg.LedgerError != nil {
		return LedgerPostResponse{}, s.cfg.LedgerError
	}
	return LedgerPostResponse{JournalID: id}, nil
}

// --- audit -------------------------------------------------------------------

func (s *Stub) Record(ctx context.Context, e AuditEvent) error {
	s.mu.Lock()
	s.AuditCalls++
	s.mu.Unlock()
	return nil
}

// LastAuthID / LastCaptureID / LastJournalID expose ids written by the stub.
func (s *Stub) LastAuthID() string    { s.mu.Lock(); defer s.mu.Unlock(); return s.lastAuthID }
func (s *Stub) LastCaptureID() string { s.mu.Lock(); defer s.mu.Unlock(); return s.lastCaptureID }
func (s *Stub) LastJournalID() string { s.mu.Lock(); defer s.mu.Unlock(); return s.lastJournalID }