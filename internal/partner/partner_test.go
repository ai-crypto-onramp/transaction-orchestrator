package partner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStubDefaultConfig(t *testing.T) {
	s := NewStub(StubConfig{})
	resp, err := s.Evaluate(context.Background(), PolicyRequest{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if resp.Decision != "" || resp.Reason != "stub" {
		t.Fatalf("expected empty decision/stub reason, got %+v", resp)
	}
}

func TestStubSleepAndCancellation(t *testing.T) {
	s := NewStub(StubConfig{Sleep: 200 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _ = s.Evaluate(ctx, PolicyRequest{})
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("sleep should have been cancelled by ctx")
	}
}

func TestStubLastIDsAndErrorPaths(t *testing.T) {
	s := NewStub(StubConfig{
		AuthorizeError: errors.New("auth-fail"),
		CaptureError:   errors.New("cap-fail"),
		LedgerError:    errors.New("ldg-fail"),
		SignError:      errors.New("sign-fail"),
		BroadcastError: errors.New("bc-fail"),
		KytError:       errors.New("kyt-fail"),
	})
	_, err := s.Authorize(context.Background(), PaymentAuthorizeRequest{TxID: "t"})
	if err == nil || err.Error() != "auth-fail" {
		t.Fatalf("authorize err: %v", err)
	}
	_, err = s.Capture(context.Background(), PaymentCaptureRequest{TxID: "t"})
	if err == nil || err.Error() != "cap-fail" {
		t.Fatalf("capture err: %v", err)
	}
	_, err = s.Screen(context.Background(), KytRequest{TxID: "t"})
	if err == nil || err.Error() != "kyt-fail" {
		t.Fatalf("kyt err: %v", err)
	}
	_, err = s.Sign(context.Background(), MpcSignRequest{TxID: "t"})
	if err == nil || err.Error() != "sign-fail" {
		t.Fatalf("sign err: %v", err)
	}
	_, err = s.Broadcast(context.Background(), BroadcastRequest{TxID: "t"})
	if err == nil || err.Error() != "bc-fail" {
		t.Fatalf("broadcast err: %v", err)
	}
	_, err = s.PostDoubleEntry(context.Background(), LedgerPostRequest{TxID: "t"})
	if err == nil || err.Error() != "ldg-fail" {
		t.Fatalf("ledger err: %v", err)
	}
	// Status does not error and returns confirmed=false by default.
	resp, err := s.Status(context.Background(), "0xh")
	if err != nil || resp.TxHash != "0xh" {
		t.Fatalf("status: %+v %v", resp, err)
	}
}

func TestStubVoidRefundAudit(t *testing.T) {
	s := NewStub(DefaultStubConfig())
	if err := s.VoidAuthorization(context.Background(), PaymentVoidRequest{TxID: "t"}); err != nil {
		t.Fatalf("void: %v", err)
	}
	if s.VoidCalls != 1 {
		t.Fatalf("void calls: %d", s.VoidCalls)
	}
	if err := s.Refund(context.Background(), PaymentRefundRequest{TxID: "t"}); err != nil {
		t.Fatalf("refund: %v", err)
	}
	if s.RefundCalls != 1 {
		t.Fatalf("refund calls: %d", s.RefundCalls)
	}
	if err := s.Record(context.Background(), AuditEvent{TxID: "t"}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if s.AuditCalls != 1 {
		t.Fatalf("audit calls: %d", s.AuditCalls)
	}
	// Happy-path ids.
	s.Authorize(context.Background(), PaymentAuthorizeRequest{TxID: "txA"})
	s.Capture(context.Background(), PaymentCaptureRequest{TxID: "txA", AuthID: "a"})
	s.PostDoubleEntry(context.Background(), LedgerPostRequest{TxID: "txA"})
	if s.LastAuthID() == "" || s.LastCaptureID() == "" || s.LastJournalID() == "" {
		t.Fatal("expected last ids set")
	}
}

func TestStubPolicyDecisionValues(t *testing.T) {
	s := NewStub(StubConfig{PolicyDecision: PolicyDeny})
	resp, err := s.Evaluate(context.Background(), PolicyRequest{TxID: "t"})
	if err != nil || resp.Decision != PolicyDeny {
		t.Fatalf("expected deny, got %+v %v", resp, err)
	}
}

func TestStubKytDecisionValues(t *testing.T) {
	for _, d := range []KytDecision{KytClear, KytReview, KytReject} {
		s := NewStub(StubConfig{KytDecision: d})
		resp, err := s.Screen(context.Background(), KytRequest{TxID: "t"})
		if err != nil || resp.Decision != d {
			t.Fatalf("expected %s, got %+v %v", d, resp, err)
		}
	}
}