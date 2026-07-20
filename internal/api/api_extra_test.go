package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/quotelocker"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func TestNewServiceNilLockerUsesNoop(t *testing.T) {
	svc := NewService(store.NewMemStore(), nil)
	if svc.Locker == nil {
		t.Fatal("expected non-nil noop locker when nil passed")
	}
	// Verify it is a noop locker by acquiring and releasing.
	rel, ok, err := svc.Locker.Acquire(context.Background(), "q")
	if err != nil || !ok {
		t.Fatalf("noop acquire: ok=%v err=%v", ok, err)
	}
	rel()
}

func TestRetryEndpointNilControlReturns202(t *testing.T) {
	svc := NewService(store.NewMemStore(), quotelocker.NewNoop())
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions/tx-any/retry", strings.NewReader(`{"step":"policy"}`))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for nil control, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCompensateEndpointNilControlReturns202(t *testing.T) {
	svc := NewService(store.NewMemStore(), quotelocker.NewNoop())
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions/tx-any/compensate", nil)
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for nil control, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRetryEndpointMissingStepAndSagaLoadError(t *testing.T) {
	// With Control set but no step in body and no saga state -> 400.
	svc := NewService(store.NewMemStore(), quotelocker.NewNoop())
	svc.Control = &fakeControl{}
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions/missing/retry", strings.NewReader(`{}`))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRetryEndpointMissingStepUsesSagaCurrentStep(t *testing.T) {
	s := store.NewMemStore()
	svc := NewService(s, quotelocker.NewNoop())
	fc := &fakeControl{}
	svc.Control = fc
	h := Mux(svc)
	// Seed a tx.
	body := `{"user_id":"u1","quote_id":"q1","amount":"100","asset":"BTC","rail":"CARD","dest_address":"0xabc"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	var resp CreateTxResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	// Retry with empty body — should fall back to saga.CurrentStep (policy).
	r = httptest.NewRequest(http.MethodPost, "/v1/transactions/"+resp.TxID+"/retry", strings.NewReader(`{}`))
	r = r.WithContext(reqWithLog(context.Background()))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	if fc.retried[resp.TxID] != "POLICY" {
		t.Fatalf("expected retry for POLICY from saga state, got %q", fc.retried[resp.TxID])
	}
}