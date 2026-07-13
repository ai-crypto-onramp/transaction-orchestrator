package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/quotelocker"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

func newTestService(t *testing.T) (*Service, *store.MemStore) {
	t.Helper()
	s := store.NewMemStore()
	return NewService(s, quotelocker.NewNoop()), s
}

func reqWithLog(ctx context.Context) context.Context {
	return logging.WithLogger(ctx, logging.New("debug"))
}

func TestHealthAndReady(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	for _, p := range []string{"/healthz", "/readyz"} {
		r := httptest.NewRequest(http.MethodGet, p, nil).WithContext(reqWithLog(context.Background()))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", p, rec.Code)
		}
	}
}

func TestCreateTxHappyPath(t *testing.T) {
	svc, s := newTestService(t)
	h := Mux(svc)

	body := `{"user_id":"u1","quote_id":"q1","amount":"100","asset":"BTC","rail":"card","dest_address":"0xabc"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp CreateTxResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TxID == "" {
		t.Fatal("expected non-empty tx_id")
	}

	ctx := context.Background()
	tx, err := s.LoadTx(ctx, resp.TxID)
	if err != nil {
		t.Fatalf("LoadTx: %v", err)
	}
	if tx.Status != statemachine.StateCreated {
		t.Fatalf("expected created, got %s", tx.Status)
	}
	steps, _ := s.ListSteps(ctx, resp.TxID)
	if len(steps) != 6 {
		t.Fatalf("expected 6 steps, got %d", len(steps))
	}
	sg, _ := s.LoadSagaState(ctx, resp.TxID)
	if sg.State != statemachine.StateCreated || sg.CurrentStep != statemachine.StepPolicy {
		t.Fatalf("unexpected saga state: %+v", sg)
	}
	pending, _ := s.ListOutboxPending(ctx, 10)
	if len(pending) != 1 || pending[0].EventType != "transaction.created" {
		t.Fatalf("unexpected outbox: %+v", pending)
	}
}

func TestCreateTxValidationFailures(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	cases := []struct {
		name string
		body string
	}{
		{"missing fields", `{"user_id":"u1"}`},
		{"invalid json", `{not json`},
		{"empty body", ``},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(c.body))
			r = r.WithContext(reqWithLog(context.Background()))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s: expected 400, got %d body=%s", c.name, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestGetTxAndSteps(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	ctx := context.Background()

	body := `{"user_id":"u1","quote_id":"q1","amount":"100","asset":"BTC","rail":"card","dest_address":"0xabc"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	r = r.WithContext(reqWithLog(ctx))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d body=%s", rec.Code, rec.Body.String())
	}
	var resp CreateTxResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)

	// GET /v1/transactions/:id
	r = httptest.NewRequest(http.MethodGet, "/v1/transactions/"+resp.TxID, nil)
	r = r.WithContext(reqWithLog(ctx))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d body=%s", rec.Code, rec.Body.String())
	}
	var tx TxResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &tx); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if tx.TxID != resp.TxID || tx.Status != string(statemachine.StateCreated) {
		t.Fatalf("unexpected get response: %+v", tx)
	}
	if len(tx.Steps) != 6 {
		t.Fatalf("expected 6 steps in response, got %d", len(tx.Steps))
	}
	if tx.CurrentStep != string(statemachine.StepPolicy) {
		t.Fatalf("expected current_step=policy, got %s", tx.CurrentStep)
	}

	// GET /v1/transactions/:id/steps
	r = httptest.NewRequest(http.MethodGet, "/v1/transactions/"+resp.TxID+"/steps", nil)
	r = r.WithContext(reqWithLog(ctx))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("steps: %d body=%s", rec.Code, rec.Body.String())
	}
	var stepsResp struct {
		Steps []StepRowResponse `json:"steps"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &stepsResp); err != nil {
		t.Fatalf("decode steps: %v", err)
	}
	if len(stepsResp.Steps) != 6 {
		t.Fatalf("expected 6 steps, got %d", len(stepsResp.Steps))
	}
}

func TestGetTxNotFound(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodGet, "/v1/transactions/does-not-exist", nil)
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestStepsNotFound(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodGet, "/v1/transactions/nope/steps", nil)
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestRequestIDPropagation(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.Header.Set("X-Request-ID", "rid-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if got := rec.Header().Get("X-Request-ID"); got != "rid-123" {
		t.Fatalf("expected request id echoed, got %q", got)
	}
}

// failingLocker returns ok=false to simulate a locked quote.
type failingLocker struct{}

func (failingLocker) Acquire(ctx context.Context, quoteID string) (func(), bool, error) {
	return func() {}, false, nil
}

func TestCreateTxQuoteLockedConflict(t *testing.T) {
	s := store.NewMemStore()
	svc := NewService(s, failingLocker{})
	h := Mux(svc)
	body := `{"user_id":"u1","quote_id":"q1","amount":"100","asset":"BTC","rail":"card","dest_address":"0xabc"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

// errorLocker simulates an acquire error.
type errorLocker struct{}

func (errorLocker) Acquire(ctx context.Context, quoteID string) (func(), bool, error) {
	return func() {}, false, errors.New("redis down")
}

func TestCreateTxQuoteLockerError(t *testing.T) {
	s := store.NewMemStore()
	svc := NewService(s, errorLocker{})
	h := Mux(svc)
	body := `{"user_id":"u1","quote_id":"q1","amount":"100","asset":"BTC","rail":"card","dest_address":"0xabc"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(body))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

// Ensure json body buffering works for empty body.
func TestCreateTxEmptyBody(t *testing.T) {
	svc, _ := newTestService(t)
	h := Mux(svc)
	r := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(nil))
	r = r.WithContext(reqWithLog(context.Background()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty body, got %d", rec.Code)
	}
}