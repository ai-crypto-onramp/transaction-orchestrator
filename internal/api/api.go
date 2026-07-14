// Package api implements the public REST API for the Transaction Orchestrator.
//
// Endpoints:
//   POST   /v1/transactions          — create a tx and its initial saga rows
//   GET    /v1/transactions/:id      — full state + current step + history
//   GET    /v1/transactions/:id/steps — ordered step list
//   POST   /v1/transactions/:id/retry     — force-retry failed step (Stage 8)
//   POST   /v1/transactions/:id/compensate — manual compensation (Stage 8)
//   GET    /healthz /readyz
//
// All handler methods take a *Service which owns the Store, QuoteLocker, and
// logger, keeping the HTTP layer thin and unit-testable.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/quotelocker"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
	"github.com/google/uuid"
)

// Service bundles the dependencies used by the HTTP handlers.
type Service struct {
	Store  store.Store
	Locker quotelocker.Locker
	// Control, if set, handles retry/compensate operations.  When nil the
	// retry/compensate endpoints return 202 with a status note (Stage 8 hook).
	Control Control
}

// Control is the saga control plane used by the retry/compensate endpoints.
type Control interface {
	// Retry force-retries the named step for txID.  Idempotent.
	Retry(ctx context.Context, txID string, step statemachine.Step) error
	// Compensate triggers the compensation cascade for txID.
	Compensate(ctx context.Context, txID string) error
}

// NewService returns a Service.
func NewService(s store.Store, l quotelocker.Locker) *Service {
	if l == nil {
		l = quotelocker.NewNoop()
	}
	return &Service{Store: s, Locker: l}
}

// --- DTOs --------------------------------------------------------------------

type CreateTxRequest struct {
	UserID      string `json:"user_id"`
	QuoteID     string `json:"quote_id"`
	Amount      string `json:"amount"`
	Asset       string `json:"asset"`
	Rail        string `json:"rail"`
	DestAddress string `json:"dest_address"`
}

type CreateTxResponse struct {
	TxID string `json:"tx_id"`
}

type TxResponse struct {
	TxID        string             `json:"tx_id"`
	UserID      string             `json:"user_id"`
	QuoteID     string             `json:"quote_id"`
	Amount      string             `json:"amount"`
	Asset       string             `json:"asset"`
	Rail        string             `json:"rail"`
	DestAddress string             `json:"dest_address"`
	Status      string             `json:"status"`
	CurrentStep string             `json:"current_step"`
	State       string             `json:"state"`
	Version     int64              `json:"version"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	Steps       []StepRowResponse  `json:"steps"`
}

type StepRowResponse struct {
	StepName       string     `json:"step_name"`
	Status         string     `json:"status"`
	Attempt        int        `json:"attempt"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Error          string     `json:"error,omitempty"`
	IdempotencyKey string     `json:"idempotency_key"`
}

type errorBody struct{ Error string `json:"error"` }

// --- Handlers ----------------------------------------------------------------

// Mux returns the HTTP mux bound to svc.
func Mux(svc *Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/readyz", readyz)
	mux.HandleFunc("POST /v1/transactions", svc.handleCreate)
	mux.HandleFunc("GET /v1/transactions/{id}", svc.handleGet)
	mux.HandleFunc("GET /v1/transactions/{id}/steps", svc.handleSteps)
	mux.HandleFunc("POST /v1/transactions/{id}/retry", svc.handleRetry)
	mux.HandleFunc("POST /v1/transactions/{id}/compensate", svc.handleCompensate)
	return withRequestID(mux)
}

func healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func readyz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Service) handleCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logging.From(ctx)

	var req CreateTxRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if err := validateCreate(req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	release, ok, err := s.Locker.Acquire(ctx, req.QuoteID)
	if err != nil {
		log.Error("quote lock error", "err", err, "quote_id", req.QuoteID)
		writeErr(w, http.StatusInternalServerError, "quote lock error")
		return
	}
	if !ok {
		writeErr(w, http.StatusConflict, "quote already in use")
		return
	}
	defer release()

	txID := uuid.NewString()
	now := time.Now().UTC()
	tx := store.Transaction{
		TxID: txID, UserID: req.UserID, QuoteID: req.QuoteID, Amount: req.Amount,
		Asset: req.Asset, Rail: req.Rail, DestAddress: req.DestAddress,
		Status: statemachine.StateCreated, CreatedAt: now, UpdatedAt: now, Version: 1,
	}
	var steps []store.StepRow
	for _, st := range statemachine.StepOrder {
		steps = append(steps, store.StepRow{
			TxID: txID, StepName: st, Status: store.StepPending, Attempt: 1,
			IdempotencyKey: store.IdempotencyKey(txID, st, 1),
		})
	}
	saga := store.SagaState{
		TxID: txID, CurrentStep: statemachine.StepPolicy, State: statemachine.StateCreated,
		Payload: map[string]any{}, Version: 1,
	}
	events := []store.OutboxEvent{{
		EventID: store.NewEventID(), TxID: txID, EventType: "transaction.created",
		Status: store.OutboxPending, DedupKey: store.DedupKey(txID, "transaction.created", "", 0),
		CreatedAt: now, Payload: map[string]any{"tx_id": txID, "user_id": req.UserID, "quote_id": req.QuoteID},
	}}

	if err := s.Store.RunInTx(ctx, func(ts store.TxStore) error {
		return ts.CreateTx(ctx, tx, steps, saga, events)
	}); err != nil {
		log.Error("create tx", "err", err, "tx_id", txID)
		if errors.Is(err, store.ErrDuplicate) {
			writeErr(w, http.StatusConflict, "tx already exists")
			return
		}
		writeErr(w, http.StatusInternalServerError, "create tx failed")
		return
	}

	log.Info("transaction.created", "tx_id", txID, "user_id", req.UserID)
	writeJSON(w, http.StatusCreated, CreateTxResponse{TxID: txID})
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	txID := r.PathValue("id")
	tx, err := s.Store.LoadTx(ctx, txID)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "load tx failed")
		return
	}
	sg, err := s.Store.LoadSagaState(ctx, txID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusInternalServerError, "load saga failed")
		return
	}
	steps, err := s.Store.ListSteps(ctx, txID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "load steps failed")
		return
	}
	resp := TxResponse{
		TxID: tx.TxID, UserID: tx.UserID, QuoteID: tx.QuoteID, Amount: tx.Amount,
		Asset: tx.Asset, Rail: tx.Rail, DestAddress: tx.DestAddress,
		Status: string(tx.Status), Version: tx.Version,
		CreatedAt: tx.CreatedAt, UpdatedAt: tx.UpdatedAt,
		CurrentStep: string(sg.CurrentStep), State: string(sg.State),
		Steps: toStepResponses(steps),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleSteps(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	txID := r.PathValue("id")
	if _, err := s.Store.LoadTx(ctx, txID); errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, "load tx failed")
		return
	}
	steps, err := s.Store.ListSteps(ctx, txID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "load steps failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"steps": toStepResponses(steps)})
}

// handleRetry force-retries a failed step.  When Control is nil it returns 202
// with a status note (Stage 8 hook for backward compatibility).
func (s *Service) handleRetry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	txID := r.PathValue("id")
	if s.Control == nil {
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "retry-accepted"})
		return
	}
	var body struct {
		Step string `json:"step"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	step := statemachine.Step(body.Step)
	if step == "" {
		sg, err := s.Store.LoadSagaState(ctx, txID)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "missing --step or current saga state")
			return
		}
		step = sg.CurrentStep
	}
	if err := s.Control.Retry(ctx, txID, step); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "retry-accepted", "step": string(step)})
}

func (s *Service) handleCompensate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	txID := r.PathValue("id")
	if s.Control == nil {
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "compensate-accepted"})
		return
	}
	if err := s.Control.Compensate(ctx, txID); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "compensate-accepted"})
}

// --- helpers -----------------------------------------------------------------

func validateCreate(r CreateTxRequest) error {
	var missing []string
	for k, v := range map[string]string{
		"user_id": r.UserID, "quote_id": r.QuoteID, "amount": r.Amount,
		"asset": r.Asset, "rail": r.Rail, "dest_address": r.DestAddress,
	} {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func toStepResponses(rows []store.StepRow) []StepRowResponse {
	out := make([]StepRowResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, StepRowResponse{
			StepName:       string(r.StepName),
			Status:         string(r.Status),
			Attempt:        r.Attempt,
			StartedAt:      r.StartedAt,
			FinishedAt:     r.FinishedAt,
			Error:          r.Error,
			IdempotencyKey: r.IdempotencyKey,
		})
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errorBody{Error: msg})
}

// --- request id middleware ---------------------------------------------------

type reqIDKey struct{}

// WithRequestID stores the id in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, reqIDKey{}, id)
}

// RequestIDFrom returns the request id stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(reqIDKey{}).(string); ok {
		return v
	}
	return ""
}

func withRequestID(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		h.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
}