// Package saga implements the orchestration core: the Step interface, the
// SagaContext passed to each step, the executor that drives a single
// transaction through its steps with bounded retry + compensation cascade,
// and the six concrete step implementations.
//
// The executor is designed to be deterministic and unit-testable: every
// external dependency (Store, partner clients, lease manager, audit) is
// injected.  The in-process worker (internal/worker) wraps this executor and
// handles dispatch/recovery; see that package for the scheduler.
package saga

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

// StepResult is returned by Step.Execute.
type StepResult struct {
	// State is the new saga state to persist on success.  Must be a legal
	// forward transition from the current saga state.
	State statemachine.State
	// PayloadMerge is merged into saga_state.payload on success (key-value).
	PayloadMerge map[string]any
	// Terminal marks a terminal outcome (e.g. failed_compensated) — no
	// further forward steps will be run.
	Terminal bool
}

// SagaContext is passed to every step.  It carries the durable tx record,
// the current saga snapshot, the current attempt number, and the partner
// clients.
type SagaContext struct {
	Tx       store.Transaction
	Saga     store.SagaState
	Attempt  int
	Partners *Clients
}

// Clients bundles the six partner clients used by steps.
type Clients struct {
	Policy     partner.Policy
	Payment    partner.Payment
	Kyt        partner.Kyt
	Mpc        partner.Mpc
	Blockchain partner.Blockchain
	Ledger     partner.Ledger
	Audit      partner.Audit
}

// Step is one saga step.  Implementations must be safe for concurrent use.
type Step interface {
	// Name returns the step name (e.g. "policy").
	Name() statemachine.Step
	// Execute runs the forward action.  Returning a non-nil error triggers a
	// retry (if recoverable) or compensation (if not).
	Execute(ctx context.Context, sc *SagaContext) (StepResult, error)
	// Compensate runs the compensating action.  A nil return indicates
	// compensation succeeded; an error indicates compensation failed and
	// the saga moves to StateFailed (needs manual ops).
	Compensate(ctx context.Context, sc *SagaContext) error
}

// ErrNonRetriable wraps a partner error to signal "do not retry, compensate
// now".  ErrDenied and ErrTransient are recognized directly by the executor.
var ErrNonRetriable = errors.New("saga: non-retriable")

// NonRetriable wraps err so the executor skips retries.
func NonRetriable(err error) error { return fmt.Errorf("%w: %v", ErrNonRetriable, err) }

// IsNonRetriable reports whether err should skip the retry path.
func IsNonRetriable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNonRetriable) {
		return true
	}
	if errors.Is(err, partner.ErrDenied) {
		return true
	}
	return false
}

// Config tunes the executor.
type Config struct {
	MaxRetries        int
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
	StepTimeout       func(step string) time.Duration
}

// DefaultConfig returns a sane default Config.
func DefaultConfig() Config {
	return Config{
		MaxRetries:  5,
		BaseBackoff: 200 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
		StepTimeout: func(string) time.Duration { return 30 * time.Second },
	}
}

// Executor runs the saga for a single tx.  It is created per-tx and is not
// safe to reuse concurrently.
type Executor struct {
	Store    store.Store
	Clients  *Clients
	Steps    []Step            // ordered; usually the six canonical steps
	Cfg      Config
	Lease    LeaseManager
}

// LeaseManager is the abstract lease manager used to guarantee single-flight
// step execution.  The no-op default is fine for single-replica tests.
type LeaseManager interface {
	// Acquire returns a release function on success.  ok=false means another
	// owner holds the lease.
	Acquire(ctx context.Context, txID, owner string, ttl time.Duration) (release func(), ok bool, err error)
}

// NoopLease is a LeaseManager that always succeeds.
type NoopLease struct{}

// Acquire always returns ok=true.
func (NoopLease) Acquire(ctx context.Context, txID, owner string, ttl time.Duration) (func(), bool, error) {
	return func() {}, true, nil
}

// NewExecutor returns an Executor with the canonical step order.
func NewExecutor(s store.Store, c *Clients, cfg Config) *Executor {
	return &Executor{
		Store: s, Clients: c, Cfg: cfg, Lease: NoopLease{},
		Steps: []Step{
			NewPolicyStep(c.Policy),
			NewPaymentStep(c.Payment),
			NewKytStep(c.Kyt),
			NewMpcSignStep(c.Mpc),
			NewBroadcastStep(c.Blockchain),
			NewLedgerStep(c.Ledger),
		},
	}
}

// StepByName returns the registered step with the given name, or nil.
func (e *Executor) StepByName(name statemachine.Step) Step {
	for _, s := range e.Steps {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

// Run drives the saga from the current saga state to a terminal state.  It
// is idempotent: re-running after a partial completion resumes from the last
// persisted step.
func (e *Executor) Run(ctx context.Context, txID, owner string) error {
	log := logging.From(ctx)
	for {
		tx, err := e.Store.LoadTx(ctx, txID)
		if err != nil {
			return err
		}
		sg, err := e.Store.LoadSagaState(ctx, txID)
		if err != nil {
			return err
		}
		if sg.State.Terminal() {
			log.Info("saga terminal", "tx_id", txID, "state", sg.State)
			return nil
		}
		step := e.StepByName(sg.CurrentStep)
		if step == nil {
			return fmt.Errorf("saga: no step for current_step=%s", sg.CurrentStep)
		}

		// Acquire lease for this step.
		release, ok, err := e.Lease.Acquire(ctx, txID, owner, e.Cfg.StepTimeout(string(step.Name()))+5*time.Second)
		if err != nil {
			return err
		}
		if !ok {
			log.Info("lease held by another owner; backing off", "tx_id", txID, "step", step.Name())
			release()
			select {
			case <-time.After(e.Cfg.BaseBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		err = e.runStepWithRetry(ctx, step, &SagaContext{
			Tx: tx, Saga: sg, Attempt: 1, Partners: e.Clients,
		})
		release()
		if err != nil {
			log.Error("step failed permanently; compensating", "tx_id", txID, "step", step.Name(), "err", err)
			if compErr := e.compensateCascade(ctx, txID, owner, step); compErr != nil {
				log.Error("compensation failed", "tx_id", txID, "err", compErr)
				return compErr
			}
			return nil
		}
		// Re-check state; if terminal (e.g. policy deny -> failed_compensated
		// inside the step), exit.
		sg2, _ := e.Store.LoadSagaState(ctx, txID)
		if sg2.State.Terminal() {
			return nil
		}
		// If the saga is in StateBroadcasted and the next step is ledger, we
		// need to advance to StateConfirmed first — either via the gateway
		// status poll or by waiting on an external confirm signal.  We poll
		// inline here; a separate ConfirmPoller could also be used.
		if sg2.State == statemachine.StateBroadcasted {
			if err := e.advanceToConfirmed(ctx, txID); err != nil {
				log.Warn("advance to confirmed failed; leaving saga in broadcasted", "tx_id", txID, "err", err)
			}
		}
	}
}

// advanceToConfirmed polls the blockchain-gateway Status endpoint for the
// recorded tx_hash and transitions the saga to StateConfirmed when the
// gateway reports the tx confirmed.
func (e *Executor) advanceToConfirmed(ctx context.Context, txID string) error {
	if e.Clients == nil || e.Clients.Blockchain == nil {
		return nil
	}
	sg, err := e.Store.LoadSagaState(ctx, txID)
	if err != nil {
		return err
	}
	txHash, _ := sg.Payload["tx_hash"].(string)
	if txHash == "" {
		return errors.New("advance: missing tx_hash")
	}
	pollCtx, cancel := context.WithTimeout(ctx, e.Cfg.StepTimeout("broadcast"))
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		st, err := e.Clients.Blockchain.Status(pollCtx, txHash)
		if err == nil && st.Confirmed {
			return e.persistConfirmedInline(pollCtx, txID, sg)
		}
		select {
		case <-tick.C:
		case <-pollCtx.Done():
			return pollCtx.Err()
		}
	}
}

func (e *Executor) persistConfirmedInline(ctx context.Context, txID string, sg store.SagaState) error {
	tx, err := e.Store.LoadTx(ctx, txID)
	if err != nil {
		return err
	}
	return e.Store.RunInTx(ctx, func(ts store.TxStore) error {
		newSaga := sg
		newSaga.State = statemachine.StateConfirmed
		newSaga.CurrentStep = statemachine.StepLedger
		newSaga.Version = sg.Version + 1
		if _, terr := statemachine.Transition(sg.State, newSaga.State); terr != nil {
			return terr
		}
		if err := ts.SaveSagaState(ctx, newSaga); err != nil {
			return err
		}
		if err := ts.UpdateTransactionStatus(ctx, txID, newSaga.State, tx.Version); err != nil {
			return err
		}
		evtType := "transaction.confirmed"
		_ = ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: txID, EventType: evtType,
			Status: store.OutboxPending, DedupKey: store.DedupKey(txID, evtType, "", 0),
			CreatedAt: time.Now().UTC(),
			Payload:   map[string]any{"tx_id": txID, "tx_hash": sg.Payload["tx_hash"]},
		}})
		return nil
	})
}

// runStepWithRetry wraps Execute with bounded retry + backoff and persists
// the resulting step row + saga transition + outbox event atomically.
func (e *Executor) runStepWithRetry(ctx context.Context, step Step, sc *SagaContext) error {
	log := logging.From(ctx)
	var lastErr error
	for attempt := 1; attempt <= e.Cfg.MaxRetries; attempt++ {
		sc.Attempt = attempt
		if err := e.runStepOnce(ctx, step, sc); err != nil {
			lastErr = err
			if IsNonRetriable(err) {
				return err
			}
			log.Warn("step attempt failed; backing off", "tx_id", sc.Tx.TxID, "step", step.Name(), "attempt", attempt, "err", err)
			backoff := e.backoff(attempt)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("saga: step %s exhausted retries: %w", step.Name(), lastErr)
}

func (e *Executor) runStepOnce(ctx context.Context, step Step, sc *SagaContext) error {
	stepCtx, cancel := context.WithTimeout(ctx, e.Cfg.StepTimeout(string(step.Name())))
	defer cancel()

	// Idempotency: if a succeeded row already exists for this attempt, the
	// step has already been executed — skip without re-calling the partner.
	var alreadySucceeded bool
	if err := e.Store.RunInTx(stepCtx, func(ts store.TxStore) error {
		existing, err := ts.LoadStep(stepCtx, sc.Tx.TxID, step.Name(), sc.Attempt)
		if err == nil && existing.Status == store.StepSucceeded {
			alreadySucceeded = true
			return nil
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		now := time.Now().UTC()
		row := store.StepRow{
			TxID: sc.Tx.TxID, StepName: step.Name(), Status: store.StepRunning,
			Attempt: sc.Attempt, StartedAt: &now,
			IdempotencyKey: store.IdempotencyKey(sc.Tx.TxID, step.Name(), sc.Attempt),
		}
		if err == nil {
			return ts.UpdateStep(stepCtx, row)
		}
		return ts.InsertStep(stepCtx, row)
	}); err != nil {
		return err
	}
	if alreadySucceeded {
		return nil
	}

	res, err := step.Execute(stepCtx, sc)
	finished := time.Now().UTC()
	if err != nil {
		// Terminal-with-error: the step produced a terminal state (e.g. policy
		// deny -> failed_compensated, or ledger failure -> failed) that should
		// be persisted, and the saga should NOT be compensated again by the
		// executor.  We persist the terminal result and swallow the error.
		if res.Terminal {
			_ = e.persistTerminal(stepCtx, step, sc, res, finished)
			return nil
		}
		row := store.StepRow{
			TxID: sc.Tx.TxID, StepName: step.Name(), Status: store.StepFailed,
			Attempt: sc.Attempt, FinishedAt: &finished, Error: err.Error(),
			IdempotencyKey: store.IdempotencyKey(sc.Tx.TxID, step.Name(), sc.Attempt),
		}
		_ = e.Store.RunInTx(stepCtx, func(ts store.TxStore) error { return ts.UpdateStep(stepCtx, row) })
		e.audit(stepCtx, sc, string(step.Name()), string(sc.Saga.State), string(res.State), err)
		return err
	}

	// Persist success: update step, saga state, outbox event in one Tx.
	return e.Store.RunInTx(stepCtx, func(ts store.TxStore) error {
		row := store.StepRow{
			TxID: sc.Tx.TxID, StepName: step.Name(), Status: store.StepSucceeded,
			Attempt: sc.Attempt, FinishedAt: &finished,
			IdempotencyKey: store.IdempotencyKey(sc.Tx.TxID, step.Name(), sc.Attempt),
		}
		if err := ts.UpdateStep(stepCtx, row); err != nil {
			return err
		}
		// Merge payload.
		newSaga := sc.Saga
		newSaga.Payload = mergePayload(sc.Saga.Payload, res.PayloadMerge)
		newSaga.State = res.State
		if res.Terminal {
			// terminal state — no next step
		} else if next, ok := statemachine.StepAfter(step.Name()); ok {
			newSaga.CurrentStep = next
		} else {
			// last step in the canonical order — if the step returned its
			// own success state (e.g. ledgered), auto-advance to completed.
			if newSaga.State == statemachine.StateLedgered {
				newSaga.State = statemachine.StateCompleted
			}
			newSaga.CurrentStep = step.Name()
		}
		newSaga.Version = sc.Saga.Version + 1
		// Validate the forward path.  A single hop must be legal; a two-hop
		// auto-advance (e.g. confirmed -> ledgered -> completed) is allowed
		// if each hop is individually legal.
		if err := validatePath(sc.Saga.State, res.State, newSaga.State); err != nil {
			return fmt.Errorf("saga: illegal transition %s -> %s: %w", sc.Saga.State, newSaga.State, err)
		}
		if err := ts.SaveSagaState(stepCtx, newSaga); err != nil {
			return err
		}
		if err := ts.UpdateTransactionStatus(stepCtx, sc.Tx.TxID, newSaga.State, sc.Tx.Version); err != nil {
			return err
		}
		evtType := fmt.Sprintf("step.%s.succeeded", step.Name())
		_ = ts.AppendOutbox(stepCtx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: sc.Tx.TxID, EventType: evtType,
			Step: string(step.Name()), Attempt: sc.Attempt, Status: store.OutboxPending,
			DedupKey: store.DedupKey(sc.Tx.TxID, evtType, string(step.Name()), sc.Attempt),
			CreatedAt: finished,
			Payload: map[string]any{
				"tx_id": sc.Tx.TxID, "step": string(step.Name()),
				"attempt": sc.Attempt, "state": string(newSaga.State),
			},
		}})
		return nil
	})
}

// persistTerminal writes the terminal state, failed step row, and outbox event
// for a step that returned a terminal result (res.Terminal == true).  Used by
// runStepOnce for policy-deny / ledger-failure paths where the executor must
// NOT subsequently run the compensation cascade.
func (e *Executor) persistTerminal(ctx context.Context, step Step, sc *SagaContext, res StepResult, finished time.Time) error {
	return e.Store.RunInTx(ctx, func(ts store.TxStore) error {
		row := store.StepRow{
			TxID: sc.Tx.TxID, StepName: step.Name(), Status: store.StepFailed,
			Attempt: sc.Attempt, FinishedAt: &finished,
			IdempotencyKey: store.IdempotencyKey(sc.Tx.TxID, step.Name(), sc.Attempt),
		}
		_ = ts.UpdateStep(ctx, row)
		newSaga := sc.Saga
		newSaga.Payload = mergePayload(sc.Saga.Payload, res.PayloadMerge)
		newSaga.State = res.State
		newSaga.Version = sc.Saga.Version + 1
		if _, terr := statemachine.Transition(sc.Saga.State, newSaga.State); terr != nil {
			return fmt.Errorf("saga: illegal terminal transition %s -> %s: %w", sc.Saga.State, newSaga.State, terr)
		}
		_ = ts.SaveSagaState(ctx, newSaga)
		_ = ts.UpdateTransactionStatus(ctx, sc.Tx.TxID, newSaga.State, sc.Tx.Version)
		evtType := "transaction." + string(newSaga.State)
		_ = ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: sc.Tx.TxID, EventType: evtType,
			Step: string(step.Name()), Attempt: sc.Attempt, Status: store.OutboxPending,
			DedupKey: store.DedupKey(sc.Tx.TxID, evtType, string(step.Name()), sc.Attempt),
			CreatedAt: finished,
			Payload: map[string]any{
				"tx_id": sc.Tx.TxID, "step": string(step.Name()),
				"attempt": sc.Attempt, "state": string(newSaga.State),
			},
		}})
		return nil
	})
}

// compensateCascade runs Compensate on every completed step in reverse order,
// starting from the failed step.  The saga ends in failed_compensated (all
// compensations ok) or failed (any compensation failed).
func (e *Executor) compensateCascade(ctx context.Context, txID, owner string, failedStep Step) error {
	log := logging.From(ctx)
	sg, err := e.Store.LoadSagaState(ctx, txID)
	if err != nil {
		return err
	}
	tx, err := e.Store.LoadTx(ctx, txID)
	if err != nil {
		return err
	}
	// Walk the registered steps in reverse, from failedStep back to the first.
	startIdx := -1
	for i, s := range e.Steps {
		if s.Name() == failedStep.Name() {
			startIdx = i
			break
		}
	}
	if startIdx == -1 {
		return fmt.Errorf("saga: failed step %s not in registered order", failedStep.Name())
	}
	var compErr error
	for i := startIdx; i >= 0; i-- {
		step := e.Steps[i]
		sc := &SagaContext{Tx: tx, Saga: sg, Attempt: 1, Partners: e.Clients}
		// Mark compensating in step row.
		_ = e.Store.RunInTx(ctx, func(ts store.TxStore) error {
			row, err := ts.LoadStep(ctx, txID, step.Name(), 1)
			if err == nil {
				row.Status = store.StepCompensating
				_ = ts.UpdateStep(ctx, row)
			}
			return nil
		})
		log.Info("compensating step", "tx_id", txID, "step", step.Name())
		if err := step.Compensate(ctx, sc); err != nil {
			log.Error("compensation error", "tx_id", txID, "step", step.Name(), "err", err)
			compErr = err
			_ = e.Store.RunInTx(ctx, func(ts store.TxStore) error {
				row, _ := ts.LoadStep(ctx, txID, step.Name(), 1)
				if row.TxID != "" {
					row.Status = store.StepFailed
					row.Error = "compensation: " + err.Error()
					_ = ts.UpdateStep(ctx, row)
				}
				return nil
			})
			break
		}
		_ = e.Store.RunInTx(ctx, func(ts store.TxStore) error {
			row, _ := ts.LoadStep(ctx, txID, step.Name(), 1)
			if row.TxID != "" {
				row.Status = store.StepCompensated
				_ = ts.UpdateStep(ctx, row)
			}
			return nil
		})
	}
	// Transition saga to failed_compensated or failed.
	target := statemachine.StateFailedCompensated
	if compErr != nil {
		target = statemachine.StateFailed
	}
	_ = e.Store.RunInTx(ctx, func(ts store.TxStore) error {
		sg.Version = sg.Version + 1
		sg.State = target
		_ = ts.SaveSagaState(ctx, sg)
		_ = ts.UpdateTransactionStatus(ctx, txID, target, tx.Version)
		evtType := "transaction." + string(target)
		_ = ts.AppendOutbox(ctx, []store.OutboxEvent{{
			EventID: store.NewEventID(), TxID: txID, EventType: evtType,
			Status: store.OutboxPending, DedupKey: store.DedupKey(txID, evtType, "", 0),
			CreatedAt: time.Now().UTC(),
			Payload: map[string]any{"tx_id": txID, "state": string(target)},
		}})
		return nil
	})
	return compErr
}

func (e *Executor) backoff(attempt int) time.Duration {
	base := e.Cfg.BaseBackoff
	max := e.Cfg.MaxBackoff
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	d := float64(base) * math.Pow(2, float64(attempt-1))
	if d > float64(max) {
		d = float64(max)
	}
	jitter := time.Duration(rand.Int63n(int64(d / 2)))
	return time.Duration(d) + jitter
}

func (e *Executor) audit(ctx context.Context, sc *SagaContext, step, before, after string, err error) {
	if e.Clients == nil || e.Clients.Audit == nil {
		return
	}
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	_ = e.Clients.Audit.Record(ctx, partner.AuditEvent{
		TxID: sc.Tx.TxID, Step: step, Attempt: sc.Attempt,
		Before: before, After: after, Err: errStr, At: time.Now().UTC(), Actor: "orchestrator",
	})
}

func mergePayload(base, merge map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(merge))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range merge {
		out[k] = v
	}
	return out
}

// validatePath checks that the saga can move from `from` to `to`, possibly via
// the intermediate state `mid` (if mid != to and mid != from).  Returns nil
// if the path is legal, an error otherwise.
func validatePath(from, mid, to statemachine.State) error {
	if from == to {
		return nil
	}
	if mid == to || mid == from {
		if _, err := statemachine.Transition(from, to); err != nil {
			return err
		}
		return nil
	}
	// Two-hop: from -> mid -> to.
	if _, err := statemachine.Transition(from, mid); err != nil {
		return err
	}
	if _, err := statemachine.Transition(mid, to); err != nil {
		return err
	}
	return nil
}