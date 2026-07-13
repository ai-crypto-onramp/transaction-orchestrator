package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

// LedgerStep implements Step 6 — ledger.post.
//
// Compensation: per the README, ledger posting is retried; no on-chain
// compensation is possible post-broadcast.  On permanent failure the saga
// parks in failed and a ledger.reconcile_required outbox event is emitted so
// an out-of-band job can reconcile.
type LedgerStep struct{ client partner.Ledger }

// NewLedgerStep returns a LedgerStep bound to client.
func NewLedgerStep(client partner.Ledger) *LedgerStep { return &LedgerStep{client: client} }

// Name returns "ledger".
func (l *LedgerStep) Name() statemachine.Step { return statemachine.StepLedger }

// Execute posts a double-entry to the ledger and transitions to ledgered,
// then to completed via the executor's outer loop.
func (l *LedgerStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	if l.client == nil {
		return StepResult{}, errors.New("ledger client not configured")
	}
	// Idempotency: if we already have a journal id, skip.
	if jid, _ := sc.Saga.Payload["ledger_journal_id"].(string); jid != "" {
		return StepResult{
			State:        statemachine.StateLedgered,
			PayloadMerge: map[string]any{"ledger_journal_id": jid},
		}, nil
	}
	resp, err := l.client.PostDoubleEntry(ctx, partner.LedgerPostRequest{
		TxID: sc.Tx.TxID, UserID: sc.Tx.UserID, Amount: sc.Tx.Amount,
		Asset: sc.Tx.Asset, Rail: sc.Tx.Rail,
	})
	if err != nil {
		if errors.Is(err, partner.ErrTransient) {
			return StepResult{}, err
		}
		// Non-retriable ledger failure post-broadcast: park with reconcile.
		return StepResult{
			State:    statemachine.StateFailed,
			Terminal: true,
			PayloadMerge: map[string]any{"ledger_reconcile_required": true},
		}, fmt.Errorf("ledger post failed: %w", err)
	}
	return StepResult{
		State: statemachine.StateLedgered,
		PayloadMerge: map[string]any{"ledger_journal_id": resp.JournalID},
	}, nil
}

// Compensate is a no-op (per README).  Reconciliation is handled async by an
// out-of-band job consuming the ledger.reconcile_required outbox event.
func (l *LedgerStep) Compensate(ctx context.Context, sc *SagaContext) error { return nil }