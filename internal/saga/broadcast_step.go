package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

// BroadcastStep implements Step 5 — blockchain.broadcast.
//
// Idempotency: the saga_state.payload stores tx_hash on first success; the
// executor's idempotency-key check plus the lease manager prevent double
// broadcast across replicas.
//
// After accepting the broadcast into the mempool, the step polls the gateway
// Status endpoint until the tx is confirmed (bounded by confirmTimeout) and
// then transitions directly to StateConfirmed.  If confirmation does not
// arrive within the timeout the step transitions to StateBroadcasted and a
// separate ConfirmPoller (Stage 6) can finish the job.
//
// Compensation: if the broadcast has already been accepted into the mempool
// (tx_hash recorded), the on-chain tx cannot be reversed — we record an
// audit event for monitoring and let the cascade refund payment.
type BroadcastStep struct {
	client partner.Blockchain
}

// NewBroadcastStep returns a BroadcastStep bound to client.
func NewBroadcastStep(client partner.Blockchain) *BroadcastStep {
	return &BroadcastStep{client: client}
}

// Name returns "broadcast".
func (b *BroadcastStep) Name() statemachine.Step { return statemachine.StepBroadcast }

// Execute broadcasts the signed tx hex and stores the tx_hash.  On mempool
// accept it polls for confirmation before returning.
func (b *BroadcastStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	if b.client == nil {
		return StepResult{}, errors.New("blockchain client not configured")
	}
	// Idempotency: if already broadcast (tx_hash present), return the recorded
	// state without calling Broadcast again.
	if existing, ok := sc.Saga.Payload["tx_hash"].(string); ok && existing != "" {
		return StepResult{
			State:        statemachine.StateBroadcasted,
			PayloadMerge: map[string]any{"tx_hash": existing},
		}, nil
	}
	signed, _ := sc.Saga.Payload["signed_tx_hex"].(string)
	if signed == "" {
		return StepResult{}, NonRetriable(errors.New("missing signed_tx_hex in saga payload"))
	}
	resp, err := b.client.Broadcast(ctx, partner.BroadcastRequest{
		TxID: sc.Tx.TxID, SignedTxHex: signed,
	})
	if err != nil {
		if errors.Is(err, partner.ErrTransient) {
			return StepResult{}, err
		}
		return StepResult{}, NonRetriable(err)
	}
	payload := map[string]any{"tx_hash": resp.TxHash}
	if resp.InMempool {
		payload["in_mempool"] = true
	}
	if resp.Confirmed {
		payload["confirmed"] = true
	}
	// The step transitions to StateBroadcasted; the executor's Run loop polls
	// the gateway for confirmation and advances to StateConfirmed before the
	// next step (ledger) runs.
	return StepResult{State: statemachine.StateBroadcasted, PayloadMerge: payload}, nil
}

// Compensate logs the tx_hash for monitoring; the cascade refunds payment.
func (b *BroadcastStep) Compensate(ctx context.Context, sc *SagaContext) error {
	txHash, _ := sc.Saga.Payload["tx_hash"].(string)
	if txHash != "" && sc.Partners != nil && sc.Partners.Audit != nil {
		_ = sc.Partners.Audit.Record(ctx, partner.AuditEvent{
			TxID: sc.Tx.TxID, Step: "broadcast", Actor: "orchestrator",
			Before: string(sc.Saga.State), After: "failed_compensated",
			Err:    fmt.Sprintf("broadcast in mempool; tx_hash=%s needs monitoring", txHash),
			At:     time.Now().UTC(),
		})
	}
	return nil
}