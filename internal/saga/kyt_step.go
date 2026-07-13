package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

// KytStep implements Step 3 — kyt.screen.
//
// Compensation: KYT has no side effect of its own, but on reject/error the
// captured payment must be refunded.  The compensation cascade in the
// executor handles this by calling PaymentStep.Compensate in reverse order;
// KytStep.Compensate is therefore a no-op.
type KytStep struct{ client partner.Kyt }

// NewKytStep returns a KytStep bound to client.
func NewKytStep(client partner.Kyt) *KytStep { return &KytStep{client: client} }

// Name returns "kyt".
func (k *KytStep) Name() statemachine.Step { return statemachine.StepKyt }

// Execute calls aml-kyt-screening on the destination address.
func (k *KytStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	if k.client == nil {
		return StepResult{}, errors.New("kyt client not configured")
	}
	resp, err := k.client.Screen(ctx, partner.KytRequest{
		TxID: sc.Tx.TxID, UserID: sc.Tx.UserID, DestAddress: sc.Tx.DestAddress,
		Amount: sc.Tx.Amount, Asset: sc.Tx.Asset,
	})
	if err != nil {
		if errors.Is(err, partner.ErrTransient) {
			return StepResult{}, err
		}
		return StepResult{}, NonRetriable(err)
	}
	switch resp.Decision {
	case partner.KytClear:
		return StepResult{
			State:        statemachine.StateKytScreened,
			PayloadMerge: map[string]any{"kyt_reason": resp.Reason},
		}, nil
	case partner.KytReview, partner.KytReject:
		// Return a non-retriable error without a terminal result so the
		// executor runs the compensation cascade (which refunds payment via
		// PaymentStep.Compensate) and then parks the saga in
		// failed_compensated.
		return StepResult{}, NonRetriable(fmt.Errorf("kyt %s: %s", resp.Decision, resp.Reason))
	default:
		return StepResult{}, NonRetriable(fmt.Errorf("kyt unknown decision %q", resp.Decision))
	}
}

// Compensate is a no-op; the executor's cascade calls PaymentStep.Compensate.
func (k *KytStep) Compensate(ctx context.Context, sc *SagaContext) error { return nil }