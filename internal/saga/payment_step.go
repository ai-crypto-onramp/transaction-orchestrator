package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

// PaymentStep implements Step 2 — payment.authorize + capture.
//
// Compensation rules:
//   - If Capture has not run (capture_id empty) -> VoidAuthorization.
//   - If Capture has run (capture_id set)       -> Refund.
type PaymentStep struct{ client partner.Payment }

// NewPaymentStep returns a new PaymentStep bound to client.
func NewPaymentStep(client partner.Payment) *PaymentStep { return &PaymentStep{client: client} }

// Name returns "payment".
func (p *PaymentStep) Name() statemachine.Step { return statemachine.StepPayment }

// Execute authorizes then captures fiat, storing both ids in saga payload.
func (p *PaymentStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	if p.client == nil {
		return StepResult{}, errors.New("payment client not configured")
	}
	// Authorize
	auth, err := p.client.Authorize(ctx, partner.PaymentAuthorizeRequest{
		TxID: sc.Tx.TxID, UserID: sc.Tx.UserID, QuoteID: sc.Tx.QuoteID,
		Amount: sc.Tx.Amount, Asset: sc.Tx.Asset, Rail: sc.Tx.Rail,
	})
	if err != nil {
		if errors.Is(err, partner.ErrTransient) {
			return StepResult{}, err
		}
		return StepResult{}, NonRetriable(err)
	}
	// Capture
	cap, err := p.client.Capture(ctx, partner.PaymentCaptureRequest{
		TxID: sc.Tx.TxID, AuthID: auth.AuthID, Amount: sc.Tx.Amount, Asset: sc.Tx.Asset,
	})
	if err != nil {
		// Pre-capture failure: void the authorization immediately so we don't
		// leave an open auth, then surface a non-retriable error so the saga
		// compensates (which will be a no-op for this step since we already
		// voided).
		_ = p.client.VoidAuthorization(ctx, partner.PaymentVoidRequest{
			TxID: sc.Tx.TxID, AuthID: auth.AuthID,
		})
		if errors.Is(err, partner.ErrTransient) {
			// transient capture failure: persist auth_id so compensate can void
			return StepResult{PayloadMerge: map[string]any{"auth_id": auth.AuthID}}, err
		}
		return StepResult{PayloadMerge: map[string]any{"auth_id": auth.AuthID}}, NonRetriable(err)
	}
	return StepResult{
		State: statemachine.StatePaymentCaptured,
		PayloadMerge: map[string]any{
			"auth_id":    auth.AuthID,
			"capture_id": cap.CaptureID,
		},
	}, nil
}

// Compensate voids an open authorization or refunds a captured payment.
func (p *PaymentStep) Compensate(ctx context.Context, sc *SagaContext) error {
	if p.client == nil {
		return errors.New("payment client not configured")
	}
	authID, _ := sc.Saga.Payload["auth_id"].(string)
	captureID, _ := sc.Saga.Payload["capture_id"].(string)
	if captureID != "" {
		if err := p.client.Refund(ctx, partner.PaymentRefundRequest{
			TxID: sc.Tx.TxID, CaptureID: captureID, Amount: sc.Tx.Amount, Asset: sc.Tx.Asset,
		}); err != nil {
			return fmt.Errorf("payment refund: %w", err)
		}
		return nil
	}
	if authID != "" {
		if err := p.client.VoidAuthorization(ctx, partner.PaymentVoidRequest{
			TxID: sc.Tx.TxID, AuthID: authID,
		}); err != nil {
			return fmt.Errorf("payment void: %w", err)
		}
	}
	return nil
}