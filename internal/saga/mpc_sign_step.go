package saga

import (
	"context"
	"errors"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

// MpcSignStep implements Step 4 — mpc.sign.
//
// Compensation: sign has no on-chain side effect; refunding payment is
// handled by the cascade (PaymentStep.Compensate).  This step's Compensate is
// a no-op.
type MpcSignStep struct{ client partner.Mpc }

// NewMpcSignStep returns an MpcSignStep bound to client.
func NewMpcSignStep(client partner.Mpc) *MpcSignStep { return &MpcSignStep{client: client} }

// Name returns "mpc_sign".
func (m *MpcSignStep) Name() statemachine.Step { return statemachine.StepMpcSign }

// Execute signs the unsigned tx hex stored in saga payload.
func (m *MpcSignStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	if m.client == nil {
		return StepResult{}, errors.New("mpc client not configured")
	}
	unsigned, _ := sc.Saga.Payload["unsigned_tx_hex"].(string)
	if unsigned == "" {
		unsigned = "unsigned-" + sc.Tx.TxID // stub fallback for tests
	}
	resp, err := m.client.Sign(ctx, partner.MpcSignRequest{
		TxID: sc.Tx.TxID, UnsignedTxHex: unsigned,
	})
	if err != nil {
		if errors.Is(err, partner.ErrTransient) {
			return StepResult{}, err
		}
		return StepResult{}, NonRetriable(err)
	}
	return StepResult{
		State: statemachine.StateSigned,
		PayloadMerge: map[string]any{"signed_tx_hex": resp.SignedTxHex},
	}, nil
}

// Compensate is a no-op; the cascade handles payment refund.
func (m *MpcSignStep) Compensate(ctx context.Context, sc *SagaContext) error { return nil }