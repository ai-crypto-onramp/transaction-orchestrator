package saga

import (
	"context"
	"errors"
	"fmt"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
)

// PolicyStep implements Step 1 — policy.evaluate.
type PolicyStep struct{ client partner.Policy }

// NewPolicyStep returns a new PolicyStep bound to client.
func NewPolicyStep(client partner.Policy) *PolicyStep { return &PolicyStep{client: client} }

// Name returns "policy".
func (p *PolicyStep) Name() statemachine.Step { return statemachine.StepPolicy }

// Execute calls the policy-risk-engine and transitions to policy_checked on
// allow, or to failed_compensated on deny (no compensation needed for policy).
func (p *PolicyStep) Execute(ctx context.Context, sc *SagaContext) (StepResult, error) {
	if p.client == nil {
		return StepResult{}, errors.New("policy client not configured")
	}
	resp, err := p.client.Evaluate(ctx, partner.PolicyRequest{
		TxID: sc.Tx.TxID, UserID: sc.Tx.UserID, QuoteID: sc.Tx.QuoteID,
		Amount: sc.Tx.Amount, Asset: sc.Tx.Asset, Rail: sc.Tx.Rail, DestAddress: sc.Tx.DestAddress,
	})
	if err != nil {
		if errors.Is(err, partner.ErrTransient) {
			return StepResult{}, err
		}
		return StepResult{}, NonRetriable(err)
	}
	if resp.Decision == partner.PolicyDeny {
		// Policy deny: terminal failure, no compensation needed.
		return StepResult{
			State:    statemachine.StateFailedCompensated,
			Terminal: true,
			PayloadMerge: map[string]any{"policy_reason": resp.Reason},
		}, fmt.Errorf("policy denied: %s", resp.Reason)
	}
	return StepResult{
		State:        statemachine.StatePolicyChecked,
		PayloadMerge: map[string]any{"policy_reason": resp.Reason},
	}, nil
}

// Compensate is a no-op for the policy step (no side effect yet).
func (p *PolicyStep) Compensate(ctx context.Context, sc *SagaContext) error { return nil }