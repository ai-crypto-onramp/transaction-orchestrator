// Package saga defines the orchestration primitives: the Step interface,
// SagaContext, and the canonical saga step ordering.
package saga

import (
	"context"
)

// StepName is a typed identifier for a saga step.
type StepName string

const (
	StepPolicy     StepName = "policy.evaluate"
	StepPayment    StepName = "payment.authorize+capture"
	StepKyt        StepName = "kyt.screen"
	StepMpcSign    StepName = "mpc.sign"
	StepBroadcast  StepName = "blockchain.broadcast"
	StepLedger     StepName = "ledger.post"
)

// Ordered is the canonical forward step ordering.
var Ordered = []StepName{
	StepPolicy,
	StepPayment,
	StepKyt,
	StepMpcSign,
	StepBroadcast,
	StepLedger,
}

// Result is the outcome of a step execution.
type Result string

const (
	ResultSucceeded Result = "succeeded"
	ResultFailed    Result = "failed"
)

// SagaContext is the in-memory + durable context carried through a saga run.
type SagaContext struct {
	TxID          string
	UserID        string
	QuoteID       string
	Amount        string
	Asset         string
	Rail          string
	DestAddress   string
	Attempt       int
	// Payload mirrors saga_state.payload (JSONB) for accumulated partner ids.
	Payload map[string]any
}

// Step is the interface implemented by every saga step. Execute drives the
// forward action; Compensate drives the reverse/undo action.
type Step interface {
	Name() StepName
	Execute(ctx context.Context, sc *SagaContext) (Result, error)
	Compensate(ctx context.Context, sc *SagaContext) error
}