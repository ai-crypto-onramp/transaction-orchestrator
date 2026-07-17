// Package statemachine encodes the legal lifecycle of a transaction saga.
//
// The set of states, the legal forward transitions, and the typed errors used
// to reject illegal moves are all exported from this package.  It is pure,
// in-memory logic — no I/O, no partner calls — so the entire transition table
// can be unit-tested deterministically.
package statemachine

import (
	"errors"
	"fmt"
)

// State is a saga state value as persisted in transactions.status and
// saga_state.state.
type State string

// The full set of saga states.  The happy-path sequence is:
//
//	created -> policy_checked -> payment_captured -> kyt_screened ->
//	signed -> broadcasted -> confirmed -> ledgered -> completed
//
// The two terminal failure states are failed_compensated and failed.
const (
	StateCreated           State = "CREATED"
	StatePolicyChecked     State = "POLICY_CHECKED"
	StatePaymentCaptured   State = "PAYMENT_CAPTURED"
	StateKytScreened       State = "KYT_SCREENED"
	StateSigned            State = "SIGNED"
	StateBroadcasted       State = "BROADCASTED"
	StateConfirmed         State = "CONFIRMED"
	StateLedgered          State = "LEDGERED"
	StateCompleted         State = "COMPLETED"
	StateFailedCompensated State = "FAILED_COMPENSATED"
	StateFailed            State = "FAILED"
)

// Terminal reports whether s is a terminal saga state.
func (s State) Terminal() bool {
	switch s {
	case StateCompleted, StateFailedCompensated, StateFailed:
		return true
	}
	return false
}

// IsFailure reports whether s is a terminal failure state.
func (s State) IsFailure() bool {
	return s == StateFailedCompensated || s == StateFailed
}

// Step is a saga step name.  Order matters: see StepOrder.
type Step string

const (
	StepPolicy     Step = "POLICY"
	StepPayment    Step = "PAYMENT"
	StepKyt        Step = "KYT"
	StepMpcSign    Step = "MPC_SIGN"
	StepBroadcast  Step = "BROADCAST"
	StepLedger     Step = "LEDGER"
)

// StepOrder is the canonical forward execution order.
var StepOrder = []Step{
	StepPolicy,
	StepPayment,
	StepKyt,
	StepMpcSign,
	StepBroadcast,
	StepLedger,
}

// StepAfter returns the step that follows s in the forward order, or ok=false
// if s is the last step.
func StepAfter(s Step) (next Step, ok bool) {
	for i, cur := range StepOrder {
		if cur == s && i+1 < len(StepOrder) {
			return StepOrder[i+1], true
		}
	}
	return "", false
}

// StateForStep is the saga state reached on successful completion of a step.
var StateForStep = map[Step]State{
	StepPolicy:    StatePolicyChecked,
	StepPayment:   StatePaymentCaptured,
	StepKyt:       StateKytScreened,
	StepMpcSign:   StateSigned,
	StepBroadcast: StateBroadcasted,
	StepLedger:    StateLedgered,
}

// StepForState is the inverse of StateForStep: it maps a state (as reached by
// some step) back to the step that produced it.  States without an owning
// step (created / confirmed / completed / failed*) map to ok=false.
var StepForState = map[State]Step{
	StatePolicyChecked:   StepPolicy,
	StatePaymentCaptured: StepPayment,
	StateKytScreened:     StepKyt,
	StateSigned:          StepMpcSign,
	StateBroadcasted:     StepBroadcast,
	StateLedgered:        StepLedger,
}

// forward is the happy-path transition table.  Keys that don't appear here
// have no legal forward transition (either terminal or reached only via the
// special broadcast -> confirmed -> ledgered -> completed tail).
var forward = map[State]State{
	StateCreated:         StatePolicyChecked,
	StatePolicyChecked:   StatePaymentCaptured,
	StatePaymentCaptured: StateKytScreened,
	StateKytScreened:     StateSigned,
	StateSigned:          StateBroadcasted,
	StateBroadcasted:     StateConfirmed,
	StateConfirmed:       StateLedgered,
	StateLedgered:        StateCompleted,
}

// ErrIllegalTransition is returned by Transition when the requested move is
// not in the legal forward or failure set.
var ErrIllegalTransition = errors.New("statemachine: illegal transition")

// Transition returns the next state for an attempted forward move from cur to
// nxt, or a typed error if the move is illegal.
//
// In addition to the forward table, any non-terminal state may legally move
// to StateFailedCompensated (compensation in progress / completed) and to
// StateFailed (compensation exhausted).  Both StateFailedCompensated and
// StateFailed are terminal and cannot be left.
func Transition(cur, nxt State) (State, error) {
	if cur.Terminal() {
		return cur, fmt.Errorf("%w: %s is terminal", ErrIllegalTransition, cur)
	}
	if nxt == StateFailedCompensated || nxt == StateFailed {
		return nxt, nil
	}
	if want, ok := forward[cur]; ok && nxt == want {
		return nxt, nil
	}
	return cur, fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, cur, nxt)
}

// MustTransition is a convenience for tests; it panics on illegal moves.
func MustTransition(cur, nxt State) State {
	out, err := Transition(cur, nxt)
	if err != nil {
		panic(err)
	}
	return out
}

// ForwardTarget returns the single legal happy-path successor of cur, or
// ok=false if cur has none (terminal states).
func ForwardTarget(cur State) (State, bool) {
	nxt, ok := forward[cur]
	return nxt, ok
}