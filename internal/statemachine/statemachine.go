package statemachine

import "errors"

// State is a typed saga state.
type State string

const (
	StateCreated             State = "created"
	StatePolicyChecked       State = "policy_checked"
	StatePaymentCaptured     State = "payment_captured"
	StateKytScreened         State = "kyt_screened"
	StateSigned              State = "signed"
	StateBroadcasted         State = "broadcasted"
	StateConfirmed           State = "confirmed"
	StateLedgered            State = "ledgered"
	StateCompleted           State = "completed"
	StateFailedCompensated   State = "failed_compensated"
	StateFailed              State = "failed"
)

// AllStates is the full ordered list of states for exhaustive testing.
var AllStates = []State{
	StateCreated,
	StatePolicyChecked,
	StatePaymentCaptured,
	StateKytScreened,
	StateSigned,
	StateBroadcasted,
	StateConfirmed,
	StateLedgered,
	StateCompleted,
	StateFailedCompensated,
	StateFailed,
}

// IsTerminal reports whether s is a terminal state.
func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailedCompensated, StateFailed:
		return true
	}
	return false
}

// forward is the canonical happy-path ordering.
var forward = []State{
	StateCreated,
	StatePolicyChecked,
	StatePaymentCaptured,
	StateKytScreened,
	StateSigned,
	StateBroadcasted,
	StateConfirmed,
	StateLedgered,
	StateCompleted,
}

// ErrIllegalTransition is returned when a transition is not allowed.
var ErrIllegalTransition = errors.New("illegal state transition")

// allowed builds the legal transition map: each non-terminal forward state may
// advance to its next forward state, and any non-terminal state may move to a
// terminal failure state (failed_compensated or failed).
func allowed() map[State]map[State]struct{} {
	m := make(map[State]map[State]struct{}, len(AllStates))
	for _, s := range AllStates {
		m[s] = make(map[State]struct{})
	}
	for i := 0; i < len(forward)-1; i++ {
		from, to := forward[i], forward[i+1]
		m[from][to] = struct{}{}
	}
	for _, from := range forward {
		if from.IsTerminal() {
			continue
		}
		m[from][StateFailedCompensated] = struct{}{}
		m[from][StateFailed] = struct{}{}
	}
	return m
}

var transitions = allowed()

// CanTransition reports whether moving from -> to is legal.
func CanTransition(from, to State) bool {
	_, ok := transitions[from][to]
	return ok
}

// Transition validates and records a move from -> to. Returns
// ErrIllegalTransition (a typed error) if the move is not allowed.
func Transition(from, to State) (State, error) {
	if !CanTransition(from, to) {
		return from, ErrIllegalTransition
	}
	return to, nil
}