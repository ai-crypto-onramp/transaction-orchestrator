package statemachine

import (
	"errors"
	"testing"
)

// legalTransitions is the full expected legal transition table, derived from the
// README state machine diagram.
func legalTransitions() map[State][]State {
	happy := []State{
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
	legal := make(map[State][]State)
	for i := 0; i < len(happy)-1; i++ {
		legal[happy[i]] = append(legal[happy[i]], happy[i+1])
	}
	for _, s := range happy {
		if s.IsTerminal() {
			continue
		}
		legal[s] = append(legal[s], StateFailedCompensated, StateFailed)
	}
	return legal
}

func TestCanTransition_AllLegalAllowed(t *testing.T) {
	t.Parallel()
	for from, tos := range legalTransitions() {
		for _, to := range tos {
			if !CanTransition(from, to) {
				t.Errorf("expected legal transition %s -> %s to be allowed", from, to)
			}
		}
	}
}

func TestTransition_AllLegalAllowed(t *testing.T) {
	t.Parallel()
	for from, tos := range legalTransitions() {
		for _, to := range tos {
			got, err := Transition(from, to)
			if err != nil {
				t.Errorf("expected legal transition %s -> %s to succeed, got error: %v", from, to, err)
			}
			if got != to {
				t.Errorf("expected resulting state %s, got %s", to, got)
			}
		}
	}
}

func TestTransition_IllegalRejected(t *testing.T) {
	t.Parallel()
	legal := legalTransitions()
	legalSet := func(from, to State) bool {
		for _, l := range legal[from] {
			if l == to {
				return true
			}
		}
		return false
	}
	for _, from := range AllStates {
		for _, to := range AllStates {
			if from == to {
				continue
			}
			if legalSet(from, to) {
				continue
			}
			got, err := Transition(from, to)
			if err == nil {
				t.Errorf("expected illegal transition %s -> %s to be rejected, but succeeded with state %s", from, to, got)
			}
			if !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("expected ErrIllegalTransition for %s -> %s, got %v", from, to, err)
			}
			if got != from {
				t.Errorf("expected state to remain %s on illegal transition, got %s", from, got)
			}
		}
	}
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()
	terminals := []State{StateCompleted, StateFailedCompensated, StateFailed}
	for _, s := range terminals {
		if !s.IsTerminal() {
			t.Errorf("expected %s to be terminal", s)
		}
	}
	nonTerminals := []State{
		StateCreated,
		StatePolicyChecked,
		StatePaymentCaptured,
		StateKytScreened,
		StateSigned,
		StateBroadcasted,
		StateConfirmed,
		StateLedgered,
	}
	for _, s := range nonTerminals {
		if s.IsTerminal() {
			t.Errorf("expected %s to be non-terminal", s)
		}
	}
}

func TestTransition_SameStateRejected(t *testing.T) {
	t.Parallel()
	for _, s := range AllStates {
		if _, err := Transition(s, s); err == nil {
			t.Errorf("expected same-state transition %s -> %s to be rejected", s, s)
		}
	}
}

func TestTransition_BackwardsRejected(t *testing.T) {
	t.Parallel()
	illegalBackward := []struct {
		from, to State
	}{
		{StatePolicyChecked, StateCreated},
		{StatePaymentCaptured, StatePolicyChecked},
		{StateKytScreened, StatePaymentCaptured},
		{StateSigned, StateKytScreened},
		{StateBroadcasted, StateSigned},
		{StateConfirmed, StateBroadcasted},
		{StateLedgered, StateConfirmed},
		{StateCompleted, StateLedgered},
	}
	for _, c := range illegalBackward {
		if _, err := Transition(c.from, c.to); err == nil {
			t.Errorf("expected backward transition %s -> %s to be rejected", c.from, c.to)
		}
	}
}

func TestTransition_FromTerminalRejected(t *testing.T) {
	t.Parallel()
	for _, from := range []State{StateCompleted, StateFailedCompensated, StateFailed} {
		for _, to := range AllStates {
			if from == to {
				continue
			}
			if _, err := Transition(from, to); err == nil {
				t.Errorf("expected transition out of terminal %s -> %s to be rejected", from, to)
			}
		}
	}
}