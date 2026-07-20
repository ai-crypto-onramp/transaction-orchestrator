package statemachine

import (
	"errors"
	"testing"
)

func TestTerminalAndFailure(t *testing.T) {
	if !StateCompleted.Terminal() {
		t.Fatal("completed should be terminal")
	}
	if !StateFailedCompensated.Terminal() || !StateFailed.Terminal() {
		t.Fatal("failure states should be terminal")
	}
	if StateCreated.Terminal() {
		t.Fatal("created should not be terminal")
	}
	if !StateFailed.IsFailure() || !StateFailedCompensated.IsFailure() {
		t.Fatal("failed* are failure states")
	}
	if StateCompleted.IsFailure() {
		t.Fatal("completed is not a failure")
	}
}

func TestForwardHappyPath(t *testing.T) {
	want := []State{
		StateCreated, StatePolicyChecked, StatePaymentCaptured, StateKytScreened,
		StateSigned, StateBroadcasted, StateConfirmed, StateLedgered, StateCompleted,
	}
	for i := 0; i+1 < len(want); i++ {
		got, err := Transition(want[i], want[i+1])
		if err != nil {
			t.Fatalf("forward %s -> %s: %v", want[i], want[i+1], err)
		}
		if got != want[i+1] {
			t.Fatalf("forward %s -> %s returned %s", want[i], want[i+1], got)
		}
	}
}

func TestIllegalForwardRejected(t *testing.T) {
	illegal := []struct{ from, to State }{
		{StateCreated, StatePaymentCaptured}, // skip policy
		{StatePolicyChecked, StateKytScreened},
		{StatePaymentCaptured, StateSigned},
		{StateKytScreened, StateBroadcasted},
		{StateSigned, StateConfirmed},
		{StateLedgered, StateCreated}, // backwards
		{StateCompleted, StateFailed}, // terminal -> anything
		{StateFailedCompensated, StateCompleted},
		{StateFailed, StateCreated},
	}
	for _, c := range illegal {
		if _, err := Transition(c.from, c.to); err == nil {
			t.Fatalf("expected illegal %s -> %s to be rejected", c.from, c.to)
		} else if !errors.Is(err, ErrIllegalTransition) {
			t.Fatalf("expected ErrIllegalTransition for %s -> %s, got %v", c.from, c.to, err)
		}
	}
}

func TestAnyNonTerminalMayFail(t *testing.T) {
	nonTerminal := []State{
		StateCreated, StatePolicyChecked, StatePaymentCaptured, StateKytScreened,
		StateSigned, StateBroadcasted, StateConfirmed, StateLedgered,
	}
	for _, s := range nonTerminal {
		if _, err := Transition(s, StateFailedCompensated); err != nil {
			t.Fatalf("non-terminal %s -> failed_compensated: %v", s, err)
		}
		if _, err := Transition(s, StateFailed); err != nil {
			t.Fatalf("non-terminal %s -> failed: %v", s, err)
		}
	}
}

func TestStepOrderAndHelpers(t *testing.T) {
	if len(StepOrder) != 6 {
		t.Fatalf("expected 6 steps, got %d", len(StepOrder))
	}
	if next, ok := StepAfter(StepPolicy); !ok || next != StepPayment {
		t.Fatalf("StepAfter(policy) = %v %v", next, ok)
	}
	if _, ok := StepAfter(StepLedger); ok {
		t.Fatal("ledger has no forward step")
	}
	if StateForStep[StepPolicy] != StatePolicyChecked {
		t.Fatal("StateForStep mismatch")
	}
	if StepForState[StatePolicyChecked] != StepPolicy {
		t.Fatal("StepForState mismatch")
	}
	if _, ok := StepForState[StateCreated]; ok {
		t.Fatal("created has no owning step")
	}

	if nxt, ok := ForwardTarget(StateCreated); !ok || nxt != StatePolicyChecked {
		t.Fatalf("ForwardTarget(created) = %v %v", nxt, ok)
	}
	if _, ok := ForwardTarget(StateCompleted); ok {
		t.Fatal("completed has no forward target")
	}
}

func TestMustTransitionPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on illegal transition")
		}
	}()
	_ = MustTransition(StateCreated, StateCompleted)
}

func TestMustTransitionSuccessReturnsNext(t *testing.T) {
	if got := MustTransition(StateCreated, StatePolicyChecked); got != StatePolicyChecked {
		t.Fatalf("expected policy_checked, got %s", got)
	}
	if got := MustTransition(StateCreated, StateFailedCompensated); got != StateFailedCompensated {
		t.Fatalf("expected failed_compensated, got %s", got)
	}
}

func TestTerminalStatesExhaustive(t *testing.T) {
	all := []State{
		StateCreated, StatePolicyChecked, StatePaymentCaptured, StateKytScreened,
		StateSigned, StateBroadcasted, StateConfirmed, StateLedgered,
		StateCompleted, StateFailedCompensated, StateFailed,
	}
	for _, s := range all {
		switch s {
		case StateCompleted, StateFailedCompensated, StateFailed:
			if !s.Terminal() {
				t.Fatalf("%s should be terminal", s)
			}
		default:
			if s.Terminal() {
				t.Fatalf("%s should not be terminal", s)
			}
		}
	}
}

func TestTransitionFromTerminalRejected(t *testing.T) {
	for _, s := range []State{StateCompleted, StateFailedCompensated, StateFailed} {
		if _, err := Transition(s, StateCreated); err == nil {
			t.Fatalf("expected error transitioning from terminal %s", s)
		}
	}
}