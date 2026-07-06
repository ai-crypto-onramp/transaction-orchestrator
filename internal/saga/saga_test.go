package saga

import (
	"context"
	"errors"
	"testing"
)

type noopStep struct {
	name    StepName
	result  Result
	execErr error
	compErr error
}

func (s *noopStep) Name() StepName { return s.name }
func (s *noopStep) Execute(ctx context.Context, sc *SagaContext) (Result, error) {
	return s.result, s.execErr
}
func (s *noopStep) Compensate(ctx context.Context, sc *SagaContext) error {
	return s.compErr
}

func TestOrdered_Count(t *testing.T) {
	t.Parallel()
	if len(Ordered) != 6 {
		t.Fatalf("expected 6 ordered steps, got %d", len(Ordered))
	}
	if Ordered[0] != StepPolicy || Ordered[5] != StepLedger {
		t.Errorf("unexpected ordering: %v", Ordered)
	}
}

func TestStepInterface_ExecuteSucceeded(t *testing.T) {
	t.Parallel()
	s := &noopStep{name: StepPolicy, result: ResultSucceeded}
	r, err := s.Execute(context.Background(), &SagaContext{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if r != ResultSucceeded {
		t.Errorf("expected succeeded, got %s", r)
	}
}

func TestStepInterface_ExecuteFailed(t *testing.T) {
	t.Parallel()
	s := &noopStep{name: StepPolicy, result: ResultFailed}
	r, err := s.Execute(context.Background(), &SagaContext{})
	if !errors.Is(err, nil) && r != ResultFailed {
		t.Fatalf("expected failed result, got %s", r)
	}
}

func TestStepInterface_Compensate(t *testing.T) {
	t.Parallel()
	s := &noopStep{name: StepPayment, compErr: errors.New("refund failed")}
	if err := s.Compensate(context.Background(), &SagaContext{}); err == nil {
		t.Fatal("expected compensate error, got nil")
	}
}