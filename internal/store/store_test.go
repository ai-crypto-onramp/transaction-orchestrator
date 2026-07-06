package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeRow struct {
	scanFn func(dest ...any) error
}

func (r *fakeRow) Scan(dest ...any) error { return r.scanFn(dest...) }

type fakeExecutor struct {
	execErr    error
	queryRowFn func(sql string, args []any) pgx.Row
}

func (f *fakeExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (f *fakeExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}
func (f *fakeExecutor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f.queryRowFn(sql, args)
}

func TestLoadSagaState_NotFound(t *testing.T) {
	t.Parallel()
	s := &pgStore{}
	ex := &fakeExecutor{
		queryRowFn: func(sql string, args []any) pgx.Row {
			return &fakeRow{scanFn: func(dest ...any) error { return pgx.ErrNoRows }}
		},
	}
	_, err := s.LoadSagaState(context.Background(), ex, uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSaveSagaState_VersionConflict(t *testing.T) {
	t.Parallel()
	s := &pgStore{}
	// executor returns empty CommandTag -> 0 rows affected.
	state := &SagaState{TxID: uuid.New(), Version: 1}
	err := s.SaveSagaState(context.Background(), &zeroTagExecutor{}, state)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}
}

func TestSaveSagaState_BumpsVersion(t *testing.T) {
	t.Parallel()
	s := &pgStore{}
	state := &SagaState{TxID: uuid.New(), Version: 5}
	_ = s.SaveSagaState(context.Background(), &fakeExecutor{}, state)
	if state.Version != 6 {
		t.Errorf("expected version bumped to 6, got %d", state.Version)
	}
}

type zeroTagExecutor struct{}

func (z *zeroTagExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (z *zeroTagExecutor) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}
func (z *zeroTagExecutor) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &fakeRow{scanFn: func(dest ...any) error { return nil }}
}

func TestCreateTx_ExecError(t *testing.T) {
	t.Parallel()
	s := &pgStore{}
	ex := &fakeExecutor{execErr: errors.New("boom")}
	err := s.CreateTx(context.Background(), ex, &Transaction{TxID: uuid.New()})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestAppendOutbox_ExecError(t *testing.T) {
	t.Parallel()
	s := &pgStore{}
	ex := &fakeExecutor{execErr: errors.New("boom")}
	err := s.AppendOutbox(context.Background(), ex, &OutboxEvent{TxID: uuid.New()})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestNullableString(t *testing.T) {
	t.Parallel()
	if nullableString("") != nil {
		t.Error("expected nil for empty string")
	}
	if nullableString("x") != "x" {
		t.Error("expected 'x' for non-empty string")
	}
}