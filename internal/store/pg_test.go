package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDecodePayloadEmptyAndInvalid(t *testing.T) {
	if got := decodePayload(nil); len(got) != 0 {
		t.Fatalf("expected empty map for nil, got %#v", got)
	}
	if got := decodePayload([]byte{}); len(got) != 0 {
		t.Fatalf("expected empty map for empty, got %#v", got)
	}
	if got := decodePayload([]byte("not json")); len(got) != 0 {
		t.Fatalf("expected empty map for invalid json, got %#v", got)
	}
}

func TestDecodePayloadValid(t *testing.T) {
	m := decodePayload([]byte(`{"a":"b","n":1}`))
	if m["a"] != "b" {
		t.Fatalf("expected a=b, got %#v", m)
	}
	if n, ok := m["n"].(float64); !ok || n != 1 {
		t.Fatalf("expected n=1 (float64), got %#v", m["n"])
	}
}

func TestMapPgErr(t *testing.T) {
	if err := mapPgErr(nil); err != nil {
		t.Fatalf("nil should pass through, got %v", err)
	}
	plain := errors.New("some error")
	if err := mapPgErr(plain); err != plain {
		t.Fatalf("non-pg error should pass through, got %v", err)
	}
	dup := &pgconn.PgError{Code: "23505"}
	if err := mapPgErr(dup); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected ErrDuplicate, got %v", err)
	}
	serial := &pgconn.PgError{Code: "40001"}
	if err := mapPgErr(serial); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
	deadlock := &pgconn.PgError{Code: "40P01"}
	if err := mapPgErr(deadlock); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
	other := &pgconn.PgError{Code: "42P01"}
	if err := mapPgErr(other); err != other {
		t.Fatalf("other pg code should pass through, got %v", err)
	}
}

func TestScanSagaErrNoRows(t *testing.T) {
	scan := func(...any) error { return pgx.ErrNoRows }
	_, err := scanSaga(scan)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestScanSagaSuccessWithLease(t *testing.T) {
	scan := func(args ...any) error {
		*args[0].(*uuid.UUID) = uuid.UUID{1, 2, 3}
		*args[1].(*string) = "tx-1"
		*args[2].(*statemachine.Step) = statemachine.StepPolicy
		*args[3].(*statemachine.State) = statemachine.StateCreated
		lo := "owner-1"
		*args[4].(**string) = &lo
		exp := time.Now().UTC()
		*args[5].(**time.Time) = &exp
		*args[6].(*[]byte) = []byte(`{"k":"v"}`)
		*args[7].(*int64) = int64(3)
		return nil
	}
	s, err := scanSaga(scan)
	if err != nil {
		t.Fatalf("scanSaga: %v", err)
	}
	if s.TxID != "tx-1" || s.CurrentStep != statemachine.StepPolicy || s.State != statemachine.StateCreated {
		t.Fatalf("unexpected saga: %+v", s)
	}
	if s.LeaseOwner != "owner-1" {
		t.Fatalf("expected lease owner owner-1, got %q", s.LeaseOwner)
	}
	if s.LeaseExpiresAt == nil {
		t.Fatal("expected non-nil lease expiry")
	}
	if s.Version != 3 {
		t.Fatalf("expected version 3, got %d", s.Version)
	}
	if s.Payload["k"] != "v" {
		t.Fatalf("expected payload k=v, got %#v", s.Payload)
	}
}

func TestScanSagaSuccessWithoutLease(t *testing.T) {
	scan := func(args ...any) error {
		*args[0].(*uuid.UUID) = uuid.UUID{}
		*args[1].(*string) = "tx-2"
		*args[2].(*statemachine.Step) = statemachine.StepPolicy
		*args[3].(*statemachine.State) = statemachine.StateCreated
		*args[4].(**string) = nil
		*args[5].(**time.Time) = nil
		*args[6].(*[]byte) = nil
		*args[7].(*int64) = int64(1)
		return nil
	}
	s, err := scanSaga(scan)
	if err != nil {
		t.Fatalf("scanSaga: %v", err)
	}
	if s.LeaseOwner != "" {
		t.Fatalf("expected empty lease owner, got %q", s.LeaseOwner)
	}
	if s.LeaseExpiresAt != nil {
		t.Fatalf("expected nil lease expiry, got %v", s.LeaseExpiresAt)
	}
	if len(s.Payload) != 0 {
		t.Fatalf("expected empty payload, got %#v", s.Payload)
	}
}

func TestScanEventSuccess(t *testing.T) {
	var e OutboxEvent
	scan := func(args ...any) error {
		*args[0].(*uuid.UUID) = uuid.UUID{}
		*args[1].(*string) = "eid"
		*args[2].(*string) = "tx"
		*args[3].(*string) = "evt"
		*args[4].(*string) = "policy"
		*args[5].(*int) = 1
		*args[6].(*[]byte) = []byte(`{"x":1}`)
		*args[7].(*time.Time) = time.Now().UTC()
		pub := time.Now().UTC()
		*args[8].(**time.Time) = &pub
		*args[9].(*OutboxStatus) = OutboxPublished
		*args[10].(*string) = "dedup"
		return nil
	}
	if err := scanEvent(scan, &e); err != nil {
		t.Fatalf("scanEvent: %v", err)
	}
	if e.EventID != "eid" || e.TxID != "tx" || e.EventType != "evt" {
		t.Fatalf("unexpected event: %+v", e)
	}
	if e.Step != "policy" || e.Attempt != 1 {
		t.Fatalf("unexpected step/attempt: %+v", e)
	}
	if e.Status != OutboxPublished {
		t.Fatalf("expected published status, got %s", e.Status)
	}
	if e.DedupKey != "dedup" {
		t.Fatalf("expected dedup, got %q", e.DedupKey)
	}
	if e.PublishedAt == nil {
		t.Fatal("expected non-nil published at")
	}
	if e.Payload["x"] == nil {
		t.Fatalf("expected payload x set, got %#v", e.Payload)
	}
}

func TestScanEventScanError(t *testing.T) {
	var e OutboxEvent
	scan := func(args ...any) error { return errors.New("scan fail") }
	if err := scanEvent(scan, &e); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// Cover the no-op pgTx Commit/Rollback by constructing one with a nil tx.
// These methods just forward to t.tx.Commit/Rollback which would panic on nil,
// so we only call them via the interface with a nil tx wrapped in a way that
// does not dereference. Since pgx.Tx is an interface, a nil interface value
// would panic; skip those and instead verify the type satisfies the Tx
// interface at compile time.
func TestPgTxImplementsTxInterface(t *testing.T) {
	var _ Tx = (*pgTx)(nil)
	var _ Tx = (*pgTx)(nil)
	_ = context.Background()
}