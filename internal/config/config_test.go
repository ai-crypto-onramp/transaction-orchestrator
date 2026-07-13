package config

import (
	"errors"
	"testing"
	"time"
)

func setEnvs(t *testing.T, m map[string]string) {
	t.Helper()
	for k, v := range m {
		t.Setenv(k, v)
	}
}

func requiredAll() map[string]string {
	return map[string]string{
		"DB_URL":        "postgres://u:p@localhost:5432/x",
		"REDIS_URL":     "redis://localhost:6379",
		"POLICY_URL":    "localhost:7001",
		"PAYMENT_URL":   "localhost:7002",
		"KYT_URL":       "localhost:7003",
		"MPC_URL":       "localhost:7004",
		"BLOCKCHAIN_URL": "localhost:7005",
		"LEDGER_URL":    "localhost:7006",
		"EVENT_BUS_URL": "nats://localhost:4222",
	}
}

func requiredMinimal() map[string]string {
	return map[string]string{
		"EVENT_BUS_URL": "nats://localhost:4222",
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnvs(t, requiredAll())
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Port != "8080" || c.GRPCPort != "9090" {
		t.Fatalf("default ports wrong: %+v", c)
	}
	if c.MaxRetries != 5 || c.RetryBaseBackoffMS != 200 || c.RetryMaxBackoffMS != 10000 {
		t.Fatalf("retry defaults wrong: %+v", c)
	}
	if c.OutboxBatchSize != 100 || c.OutboxPollIntervalMS != 100 {
		t.Fatalf("outbox defaults wrong: %+v", c)
	}
	if c.WorkerConcurrency != 256 {
		t.Fatalf("worker default wrong: %d", c.WorkerConcurrency)
	}
	if c.LeaseTTLOffset != 30*time.Second {
		t.Fatalf("lease ttl default wrong: %v", c.LeaseTTLOffset)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	for k := range requiredMinimal() {
		setEnvs(t, requiredMinimal())
		t.Setenv(k, "")
		_, err := Load()
		if err == nil || !errors.Is(err, ErrRequired) {
			t.Fatalf("missing %s should return ErrRequired, got %v", k, err)
		}
	}
}

func TestStepTimeoutOverrides(t *testing.T) {
	setEnvs(t, requiredAll())
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cases := map[string]time.Duration{
		"policy":    5 * time.Second,
		"payment":   30 * time.Second,
		"kyt":       15 * time.Second,
		"mpc_sign":  20 * time.Second,
		"broadcast": 30 * time.Second,
		"ledger":    10 * time.Second,
		"unknown":   30 * time.Second,
	}
	for step, want := range cases {
		if got := c.StepTimeout(step); got != want {
			t.Fatalf("StepTimeout(%s) = %v, want %v", step, got, want)
		}
	}
}