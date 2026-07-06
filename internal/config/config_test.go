package config

import (
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	t.Parallel()
	c := Default()
	if c.Port != 8080 || c.GRPCPort != 9090 {
		t.Errorf("expected default ports 8080/9090, got %d/%d", c.Port, c.GRPCPort)
	}
	if c.StepTimeoutSeconds != 30*time.Second {
		t.Errorf("expected default step timeout 30s, got %v", c.StepTimeoutSeconds)
	}
	if c.MaxRetries != 5 || c.WorkerConcurrency != 256 || c.OutboxBatchSize != 100 {
		t.Errorf("unexpected defaults: retries=%d workers=%d batch=%d",
			c.MaxRetries, c.WorkerConcurrency, c.OutboxBatchSize)
	}
	if c.StepTimeouts["policy"] != 5*time.Second {
		t.Errorf("expected policy timeout default 5s, got %v", c.StepTimeouts["policy"])
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	t.Setenv("DB_URL", "")
	t.Setenv("REDIS_URL", "")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error when required vars missing")
	}
}

func TestLoad_RequiredProvided(t *testing.T) {
	t.Setenv("DB_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	c, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c.DBURL != "postgres://u:p@localhost:5432/db" {
		t.Errorf("unexpected DB_URL %q", c.DBURL)
	}
	if c.RedisURL != "redis://localhost:6379" {
		t.Errorf("unexpected REDIS_URL %q", c.RedisURL)
	}
}

func TestLoad_InvalidPort(t *testing.T) {
	t.Setenv("DB_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	t.Setenv("PORT", "not-a-number")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid PORT")
	}
}

func TestLoad_PerStepTimeoutOverrides(t *testing.T) {
	t.Setenv("DB_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	t.Setenv("STEP_TIMEOUT_KYT_SECONDS", "42")
	c, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if c.StepTimeouts["kyt"] != 42*time.Second {
		t.Errorf("expected kyt override 42s, got %v", c.StepTimeouts["kyt"])
	}
}

func TestValidate_RetryBackoff(t *testing.T) {
	c := Default()
	c.DBURL = "x"
	c.RedisURL = "x"
	c.RetryBaseBackoff = 5 * time.Second
	c.RetryMaxBackoff = 1 * time.Second
	if err := c.Validate(); err == nil {
		t.Error("expected error when max backoff < base backoff")
	}
}

func TestValidate_LogLevel(t *testing.T) {
	c := Default()
	c.DBURL = "x"
	c.RedisURL = "x"
	c.LogLevel = "trace"
	if err := c.Validate(); err == nil {
		t.Error("expected error for invalid LOG_LEVEL")
	}
}