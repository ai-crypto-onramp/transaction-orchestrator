// Package config loads orchestrator configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the orchestrator.
type Config struct {
	Port                  int
	GRPCPort              int
	DBURL                 string
	RedisURL              string
	StepTimeoutSeconds    time.Duration
	StepTimeouts          map[string]time.Duration
	MaxRetries            int
	RetryBaseBackoff      time.Duration
	RetryMaxBackoff       time.Duration
	EventBusURL           string
	OutboxPollInterval   time.Duration
	OutboxBatchSize       int
	WorkerConcurrency     int
	LeaseTTLSeconds       time.Duration
	LogLevel              string
}

// Default returns a Config populated with documented defaults.
func Default() Config {
	return Config{
		Port:                8080,
		GRPCPort:            9090,
		StepTimeoutSeconds:  30 * time.Second,
		StepTimeouts: map[string]time.Duration{
			"policy":    5 * time.Second,
			"payment":   30 * time.Second,
			"kyt":       15 * time.Second,
			"mpc":       20 * time.Second,
			"broadcast": 30 * time.Second,
			"ledger":    10 * time.Second,
		},
		MaxRetries:         5,
		RetryBaseBackoff:   200 * time.Millisecond,
		RetryMaxBackoff:    10 * time.Second,
		OutboxPollInterval: 100 * time.Millisecond,
		OutboxBatchSize:    100,
		WorkerConcurrency:  256,
		LeaseTTLSeconds:    30 * time.Second,
		LogLevel:           "info",
	}
}

// Load reads configuration from environment variables, applying defaults and
// validating required values. The env var table matches README.md.
func Load() (Config, error) {
	c := Default()

	if v, ok := os.LookupEnv("PORT"); ok {
		p, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid PORT %q: %w", v, err)
		}
		c.Port = p
	}
	if v, ok := os.LookupEnv("GRPC_PORT"); ok {
		p, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid GRPC_PORT %q: %w", v, err)
		}
		c.GRPCPort = p
	}
	c.DBURL = os.Getenv("DB_URL")
	c.RedisURL = os.Getenv("REDIS_URL")
	c.EventBusURL = os.Getenv("EVENT_BUS_URL")
	c.LogLevel = getenvDefault("LOG_LEVEL", c.LogLevel)
	c.LogLevel = strings.ToLower(c.LogLevel)

	if v, ok := os.LookupEnv("STEP_TIMEOUT_SECONDS"); ok {
		s, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid STEP_TIMEOUT_SECONDS %q: %w", v, err)
		}
		c.StepTimeoutSeconds = time.Duration(s) * time.Second
	}
	c.StepTimeouts = map[string]time.Duration{
		"policy":     getenvDuration("STEP_TIMEOUT_POLICY_SECONDS", 5*time.Second),
		"payment":    getenvDuration("STEP_TIMEOUT_PAYMENT_SECONDS", 30*time.Second),
		"kyt":        getenvDuration("STEP_TIMEOUT_KYT_SECONDS", 15*time.Second),
		"mpc":        getenvDuration("STEP_TIMEOUT_MPC_SECONDS", 20*time.Second),
		"broadcast":  getenvDuration("STEP_TIMEOUT_BROADCAST_SECONDS", 30*time.Second),
		"ledger":     getenvDuration("STEP_TIMEOUT_LEDGER_SECONDS", 10*time.Second),
	}
	if v, ok := os.LookupEnv("MAX_RETRIES"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid MAX_RETRIES %q: %w", v, err)
		}
		c.MaxRetries = n
	}
	if v, ok := os.LookupEnv("RETRY_BASE_BACKOFF_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid RETRY_BASE_BACKOFF_MS %q: %w", v, err)
		}
		c.RetryBaseBackoff = time.Duration(n) * time.Millisecond
	}
	if v, ok := os.LookupEnv("RETRY_MAX_BACKOFF_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid RETRY_MAX_BACKOFF_MS %q: %w", v, err)
		}
		c.RetryMaxBackoff = time.Duration(n) * time.Millisecond
	}
	if v, ok := os.LookupEnv("OUTBOX_POLL_INTERVAL_MS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid OUTBOX_POLL_INTERVAL_MS %q: %w", v, err)
		}
		c.OutboxPollInterval = time.Duration(n) * time.Millisecond
	}
	if v, ok := os.LookupEnv("OUTBOX_BATCH_SIZE"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid OUTBOX_BATCH_SIZE %q: %w", v, err)
		}
		c.OutboxBatchSize = n
	}
	if v, ok := os.LookupEnv("WORKER_CONCURRENCY"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid WORKER_CONCURRENCY %q: %w", v, err)
		}
		c.WorkerConcurrency = n
	}
	if v, ok := os.LookupEnv("LEASE_TTL_SECONDS"); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return c, fmt.Errorf("invalid LEASE_TTL_SECONDS %q: %w", v, err)
		}
		c.LeaseTTLSeconds = time.Duration(n) * time.Second
	}

	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Validate checks required fields and constraint coherence.
func (c Config) Validate() error {
	if c.DBURL == "" {
		return fmt.Errorf("DB_URL is required")
	}
	if c.RedisURL == "" {
		return fmt.Errorf("REDIS_URL is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid PORT %d", c.Port)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MAX_RETRIES must be >= 0, got %d", c.MaxRetries)
	}
	if c.RetryBaseBackoff <= 0 {
		return fmt.Errorf("RETRY_BASE_BACKOFF_MS must be > 0")
	}
	if c.RetryMaxBackoff < c.RetryBaseBackoff {
		return fmt.Errorf("RETRY_MAX_BACKOFF_MS must be >= RETRY_BASE_BACKOFF_MS")
	}
	if c.WorkerConcurrency <= 0 {
		return fmt.Errorf("WORKER_CONCURRENCY must be > 0")
	}
	if c.OutboxBatchSize <= 0 {
		return fmt.Errorf("OUTBOX_BATCH_SIZE must be > 0")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("invalid LOG_LEVEL %q", c.LogLevel)
	}
	return nil
}

func getenvDefault(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	s, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return time.Duration(s) * time.Second
}