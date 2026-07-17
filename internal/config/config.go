// Package config loads runtime configuration from environment variables.
//
// The struct below mirrors the env-var table in the project README; missing
// optional values fall back to the documented defaults.  Required values are
// validated by Load and produce a typed error if unset.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved runtime configuration.
type Config struct {
	Port       string
	GRPCPort   string
	DBURL      string
	RedisURL   string
	PolicyURL  string
	PaymentURL string
	KytURL     string
	MpcURL     string
	BlockchainURL string
	LedgerURL  string

	StepTimeoutSeconds             int
	StepTimeoutPolicySeconds       int
	StepTimeoutPaymentSeconds      int
	StepTimeoutKytSeconds          int
	StepTimeoutMpcSeconds          int
	StepTimeoutBroadcastSeconds    int
	StepTimeoutLedgerSeconds       int

	MaxRetries          int
	RetryBaseBackoffMS  int
	RetryMaxBackoffMS   int

	EventBusURL        string
	OutboxPollIntervalMS int
	OutboxBatchSize    int

	WorkerConcurrency int
	LeaseTTLOffset    time.Duration

	LogLevel string
}

// ErrRequired is returned by Load when a required env var is missing.
var ErrRequired = errors.New("config: required env var missing")

// Defaults applied when the corresponding env var is unset.
const (
	defaultPort             = "8080"
	defaultGRPCPort         = "9090"
	defaultStepTimeout      = 30
	defaultTimeoutPolicy    = 5
	defaultTimeoutPayment   = 30
	defaultTimeoutKyt       = 15
	defaultTimeoutMpc       = 20
	defaultTimeoutBroadcast = 30
	defaultTimeoutLedger    = 10
	defaultMaxRetries       = 5
	defaultRetryBase        = 200
	defaultRetryMax         = 10000
	defaultOutboxInterval   = 100
	defaultOutboxBatch      = 100
	defaultWorkers          = 256
	defaultLeaseTTL         = 30
	defaultLogLevel         = "info"
)

// Load reads env vars and returns a populated Config, or a typed error if a
// required value is missing.
func Load() (Config, error) {
	c := Config{
		Port:       getenv("PORT", defaultPort),
		GRPCPort:   getenv("GRPC_PORT", defaultGRPCPort),
		DBURL:      os.Getenv("DB_URL"),
		RedisURL:   os.Getenv("REDIS_URL"),
		PolicyURL:  os.Getenv("POLICY_URL"),
		PaymentURL: os.Getenv("PAYMENT_URL"),
		KytURL:     os.Getenv("KYT_URL"),
		MpcURL:     os.Getenv("MPC_URL"),
		BlockchainURL: os.Getenv("BLOCKCHAIN_URL"),
		LedgerURL:  os.Getenv("LEDGER_URL"),

		StepTimeoutSeconds:          getenvInt("STEP_TIMEOUT_SECONDS", defaultStepTimeout),
		StepTimeoutPolicySeconds:    getenvInt("STEP_TIMEOUT_POLICY_SECONDS", defaultTimeoutPolicy),
		StepTimeoutPaymentSeconds:   getenvInt("STEP_TIMEOUT_PAYMENT_SECONDS", defaultTimeoutPayment),
		StepTimeoutKytSeconds:       getenvInt("STEP_TIMEOUT_KYT_SECONDS", defaultTimeoutKyt),
		StepTimeoutMpcSeconds:       getenvInt("STEP_TIMEOUT_MPC_SECONDS", defaultTimeoutMpc),
		StepTimeoutBroadcastSeconds: getenvInt("STEP_TIMEOUT_BROADCAST_SECONDS", defaultTimeoutBroadcast),
		StepTimeoutLedgerSeconds:    getenvInt("STEP_TIMEOUT_LEDGER_SECONDS", defaultTimeoutLedger),

		MaxRetries:         getenvInt("MAX_RETRIES", defaultMaxRetries),
		RetryBaseBackoffMS: getenvInt("RETRY_BASE_BACKOFF_MS", defaultRetryBase),
		RetryMaxBackoffMS:  getenvInt("RETRY_MAX_BACKOFF_MS", defaultRetryMax),

		EventBusURL:          os.Getenv("EVENT_BUS_URL"),
		OutboxPollIntervalMS: getenvInt("OUTBOX_POLL_INTERVAL_MS", defaultOutboxInterval),
		OutboxBatchSize:      getenvInt("OUTBOX_BATCH_SIZE", defaultOutboxBatch),

		WorkerConcurrency: getenvInt("WORKER_CONCURRENCY", defaultWorkers),
		LeaseTTLOffset:    time.Duration(getenvInt("LEASE_TTL_SECONDS", defaultLeaseTTL)) * time.Second,

		LogLevel: strings.ToLower(getenv("LOG_LEVEL", defaultLogLevel)),
	}

	var missing []string
	for k, v := range map[string]string{
		"EVENT_BUS_URL": c.EventBusURL,
	} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("%w: %s", ErrRequired, strings.Join(missing, ", "))
	}
	return c, nil
}

// StepTimeout returns the per-step timeout for step, falling back to the
// global default.
func (c Config) StepTimeout(step string) time.Duration {
	switch step {
	case "POLICY":
		return time.Duration(c.StepTimeoutPolicySeconds) * time.Second
	case "PAYMENT":
		return time.Duration(c.StepTimeoutPaymentSeconds) * time.Second
	case "KYT":
		return time.Duration(c.StepTimeoutKytSeconds) * time.Second
	case "MPC_SIGN":
		return time.Duration(c.StepTimeoutMpcSeconds) * time.Second
	case "BROADCAST":
		return time.Duration(c.StepTimeoutBroadcastSeconds) * time.Second
	case "LEDGER":
		return time.Duration(c.StepTimeoutLedgerSeconds) * time.Second
	default:
		return time.Duration(c.StepTimeoutSeconds) * time.Second
	}
}

func getenv(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}