// Package main is the orchestrator HTTP server entry point.
//
// It loads config, opens the store, and serves the REST API with graceful
// shutdown on SIGTERM/SIGINT.  The saga worker (Stages 3–8) is started here as
// well, behind a feature flag so Stage 2 can boot without partner services.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/api"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/config"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/grpcclient"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/lease"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/outbox"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/partner"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/quotelocker"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/saga"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		fmtPrintln("config error:", err)
		return err
	}
	log := logging.New(cfg.LogLevel)
	ctx = logging.WithLogger(ctx, log)

	devMode := os.Getenv("DEV_MODE") == "1"

	// Open the store.  The Postgres pool is opened lazily; for Stage 2 unit
	// tests we accept either DB_URL (Postgres) or its absence (in-memory).
	var s store.Store
	if cfg.DBURL == "" {
		if !devMode {
			log.Error("DB_URL not set and DEV_MODE!=1; refusing to start in production mode")
			return errors.New("DB_URL not set and DEV_MODE!=1; refusing to start in production mode")
		}
		log.Warn("DEV_MODE=1: DB_URL unset — using in-memory store (NOT FOR PRODUCTION)")
		s = store.NewMemStore()
	} else {
		ps, err := store.NewPgStore(ctx, cfg.DBURL)
		if err != nil {
			log.Error("open postgres", "err", err)
			return err
		}
		defer ps.Close()
		s = ps
	}

	svc := api.NewService(s, quotelocker.NewNoop())

	// Build the saga partner clients.  Two modes:
	//   - DEV_MODE=1 (or ENABLE_STUB_PARTNERS=1): stubs are the default; any
	//     *_URL that is set and dials successfully replaces the stub for that
	//     partner.  Dial failures fall back to the stub with a warning.  This
	//     is the test / dev path.
	//   - DEV_MODE!=1 (the compose default): real partners are required.  The
	//     four gRPC partners (policy, kyt, mpc, ledger) must have their *_URL
	//     set and must dial successfully; any failure is fatal.
	//     payment-orchestration and blockchain-gateway are REST-only today and
	//     the orchestrator has no REST-to-gRPC adapter yet, so PAYMENT_URL /
	//     BLOCKCHAIN_URL being set is also fatal — set DEV_MODE=1
	//     until the gRPC adapter (workstream 1) lands.
	stub := partner.NewStub(partner.DefaultStubConfig())
	clients := &saga.Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	stubMode := devMode || os.Getenv("ENABLE_STUB_PARTNERS") == "1"
	if stubMode {
		log.Warn("DEV_MODE=1 or ENABLE_STUB_PARTNERS=1: using in-memory partner stubs; not safe for production")
	}
	grpcConns := dialPartners(ctx, cfg, clients, log, stubMode)
	defer closeConns(grpcConns)

	// Build the executor + lease manager + worker pool.
	execCfg := saga.Config{
		MaxRetries:  cfg.MaxRetries,
		BaseBackoff: time.Duration(cfg.RetryBaseBackoffMS) * time.Millisecond,
		MaxBackoff:  time.Duration(cfg.RetryMaxBackoffMS) * time.Millisecond,
		StepTimeout: cfg.StepTimeout,
	}
	exec := saga.NewExecutor(s, clients, execCfg)
	var leaseMgr saga.LeaseManager = saga.NoopLease{}
	if cfg.RedisURL != "" {
		rl, err := lease.NewRedisLease(cfg.RedisURL, cfg.LeaseTTLOffset)
		if err == nil {
			leaseMgr = rl
			defer rl.Close()
		} else {
			log.Warn("redis lease init failed; using no-op lease", "err", err)
		}
	}
	exec.Lease = leaseMgr

	dispatcher := worker.New(s, exec, cfg.WorkerConcurrency, "")
	if err := dispatcher.Recover(ctx); err != nil {
		log.Warn("crash recovery scan failed", "err", err)
	}
	dispatcher.Start(ctx)
	defer dispatcher.Stop()
	svc.Control = &worker.Control{Dispatcher: dispatcher, Executor: exec}

	// Start the outbox relay.
	pub, err := outbox.NewPublisher(cfg.EventBusURL)
	if err != nil {
		log.Warn("event-bus publisher init failed; outbox relay disabled", "err", err)
	} else {
		defer pub.Close()
		relay := outbox.NewRelay(s, pub, cfg.OutboxBatchSize, time.Duration(cfg.OutboxPollIntervalMS)*time.Millisecond)
		relay.Start(ctx)
		defer relay.Stop()
	}

	handler := api.Mux(svc)

	addr := ":" + cfg.Port
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Info("http listen", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			cancel()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutdown signal received")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("graceful shutdown", "err", err)
	}
	log.Info("bye")
	return nil
}

// fmtPrintln is a tiny indirection so tests can replace it.
var fmtPrintln = func(a ...any) { fmt.Println(toString(a)) }

func toString(a []any) string {
	s := ""
	for i, v := range a {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%v", v)
	}
	return s
}

// grpcConn bundles a *grpc.ClientConn with its name for deferred close.
type grpcConn struct {
	cc   *grpc.ClientConn
	name string
}

func closeConns(conns []*grpcConn) {
	for _, c := range conns {
		_ = c.cc.Close()
	}
}

// dialPartners dials every partner service whose *_URL is set to a non-stub
// endpoint and replaces the corresponding stub in clients with the real gRPC
// client.
//
// In stub mode (ENABLE_STUB_PARTNERS=1) a missing URL or a dial failure falls
// back to the stub and logs a warning.  In prod mode (ENABLE_STUB_PARTNERS !=
// "1") the four gRPC partners (policy, kyt, mpc, ledger) must have their URL
// set and must dial successfully; any failure is fatal.  payment and blockchain
// have no gRPC server yet and the orchestrator has no REST adapter, so a set
// PAYMENT_URL / BLOCKCHAIN_URL is fatal in prod mode — the operator must set
// ENABLE_STUB_PARTNERS=1 or implement the adapter (workstream 1).
func dialPartners(ctx context.Context, cfg config.Config, clients *saga.Clients, log *slog.Logger, stubMode bool) []*grpcConn {
	var conns []*grpcConn

	restOnly := []struct {
		name, url string
	}{
		{"payment", cfg.PaymentURL},
		{"blockchain", cfg.BlockchainURL},
	}
	for _, r := range restOnly {
		if r.url == "" || r.url == "stub://" {
			continue
		}
		if stubMode {
			log.Warn(r.name+" URL is set but orchestrator has no REST-to-gRPC adapter; staying on stub", "url", r.url)
			continue
		}
		log.Error(r.name + " gRPC adapter not yet implemented; set ENABLE_STUB_PARTNERS=1 or implement adapter (workstream 1)")
		os.Exit(1)
	}

	dials := []struct {
		name string
		url  string
	}{
		{"policy", cfg.PolicyURL},
		{"kyt", cfg.KytURL},
		{"mpc", cfg.MpcURL},
		{"ledger", cfg.LedgerURL},
	}
	for _, d := range dials {
		if d.url == "" || d.url == "stub://" {
			if stubMode {
				continue
			}
			log.Error("missing required partner URL (ENABLE_STUB_PARTNERS != 1)", "partner", d.name)
			os.Exit(1)
		}
		cc, err := dialOnePartner(ctx, d.name, d.url, clients, log, stubMode)
		if err != nil {
			os.Exit(1)
		}
		if cc != nil {
			conns = append(conns, &grpcConn{cc, d.name})
		}
	}
	return conns
}

func dialOnePartner(ctx context.Context, name, url string, clients *saga.Clients, log *slog.Logger, stubMode bool) (*grpc.ClientConn, error) {
	var (
		cc  *grpc.ClientConn
		err error
	)
	switch name {
	case "policy":
		var c *grpcclient.PolicyClient
		c, cc, err = grpcclient.NewPolicy(ctx, url)
		if err == nil {
			clients.Policy = c
		}
	case "kyt":
		var c *grpcclient.KytClient
		c, cc, err = grpcclient.NewKyt(ctx, url)
		if err == nil {
			clients.Kyt = c
		}
	case "mpc":
		var c *grpcclient.MpcClient
		c, cc, err = grpcclient.NewMpc(ctx, url)
		if err == nil {
			clients.Mpc = c
		}
	case "ledger":
		var c *grpcclient.LedgerClient
		c, cc, err = grpcclient.NewLedger(ctx, url)
		if err == nil {
			clients.Ledger = c
		}
	default:
		return nil, nil
	}
	if err != nil {
		if stubMode {
			log.Warn(name+" dial failed; using stub", "err", err)
			return nil, nil
		}
		log.Error(name+" dial failed (ENABLE_STUB_PARTNERS != 1)", "err", err)
		return nil, err
	}
	return cc, nil
}