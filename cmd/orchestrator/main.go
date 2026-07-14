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

	// Open the store.  The Postgres pool is opened lazily; for Stage 2 unit
	// tests we accept either DB_URL (Postgres) or its absence (in-memory).
	var s store.Store
	if cfg.DBURL == "" {
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

	// Build the saga partner clients.  When a *_URL is set to "stub://" (or
	// empty) we use the in-memory Stub so the server can boot without real
	// partner services.  Otherwise we dial the gRPC endpoint.
	stub := partner.NewStub(partner.DefaultStubConfig())
	clients := &saga.Clients{Policy: stub, Payment: stub, Kyt: stub, Mpc: stub, Blockchain: stub, Ledger: stub, Audit: stub}
	grpcConns := dialPartners(ctx, cfg, clients, log)
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
// client.  Dial failures fall back to the stub and log a warning.
func dialPartners(ctx context.Context, cfg config.Config, clients *saga.Clients, log *slog.Logger) []*grpcConn {
	var conns []*grpcConn
	dials := []struct {
		name string
		url  string
	}{
		{"policy", cfg.PolicyURL},
		{"payment", cfg.PaymentURL},
		{"kyt", cfg.KytURL},
		{"mpc", cfg.MpcURL},
		{"blockchain", cfg.BlockchainURL},
		{"ledger", cfg.LedgerURL},
	}
	for _, d := range dials {
		if d.url == "" || d.url == "stub://" {
			continue
		}
		switch d.name {
		case "policy":
			c, cc, err := grpcclient.NewPolicy(ctx, d.url)
			if err != nil {
				log.Warn("policy dial failed; using stub", "err", err)
				continue
			}
			clients.Policy = c
			conns = append(conns, &grpcConn{cc, d.name})
		case "payment":
			c, cc, err := grpcclient.NewPayment(ctx, d.url)
			if err != nil {
				log.Warn("payment dial failed; using stub", "err", err)
				continue
			}
			clients.Payment = c
			conns = append(conns, &grpcConn{cc, d.name})
		case "kyt":
			c, cc, err := grpcclient.NewKyt(ctx, d.url)
			if err != nil {
				log.Warn("kyt dial failed; using stub", "err", err)
				continue
			}
			clients.Kyt = c
			conns = append(conns, &grpcConn{cc, d.name})
		case "mpc":
			c, cc, err := grpcclient.NewMpc(ctx, d.url)
			if err != nil {
				log.Warn("mpc dial failed; using stub", "err", err)
				continue
			}
			clients.Mpc = c
			conns = append(conns, &grpcConn{cc, d.name})
		case "blockchain":
			c, cc, err := grpcclient.NewBlockchain(ctx, d.url)
			if err != nil {
				log.Warn("blockchain dial failed; using stub", "err", err)
				continue
			}
			clients.Blockchain = c
			conns = append(conns, &grpcConn{cc, d.name})
		case "ledger":
			c, cc, err := grpcclient.NewLedger(ctx, d.url)
			if err != nil {
				log.Warn("ledger dial failed; using stub", "err", err)
				continue
			}
			clients.Ledger = c
			conns = append(conns, &grpcConn{cc, d.name})
		}
	}
	return conns
}