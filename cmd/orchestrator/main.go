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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/api"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/config"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/quotelocker"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
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