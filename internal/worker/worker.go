// Package worker is the in-process dispatcher that picks up in-flight sagas
// and drives them through the executor.
//
// It supports:
//   - manual enqueue via Submit,
//   - crash recovery on startup (scans saga_state for non-terminal rows),
//   - a bounded worker pool partitioned by tx_id hash.
//
// A single worker instance is safe for concurrent use.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/saga"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

// Dispatcher owns the worker pool and routes tx ids to workers.
type Dispatcher struct {
	Store     store.Store
	Executor  *saga.Executor
	Concurrency int
	Owner       string // lease owner identifier for this process

	jobs   chan string
	stop   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

// New returns a Dispatcher.
func New(s store.Store, ex *saga.Executor, concurrency int, owner string) *Dispatcher {
	if concurrency <= 0 {
		concurrency = 32
	}
	if owner == "" {
		owner = fmt.Sprintf("orch-%d", time.Now().UnixNano())
	}
	return &Dispatcher{
		Store: s, Executor: ex, Concurrency: concurrency, Owner: owner,
		jobs: make(chan string, concurrency*2),
		stop: make(chan struct{}),
	}
}

// Start spins up the worker pool.  It is idempotent.
func (d *Dispatcher) Start(ctx context.Context) {
	for i := 0; i < d.Concurrency; i++ {
		d.wg.Add(1)
		go d.worker(ctx, i)
	}
}

// Stop signals workers to drain and exit.
func (d *Dispatcher) Stop() {
	d.once.Do(func() { close(d.stop) })
	d.wg.Wait()
}

// Submit enqueues a tx id for saga execution.  It never blocks for longer
// than the dispatcher is alive.
func (d *Dispatcher) Submit(txID string) {
	select {
	case d.jobs <- txID:
	case <-d.stop:
	}
}

// Recover scans saga_state for non-terminal rows and enqueues them.  Called
// on startup; must complete within 30s (enforced by ctx).
func (d *Dispatcher) Recover(ctx context.Context) error {
	log := logging.From(ctx)
	rows, err := d.Store.ListInflightSagaIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range rows {
		d.Submit(id)
	}
	log.Info("recovery enqueued in-flight sagas", "count", len(rows))
	return nil
}

func (d *Dispatcher) worker(ctx context.Context, idx int) {
	defer d.wg.Done()
	log := logging.From(ctx)
	for {
		select {
		case <-d.stop:
			return
		case <-ctx.Done():
			return
		case txID := <-d.jobs:
			log.Info("running saga", "tx_id", txID)
			if err := d.Executor.Run(ctx, txID, d.Owner); err != nil {
				log.Error("saga run failed", "tx_id", txID, "err", err)
			}
		}
	}
}