// Package worker is the in-process dispatcher that picks up in-flight sagas
// and drives them through the executor.
//
// It supports:
//   - manual enqueue via Submit,
//   - crash recovery on startup (scans saga_state for non-terminal rows),
//   - a bounded worker pool partitioned by tx_id hash.
//   - a Control implementation backing the retry/compensate REST endpoints.
//
// A single worker instance is safe for concurrent use.
package worker

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/logging"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/saga"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/statemachine"
	"github.com/ai-crypto-onramp/transaction-orchestrator/internal/store"
)

// Dispatcher owns the worker pool and routes tx ids to workers.
type Dispatcher struct {
	Store     store.Store
	Executor  *saga.Executor
	Concurrency int
	Owner       string // lease owner identifier for this process

	jobs   chan string // legacy; unused after partitioned pool
	partitions []chan string
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
	d := &Dispatcher{
		Store: s, Executor: ex, Concurrency: concurrency, Owner: owner,
		jobs:  make(chan string, concurrency*2),
		stop:  make(chan struct{}),
	}
	d.partitions = make([]chan string, concurrency)
	for i := range d.partitions {
		d.partitions[i] = make(chan string, concurrency)
	}
	return d
}

// partitionOf returns the worker partition index for txID (stable hash).
func (d *Dispatcher) partitionOf(txID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(txID))
	return int(h.Sum32()) % len(d.partitions)
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
// than the dispatcher is alive.  The tx id is routed to a stable partition so
// the same tx is always processed by the same worker (in-order delivery).
func (d *Dispatcher) Submit(txID string) {
	p := d.partitionOf(txID)
	select {
	case d.partitions[p] <- txID:
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
	ch := d.partitions[idx]
	for {
		select {
		case <-d.stop:
			return
		case <-ctx.Done():
			return
		case txID := <-ch:
			log.Info("running saga", "tx_id", txID, "worker", idx)
			if err := d.Executor.Run(ctx, txID, d.Owner); err != nil {
				log.Error("saga run failed", "tx_id", txID, "err", err)
			}
		}
	}
}

// Control implements api.Control, backing the retry/compensate REST
// endpoints.  Retry re-enqueues the tx for saga execution; Compensate runs the
// compensation cascade directly.
type Control struct {
	Dispatcher *Dispatcher
	Executor   *saga.Executor
}

// Retry re-enqueues txID for saga execution.  If the saga is already terminal
// it returns an error so the operator knows there is nothing to retry.
func (c *Control) Retry(ctx context.Context, txID string, step statemachine.Step) error {
	sg, err := c.Executor.Store.LoadSagaState(ctx, txID)
	if err != nil {
		return err
	}
	if sg.State.Terminal() {
		return fmt.Errorf("saga is terminal (%s); nothing to retry", sg.State)
	}
	c.Dispatcher.Submit(txID)
	return nil
}

// Compensate runs the compensation cascade for txID directly on the executor
// (blocking the calling goroutine until complete).
func (c *Control) Compensate(ctx context.Context, txID string) error {
	sg, err := c.Executor.Store.LoadSagaState(ctx, txID)
	if err != nil {
		return err
	}
	if sg.State.IsFailure() {
		return fmt.Errorf("saga already in failure state %s", sg.State)
	}
	if sg.State.Terminal() {
		return fmt.Errorf("saga is terminal (%s); nothing to compensate", sg.State)
	}
	step := c.Executor.StepByName(sg.CurrentStep)
	if step == nil {
		return fmt.Errorf("no step for current_step=%s", sg.CurrentStep)
	}
	return c.Executor.CompensateCascade(ctx, txID, c.Dispatcher.Owner, step)
}