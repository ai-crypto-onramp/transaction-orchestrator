# Project Plan — Transaction Orchestrator

The Transaction Orchestrator drives each end-user crypto purchase as a durable
**saga**: `policy.evaluate -> payment.authorize+capture -> kyt.screen ->
mpc.sign -> blockchain.broadcast -> ledger.post`, with compensating actions on
partial failure and at-least-once event emission via the outbox pattern.

This plan breaks the build into eight ordered stages. Each stage is independently
mergeable, ends in a green CI run, and is tracked as a GitHub issue
("Stage N: <name>"). Stages 1–2 establish the substrate (DB + state machine,
API surface); Stages 3–8 add the six saga steps one at a time, then wire
outbox/event-bus, operational tooling, and tests/packaging on top.

## Stage 1

### Goal

Lay the durable storage substrate: PostgreSQL schema for `transactions`,
`transaction_steps`, `saga_state`, `outbox_events`, plus the in-process state
machine that governs transitions between saga states. Nothing here talks to a
partner service yet — it is pure persistence + state-machine logic.

### Tasks

- [x] Define Go module layout: `internal/`, `cmd/orchestrator/`, `cmd/orchctl/`,
      `internal/store/`, `internal/saga/`, `internal/statemachine/`.
- [x] Write SQL migrations (or embed via `golang-migrate`) for:
      - `transactions` (`tx_id`, `user_id`, `quote_id`, `amount`, `asset`,
        `rail`, `dest_address`, `status`, `created_at`, `updated_at`,
        `version`).
      - `transaction_steps` (`tx_id`, `step_name`, `status`, `attempt`,
        `started_at`, `finished_at`, `error`, `idempotency_key`) with unique
        index on `(tx_id, step_name, attempt)`.
      - `saga_state` (`tx_id`, `current_step`, `state`, `lease_owner`,
        `lease_expires_at`, `payload` JSONB, `version`).
      - `outbox_events` (`event_id`, `tx_id`, `event_type`, `step`, `attempt`,
        `payload` JSONB, `created_at`, `published_at`, `status`, dedup_key)
        with index on `(status)` and unique index on dedup_key.
- [x] Implement `internal/store/` with a `Store` interface (BeginTx, CreateTx,
      UpdateStep, LoadSagaState, SaveSagaState, AppendOutbox) backed by
      `pgxpool`.
- [x] Implement `internal/statemachine/` — typed states
      (`created, policy_checked, payment_captured, kyt_screened, signed,
      broadcasted, confirmed, ledgered, completed, failed_compensated,
      failed`), allowed transitions, guard that rejects illegal moves, and
      unit tests over the full transition table.
- [x] Add config loading (`DB_URL`, `REDIS_URL`, timeouts) via env vars (table
      in README) using `envconfig` or equivalent.

### Acceptance criteria

- `go build ./...` and `go test ./...` pass.
- `make migrate-up` applies all migrations against a throwaway Postgres
  container; `make migrate-down` rolls them back cleanly.
- State-machine unit tests assert every legal transition is allowed and every
  illegal one is rejected with a typed error.
- No partner gRPC client is imported anywhere in the tree yet.

## Stage 2

### Goal

Stand up the REST API tier and the transaction-creation entry point. A client
can `POST /v1/transactions`, which creates the top-level row, the initial
`saga_state` row (`created`), the six `transaction_steps` rows (`pending`),
and the first `outbox_events` row — all in one DB transaction — and returns a
`tx_id`. The quote lock semantics are stubbed for now (placeholder) but the
hook is in place.

### Tasks

- [x] Scaffold `cmd/orchestrator` (HTTP server, graceful shutdown, health
      `/healthz`, readiness `/readyz`).
- [x] Implement `POST /v1/transactions`:
      - Validate request body (`user_id`, `quote_id`, `amount`, `asset`,
        `rail`, `dest_address`).
      - Insert `transactions` row with `status = created`, `version = 1`.
      - Insert six `transaction_steps` rows (`pending`).
      - Insert `saga_state` (`state = created`, `current_step = policy`).
      - Insert `outbox_events` (`transaction.created`).
      - All within a single DB transaction.
- [x] Implement `GET /v1/transactions/:id` returning full state + current
      step + history.
- [x] Implement `GET /v1/transactions/:id/steps` returning ordered step list.
- [x] Add a `QuoteLocker` interface with a no-op default implementation and
      wire it into the create path (real Redis-backed impl in Stage 8).
- [x] Add structured logging (zap or slog) and request IDs.

### Acceptance criteria

- `curl -X POST /v1/transactions` returns `{tx_id}` and 201; the DB holds one
  `transactions` row, six `transaction_steps` rows, one `saga_state` row,
  and one `pending` `outbox_events` row.
- `GET /v1/transactions/:id` and `GET .../steps` return the expected JSON.
- Handler unit tests cover happy path + validation failures (400) + not-found
  (404).
- No saga step execution happens yet — endpoints only persist.

## Stage 3

### Goal

Implement Step 1 of the saga — `policy.evaluate` — the forward action only.
Wire the `policy-risk-engine` gRPC client, drive the state machine from
`created -> policy_checked`, and persist the result. Compensation is a no-op
for this step (no side effect yet) per the README matrix.

### Tasks

- [x] Define the `Step` interface in `internal/saga/`:
      `Execute(ctx, *SagaContext) (StepResult, error)` and
      `Compensate(ctx, *SagaContext) error`.
- [x] Generate gRPC client stubs for `policy-risk-engine` (proto contract).
- [x] Implement `PolicyStep.Execute`:
      - Call policy-risk-engine with `(user_id, quote_id, amount, asset,
        rail, dest_address)`.
      - Honor per-step timeout (`STEP_TIMEOUT_POLICY_SECONDS`).
      - On policy deny -> mark step `failed` and trigger terminal
        `failed_compensated` (no compensation needed for policy).
      - On policy allow -> mark step `succeeded`, transition saga to
        `policy_checked`, write `outbox_events` for the transition.
- [x] Implement `PolicyStep.Compensate` as a no-op that returns nil.
- [x] Idempotency: build `idempotency_key = sha256(tx_id || step_name ||
      attempt)`; the step refuses to execute twice for the same attempt.
- [x] Wire the worker dispatcher: when a `saga_state` row enters `created`,
      an in-process worker picks it up and runs `PolicyStep`.

### Acceptance criteria

- A created tx advances to `policy_checked` when the policy stub returns
  allow; the step row is `succeeded` and an outbox event is written.
- A policy deny moves the tx to `failed_compensated` with no compensation
  action executed.
- Retrying the same attempt is a no-op (idempotency key check).
- Unit tests use a mocked policy client covering allow / deny / timeout /
  transient-error-with-retry.

## Stage 4

### Goal

Implement Step 2 — `payment.authorize+capture` — forward action plus the first
real compensating action. Transitions `policy_checked -> payment_captured`.
On failure *before* capture: void authorization. On failure *at or after*
capture: refund.

### Tasks

- [x] Generate gRPC client stubs for `payment-orchestration`.
- [x] Implement `PaymentStep.Execute`:
      - Call `Authorize` then `Capture` on payment-orchestration.
      - Store `auth_id` and `capture_id` into `saga_state.payload` (JSONB).
      - Transition to `payment_captured` on success; write outbox event.
      - Honor `STEP_TIMEOUT_PAYMENT_SECONDS`.
- [x] Implement `PaymentStep.Compensate`:
      - If `auth_id` set and `capture_id` empty -> call `VoidAuthorization`.
      - If `capture_id` set -> call `Refund`.
      - Record compensation in `transaction_steps` (`compensating` ->
        `compensated`).
- [x] Update the partial-failure matrix: when `PaymentStep.Execute` fails, the
      orchestrator runs `PolicyStep.Compensate` (no-op) then
      `PaymentStep.Compensate` in reverse order.

### Acceptance criteria

- Happy path: `saga_state.payload` contains `auth_id` + `capture_id`; tx is
  `payment_captured`.
- Pre-capture failure: `VoidAuthorization` is called exactly once; tx is
  `failed_compensated`.
- Post-capture failure (injected): `Refund` is called exactly once; tx is
  `failed_compensated`.
- Compensation is executed in reverse saga order (policy no-op then payment).
- Idempotency keys prevent double authorize/capture on retry.

## Stage 5

### Goal

Implement Step 3 — `kyt.screen` — forward action with payment-refund
compensation. Transitions `payment_captured -> kyt_screened`. On KYT reject
or unrecoverable error: refund the captured payment and move to
`failed_compensated`.

### Tasks

- [x] Generate gRPC client stubs for `aml-kyt-screening`.
- [x] Implement `KytStep.Execute`:
      - Call `Screen` with `(user_id, dest_address, tx_id, amount, asset)`.
      - Honor `STEP_TIMEOUT_KYT_SECONDS` (note: KYT p99 <2s).
      - On `review`/`reject` -> mark failed, transition to
        `failed_compensated`.
      - On `clear` -> mark `succeeded`, transition to `kyt_screened`,
        write outbox event.
- [x] Implement `KytStep.Compensate`:
      - Triggers `PaymentStep.Compensate` (refund) — compensation cascades
        backward through completed steps.
- [x] Verify compensation ordering: policy (no-op) -> payment (refund) ->
  kyt (none of its own).

### Acceptance criteria

- Clear: tx advances to `kyt_screened`; outbox event written.
- Reject: payment is refunded; tx is `failed_compensated`; the
  `transaction_steps` row for payment shows `compensated`.
- Transient KYT error hits the retry path (exponential backoff, capped at
  `MAX_RETRIES`); after exhaustion, compensation runs.
- Idempotency: duplicate `Screen` calls for the same attempt are no-ops.

## Stage 6

### Goal

Implement Steps 4 & 5 — `mpc.sign` and `blockchain.broadcast` — together
because they share the "refund payment on failure" compensation and the
signed-payload handoff. Transitions `kyt_screened -> signed -> broadcasted
-> confirmed`.

### Tasks

- [x] Generate gRPC stubs for `mpc-signing-service` and `blockchain-gateway`.
- [x] Implement `MpcSignStep.Execute`:
      - Call `Sign` with unsigned tx hex from `saga_state.payload`.
      - Store `signed_tx_hex` into `saga_state.payload`.
      - Transition to `signed`; write outbox event.
- [x] Implement `MpcSignStep.Compensate`: refund payment (cascade).
- [x] Implement `BroadcastStep.Execute`:
      - Call `Broadcast` with `signed_tx_hex`; store returned `tx_hash`.
      - Transition to `broadcasted`; outbox event.
      - Guard against double-broadcast: idempotency key on
        `(tx_id, step_name, attempt)` plus a Redis lease so only one replica
        broadcasts.
- [x] Implement `BroadcastStep.Compensate`:
      - Refund payment, but note on-chain tx may already be in mempool — log
        the `tx_hash` to audit-event-log and mark for monitoring; do not
        attempt on-chain reversal.
- [x] Implement confirmation polling hook: `BroadcastStep` optionally waits
      for `confirmed` via blockchain-gateway status call (configurable; can
      be deferred to a separate poller).

### Acceptance criteria

- Happy path: tx reaches `broadcasted` with `tx_hash` stored; eventually
  `confirmed`.
- Sign failure: payment refunded; tx `failed_compensated`.
- Broadcast failure (pre-mempool accept): payment refunded; tx
  `failed_compensated`.
- Broadcast failure (post-mempool accept, simulated): refund still runs but
  audit-event-log records the `tx_hash` for monitoring.
- No double-broadcast across two simulated replicas holding the same lease.

## Stage 7

### Goal

Implement Step 6 — `ledger.post` — forward action and the asynchronous
reconciliation fallback (no on-chain compensation possible post-broadcast).
Transitions `confirmed -> ledgered -> completed`. Then build the full outbox
relay and event-bus emission so every transition flows to treasury /
notification / audit-event-log.

### Tasks

- [x] Generate gRPC stubs for `ledger-accounting`.
- [x] Implement `LedgerStep.Execute`:
      - Call `PostDoubleEntry` with `(tx_id, amount, asset, rail, user_id)`.
      - Store `ledger_journal_id` into `saga_state.payload`.
      - Transition to `ledgered` then `completed` (terminal success);
        outbox event.
- [x] Implement `LedgerStep.Compensate` as a no-op (per README: reconcile
      async; ledger posting is retried, no on-chain compensation).
      Persist a `ledger.reconcile_required` outbox event so an out-of-band
      job can reconcile.
- [x] Build the outbox relay worker in `internal/outbox/`:
      - Poll `outbox_events` with `SELECT ... FOR UPDATE SKIP LOCKED` in
        batches of `OUTBOX_BATCH_SIZE`.
      - Publish each event to the event bus (Kafka).
      - Mark rows `published` and set `published_at`.
      - Honor `OUTBOX_POLL_INTERVAL_MS`.
- [x] Implement event-bus publisher abstraction with a Kafka
      implementations behind one interface; selected by `EVENT_BUS_URL`
      scheme.
- [x] Emit events for: state transitions, step start/success/failure,
      compensation start/success, terminal states.

### Acceptance criteria

- A full happy-path saga emits all transition events in order; treasury
  receives `transaction.completed`.
- Relay is at-least-once; a simulated consumer dedups on
  `(tx_id, event_type, step, attempt)` and sees no duplicates after a relay
  restart.
- Ledger failure post-broadcast does NOT refund on-chain; instead a
  `ledger.reconcile_required` event is emitted and the saga retries ledger
  posting up to `MAX_RETRIES` before parking in `failed` (needs manual ops).
- Relay scales: two relay instances with `SKIP LOCKED` do not double-publish.

## Stage 8

### Goal

Add operational tooling and reliability hardening: bounded retry with
backoff + jitter, per-step timeouts, Redis-backed leases for single-flight
execution, crash recovery on startup, and the `orchctl` CLI for replay /
compensate / retry / dry-run. Close out with tests and the
production Docker image.

### Tasks

- [x] Implement retry: exponential backoff (`RETRY_BASE_BACKOFF_MS` ->
      `RETRY_MAX_BACKOFF_MS`) with jitter, capped at `MAX_RETRIES` per step.
- [x] Implement per-step timeouts wired from env (per-step overrides in
      README).
- [x] Implement Redis lease manager (`LEASE_TTL_SECONDS`) — acquire before
      executing a step; release on success; renew on long steps.
- [x] Implement crash recovery: on startup, scan `saga_state` for in-flight
      rows whose lease has expired; re-queue them. Recovery completes within
      30s of startup.
- [x] Build `cmd/orchctl`:
      - `orchctl retry --tx-id <id> --step <name>` — force-retry a failed
        step.
      - `orchctl compensate --tx-id <id>` — manual compensation flow.
      - `orchctl replay --tx-id <id> --dry-run` — replay from persisted
        step history against stub partner services (no side effects).
- [x] Implement worker pool with `WORKER_CONCURRENCY` cap and per-tx
      partitioning by `tx_id` hash.
- [ ] Integration test harness: spin up Postgres + Redis + six partner
      service stubs via `testcontainers-go`; run a full saga end-to-end plus
      a compensation scenario.
- [x] Coverage reported in CI; Codecov upload.
- [x] Finalize `Dockerfile` (multi-stage, distroless runtime) and
      `docker-compose.yml` for local dev with all dependencies.

### Acceptance criteria

- `orchctl replay --tx-id <id> --dry-run` reproduces the recorded saga with
  no side effects and matches the persisted step history.
- A process crash mid-saga followed by restart resumes from the last
  persisted step within 30s.
- Two orchestrator replicas cannot execute the same tx concurrently; the
  second waits on the lease.
- `go test -tags=integration ./...` passes end-to-end including a forced
  post-capture failure that triggers a refund.
- `go test ./...` coverage reported to Codecov; CI is green on push.
- `docker build` produces a working image runnable via `docker-compose up`.