# Transaction Orchestrator

![CI](https://github.com/ai-crypto-onramp/transaction-orchestrator/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/ai-crypto-onramp/transaction-orchestrator/branch/main/graph/badge.svg)](https://codecov.io/gh/ai-crypto-onramp/transaction-orchestrator)

The saga engine tying payment -> policy -> sign -> deliver into one atomic, recoverable flow with compensation.

## Overview / Responsibilities

The Transaction Orchestrator is the central service on the transaction path of the
crypto on-ramp. It drives each end-user purchase as a **saga**: a sequence of
discrete steps (policy check, payment capture, KYT screen, MPC sign, blockchain
broadcast, ledger posting) that together form one atomic, recoverable flow.

Responsibilities:

- Execute per-transaction sagas end-to-end, advancing each tx through a durable
  state machine.
- Guarantee atomicity across heterogeneous partner services via compensating
  actions on partial failure (e.g. refund payment if signing fails).
- Persist durable workflow state so in-flight sagas survive restarts and crashes.
- Emit domain events on every state transition via the outbox pattern.
- Provide operational tooling for retry, manual compensation, replay, and dry-run.
- Orchestrate many concurrent transactions at scale with per-step timeouts and
  bounded retry with backoff.
- Audit every transition to the audit-event-log.

## Language & Tech Stack

- **Language:** Go
- **Pattern:** SAGA orchestration with explicit step functions and compensating
  actions per step.
- **Event emission:** Outbox pattern — events written transactionally with saga
  state, relayed to the event bus by a separate poller/publisher.
- **Durable state:** Workflow state persisted in PostgreSQL; in-flight sagas are
  recovered on startup and resumed from their last persisted step.
- **Concurrency:** Goroutine-per-tx with worker pool and Redis-backed lease/
  locking for single-flight execution of a given tx across replicas.
- **RPC:** REST (public/clients) + internal gRPC (partner services on the saga
  path).

## System Requirements

- Atomic end-to-end transaction flow with compensation across all six steps.
- Per-tx state machine:
  `created -> policy_checked -> payment_captured -> kyt_screened -> signed -> broadcasted -> confirmed -> ledgered -> completed`
  with terminal alternative `failed_compensated` (and `failed` for unrecoverable
  failures after compensation attempts exhausted).
- Idempotent step execution — each step keyed by `(tx_id, step_name, attempt)`;
  partner services must be safe to retry.
- Retry with exponential backoff per step, capped, with jitter.
- Timeout per step (configurable globally and overridable per partner/rail).
- Manual recovery tooling: force-retry, manual compensate, replay from step,
  dry-run mode that executes the saga against stubs/no side effects.
- Partial-failure compensation matrix:
  - `payment.capture` failed before capture -> void authorization.
  - `mpc.sign` or `blockchain.broadcast` fails after capture -> refund payment.
  - `ledger.post` fails after broadcast -> reconcile asynchronously; ledger
    posting is retried (no on-chain compensation possible).
- Concurrent tx orchestration at scale — thousands of in-flight sagas per
  instance, horizontally scalable, partitioned by tx_id.
- Event emission at each transition (state change, step start, step success,
  step failure, compensation start, compensation success, terminal state).

## Non-Functional Requirements

- Step latency targets per partner (e.g. policy <100ms p99, payment <500ms p99,
  KYT <2s p99, MPC sign <3s p99, broadcast <1s p99, ledger <200ms p99).
- Durability across restarts — no in-flight saga is lost on process crash;
  recovery completes within 30s of startup.
- No double-spend — payment capture and on-chain broadcast are each guarded by
  idempotency keys and leases; a tx is never broadcast twice.
- At-least-once event emission with dedup — outbox relay guarantees delivery;
  consumers dedup on `(tx_id, event_type, step, attempt)`.
- 99.99% availability — stateless API tier behind a load balancer; stateful
  workers fail over via Redis leases and DB-locked recovery.
- Audit every transition — every state change is written to the audit-event-log
  (async) with full context (tx_id, step, attempt, actor, timestamp, before/after
  state, error).

## Technical Specifications

### API Surface

- **REST** — public/clients and partner-facing operational endpoints (JSON).
- **Internal gRPC** — calls to saga partner services (policy, payment, KYT, MPC,
  blockchain-gateway, ledger). gRPC clients are configured per service with
  per-call timeouts and retry policies.

### Endpoints

| Method | Path | Body / Response |
|---|---|---|
| POST | `/v1/transactions` | Request: `{user_id, quote_id, amount, asset, rail, dest_address}` -> Response: `{tx_id}` |
| GET | `/v1/transactions/:id` | Response: full tx state + current step + history |
| POST | `/v1/transactions/:id/retry` | Retries the current/failed step; idempotent |
| POST | `/v1/transactions/:id/compensate` | Triggers manual compensation flow |
| GET | `/v1/transactions/:id/steps` | Response: ordered list of saga steps with status, attempts, timestamps |

### Saga Steps

| # | Step | Forward action | Compensation |
|---|---|---|---|
| 1 | `policy.evaluate` | Call policy-risk-engine; gate before any funds move. | None (no side effect yet). |
| 2 | `payment.authorize+capture` | Call payment-orchestration: authorize then capture fiat. | Void authorization; if captured, refund payment. |
| 3 | `kyt.screen` | Call aml-kyt-screening on destination address. | Refund payment (capture already done). |
| 4 | `mpc.sign` | Call mpc-signing-service for threshold signature. | Refund payment. |
| 5 | `blockchain.broadcast` | Call blockchain-gateway to broadcast signed tx. | Refund payment (broadcast may already be in mempool — handled by idempotency + monitoring). |
| 6 | `ledger.post` | Call ledger-accounting to post double-entry. | None (reconcile async if posting fails post-broadcast). |

### Data Model

- **transactions** — top-level tx record: `tx_id`, `user_id`, `quote_id`,
  `amount`, `asset`, `rail`, `dest_address`, `status` (state-machine value),
  `created_at`, `updated_at`, `version`.
- **transaction_steps** — one row per `(tx_id, step_name)` with `status`
  (`pending|running|succeeded|failed|compensating|compensated`), `attempt`,
  `started_at`, `finished_at`, `error`, `idempotency_key`.
- **saga_state** — durable workflow snapshot: `tx_id`, `current_step`,
  `state` (state-machine value), `lease_owner`, `lease_expires_at`, `payload`
  (JSONB — accumulated context: auth_id, capture_id, signed_tx_hex, tx_hash,
  ledger_journal_id), `version`.
- **outbox_events** — `event_id`, `tx_id`, `event_type`, `step`, `attempt`,
  `payload` (JSONB), `created_at`, `published_at`, `status`
  (`pending|published`), dedup key.

### State Machine (text)

```
                            +-----------------+
                            |     created     |
                            +-----------------+
                                    |
                                    v
                            +-----------------+
                            | policy_checked  |
                            +-----------------+
                                    |
                                    v
                            +-----------------+
                            | payment_captured |
                            +-----------------+
                                    |
                                    v
                              +-----------------+
                              |  kyt_screened   |
                              +-----------------+
                                    |
                                    v
                              +-----------------+
                              |     signed      |
                              +-----------------+
                                    |
                                    v
                              +-----------------+
                              |   broadcasted   |
                              +-----------------+
                                    |
                                    v
                              +-----------------+
                              |    confirmed    |
                              +-----------------+
                                    |
                                    v
                              +-----------------+
                              |    ledgered     |
                              +-----------------+
                                    |
                                    v
                              +-----------------+
                              |    completed    | (terminal, success)
                              +-----------------+

  Any non-terminal state, on unrecoverable step failure:
                              +---------------------+
                              | failed_compensated  | (terminal, compensated)
                              +---------------------+

  Compensation exhausted / unrecoverable:
                              +-----------------+
                              |     failed      | (terminal, needs manual ops)
                              +-----------------+
```

### Integrations

**Synchronous (saga path):**

- `policy-risk-engine` — step 1 (policy.evaluate)
- `payment-orchestration` — step 2 (payment.authorize+capture)
- `aml-kyt-screening` — step 3 (kyt.screen)
- `mpc-signing-service` — step 4 (mpc.sign)
- `blockchain-gateway` — step 5 (blockchain.broadcast)
- `ledger-accounting` — step 6 (ledger.post)

**Asynchronous (event bus):**

- `treasury-orchestration` — receives `transaction.completed` to batch into
  aggregate buys / manage float.
- `notification` — receives all transition events for user/partner comms.
- `audit-event-log` — receives every transition event for compliance forensics.

### Outbox + Event Bus

- Every state transition writes a row to `outbox_events` in the **same DB
  transaction** as the `saga_state`/`transaction_steps` update, guaranteeing
  atomic state + event persistence (no lost events on crash).
- A dedicated relay worker polls `outbox_events` (status = `pending`) and
  publishes to the event bus (e.g. NATS/Kafka), then marks rows `published`.
- At-least-once delivery; consumers dedup on the outbox dedup key
  `(tx_id, event_type, step, attempt)`.
- Relay is horizontally scalable with row-level locking (`SELECT ... FOR UPDATE
  SKIP LOCKED`).

## Dependencies

- **PostgreSQL** — durable saga state, transaction_steps, outbox_events.
- **Redis** — per-tx execution leases, distributed locks, rate-limiting of
  partner calls, retry backoff coordination.
- **policy-risk-engine** (gRPC)
- **payment-orchestration** (gRPC)
- **aml-kyt-screening** (gRPC)
- **mpc-signing-service** (gRPC)
- **blockchain-gateway** (gRPC)
- **ledger-accounting** (gRPC)
- **audit-event-log** (async via event bus)
- **Event bus** (NATS or Kafka) — outbox relay target.

## Configuration

| Env var | Description | Default |
|---|---|---|
| `PORT` | REST API listen port. | `8080` |
| `GRPC_PORT` | Internal gRPC listen port (if exposed for ops). | `9090` |
| `DB_URL` | PostgreSQL DSN for saga state + outbox. | required |
| `REDIS_URL` | Redis address for leases/locks. | required |
| `POLICY_URL` | policy-risk-engine gRPC endpoint. | required |
| `PAYMENT_URL` | payment-orchestration gRPC endpoint. | required |
| `KYT_URL` | aml-kyt-screening gRPC endpoint. | required |
| `MPC_URL` | mpc-signing-service gRPC endpoint. | required |
| `BLOCKCHAIN_URL` | blockchain-gateway gRPC endpoint. | required |
| `LEDGER_URL` | ledger-accounting gRPC endpoint. | required |
| `STEP_TIMEOUT_SECONDS` | Default per-step timeout (overridable per step). | `30` |
| `STEP_TIMEOUT_POLICY_SECONDS` | Override for policy.evaluate. | `5` |
| `STEP_TIMEOUT_PAYMENT_SECONDS` | Override for payment step. | `30` |
| `STEP_TIMEOUT_KYT_SECONDS` | Override for kyt.screen. | `15` |
| `STEP_TIMEOUT_MPC_SECONDS` | Override for mpc.sign. | `20` |
| `STEP_TIMEOUT_BROADCAST_SECONDS` | Override for blockchain.broadcast. | `30` |
| `STEP_TIMEOUT_LEDGER_SECONDS` | Override for ledger.post. | `10` |
| `MAX_RETRIES` | Max retry attempts per step. | `5` |
| `RETRY_BASE_BACKOFF_MS` | Initial backoff for step retries. | `200` |
| `RETRY_MAX_BACKOFF_MS` | Cap for exponential backoff. | `10000` |
| `EVENT_BUS_URL` | Event bus (NATS/Kafka) broker URL for outbox relay. | required |
| `OUTBOX_POLL_INTERVAL_MS` | Outbox relay poll interval. | `100` |
| `OUTBOX_BATCH_SIZE` | Max events published per relay poll. | `100` |
| `WORKER_CONCURRENCY` | Max concurrent sagas per instance. | `256` |
| `LEASE_TTL_SECONDS` | Redis lease TTL for single-flight saga execution. | `30` |
| `LOG_LEVEL` | Log level (`debug`/`info`/`warn`/`error`). | `info` |

## Local Development

```bash
# Build
go build -o bin/orchestrator ./cmd/orchestrator
go build -o bin/orchctl ./cmd/orchctl

# Run (requires PostgreSQL, Redis, and stubbed partner services)
go run ./cmd/orchestrator

# Test
go test ./...

# Integration tests (spins up partner service stubs)
go test -tags=integration ./...

# Migrations (embedded SQL, applied via the migrate command).
# make migrate-up applies all *.up.sql; make migrate-down rolls them back.
# The default DSN points at a throwaway local Postgres on :5433 — spin one up
# with `make pg-start`, or point DB_DSN at your own instance.
make migrate-up DB_DSN="postgres://orch:orch@localhost:5432/orch"
make migrate-down DB_DSN="postgres://orch:orch@localhost:5432/orch"

# Saga replay tooling — replay a finished tx from its persisted step history
# against stub partner services (dry-run, no side effects):
go run ./cmd/orchctl replay --tx-id <tx_id> --dry-run

# Manual compensate a stuck tx:
go run ./cmd/orchctl compensate --tx-id <tx_id>

# Force-retry a failed step:
go run ./cmd/orchctl retry --tx-id <tx_id> --step <step_name>
```
