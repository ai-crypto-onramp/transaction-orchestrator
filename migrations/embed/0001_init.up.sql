-- migrations/0001_init.up.sql
-- Transaction Orchestrator: initial schema.
-- Tables: transactions, transaction_steps, saga_state, outbox_events.

CREATE TABLE IF NOT EXISTS transactions (
    tx_id        TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    quote_id     TEXT NOT NULL,
    amount       TEXT NOT NULL,
    asset        TEXT NOT NULL,
    rail         TEXT NOT NULL,
    dest_address TEXT NOT NULL,
    status       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version      BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions (status);
CREATE INDEX IF NOT EXISTS idx_transactions_user   ON transactions (user_id);

CREATE TABLE IF NOT EXISTS transaction_steps (
    tx_id          TEXT NOT NULL REFERENCES transactions(tx_id) ON DELETE CASCADE,
    step_name      TEXT NOT NULL,
    status         TEXT NOT NULL,
    attempt        INT  NOT NULL,
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    error          TEXT,
    idempotency_key TEXT NOT NULL,
    PRIMARY KEY (tx_id, step_name, attempt)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_steps_idem
    ON transaction_steps (tx_id, step_name, idempotency_key);

CREATE TABLE IF NOT EXISTS saga_state (
    tx_id            TEXT PRIMARY KEY REFERENCES transactions(tx_id) ON DELETE CASCADE,
    current_step     TEXT NOT NULL,
    state            TEXT NOT NULL,
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    version          BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tx_id        TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    step         TEXT,
    attempt      INT,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'pending',
    dedup_key    TEXT NOT NULL
);
CREATE INDEX        IF NOT EXISTS idx_outbox_status    ON outbox_events (status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_dedup     ON outbox_events (dedup_key);
CREATE INDEX        IF NOT EXISTS idx_outbox_tx       ON outbox_events (tx_id);