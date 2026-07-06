CREATE TABLE transactions (
    tx_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      TEXT NOT NULL,
    quote_id     TEXT NOT NULL,
    amount       NUMERIC(20,8) NOT NULL,
    asset        TEXT NOT NULL,
    rail         TEXT NOT NULL,
    dest_address TEXT NOT NULL,
    status       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    version      BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX transactions_status_idx ON transactions (status);

CREATE TABLE transaction_steps (
    tx_id           UUID NOT NULL REFERENCES transactions (tx_id) ON DELETE CASCADE,
    step_name       TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempt         INTEGER NOT NULL,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error           TEXT,
    idempotency_key TEXT NOT NULL,
    PRIMARY KEY (tx_id, step_name, attempt)
);

CREATE UNIQUE INDEX transaction_steps_idem_key_idx ON transaction_steps (idempotency_key);

CREATE TABLE saga_state (
    tx_id            UUID PRIMARY KEY REFERENCES transactions (tx_id) ON DELETE CASCADE,
    current_step     TEXT NOT NULL,
    state            TEXT NOT NULL,
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    version          BIGINT NOT NULL DEFAULT 1
);

CREATE INDEX saga_state_state_idx ON saga_state (state);
CREATE INDEX saga_state_lease_expires_at_idx ON saga_state (lease_expires_at);

CREATE TABLE outbox_events (
    event_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tx_id        UUID NOT NULL REFERENCES transactions (tx_id) ON DELETE CASCADE,
    event_type   TEXT NOT NULL,
    step         TEXT,
    attempt      INTEGER,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'pending',
    dedup_key    TEXT NOT NULL
);

CREATE INDEX outbox_events_status_idx ON outbox_events (status);
CREATE UNIQUE INDEX outbox_events_dedup_key_idx ON outbox_events (dedup_key);