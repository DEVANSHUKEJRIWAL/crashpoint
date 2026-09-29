-- Replication role for Crashpoint's CDC reader.
-- Does not need superuser; REPLICATION + SELECT on the effects table is sufficient.
CREATE ROLE crashpoint WITH REPLICATION LOGIN PASSWORD 'crashpoint';

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------

-- Accounts: one row per account.
-- balance is the I7 aggregate oracle: the checker sums ledger_entries.amount
-- per account and compares to this value.
CREATE TABLE accounts (
    id      bigserial PRIMARY KEY,
    balance numeric   NOT NULL DEFAULT 0
);

-- Effects table.
-- Deliberately NO UNIQUE constraint on event_id.
-- Adding UNIQUE(event_id) here would let the database silently enforce
-- at-most-once and mask corpus bugs 3 and 4 (duplicate effects).
-- The application is supposed to enforce deduplication itself via
-- processed_events; Crashpoint's job is to verify that it does.
CREATE TABLE ledger_entries (
    id         bigserial   PRIMARY KEY,
    account_id bigint      NOT NULL REFERENCES accounts (id),
    event_id   text        NOT NULL,   -- T1: id of the input that caused this row
    seq        bigint      NOT NULL,   -- T2: per-key sequence number (for I3 ordering)
    writer_id  text,                   -- T3: consumer instance id (nullable; enables precise I4)
    amount     numeric     NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Idempotent-consumer dedup table.
-- event_id IS unique here: this is the guard correct applications use to
-- prevent duplicate effects.  Applications that mis-time or skip this check
-- produce duplicate ledger_entries rows, which Crashpoint detects.
CREATE TABLE processed_events (
    event_id     text        PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Logical replication
-- ---------------------------------------------------------------------------

-- Publication for Crashpoint's effect-stream reader.
-- Covers ledger_entries only; other tables are not effects.
CREATE PUBLICATION crashpoint_effects FOR TABLE ledger_entries;

-- Allow the replication role to read the published table (needed for the
-- initial snapshot when creating a subscription or replication slot).
GRANT SELECT ON ledger_entries TO crashpoint;
