package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Payment is the input contract on payments.requested. The workload producer
// (issue #4) and observed-mode identity extraction (v2) must both yield this
// shape. Amount stays a json.Number so the exact decimal text reaches Postgres
// NUMERIC without a float round-trip — this is a money path, floats need not
// apply.
type Payment struct {
	EventID   string      `json:"event_id"`   // T1: identity of the causing input
	AccountID int64       `json:"account_id"` // T2: per-key sequence is scoped to this
	Seq       int64       `json:"seq"`        // T2: per-key sequence number
	Amount    json.Number `json:"amount"`
}

// validate rejects records at the trust boundary before any DB work. The
// workload only emits valid JSON, so this is defensive, not hot-path.
func (p Payment) validate() error {
	if p.EventID == "" {
		return fmt.Errorf("empty event_id")
	}
	if p.AccountID <= 0 {
		return fmt.Errorf("non-positive account_id %d", p.AccountID)
	}
	if _, ok := new(big.Rat).SetString(string(p.Amount)); !ok {
		return fmt.Errorf("amount %q is not a valid decimal", p.Amount)
	}
	return nil
}

// processControl applies one payment the correct, idempotent way: the dedup
// marker and the effect live in ONE transaction, so a crash can never leave a
// ledger row without its guard or a guard without its row. The caller commits
// the Kafka offset only AFTER this returns nil — read-process-commit ordering.
//
// applied == false means event_id was already processed (a duplicate delivery):
// correct behaviour, not an error, and the offset should still advance.
//
// This is DESIGN.md corpus row C. It must never produce a violation. Bug modes
// 1-3 (issue #3) will branch elsewhere; they do not touch this function.
func processControl(ctx context.Context, pool *pgxpool.Pool, writerID string, p Payment) (applied bool, err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	// The idempotency guard. ON CONFLICT DO NOTHING makes a redelivery a no-op:
	// 0 rows affected means this event's effect is already durable.
	ct, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, p.EventID)
	if err != nil {
		return false, fmt.Errorf("dedup insert: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}

	// Ensure the FK parent row exists.
	// ponytail: explicit-id insert doesn't advance the bigserial, which is fine
	// because accounts are only ever created here, keyed by the input's
	// account_id; nothing else inserts into accounts.
	if _, err = tx.Exec(ctx, `INSERT INTO accounts (id, balance) VALUES ($1, 0) ON CONFLICT DO NOTHING`, p.AccountID); err != nil {
		return false, fmt.Errorf("ensure account: %w", err)
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO ledger_entries (account_id, event_id, seq, writer_id, amount) VALUES ($1, $2, $3, $4, $5::numeric)`,
		p.AccountID, p.EventID, p.Seq, writerID, string(p.Amount)); err != nil {
		return false, fmt.Errorf("insert effect: %w", err)
	}
	if _, err = tx.Exec(ctx,
		`UPDATE accounts SET balance = balance + $1::numeric WHERE id = $2`,
		string(p.Amount), p.AccountID); err != nil {
		return false, fmt.Errorf("apply balance: %w", err)
	}
	return true, tx.Commit(ctx)
}
