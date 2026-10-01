package main

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Idempotency is the entire contract of control mode and it's a money path, so
// it gets one real check against Postgres. Skips unless CRASHPOINT_TEST_DSN
// points at a database with sut/postgres/init.sql applied, e.g.:
//
//	CRASHPOINT_TEST_DSN=postgres://crashpoint:crashpoint@localhost:55432/crashpoint go test ./...
func TestProcessControlIdempotent(t *testing.T) {
	dsn := os.Getenv("CRASHPOINT_TEST_DSN")
	if dsn == "" {
		t.Skip("set CRASHPOINT_TEST_DSN to run (needs a Postgres with init.sql applied)")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	acct := int64(999000 + os.Getpid()%1000) // isolate from other rows / parallel runs
	ea, eb := "evt-a-"+t.Name(), "evt-b-"+t.Name()
	cleanup := func() {
		pool.Exec(ctx, `DELETE FROM ledger_entries WHERE account_id=$1`, acct)
		pool.Exec(ctx, `DELETE FROM processed_events WHERE event_id=ANY($1)`, []string{ea, eb})
		pool.Exec(ctx, `DELETE FROM accounts WHERE id=$1`, acct)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := Payment{EventID: ea, AccountID: acct, Seq: 1, Amount: json.Number("10.00")}

	// First delivery applies.
	if applied, err := processControl(ctx, pool, "w1", p); err != nil || !applied {
		t.Fatalf("first apply: applied=%v err=%v", applied, err)
	}
	// Redelivery (even from a different writer) is a no-op.
	if applied, err := processControl(ctx, pool, "w2", p); err != nil || applied {
		t.Fatalf("redelivery must be a no-op: applied=%v err=%v", applied, err)
	}
	// A distinct event applies again.
	p2 := Payment{EventID: eb, AccountID: acct, Seq: 2, Amount: json.Number("5.50")}
	if applied, err := processControl(ctx, pool, "w1", p2); err != nil || !applied {
		t.Fatalf("second event: applied=%v err=%v", applied, err)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE account_id=$1`, acct).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("want exactly 2 ledger rows (no duplicate), got %d", rows)
	}
	var balance string
	if err := pool.QueryRow(ctx, `SELECT balance::text FROM accounts WHERE id=$1`, acct).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != "15.50" {
		t.Fatalf("want balance 15.50 (10.00+5.50, each applied once), got %s", balance)
	}
}

// Happy-path smoke for bug3: one delivery is correct (bug invisible without
// faults); a redelivery double-writes (the bug only a redelivery exposes).
func TestProcessNoDedup(t *testing.T) {
	dsn := os.Getenv("CRASHPOINT_TEST_DSN")
	if dsn == "" {
		t.Skip("set CRASHPOINT_TEST_DSN to run (needs a Postgres with init.sql applied)")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	acct := int64(998000 + os.Getpid()%1000)
	ev := "evt-nd-" + t.Name()
	cleanup := func() {
		pool.Exec(ctx, `DELETE FROM ledger_entries WHERE account_id=$1`, acct)
		pool.Exec(ctx, `DELETE FROM accounts WHERE id=$1`, acct)
	}
	cleanup()
	t.Cleanup(cleanup)

	p := Payment{EventID: ev, AccountID: acct, Seq: 1, Amount: json.Number("7.25")}

	// Single delivery: correct.
	if applied, err := processNoDedup(ctx, pool, "w1", p); err != nil || !applied {
		t.Fatalf("first apply: applied=%v err=%v", applied, err)
	}
	var rows int
	pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE account_id=$1`, acct).Scan(&rows)
	if rows != 1 {
		t.Fatalf("happy path: want 1 ledger row, got %d", rows)
	}

	// Redelivery: no guard, so it double-writes — the bug this mode seeds.
	if _, err := processNoDedup(ctx, pool, "w1", p); err != nil {
		t.Fatalf("redelivery: err=%v", err)
	}
	pool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE account_id=$1`, acct).Scan(&rows)
	if rows != 2 {
		t.Fatalf("redelivery should duplicate (no dedup): want 2 rows, got %d", rows)
	}
	var balance string
	pool.QueryRow(ctx, `SELECT balance::text FROM accounts WHERE id=$1`, acct).Scan(&balance)
	if balance != "14.50" {
		t.Fatalf("double-applied balance: want 14.50, got %s", balance)
	}
}
