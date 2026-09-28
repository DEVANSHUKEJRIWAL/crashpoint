# ADR-0004: Observe application effects through Postgres logical replication

**Status:** Accepted · **Date:** 2026-09-20

## Context

The checker needs to know which effects the application committed, and in what order, without requiring application code changes.

## Options considered

1. **Poll the effects table after the trial.** Simple. Loses commit ordering (needed for I3 and I4), and can't feed live triggers.
2. **Application SDK that reports effects.** Precise, but requires code changes and trusting the application's own report of what it did, which defeats the point.
3. **Postgres logical replication slot.** Observes only committed rows, delivered in commit order (LSN), live, with no application changes.
4. **Database triggers writing to an audit table.** Works, but modifies the application's schema, and its ordering is less clear than LSNs.

## Decision

Use a logical replication slot on a publication covering the effects table.

## Consequences

- Postgres must run with `wal_level=logical`, and the connecting role needs replication permission. Both are easy in test environments.
- Each trial must drop its replication slot on cleanup, or WAL accumulates on disk.
- v1 supports Postgres only. Other effect sources (HTTP sink, other databases) come later through the same effect-event interface.
