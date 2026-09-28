# ADR-0008: Detect and refuse unsupported SUTs instead of reporting on them

**Status:** Accepted · **Date:** 2026-09-20

## Context

Some system shapes break the checker's model. A transactional consumer commits offsets inside a Kafka transaction (`TxnOffsetCommit`) and reads with `read_committed`, so Crashpoint's offset model does not describe what the application is doing. A consumer using manual partition assignment produces no group assignments, so ownership intervals cannot be built. Share groups (KIP-932) use an entirely different ownership model.

Listing these as "non-goals" doesn't prevent a user from pointing Crashpoint at one and receiving a confident, wrong answer. That would violate the project's second principle: no false positives.

## Options considered

1. **Document the limitation only.** Cheapest, and a trap for users.
2. **Best-effort handling.** Attempt to model transactional commits partially. High risk of subtly wrong verdicts, high implementation cost.
3. **Detect and refuse.** The proxy already sees every request, so the detection is nearly free.

## Decision

Option 3. On detecting `InitProducerId`, `AddOffsetsToTxn`, `TxnOffsetCommit`, or `EndTxn` on a SUT connection, manual assignment (no group protocol traffic in the startup window), or share-group APIs, the trial ends with `UNSUPPORTED` and a message naming the condition. A calibration trial that produces no effects ends with a configuration error rather than a loss report.

## Consequences

- Users get an accurate "I can't check this" instead of a plausible but wrong verdict.
- `UNSUPPORTED` becomes a first-class verdict, excluded from detection statistics.
- Transactional support remains a possible v2 feature, which would supersede this ADR in part.
