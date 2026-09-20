# ADR-0006: Tier the invariants by available instrumentation

**Status:** Accepted · **Date:** _(date)_

## Context

The checker needs evidence: which input caused an effect (`event_id`), the business key and sequence number for ordering, and which consumer instance wrote a row for zombie detection. The reference SUT has all of these because we wrote it. Real applications usually have an event id (the idempotent-consumer pattern is widespread), rarely a source sequence number, and almost never a writer-identity column.

The original design claimed "no application changes" while assuming all of these columns — a contradiction that any evaluator would find in five minutes.

## Options considered

1. **Require the full schema.** Precise checks, but adoption drops to roughly zero and the headline claim is false.
2. **Drop the invariants that need extra columns.** Honest, but the tool loses most of its value.
3. **Tier the invariants by available evidence**, enable only what is supported, and state the tier explicitly. Add a T0 oracle (state convergence) that needs no schema changes: the final aggregate state must equal applying each acknowledged input exactly once.

## Decision

Option 3. Four tiers (T0–T3), with T1 as the realistic default. Crashpoint prints the active tier at startup, plus the column that would unlock each disabled invariant. Zombie detection degrades at T1/T2 to "duplicate with concurrent ownership," which reports the duplicate plus proxy-side ownership evidence without claiming a proven zombie write.

## Consequences

- The tool can run against applications nobody instrumented for it, which makes evaluation possible at all.
- The README claim becomes accurate: no logic changes ever; schema changes only to unlock higher tiers.
- More checker code paths, and tier must be recorded with every benchmark result, or detection rates aren't comparable.
- T0 requires an arithmetically composable workload, so it is generated-mode only.
