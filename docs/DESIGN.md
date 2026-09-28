# Crashpoint Design: The Correctness Contract

**Version:** 0.4 · **Status:** Accepted for implementation · **Owner:** Devanshu Kejriwal

This document defines precisely what Crashpoint checks, under which assumptions, and what it cannot detect. The checker implementation must follow these definitions exactly. If the code and this document disagree, the document is fixed first, then the code.

See [SUPPORTED.md](SUPPORTED.md) for what Crashpoint runs against, and [COMPETITORS.md](COMPETITORS.md) for how it differs from existing tools.

---

## 1. Goals and non-goals

### Goals (v1)

- **G1.** Detect lost, duplicated, reordered, and concurrently-written effects in Kafka consumer applications that write to Postgres.
- **G2.** Work with **minimal instrumentation**, and be explicit about what each level of instrumentation buys. Crashpoint never requires changes to application *logic*; at the lowest tier it requires no schema changes at all. See Section 3.
- **G3.** Place faults at precise, event-triggered protocol moments rather than at random times.
- **G4.** Never report a violation that did not happen. When evidence is insufficient, stay silent or refuse to run.
- **G5.** Shrink any failure to a minimal, replayable fault plan.

### Non-goals (v1)

- Testing Kafka brokers themselves.
- Transactional / exactly-once consume-transform-produce pipelines. These are **detected and refused** (Section 8), not silently mishandled.
- Share groups (KIP-932). KIP-848 group protocol support is a stretch goal, not v1.
- Effects anywhere other than Postgres.
- Load testing; Crashpoint is a correctness tool.
- Full determinism. Reproducibility is measured and reported, never claimed.

---

## 2. Terminology

| Term | Definition |
|---|---|
| **SUT** | System under test: consumer instances, their topics, and their Postgres database. |
| **Input** | An event on the input topic, with an identity (`event_id`), a key, and a per-key sequence number `seq`. |
| **Input status** | *Acknowledged*, *failed*, or *indeterminate* (timed out: may or may not exist). |
| **Effect** | A row committed to the effects table caused by an input. |
| **Member** | One consumer instance in the group, identified by member ID. |
| **Ownership epoch** | Classic protocol: generation ID. KIP-848: member epoch. |
| **Ownership interval** | A span, in the proxy's event order, during which a member held a partition at a given epoch. |
| **Delivered range** | Offsets a member received in a Fetch response, clamped to the requested fetch offset. |
| **Quiescence** | The post-fault phase during which the SUT is left to recover before checking. |
| **Trial** | One run: provision → workload + faults → quiescence → check → verdict. |
| **Fault plan** | A versioned JSON document describing faults, their triggers, and actions. |
| **Violation** | An invariant broken by the SUT. |
| **Harness error** | Crashpoint itself failed or lost data. Never reported as a violation. |
| **Tier** | The instrumentation level available in the SUT's schema, which determines the checkable invariants (Section 3). |

---

## 3. Instrumentation tiers

Crashpoint enables only the invariants its evidence can support. It determines the tier from the configured schema mapping, prints it at startup, and records it with every result.

| Tier | Requires in the effects table | Invariants enabled |
|---|---|---|
| **T0** | Nothing beyond a table whose rows are the effects, plus a numeric column to aggregate | I6 liveness, I7 state convergence |
| **T1** | An `event_id` column identifying the causing input | + I1 no lost effects, I2 at most one effect, I5 commit-ahead warning |
| **T2** | + key column and source `seq` column | + I3 per-key order |
| **T3** | + a writer-identity column naming the consumer instance | + I4 zombie writes (precise form) |

**T1 is the realistic default**, because the idempotent-consumer pattern (a `processed_events` table keyed by event ID) is already widespread. T0 exists so Crashpoint can run against applications nobody instrumented for it. T3 costs one extra column and is the only way to attribute a write to a specific consumer instance; without it, I4 degrades as described in Section 5.

Crashpoint must never silently skip an invariant. Startup output names the tier, the enabled invariants, and, for each disabled one, the column that would enable it.

---

## 4. Input modes and identity

The checker needs to know which inputs exist and where they landed.

### Generated mode (v1)

Crashpoint produces the inputs and records each one's status, partition, and offset. Payloads come from a **payload encoder**; v1 ships JSON. A SUT whose records use another format (Avro, Protobuf, Schema Registry framing) needs an encoder for that format, or observed mode.

### Observed mode (v2, interface designed in v1)

The SUT's own producer sends traffic through the proxy. Crashpoint extracts each input's identity from the observed Produce request using a configured **identity extractor**: a record header name, or a JSON path into the value. The proxy already sees produce responses, so it learns the assigned partitions and offsets with no extra machinery.

The checker must depend only on the `Input` interface, never on the fact that Crashpoint produced the data.

### Processing time is a workload dimension, not a fault

How long the SUT takes per record decides which bugs are reachable, so it is configured per trial (`work_delay`) and varied by the explorer, independently of faults. At least one scenario must run the handler slower than the consumer's poll interval with **no faults armed**: client libraries have lost records in exactly that situation, with no crash, rebalance, or broker involvement (corpus bug 8).

### Input status handling

- **Acknowledged** inputs are checked for loss.
- **Failed** inputs are ignored.
- **Indeterminate** inputs are excluded from loss checks but included in duplicate checks.
- Inputs are identified by `event_id`, never by offset. A producer retry can place one input at two offsets; that is one input, not two.

---

## 5. Invariants

### I1. No lost effects (T1)

**Definition.** For every acknowledged input for which an effect is expected, at least one effect exists at the end of quiescence.

**Expected effects.** By default every input is expected to produce an effect. A SUT that legitimately filters records must declare an `expect_effect` predicate in the config (for example, "amount > 0"), or use T0 instead. Undeclared filtering produces false losses, so Crashpoint runs a fault-free calibration trial first and warns when inputs and effects diverge beyond a configured tolerance.

**Risk.** Ending quiescence too early makes a slow consumer look lossy. Mitigated by Section 6: if the system hasn't finished making progress, the verdict is I6, never I1.

### I2. At most one effect per input (T1)

**Definition.** For every input, at most one effect exists.

**Note.** Duplicate input *records* (one `event_id` at two offsets) are expected after producer retries and are not violations.

### I3. Per-key order (T2)

**Definition.** For every key, effects ordered by `(commit LSN, position within the transaction stream)` have strictly increasing `seq`. Gaps are permitted; missing effects are I1's concern.

**Tie-break.** Effects committed in one transaction share an LSN. Logical decoding delivers them in a defined order within the transaction, and that order is the tie-break. Without this rule, a batch consumer writing several effects per transaction produces spurious violations.

**Configurable.** A SUT that intentionally processes a key out of order disables this invariant.

### I4. Zombie writes

**T3 (precise).** An effect *e* for partition *p*, committed at LSN *L* by member *m* at epoch *x*, is a violation if there exists an effect *e′* for *p* committed at LSN *L′ < L* by a member whose ownership epoch is greater than *x*. In words: the old owner wrote after the new owner had already written.

**T1/T2 (degraded: "duplicate with concurrent ownership").** Without writer identity, Crashpoint cannot attribute an effect to a member. When it finds a duplicate (I2) whose two effects fall inside a window where two members held overlapping ownership of the same partition, it reports the duplicate **and attaches the ownership evidence**, labelled "duplicate with concurrent ownership." It must not call this a proven zombie write.

**Risk.** Both forms may miss zombies that finish before the new owner's first write. Missing a violation is acceptable; inventing one is not.

### I5. Commit not ahead of effects (T1, warning level)

**Definition.** When the coordinator accepts an OffsetCommit for partition *p* at offset *o*, every acknowledged input in *p* below *o* that is expected to have an effect should already have one visible in CDC.

**Severity: WARNING.** The check spans two sources with no shared clock, so replication lag can make a correct commit look early. It escalates to a violation only when I1 later confirms a loss for an affected input.

**Value.** It exposes commit-ordering bugs even in trials where no crash happened to hit the window.

### I6. Liveness (T0)

**Definition.** After faults stop, committed offsets reach the high-water mark for every input partition and every expected effect exists, within the quiescence rules of Section 6.

### I7. State convergence (T0)

**Definition.** For an aggregate column (for example a balance), the final aggregate state equals the value obtained by applying each acknowledged input exactly once.

**Purpose.** T0's oracle. It catches loss and duplication in aggregate without naming the offending event, and needs no schema changes at all.

**Requirement.** The workload must be arithmetically composable, so it is available only in generated mode with a numeric amount field.

---

## 6. Quiescence procedure

1. Stop the workload, then stop fault injection and release all held frames.
2. Restore network rules and restart stopped or paused consumers, returning to the configured instance count.
3. Wait until **both**: committed offsets equal the high-water mark for every input partition, and the CDC reader has caught up to the database's current WAL position.
4. **Progress rule.** The wait extends while committed offsets or effects keep advancing, up to a hard cap (default: 30 s base, 120 s cap). If progress stalls for 30 s without completing, record an **I6 violation**. If the hard cap is reached while still progressing, record `HARNESS_ERROR: quiescence cap reached`, because a slow environment is not an application bug.
5. When I6 fires, skip I1 and I7: loss checks would be unreliable.
6. Confirm that every armed fault either fired or was explicitly released as a missed window. A trial must never be checked while a fault is still pending; otherwise it can pass before the fault ever had a chance to act.
7. Run the checker.

---

## 7. Faults

### 7.1 Catalogue

| Fault | Layer | What it models | Version |
|---|---|---|---|
| `kill` (SIGKILL) | Process | Crash, OOM kill, node loss | v1 |
| `pause` / `unpause` (cgroup freeze) | Process | Long GC pause, VM stall, CPU starvation | v1 |
| `blackhole` / `unblackhole` a member's Kafka traffic | Proxy | Network partition between one instance and Kafka while its database stays reachable: the process keeps running and writing while the group evicts it (the most realistic zombie generator) | v1 |
| `hold` request until trigger | Proxy | Precise placement of a crash relative to a request | v1 |
| `drop_response` | Proxy | Lost acknowledgement: the broker applied it, the client never learns | v1 |
| `stop` (SIGTERM) | Process | Rolling deploy | v1 |
| `drop_request` | Proxy | Network failure before the broker received the request | v1 |
| `inject_error` | Proxy | Coordinator moved, rebalance in progress, broker overload | v1 if time allows |
| `scale_out` / `scale_in` | Process | Autoscaling, deploy surge | v1 if time allows |
| `delay` | Proxy | Network congestion | v2 |
| `coordinator_partition` | Proxy | Asymmetric network partition | v2 |
| `broker_restart` | Broker | Broker crash or upgrade | v2 |

### 7.2 Triggers and causal gating

Faults fire on **event predicates** over the live event stream, never on wall-clock times. Predicates may combine proxy and CDC events, for example: "an effect for partition 3 has committed **and** an OffsetCommit for partition 3 from the same member is in flight."

**Causal gating.** When a trigger involves an in-flight request, the proxy holds that request until the trigger resolves, so the request physically cannot reach the broker before the fault fires.

**Speculative holding.** A CDC event can arrive after the OffsetCommit it relates to, in which case a naive trigger misses its window and the tool degrades to random chaos. So when a crash-window trigger is armed, the proxy holds **every** OffsetCommit for the targeted partition for a short budget (default 10 ms, always well below the client's `request.timeout.ms`), then decides:

- the CDC event arrives within the budget → fire the fault;
- the budget expires → release the commit untouched and count a **missed window**.

The hold budget is a deliberate perturbation of the SUT and is reported with every result. `windows_hit / windows_armed` is a first-class metric (BENCHMARKS.md, D4).

**Roles.** Members in plans are roles (`$m` = "whoever owns partition 3"), bound at run time, so plans replay when member IDs change.

**Harness safety.** Every connection is classified at accept time as `sut` or `harness`. Faults may target only `sut` connections; a test asserts that Crashpoint never faults its own workload connections.

**Fault accounting.** A fault counts as fired only at the moment it is actually applied to a frame or process, never when its rule is merely evaluated. When several rules could match one event, exactly one acts, and only its counter advances. (Apache Kafka's fault proxy shipped with a version of this bug caught in review, where rules reported firing without acting, which silently broke the tests that relied on "the fault fired" as evidence.)

**Fail closed.** If the proxy cannot apply a fault to a frame it targets (a decode or re-encode failure), the trial ends as `HARNESS_ERROR`. Forwarding the original bytes instead would let the trial pass with the fault silently skipped.

### 7.3 Unrealistic faults (must NOT be injected)

| Forbidden | Why |
|---|---|
| Reordering records within a partition in a Fetch response | Kafka guarantees per-partition order; a real broker never does this, so any violation found would be impossible in production. |
| Duplicating records inside one Fetch response | Real duplicates come from offset regression, which Crashpoint causes realistically. |
| Fabricating committed offsets | Brokers don't invent offsets. |
| Error codes invalid for the API and version | Clients may behave arbitrarily on impossible errors. |
| Corrupting record payloads | Tests deserialization, not correctness, and CRC checks reject them anyway. |

### 7.4 Operational caveat

Holding a request delays every later response on that connection, because Kafka responses must be returned in request order. Clients differ in which requests share a connection. Crashpoint records the client's connection layout, warns when a hold could exceed the client's request timeout, and attaches that warning to any finding from the trial.

---

## 8. Refusal conditions

Crashpoint **refuses to run** rather than produce unreliable verdicts when it detects:

| Condition | Detected by | Message |
|---|---|---|
| Transactional consumer | `InitProducerId`, `AddOffsetsToTxn`, `TxnOffsetCommit`, or `EndTxn` on a SUT connection | `UNSUPPORTED: transactional consumer detected` |
| No consumer group (manual `assign`) | No JoinGroup/SyncGroup or ConsumerGroupHeartbeat within the startup window | `UNSUPPORTED: manual partition assignment` |
| Share groups (KIP-932) | ShareFetch or ShareAcknowledge observed | `UNSUPPORTED: share groups` |
| Records the SUT cannot deserialize | Calibration trial yields zero effects | `CONFIG: SUT produced no effects; check the payload encoder` |
| Unmapped effects schema | Configuration validation | Names the missing columns and the tier they would unlock |

---

## 9. Why this finds bugs

Crashpoint does **not** search a large space of thread interleavings; at roughly one trial per 30 seconds, random search would be hopeless. It enumerates a small, curated set of protocol windows known to break at-least-once processing, and places faults precisely inside them:

1. **Effect durable, offset not yet committed** → crash → redelivery → duplicate unless dedup is atomic with the effect.
2. **Offset committed, effect not yet durable** → crash → permanent loss.
3. **Partition revoked, batch still in flight** → the old owner writes after the new owner started → duplicate or out-of-order effect.
4. **Commit applied by the broker, acknowledgement lost** → the client retries or reprocesses.
5. **Repeated rebalances during a deploy** → revoke callbacks run with stale cached state.

Targeted enumeration beats random search when the target windows are known in advance. That claim is testable, and benchmark D4 (hold-and-kill vs. random kill hit rate) is the experiment that tests it.

---

## 10. Trial verdicts

| Verdict | Meaning |
|---|---|
| `PASS` | All enabled invariants hold. |
| `VIOLATION` | An invariant is broken, with attached evidence. |
| `WARNING` | Only warning-level findings (I5). |
| `HARNESS_ERROR` | Crashpoint failed or lost data: proxy crash, CDC failure, recorder overflow, held-frame budget exceeded, quiescence cap reached. Never a bug report. |
| `NOISY` | Unplanned proxy↔broker latency exceeded a threshold. Excluded from detection statistics. |
| `UNSUPPORTED` | A refusal condition from Section 8. |
| `NOT_EXERCISED` | No armed fault actually fired **and no invariant was broken**. Not evidence of correctness, and never counted as a pass. |

**Verdict precedence.** A violation is always reported, whatever the faults did. Some real bugs need no fault at all (see corpus bug 8): if an invariant breaks while every armed fault missed its window, the verdict is `VIOLATION`, never `NOT_EXERCISED`.

Every violation report carries the invariant, the affected inputs, and the minimal history slice explaining it: ownership intervals, deliveries, commits, effects, and faults for the affected partitions.

---

## 11. Seeded bug corpus

Used to measure detection rate. Bugs marked **real** are modelled on publicly reported issues, so the corpus isn't purely self-referential. Bug H is **held out**: written only after the checker is complete, and never used to tune detection.

| # | Bug | Origin | Exposing fault | Expected finding |
|---|---|---|---|---|
| 1 | Auto-commit on, records processed asynchronously | Common footgun | `kill` after commit | I1 / I7 |
| 2 | Offsets committed before the database transaction | Common footgun | `kill` in the window | I1, I5 warning |
| 3 | No dedup on insert | Common footgun | `hold` commit + `kill` | I2 / I7 |
| 4 | Dedup check and effect in separate transactions | Common footgun | `pause` past session timeout | I2 + concurrent ownership (I4 at T3) |
| 5 | Parallel workers commit the highest completed offset, not the contiguous prefix | **Real** (classic parallel-consumer hazard) | `kill` while an earlier offset is in flight | I1 |
| 6 | Retry topic that ignores per-key ordering | **Real** (retry-topic pattern) | `inject_error`, then recovery | I3 |
| 7 | Off-by-one on the seek/commit after an internal cache reset: seeks to the last consumed offset instead of that offset plus one | **Real** (confluent-kafka-javascript [#417](https://github.com/confluentinc/confluent-kafka-javascript/issues/417), maintainer-confirmed) | `stop` during a rebalance, or a slow handler that trips cache expiration | I2 — exactly one duplicate per occurrence, which is a sensitivity test for the checker |
| 8 | Slow handler: cache expiration discards fetched-but-undelivered records, the rewind is skipped, and auto-commit advances past them | **Real** (confluent-kafka-javascript [#528](https://github.com/confluentinc/confluent-kafka-javascript/issues/528): 118 of 200 records committed but never delivered) | **No fault.** Handler slower than the poll interval | I1 and I7; I5 warns that commits ran ahead of effects |
| H | Held-out bug, written after the checker | — | — | Unknown to the detector |
| C | **Control:** dedup marker and effect in one transaction, commit after, rebalance blocked during processing | — | All faults | **Never a violation** |

---

## 12. What Crashpoint cannot detect (v1)

- Effects not written to Postgres (external APIs, caches, emails). An HTTP effect sink is planned for v2.
- Anomalies in transactional pipelines (refused, Section 8).
- Broker bugs; the broker is assumed correct.
- Bugs that appear only at production scale or after hours of runtime.
- Bugs caused by clock skew between hosts.
- Deserialization and schema-evolution bugs.
- Zombie writes completing before the new owner's first effect.
- At T1/T2: which consumer instance wrote an effect.
- At T0: which specific event was lost or duplicated (only that the aggregate is wrong).

### Threats to validity

- **Shortened session timeouts** speed up trials but may hide or create timeout-sensitive behavior. Confirm findings with production-like timeouts before reporting them.
- **Proxy latency and the speculative hold budget** perturb the SUT. Both are measured and reported.
- **CDC lag** drives the conservatism of I4 and the warning level of I5.
- **Shared brokers across parallel trials** shift timing; the effect on reproducibility is measured (BENCHMARKS C3).
- **The corpus is partly self-designed.** Mitigated by the real-world-derived bugs and the held-out bug, reported as separate numbers.

---

## 13. Decisions (formerly open questions)

| # | Question | Decision |
|---|---|---|
| Q1 | Configurable effect semantics? | Two modes only: `exactly_one_effect` (default, T1+) and `state_convergence` (T0). |
| Q2 | How is an effect attributed to a member? | Optional writer-identity column at T3; degraded "duplicate with concurrent ownership" at T1/T2. |
| Q3 | Quiescence timeout | 30 s base, 120 s hard cap, plus the progress rule in Section 6. |
| Q4 | Officially supported clients in v0.1 | franz-go and the Java client. Others: untested. |
| Q5 | Shrinker acceptance rule | A candidate "still fails" if it fails in ≥1 of 3 replays; the final minimal plan is confirmed with 5 replays. |
| Q6 | Refuse `hold` on shared connections? | No. Warn, record it in trial metadata, and attach the warning to findings. |

---
**Status:** All six accepted on 2026-09-20. None of them constrain future extension: Q3 and Q5 are tunable values, Q1/Q2/Q6 add modes or warnings that later work can extend, and Q4 is a testing commitment. The decisions that do shape extensibility are the tier model (§3), the input-mode interface (§4), effects as an event stream, and faults as versioned data — all chosen so that new evidence sources, new fault types, and new effect sinks are additive.

## Changelog

| Version | Date | Change |
|---|---|---|
| 0.1 | _(date)_ | Initial draft |
| 0.4 | _(date)_ | Verdict precedence (a violation always outranks `NOT_EXERCISED`); processing time as a workload dimension; corpus bugs 7 and 8 re-grounded in confluent-kafka-javascript #417 and #528 |
| 0.3 | _(date)_ | Lessons from Apache Kafka's fault proxy review: `NOT_EXERCISED` verdict, fault accounting at application time, fail-closed on transform errors, pending-fault check before quiescence ends, `blackhole` fault promoted to v1 |
| 0.2 | _(date)_ | Instrumentation tiers (T0–T3) and I7 state convergence; input modes and identity extraction; refusal conditions; speculative holding; harness connection safety; I3 transaction tie-break; `expect_effect`; quiescence progress rule; "Why this finds bugs"; corpus bugs derived from real reports plus a held-out bug; open questions decided |
