# Engineering Journal

A dated log of decisions, surprises, bugs, and measurements. It's written for future you: the source for interview stories, ADRs, and technical posts.

**Rules:** write an entry for every working session, keep it short, and record what you *learned*, not just what you did. Surprises and mistakes are the most valuable entries.

---

## Entry template

```markdown
### YYYY-MM-DD: <one-line summary>

**Goal:** what I set out to do.
**Done:** what actually got done.
**Surprise / learned:** anything that behaved differently than I expected, and why.
**Decision:** any design decision made (link an ADR if one was written).
**Measurement:** any number measured, with a link to raw data.
**Open question:** anything unresolved.
**Post idea:** is this worth sharing? One sentence.
```

---

## Phase 0: Reading notes

### Apache Kafka PR #23438 (protocol fault proxy)

- Three design choices that differ from ours, and whether theirs are better:
- How they handle connection lifecycle on test failure, and our equivalent:
- One question worth asking on the PR once we have our own data:

### Real bug sources

- confluent-kafka-javascript pause/rebalance offset issue → corpus bug 7. What exactly goes wrong:
- Framework rebalance-callback documentation → what is unsafe inside a revoke callback:

---

## Phase 0: Mental-model questions

Answer in your own words (see GETTING_STARTED.md, Step 2).

1. **A Kafka response has no API key in its header. How does anything decode a response?**
   _Answer:_

2. **Why does a client connect directly to brokers after its first Metadata request, and what does that mean for a proxy?**
   _Answer:_

3. **What is the difference between a consumer's fetched offset and the group's committed offset?**
   _Answer:_

4. **In the classic protocol, what prevents a consumer that lost its partition from committing offsets? What plays that role under KIP-848?**
   _Answer:_

5. **Why can a Fetch response contain records before the offset the client asked for?**
   _Answer:_

6. **In Postgres, why is commit order (LSN) more trustworthy than a `created_at` timestamp — and why isn't LSN alone enough to order two effects written in the same transaction?**
   _Answer:_

---

## Entries

### 2026-09-20: Project started

**Goal:** Create the repository and documentation skeleton.
**Done:**
**Surprise / learned:**
**Decision:**
**Open question:**

### 2026-09-28: Control-mode payments consumer (issue #2)

**Goal:** The correct, idempotent consumer (corpus row C) the checker must always pass.
**Done:** `sut/` as a separate Go module — franz-go + pgx consumer. Dedup guard (`processed_events`) and effect (`ledger_entries` + `accounts` balance) in one transaction; offset committed only after the DB tx. Dockerfile + a `payments-consumer` compose service. One idempotency test (skips without `CRASHPOINT_TEST_DSN`). `go vet` and build clean; integration test not yet run (no Docker in this env).
**Surprise / learned:** The safe rebalance behavior falls straight out of two franz-go options — `DisableAutoCommit` + `BlockRebalanceOnPoll`, then commit before `AllowRebalance`. No revoke callback needed: blocking the rebalance until after the commit makes it redundant.
**Decision:** Commit the *contiguous prefix* per partition (break on first failure), never the highest success — committing past a failed offset is corpus bug 5, so the control consumer must not. Money kept as exact decimal text (`json.Number` → `::numeric`), never float.
**Measurement:** —
**Open question:** Should SIGTERM (`stop` fault) flush an in-flight commit before exiting, or is idempotent redelivery enough? Chose redelivery for now; revisit if duplicate reprocessing on rolling deploys shows up as noise.
**Post idea:** "The correct Kafka consumer is four options and one transaction" — how little code the *right* at-least-once consumer needs, versus the bug corpus around it.

### 2026-09-30: Bug modes 1-3 behind a `-mode` flag (issue #3)

**Goal:** Seed the three common-footgun bugs, invisible without faults.
**Done:** `-mode` flag (control|bug1|bug2|bug3), default from `MODE` env. bug1 = auto-commit left on + async handlers; bug2 = commit offset before the DB tx; bug3 = effect write with no dedup guard (`processNoDedup`). Happy-path smoke test for the no-dedup path (single delivery correct, redelivery duplicates). `CONSUMER_MODE` wired into compose. vet + build clean.
**Surprise / learned:** The bugs don't live in one swappable DB function — only bug3 needed a new write. bug1 and bug2 are purely *orchestration*: commit timing and sync-vs-async in the poll loop. So the mode switch lives in main's loop, not behind a write interface. Trying to force all three into one function signature would have been the over-engineered version.
**Decision:** Only bug3 gets a distinct DB write; bug1/bug2 reuse the idempotent `processControl` and differ solely in commit ordering. A `switch mode` in the loop beats a strategy interface with four one-method impls.
**Measurement:** —
**Open question:** bug1's async handlers leak on clean shutdown (no WaitGroup). Fine for a deliberately-buggy mode under trial quiescence, but confirm the explorer never mistakes shutdown-dropped writes for the seeded loss.
**Post idea:** "Three Kafka consumer bugs, and which line moved" — same ~30 lines, three incident reports.

### 2026-09-30: Workload producer with tri-state status (issue #4)

**Goal:** Generate unique, per-key-ordered inputs and record each one's delivery status.
**Done:** Created the tool module (`go.mod` at root) with `internal/workload` (seeded `Generator`, `PayloadEncoder` interface, `JSONEncoder`) and `cmd/workload`. ProduceSync per record with a per-produce timeout; classifies acknowledged/failed/**indeterminate** and writes JSONL. Tests cover per-key seq + unique ids and the amount-as-number contract. vet/build/test green.
**Surprise / learned:** The amount bites across the module boundary. The consumer's `Amount json.Number` only decodes a bare JSON number token, so the producer has to marshal amount through `json.Number` too — a string `"10.00"` would fail decode. Caught it with a cross-module contract test rather than at runtime. Floats would have "worked" and silently drifted.
**Decision:** Timeout → indeterminate, never failed (the record may have landed); the checker excludes indeterminate from loss checks but keeps it for duplicates. `Input` stays a struct — the `Input` interface the design mentions is a checker-side concern worth deferring until observed mode (v2) is a real second producer; only `PayloadEncoder` is an interface now, because Avro/Protobuf are the concrete second cases.
**Measurement:** —
**Open question:** ProduceSync is one-at-a-time; fine at trial rates (≤ a few k/s) but if the explorer wants heavier load, switch to async `Produce` with a callback that records status. Named the ceiling in a comment.
**Post idea:** "indeterminate is not failed" — the one status split that decides whether a fuzzer lies about loss.

### 2026-09-30: Reproduce a duplicate by hand (issue #11) — POST #1

**Goal:** Prove the premise: crash the no-dedup consumer in the effect→commit window and watch a duplicate ledger row appear.
**Done:** Added a `WINDOW_DELAY` knob to the consumer (sleeps between the durable effect and the offset commit; default 0 = the real window, untouched in trials) and `scripts/manual-duplicate.sh`, which produces a batch, starts bug3, `kill -9`s it the moment effects become durable (i.e. inside the window), restarts to reprocess the redelivered-but-uncommitted batch, and checks for duplicate `event_id`s — looping until one appears or the stop condition trips.
**Not yet run here:** the Docker daemon wasn't available in this session, so the measured numbers below are blank until the script is run on a machine with the env up (`bash scripts/manual-duplicate.sh`). No numbers invented.
**Mechanism (why the duplicate happens):** bug3 writes `ledger_entries` (tx commits) and only then commits the Kafka offset. Kill in between → the offset never advances → Kafka redelivers on restart → with no `processed_events` guard, a second row is inserted for the same `event_id`.
**Proving SQL:**
```sql
SELECT event_id, count(*) AS n, array_agg(id ORDER BY id) AS ledger_ids
FROM ledger_entries GROUP BY event_id HAVING count(*) > 1;
```
**Measurement:** attempts to first duplicate = _TBD_; window width (`WINDOW`) = _TBD_. Run first with a widened `WINDOW` to confirm the mechanism, then with `WINDOW=0s` to feel how hard the real (sub-millisecond) window is by hand — that difficulty is the whole argument for the tool.
**Surprise / learned:** Hitting the window reliably needed a timing signal, not a stopwatch. The script kills the instant the ledger row count grows (effects durable) rather than after a fixed sleep — the deterministic version of what a human does by eye, and the honest way to show the by-hand version is near-impossible once `WINDOW=0`.
**Open question:** If `WINDOW=0s` never reproduces within `MAX_ATTEMPTS`, that's the STOP CONDITION (RISKS.md §5): the precise-placement nemesis is the point, so confirm the automated proxy-timed window hits it before trusting detection rates.
**Post idea:** "I charged a test customer twice on purpose" — the kill-in-the-window experiment, with the attempt count as the punchline.
