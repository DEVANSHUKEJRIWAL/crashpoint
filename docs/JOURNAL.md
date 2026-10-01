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
