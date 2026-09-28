# Getting Started: How to Start Crashpoint

This project is documentation-first, but the design phase is **one working day**, not a week. The design decisions recorded here are the specification for the checker and the source of your interview answers; everything else waits for code.

---

## Phase 0: one day

| Block | Step | Output |
|---|---|---|
| 1 hour | Step 1: create the repository | Public repo with docs and license |
| 1 hour | Step 2: read the Kafka fault proxy PR | Notes in `JOURNAL.md` |
| 1 hour | Step 3: read the two real bug sources | Corpus bugs 5–7 grounded in reality |
| 2 hours | Step 4: answer the mental-model questions | Six written answers |
| 1 hour | Step 5: confirm the design decisions | DESIGN.md decisions accepted or changed |
| 1 hour | Step 6: create the Week 1 issues | Board with acceptance criteria |
| 30 min | Step 7: tag the design | `design-v0.2` |

Optional and non-blocking: post one validation question to r/apachekafka or the Confluent forum. Do not wait for replies.

---

## Prerequisites

| Tool | Needed from |
|---|---|
| Git and a GitHub account | Day 1 |
| Go (current stable) | Week 1 |
| Docker Desktop or Docker Engine + Compose | Week 1 |
| `psql` and `jq` | Week 1 |

---

## Step 1: Create the repository (1 hour)

1. Create a public repo named `crashpoint` (check the name is free as a Go module path too).
2. Description: *"Find the Kafka consumer bugs that lose or double-apply events, before production."*
3. Topics: `kafka`, `distributed-systems`, `testing`, `fault-injection`, `golang`.
4. Initialize with the **Apache-2.0 license** and the **Go `.gitignore`**.
5. Copy in this documentation set:

```text
crashpoint/
├── README.md
├── CONTRIBUTING.md
└── docs/
    ├── GETTING_STARTED.md   DESIGN.md      SUPPORTED.md
    ├── ARCHITECTURE.md      COMPETITORS.md RISKS.md
    ├── BENCHMARKS.md        ROADMAP.md     JOURNAL.md
    └── adr/0000-template.md … 0008-*.md
```

6. Commit: `docs: design v0.2`.

---

## Step 2: Read Apache Kafka PR #23438 (1 hour)

[apache/kafka#23438](https://github.com/apache/kafka/pull/23438) (merged September 2026) adds a Kafka wire-protocol fault-injection proxy as a test fixture: `injectError`, `disconnectOn` (which models the exactly-once "commit gap"), `delayOn`, `blackholeClient`, and a deterministic trigger DSL.

This is the closest thing to a free design review you will get: Kafka committers converging on the same primitive you planned. Read the description and the review comments, then write in `JOURNAL.md`:

1. Three design choices they made that differ from `ARCHITECTURE.md`, and whether they're better.
2. How they handle connection lifecycle when a test fails (a reviewer raises this) and what your equivalent is.
3. One question you could ask about the PR that shows you actually read it.

The PR is merged, so that question belongs on the JIRA ticket (KAFKA-21074) or on the follow-up PR the author announced for multi-broker bootstraps. Save it for when you have data from your own implementation; it is a better networking move than any post.

---

## Step 3: Read the two real bug sources (1 hour)

1. The `confluent-kafka-javascript` issue where `pause()` during shutdown with auto-commit leaves stale committed offsets and causes reprocessing after a rebalance. This is corpus bug 7.
2. Documentation for one framework's rebalance callbacks (Spring Kafka or your client of choice), focusing on what is unsafe inside a revoke callback.

Record in `JOURNAL.md` how each maps to a seeded bug in `DESIGN.md` Section 11.

---

## Step 4: Answer the mental-model questions (2 hours)

Answer in your own words in `JOURNAL.md`. If you can't answer one, read the Kafka protocol guide section that covers it.

1. A Kafka response header has no API key. How does anything decode a response?
2. Why do clients connect directly to brokers after their first Metadata request, and what does that mean for a proxy?
3. What's the difference between a consumer's fetched offset and the group's committed offset?
4. In the classic protocol, what stops a consumer that lost its partition from committing offsets? What plays that role under KIP-848?
5. Why can a Fetch response contain records *before* the requested offset, and what would happen if the checker ignored that?
6. Why is Postgres commit order (LSN) more trustworthy than a `created_at` timestamp — and why isn't LSN alone enough to order two effects in the same transaction?

These are interview answers, not homework.

---

## Step 5: Confirm the design decisions (1 hour)

`DESIGN.md` Section 13 now contains decisions, not open questions. Read each and either accept it or change it. Then sanity-check three things:

- [ ] Every invariant maps to a tier in Section 3, and you could explain to a stranger why T1 is the realistic default.
- [ ] Every fault in the v1 catalogue names a real-world event it models.
- [ ] Every refusal condition in Section 8 is something the proxy can actually detect from the traffic it sees.

If you change a decision, add an ADR rather than editing an accepted one.

---

## Step 6: Create the Week 1 issues (1 hour)

Create a project board (`Backlog`, `This week`, `In progress`, `Done`), milestones `Week 1`–`Week 8`, and these issues with acceptance criteria from `ROADMAP.md`:

| # | Issue |
|---|---|
| 1 | Docker Compose: Kafka (KRaft) + Postgres with logical WAL |
| 2 | Reference payments consumer: control mode |
| 3 | Reference consumer: bug modes 1–3 behind a flag |
| 4 | Workload producer with acknowledged / failed / indeterminate tracking |
| 5 | Proxy: framing, request headers, correlation tracking |
| 6 | Proxy: Metadata / FindCoordinator / DescribeCluster rewriting |
| 7 | Proxy: connection classification (`sut` vs `harness`) and a test that harness connections are never faulted |
| 8 | Proxy: fuzz tests for every parser |
| 9 | Recorder: frame events to an inspectable log |
| 10 | Baseline benchmark: direct vs. proxied |
| 11 | Manual experiment: reproduce a duplicate by crashing the no-dedup consumer |

---

## Step 7: Tag the design (30 minutes)

`git tag design-v0.2 && git push --tags`. You now have a public, dated record that the design preceded the code.

---

## Week 1, in order

1. **Environment** (issue 1). You can't test a proxy without a broker.
2. **System under test** (issues 2–4). You need something worth breaking.
3. **Manual experiment** (issue 11). Crash the no-dedup consumer by hand until you see a duplicate. This proves the premise, shows how hard the window is to hit manually, and is your first journal entry and first post. **If you cannot hit it by hand at all, stop and re-read `RISKS.md` Section 5.**
4. **Proxy** (issues 5–9). Write the frame parser, header parsing, and address rewriting yourself; they're exactly what interviewers probe. Fuzz tests the same day as the parser.
5. **Baseline benchmark** (issue 10). Measure before optimizing anything.

Before each coding session, reread the relevant `DESIGN.md` section. If the code needs to diverge, change the design first.
