# Crashpoint

> Find the Kafka consumer bugs that lose or double-apply events, before they reach production.

**Status: Design phase.** No implementation yet. This repository holds the design the implementation will follow. Coding starts once the one-day checklist in [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md) is done.

---

## The problem

Most Kafka consumers claim to be "at-least-once and idempotent." Few teams have evidence that this holds under the failures that actually break it:

- a crash **after** the database commit but **before** the offset commit;
- a pause (GC, VM stall) long enough to lose the partition while a batch is still running;
- an offset-commit acknowledgement lost on the network;
- back-to-back rebalances during a rolling deploy.

These bugs rarely appear in integration tests. In production they surface as incidents nobody can reproduce: "a customer was charged twice last Tuesday."

People already did this by hand. One recent report of silent message loss in a Kafka client was proved by patching the client with tracing, running five reproductions, and manually diffing delivered offsets against committed offsets. Crashpoint automates that loop.

## What Crashpoint will do

1. Sit between your consumers and Kafka as a **transparent wire-protocol proxy**; only `bootstrap.servers` changes.
2. Drive a **workload** of uniquely identified events and record which ones Kafka acknowledged.
3. Record a **history**: deliveries, offset commits, and group assignments from the proxy, plus committed side effects from Postgres logical replication.
4. **Place faults at exact protocol moments** — kill, pause, hold, or drop a request or response — instead of injecting them at random.
5. **Check invariants**: no lost effects, no duplicate effects, per-key order, no writes from a consumer that lost its partition.
6. **Shrink** any failure into a minimal fault plan that replays locally or in CI.

Crashpoint checks only what your schema can support, and says so at startup. The lowest tier needs no schema changes at all; see [`docs/SUPPORTED.md`](docs/SUPPORTED.md).

## What is and isn't novel

Fault injection for Kafka is commoditized — Conduktor, Khaos, `kfake`, and [Apache Kafka's own test fixture](https://github.com/apache/kafka/pull/23438) all do it. **Crashpoint's contribution is the oracle and the shrinker**: deciding whether the application actually behaved correctly, and reducing any failure to something reproducible. See [`docs/COMPETITORS.md`](docs/COMPETITORS.md) for the full comparison.

## Documentation

| # | Document | Purpose |
|---|---|---|
| 1 | [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md) | How to start: a one-day design checklist, then Week 1 |
| 2 | [`docs/DESIGN.md`](docs/DESIGN.md) | The correctness contract: tiers, invariants, faults, refusals, limits |
| 3 | [`docs/SUPPORTED.md`](docs/SUPPORTED.md) | What it runs against, what it needs, when it refuses |
| 4 | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Components, data flow, concurrency, storage, testing |
| 5 | [`docs/COMPETITORS.md`](docs/COMPETITORS.md) | Landscape and honest positioning |
| 6 | [`docs/RISKS.md`](docs/RISKS.md) | Risk register, minimum product, cut lines, stop conditions |
| 7 | [`docs/adr/`](docs/adr/) | Decision records |
| 8 | [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) | Methodology and targets, fixed before measuring |
| 9 | [`docs/ROADMAP.md`](docs/ROADMAP.md) | Milestones with exit criteria |
| 10 | [`docs/JOURNAL.md`](docs/JOURNAL.md) | Engineering log |

## Principles

1. **The checker is the product.** Fault injection alone is a commodity.
2. **No false positives.** When evidence is insufficient, stay silent or refuse to run.
3. **Harness failures are never reported as application bugs.**
4. **Measure, don't claim.** Every number comes from a committed, reproducible benchmark, including the misses.
5. **State the limits.** [`docs/DESIGN.md`](docs/DESIGN.md) Section 12 lists what Crashpoint cannot detect.

## License

Apache-2.0.
