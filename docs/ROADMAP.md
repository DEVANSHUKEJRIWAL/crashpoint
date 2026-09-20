# Roadmap

Each milestone ends in something demonstrable. A milestone is complete only when every exit criterion is met. When a milestone slips more than four days, apply the next cut from [RISKS.md](RISKS.md) instead of extending the schedule.

The **Minimum Impressive Product** (RISKS.md Section 2) must exist by Week 6. Everything after that is hardening and launch.

---

## Phase 0: Design (1 day)

**Deliverable:** design tagged `design-v0.2`, Week 1 issues created.
**Exit:** the seven steps in [GETTING_STARTED.md](GETTING_STARTED.md) are done.

---

## Week 1: See a rebalance on the wire

**Build:** Docker Compose environment (Kafka KRaft, Postgres with logical WAL); reference payments consumer (control + bugs 1–3); workload producer with tri-state input status; proxy with framing, header parsing, correlation tracking, address rewriting, and connection classification; frame recorder.

**Exit:**
- [ ] franz-go and Java consumers both work through the proxy
- [ ] Recorded frames show no Produce or Fetch bypassing the proxy
- [ ] Fuzz test for every parser; a test asserts harness connections are never faulted
- [ ] Baseline overhead measured (P1, P2 preliminary)
- [ ] A duplicate effect reproduced **manually** by crashing the no-dedup consumer

**Demo:** terminal timeline of a rolling restart — who owned which partition, what was delivered, what was committed.

---

## Week 2: Know what the application actually did

**Build:** CDC reader; binary history log; checker v1 (I1, I2, I6, I7) with the quiescence progress rule; calibration trial; tier detection and startup banner.

**Exit:**
- [ ] The checker flags bug 2 after a manual kill, naming the events
- [ ] T0 state convergence catches bug 3 with **no** schema mapping configured
- [ ] Control consumer passes 50 consecutive trials
- [ ] Refusal conditions implemented for transactional and manually-assigned consumers

---

## Week 3: Crash at the exact wrong moment

**Build:** nemesis with `kill`, `pause`, `hold` (with speculative holding), `drop_response`, `drop_request`, `stop`; event-based triggers with role binding; versioned JSON plans; `crashpoint run`.

**Exit:**
- [ ] A hold-and-kill plan reproduces bug 3 in most runs
- [ ] `windows_hit / windows_armed` measured and reported
- [ ] Hold-and-kill vs. random-kill hit rate measured (benchmark D4)

**Demo:** the same bug, hit reliably versus hit by luck — the graph that justifies the whole design.

---

## Week 4: Explain why

**Build:** checker v2 — degraded I4 (duplicate with concurrent ownership), precise I4 at T3, I3 with the transaction tie-break, I5 warnings, evidence extraction; text timeline output; corpus bugs 4–7, including the real-world-derived ones.

**Exit:**
- [ ] Bug 4 reported with ownership evidence at T1 and attributed to a member at T3
- [ ] Checker property tests plus a brute-force cross-check on small histories in CI
- [ ] Every violation prints a minimal, readable history slice

---

## Week 5: Find bugs unattended

**Build:** explorer (seeded, swarm testing; serial trials are fine); trial isolation, leases, reaper; SQLite run index; detection-rate and false-positive benchmarks.

**Exit:**
- [ ] Unattended exploration finds most corpus bugs within the budget
- [ ] Zero leaked containers, topics, groups, or replication slots after 200 trials
- [ ] Control run of 500 trials with zero false positives (D3)

---

## Week 6: Performance

**Build:** `crashpoint bench`; profiling-driven optimizations (buffer pooling, partial Fetch decoding, recorder batching); streaming checker.

**Exit:**
- [ ] P1–P6 and C1–C2 measured with raw data committed
- [ ] Before/after flame graphs for every optimization
- [ ] Any missed target published with an explanation

---

## Week 7: Hardening

**Build:** flakiness-aware shrinker (≥1 of 3 replays; final plan confirmed with 5); `crashpoint replay` with reproduction rate; memory budgets and `HARNESS_ERROR` classification; `NOISY` detection; GitHub Action; `SECURITY.md`; `REPORT_GUIDE.md`.

**Exit:**
- [ ] D5 and D6 measured
- [ ] The held-out corpus bug (H) written **now**, then tested once, with the result published either way

---

## Week 8: Launch

**Build:** README quickstart under 15 minutes; demo video; architecture write-up; v0.1.0 release. Stretch only if ahead: KIP-848 support, a run against real-world consumer patterns, observed input mode.

**Exit:**
- [ ] Someone who has never seen the project completes the quickstart unaided
- [ ] Any third-party issue found is reported privately to maintainers first
- [ ] All benchmark tables filled with measured values or documented misses
- [ ] One substantive technical comment posted on Apache Kafka PR #23438, informed by your own data

---

## Out of scope for v0.1

Web dashboard · AI/LLM features · TLS/SASL · non-Kafka brokers · Kubernetes operator · transactional/EOS checking · share groups · hypervisor-level determinism · hosted service · parallel runners (unless time remains).
