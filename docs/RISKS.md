# Risk Register and Cut Lines

Crashpoint is built by one person, part time, in eight weeks, alongside a master's coursework load, at a target of **12 hours per week**. This document exists so that falling behind triggers a planned cut rather than a panic.

---

## 1. Risks

| # | Risk | Likelihood | Impact | Mitigation | Trigger to act |
|---|---|---|---|---|---|
| R1 | Scope overrun: the tool is half-built at Week 8 | High | Severe | Minimum Impressive Product (Section 2) plus ordered cut lines (Section 3) | Any milestone slips by more than 4 days |
| R2 | The checker produces false positives that can't be eliminated | Medium | Severe | Conservative I4, warning-level I5, control trials in CI from Week 2 | Any false positive survives two fix attempts |
| R3 | Crash windows can't be hit reliably, so detection depends on luck | Medium | High | Speculative holding; measure `windows_hit / windows_armed` from Week 3 | Hit rate below 50% at Week 4 |
| R4 | The proxy is unstable or protocol handling is wrong | Medium | High | Fuzz tests with the parser; end-to-end tests with two clients; golden-file encoding tests | Proxy not stable by end of Week 3 → fall back to a `kfake`-based harness |
| R5 | No real-world bugs found in Week 8 | High | Medium | Reproduce a *known published* bug as a regression test instead; publish the honest result | Week 7 ends with no external target prepared |
| R6 | Apache Kafka's own fault proxy (PR #23438, merged) expands into a verification framework | Low | Medium | Track KAFKA-21074 and its follow-ups; differentiate on the checker and shrinker, which it does not have | A follow-up adds an application-level oracle |
| R7 | Nobody adopts the tool | High | Low | Success is measured on artifacts you control (Section 4) | Not applicable; expected |
| R8 | Docker orchestration consumes disproportionate time | Medium | Medium | Serial trials first; parallelism only if time remains | More than 3 days spent on the runner |
| R9 | Benchmarks are unreproducible or embarrassing | Low | Medium | Methodology fixed before measuring; publish misses | Any published number that can't be reproduced on demand |
| R10 | Coursework load spikes (midterms, finals, project deadlines) | High | Medium | Cut lines; weekly milestones that each end in something demonstrable; bank hours before known deadline weeks | **Target: 12 h/week.** Two consecutive weeks below 7 hours |

---

## 2. Minimum Impressive Product

If only this exists by Week 6, the project has succeeded.

1. Proxy: framing, headers, correlation tracking, broker-address rewriting, connection classification
2. Recorder: fetch ranges, commits, assignments
3. CDC reader
4. Workload generator (generated mode)
5. Checker: I1, I2, I6, I7 (T0 convergence), I4 in degraded form
6. Faults: `kill`, `pause`, `hold` with speculative holding, `drop_response`
7. Shrinker
8. Bug corpus: 7 bugs + control, at least 3 derived from real reports, plus the held-out bug
9. Benchmarks: proxy overhead, detection rate, false positives, hold-vs-random hit rate

---

## 3. Cut lines, in the order to cut

| Order | Cut | What is lost | Cut when |
|---|---|---|---|
| 1 | KIP-848 dual-protocol support | A good post; the classic protocol still covers most deployments | Immediately; it is a Week 8 stretch, not a commitment |
| 2 | HTML swimlane report | Visual polish; replace with a clear text timeline plus one hand-drawn diagram for the demo | Week 4 slips |
| 3 | `inject_error` and `scale_out` faults | Fault variety; corpus bug 6 then needs a different trigger | Week 5 slips |
| 4 | Parallel trial runners | Throughput; run trials serially | Week 5 slips |
| 5 | Observed input mode | Adoption reach; already v2 | Never attempt before Week 8 |
| 6 | Real-world bug hunt | The best outcome, but the least controllable; replace with reproducing a known published bug | Week 7 slips |

Never cut: the checker, the shrinker, the control trials, or the benchmark methodology. They are the project.

---

## 4. Success criteria that don't depend on anyone else

| Outcome | Target | In your control |
|---|---|---|
| Detects ≥6 of 8 corpus bugs, including the held-out one | Yes | Yes |
| Zero false positives across 500 control trials | Yes | Yes |
| Published benchmark methodology and raw data | Yes | Yes |
| Hold-and-kill measurably beats random kill | Yes | Mostly |
| A 3-minute demo a stranger understands | Yes | Yes |
| One substantive technical contribution to the Kafka fault-proxy follow-ups (a JIRA comment, a review of the multi-broker follow-up PR, or a small patch) | Yes | Yes |
| Maintainer-confirmed bug in a third-party project | Nice to have | No |
| GitHub stars | Ignore below 100 | No |

---

## 5. Stop conditions

Stop or pivot if:

1. You cannot reproduce a duplicate by hand in Week 1. The hold-and-kill premise needs rethinking before more is built on it.
2. False positives survive into Week 5. Retreat to T0 state convergence, which is far more robust.
3. The proxy is still unstable at the end of Week 3. Switch to a `kfake`-based harness: less impressive, still finishable.
4. Kafka's fault proxy grows a user-facing application-level oracle. Re-read COMPETITORS.md and re-evaluate.
