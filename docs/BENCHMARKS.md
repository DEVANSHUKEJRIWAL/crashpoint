# Benchmarks

**Status:** Methodology defined; nothing measured yet.

Benchmarks are defined **before** implementation, so results can't be cherry-picked afterwards. Every number in the "Measured" column must come from a committed script and raw output stored in `bench/`.

---

## 1. Environment

Fill this in before the first measurement. Any change to it starts a new results table.

> **Harness ready (issue #10):** `bench/baseline/run.sh` drives the open-loop
> generator (`cmd/bench`) through `cmd/crashpoint proxy` for the three P1/P2
> conditions (direct, proxy recording off, proxy recording on). Fill this table
> and the P1/P2 **Measured** column from a run on the target machine.

| Item | Value |
|---|---|
| Machine / CPU / cores | |
| Memory | |
| Disk | |
| OS and kernel | |
| Docker version | |
| Kafka version and mode | |
| Postgres version | |
| Go version | |
| Client libraries and versions | |
| Crashpoint commit | |

---

## 2. Methodology rules

1. **Always compare against a baseline.** Proxy benchmarks compare direct-to-broker vs. through the proxy (recording off, then on).
2. **Open-loop load generation.** Send at a fixed rate regardless of responses, to avoid coordinated omission (a slow system reducing its own load and hiding latency).
3. **Latency histograms, not averages.** Report p50, p99, p99.9, and max.
4. **Warm up for 60 seconds**, then run **5 repetitions of 3 minutes**. Report the median repetition and the spread.
5. **Commit scripts and raw output** under `bench/<benchmark>/<date>/`.
6. **Report misses.** A missed target is published with its explanation. Results are never removed because they look bad.
7. **Detection benchmarks use the fixed seeded corpus** from DESIGN.md Section 11, plus the control consumer. Report three numbers separately: all bugs, the real-world-derived bugs (5–7), and the held-out bug (H). The held-out bug is written only after the checker is complete and is tested exactly once.
8. **Report the perturbation you introduce.** Every detection result states the speculative hold budget and the session timeout used, because both change what the SUT experiences.

---

## 3. Benchmarks and targets

Targets are aspirations, not results.

### Proxy performance

| # | Benchmark | Baseline | Target | Measured |
|---|---|---|---|---|
| P1 | Max pass-through throughput (1 KB records, recording off) | Direct to broker | ≥ 150 MB/s or ≥ 150k records/s | — |
| P2 | Added latency at 50k records/s (recording on) | Direct to broker | p99 ≤ 1 ms, p99.9 ≤ 3 ms | — |
| P3 | Partial vs. full Fetch decoding | Full decode | CPU and allocations reduced (report %) | — |
| P4 | 5,000 connections (500 active) | — | No errors; memory ≤ 400 MB | — |
| P5 | Trigger evaluation (20 armed triggers, 100k events/s) | — | p99 ≤ 200 µs per event | — |
| P6 | History append | — | ≥ 500k operations/s, p99 ≤ 100 µs | — |

### Checker and trials

| # | Benchmark | Target | Measured |
|---|---|---|---|
| C1 | Checker throughput (synthetic history) | ≥ 1M operations/s; 10M operations in ≤ 30 s and ≤ 2 GB | — |
| C2 | Trial throughput (3 consumers, 4k events, shared broker, 4 parallel) | ≥ 100 trials/hour | — |
| C3 | Shared vs. dedicated broker | Report throughput gain and reproducibility cost | — |

### Detection quality

| # | Benchmark | Target | Measured |
|---|---|---|---|
| D1 | Seeded bugs found (7 bugs × 10 runs, 30-minute budget each) | ≥ 6 of 7 | — |
| D2 | Median time to detect per bug | ≤ 5 minutes | — |
| D3 | False positives (control consumer, 500 trials, both group protocols) | 0 | — |
| D4 | Hold-and-kill window hit rate vs. random kill | Hold-and-kill ≥ 5x the random rate | — |
| D4b | Windows hit / windows armed (speculative holding) | ≥ 70% | — |
| D7 | Held-out bug (H): detected without tuning? | Reported either way | — |
| D8 | Detection at T0 (no schema mapping) vs. T1 | Report both | — |
| D5 | Shrinking result | ≤ 3 faults, ≤ 1,000 events, ≤ 15 minutes | — |
| D6 | Replay reproducibility of shrunk plans (20 replays) | ≥ 80% per bug | — |

### Robustness

| # | Benchmark | Target | Measured |
|---|---|---|---|
| R1 | Kill Crashpoint mid-trial, 100 times | All resources reaped in ≤ 30 s; 0 leaks | — |
| R2 | 1,000 consecutive trials | 0 leaked containers, topics, groups, or replication slots | — |

---

## 4. Results log

| Date | Benchmark | Commit | Result | Notes / link to raw data |
|---|---|---|---|---|
| | | | | |
