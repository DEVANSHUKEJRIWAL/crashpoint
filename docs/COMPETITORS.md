# Competitive Landscape

Written so that "what already does this?" has an honest, specific answer. Update it whenever something new appears.

---

## 1. Summary

Fault **injection** for Kafka is commoditized: several tools do it, including Apache Kafka itself. Fault **checking** at the application level is not. Crashpoint's contribution is the oracle and the shrinker, not the proxy.

---

## 2. Direct comparisons

| Tool | What it is | Fault injection | Application-level oracle | Shrinking | Licence |
|---|---|---|---|---|---|
| **Apache Kafka `KafkaProtocolFaultProxy`** ([PR #23438](https://github.com/apache/kafka/pull/23438), opened Sep 11 2026) | A wire-protocol fault proxy as an internal test fixture for Kafka's own integration tests. Sits in front of an embedded cluster; any client can point at it. Decodes with Kafka's own protocol classes, so it is correct across wire versions. Primitives: `injectError`, `disconnectOn` (models the EOS "commit gap"), `delayOn`, `blackholeClient`, with a deterministic trigger DSL | Yes, protocol-aware | No | No | Apache 2.0, but a test fixture, not a product |
| **Conduktor Gateway / Shield** | Commercial Kafka proxy with eight chaos interceptors (duplicate messages, corruption, broken brokers, leader-election errors, slow brokers, latency, invalid schema ids). Markets "verify idempotency" as a use case | Yes, mostly rate-based | No: the user decides whether behavior was correct | No | Commercial |
| **Khaos** | Open-source Kafka load-testing and chaos CLI; YAML scenarios for rebalances, broker failures, lag, hot partitions | Yes, timeline-based | No | No | Apache 2.0 |
| **franz-go `kfake`** | In-memory fake Kafka cluster with per-partition fault hooks, for Go tests | Yes, in-process | No | No | BSD |
| **Toxiproxy** | TCP-level fault injection | Yes, protocol-blind | No | No | MIT |
| **LitmusChaos / Steadybit** | Kubernetes chaos platforms with Kafka experiments | Yes, infrastructure-level | No | No | Mixed |
| **Jepsen** | Safety testing of databases and brokers, with history checking | Yes | Yes, but for the *system under analysis*, not your application; requires a bespoke Clojure harness per system | No | EPL |
| **Antithesis** | Commercial deterministic-simulation platform on a proprietary hypervisor | Yes, whole-system | Via user-defined properties | Yes | Commercial; publicly reported to start around $168k/year |
| **gosim / detsim / dstsim** | Go deterministic-simulation libraries; `detsim` also does delta-debugging minimization | Yes, simulated | User-written assertions | `detsim`: yes | Open source, near-zero adoption |
| **Crashpoint** | Protocol-precise fault placement + application-effect oracle + shrinking, against real brokers and unmodified apps | Yes, protocol-aware, causally gated | **Yes**: lost / duplicate / out-of-order / concurrently-written effects | **Yes** | Apache 2.0 |

---

## 3. Honest positioning

**What is not novel.** History checking (Jepsen), simulation-guided exploration (FoundationDB, Antithesis, TigerBeetle), delta debugging (Zeller & Hildebrandt, 2002), and Kafka protocol proxies (Kroxylicious, Conduktor, and now Kafka itself) all predate this project.

**What is.** Combining protocol-precise fault placement with an application-level effect oracle and automatic shrinking, aimed at ordinary Kafka consumer applications, open source, running against real brokers in CI.

**What could displace it.** If Kafka's fault proxy grows a user-facing application oracle, or Conduktor ships verdicts rather than just injection, the gap closes. Both are tracked in RISKS.md (R6).

---

## 4. Answers to predictable questions

**"Why not Toxiproxy?"** It's protocol-blind: it cannot drop only the OffsetCommit response for one partition, and Kafka clients bypass a naive TCP proxy after the first Metadata response.

**"Why not Conduktor?"** It injects but doesn't judge. It's also commercial, and its faults are rate-based rather than placed at a specific protocol moment.

**"Why not use Kafka's own fault proxy?"** It's Java test infrastructure inside the Kafka repository for Kafka's own tests, with no history recorder, no oracle, and no shrinking. Its existence validates the fault-injection design; it doesn't cover the part that matters here. (Its author reached the same conclusion about deterministic triggers: they are safe to assert on, while probabilistic ones are chaos mode only.)

**"Why not kfake?"** It's a fake broker for Go unit tests. Coordinator semantics are exactly what must be real here. It remains useful for testing Crashpoint itself.

**"Why not Jepsen?"** Jepsen tests the datastore or broker, with a bespoke harness per system. Crashpoint tests *your application* with no harness code.

**"Why not Antithesis?"** Price, and the fact that it requires running the whole system on their platform. Crashpoint runs in your CI, on your Docker setup, for free — at the cost of far weaker determinism, which is why reproducibility is measured rather than claimed.
