# What Crashpoint Supports

**Status:** Target for v0.1. Nothing here is verified until the matrix below says "Verified."

Read this before evaluating Crashpoint. It states exactly what it runs against, what it needs from your schema, and when it refuses to run.

---

## 1. What you must provide

| Requirement | Why | Negotiable? |
|---|---|---|
| A consumer group (no manual partition assignment) | Ownership intervals come from group assignments | No; refused otherwise |
| Non-transactional consumers | Transactional offset commits are not modelled in v1 | No; refused otherwise |
| Effects committed to PostgreSQL | The effect oracle reads logical replication | No in v1 |
| Postgres with `wal_level=logical` and a role with replication permission | Required for the CDC reader | No |
| A PLAINTEXT Kafka test cluster | No TLS/SASL in v1 | No in v1 |
| A unique `client.id` per consumer instance | Maps proxy connections to instances | Yes, but degrades member-level evidence |
| Docker | Trials provision and control containers | No in v1 |

**Crashpoint never asks you to change application logic.** Higher tiers ask for extra columns on the effects table (Section 2), and those are the only schema requests.

---

## 2. Instrumentation tiers

Crashpoint checks only what your schema can support. It prints the active tier at startup, along with the column that would unlock each disabled invariant.

| Tier | Effects table needs | You get | Typical effort |
|---|---|---|---|
| **T0** | The effects table and one numeric column to aggregate | Liveness, state convergence ("the final balance is wrong") | None |
| **T1** *(default)* | + a column holding the causing event's id | Lost effects, duplicate effects, commit-ahead warnings, each naming the exact event | None if you already use the idempotent-consumer pattern |
| **T2** | + the business key and the source sequence number | Per-key ordering violations | One column, if `seq` isn't already stored |
| **T3** | + a writer-identity column (which consumer instance wrote the row) | Zombie writes attributed to a specific instance and epoch | One column |

At T1/T2, a zombie write is reported as a **duplicate with concurrent ownership**: the duplicate plus proxy evidence that two members held the partition at the time. It is not claimed as a proven zombie write.

---

## 3. Client libraries

| Client | v0.1 status | Notes |
|---|---|---|
| franz-go | **Officially supported** | Reference client for the SUT and tests |
| Java client (`org.apache.kafka:kafka-clients`) | **Officially supported** | Connection layout differs from franz-go; documented in the trial metadata |
| librdkafka-based clients (confluent-kafka-go, -python, -js) | Untested | Expected to work; report issues |
| Sarama | Untested | Expected to work |
| Kafka Streams | Not supported | Transactional by default; refused |

"Officially supported" means an end-to-end test runs that client through the proxy on every CI build.

---

## 4. Brokers and protocols

| Item | v0.1 |
|---|---|
| Apache Kafka 4.x (KRaft) | Target |
| Classic consumer group protocol | Supported |
| KIP-848 (`group.protocol=consumer`) | Stretch goal; refused with a clear message if not implemented |
| Share groups (KIP-932) | Refused |
| Redpanda, WarpStream, AutoMQ, other Kafka-API systems | Untested; may work, unvalidated |
| TLS / SASL | Not supported |

---

## 5. Record formats

| Format | Generated mode | Observed mode (v2) |
|---|---|---|
| JSON | Shipped | Supported via JSON-path identity extraction |
| Avro / Protobuf with Schema Registry | Needs a payload encoder you supply | Supported via a record-header identity extractor |
| Anything else | Write an encoder against the `PayloadEncoder` interface | Header-based extraction |

If the SUT cannot deserialize Crashpoint's records, the calibration trial produces zero effects and Crashpoint stops with a configuration error rather than reporting loss.

---

## 6. Refusal conditions

Crashpoint stops instead of producing an unreliable verdict when it detects:

- a transactional consumer (`InitProducerId`, `AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`);
- manual partition assignment (no group protocol traffic);
- share groups (`ShareFetch`, `ShareAcknowledge`);
- an effects schema mapping that doesn't resolve;
- a calibration trial that produces no effects.

Each refusal names the condition and what to change.

---

## 7. Verification matrix

Fill this in as you build. Do not claim support before the matrix does.

| Scenario | Verified | Date | Evidence |
|---|---|---|---|
| franz-go consumer, classic protocol, T1 | ☐ | | |
| Java consumer, classic protocol, T1 | ☐ | | |
| T0 state convergence with no schema mapping | ☐ | | |
| T3 zombie attribution | ☐ | | |
| Transactional consumer correctly refused | ☐ | | |
| Manual assignment correctly refused | ☐ | | |
| KIP-848 protocol (stretch) | ☐ | | |
