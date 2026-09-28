# ADR-0002: Build a protocol-aware Kafka proxy instead of reusing a generic or existing proxy

**Status:** Proposed · **Date:** _(date)_

## Context

Crashpoint must inject faults at precise protocol moments, for example "drop only the OffsetCommit response for partition 3 from member B," and combine protocol events with database events in its triggers.

## Options considered

1. **Toxiproxy (TCP level).** Simple and popular, but blind to the protocol: it can't target one API, partition, or member. Kafka clients also bypass a TCP proxy after bootstrapping, because brokers advertise their own addresses.
2. **Conduktor Gateway.** Kafka-aware with built-in chaos interceptors, but commercial and closed source. Its faults are rate-based, and it offers no application-level checking or shrinking.
3. **Kroxylicious filter (Java, open source).** A mature Kafka proxy framework. Filters are per-connection and in-band, while Crashpoint's triggers join events across connections and with database events, and drive process-level actions.
4. **Envoy's Kafka broker filter.** Experimental and aimed at observability, not fault orchestration.
5. **Apache Kafka's own `KafkaProtocolFaultProxy`** ([PR #23438](https://github.com/apache/kafka/pull/23438), merged September 2026). A wire-protocol fault proxy with deterministic triggers, decoding with Kafka's own protocol classes. It is Java internal test infrastructure inside the Kafka repository, intended for Kafka's own integration tests: no history recording, no application-level oracle, no shrinking, and not distributed as a usable artifact for application teams. Its existence is strong validation of the approach and it should be tracked, but it does not cover the checking that gives Crashpoint its value.
6. **Custom Go proxy.** Full control over triggers, holding, and event recording.

## Decision

Build a custom proxy focused on transparent forwarding, address rewriting, event recording, and fault actions. Keep the protocol surface small (about 12 decoded APIs; everything else is forwarded untouched).

## Consequences

- The project owns protocol-correctness risk. It must be covered by fuzz tests and end-to-end tests with multiple clients.
- TLS and SASL are out of scope in v1.
- A Kroxylicious adapter could reuse the checker later, so this decision doesn't preclude it.
- The fault-injection layer is **not** a novelty claim. Kafka committers are building an equivalent primitive; the contribution is the oracle and the shrinker (see COMPETITORS.md).
