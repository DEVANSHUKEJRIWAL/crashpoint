# ADR-0003: Run trials against real Kafka brokers; use kfake only for unit and end-to-end tests of Crashpoint itself

**Status:** Proposed · **Date:** _(date)_

## Context

Crashpoint's findings are only meaningful if the group coordinator behaves like a real one, including rebalance timing and KIP-848 member epochs.

## Options considered

1. **Real Apache Kafka in Docker (KRaft).** Faithful semantics. Slower startup, and heavier on resources.
2. **franz-go `kfake` in-memory cluster.** Very fast and embeddable, with fault hooks. It's a fake, though: coordinator behavior may diverge from Apache Kafka in exactly the edge cases Crashpoint targets.
3. **Both, tiered.** `kfake` for fast smoke exploration, real brokers to confirm findings.

## Decision

Trials run against real Apache Kafka. `kfake` is used only to test Crashpoint's own proxy and parsers.

## Consequences

- Trial throughput is limited by container startup; mitigated by sharing a broker per runner with per-trial topic and group names.
- Every reported violation reflects real broker behavior.
- Revisit tiered exploration (option 3) if throughput becomes the main bottleneck.
