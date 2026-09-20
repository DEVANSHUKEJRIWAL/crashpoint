# ADR-0005: Ship a single binary with in-process components; don't use Kafka for Crashpoint's own events

**Status:** Proposed · **Date:** _(date)_

## Context

Crashpoint has several logical components (proxy, recorder, nemesis, checker, runner). It could be split into services communicating over a message bus.

## Options considered

1. **Microservices connected by Kafka.** Looks impressive, but Crashpoint injects faults into Kafka, so its own event path would be subject to the same faults (a circular dependency). It adds operational burden and network hops that blur trigger timing.
2. **Microservices connected by gRPC.** Avoids circularity, but adds latency to triggers that need sub-millisecond reaction, with no benefit at single-machine scale.
3. **Single binary with bounded in-process channels.** Fastest trigger path, simplest to install and run in CI.

## Decision

A single binary. Components communicate through bounded in-process channels. Trial metadata is stored in SQLite.

## Consequences

- Very simple deployment: download one binary and run it.
- Horizontal scaling happens only at the trial-runner level (see ARCHITECTURE.md Section 9).
- Revisit if a hosted, multi-team version is built: the control plane and runners would then become separate services.
