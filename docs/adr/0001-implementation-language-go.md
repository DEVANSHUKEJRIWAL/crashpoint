# ADR-0001: Implement Crashpoint in Go

**Status:** Accepted · **Date:** 2026-09-20

## Context

The proxy sits in the data path of every Kafka connection, so its overhead must be small and predictable. The tool must also be easy to install in CI, and it needs a complete, correct implementation of Kafka's request/response formats.

## Options considered

1. **Go.** Lightweight goroutines for thousands of connections, and a single static binary. franz-go's `kmsg` package provides generated codecs for every Kafka request and response type. Built-in profiling (`pprof`) and fuzzing.
2. **Java.** Home of the official client and Kroxylicious. Mature, but JVM startup and memory tuning complicate a CI tool, and it's heavier to distribute.
3. **Rust.** Best raw performance, but a slower iteration speed for one developer on an 8-week timeline, and a less complete Kafka protocol ecosystem.
4. **Python.** Fastest to prototype, but proxy overhead would distort the timing being tested and undermine benchmarks.

## Decision

Use Go, with franz-go's `kmsg` for protocol codecs. Write the framing, header parsing, partial Fetch decoding, and address rewriting by hand.

## Consequences

- One static binary; easy CI and Docker distribution.
- `kmsg` major versions can change when Kafka makes incompatible protocol changes; pin versions and test upgrades.
- Revisit if profiling shows garbage collection pauses materially affect proxy latency percentiles.
