# Contributing to Crashpoint

Crashpoint is in its **design phase**. The most valuable contributions right now are design feedback and real-world failure stories.

## Ways to help

- **Review the design.** Read [docs/DESIGN.md](docs/DESIGN.md) and open an issue if an invariant could produce false positives, a fault is unrealistic, or a detection limit is missing.
- **Tell us your tier.** If your effects table can't reach T1 (see [docs/SUPPORTED.md](docs/SUPPORTED.md)), we want to know what it looks like — the tier model is only as good as the schemas it survives.
- **Share a production bug.** If a Kafka consumer has lost or double-applied events in your system, describe the pattern (no confidential details). It may become part of the seeded bug corpus.
- **Suggest a client or framework** whose rebalance handling deserves testing.

## Design changes

Significant design changes are proposed as Architecture Decision Records:

1. Copy [docs/adr/0000-template.md](docs/adr/0000-template.md) to the next number.
2. Open a pull request with the ADR in status **Proposed**.
3. Once discussed, it's marked **Accepted** and the affected documents are updated in the same PR.

Accepted ADRs are never edited to reverse a decision. A new ADR supersedes them.

## Code contributions (once implementation begins)

- Every parser change includes fuzz tests.
- Every checker change includes a history-based test, with a known violation and a valid look-alike.
- Any change that could weaken an invariant's soundness must state which tier it applies to and why it cannot produce a false positive.
- Performance claims include a benchmark with raw output.
- Commit messages follow `area: summary` (for example `proxy: rewrite DescribeCluster broker addresses`).

## Reporting issues found by Crashpoint in other projects

If Crashpoint reveals a problem in a third-party client or framework, report it **privately to the maintainers first**, and only discuss it publicly after they confirm or respond.
