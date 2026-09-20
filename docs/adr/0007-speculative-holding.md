# ADR-0007: Hold offset commits speculatively to hit crash windows

**Status:** Accepted · **Date:** _(date)_

## Context

Crashpoint's central capability is crashing a consumer between its database commit and its offset commit. The trigger requires two events: the effect appearing in the CDC stream, and the OffsetCommit being in flight at the proxy. Logical replication lag is usually milliseconds, but so is the window. If the OffsetCommit arrives at the proxy before the CDC event, the proxy has already forwarded it and the window is missed.

Left unsolved, the tool's headline capability degrades to luck under load — exactly what it claims to improve on.

## Options considered

1. **Accept misses.** Count them and move on. Simple, but detection becomes rate-dependent and the "precise placement" claim weakens.
2. **Block all commits until the checker confirms.** Reliable, but changes SUT timing so much that the test stops resembling production.
3. **Speculative holding with a bounded budget.** When a crash-window trigger is armed, hold every OffsetCommit for the targeted partition for a short budget, then fire or release.

## Decision

Option 3, with a default budget of 10 ms, always well below the client's `request.timeout.ms`. If the CDC event arrives within the budget, the fault fires; otherwise the commit is released untouched and a missed window is counted. `windows_hit / windows_armed` is a first-class metric, and the budget is reported with every result.

## Consequences

- Hit rates become high and measurable rather than luck-dependent, and benchmark D4 can test the claim that targeted placement beats random chaos.
- Crashpoint perturbs the SUT by design; that perturbation is disclosed with every finding.
- The budget interacts with client timeouts and with connection sharing between group and fetch traffic; both are recorded in trial metadata.
- Revisit if measured hit rates stay below 70% with a reasonable budget.
