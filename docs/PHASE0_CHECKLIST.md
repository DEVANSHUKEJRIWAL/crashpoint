# Phase 0 Execution Checklist

One working day, seven blocks. Tick each box. Where a block says "write," the writing goes in `docs/JOURNAL.md` unless stated otherwise.

**Total: ~7.5 hours.** If you only have half a day, do blocks 1, 2, 5 and 7, and move blocks 3 and 4 into Week 1 evenings.

---

## Block 1 — Repository (1 hour)

- [ ] Install prerequisites: `git`, the GitHub CLI (`gh`), Go (current stable), Docker
- [ ] `gh auth login` (choose HTTPS, authenticate in browser)
- [ ] Unzip the documentation set into an empty folder named `crashpoint`
- [ ] From inside that folder, run `bash scripts/bootstrap-repo.sh --dry-run` and read what it intends to do
- [ ] Run `bash scripts/bootstrap-repo.sh` for real
- [ ] Open the repository in the browser: docs render, license is present, 11 issues exist under milestone "Week 1"
- [ ] Add the repository description and topics if the script couldn't (it tries)

**Output:** a public repo with the design documents, Apache-2.0 license, milestones, labels, and Week 1 issues.

---

## Block 2 — Read Apache Kafka PR #23438 (1 hour)

<https://github.com/apache/kafka/pull/23438>

Kafka committers built (and merged in September 2026) a wire-protocol fault-injection proxy with deterministic triggers: `injectError`, `disconnectOn` (which models the exactly-once "commit gap"), `delayOn`, `blackholeClient`. Read the description, the file changes, and every review comment.

Write (about 400 words total):

- [ ] **Three design choices they made that differ from `ARCHITECTURE.md`**, and for each, whether theirs is better and why
- [ ] **How they handle connection lifecycle when a test fails** (a reviewer raises this) and what Crashpoint's equivalent is
- [ ] **Why they separate deterministic triggers from probabilistic ones**, and where that distinction appears in `DESIGN.md`
- [ ] **One question or contribution for the follow-ups** (JIRA KAFKA-21074, or the announced multi-broker follow-up PR) once you have your own measurements. Save it; don't post yet.

**Why this matters:** this is your design review. It's also the single best interview line available to you: *"While designing mine, Kafka committers published a proxy with the same primitives; here's where our designs differ and why."*

---

## Block 3 — Ground the bug corpus in reality (1 hour)

- [ ] Read the `confluent-kafka-javascript` issue where `pause()` during shutdown with auto-commit leaves stale committed offsets and causes reprocessing after a rebalance. Search: `confluent-kafka-javascript pause rebalance offset issue 404`
- [ ] Write: the exact sequence of events that produces the bug, in five bullet points
- [ ] Read one framework's rebalance-callback documentation (Spring Kafka, or your client's), focusing on what is unsafe inside a revoke callback
- [ ] Write: which corpus bug in `DESIGN.md` Section 11 each maps to, and whether the fault listed there would actually expose it

**Output:** corpus bugs 5, 6 and 7 are grounded in real reports rather than invented, which is what makes your detection-rate benchmark credible.

---

## Block 4 — Answer the six mental-model questions (2 hours)

In `docs/JOURNAL.md`, answer in your own words. Use the Kafka protocol guide and the Postgres logical replication docs for anything you can't answer.

- [ ] 1. A Kafka response header has no API key. How does anything decode a response?
- [ ] 2. Why do clients connect directly to brokers after their first Metadata request, and what does that mean for a proxy?
- [ ] 3. What's the difference between a fetched offset and a committed offset?
- [ ] 4. Classic protocol: what stops a consumer that lost its partition from committing? What plays that role under KIP-848?
- [ ] 5. Why can a Fetch response contain records *before* the requested offset, and what breaks if the checker ignores that?
- [ ] 6. Why is commit order (LSN) more trustworthy than `created_at` — and why isn't LSN alone enough to order two effects in one transaction?

**Rule:** if an answer takes more than 150 words, you don't understand it yet. Reread and compress.

---

## Block 5 — Confirm the design decisions (1 hour)

- [ ] Read `DESIGN.md` Section 13 (the six decisions). Accept each, or change it and write an ADR
- [ ] Check: every invariant maps to a tier in Section 3, and you can explain to a non-expert why T1 is the default
- [ ] Check: every v1 fault names a real-world event it models
- [ ] Check: every refusal condition in Section 8 is detectable from traffic the proxy actually sees
- [ ] Read `RISKS.md` Sections 2 and 3 and commit to the cut order **now**, while you're not under pressure
- [ ] Set the changelog dates in `DESIGN.md` and the ADRs

---

## Block 6 — Prepare Week 1 (1 hour)

- [ ] Review the 11 Week 1 issues; add acceptance criteria to any that look vague to you
- [ ] Create a GitHub Project board (Backlog / This week / In progress / Done) and add the Week 1 issues to "This week"
- [ ] Decide your weekly working window (for example: two weekday evenings plus Saturday morning) and put it in the calendar
- [ ] Write the first journal entry: what you plan to build in Week 1 and what could go wrong

---

## Block 7 — Tag the design (30 minutes)

- [ ] `git add -A && git commit -m "docs: design v0.2 with Phase 0 notes"`
- [ ] `git push`
- [ ] `git tag design-v0.2 && git push --tags`
- [ ] Confirm the tag appears in the repo's Releases/Tags view

**Output:** a public, dated record that the design preceded the code. That is itself a signal to anyone who reads the repository.

---

## Optional, non-blocking (5 minutes)

- [ ] Post one question to r/apachekafka or the Confluent Community forum:

> How does your team test that Kafka consumers don't lose or double-apply events when a consumer crashes between its database write and its offset commit, or gets paused long enough to lose its partition mid-batch? Hand-written integration tests, chaos tooling, or nothing?

Check replies once a week. Never block on it.

---

## Phase 0 is done when

- [ ] The repository is public with all documents, license, milestones and issues
- [ ] `JOURNAL.md` contains the PR notes, the bug-source notes, and six answers
- [ ] The design decisions are accepted (or changed, with ADRs)
- [ ] The cut order is agreed with yourself
- [ ] `design-v0.2` is tagged and pushed

Then start Week 1, issue 1.
