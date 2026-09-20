# Engineering Journal

A dated log of decisions, surprises, bugs, and measurements. It's written for future you: the source for interview stories, ADRs, and technical posts.

**Rules:** write an entry for every working session, keep it short, and record what you *learned*, not just what you did. Surprises and mistakes are the most valuable entries.

---

## Entry template

```markdown
### YYYY-MM-DD: <one-line summary>

**Goal:** what I set out to do.
**Done:** what actually got done.
**Surprise / learned:** anything that behaved differently than I expected, and why.
**Decision:** any design decision made (link an ADR if one was written).
**Measurement:** any number measured, with a link to raw data.
**Open question:** anything unresolved.
**Post idea:** is this worth sharing? One sentence.
```

---

## Phase 0: Reading notes

### Apache Kafka PR #23438 (protocol fault proxy)

- Three design choices that differ from ours, and whether theirs are better:
- How they handle connection lifecycle on test failure, and our equivalent:
- One question worth asking on the PR once we have our own data:

### Real bug sources

- confluent-kafka-javascript pause/rebalance offset issue → corpus bug 7. What exactly goes wrong:
- Framework rebalance-callback documentation → what is unsafe inside a revoke callback:

---

## Phase 0: Mental-model questions

Answer in your own words (see GETTING_STARTED.md, Step 2).

1. **A Kafka response has no API key in its header. How does anything decode a response?**
   _Answer:_

2. **Why does a client connect directly to brokers after its first Metadata request, and what does that mean for a proxy?**
   _Answer:_

3. **What is the difference between a consumer's fetched offset and the group's committed offset?**
   _Answer:_

4. **In the classic protocol, what prevents a consumer that lost its partition from committing offsets? What plays that role under KIP-848?**
   _Answer:_

5. **Why can a Fetch response contain records before the offset the client asked for?**
   _Answer:_

6. **In Postgres, why is commit order (LSN) more trustworthy than a `created_at` timestamp — and why isn't LSN alone enough to order two effects written in the same transaction?**
   _Answer:_

---

## Entries

### _(date)_: Project started

**Goal:** Create the repository and documentation skeleton.
**Done:**
**Surprise / learned:**
**Decision:**
**Open question:**
