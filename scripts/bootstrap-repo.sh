#!/usr/bin/env bash
# Crashpoint Phase 0 bootstrap.
#
# Creates the GitHub repository, license, .gitignore, milestones, labels and the
# Week 1 issues. Run it from inside the folder holding README.md and docs/.
#
#   bash scripts/bootstrap-repo.sh --dry-run    # show what would happen
#   bash scripts/bootstrap-repo.sh              # do it
#
# Requires: git, gh (authenticated with `gh auth login`).
# Safe to re-run: existing milestones, labels and issues are skipped.

set -euo pipefail

REPO_NAME="${REPO_NAME:-crashpoint}"
VISIBILITY="${VISIBILITY:---public}"
DESCRIPTION="Find the Kafka consumer bugs that lose or double-apply events, before production."
TOPICS=(kafka distributed-systems testing fault-injection golang chaos-engineering)

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }
run() {
  if [[ $DRY_RUN -eq 1 ]]; then
    printf '    [dry-run] %s\n' "$*"
  else
    eval "$@"
  fi
}

# ---------------------------------------------------------------- preflight
say "Preflight"
for cmd in git gh; do
  command -v "$cmd" >/dev/null || { echo "ERROR: $cmd is not installed."; exit 1; }
done
[[ -f README.md && -d docs ]] || {
  echo "ERROR: run this from the folder containing README.md and docs/."; exit 1; }
if [[ $DRY_RUN -eq 0 ]]; then
  gh auth status >/dev/null 2>&1 || { echo "ERROR: run 'gh auth login' first."; exit 1; }
fi

# Refuse to run inside SOMEONE ELSE'S repository. Git searches parent
# directories for .git, so a project nested under another repo (or under a
# home directory that is itself a repo) would otherwise target the wrong remote.
if [[ ! -d .git ]]; then
  enclosing="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [[ -n "$enclosing" && "$enclosing" != "$PWD" ]]; then
    cat <<ERR
ERROR: this directory is nested inside another git repository:
         $enclosing
       Milestones, labels and issues would be created on that repository.
       Fix it with either:
         git init                       # claim this directory as its own repo
       or move the project somewhere outside that repository first.
ERR
    exit 1
  fi
fi
info "ok"

# ------------------------------------------------------------------ license
say "License (Apache-2.0)"
if [[ -f LICENSE ]]; then
  info "LICENSE already exists, skipping"
else
  run "gh api /licenses/apache-2.0 --jq .body > LICENSE"
  [[ $DRY_RUN -eq 0 ]] && info "wrote LICENSE"
fi

# ---------------------------------------------------------------- gitignore
say ".gitignore"
if [[ -f .gitignore ]]; then
  info ".gitignore already exists, skipping"
elif [[ $DRY_RUN -eq 1 ]]; then
  info "[dry-run] would write .gitignore"
else
  cat > .gitignore <<'IGNORE'
# Binaries and build output
/crashpoint
/bin/
*.exe
*.test
*.out

# Trial output and local state
/crashpoint-out/
/out/
*.cpb
*.db

# Benchmark raw data is committed; local scratch is not
/bench/scratch/

# Go
/vendor/
go.work
go.work.sum

# Editors and OS
.idea/
.vscode/
.DS_Store
IGNORE
  info "wrote .gitignore"
fi

# ------------------------------------------------------------- git + remote
say "Git repository"
if [[ -d .git ]]; then
  info "git already initialized"
else
  run "git init -q"
  run "git branch -M main"
fi
run "git add -A"
if [[ $DRY_RUN -eq 1 ]]; then
  info "[dry-run] would commit 'docs: design v0.2'"
else
  git diff --cached --quiet && info "nothing to commit" \
    || git commit -q -m "docs: design v0.2" && info "committed"
fi

if git remote get-url origin >/dev/null 2>&1; then
  info "remote 'origin' already set: $(git remote get-url origin)"
  run "git push -u origin main"
else
  run "gh repo create '$REPO_NAME' $VISIBILITY --source=. --remote=origin --push --description '$DESCRIPTION'"
fi

# Resolve the target from origin's URL, NOT from `gh repo view`: gh may resolve
# to a parent/upstream repository and send every later call to the wrong place.
origin_url="$(git remote get-url origin 2>/dev/null || true)"
SLUG="$(sed -E 's#(git@|https://)github.com[:/]##; s#\.git$##' <<<"$origin_url")"
if [[ $DRY_RUN -eq 1 && -z "$SLUG" ]]; then
  SLUG="$(gh api user --jq .login 2>/dev/null || echo OWNER)/$REPO_NAME"
  info "repository (predicted): $SLUG"
elif [[ -z "$SLUG" ]]; then
  echo "ERROR: no 'origin' remote; repository creation did not succeed."; exit 1
else
  me="$(gh api user --jq .login 2>/dev/null || true)"
  owner="${SLUG%%/*}"
  if [[ -n "$me" && "$owner" != "$me" ]]; then
    echo "ERROR: origin points at '$SLUG', which is not owned by '$me'. Refusing to modify it."
    exit 1
  fi
  info "repository: $SLUG"
fi

# ------------------------------------------------------------------- topics
say "Topics"
for t in "${TOPICS[@]}"; do
  run "gh repo edit '$SLUG' --add-topic '$t' >/dev/null" \
    || info "could not add topic '$t'; add it in the repo settings"
done

# --------------------------------------------------------------- milestones
say "Milestones"
existing_ms="$(gh api "repos/$SLUG/milestones?state=all" --jq '.[].title' 2>/dev/null || true)"
for w in 1 2 3 4 5 6 7 8; do
  title="Week $w"
  if grep -qx "$title" <<<"$existing_ms"; then
    info "$title exists"
  else
    run "gh api -X POST 'repos/$SLUG/milestones' -f title='$title' >/dev/null"
    info "created $title"
  fi
done

# ------------------------------------------------------------------- labels
say "Labels"
create_label() {
  local name="$1" color="$2" desc="$3"
  if gh label list --limit 200 --json name -q '.[].name' 2>/dev/null | grep -qx "$name"; then
    info "label '$name' exists"
  else
    run "gh label create '$name' --color '$color' --description '$desc' >/dev/null"
    info "created label '$name'"
  fi
}
create_label "proxy"     "1d76db" "Kafka wire protocol and proxying"
create_label "checker"   "0e8a16" "Invariants and evidence"
create_label "nemesis"   "b60205" "Fault injection and triggers"
create_label "sut"       "5319e7" "Reference system under test"
create_label "benchmark" "fbca04" "Measurement and performance"
create_label "docs"      "c5def5" "Documentation"

# ------------------------------------------------------------------- issues
say "Week 1 issues"
create_issue() {
  local title="$1" labels="$2" body="$3"
  if gh issue list --limit 200 --state all --json title -q '.[].title' 2>/dev/null | grep -qxF "$title"; then
    info "skip (exists): $title"
    return
  fi
  if [[ $DRY_RUN -eq 1 ]]; then
    info "[dry-run] would create: $title"
    return
  fi
  gh issue create --title "$title" --milestone "Week 1" --label "$labels" --body "$body" >/dev/null
  info "created: $title"
}

create_issue "Local environment: Kafka (KRaft) + Postgres with logical WAL" "sut" \
"## Acceptance criteria
- [ ] \`docker compose up -d\` starts Apache Kafka 4.x in KRaft mode and Postgres
- [ ] Postgres runs with \`wal_level=logical\`, and a \`crashpoint\` role has REPLICATION
- [ ] Schema applied: accounts, ledger_entries (NO unique constraint on the event id), processed_events
- [ ] Publication \`crashpoint_effects\` exists for the effects table
- [ ] Topic \`payments.requested\` created with 6 partitions
- [ ] README section documents ports and how to reset state

See DESIGN.md sections 3 and 4."

create_issue "Reference payments consumer: control mode" "sut" \
"## Acceptance criteria
- [ ] Consumes \`payments.requested\`, writes ledger rows and updates balances
- [ ] Dedup marker and effect commit in ONE transaction
- [ ] Offsets committed only after the transaction commits
- [ ] Rebalance cannot complete between poll and commit
- [ ] Unique client.id per instance (SUPPORTED.md section 1)
- [ ] Effects table carries event id, key, seq and writer id, so all tiers can be exercised

This is the control: it must never produce a violation."

create_issue "Reference consumer: bug modes 1-3 behind a flag" "sut" \
"## Acceptance criteria
- [ ] \`-mode\` flag selects control or a seeded bug
- [ ] bug1: auto-commit with asynchronous processing
- [ ] bug2: offsets committed before the database transaction
- [ ] bug3: no dedup on insert
- [ ] Every mode passes a happy-path smoke test (bugs must be invisible without faults)

See DESIGN.md section 11."

create_issue "Workload producer with tri-state input status" "sut" \
"## Acceptance criteria
- [ ] Produces events with unique event_id, key and per-key seq
- [ ] Records every input as acknowledged / failed / indeterminate with partition and offset
- [ ] Timeouts classified as indeterminate, not failed
- [ ] Payload encoding behind a \`PayloadEncoder\` interface (JSON ships)
- [ ] Configurable rate and event count

See DESIGN.md section 4."

create_issue "Proxy: framing, request headers, correlation tracking" "proxy" \
"## Acceptance criteria
- [ ] Reads size-prefixed frames with a maximum frame guard
- [ ] Parses request headers v1/v2 including client id and tagged fields
- [ ] Per-connection correlation id -> (api key, version) map, registered BEFORE forwarding
- [ ] Produce with acks=0 is not registered (no response will come)
- [ ] ApiVersions responses use header v0 even at flexible versions
- [ ] Unknown API keys are forwarded untouched"

create_issue "Proxy: Metadata / FindCoordinator / DescribeCluster rewriting" "proxy" \
"## Acceptance criteria
- [ ] Broker addresses in all three responses are rewritten to proxy addresses
- [ ] One listener per broker node, created lazily on first sighting
- [ ] FindCoordinator v0-3 (top level) and v4+ (batched) both handled; errored entries skipped
- [ ] Recorded frames prove Produce and Fetch traffic reaches node listeners (no bypass)
- [ ] Works with franz-go and the Java client"

create_issue "Proxy: connection classification and harness safety" "proxy" \
"## Acceptance criteria
- [ ] Every accepted connection tagged \`sut\` or \`harness\` at accept time
- [ ] Faults may target only \`sut\` connections
- [ ] A test asserts a fault targeting everything never touches a harness connection

Crashpoint must never fault its own workload: that would corrupt the ground truth.
See DESIGN.md section 7.2."

create_issue "Proxy: fuzz tests for every parser" "proxy" \
"## Acceptance criteria
- [ ] Fuzz target for frame reading
- [ ] Fuzz target for request header parsing
- [ ] Fuzz target for tagged-field skipping
- [ ] No panics and no out-of-range results on arbitrary input
- [ ] Short fuzz run wired into CI

A protocol parser without fuzzing is a red flag."

create_issue "Recorder: frame events to an inspectable log" "proxy" \
"## Acceptance criteria
- [ ] One record per frame: time, connection, listener, direction, api, version, correlation id, size
- [ ] Responses carry the latency since their request
- [ ] Output greppable with jq during Week 1 (binary format lands in Week 2)
- [ ] Recording can be switched off for benchmark baselines"

create_issue "Baseline benchmark: direct vs proxied" "benchmark" \
"## Acceptance criteria
- [ ] Open-loop load generator (no coordinated omission)
- [ ] Direct-to-broker vs through-proxy, recording off and on
- [ ] p50/p99/p99.9 latency and max throughput reported
- [ ] Environment table in BENCHMARKS.md filled in
- [ ] Raw output committed under bench/baseline/

Measure before optimizing anything."

create_issue "Manual experiment: reproduce a duplicate by hand" "sut" \
"## Acceptance criteria
- [ ] Run bug3 (no dedup), kill -9 a consumer between the effect and the offset commit
- [ ] A duplicate ledger row is observed and the SQL proving it is recorded
- [ ] Record how many attempts it took, and the approximate window width
- [ ] Journal entry written; this is post #1 and the motivation for the whole tool

STOP CONDITION: if the window cannot be hit by hand at all, re-read RISKS.md section 5
before building further."

say "Done"
if [[ $DRY_RUN -eq 1 ]]; then
  info "Dry run only. Re-run without --dry-run to apply."
else
  info "Repository: https://github.com/$SLUG"
  info "Next: docs/PHASE0_CHECKLIST.md, block 2."
fi
