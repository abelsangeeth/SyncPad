# SyncPad

A real-time collaborative document editor. Many people type into the same
document at once; nobody waits for a lock, nobody's keystrokes are discarded,
and every participant ends up looking at identical text.

Python · FastAPI · WebSockets · RGA CRDT · Redis Pub/Sub · PostgreSQL · GitHub Actions

---

## The idea in one paragraph

Fifty people typing into one paragraph is not a locking problem, it is an
**ordering** problem. Operational transformation solves it by rewriting
operations against one another, which needs a central server to fix an order and
gets combinatorially unpleasant past three concurrent sites. SyncPad uses a
**Replicated Growable Array** instead: every character carries an immutable
identity, and the rule for where a character belongs depends only on identities
the replica already holds. No server needs to referee, so the server does not
need to be a single machine — which is what makes the rest of the architecture
possible.

The integration rule is three lines:

> To insert element `e` after origin `p`, scan right from `p`, skipping every
> element whose id sorts above `e`'s, and place `e` before the first element
> whose id sorts below.

It converges because of one invariant: a character's id always sorts above its
left origin's. So an element `x` with `x.id > e.id` has an entire subtree that
also sorts above `e`, and the scan steps over whole subtrees atomically without
ever having to identify them. Every replica walks the same elements to the same
conclusion regardless of the order operations arrived in.

That claim is not argued, it is [tested](#what-is-actually-verified).

---

## Quick start

No Docker, no database server, no build step:

```bash
cd syncpad                                         # the directory holding this README
python -m venv .venv && . .venv/Scripts/activate   # or bin/activate
pip install -e "./backend[dev]"
python -m syncpad --reload
# → http://127.0.0.1:8000
```

That runs SQLite and an in-process hub. The same code with two environment
variables set runs Postgres and Redis across N replicas:

```bash
export SYNCPAD_DATABASE_URL=postgresql://syncpad:syncpad@localhost:5432/syncpad
export SYNCPAD_REDIS_URL=redis://localhost:6379/0
```

Or bring up the whole thing — three gateway replicas behind nginx, Postgres and
Redis:

```bash
docker compose up --build        # → http://localhost:8080
```

To deploy it for real, see **[DEPLOY.md](DEPLOY.md)**. The short version: the
database goes on managed Postgres (Supabase), and the gateway goes on anything
that runs a container, because a WebSocket server needs a long-lived process
that serverless platforms do not offer.

---

## Architecture

```
                      ┌────────────────────────────────┐
  browser ──ws──┐     │  nginx — round-robin, NO        │
  browser ──ws──┼────▶│  sticky sessions, NO ip_hash    │
  browser ──ws──┘     └───────────────┬────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
       ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
       │  gateway 1  │         │  gateway 2  │         │  gateway 3  │
       │  (stateless)│         │  (stateless)│         │  (stateless)│
       └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
              │   Redis Pub/Sub — one connection per replica,  │
              └────────────┬──────────┴───────────┬────────────┘
                           ▼                      ▼
                  ┌─────────────────┐   ┌───────────────────┐
                  │  Redis          │   │  PostgreSQL       │
                  │  fan-out +      │   │  append-only op   │
                  │  presence (TTL) │   │  log + snapshots  │
                  └─────────────────┘   └───────────────────┘
```

**Nothing is pinned.** A replica holding a document is holding a *cache*, not
ownership; three replicas may hold the same document simultaneously and that is
fine. `deploy/nginx.conf` deliberately uses plain round-robin — if SyncPad
needed session affinity, that config would break it, which is precisely why it
is written that way.

### Why each piece is there

| Concern | Mechanism | Why not the obvious alternative |
|---|---|---|
| Concurrent edits | RGA CRDT | OT needs a central sequencer; CRDT does not, so the gateway can be replicated |
| Cross-replica delivery | Redis Pub/Sub | Sticky sessions put state in the load balancer and make a restart disconnect everyone that replica owned |
| Durability | Append-only op log | Never rewriting history makes every read reproducible and gives an audit trail free |
| Open an old document fast | Periodic snapshots | Replaying a year of keystrokes is unbounded work for a bounded document |
| Comment anchors | Character ids | Offsets rot the instant anyone types above them |
| Ghost collaborators | Presence TTL, swept on read | A crashed process never sends a clean "leave", so cleanup cannot depend on one |

---

## Features

**Editing** — concurrent rich text (bold, italic, code, strikethrough, links,
headings) with span-anchored marks, so typing inside a bold word stays bold and
a mark applied concurrently with an insert still covers the new text.

**Presence** — live cursors and selections with per-user colours, rendered from
CRDT anchors rather than offsets, so a remote edit three paragraphs up never
moves your caret.

**Offline-first** — edits apply to the local replica immediately and queue for
transmission. Lose the connection and typing continues; on reconnect the client
sends `since`, receives only what it missed, replays its unacknowledged buffer,
and converges. Replay is safe because operations are idempotent.

**Version history** — every revision is listed, any retained point can be
previewed, and restore **appends** the operations that transform the present
into the past rather than deleting anything. History only ever grows, so a
restore is itself a restorable point, and it merges with concurrent edits like
any other change.

**Comments** — anchored to the ids of the first and last character, so they
track their text through any amount of concurrent editing. When the anchored
text is finally deleted the comment is reported as `orphaned` rather than
silently relocated onto whatever now occupies those offsets.

**Sharing** — per-document roles (owner / editor / commenter / viewer),
capability share links with optional expiry, and public read-only documents.

**Export** — Markdown, HTML and plain text, driven by the CRDT's span
rendering. The HTML exporter escapes everything and refuses to render
`javascript:` links, because this is the one path where one user's text becomes
another user's page.

**Operations** — Prometheus metrics, `/healthz` (liveness, touches nothing
external) and `/readyz` (readiness, checks every dependency and 503s).

---

## What is actually verified

Claims in this section are produced by scripts in `bench/` and tests in
`backend/tests/`. Everything below was measured on the development machine
(20 cores, Windows, Python 3.14); absolute numbers will differ on other
hardware, but every figure is reproducible with the command shown.

### Convergence

```
backend/tests/test_rga_convergence.py    # 2–5 replicas, randomised interleaving
```

Property-based, not example-based: Hypothesis generates concurrent editing
sessions, delivers every operation to every replica in random order with random
duplicates, and asserts all replicas agree on text *and* formatting. It also
asserts idempotence, that shuffled and causal delivery agree, and that a
snapshot-restored replica stays convergent with one that never restarted.

These tests have teeth — sabotaging the tie-break to compare by site instead of
Lamport counter makes Hypothesis produce a **one-step** counterexample.

### The browser and the server agree

```bash
python bench/conformance.py --trials 250 --sites 4 --steps 30
# OK: 250 trials, 7500 operations, 4 concurrent sites — Python and JavaScript agree exactly
```

SyncPad has two independent RGA implementations — one in Python, one in
JavaScript — and they only interoperate if they agree exactly on id ordering,
the integration rule, and the wire format. Unit tests on either side cannot
catch a disagreement, because each is self-consistent. So this generates
concurrent operation logs in Python, replays them in Node in a *different*
order, and demands identical text and spans back.

It has already earned its place twice. It caught the JavaScript side indexing
strings by UTF-16 code unit while Python used code points, which split every
emoji into two independently-reorderable halves; and it caught span merging
being key-order sensitive in JavaScript but not in Python.

### Latency

```bash
python bench/loadtest.py --clients 25 --duration 20 --workers 4 --assert-p95-ms 150
```

Measured end to end: client A sends, the gateway applies, **persists**, and
fans out, client B receives. Persistence is inside the measurement because
excluding it would measure a system nobody runs.

| Editors in one document (single replica) | p50 | p95 |
|---:|---:|---:|
| 2 (uncontended) | 6 ms | 7 ms |
| 10 | 18 ms | 32 ms |
| 25 | 56 ms | 96 ms |
| 50 | 110–138 ms | 227–352 ms |

Ranges are the spread across repeated runs on a machine that is also hosting
the clients; single figures are one representative run. Treat the shape as the
result, not the third significant digit.

Fan-out is inherently N², so a single process saturates: fifty editors at ~65
operations per second means roughly 3,200 socket writes per second from one
event loop. **Median stays under 60 ms to about 25 concurrent editors per
replica.** Past that the answer is more replicas, not a faster loop — which is
the next measurement.

Clients are sharded across processes, and that detail is the difference between
a benchmark and a fiction: a single-process harness saturates before the server
does and reports its own backlog as server latency. The run also prints the
server's own Prometheus histogram as a cross-check; if the two disagree
sharply, the harness is what is under load.

### Horizontal scaling

```bash
python -m bench.scaleout --clients 50 --duration 15 --replicas 3
```

Same load, same code, Postgres and Redis, 50 editors in one document:

| Configuration | p50 | p95 |
|---|---:|---:|
| 1 replica | 2140 ms | 3805 ms |
| 3 replicas | 124 ms | 236 ms |

**p95 improves 16×.** Read honestly: at this load one replica is well past its
knee, and queueing amplifies the ratio. The point is not the multiple but the
direction — adding identical, stateless processes behind a dumb load balancer
reduces latency, which is only true because nothing is pinned.

### Cross-replica correctness

```bash
python -m bench.replica_convergence --replicas 4 --clients 16 --ops 60
# 960 concurrent edits issued in 1.3s
# settled after 5.6s
# OK: 16 clients across 4 replicas converged on identical text and formatting,
#     and the database agrees
```

Sixteen clients spread round-robin across four gateway processes, editing
simultaneously, with aggressive mid-session snapshotting to provoke exactly the
kind of bug that would lose an operation. Every earlier test stubs something
out; this one stubs out nothing.

The harness waits for **quiescence** rather than a fixed settle time, because
offered load deliberately exceeds what one Postgres absorbs. That distinction
matters: a snapshot taken too early cannot tell "permanently diverged" from
"still draining", and the first version of this harness reported a false
failure for exactly that reason.

### Cold load

```bash
python bench/coldload.py --ops 3000 --snapshot-every 200 --max-chars 20000
# full replay : 3000 batches in 3382ms
# snapshot    : 0 batches in 792ms
# replay work reduced 3000x, cold-load time reduced 76.6%
```

Modelled on a long-lived document: it reaches a size and then stops growing
while its history keeps accumulating, which is what real documents do. Replay
work is the primary figure because it is deterministic — identical on a laptop
and in CI — so a regression fails the build instead of flaking it.

### Test suite

```
359 passed, 1 skipped — 96% statement coverage on syncpad/
```

The whole suite runs with no Postgres and no Redis, in about 37 seconds. That
is a constraint on the design, not a convenience: if a feature cannot be
exercised without a server, it has leaked dialect-specific behaviour somewhere
it should not have. The Redis adapters are covered against an in-process fake
so the multiplexing and reconnect paths run on every commit, and tests that
genuinely need live infrastructure are marked `integration`.

---

## Layout

```
backend/syncpad/
  crdt/          pure, synchronous, no I/O — ids, ops, rga, marks, compaction
  storage/       models, repo, snapshots — append-only log + snapshot cache
  gateway/       protocol, hub (Redis/in-memory), presence, ws endpoint
  services/      rooms (per-process document cache), rate limiting
  api/           auth, documents, history, comments, metrics
  main.py        app assembly and lifecycle
backend/tests/   359 tests
backend/alembic/ production migrations (dev/test use create_all)
frontend/        plain ES modules, no build step
  src/crdt.js    the RGA again, in the browser
  src/session.js socket, offline buffer, reconnect
  src/editor.js  contenteditable with id-anchored selection
bench/           the scripts behind every number above
deploy/          nginx, deliberately without sticky sessions
```

The CRDT layer imports nothing from storage, gateway or the API, does no I/O,
and is fully synchronous. That is enforced by its import list and is what makes
the property tests cheap enough to run thousands of cases per commit.

---

## Design decisions worth explaining

**Restore appends, never rewinds.** Deleting operations after a chosen point
would race replicas that still hold them and clients that are offline with
unacknowledged edits descending from them, and it would destroy the audit
trail. So restore diffs current text against historical text and appends the
operations achieving it — using a real diff, so restoring one paragraph
produces a handful of operations rather than tombstoning the document.

**Snapshots are a cache.** Every snapshot could be deleted and every document
would still rebuild from its log. Pruning is what makes them load-bearing, so
it happens only after the snapshot is durably written, and only below the
*oldest retained* snapshot — cutting at the newest would orphan the others and
make `snapshot_keep` a lie. Time travel beyond the retention horizon returns
**410 Gone**, never a plausible-looking empty document.

**Fan-out leaves the critical path.** Socket writes are queued and flushed on
the next event-loop iteration. When the loop is idle each frame goes out alone
with no added delay; when it is busy, everything that piled up during the tick
leaves as one `batch` frame. The batching appears exactly when it is needed and
costs nothing when it is not. A client that stops draining is disconnected
rather than buffered without limit — it reconnects with `since` and is caught
up from the log.

**Cleanup survives cancellation.** A WebSocket handler is *cancelled*, not
returned from, when a server shuts down, and `CancelledError` derives from
`BaseException` — so an inline `suppress(Exception)` would let teardown stop
part-way, leaving a ghost collaborator in everyone else's sidebar and a room
whose refcount never reaches zero. Teardown runs as a shielded task.

**PBKDF2, not bcrypt.** Not because it is better, but because passlib's bcrypt
backend is broken against current bcrypt releases, and a hashing scheme that
raises on import is worse than one that is merely conventional.

**Adapter seams everywhere.** Postgres/SQLite and Redis/in-memory are chosen in
`Settings` and nowhere else. That is what lets the full suite — WebSockets,
snapshots, cross-replica fan-out — run on a laptop with nothing installed.

---

## Configuration

Every value has a working default; see `.env.example`.

| Variable | Default | Notes |
|---|---|---|
| `SYNCPAD_DATABASE_URL` | SQLite file | Bare `postgresql://` is upgraded to asyncpg automatically |
| `SYNCPAD_REDIS_URL` | unset | Unset ⇒ single-process in-memory hub |
| `SYNCPAD_JWT_SECRET` | dev value | **Set this in production** |
| `SYNCPAD_SNAPSHOT_EVERY_OPS` | 200 | Bounds replay on cold load |
| `SYNCPAD_SNAPSHOT_KEEP` | 5 | Also sets the time-travel horizon |
| `SYNCPAD_RATE_OPS_PER_S` / `_BURST` | 120 / 400 | Per-connection token bucket |
| `SYNCPAD_PRESENCE_TTL_S` | 30 | How long before a silent peer expires |

---

## Known limits

Stated plainly, because a portfolio project that claims no limits is not
describing a real system.

- **One document is bounded by one replica's fan-out.** Redis spreads editors
  across replicas, but every participant in a document still receives every
  operation. Past a few hundred concurrent editors in a *single* document the
  right answer is operation batching with a deliberate latency window, not more
  processes.
- **Tombstone GC is implemented but not wired to a coordinator.**
  `gc_tombstones` is correct and tested, and refuses to drop anything not
  covered by the supplied version vector — but computing a true cluster-wide
  minimum needs a membership service that does not exist here. A document
  edited forever grows its element array forever.
- **The editor is a custom `contenteditable`.** It handles `beforeinput`
  intents rather than diffing the DOM, which keeps it honest, but it has not
  been hardened against every IME and mobile keyboard in existence.
- **Presence is per connection, not per user.** Two tabs are two collaborators
  with two cursors. That is correct for the CRDT and occasionally surprising in
  the sidebar.
- **`scaleout.py`'s 16× is measured past the knee.** See the note there.

---

## Running the checks

```bash
# tests + coverage (no services required)
cd backend && pytest -q --cov=syncpad --cov-report=term-missing

# lint and format
ruff check syncpad tests ../bench && ruff format --check syncpad tests ../bench

# the CRDTs agree with each other
python bench/conformance.py --trials 250 --sites 4 --steps 30

# latency and cold load
python bench/loadtest.py --clients 25 --duration 20 --workers 4 --assert-p95-ms 150
python bench/coldload.py --ops 3000 --snapshot-every 200 --max-chars 20000

# the distributed claims (needs Postgres + Redis)
docker run -d --rm --name pg -e POSTGRES_USER=syncpad -e POSTGRES_PASSWORD=syncpad \
    -e POSTGRES_DB=syncpad -p 5442:5432 postgres:16-alpine
docker run -d --rm --name redis -p 6399:6379 redis:7-alpine
python -m bench.replica_convergence --replicas 4 --clients 16 --ops 60
python -m bench.scaleout --clients 50 --duration 15 --replicas 3
```

CI runs all of it on every push: lint, the suite with a coverage gate,
integration tests against real Postgres and Redis, `alembic check` to prove the
migrations still match the models, the benchmarks with assertion thresholds,
and a Docker build that brings the three-replica stack up and smoke-tests it
through the load balancer.
