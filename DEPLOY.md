# Deploying SyncPad on Supabase

## Supabase cannot host the whole thing, and that is not a configuration problem

SyncPad's gateway is a process that holds thousands of WebSocket connections
open for as long as people are editing. Supabase's only compute product is Edge
Functions: Deno, request-scoped, not a place to keep a Python process and a
socket table alive. There is no setting that changes this.

So the deployment is split, and Supabase does the part it is excellent at:

```
   browser ──── wss ────►  Fly.io machine          ── TLS ──►  Supabase
                           (FastAPI gateway,                   (Postgres:
                            stateless, holds                    operation log,
                            the sockets)                        snapshots, users,
                                                                comments, ACLs)
```

Anything that can run a container works in Fly's place — Render, Railway,
Koyeb, a VPS. The config in [`fly.toml`](fly.toml) is written for Fly because it
handles WebSockets without extra configuration and needs no sticky sessions.

---

## 1. Create the Supabase project and take the right connection string

Supabase offers three, and they are not interchangeable.

| Shown as | Host and port | Use it? |
|---|---|---|
| **Session pooler** | `aws-0-<region>.pooler.supabase.com:5432` | **Yes — use this** |
| Transaction pooler | `aws-0-<region>.pooler.supabase.com:6543` | Works, but see below |
| Direct connection | `db.<ref>.supabase.co:5432` | Usually IPv6-only now |

Take the **session pooler** string. A long-running server keeps its own
connection pool, which is exactly what session mode is for: one Postgres backend
per pooled connection, so prepared statements are cached and `LISTEN`/`NOTIFY`
would work if you ever want it.

The transaction pooler also works — SyncPad detects it and turns off
prepared-statement caching automatically — but it is aimed at serverless
functions that open a connection per request, which is the opposite of this
workload, and it costs you the statement cache.

Two things that will waste an hour if you miss them:

- **The pooler username is `postgres.<project-ref>`, not `postgres`.** Copy the
  string whole rather than assembling it.
- **Percent-encode the password.** If it contains `@ / : # ? &`, paste it into a
  URL as-is and the parser will read your password as a hostname. `@` is `%40`.

Nothing else needs editing. `?sslmode=require` is translated for you — see
[`backend/syncpad/storage/dsn.py`](backend/syncpad/storage/dsn.py) for why that
is not a no-op.

---

## 2. Create the Fly app and set secrets

```bash
cd syncpad
fly apps create your-syncpad-name          # must be globally unique
```

Then edit the `app` and `primary_region` lines in `fly.toml`. **Put the machine
in the same region as the Supabase project.** Every operation is persisted before
it is broadcast, so a cross-region hop is added to every keystroke, not just to
page loads. Supabase in `ap-south-1` means Fly in `bom`.

```bash
# 1) the database
fly secrets set SYNCPAD_DATABASE_URL="postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres"

# 2) a real signing key - every session token is signed with it, so changing
#    it later logs everyone out
fly secrets set SYNCPAD_JWT_SECRET="$(python -c 'import secrets; print(secrets.token_urlsafe(48))')"

# 3) lock CORS to your own origin instead of the "*" dev default
fly secrets set SYNCPAD_CORS_ORIGINS='["https://your-syncpad-name.fly.dev"]'
```

## 3. Deploy

```bash
fly deploy
```

`release_command = "alembic upgrade head"` runs the migrations against Supabase
before the new version takes traffic. A failed migration aborts the release and
leaves the running version alone.

```bash
fly logs
curl https://your-syncpad-name.fly.dev/readyz
```

`/readyz` returns `{"ready":true,"checks":{"database":"ok",...}}` when the
machine can actually reach Supabase, and `503` when it cannot — it is the health
check in `fly.toml` for that reason. `/healthz` only says the process is alive.

---

## Deploying to a Hugging Face Space instead of Fly

A Space runs one Docker container with no card required, which suits this project
because the gateway is a single stateless process. Two constraints follow from
"one container": there is no release step, so migrations run at boot; and there
is no horizontal scaling, so the in-process hub is the right choice and Redis is
not needed. (It also means the Redis section below does not apply until you move
somewhere you can run more than one instance.)

A Space is a separate git repo needing `Dockerfile` and `README.md` at its root,
which is not this project's layout. Rather than keep a second Dockerfile that
drifts, assemble the Space tree from the real one:

```bash
python deploy/hf-space/build_space.py --out build/space
hf auth login                                     # token stays on your machine
hf repos create <your-username>/syncpad --type space --space-sdk docker
hf upload <your-username>/syncpad build/space . --type space
```

Then set two secrets under **Settings -> Variables and secrets** and restart:

| Secret | Value |
|---|---|
| `SYNCPAD_DATABASE_URL` | The Supabase session-pooler string, password included |
| `SYNCPAD_JWT_SECRET` | `python -c "import secrets; print(secrets.token_urlsafe(48))"` |
| `SYNCPAD_MIGRATE_ON_START` | `1` — a Space has no release hook |

`SYNCPAD_ENV=prod` and `SYNCPAD_MIGRATE_ON_START=1` are already in the Space's
Dockerfile environment via the build; set them as variables if you change them.

### The live database for this project

Supabase project `syncpad`, ref `uaoyiajhobbpedaysarp`, region `us-east-1`
(chosen to sit in the same region as the Space, because every operation is
persisted before it is broadcast). Postgres 17.6.

Schema was applied through the Supabase management API from the same Alembic
revision, `4a186858e2ad`, including the `alembic_version` row — so the container's
boot migration is a no-op rather than a conflict. **RLS is enabled on all eight
tables with no policies**, which blocks the anon key and PostgREST entirely while
leaving the application untouched, because SyncPad connects as `postgres` over
asyncpg and never uses either. Verified: the app role sees a test row, `anon` sees
zero of it.

---

## Scaling past one machine requires Redis. This one is a correctness issue.

Fan-out between replicas goes through Redis Pub/Sub. With no `SYNCPAD_REDIS_URL`,
each machine uses an in-process hub, which is correct for one machine and
**silently wrong for two**: clients that land on different machines do not
receive each other's operations. Nothing errors. The documents simply diverge,
and each user sees a plausible document that is missing the other's edits.

`fly.toml` therefore pins `min_machines_running = 1`. Before raising it:

```bash
fly secrets set SYNCPAD_REDIS_URL="rediss://default:<password>@<host>.upstash.io:6379"
fly scale count 3
```

Any managed Redis works; Upstash has a free tier and speaks TLS via `rediss://`.
Sticky sessions are deliberately not configured — the gateway holds no session
state, and not needing them is the property the Redis fan-out buys.

### Connection budget

Supabase caps how many client connections a project may hold, shared across
everything you point at it. `fly.toml` sets `pool_size=5` and `max_overflow=5`
per machine, so the ceiling is `10 x machines`. Raising `fly scale count`
without checking that budget produces connection-refused errors under load
rather than at deploy time.

---

## What was verified before writing this, and what was not

Reproduced locally against Postgres 16 behind a real PgBouncer 1.25 in
transaction mode, which is the closest available stand-in for Supavisor:

- the container image builds, and `alembic upgrade head` runs inside it against
  an empty database and produces all 8 tables
- the image serves real WebSocket traffic through the pooler: 220 operations,
  2239 frames, **0 errors**, p50 34ms / p95 64ms
- `/readyz` returns 503, not 500, when the database is unreachable
- the prepared-statement fix is load-bearing: two processes sharing one pooled
  backend produce 61 failed queries without it and 0 with it

Not verified, because it needs a live project: Supavisor's own behaviour, and
real latency between Fly and Supabase. **The latency numbers in
[`README.md`](README.md) were measured with the database on the same machine.**
Expect them to be worse here by roughly the round-trip time to your Supabase
region, on every operation.

---

## Troubleshooting

Each of these was produced deliberately while building this deployment.

| Symptom | Cause |
|---|---|
| `TypeError: connect() got an unexpected keyword argument 'sslmode'` | A libpq URL reached asyncpg untranslated. Every path should go through `plan_connection`; if you added a new one, route it through there too. |
| `TypeError: ... unexpected keyword argument 'pgbouncer'` | Same cause, Prisma-style URL. |
| `DuplicatePreparedStatementError: prepared statement "__asyncpg_stmt_1__" already exists` | Transaction pooling with statement caching on. asyncpg numbers statements from a per-process counter, so two machines both start at `_1_`. Confirm the URL is port 6543 or carries `?pgbouncer=true` so it is detected. |
| `ImportError: The SQLAlchemy asyncio module requires ... greenlet` | `sqlalchemy[asyncio]` missing. Invisible locally when another package already provided greenlet; it appears only in a clean image. |
| `InterpolationSyntaxError` from Alembic | A `%` in the password. Alembic reads the URL through ConfigParser; `env.py` escapes it, so this means a URL bypassed `env.py`. |
| Container exits at once: `SYNCPAD_DATABASE_URL must be set explicitly when SYNCPAD_ENV=prod` | Working as intended. The setting's SQLite default would store documents in an ephemeral filesystem while reporting itself healthy, so prod refuses it. |
| `404` on `/` while every API route works | The static frontend was not found. Set `SYNCPAD_FRONTEND_DIR`, or check the startup log, which names the directory it chose or warns that it found none. |
| `/readyz` returns 503, logs say `database unreachable` | Wrong host, wrong username form (`postgres` instead of `postgres.<ref>`), or an unencoded password. |
| Two users editing the same document never see each other | More than one machine with no `SYNCPAD_REDIS_URL`. See above. |
| Everyone disconnects when a document goes idle | `auto_stop_machines` was enabled. Suspending a machine drops every socket it holds. |

---

## Rollback

```bash
fly releases                  # list
fly deploy --image <previous-image-ref>
```

Migrations are not rolled back automatically. The schema is additive, so an
older gateway runs against a newer schema; if you need the schema back, run
`alembic downgrade -1` yourself and check the revision first.
