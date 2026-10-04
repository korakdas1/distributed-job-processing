# Distributed Job Processing Platform

A distributed background-job platform built from first principles using FastAPI, PostgreSQL, Redis Streams, and custom workers. It provides durable submission, crash recovery, retries, scheduling, observability, containerized worker scaling, a read-only operator dashboard, and a hardened HTTPS deployment profile.

PostgreSQL is the durable source of truth. Redis is operational dispatch, scheduling, and heartbeat infrastructure. Execution is **at-least-once**, not exactly-once.

## Overview

Clients submit jobs over HTTP. FastAPI commits the job and a transactional outbox row in PostgreSQL, then returns `202`. An independent publisher process reads unpublished outbox rows and dispatches work to Redis. Independent worker processes claim deliveries, acquire durable PostgreSQL ownership, run an allowlisted handler, persist the result, and only then `XACK`.

A delayed-job scheduler promotes due work. Workers publish Redis TTL heartbeats. A GET-only dashboard observes the system. Optional VIEWER / OPERATOR API keys and a TLS-terminated nginx overlay provide a local secure profile.

## Features

**Durability.** PostgreSQL stores jobs, attempts, outbox events, worker history, and submission-idempotency records. `POST /jobs` commits the job and outbox row in one transaction, then returns `202`. Redis is not required for acceptance.

**Execution.** Independent worker processes claim Redis deliveries, acquire durable PostgreSQL ownership, run an allowlisted handler, persist the result, and only then `XACK`.

**Recovery.** Abandoned deliveries remain in the Redis pending-entry list (PEL). After the processing lease, another worker uses `XAUTOCLAIM` to reclaim them.

**Retry.** Handlers are classified retryable or non-retryable. Bounded `max_attempts`, exponential backoff with bounded jitter, then an operational dead-letter stream (`jobs:dead`).

**Scheduling.** Four priorities (`CRITICAL`, `HIGH`, `NORMAL`, `LOW`) with weighted fairness (default 8/4/2/1). Delayed first-run jobs and delayed retries share `jobs:delayed`.

**Control plane.** Optional `Idempotency-Key` on submit. Cooperative `DELETE /jobs/{id}` cancellation. Waiting jobs become `CANCELLED` immediately; `RUNNING` jobs stop at checkpoints.

**Workers.** Unique `worker-<hex>` IDs, PostgreSQL registry, Redis TTL heartbeats, derived liveness (`ACTIVE` / `EXPIRED` / `STOPPED` / `UNKNOWN`).

**Observability.** `GET /health`, `GET /ready`, Prometheus `GET /metrics`, JSON `GET /metrics/summary`, and a GET-only operator dashboard.

**Security.** Optional VIEWER / OPERATOR Bearer API keys, process-local rate limiting, TLS at nginx, and a separate secure Compose overlay. Development stays loopback HTTP with auth off.

**Infrastructure.** Docker Compose. Scale workers with `--scale worker=N`. One API, one publisher, and one scheduler in the default stack.

## Architecture

```text
Client / Browser
       |
       |  development: HTTP 127.0.0.1:3000 / 8000
       |  secure profile: HTTPS 127.0.0.1:8443
       v
Dashboard nginx (static UI + /api proxy)
       |
       |  Bearer on protected GETs when auth is on
       v
FastAPI
       |
       |  one PostgreSQL transaction: Job + Outbox
       v
PostgreSQL   <-----------------------------+
       |                                   |
       v                                   | persist result / attempt
Publisher                                  |
       |                                   |
       |  JOB_DISPATCH                     |
       v                                   |
Redis Streams (jobs:critical/high/normal/low)
       |                                   |
       v                                   |
Workers -----------------------------------+
       then XACK


Delayed path:

PostgreSQL / outbox
       |
       |  JOB_INITIAL_SCHEDULE / JOB_RETRY_SCHEDULE
       v
jobs:delayed (ZSET)
       |
       v
Scheduler  -->  QUEUED + JOB_DISPATCH  -->  priority stream


Observational only (not the job path):

Workers --> Redis TTL heartbeats --> GET /workers liveness overlay
Dashboard --> GET /health /ready /jobs /workers /metrics/summary
```

Publisher, scheduler, and workers are internal Compose services. In the secure profile they do not receive VIEWER/OPERATOR API keys. Only FastAPI validates Bearer credentials.

## Correctness Model

**PostgreSQL is durable truth.** Job status, results, attempts, cancellation, and unpublished outbox rows live there.

**Redis is operational.** It distributes ready work, holds delayed eligibility, and stores short-lived heartbeats. A Redis outage does not prevent `POST /jobs` from returning `202`.

**Submission.** `POST /jobs` validates the payload, optionally authenticates/authorizes and rate-limits, commits job + outbox (+ optional hashed idempotency row), and returns `202`. It does not execute the handler and does not call Redis.

**Publisher.** An independent process reads unpublished outbox rows and `XADD`s / `ZADD`s Redis.

**Delayed schedule identity.** `jobs:delayed` keeps job-ID members and timestamp scores.
The companion hash `jobs:delayed:generations` stores each scheduling outbox event's ID.
The scheduler reads member, score, and ID atomically; cleanup and rescore mutate only
that exact observation. Equal scores from different retries therefore remain distinct.
Duplicate publication of the same event retains its identity. The publisher locks the
job row and checks its current status, attempt number, and due time before writing;
obsolete scheduling events are consumed without overwriting Redis. Promotion still
commits the job transition and dispatch outbox before cleanup, and creates no attempt.

Existing members without generation metadata remain readable and are removed or
rescored only while their observed score and missing metadata still match. No database
migration or ZSET conversion is required. When upgrading, stop the old publisher and
scheduler before starting the updated versions; mixed old/new writers are not safe.
The companion hash must stay with its ZSET (no independent expiry or manual edits).
Its entry is removed with the matching schedule. This does not reconstruct lost Redis data.

**Worker.** Claim a Redis delivery → inspect PostgreSQL → acquire ownership (`QUEUED` → `RUNNING`) → execute an allowlisted handler → persist attempt/result → `XACK`. Persist-before-XACK is the crash-safety rule.

Workers automatically recreate missing ready-stream consumer groups while running,
starting at ID `0` so retained entries remain consumable. Concurrent repair is safe.
This repairs group existence only: deleted ready deliveries, missing delayed schedules,
and abandoned RUNNING attempts after delivery/PEL loss still require separate recovery.
Retained RUNNING deliveries continue to follow existing ownership and lease rules.

The transactional outbox closes the “Postgres committed, Redis never published” gap. It does **not** make publication or execution exactly-once.

## At-Least-Once Delivery

The platform provides **at-least-once** execution.

It does **not** provide exactly-once execution.

Crashes before `XACK`, duplicate outbox `XADD`, and lease reclaim can redeliver the same `job_id`. Correctness relies on durable PostgreSQL state, ownership checks, attempt tracking, and **idempotent handlers** wherever side effects matter.

`Idempotency-Key` protects duplicate **client submission**. It does not make handler execution exactly-once.

## Quick Start

Validated with **`docker-compose` 1.29.2**. The `docker compose` v2 plugin is not required.

Do **not** overwrite `.env` if it already exists. Do **not** rotate `POSTGRES_PASSWORD` against an existing development volume.

```bash
# only if .env does not exist
cp .env.example .env

python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-build-isolation --no-deps -e .

docker-compose up -d --build
# v2: docker compose up -d --build

curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ready
curl http://127.0.0.1:3000/
```

The API entrypoint runs `alembic upgrade head` before Uvicorn. A fresh Compose volume does not need a manual host migration.

| Surface | URL |
| --- | --- |
| Dashboard | http://127.0.0.1:3000 |
| API | http://127.0.0.1:8000 |
| OpenAPI docs | http://127.0.0.1:8000/docs (development only) |
| Health | http://127.0.0.1:8000/health |
| Ready | http://127.0.0.1:8000/ready |

Development remains **loopback HTTP**, authentication optional/off, docs on.

Default host ports: dashboard `127.0.0.1:3000`, API `127.0.0.1:8000`, PostgreSQL `127.0.0.1:5433`, Redis `127.0.0.1:6379`.

Scale workers:

```bash
docker-compose up -d --scale worker=4
curl http://127.0.0.1:8000/workers
```

### Do not casually destroy development data

```bash
docker-compose down          # stop containers; keep named volumes
```

**Do not casually run `docker-compose down -v` against the development stack if you care about development data.** `-v` deletes named volumes (PostgreSQL and Redis persistence).

### Host processes (optional)

Host pytest and a manual process layout talk to loopback Postgres/Redis. When the **full Compose stack** is already running, do not also start host uvicorn/publisher/scheduler/worker on the same ports.

```bash
# skip if .env already exists
cp .env.example .env

python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"

docker-compose up -d postgres redis
alembic upgrade head

uvicorn job_platform.api.app:app --host 127.0.0.1 --port 8000
python -m job_platform.outbox
python -m job_platform.scheduler
python -m job_platform.worker
```

Start as many workers as you want. Each process generates its own `worker-<hex>` identity.

## Secure Profile

The secure overlay is **production-oriented local hardening**, not a cloud deployment and not a compliance certification.

Local TLS is **self-signed**. `curl -k` is only for local acceptance. A real deployment needs a trusted CA certificate or trusted internal PKI. Do not treat the generated certificate as production trust.

1. Generate API secrets (prints paths, not values):

```bash
python scripts/generate_secrets.py
```

2. Generate a local self-signed TLS certificate:

```bash
sh scripts/generate_tls.sh
```

3. Set an explicit PostgreSQL password for this profile. Do **not** use this to rotate an existing development database password.

```bash
export POSTGRES_PASSWORD='...'   # choose a password; do not commit it
```

4. Start the overlay:

```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

5. Open the HTTPS dashboard at `https://127.0.0.1:8443`. The browser will warn about the self-signed certificate.

6. Use VIEWER for reads and OPERATOR for submit/cancel. Paste the key into the dashboard at runtime (stored in `sessionStorage` only).

Compose fails closed if `POSTGRES_PASSWORD` is unset. VIEWER/OPERATOR secret files are mounted **only into the API container**. Publisher, scheduler, and worker do not receive those credentials.

See [SECURITY.md](SECURITY.md) for reporting and the security model below.

## API Examples

Development (auth off):

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_type":"word_count","payload":{"text":"hello distributed world"},"priority":"HIGH"}'

curl -s http://127.0.0.1:8000/jobs/{id}
```

Expected initial status: `QUEUED`. The publisher later `XADD`s `jobs:high`. `GET /jobs/{id}` shows `SUCCEEDED` and `{"word_count": 3}` when finished.

Delayed first-run:

```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"job_type":"word_count","payload":{"text":"hello later"},"priority":"LOW","delay_seconds":5}'
```

Expected initial status: `SCHEDULED`. After `run_after`, the scheduler promotes it to `QUEUED`.

Secure profile (do not echo the keys):

```bash
VIEWER_KEY="$(cat secrets/viewer_api_key)"
OPERATOR_KEY="$(cat secrets/operator_api_key)"

curl -sk -H "Authorization: Bearer ${VIEWER_KEY}" \
  https://127.0.0.1:8443/api/jobs

curl -sk -X POST https://127.0.0.1:8443/api/jobs \
  -H "Authorization: Bearer ${OPERATOR_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"job_type":"word_count","payload":{"text":"hello"},"priority":"NORMAL"}'
```

VIEWER cannot `POST /jobs` (`403`). Public `GET /health` and `GET /ready` do not require a key.

Implemented HTTP routes:

```text
POST   /jobs
GET    /jobs
GET    /jobs/{id}
DELETE /jobs/{id}
GET    /workers
GET    /health
GET    /ready
GET    /metrics
GET    /metrics/summary
```

There is no `GET /workers/{worker_id}`.

## Dashboard

The operator UI has four pages: **Overview**, **Jobs**, **Workers**, **Queues**.

It is observational and **read-only**. It does not submit, cancel, retry, kill, scale, or redrive DLQ jobs.

In the secure profile the operator pastes an API key at runtime. The key lives in `sessionStorage`, not `localStorage`, not the URL, and not the frontend bundle. The client is GET-only. Public `/health` and `/ready` omit `Authorization`; protected GETs send Bearer.

Session charts are browser-local, bounded, and reset on reload. They are not Prometheus persistence.

Treat `null` as missing data, not zero. `UNKNOWN` is not `EXPIRED`. Stream `XLEN` is not the ready backlog. DLQ entries are not unique failed jobs. Stale last-good snapshots can remain on screen while a dependency is down.

## Security Model

| Class | Access |
| --- | --- |
| Public | `GET /health`, `GET /ready` |
| VIEWER | read jobs, workers, metrics |
| OPERATOR | VIEWER plus `POST /jobs` and `DELETE /jobs/{id}` |

Transport: `Authorization: Bearer <api-key>`.

Development: auth optional/off, HTTP on loopback, docs on.

Secure profile: auth required, rate limiting required, docs disabled, TLS at nginx, explicit `POSTGRES_PASSWORD`, secret files mounted only on `api`.

This is not “fully secure,” not OWASP-certified, and not a multi-user identity system. See [SECURITY.md](SECURITY.md).

## Testing

GitHub Actions runs **Verify** on pull requests and pushes to `main`, with separate
backend quality/unit, ordinary integration, and frontend checks. The canonical
verification environment is **Linux, Python 3.12, and Node 24.21.0** (also recorded
in `.node-version`). The package still supports Python >=3.11; CI does not yet
verify every supported Python version.

Install the tested Python dependency/tool set in a fresh environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-build-isolation --no-deps -e .
python -m pip check
ruff check .
ruff format --check .
mypy
pytest tests/unit
```

`pyproject.toml` remains the package metadata and compatibility-range source of
truth. `requirements-dev.lock` records the exact dependencies, build tools, and
verification tools tested by CI; it does not restrict the package's public ranges.
Regenerate it intentionally from those ranges in a clean Python 3.12 environment,
then rerun verification and review every version change:

```bash
lock_env=$(mktemp -d)
python3.12 -m venv "$lock_env"
"$lock_env/bin/python" -m pip install --upgrade pip setuptools wheel
"$lock_env/bin/python" -m pip install -e ".[dev]"
"$lock_env/bin/python" -m pip freeze --all --exclude job-platform > requirements-dev.lock
```

This uses pip's existing environment export rather than another package manager.
The temporary regeneration environment can be removed after reviewing the lock.

For the dashboard, use the Node version in `.node-version` and the existing npm lock:

```bash
cd dashboard
npm ci
npm run lint
npm run typecheck
npm run test
npm run build
```

Frontend lint warnings retain the tool's existing non-fatal policy; nonzero exits
fail CI. The npm lock uses the public npm registry and pins dependency integrity.

For ordinary local integration tests, configure local PostgreSQL
(`job_platform_test`) and Redis DB 15, then run:

```bash
pytest tests/integration -rs
```

These tests clear the test stores between tests. CI supplies fresh PostgreSQL 16
and Redis 7 service containers with explicit CI-only credentials. Ordinary
`pytest` and `pytest tests/integration` automatically skip `disruptive` tests and
never stop or restart PostgreSQL/Redis services. Process tests may terminate their
own application subprocesses. Static Compose checks need `docker-compose`; CI
maps that command to its preinstalled `docker compose` CLI for configuration checks.

Outage and Redis group/stream-loss acceptance tests are opt-in `disruptive` tests.
Run them with Docker and `docker-compose` available:

```bash
python -m tests.run_disruptive
# Or select one outage test:
python -m tests.run_disruptive tests/integration/test_worker_registry.py::test_worker_starts_while_postgres_is_down
# Or the consumer-group loss acceptance tests:
python -m tests.run_disruptive tests/integration/test_consumer_group_recovery.py
```

The runner creates a unique `job-platform-disruptive-` project with only
PostgreSQL and Redis, random loopback ports, disposable credentials, and isolated
storage. It supplies `--run-disruptive` and a private infrastructure manifest.
The flag alone cannot authorize service control or Redis-state destruction. Helpers
check effective targets, Docker ownership labels, container identities, ports, mounts,
and network membership before acting on exact container IDs or disposable Redis data.
The development Compose project is never stopped.
Guarded cleanup runs even after test failure and verifies that no owned containers,
volumes, or networks remain. Diagnostics are saved outside the repository; ambiguous
ownership refuses cleanup and reports the failure for inspection. Run these tests
serially; concurrent outage tests must not share an infrastructure instance.

Do **not** run `docker-compose down -v` against the development project.

A small dashboard demo workload (public API only; does not start Docker):

```bash
python scripts/demo_dashboard_workload.py
```

## Performance

Numbers are from a **local Docker Compose benchmark**. They are not production capacity and not an SLA.

On that documented baseline:

- Best measured submission throughput: **~320.8 jobs/s** (concurrency 10).
- Lightweight `word_count` completion flattened as workers increased (infrastructure overhead; handler ~1 ms).
- Sleep 0.5 s workload showed worker-slot scaling:
  - 1 worker ~**1.9** jobs/s
  - 2 workers ~**3.4** jobs/s
  - 4 workers ~**6.3** jobs/s
  - 4-worker speedup ~**3.32x**
- CPU `prime_calculation` (limit=100000) was too cheap/noisy (~9 ms handler) to support a strong CPU-scaling claim.

These are local Docker Compose measurements, not production capacity.

Do not read these as “the system handles 320 jobs/s in production” or “four workers scale linearly for all workloads.”

Harness and canonical result summaries: [`benchmarks/load/`](benchmarks/load/).

```bash
python benchmarks/load/run_benchmarks.py --quick
python benchmarks/load/run_benchmarks.py --profile all
```

`--quick` is a harness smoke. It is not the canonical baseline.

## Failure / Recovery Behavior

Isolated Compose and host tests cover:

- worker crash recovery via lease + `XAUTOCLAIM`
- 3-of-4 worker loss without lost jobs
- Redis outage: jobs still accepted; dispatch resumes
- PostgreSQL outage: `/ready` 503; jobs not accepted
- publisher outage: unpublished outbox backlog, then drain
- scheduler outage: delayed work waits, then promotes
- idempotency races
- cancellation races
- low-priority progress under higher-priority load

Duplicate Redis deliveries remain expected under at-least-once semantics.

## Known Limitations

- At-least-once execution, not exactly-once. External side effects need idempotent handler design.
- Static shared VIEWER/OPERATOR API keys. No automatic rotation, expiry, or per-user identities.
- No multi-tenancy, billing, quotas, or per-user job ownership.
- Rate limiter is process-local (one API process in Compose).
- One API, one publisher, and one scheduler in Compose. No Kubernetes, Helm, or cloud deployment.
- No centralized secrets manager.
- Local secure-profile TLS is self-signed, not publicly trusted.
- Official nginx image: master process remains root; `cap_drop: ALL` is omitted for that image.
- `no-new-privileges` is omitted because it is incompatible with this host (Linux 7.0 + Docker 29: `execve` EPERM).
- No Prometheus server, Grafana, Alertmanager, or distributed tracing.
- Dashboard history is browser-session only and resets on reload.
- No automatic DLQ redrive or retry-from-UI control plane.
- Benchmark numbers are a local Compose baseline only.

Implemented features (dashboard, auth, TLS, rate limiting, outbox, cancellation, idempotent submission) are **not** listed as missing.

## Technology stack

| Layer | Choice |
| --- | --- |
| Language | Python 3.11+ (tested with Python 3.12) |
| API | FastAPI + Pydantic |
| Durable state | PostgreSQL 16 |
| ORM / migrations | SQLAlchemy 2.x + Alembic |
| Queue | Redis 7 Streams + consumer groups (no Celery) |
| Workers | Custom process using redis-py |
| Dashboard | React + TypeScript + nginx |
| Local infrastructure | Docker Compose |
| Tests | pytest, HTTPX, Vitest |

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
