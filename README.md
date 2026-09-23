# replicron

**Cron-driven, idempotent table replication between SQL databases.**

[![CI](https://github.com/Cikouyanqu/replicron/actions/workflows/ci.yml/badge.svg)](https://github.com/Cikouyanqu/replicron/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

replicron moves rows from a source query into a target table on a schedule,
safely re-runnable by design: every write path is an idempotent upsert, so a
re-run, a retry or a crash mid-cycle can never duplicate data. It is a small,
single-binary alternative to running a full ELT platform when all you need is
"copy table X into database Y every night".

Supported engines: **PostgreSQL, MySQL, SQLite, SQL Server** — any of them can
be source or target (SQLite is a great zero-dependency choice for testing).

## Features

- **Declarative tasks** — one YAML file describes what to copy, where, how often.
- **Idempotent upserts** — `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE` per
  dialect; re-running a task is always safe.
- **Cron scheduling** — 6-field expressions (with seconds) via a built-in
  scheduler; manual one-shot runs also supported.
- **Graceful degradation** — batch writes that fail fall back to row-by-row so
  a single bad row never sinks a run; failures are sampled into the run log.
- **Append-only run log** — every run is persisted to SQLite with progress
  events; runs orphaned by a crash are swept to `interrupted` on restart.
- **Metrics** — Prometheus `/metrics` and `/healthz` endpoints.
- **Secrets stay out of config** — DSNs resolve from environment variables
  (`dsn_env`), so credentials never live in YAML or git.
- **Single static binary** — pure Go (no CGO), cross-compiles anywhere;
  distroless Docker image available.

## Quickstart

```bash
git clone https://github.com/Cikouyanqu/replicron && cd replicron
make build

cat > replicron.yaml <<'YAML'
tasks:
  - name: users-sync
    schedule: "0 0 4 * * *"        # daily at 04:00; empty = manual only
    source:
      type: sqlite
      dsn: "file:source.db"
      query: "SELECT id, name FROM users"
    target:
      type: postgres
      dsn_env: TARGET_DSN
      table: public.users
      mode: upsert
      keys: [id]
YAML

export TARGET_DSN="postgres://user:pass@localhost:5432/app"

./replicron validate -c replicron.yaml   # check the config
./replicron run -c replicron.yaml        # one-shot run now
./replicron runs -n 5                    # inspect recent runs
./replicron schedule -c replicron.yaml   # daemon: cron + /metrics + /healthz
```

Or with Docker:

```bash
docker build -t replicron .
docker run --rm \
  -v "$PWD/replicron.yaml:/replicron.yaml:ro" \
  -e TARGET_DSN="postgres://..." \
  replicron run -c /replicron.yaml
```

A three-container demo (PostgreSQL + MySQL + replicron) is in
[examples/docker-compose.yml](examples/docker-compose.yml).

## Configuration reference

```yaml
tasks:
  - name: orders-sync            # required, unique
    schedule: "0 */30 * * * *"   # 6-field cron (sec min hour dom mon dow); optional
    timeout: 30m                 # Go duration; default 30m, hard deadline per run
    enabled: true                # default true
    page_size: 5000              # source fetch page size, default 5000

    source:
      type: postgres             # postgres | mysql | sqlite | sqlserver
      dsn_env: SRC_DSN           # either dsn_env (recommended) or inline dsn
      query: "SELECT id, total, updated_at FROM orders WHERE updated_at > '2026-01-01'"

    target:
      type: mysql
      dsn_env: DST_DSN
      table: reporting.orders    # optionally schema-qualified
      mode: upsert               # upsert (default) | insert
      keys: [id]                 # required for upsert; conflict target
      batch_size: 500            # rows per multi-row statement, default 500
```

Notes:

- Column names in the source query must match target columns exactly
  (case-sensitive); use `SELECT a AS b` for mapping. The target table must
  already exist — replicron copies data, not schema.
- `mode: insert` issues plain multi-row inserts (fast initial loads into
  empty tables; key conflicts will error).
- For MySQL, conflict detection follows the target table's UNIQUE indexes;
  `keys` only controls which columns are excluded from the update set.
- SQL Server batches are automatically capped to stay under its 2100-parameter
  limit.

## CLI

| Command | Purpose |
|---|---|
| `replicron run -c FILE [-t TASK]` | Run matching tasks once, then exit |
| `replicron schedule -c FILE [--addr :9101]` | Daemon: cron loop + `/metrics` + `/healthz` |
| `replicron validate -c FILE` | Parse and validate the config, print a summary |
| `replicron runs [--db FILE] [-t TASK] [-n 20]` | List recent run records |

## Metrics

Exposed on `/metrics` in `schedule` mode:

| Metric | Labels | Meaning |
|---|---|---|
| `replicron_runs_total` | `task`, `status` | completed runs by outcome |
| `replicron_rows_total` | `task`, `outcome` | rows written (`ok`/`fail`) |
| `replicron_run_duration_seconds` | `task` | run wall time |

## Design principles

- **Idempotency is the correctness model.** There are no cross-database
  transactions; the guarantee instead is that any run can be replayed without
  duplicating rows.
- **One lease per task.** Concurrent triggers of the same task are skipped
  (`status=skipped`), and leases are released only by their owning token —
  a failed attempt can never unlock somebody else's run.
- **The run log is append-only.** Status lives in a single row finalized once;
  detail goes to `run_events`. On startup, rows left `running` by a crash are
  marked `interrupted`, so the log never lies about activity.
- **Credentials are environment-sourced.** Task files contain `dsn_env`
  references, never secrets.
- **Identifiers are allowlisted.** Table/column/key names must match
  `[A-Za-z_][A-Za-z0-9_]*` and are dialect-quoted; all values are bound
  parameters. Source column names cannot smuggle SQL into the target.
- **Per-run connections with deadlines.** Every run opens fresh connections
  under a `context` deadline; nothing hangs forever and nothing leaks across
  runs.

## Limitations (v0.1)

- Full-refresh extraction only: no watermark/incremental mode (roadmap).
- No transforms — columns are copied 1:1 by name.
- BLOB/binary columns are not supported (text `[]byte` values are copied as
  strings).
- Single instance: the scheduler mutex is in-process (roadmap: DB lease for
  multi-instance HA).
- Heterogeneous result sets (rows with differing columns) are rejected.

## Roadmap

- [ ] Incremental sync (watermark column + state tracking)
- [ ] Multi-instance locking via a database lease table
- [ ] Webhook notifications (Slack/DingTalk/Feishu) on failed runs
- [ ] Native bulk paths (`COPY`, `SqlBulkCopy`, `LOAD DATA`) for large volumes
- [ ] Optional minimal web UI over the same run log

## Development

```bash
make test      # go test -race ./...
make lint      # golangci-lint
make build     # static binary
```

The test suite is fully offline: connectors are tested via SQL-text
assertions, the engine via in-memory fakes, the run log via temp-file SQLite.

## License

[MIT](LICENSE)
