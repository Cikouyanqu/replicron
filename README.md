# replicron

[![CI](https://github.com/Cikouyanqu/replicron/actions/workflows/ci.yml/badge.svg)](https://github.com/Cikouyanqu/replicron/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**基于 cron 的跨 SQL 数据库幂等表复制工具。** / **Cron-driven, idempotent table replication between SQL databases.**

[中文文档](#中文文档) ｜ [English](#english)

---

## 中文文档

replicron 按计划把源查询的结果行写入目标表，且天然支持安全重跑：所有写入路径都是幂等 upsert，因此重跑、重试或中途崩溃都不会产生重复数据。当你只需要"每天把 X 表同步到 Y 库"时，它是运行一整套 ELT 平台之外的轻量单二进制选择。

支持 **PostgreSQL、MySQL、SQLite、SQL Server**，任意组合互为源/目标（SQLite 是零依赖测试场景的好选择）。

### 特性

- **声明式任务**：一个 YAML 描述"复制什么、到哪里、多久一次"。
- **幂等 upsert**：按方言生成 `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE`，重跑永远安全。
- **cron 调度**：6 位表达式（含秒）；也支持手动一次性执行。
- **优雅降级**：批量写失败自动降级逐行重试，单行坏数据不拖垮整轮；失败样本留存到运行日志。
- **追加式运行日志**：每次运行落 SQLite（含进度事件）；崩溃遗留的 running 记录重启时清扫为 interrupted。
- **可观测性**：Prometheus `/metrics` 与 `/healthz`。
- **密钥不落配置**：DSN 通过环境变量（`dsn_env`）解析，凭据不进 YAML、不进 git。
- **纯 Go 单静态二进制**：无 CGO，可交叉编译；提供 distroless 镜像。

### 快速开始

```bash
git clone https://github.com/Cikouyanqu/replicron && cd replicron
make build

cat > replicron.yaml <<'YAML'
tasks:
  - name: users-sync
    schedule: "0 0 4 * * *"        # 每天 04:00；留空 = 仅手动
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

./replicron validate -c replicron.yaml   # 校验配置
./replicron run -c replicron.yaml        # 立即执行一次
./replicron runs -n 5                    # 查看最近运行记录
./replicron schedule -c replicron.yaml   # 守护进程：cron + /metrics + /healthz
```

Docker 方式：

```bash
docker build -t replicron .
docker run --rm \
  -v "$PWD/replicron.yaml:/replicron.yaml:ro" \
  -e TARGET_DSN="postgres://..." \
  replicron run -c /replicron.yaml
```

三容器演示（PostgreSQL + MySQL + replicron）见 [examples/docker-compose.yml](examples/docker-compose.yml)。

### 配置参考

```yaml
tasks:
  - name: orders-sync            # 必填，唯一
    schedule: "0 */30 * * * *"   # 6 位 cron（秒 分 时 日 月 周）；可选
    timeout: 30m                 # Go 时长格式；默认 30m，每轮硬截止
    enabled: true                # 默认 true
    page_size: 5000              # 源读取页大小，默认 5000

    source:
      type: postgres             # postgres | mysql | sqlite | sqlserver
      dsn_env: SRC_DSN           # dsn_env（推荐）或内联 dsn 二选一
      query: "SELECT id, total, updated_at FROM orders WHERE updated_at > '2026-01-01'"

    target:
      type: mysql
      dsn_env: DST_DSN
      table: reporting.orders    # 可带 schema 前缀
      mode: upsert               # upsert（默认）| insert
      keys: [id]                 # upsert 必填；冲突判定键
      batch_size: 500            # 多行语句每批行数，默认 500
```

说明：

- 源查询的列名必须与目标列**完全一致**（区分大小写）；映射用 `SELECT a AS b`。目标表需预先存在——replicron 只搬数据，不搬结构。
- `mode: insert` 走普通多行插入（空表初次装载更快；键冲突会报错）。
- MySQL 的冲突判定跟随目标表现有的 UNIQUE 索引；`keys` 仅决定哪些列不参与更新。
- SQL Server 批量自动限制在 2100 参数上限之内。

### CLI

| 命令 | 用途 |
|---|---|
| `replicron run -c FILE [-t TASK]` | 匹配的任务各跑一次后退出 |
| `replicron schedule -c FILE [--addr :9101]` | 守护进程：cron 循环 + `/metrics` + `/healthz` |
| `replicron validate -c FILE` | 解析校验配置并打印摘要 |
| `replicron runs [--db FILE] [-t TASK] [-n 20]` | 查看最近的运行记录 |

### 指标

`schedule` 模式下暴露在 `/metrics`：

| 指标 | 标签 | 含义 |
|---|---|---|
| `replicron_runs_total` | `task`, `status` | 按结果统计的完成运行数 |
| `replicron_rows_total` | `task`, `outcome` | 写入行数（`ok`/`fail`） |
| `replicron_run_duration_seconds` | `task` | 运行耗时 |

### 设计原则

- **幂等即正确性模型**：没有跨库事务；保证的是任意一轮可重放且不产生重复行。
- **一任务一租约**：同任务并发触发跳过（`status=skipped`），且租约只能由持有令牌的运行释放——失败的尝试绝不会误开别人的锁。
- **运行日志只追加**：状态行只终结一次，明细进 `run_events`；启动时把崩溃遗留的 running 清扫为 interrupted，日志永远不撒谎。
- **凭据来自环境变量**：任务文件里只有 `dsn_env` 引用，没有秘密。
- **标识符走白名单**：表/列/键名必须匹配 `[A-Za-z_][A-Za-z0-9_]*` 并做方言转义；所有值都是绑定参数。源列名无法把 SQL 夹带进目标库。
- **每轮独立连接 + 截止时间**：每轮在新 `context` 截止时间下开新连接；不挂死、不跨轮泄漏。

### 已知限制（v0.1）

- 仅全量抽取：无水位/增量模式（见 Roadmap）。
- 无转换——列按名 1:1 复制。
- 不支持 BLOB/二进制列（文本 `[]byte` 按字符串复制）。
- 单实例：调度互斥在进程内（Roadmap：数据库租约支持多实例 HA）。
- 异构结果集（行间列不一致）会被拒绝。

### Roadmap

- [ ] 增量同步（水位列 + 状态跟踪）
- [ ] 数据库租约表实现多实例互斥
- [ ] 失败运行的 webhook 通知（Slack / 钉钉 / 飞书）
- [ ] 原生批量通道（`COPY`、`SqlBulkCopy`、`LOAD DATA`）应对大流量
- [ ] 基于同一运行日志的最小 Web UI

### 开发

```bash
make test      # go test -race ./...
make lint      # golangci-lint
make build     # 静态二进制
```

测试套件完全离线：连接器用 SQL 文本断言、引擎用内存 fake、运行日志用临时文件 SQLite。

### 许可证

[MIT](LICENSE)

---

## English

replicron moves rows from a source query into a target table on a schedule,
safely re-runnable by design: every write path is an idempotent upsert, so a
re-run, a retry or a crash mid-cycle can never duplicate data. It is a small,
single-binary alternative to running a full ELT platform when all you need is
"copy table X into database Y every night".

Supported engines: **PostgreSQL, MySQL, SQLite, SQL Server** — any of them can
be source or target (SQLite is a great zero-dependency choice for testing).

### Features

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

### Quickstart

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

### Configuration reference

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

### CLI

| Command | Purpose |
|---|---|
| `replicron run -c FILE [-t TASK]` | Run matching tasks once, then exit |
| `replicron schedule -c FILE [--addr :9101]` | Daemon: cron loop + `/metrics` + `/healthz` |
| `replicron validate -c FILE` | Parse and validate the config, print a summary |
| `replicron runs [--db FILE] [-t TASK] [-n 20]` | List recent run records |

### Metrics

Exposed on `/metrics` in `schedule` mode:

| Metric | Labels | Meaning |
|---|---|---|
| `replicron_runs_total` | `task`, `status` | completed runs by outcome |
| `replicron_rows_total` | `task`, `outcome` | rows written (`ok`/`fail`) |
| `replicron_run_duration_seconds` | `task` | run wall time |

### Design principles

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

### Limitations (v0.1)

- Full-refresh extraction only: no watermark/incremental mode (roadmap).
- No transforms — columns are copied 1:1 by name.
- BLOB/binary columns are not supported (text `[]byte` values are copied as
  strings).
- Single instance: the scheduler mutex is in-process (roadmap: DB lease for
  multi-instance HA).
- Heterogeneous result sets (rows with differing columns) are rejected.

### Roadmap

- [ ] Incremental sync (watermark column + state tracking)
- [ ] Multi-instance locking via a database lease table
- [ ] Webhook notifications (Slack/DingTalk/Feishu) on failed runs
- [ ] Native bulk paths (`COPY`, `SqlBulkCopy`, `LOAD DATA`) for large volumes
- [ ] Optional minimal web UI over the same run log

### Development

```bash
make test      # go test -race ./...
make lint      # golangci-lint
make build     # static binary
```

The test suite is fully offline: connectors are tested via SQL-text
assertions, the engine via in-memory fakes, the run log via temp-file SQLite.

### License

[MIT](LICENSE)
