# replicron

[![CI](https://github.com/Cikouyanqu/replicron/actions/workflows/ci.yml/badge.svg)](https://github.com/Cikouyanqu/replicron/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**基于 cron 的跨 SQL 数据库幂等表复制工具。** / **Cron-driven, idempotent table replication between SQL databases.**

[中文文档](#中文文档) ｜ [English](#english)

---

## 中文文档

replicron 按计划把源查询的结果行写入目标表，且天然支持安全重跑：所有写入路径都是幂等 upsert，因此重跑、重试或中途崩溃都不会产生重复数据。抽取可选全量或水位增量；单进程即可运行，多实例也能通过共享租约表互斥。当你只需要"每天把 X 表同步到 Y 库"时，它是运行一整套 ELT 平台之外的轻量单二进制选择。

支持 **PostgreSQL、MySQL、SQLite、SQL Server**，任意组合互为源/目标（SQLite 是零依赖测试场景的好选择）。

### 特性

- **声明式任务**：一个 YAML 描述"复制什么、到哪里、多久一次"。
- **幂等 upsert**：按方言生成 `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE`，重跑永远安全。
- **增量同步**：可选水位列 + `:watermark` 占位符；只在零失败运行后推进水位，失败自动重读同一窗口。
- **cron 调度**：6 位表达式（含秒）；也支持手动一次性执行。
- **弹性部署**：单进程零依赖；多实例共享 SQLite 租约表互斥（`--locking db`，TTL 过期自动让位、按进度续租）。
- **优雅降级**：批量写失败自动降级逐行重试，单行坏数据不拖垮整轮；失败样本留存到运行日志。
- **原生批量通道（实验性）**：PostgreSQL `COPY`、SQL Server bulk 协议，`target.bulk` 开启，出错自动回退批量语句。
- **追加式运行日志**：每次运行落 SQLite（含进度事件）；崩溃遗留的 running 记录重启时清扫为 interrupted。
- **可观测性**：Prometheus `/metrics`、`/healthz`，以及 `--ui` 开启的只读运行面板（服务端渲染、零 JS、5 秒自刷新，展示运行列表/详情/事件与失败样本）。
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
      query: "SELECT id, total, updated_at FROM orders WHERE updated_at > :watermark"

    target:
      type: mysql
      dsn_env: DST_DSN
      table: reporting.orders    # 可带 schema 前缀
      mode: upsert               # upsert（默认）| insert
      keys: [id]                 # upsert 必填；冲突判定键
      batch_size: 500            # 多行语句每批行数，默认 500

    incremental:                 # 可选：水位增量抽取（见「增量同步」一节）
      column: updated_at         # 引擎取本轮结果集中该列的最大值作为新水位
      initial: "'1970-01-01'"    # 首次运行（无水位时）替换进查询的 SQL 字面量
```

说明：

- 源查询的列名必须与目标列**完全一致**（区分大小写）；映射用 `SELECT a AS b`。目标表需预先存在——replicron 只搬数据，不搬结构。
- `mode: insert` 走普通多行插入（空表初次装载更快；键冲突会报错）。
- MySQL 的冲突判定跟随目标表现有的 UNIQUE 索引；`keys` 仅决定哪些列不参与更新。
- SQL Server 批量自动限制在 2100 参数上限之内。
- `bulk: true`（实验性）启用目标库**原生批量通道**：PostgreSQL 走 `COPY`（upsert 经会话临时表 + `ON CONFLICT` 合并），SQL Server 走 bulk 协议（`##` 全局临时表 + `MERGE`）。任何 bulk 错误会自动回退到常规批量语句并告警。MySQL 的 `LOAD DATA` 暂缓：go-sql-driver 的本地文件 handler 只能注册不能反注册，逐批注册会泄漏。

### CLI

| 命令 | 用途 |
|---|---|
| `replicron run -c FILE [-t TASK]` | 匹配的任务各跑一次后退出 |
| `replicron schedule -c FILE [--addr :9101] [--locking db] [--ui]` | 守护进程：cron 循环 + `/metrics` + `/healthz` + `/ui` 运行面板 |
| `replicron validate -c FILE` | 解析校验配置并打印摘要 |
| `replicron runs [--db FILE] [-t TASK] [-n 20]` | 查看最近的运行记录 |

**多实例**：默认互斥在进程内。多个 replicron 实例共享同一个 `--db` 文件时，加 `--locking db` 启用数据库租约表互斥——租约带 TTL（`--lease-ttl`，默认 10m，崩溃实例到期自动让位），长任务按分页进度续租；取值应大于最慢单页耗时。

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

### 增量同步

配置 `incremental` 后，抽取从"每轮全量"变为"水位窗口"：

1. 源查询中写 `:watermark` 占位符（必须存在，否则任务校验失败）。
2. 每轮开始时，占位符被替换为**上一次干净运行**记录的水位；从未成功过则替换为 `initial` 字面量。
3. 流式过程中引擎跟踪 `column` 列的最大值（按值的真实类型比较：时间/整数/浮点/字符串）。
4. 仅当整轮**零行失败**时推进水位；有任何失败行则保持旧水位，下一轮重读同一窗口——配合幂等 upsert，重读无害、丢行不可接受。水位持久化失败同样只记日志不失败任务，下一轮照旧重读。

注意事项：

- `initial` 是**原样替换进 SQL 的字面量**：字符串要自带引号（`"'1970-01-01'"`），数字写 `0`。
- 时间型水位统一按 UTC 归一后以 `'YYYY-MM-DD HH:MM:SS.ffffff'` 渲染；四种源方言均可解析。
- MySQL 源建议 DSN 加 `parseTime=true`，让时间列成为真时间值；否则水位按字符串比较（标准格式下字典序安全）。
- 水位列的类型必须稳定（同列不同行出现字符串与数字混型会判失败）；NULL 值跳过。

### 已知限制

- 无转换——列按名 1:1 复制（映射用 `SELECT a AS b`）。
- 不支持 BLOB/二进制列（文本 `[]byte` 按字符串复制）。
- 增量依赖源查询可重放（幂等 upsert 兜底重复）；没有 CDC/日志追踪能力。
- 多实例互斥要求各实例共享同一个 `--db` 文件（如 NFS）：跨网络盘的 SQLite 有性能与一致性代价，实例同机或同卷最稳。
- 原生 bulk 通道为实验性：PostgreSQL 与 SQL Server 可用，MySQL 暂缓（驱动限制，见配置说明）。
- Web UI 只读且无内置鉴权：对外暴露端口请置于反代或内网之后。
- 异构结果集（行间列不一致）会被拒绝。

### Roadmap

- [x] 增量同步（水位列 + 状态跟踪）
- [x] 数据库租约表实现多实例互斥
- [ ] 失败运行的 webhook 通知（Slack / 钉钉 / 飞书）
- [x] 原生批量通道（PostgreSQL `COPY` + SQL Server bulk 协议；实验性，出错自动回退。MySQL `LOAD DATA` 暂缓，见配置说明）
- [x] 基于同一运行日志的最小 Web UI（`--ui`，只读、零 JS）

### 开发

```bash
make test      # go test -race ./...
make lint      # golangci-lint
make build     # 静态二进制
```

测试套件完全离线：连接器用 SQL 文本断言、引擎用内存 fake、运行日志用临时文件 SQLite。原生 bulk 路径另带集成测试（默认跳过）：`docker compose -f examples/docker-compose.yml up -d` 后执行 `go test -tags integration ./internal/connector/ -run Bulk -v`（SQL Server 需自备实例并设置 `REPLICRON_IT_MSSQL_DSN`）。

### 许可证

[MIT](LICENSE)

---

## English

replicron moves rows from a source query into a target table on a schedule,
safely re-runnable by design: every write path is an idempotent upsert, so a
re-run, a retry or a crash mid-cycle can never duplicate data. Extraction is
full-refresh or watermark-incremental; a single process is enough, and
multiple instances can coordinate through a shared lease table. It is a small,
single-binary alternative to running a full ELT platform when all you need is
"copy table X into database Y every night".

Supported engines: **PostgreSQL, MySQL, SQLite, SQL Server** — any of them can
be source or target (SQLite is a great zero-dependency choice for testing).

### Features

- **Declarative tasks** — one YAML file describes what to copy, where, how often.
- **Idempotent upserts** — `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE` per
  dialect; re-running a task is always safe.
- **Incremental sync** — optional watermark column plus a `:watermark` token
  in the query; the watermark advances only after zero-failure runs, and
  failures re-read the same window.
- **Cron scheduling** — 6-field expressions (with seconds) via a built-in
  scheduler; manual one-shot runs also supported.
- **Flexible deployment** — single process with zero dependencies, or several
  instances coordinating through a shared SQLite lease table
  (`--locking db`, TTL expiry with per-page renewal).
- **Graceful degradation** — batch writes that fail fall back to row-by-row so
  a single bad row never sinks a run; failures are sampled into the run log.
- **Native bulk channel (experimental)** — PostgreSQL `COPY` and the SQL
  Server bulk protocol behind `target.bulk`, with automatic fallback to batch
  statements on error.
- **Append-only run log** — every run is persisted to SQLite with progress
  events; runs orphaned by a crash are swept to `interrupted` on restart.
- **Metrics & UI** — Prometheus `/metrics`, `/healthz`, and an opt-in
  read-only run dashboard at `/ui` (`--ui`): server-rendered, zero-JS,
  auto-refreshing run list with per-run detail, events and failure samples.
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
- `bulk: true` (experimental) enables the target's **native bulk channel**:
  PostgreSQL uses `COPY` (upserts merge through a session temp table with
  `ON CONFLICT`), SQL Server uses the bulk protocol (a `##` global temp table
  plus `MERGE`). Any bulk error falls back to regular batch writes with a
  warning. MySQL `LOAD DATA` is deferred: go-sql-driver's local-file handlers
  register without an unregister API, so per-batch registration would leak.

### CLI

| Command | Purpose |
|---|---|
| `replicron run -c FILE [-t TASK]` | Run matching tasks once, then exit |
| `replicron schedule -c FILE [--addr :9101] [--locking db] [--ui]` | Daemon: cron loop + `/metrics` + `/healthz` + `/ui` run dashboard |
| `replicron validate -c FILE` | Parse and validate the config, print a summary |
| `replicron runs [--db FILE] [-t TASK] [-n 20]` | List recent run records |

**Multi-instance**: by default the task mutex is in-process. When several
replicron instances share the same `--db` file, pass `--locking db` to switch
to a database lease table — leases carry a TTL (`--lease-ttl`, default 10m,
so crashed instances yield automatically), long runs renew per page, and the
TTL should exceed your slowest page.

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

### Incremental sync

With `incremental` configured, extraction switches from full refresh to a
watermark window:

1. The source query references the `:watermark` token (required; the task
   fails validation otherwise).
2. At the start of each run the token is replaced with the watermark recorded
   by the **last clean run**, or with the `initial` literal when none exists.
3. While streaming, the engine tracks the maximum of `column` (compared by
   the value's real type: time / integer / float / string).
4. The watermark advances only after a run with **zero failed rows**; any
   failure keeps the old watermark and the next run re-reads the same window —
   with idempotent upserts, re-reading is harmless while losing rows is not.
   A failed watermark persist is logged, not fatal, for the same reason.

Notes:

- `initial` is a SQL literal substituted verbatim: strings need embedded
  quotes (`"'1970-01-01'"`), numbers are plain (`0`).
- Time watermarks are normalized to UTC and rendered as
  `'YYYY-MM-DD HH:MM:SS.ffffff'`, which all four source dialects parse.
- For MySQL sources, prefer a DSN with `parseTime=true` so datetime columns
  become real time values; otherwise the watermark compares as strings
  (lexicographically safe for standard formats).
- The watermark column type must be stable (mixed string/number values in one
  column fail the run); NULL values are skipped.

### Limitations

- No transforms — columns are copied 1:1 by name (map with `SELECT a AS b`).
- BLOB/binary columns are not supported (text `[]byte` values are copied as
  strings).
- Incremental mode relies on replayable source queries (idempotent upserts
  absorb duplicates); there is no CDC/log-tailing capability.
- Multi-instance locking requires every instance to share the same `--db`
  file (e.g. NFS): SQLite over a network share pays performance and
  consistency costs — keep instances on the same host or volume.
- The native bulk channel is experimental: available for PostgreSQL and SQL
  Server, deferred for MySQL (driver limitation, see the configuration notes).
- The web UI is read-only and unauthenticated by design: keep the port behind
  a reverse proxy or on an internal network.
- Heterogeneous result sets (rows with differing columns) are rejected.

### Roadmap

- [x] Incremental sync (watermark column + state tracking)
- [x] Multi-instance locking via a database lease table
- [ ] Webhook notifications (Slack/DingTalk/Feishu) on failed runs
- [x] Native bulk paths (PostgreSQL `COPY` + SQL Server bulk protocol; experimental with automatic fallback. MySQL `LOAD DATA` deferred, see the configuration notes)
- [x] Optional minimal web UI over the same run log (`--ui`, read-only, zero JS)

### Development

```bash
make test      # go test -race ./...
make lint      # golangci-lint
make build     # static binary
```

The test suite is fully offline: connectors are tested via SQL-text
assertions, the engine via in-memory fakes, the run log via temp-file SQLite.
The native bulk paths additionally have integration tests (skipped by
default): start `docker compose -f examples/docker-compose.yml up -d` and run
`go test -tags integration ./internal/connector/ -run Bulk -v` (SQL Server
needs a local instance via `REPLICRON_IT_MSSQL_DSN`).

### License

[MIT](LICENSE)
