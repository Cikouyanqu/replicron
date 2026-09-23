# replicron（中文说明）

**基于 cron 的跨 SQL 数据库幂等表复制工具。**

replicron 按计划把源查询的结果行写入目标表，且天然支持安全重跑：所有写入路径都是幂等 upsert，因此重跑、重试或中途崩溃都不会产生重复数据。当你只需要"每天把 X 表同步到 Y 库"时，它是运行一整套 ELT 平台之外的轻量单二进选择。

支持 **PostgreSQL、MySQL、SQLite、SQL Server**，任意组合互为源/目标。

## 核心特性

- **声明式任务**：一个 YAML 描述"复制什么、到哪里、多久一次"。
- **幂等 upsert**：按方言生成 `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE`，重跑永远安全。
- **cron 调度**：6 位表达式（含秒）；也支持手动一次性执行。
- **优雅降级**：批量写入失败自动降级为逐行重试，单行坏数据不会拖垮整轮；失败样本留存到运行日志。
- **追加式运行日志**：每次运行落 SQLite（含进度事件）；进程崩溃遗留的 running 记录在重启时清扫为 interrupted。
- **可观测性**：Prometheus `/metrics` 与 `/healthz`。
- **密钥不落配置**：DSN 通过环境变量（`dsn_env`）解析，凭据不进 YAML、不进 git。
- **纯 Go 单静态二进制**：无 CGO，可交叉编译；提供 distroless 镜像。

## 快速上手

```bash
make build

cat > replicron.yaml <<'YAML'
tasks:
  - name: users-sync
    schedule: "0 0 4 * * *"
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
./replicron schedule -c replicron.yaml   # 守护进程：定时 + /metrics + /healthz
```

完整配置项与设计说明见[英文 README](../README.md)。

## 设计要点

- **幂等即正确性模型**：没有跨库事务，保证的是"任意一轮可重放且不重复"。
- **一任务一租约**：同任务并发触发返回 skipped；租约只能由持有令牌的运行释放，失败尝试绝不会误开别人的锁。
- **运行日志只追加**：状态行只终结一次，明细进事件表；启动时把崩溃遗留的 running 清扫为 interrupted，日志永远不撒谎。
- **凭据来自环境变量**，标识符走白名单 + 方言转义，值全部参数化绑定。
- **每轮独立连接 + 截止时间**：不挂死、不跨轮泄漏。

## 已知限制（v0.1）

仅全量抽取（无增量水位）；无转换；不支持二进制列；单实例（进程内互斥）；
异构结果集（行间列不一致）会被拒绝。

## 许可证

[MIT](../LICENSE)
