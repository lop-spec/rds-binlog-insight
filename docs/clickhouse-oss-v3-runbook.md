# ClickHouse OSS v3 全历史回填与切流

本流程只接受云端 CI 生成的 `1.26.9-rawoss` 镜像。整个迁移是加法变更：
旧查询链、旧镜像、v2 表和源 OSS 对象均保留；在所有硬门通过前，
`RDS_BINLOG_CLICKHOUSE_OSS_SERVING_ENABLED` 必须保持 `0`。

## 不变量

- v3 覆盖 `history_days=0` 的全部查询可见 database part，不设 7/30 天上限。
- 历史 standalone Parquet 使用单 worker 两阶段回填：OSS → time stage →
  name stage → 双表逐 part 校验 → `MOVE PARTITION` → manifest ready。
- 两阶段历史回填期间禁止创建增量 MV，否则 name 表会重复写入。
- 历史 direct 完成后才创建 MV；旧 pack、实时新增、替换和删除由常驻 ingester
  处理，而且每个 part 必须在 time/name 两表同时通过行数、SHA-256 和 revision
  校验后才能 ready。
- 通用 v3 object-serving 在 manifest 覆盖不全时仍可返回旧 Parquet/OSS 路径；
  raw-serving 已声明全量接管后，ClickHouse 异常必须返回明确的 HTTP 503，禁止在
  Web 进程启动无界 Parquet/OSS 回退，也不允许把“未覆盖”当作“无结果”。
- raw-serving 的持续采集门必须按请求时间窗判断；窗外 pending/unready 不得使历史
  查询闪回旧链，窗内 pending、未知范围删除或未就绪 pack 必须 fail closed。

## 0. 发布前冻结与证据

1. 确认主服务 healthy、ClickHouse healthy、数据盘可用空间不少于 120 GiB。
2. 确认旧 v2 回填容器未运行，v3 查询开关仍为 `0`。
3. 保存改前 Compose、源码、`metadata.sqlite3` 全量副本及镜像 ID。
4. 下载云端 CI artifact，先执行其 `.sha256` 校验，再 `docker load`；禁止在
   生产机或开发机本地构建正式镜像。

## 1. 显式创建 v3 final/stage 表，暂不创建 MV

`clickhouse-oss-ingester` 服务已经固定 v3 prefix、四张表、manifest、
`history_days=0` 和 query ingest mode。一次性迁移只覆盖阶段开关：

```bash
docker compose --profile clickhouse-oss run --rm --no-deps \
  -e RDS_BINLOG_CLICKHOUSE_OSS_STAGED_BACKFILL_ENABLED=1 \
  -e RDS_BINLOG_CLICKHOUSE_OSS_INCREMENTAL_MV_ENABLED=0 \
  clickhouse-oss-ingester python -m app.clickhouse_migrate \
  --data-dir /data --oss-object-tables
```

迁移只允许显式入口执行；主服务和索引器运行时不得执行 DDL。

## 2. 全量 direct 两阶段回填

复用同一个 service 和阶段开关运行：

```bash
docker compose --profile clickhouse-oss run --rm --no-deps \
  -e RDS_BINLOG_CLICKHOUSE_OSS_STAGED_BACKFILL_ENABLED=1 \
  -e RDS_BINLOG_CLICKHOUSE_OSS_INCREMENTAL_MV_ENABLED=0 \
  clickhouse-oss-ingester python -m tools.clickhouse_oss_backfill \
  --data-dir /data --batch-parts 64 \
  --batch-bytes 536870912 --batch-rows 1000000
```

`data/logs/clickhouse-oss-all-v3-backfill-status.json` 必须最终为 `complete`、
`failedParts=0`。崩溃后直接重跑同一命令，journal 会先完成或安全丢弃 staging；
禁止手工删除 final 表中的半批数据。

## 3. 创建增量 MV 并接管 pack/实时数据

```bash
docker compose --profile clickhouse-oss run --rm --no-deps \
  -e RDS_BINLOG_CLICKHOUSE_OSS_STAGED_BACKFILL_ENABLED=0 \
  -e RDS_BINLOG_CLICKHOUSE_OSS_INCREMENTAL_MV_ENABLED=1 \
  clickhouse-oss-ingester python -m app.clickhouse_migrate \
  --data-dir /data --oss-object-tables
```

随后启动单实例增量 ingester：

```bash
docker compose --profile clickhouse-oss up -d --no-deps clickhouse-oss-ingester
```

它会继续处理历史 pack、direct 回填水位之后的新 part、内容替换和删除。
`oss-all-v3-manifest.sqlite3` 的 pending、failed、delete 必须全部收敛为 0。

## 4. 全 OSS 双表硬验收

使用与第 3 步相同的 v3 环境运行：

```bash
docker compose --profile clickhouse-oss run --rm --no-deps \
  -e RDS_BINLOG_CLICKHOUSE_OSS_STAGED_BACKFILL_ENABLED=0 \
  -e RDS_BINLOG_CLICKHOUSE_OSS_INCREMENTAL_MV_ENABLED=1 \
  clickhouse-oss-ingester \
  python -m tools.clickhouse_oss_verify --data-dir /data
```

未显式传 `--end-us` 时，verifier 固定使用 manifest 最近一次完整 reconcile 的
`reconcile_end_epoch_us`，不会追逐持续移动的实时尾部；该水位之后的新数据由第 6 步
增量链路检查证明。需要复现某次验收时可显式传回同一个 `--end-us`。

退出码必须为 0，且输出中所有 `gate.checks` 均为 `true`。该验证覆盖：

- 源窗口没有未归档或因 1,000,001 上限被截断的 part；
- 源 inventory 在验证前后未变化；
- manifest 全覆盖且队列、failed、delete、lastError 均为空；
- 每个源 identity 在 time/name 两表的行数、SHA-256、revision 完全一致；
- 两张 final 表总行数等于源总行数，没有额外行；
- stage 为空、journal 不存在、四张物理表和增量 MV 均存在。

## 5. 结果与 ≥2× 性能门

在旧 API 仍提供生产流量、v3 serving 仍关闭时，对固定水位分别执行 1、7、30 天
场景。每个场景至少交替运行 5 次：

```bash
python -m tools.clickhouse_poc_benchmark \
  --current-api http://insight:8769 \
  --current-host-header 192.0.2.10 \
  --clickhouse-host rds-binlog-insight-clickhouse \
  --clickhouse-table insight.events_query_by_name_oss_all_v3 \
  --start-epoch-us <固定开始水位> --end-epoch-us <固定结束水位> \
  --instance <实例> --database <库> --table <表> \
  --repeats 5 --min-speedup 2
```

三个场景都必须 `ok=true`、`exact_match=true`、`p50_speedup>=2`。此外运行慢日志
专用全窗口 parity/performance verifier，并确认 catalog、采集、搜索索引和 analytics
队列均无 failed/lastError。

## 6. 切流与回滚

只有第 4、5 步全部通过后，才在 `.env` 设置：

```text
RDS_BINLOG_CLICKHOUSE_OSS_SERVING_ENABLED=1
```

仅重建主服务，连续请求 3 次 `/healthz`、`/api/storage` 和 1/7/30 天查询；验证
HTTP 200、结果哈希不变、`tiers_used` 命中 ClickHouse，且 restart、watchdog
restart、lastError、`database is locked` 均无异常。

回滚只需将该开关恢复为 `0` 并重建主服务；不要删除 v3 表、v2 表或 OSS 对象。
旧镜像仍能读取加法 schema，但会恢复旧查询性能风险。短窗口通过只证明发布即时状态，
不能宣称长期稳定；至少保留 24 小时 restart、锁、失败队列和 P95 延迟观测后再清理
隔离 bench 表，v2 数据继续保留到单独批准清理。
