# RDS Binlog Insight

面向阿里云 RDS MySQL / MariaDB 的 Binlog 下载、精简解析、OSS 分层保存与查询 GUI。RDS 侧全程只读。

## 已实现

- 使用阿里云 RDS `DescribeDBInstanceAttribute` 核验实例身份，并通过
  `DescribeDBInstanceHAConfig` 自动锁定当前 Master 节点，避免把一主多备
  的同一批 Binlog 重复解析。
- 使用 `DescribeBinlogFiles` 分页查询可下载日志，只处理 `Completed`。
- Binlog 文件只使用 RDS 返回的内网下载 URL；缺失时硬失败，绝不回退公网 URL。
- GUI 可指定本地起止时间；后端转换为 UTC，仅下载与该窗口相交的 Binlog。
- 按 `LogBeginTime → LogEndTime → LogFileName` 排序，网络下载最多预取 3 个文件。
- 支持断点续传、文件大小、SHA-256、RDS CRC64-ECMA/XZ 校验。
- 支持原始 Binlog、`.gz`、`.tar(.gz)`、`.zip`、`.zst`；压缩包条目按流读取。
- 下载、解析/Parquet 与 OSS 归档使用独立有界执行池；CPU 密集的 Binlog
  解析和 DuckDB/Parquet 转换固定为单文件、单进程、单线程，主服务容器与
  索引容器均硬限制 1 CPU。网络下载仍最多预取 3 个文件，OSS 使用 4 个
  I/O 线程隐藏等待。Go 解析器直接按 200,000 条或 384 MiB（先到者）落盘并
  原子发布 NDJSON 批，Python 只消费每批一条清单并用 ACK 控制有界背压；
  最多同时保留当前批和一个预取批（上限 768 MiB），不会填满 1 GiB 暂存区；
  DuckDB 批量导入后删除该批 NDJSON 和中间文件。
- 保存 INSERT / UPDATE / DELETE 行前后值、DDL/Query、GTID、事务、位置、服务端 ID 等。
- 新写入与最近 1 天的 Parquet 使用 ZSTD 1；确认同步已追平、没有待处理
  Binlog 且没有查询压力后，后台才单线程、一次一个分片转换为 ZSTD 9。
  转换逐项校验行数、时间范围、Schema 与 Row Group，上传成功后原子切换物理
  对象；发现新 Binlog 或查询压力时在当前分片边界让路。单个 Parquet 分片内
  先按库表、操作和 5 分钟时间桶聚簇，再切成最多 8,192 行的独立 Row Group。
- 新生成的 Parquet 校验后立即作为独立、内容寻址对象直传 OSS，不再为打包
  重读和重写本地正文；历史迁移仍兼容约 128 MiB 聚合对象及精确
  `offset + length` Range 读取。认证使用 ECS RAM 角色或受保护 AccessKey，
  并固定走同地域内网 Endpoint。
- Parquet 正文重写、索引读取和本地释放使用跨容器版本锁；释放前再次核对
  当前 SHA-256 与 OSS 归档状态。搜索索引绑定稳定 `logical_part_id`，物理
  `object_sha256` 独立保存，冷压缩无需重建索引；旧 OSS pack 仅在所有引用
  都迁移并经过查询安全宽限期后删除。
- 查询固定按“本地搜索索引 → OSS Range → RDS 范围核对”执行。SQLite
  FTS5 只保存 Row Group 级库表、完整词/标量值和 trigram 兼容关系，不保存
  事件正文；完整词/值优先走精确倒排，短词和标点查询走兼容慢路径。PyArrow
  只读取命中的 Row Group 与必要列。索引缺失时保持准确回退，后台从最新对象
  开始补齐。两层均无覆盖时才核对 RDS，实例也不存在时明确返回缺失的 UTC
  时间范围。
- GUI 每次提交都会创建独立、持久化的查询任务；默认最多并行运行 2 个任务，
  所有任务共享最多 4 个分区扫描槽。任务在首个 OSS 读取前发布候选分区数和
  最坏扫描字节估算，可独立停止、切换结果；停止信号会在扫描槽等待、分区读取
  前后和下一批提交前生效。服务重启不会保留虚假的“运行中”状态。
- 索引与结构目录在独立 1 CPU 容器中串行构建，每个工作进程连续处理最多
  64 个逻辑 Parquet，再回收原生库内存；
  Arrow/SQLite 即使卡死也不会占住 HTTP 或 Binlog 同步。10 分钟无进度由独立
  看门狗强制重启，并从 SQLite 已提交断点继续；新对象按最新时间优先补齐。
- 自动任务每 5 分钟重扫 60 天窗口；缺失 Binlog 按最早文件顺序转换、
  开放查询和提交，同时用下载预取隐藏网络等待。
- 本地长期只保留最小搜索索引和对象清单；新 Parquet 写入 OSS 聚合对象后可在
  1 GiB 有界交接区短暂等待本地索引，索引提交后立即删除，积压超限的旧正文
  回退 OSS Range。该交接区不是查询 LRU。
- 成功写入、逐分区回读计数、OSS 校验并按源文件顺序提交元数据后，才删除
  **本地临时 Binlog**；后续并行文件即使先完成也保持隐藏和可恢复。
- 当前顺序最早的 Binlog 每批 Parquet 原子发布后便可查询；Row Group 搜索
  索引继续在独立进程异步构建。
- 文件完成后同时清空 SQLite 中的临时 OSS 签名下载 URL。
- 精确 60 天查询边界；独立后台每小时执行一次物理清理，不依赖自动同步，
  同时支持手动清理、断点恢复、失败硬停止和 CSV 导出。
- Windows Credential Manager 或 Linux `0600` 受保护文件保存阿里云
  AccessKey；配置文件、SQLite 与日志不落明文密钥。RDS 与 OSS 可复用该
  凭据；OSS 也支持更推荐的 ECS RAM 角色自动轮换凭据。

> 本工具绝不会删除 RDS 上的远程 Binlog。OSS 生命周期只作用于配置的程序
> 对象前缀；应用只会删除该前缀内已被新编码替代、所有引用均已迁移并经过
> 安全宽限期的旧对象。

## 启动

依赖已安装，直接双击：

`启动 Binlog Insight.vbs`

入口会优先使用 Microsoft Edge，未安装时使用 Google Chrome；均以隔离
profile 的应用窗口运行，不复用日常浏览器会话。

首次配置需要：

1. RDS Region ID、实例 ID、API Endpoint。
2. 最小 RAM 权限：`rds:DescribeDBInstanceAttribute`、
   `rds:DescribeDBInstanceHAConfig` 与 `rds:DescribeBinlogFiles`。
3. AccessKey，或进程环境变量：
   `ALIBABA_CLOUD_ACCESS_KEY_ID`、`ALIBABA_CLOUD_ACCESS_KEY_SECRET`，
   临时凭据另加 `ALIBABA_CLOUD_SECURITY_TOKEN`。
4. OSS Bucket、Region、同地域内网 Endpoint、对象前缀，以及认证方式。
   ECS RAM 角色或 AccessKey 至少需要目标前缀的 `oss:ListObjects`、
   `oss:GetObject`、`oss:PutObject`、`oss:DeleteObject`，以及 Bucket 的
   `oss:GetBucketLifecycle`、`oss:PutBucketLifecycle`。

## Binlog 的客观边界

普通 Binlog 不包含连接用户名、客户端 IP，也不能保证保存原始 SQL 文本。行模式下会保存精确行值，但旧版本/非 FULL 元数据通常只有 `@1`、`@2` 这样的列序号。界面会区分：

- `原始 SQL`：QueryEvent 或 RowsQueryEvent 实际携带的文本。
- `重建 SQL`：根据行镜像生成，仅用于阅读，不用于回放。

## TB 级主键精确索引（v1.8.31）

当前任意子串索引在覆盖完整时可以把查询缩到 Row Group，但索引未覆盖的分区
仍必须回读 OSS 才能保证不漏结果；因此“选择一天”不等于读取量小，也不能仅靠
增加扫描并发实现稳定秒查。

v1.8.31 已把 `完整库名 + 完整表名 + 单列主键值` 从关键词搜索中拆成独立
ExactIndex 路径：

1. 解析器写入 `table_map_id` 与 `schema_version_id`；非 FULL Binlog 的显式映射
   必须绑定完整 schema 指纹。仓库内的 `app/exact_schema_registry.json` 是空模板；
   部署方把真实映射放在权限受控的数据卷，并用
   `RDS_BINLOG_EXACT_SCHEMA_REGISTRY` 指向它。任一列数、类型、顺序或元数据变化都会
   使旧映射失效并降级为 unknown。
2. L0 复用时间、库、表、操作的 Row Group 结构目录；L1 在不可变 SQLite sidecar
   中保存类型化 value hash、原始 canonical bytes 与文档定位。高基数主键采用
   B-tree posting，避免为几乎唯一的值制造稀疏 Roaring 容器。回填先用已完成的
   L0 目录跳过不含注册表的对象，再在 Arrow Row Group 内只物化注册表对应行；
   目录缺失或 SHA 过期时仍保守纳入，不能靠裁剪制造假阴性。每个分区只在
   自己的 sidecar 提交期间持有一个正文锁，避免多分区预持锁与同步链形成死锁；
   四个 I/O worker 各自独立建段，容器仍受 1 CPU、3 GiB 硬限制。
3. L2 直接保存列表所需的事件时间、操作、对象和
   `logical_part_id:row_group_id`。覆盖完整时，无命中和结果列表都不读取 OSS；
   只有打开详情才读取命中的 Parquet Row Group。
4. Sidecar 经完整性校验后原子发布，并同时绑定 `logical_part_id`、物理对象 SHA、
   Row Group 布局、schema version 与 registry SHA。registry 或逻辑身份变化会强制
   重建；覆盖缺失时界面显示准确扫描兜底，不能返回假阴性。

生产验收门槛仍是：完整覆盖的一天主键无命中查询 `GetObject=0`、OSS 读取
`0 B`；有命中列表同样 `0 B`，详情只访问候选对象；与 Parquet 暴力扫描对拍
召回率 100%。后台按新到旧生成 sidecar，尚未完成覆盖的时间范围不会宣称稳定秒查。

## 全量 SQL、事务与锁分析（v1.9.0）

界面新增「分析洞察」页，对选定时间窗内的**全部事件**做聚合，不限于关键词命中
的部分。三类结论共用一次请求：

| 维度 | 输出 | 依据 |
|---|---|---|
| 全量 SQL | 语句指纹 TopN、对象分布、操作分布、写入趋势 | 参数归一后的语句指纹 |
| 事务 | 事务数、行事件、大小与时长直方图、最长/最大事务、提交趋势 | 按 GTID / XID / 事务 ID 归并 |
| 锁争用（推断） | 行热点、表级写热点、长事务、大事务、DDL 与并发写窗口 | 事务大小、时长与同行改写频次 |

### 语句指纹的四种来源

Binlog 不保证保存语句文本，因此指纹来源必须随结果一起标注，不能互相冒充：

| 来源 | 界面标签 | 含义 |
|---|---|---|
| `original` | 原始 SQL | QueryEvent 携带的真实语句（DDL、非行模式语句） |
| `rows-query` | 原始 SQL | 开启 `binlog_rows_query_log_events` 后 RowsQueryEvent 携带的真实语句 |
| `reconstructed` | 重建 SQL | 行事件按行镜像生成，**仅供阅读**，不是真实语句文本，也不能用于回放 |
| `synthetic` | 合成模板 | 既无语句文本也无 RowsQuery 时按 `操作 + 库.表` 合成，只能回答「哪张表被怎么改了多少次」 |

事务边界（BEGIN / COMMIT / ROLLBACK / SAVEPOINT / XA）单独归为 `boundary`：它的
数量与事务数同量级，混进语句榜单会把业务 SQL 全部挤出，因此**从 SQL 维度排除并
单独计数**，由事务维度负责解读。注意 MySQL 把 `BEGIN` 写成普通 QueryEvent
（`sql_kind='ORIGINAL'`），所以判定按**语句动作**而不是只看 `sql_kind`。

归一化会去注释、把字面量参数化、折叠 `IN (?)` 与多组 `VALUES (?)`、统一比较
操作符两侧空格，因此同一条语句的不同参数与不同书写风格落在同一指纹。列名元数据
缺失时出现的 `@1`、`@2` 是**列序号不是字面量**，不会被参数化，否则不同列的更新
会被错误归并。

### 事务时长的精度边界

Binlog 事件头时间戳是**秒级**（解析器按 `header.Timestamp × 1e6` 换算），因此：

- 事务时长分辨率是 **1 秒**，同一秒内完成的事务时长恒为 `0`；
- OLTP 短事务绝大多数落在「同秒完成」档，这**不代表耗时为零**；
- 长事务分析只能识别**跨秒**的事务，时长直方图按秒分档
  （同秒完成 / ≤1 秒 / 2–5 秒 / 6–30 秒 / >30 秒）。

### 锁分析的硬边界

**Binlog 不包含 InnoDB 锁等待、死锁和 MDL 等待信息。** 本页锁结论全部是基于
binlog 的争用风险推断，界面同样如实标注：

- 事务持续时间取自同一事务内首末事件的时间戳，是**持锁时长的下界估计**；
  binlog 只在提交时落盘，事务开始等待与空闲阶段都不可见。
- 行热点是「同一主键在窗口内被多个事务反复改写」的**改写频次**，不是实测锁等待。
  它依赖行镜像里的主键元数据：实例未开启 `binlog_row_metadata=FULL` 时，列名只有
  `@1`、`@2` 且没有主键标记，**行级归因不可用**，界面会明确说明原因并要求改看
  表级写热点，而不是显示一张空表。
- DDL 窗口给出同表 DML 在 ±5 分钟内的事件量，只说明**存在被阻塞的可能**，
  binlog 无法证明是否真的等待过。
- 带 `*` 的事务数按 5 分钟桶累计，跨桶的同一事务会被重复计入，仅作量级参考；
  事件数与行数是精确值。

### 覆盖度与扫描兜底

正文长期只在 OSS，因此聚合由后台索引流水线按分区一次性完成（`analytics` 阶段，
排在关键词全文索引之前，避免全文积压把分析覆盖饿死），本地只保留
`index/analytics-v1/manifest.sqlite3`。查询时：

- 已覆盖分区直接读本地聚合，不读 OSS；
- 未覆盖分区在单次请求内最多即时扫描 N 个（界面可选 0 / 4 / 8 / 16），结果同时
  落盘供后续复用；
- 其余分区在 `coverage` 中原样列出待补齐数量，**空结果不会冒充「没有写入」**；
  时间窗内一个已同步分区都没有时，界面明确提示先去同步。

聚合绑定 `logical_part_id` 与物理对象 SHA：分区被重写或删除后，旧聚合立即失效
并由后台重建，保留期清理时一并回收。

## 数据目录

默认位于程序目录下 `data`：

- `metadata.sqlite3`：任务、文件、分区清单和配置，不保存大批事件。
- `query-results/`：最近 100 个查询任务的 gzip 结果页；任务元数据只保存文件名
  和压缩字节数，不把结果正文写入 SQLite。
- `index/search.sqlite3`：唯一长期保留的本地搜索索引，不含事件正文。
- `index/exact-v1/manifest.sqlite3`：ExactIndex 覆盖清单和 sidecar 身份。
- `index/exact-v1/segments/`：内容寻址、只读打开的不可变主键 sidecar。
- `index/analytics-v1/manifest.sqlite3`：SQL 指纹、事务与锁推断的预聚合，
  不保存事件正文，只保存聚合计数、直方图和 TopN 明细。
- `events/event_date=YYYY-MM-DD/*.parquet`：尚未完成 OSS 大小与 SHA-256 回读
  校验的临时 Parquet；校验完成后立即删除，不等待后台索引。
- `downloads/`：尚未完成解析的临时下载。
- `staging/`：线上容器使用独立 1 GiB RAM 暂存盘承载单个 Go 解析器的
  有界 NDJSON；只有完整 `.ndjson` 会交给转换进程，未完成的 `.part`
  在失败收尾时清理。它不保存唯一数据，容器异常后从持久化原 Binlog 重试。
- `scratch/`：DuckDB/Arrow 的临时溢写目录，位于 `/data` 的 NVMe 卷；
  不再使用容器 256 MiB 的 `/tmp` 内存盘承载并行转换。
- `query-cache/`、`cache/`：旧版正文缓存目录；升级后自动清空，不再写入。
- `logs/`：滚动日志。
- `exports/`：GUI 导出的 CSV；应用会清理超过 24 小时的本地中转副本。

可用环境变量 `RDS_BINLOG_DATA_DIR` 将数据目录放到其他磁盘。

处理期间需给 `downloads/` 预留一个当前 Binlog 加最多三个网络预取文件的
瞬时空间；文件按顺序提交后立即删除。`staging/` 最多存在一个正在转换和
一个等待 ACK 的批次，每批上限 384 MiB，全局受 1 GiB RAM 硬上限约束；
OSS 以约 128 MiB 聚合后提交，每文件最多积压两个上传任务。DuckDB 溢写继续
使用 `/data/scratch` 的 NVMe，不占 RAM 暂存盘。

## 慢 SQL 的完整性、Node ID 与明细（v1.26.5+）

DAS 按 `QueryStartTime` 过滤，但慢日志可能到语句执行结束后才发布。采集器因此在
前向水位之外默认回放最近 7,200 秒；`replaySeconds` 可在每个采集项中覆盖。事件只在
Parquet/元数据提交成功后才进入跨轮去重集合，多个 Node 的提交段进程内串行，历史回放
还会用 canonical serving index 跳过已持久化事件。这样长查询晚到、SQLite 短时写锁和
服务重启都不会再把记录永久越过水位，也不会靠重复写入换完整性。

`data/slow-log-instances.json` 的每个采集项可选填阿里云 DAS `NodeId`。同一个
RDS 实例可以按节点写多项；每项拥有独立水位线与默认 OSS 前缀，采集请求会把
`NodeId` 原样传给 `DescribeSlowLogRecords`，不会把两个节点的批次或断点混在一起：

```json
{
  "instances": [
    {
      "instanceId": "rm-example",
      "nodeId": "pi-node-a",
      "enabled": true,
      "replaySeconds": 7200
    },
    {
      "instanceId": "rm-example",
      "nodeId": "pi-node-b",
      "enabled": true
    }
  ]
}
```

「分析洞察」切到「RDS 慢日志」后可按 Node ID 过滤。Top 慢 SQL 的 SQL 文本是
可点击入口，点开后复用事件详情抽屉展示完整 SQL、执行耗时、锁等待、扫描/返回行数、
账号、客户端、线程、实例和 Node ID。`/api/analytics?source=slowlog` 的每条选中指纹
同时返回 `max_scan_event_id` 与 `max_query_event_id`；对应原始执行统一放在
`sql.sample_events[event_id]`，包含精确 UTC 时间、账号、`client_ip`、线程、实际 SQL、
扫描/返回行数、耗时与锁等待。调用方无需再用任意代表 SQL 猜测最坏执行的来源。

Node ID 从新采集事件开始持久化；升级前已入索引但没有节点元数据的历史记录保留
为空，因此显式选择节点时不会被错误归入任何节点。未配置 `nodeId` 的旧配置继续
按实例采集，行为不变。

慢日志 schema/Node 索引只在显式迁移入口创建，运行时不执行 DDL：
`python -m app.slowlog_migrate --data-dir /data`。需要全库完整性扫描时另加
`--quick-check`；常规发布验收只校验 schema 版本和必需索引，避免在生产高 I/O
窗口默认扫描整个 1.8 GB 索引库。

## ClickHouse 分析层（v1.24.0）

### 全历史 OSS v3（v1.26.4，v1.26.6 持续兼容）

v3 不再把 ClickHouse 定义为最近若干天的缓存。所有查询可见 database part 都以
`history_days=0` 回填到 OSS-backed MergeTree，原始数据仍保存在 OSS，扩容后的
本地盘只承担 cache、staging 和本地 metadata。历史 standalone Parquet 采用无增量
MV 的两阶段 staging 回填，验证双表后按日期 `MOVE PARTITION`；随后创建 MV，让旧
pack、实时新增、内容替换和删除进入同一 durable manifest。任一请求窗口覆盖不完整时
仍自动回到原 Parquet/OSS 链路，因此迁移期间和故障时都不会形成查询空洞。

`1.26.6-rawoss` 对直接查询原始 OSS 的路径采用独立保护：单次 S3 查询最多组合 4 个
对象，Parquet reader 使用 1024 行 block、单下载/解析线程并关闭 row-group 预取，
继续保留 500 MB 交互查询上限。raw-serving 已声明全量接管后，ClickHouse 异常会返回
明确的 `CLICKHOUSE_RAW_OSS_QUERY_UNAVAILABLE`（HTTP 503），不会再在 Web 进程内
启动无界 Parquet/OSS 回退。持续采集期间的覆盖门按请求时间窗判断：窗外 pending
不会使完整历史查询闪回旧链，窗内 pending、删除或未就绪 pack 则安全返回 503；
普通 hot 模式原有的小窗口回退保持不变。

正式切流由 `tools/clickhouse_oss_verify.py` 和
`tools/clickhouse_poc_benchmark.py` 双门控制：前者全量逐 part 对账 time/name 两张
物理表的行数、SHA-256、revision，并检查源 inventory、总行数、staging、journal、
MV 和持久化队列；后者要求固定 1/7/30 天 Top-100 结果完全一致且 p50 至少快 2 倍。
完整发布、回滚及短/长期观测边界见
`docs/clickhouse-oss-v3-runbook.md`。

数据库事件查询在最近 25 小时优先使用 ClickHouse，ingester 对账窗口为 27
小时，表自身 TTL 为 30 小时。`index/clickhouse/manifest.sqlite3` 是唯一覆盖
真值：只有请求时间窗内所有源 part 的 `logical_part_id`、SHA-256、
`content_revision` 和行数都已确认 ready，查询才走 `clickhouse-hot`；覆盖不全、
ClickHouse 不可用或查询失败时自动回到原来的 Parquet/OSS 路径。慢日志继续走
已有的精确 SQLite 索引生成 fingerprint 与实际指标，但聚合查询由独立的
`insight.slowlog_events` 承担。该表按 `(instance_id,event_epoch_us)` 排序并保留
61 天，覆盖真值单独位于 `index/clickhouse/slowlog-manifest.sqlite3`；它不会把通用
`events` 表的排序键错误复用于实例时间范围分析。

`clickhouse-slowlog-ingester` 每批复用精确索引中最多 64 个源 part，一次 Parquet
导出、一次幂等删除和一次批量插入；新建、内容替换、删除、崩溃重试和 TTL 过期均由
manifest 自动收敛。重叠 part 的所有 occurrence 都保留，查询先按
`(instance_id,event_id)` 选择稳定 occurrence，再做 fingerprint、对象、操作与趋势
聚合；删除任一重叠 part 后，其余 occurrence 仍能自动成为结果来源。

源 Parquet part 允许因重叠窗口包含同一个 `event_id`。热层维护两个窄查询排序：
`insight.events_query` 服务纯时间倒序，`insight.events_query_by_name` 服务实例、库、
表精确范围内的时间倒序。API 的模糊库表条件先由 `events_query` 上的小时级名称聚合
投影解析为不超过 64 个精确三元组，再访问名称排序表；过宽条件自动退回时间排序表。
正文按稳定顺序分批读取并在应用侧选择每个 `event_id` 的最新 occurrence，避免
ClickHouse 在宽时间窗为 `LIMIT BY` 保存全量键。迁移验收同时比较行数与 Top-100
内容哈希，速度达标但结果不一致时禁止切流。

Schema 只允许通过 `python -m app.clickhouse_migrate` 显式创建或升级，主服务、
索引器和 ingester 的运行时打开不会执行 DDL。metadata 和慢日志索引同样分别由
`python -m app.metadata_migrate` 与 `python -m app.slowlog_migrate` 显式迁移；
运行时只校验版本与必需对象，不完整时明确拒绝启动。ingester 幂等处理新增、内容替换和
删除，并在每次写入前检查三道自动熔断：生产 `/api/storage` 必须在 1 秒内返回、
数据盘至少保留 120 GiB；业务 `/api/storage` 必须在 1 秒内完整返回，否则新 ingest
立即 fail-closed 暂停。一次成功探针最多复用 1 秒，失败从不缓存；每个新 part 写入前
还读取宿主机 `full avg10`。生产 A/B 已证明该信号不是假阳性：回填运行时
`full avg10` 达到 76% 且 `/api/storage` 超时，停止回填后约 30 秒降至 1%，接口恢复
为 48 ms。因此生产将 `RDS_BINLOG_CLICKHOUSE_IO_FULL_AVG10_MAX=10`，超过 10%
自动暂停，只有降到 5% 或以下才恢复。空间底线、业务接口 SLO、PSI 滞回与设备硬限速
全部自动生效，无需人工维护 pending 清单。共享盘
上的后台 merge 使用一个工作线程连续完成已启动的合并，不执行 `STOP MERGES` 取消
在途合并。服务配置和表设置同时把单次自动 merge 限制为 128 MiB；回填由健康探针、
磁盘空间与 PSI 滞回共同限流，不再配置会让前台查询和后台写入互相饿死的容器块设备
硬限速，因此无需人工开关 merge。

两个查询开关相互独立且默认关闭：通用事件使用
`RDS_BINLOG_CLICKHOUSE_SERVING_ENABLED`，慢日志分析使用
`RDS_BINLOG_CLICKHOUSE_SLOWLOG_SERVING_ENABLED`。只有各自 manifest 全覆盖、
结果对账与性能门槛都通过时才允许打开；慢日志开关打开后，ClickHouse 异常或覆盖不全
仍自动回到 SQLite，回退扫描被串行化并可由查询取消信号中断。新增长期人工维护点：无；
正常增量、历史对账、失败重试、覆盖判断和过期清理由服务自动完成。

Parquet 元数据发布在同一个 SQLite 写事务内返回提交快照，归档字段不再通过提交后的
第二个连接回读；因此瞬时锁竞争不会形成“元数据已提交但本地正文被异常清理”的悬挂
记录。全局归档只接管 `stored`/`done` 文件，`parsing` 文件由原采集重试自恢复；慢
日志和 general log 批次也只在进入 `done` 后对查询可见。

每个运行时进程会持有一个只读 WAL 锚连接，避免短连接关闭时反复触发 SQLite 的
最终 checkpoint 及 WAL/SHM 删除所需的 EXCLUSIVE 锁；显式迁移进程不持有该锚，
仍可正常退出和备份。OSS 上传结果按最多 128 个分区一次原子提交，保留
`synchronous=FULL` 的断电持久性，同时把逐分区事务和 fsync 数量压到原来的至多
1/128。该机制随进程生命周期自动建立和释放，无人工 checkpoint 维护点。

新鲜分区归档不再由主服务打开全局搜索索引或扫描本地交接缓存；独立 indexer 在
索引提交后立即释放正文，同步结束与每小时清理继续执行容量兜底。这样多个慢日志
Node 同时轮询时不会把重复索引检查堆在 HTTP/采集进程内，liveness 不再因这条后台
维护链路饿死。历史索引追赶本身也受共享 NVMe 的 10 MB/s 读、4 MB/s 写设备级
上限保护；`ionice` 只负责调度优先级，不再是唯一的资源隔离手段。

完整保留窗口同步仍由主服务内的后台线程发布 Parquet；因此主服务本身也设置共享
NVMe 的 10 MB/s 读、4 MB/s 写硬上限。这个上限约束后台发现、解析和发布的总 I/O，
避免一次历史追赶把宿主机 full I/O PSI 打满并连带拖慢 API 与 Docker 健康检查；HTTP
元数据读保留在同一高权重 cgroup 内，不需要人工启停或单独维护第二套同步配置。

OSS 正文使用内容寻址键和禁止覆盖头，上传遇到超时、TLS EOF、429 或 5xx 时执行
有界指数退避。每次不确定失败都先 HEAD 回读校验：若服务端其实已经提交就直接确认，
只有对象确实不存在才重试；403 等永久错误立即失败，因此不会重复对象或隐藏凭据问题。

ClickHouse 26.3 的 ParquetV3 读取器在生产
`PLAIN_DICTIONARY` 字符串列上可错误申请 1 GiB；ingester 对 Parquet 导入显式关闭
V3，使用已在两个原始失败 part 上验证行数、SHA256 和 content revision 完全一致的
稳定读取器。每个源 Parquet 本身已是 5–7 万行的理想批次；生产
`part_log` 按 insert `query_id` 对账为每个源 part 生成 2 个 MergeTree part，不需要
额外修改 block 或 async insert 参数。

manifest 已有 pending backlog 时，worker 重启会直接续跑持久化队列，不重复冷扫
metadata；pending 超过 1000 个的初始大回填把全量范围对账退避到每小时一次，
降到 1–1000 个后改为每 10 分钟，清空后自动恢复 30 秒对账。因此正常增量仍
近实时，历史回填期间的新数据在尚未覆盖时继续由旧链路兜底。

长期维护点只有三项：跟随 ClickHouse LTS 安全更新；对 manifest 的 failed/
pending 年龄告警；对 120 GiB 磁盘安全线告警。正常新增、重写、删除、过期和
故障恢复均为自动流程，无人工同步任务。

## 重新安装依赖

若移动到另一台 Windows 机器，运行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\安装依赖.ps1
```

## 安全停止

GUI 的“服务设置”页可以停止后台服务。关闭应用窗口不会停止后台自动同步。

## Linux / Docker 部署

仓库已包含 `Dockerfile`、`compose.yaml` 和 Linux amd64 解析器。当前生产
Compose 按需求将 `8769` 发布到服务器公网地址；必须继续在阿里云安全组中
只放行可信来源 IP。

1. 将 `.env.example` 复制为 `.env` 并把文件权限设为 `0600`。RDS
   AccessKey 可以写在环境变量中，也可以启动后在 GUI 中保存。
   将 `clickhouse/runtime.env.example` 复制为 `clickhouse/runtime.env`，设置
   独立强密码并同样设为 `0600`。
2. 将 `RDS_BINLOG_UID/GID` 设置为宿主机运行用户的 UID/GID。
3. 优先给 ECS 挂载实例 RAM 角色；若选择 AccessKey，可通过 GUI 或受控
   注入写入 `/data/.credentials`，不要写进镜像、Compose 或普通设置。
4. 构建统一镜像，然后依次执行显式 schema 迁移：
   `docker compose build insight`；
   `docker compose run --rm --no-deps insight python -m app.metadata_migrate --data-dir /data`；
   `docker compose run --rm --no-deps insight python -m app.slowlog_migrate --data-dir /data`。
5. 启动 ClickHouse 并迁移其 schema：
   `docker compose up -d clickhouse`；
   `docker compose run --rm --no-deps insight python -m app.clickhouse_migrate --data-dir /data`。
6. 保持 `.env` 中 `RDS_BINLOG_CLICKHOUSE_SERVING_ENABLED=0`，先启动 ingester
   完成 manifest 回填、结果哈希和性能验收；只有这些门槛通过后才显式改为 `1`
   并重建 `insight`。单纯启动 ClickHouse 或 ingester 不会改变生产查询路径。
7. 执行 `docker compose up -d` 启动其余服务。所有容器共用有界、自动轮转的
   `local` 日志驱动和 4 MB 非阻塞缓冲，磁盘日志回压不会卡住健康探针。
8. 打开 `http://192.0.2.10:8769/`。Compose 已绑定 `0.0.0.0`，并通过
   `RDS_BINLOG_ALLOWED_HOSTS` 限定 Host；该管理界面没有登录认证，不应在
   云安全组中对 `0.0.0.0/0` 开放。

Linux 环境变量中的凭据优先级最高。GUI 保存的凭据原子写入数据卷
`/data/.credentials`，目录和文件权限分别为 `0700`、`0600`；它是权限保护
的明文文件，不会写入 SQLite、镜像、Compose 或接口响应。

`deploy/seed-settings.json` 只包含禁用自动同步和 OSS 的示例值，不包含任何部署
标识或凭据。生产参数通过 `.env`、权限受控的数据卷或管理界面注入；切换为 RAM
角色时角色名可留空由 IMDSv2 自动发现。本地不再配置正文容量上限。

## 本地回归

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```
