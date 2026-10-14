# 慢 SQL 性能关联排序与 Pod 详情

## 状态与验收边界

已知案例回测使用独立保存的调查结论；不注入真值 SQL，不因缺记录而降低目标排名。严格验收固定三个时间点，比较原报告明确优先次序的 SQL ID 前缀，而非“进前三即通过”：

| 北京时间性能点 | 固定分析窗口 | 对照次序 | 验收 |
|---|---|---|---|
| 2026-09-03 11:14 | 10:40–11:35，告警备节点 | 商品信息全表扫描第一，商品扩展扫描第二 | 2 条顺序相等 |
| 2026-09-07 15:52 | 15:47–15:57，告警备节点 | 店铺列表／日用量聚合第一 | 首位相等 |
| 2026-09-07 16:15 | 16:10–16:21，主节点 | 待办主表 UPDATE 第一，数字分表 UPDATE 家族第二 | 2 条顺序相等 |

三点在真实 ClickHouse、真实昨日基线与现有云凭据的 CMS 查询中全部匹配，单次只读探针约 0.84–0.87 秒；这是单次实测，不是压测。候选浏览器界面也逐点核对通过，普通排序与性能排序往返新增请求为 0。9 月 7 日报告只有候选优先序，原因置信度为 hypothesis，不能宣称因果已确认；副节点报告没有完整有序 Top N，不能编造第二、第三名或宣称完整 Top N 已一致。

下面保留首次未引入昨日基线、未合并原生 SQL 家族的探索性 v1 回测，**不是 v2 的命中率**：

| 北京时间窗口 | 真值场景 | 原计数增量 r 排名 → 新负载重合排名 | 验收 |
|---|---|---|---|
| 2026-09-02 14:45–18:10 | 超长订单全表扫描 | 缺记录 → 缺记录 | **失败** |
| 2026-09-03 10:40–11:35 | 同一连接先后两次商品全表扫描 | 98 / 10 → 1 / 2 | 通过 |
| 2026-09-03 23:45–09-04 00:40 | 备节点切片导出 | 31 → 1 | 通过 |
| 同上，主节点 | 热度批处理 INSERT / UPDATE | 2 / 1 → 1 / 2 | 通过 |
| 2026-09-07 09:00–10:00 | 平稳对照 | 主节点 IOPS 峰值 23.904% | 指标对照通过，不是异常检测器验收 |

v1 四个事故节点场景中通过三个；不能把平稳对照加入事故命中率，也不能宣称近一周全部性能问题均已覆盖。v2 修正的两个问题是：把常态慢查询误当增长主因，以及把原模型同一个数字分表 SQL ID 拆散排名。这些仍是已知案例的回顾验证，不是独立留出集。排名方法未使用 SQL 文本、账号、历史标签或模型。

失败场景的原始调查记录包含一条约 2.86 小时、扫描约 7.34 亿行的查询，而当前 Insight 对应窗口无此执行。索引覆盖完整仅表示已入库分区全部可查询。当前默认回放 7200 秒、发布滞后 300 秒；超长语句结束后才发布、按启动时间过滤，足以解释漏采风险，但缺少当时完整运行日志，不能断言唯一原因。修复方向是独立的低频历史窗口源端对账与补采，不是每两分钟无限扩大热回放。补采写入、长期调度及生产 Pod 数据源尚待授权和验收，本变更不修改采集水位或生产数据库。

## 使用与算法

选择一个实例、慢日志来源和时间窗口，开始分析后在 SQL 排序中选择“性能关联（昨日基线）”。也可从“更多操作 → 对齐节点 IOPS（性能关联）”打开详情。原计数增长 r 仍保留；它不等于性能关联排序分。普通排序快照不变；首次显式选择性能排序读取新接口，之后同一分析快照复用结果，重新分析时失效缓存。

- API：`GET /api/slowlog-impact`，沿用分析页时间、实例、节点、库表和操作筛选。
- 对规范去重后的每条执行，用 `[开始, 开始 + 耗时)` 与完整分钟求交；重叠执行的微秒数相加。只覆盖所选窗口内启动的已采集慢查询，不推断窗口前已在运行但未纳入的查询。
- CMS `Cluster_IOPSUsage` 的采样时间 t 对齐前一分钟 `(t−60s, t]`。这是显式对齐约定，不搜索时间偏移。接口左开右闭；时间参数使用 Unix 毫秒，避免时区歧义。
- 每节点独立计算基线：所选窗口指标的经验下 20% 分位数（排序后下标 `floor((n−1)/5)`）。这不是学习出的静默期。
- 原始重合值 A：`Σ(执行微秒 × max(IOPS使用率 − P20基线, 0))`。
- v2 排名值：`A × max(当前窗口执行微秒 − 昨日同窗执行微秒, 0) / 当前窗口执行微秒`。窗口严格相隔 24 小时、筛选一致，均只取完整分钟内的实际交集。没有增长时不静默退回绝对负载排名。
- 同节点、同数据库、同原生 SQL ID 的实际指纹合并为一个 SQL 家族；保留实际指纹列表，最慢样本不能代表整个分表家族。无原生 SQL ID 时保留实际指纹并记录原因日志。
- 排序用精确有理数，排名值的分子、分母以字符串返回；最后才转为显示份额。完整排名后再截取每节点前十。增长关联份额不是 IOPS 贡献率。
- 另显示执行区间时序与节点 IOPS 的 Pearson r。整数充分统计量计算，最后才做浮点除法；云指标保留输入十进制精度。API 的排名值分子以字符串输出，避免浏览器 53 位整数精度丢失。
- 展开“核对分钟计算序列”可见分钟结束时间、整数指标及比例尺、执行微秒序列和排名值分子。
- 耗时包含等待；相关和重合都不能区分原因、受害者与共同原因。平稳时段也可能存在排名，不把排名第一自动称为事故根因。
- 不完整分钟不参与；缺指标不补零；少于六个完整分钟、指标恒定、当前或昨日索引不完整、昨日超出保留期等返回原因并写 warning 日志。六分钟仅是显示门槛。
- 当前仅支持已有 ClickHouse 查询后端，最多七天、25 万条执行，超限明确拒绝，不静默采样或切换引擎。普通分析和 SQLite 行为不变。新增分析由用户显式触发，不影响原排序零额外请求的契约。
- CMS 复用现有云凭据，仅调用 `DescribeMetricList`。分页最多 48 页，20 秒总预算（最后一次请求可能额外占用最多 8 秒）；不修改 RAM 权限，不执行数据库写操作。

官方接口：[DescribeMetricList](https://help.aliyun.com/zh/cms/cloudmonitor-1-0/developer-reference/api-cms-2019-01-01-describemetriclist)。一分钟指标保留 31 天；本功能采用更窄的窗口和记录数安全边界。

## Pod 元数据接口

慢日志 `/api/event` 增加 `pod` 字段，详情显示 Pod、Namespace、集群、工作负载、UID、Node、Host IP 与绑定观测区间。

Web **不**持有 Kubernetes 凭据，不执行 SSH/sudo，也不挂载 Docker socket。它只读 `${data_root}/pod-inventory.json`，或环境变量 `RDS_BINLOG_POD_INVENTORY` 指定的元数据文件。生产数据源未接入时，详情和日志都明确报告未配置；不能用测试集群冒充生产集群。

文件由获授权的 Kubernetes 只读元数据采集方原子发布；格式如下（地址为文档保留地址）：

```json
{
  "schema_version": 1,
  "instance_ids": ["database-instance-id"],
  "bindings": [{
    "ip": "192.0.2.10", "cluster": "cluster-id", "namespace": "example",
    "name": "app-pod", "uid": "immutable-pod-uid", "node": "worker",
    "host_ip": "192.0.2.20", "owner": "Deployment/app",
    "created_us": 1000000, "first_seen_us": 2000000, "last_seen_us": 3000000
  }]
}
```

- `first_seen_us`/`last_seen_us` 必须来自同 UID、同 IP 的连续观测；采集断档必须分段，不能用 Pod 创建时间冒充 IP 绑定开始时间。
- 历史事件落在观测区间外只能显示候选，不能归属。轮询间的短暂变化仍不可排除，界面明确提示此限制。
- IP 复用、跨集群冲突、hostNetwork 多 Pod 同地址不任意挑选；宿主机或 NAT 地址可能没有 Pod 映射。
- 校验数据库实例适用范围、元数据格式及 8 MiB 上限。元数据不应包含容器环境变量或 Secret。
- 历史文件和凭据不得被本功能重写；本变更不安装新的采集器或计划任务。

## 可重复验证

```text
python -m unittest tests.test_slowlog_impact tests.test_pod_lookup -q
node --test tests/test_workspace.cjs tests/test_workspace_impact.cjs
python tools/backtest_slowlog_impact.py --events private-events.json --baseline private-yesterday.json --metrics private-metrics.json --truth private-truth.json --output new-report.json
```

回测工具只读输入，不覆盖已有报告；缺失真值或排名不达标时退出码 1。真实实例、SQL 与原始证据保留在运维私有目录，不进入公共仓库。

## 上涨排查（1.29.21）

分析洞察选 RDS 后，默认页是「上涨排查」：选实例、时间窗（小于 24 小时）和一个主要指标，得到每个节点里按相关度排序的 SQL（1.30.0 起页面默认改为增长排序，r 退为核对列，见文末），并列出相对昨日同窗的成本增量。慢日志、Binlog 写入、事务、锁争用四个原始视图收在「更多视图」里，第一次打开时才按同一条件加载。

- 接口：`GET /api/slowlog-impact?metric=<id>&order=correlation`（其余参数不变）。`metric`：`cpu`（`Cluster_CpuUsage`）、`iops`（`Cluster_IOPSUsage`，缺省）、`rows_read`（`Cluster_InnoDBRowRead`）、`row_lock`（`Cluster_InnoDBRowLockTimePs`）、`threads`（`Cluster_ThreadsRunning`）。这些指标都按节点返回，取法与 IOPS 相同（`DescribeMetricList`，一分钟点，缺点不补零）。
- `order=correlation`：每个节点内按带符号皮尔逊 r（SQL 每分钟执行微秒与该指标的序列）从高到低；算不出来的（序列恒定、分钟数不足）排在后面并按稳定标识排序，没有隐藏的成本排序。规则与 MongoDB 工作台相同。每个节点最多返回 20 个 SQL 家族（缺省排序仍是 10 个），并带 `correlation_rank`、`active_minutes`。
- 成本增量列：耗时（窗口 vs 昨日同窗）、执行次数、扫描行与锁等待增量，都来自已有字段，缺字段显示 —，不按 0 计算。
- 不带 `order=correlation` 的调用（旧界面的「性能关联」）行为不变。相关只是时序关联，不是资源消耗的度量，也不是因果。

### 自动对齐到最近有索引的时段（1.29.22）

所选时段的慢日志索引不完整（分区尚未入库，或窗口越过了最新已入库分区），或它的昨日对照日索引不完整时，服务端不再直接报「索引尚未完整」，而是在所选窗口前后 6 小时内找**最近的同长度、索引完整**的窗口来分析。

- 判据：窗口只碰已入库（ready）的分区、不碰任何未入库分区、终点不晚于最新已入库分区，昨日同窗同样满足；不晚于当前时刻，昨日对照日仍在保留期内。索引从未完成对账（reconcile）时不猜，直接报错。
- 搜索：以 1 分钟为步长按距离由近到远，距离相同先取更早的窗口；只用两次分区列表（当前范围、对照日范围）在内存里判断全部候选，不逐候选查询。
- 返回：`adjusted = {reason, requested_start_us, requested_end_us, shift_us}`（未挪动时为 `null`），`start_us` / `end_us` 是实际分析的窗口。`reason` 为 `incomplete_index` 或 `incomplete_baseline_index`。挪动后再用清单复核一次，复核不过按原状态报错并写警告日志。
- 找不到：仍返回 `incomplete_index` / `incomplete_baseline_index`，页面提示「前后 6 小时内没有索引完整的同长度时段」。
- 页面：时间框和「更多视图」的查询条件同步改成实际窗口，时间范围下拉切到「自定义」，结果顶部说明早/晚了多久。

## 默认排序、节点角色、实时性与证据来源（1.30.0）

起因：一次集群版备节点 IOPS 90%+ 的事件里，上涨排查按 r 排序把「耗时 2 秒的小查询」排第 1，真正的元凶（三条缺店铺条件的统计查询，各自全表扫描）排第 2/5/6，而旧的「对齐节点 IOPS（性能关联）」排序里它们是第 1–3；同时采集水位恒落后约 6 分钟，窗口末尾没入库时整个窗口被前移到更早的时段，「同一时间点」查不到。

- **排序合并**：页面默认请求 `order=growth`，旧的「对齐节点 IOPS」按钮、抽屉和慢日志页的「性能关联」排序项删除。接口的三种取值都保留：`order=correlation`（按 r）、`order=executions`（旧名，与 `growth` 等价；外部的告警卡片调用方在用）、`order=growth`。增长排序每节点内先列相比昨日同窗**扫描行增加**的 SQL（按增加量），再按**增长份额**（每分钟「执行耗时 × 高于窗口 P20 基线的指标超额」乘以相比昨日同窗的正向耗时增长比例，在节点内占比）；r 只是核对列。**（1.30.1 起不再以扫描行增加为第一级，见文末「增长排序修正」。）**五个指标（cpu / iops / rows_read / row_lock / threads）共用同一套。份额不是资源贡献率，耗时含等待，也不是因果。
- **节点角色**：`GET /api/status` 的 `slowLogs[]` 多 `role`（`primary` / `secondary`，未取到为空）和 `zone`；`/api/slowlog-impact` 每个节点多 `role`、`zone`。来源是 `DescribeDBInstanceAttribute` 的 `DBClusterNodes.DBClusterNode[]`（`NodeId` 与云监控/DAS 的 `rn-…` 一致，`NodeRole`、`NodeZoneId`）——服务本来就为核验实例调用的只读接口，不需要新权限；`DescribeDBInstanceHAConfig` 返回的是数字 ID，不能用。后台线程每 5 分钟刷新，请求路径只读内存；刷新失败保留上次结果并按失败段记一次警告，主备变化记警告。显示的是**当前**角色，主备切换后事故当时的角色可能不同。
- **窗口末尾截断**：所选窗口的慢日志索引不完整时，先看是不是只缺末尾（最新分区还在入库）：是则保留起点、截到最近已入库的整分钟，`adjusted.kind = "truncated"`、`truncated_us`；起点本身没入库、截完不足 6 个完整分钟、或昨日对照日有洞，才按 1.29.22 的规则前后 6 小时内改查最近的同长度窗口（`kind = "shifted"`）。`adjusted` 现在统一为 `{kind, reason, requested_start_us, requested_end_us, shift_us | truncated_us}`；缺 `kind` 的旧响应按 `shifted` 处理。离现在不到 30 分钟的窗口另返回 `ready_through_us`（最新已入库的慢日志事件时间），页面写「数据截至 HH:MM，落后约 N 分钟」。挪窗、截断的日志从 INFO 改为 WARNING。
- **Binlog 来源同样对齐**：`/api/analytics?source=binlog`（单个实例）按 Binlog 行存储的覆盖区间做同样两步，`step` 为 5 分钟，响应带 `adjusted`（`reason = incomplete_binlog_coverage`）和 `ready_through_us`；覆盖读不到或前后 6 小时内没有同长度的覆盖窗口时按所选窗口照常查询并写警告日志，不失败。
- **采集提速**：`slow-log-instances.json` 的 `lagSeconds` 默认从 300 降到 90（下限仍为 60）。晚发布的记录由 `replaySeconds`（默认 2 小时）每轮回放兜底，裕量只需覆盖 DAS 发布抖动；水位由「现在 − 约 6.3 分钟」降到「现在 − 约 2.5–3.5 分钟」（下游入库滞后另计）。配置文件里显式写的值优先于默认值，线上 8 个采集器需要改文件后重启生效。
- **证据来源可勾选**：表单里「慢日志」「Binlog 写入」两个勾选项，默认都勾，只请求勾选的。慢日志 = 上涨排查页；Binlog 写入 = 写入 SQL 页（与事务、锁争用推断共用同一次响应）；慢日志明细仍在「更多视图」里按需加载。只勾 Binlog 时不需要选实例和性能指标。
- **时间框**：开始/结束取整到整分钟（只勾 Binlog 时 5 分钟）；上涨排查要和昨日同窗不重叠，窗口会压到最近 23 小时 55 分并提示——默认的「最近 24 小时」不再直接报「与昨日同窗重叠」。

验证：`python -m unittest tests.test_slowlog_impact tests.test_node_roles tests.test_window_align tests.test_slowlog_collector -q`；`node --test tests/test_workspace.cjs tests/test_workspace_impact.cjs`。

## 增长排序修正、用户列与在途长查询（1.30.1）

起因：一次集群版备节点 IOPS 使用率 100% 持续约 14 分钟，元凶是同一个分析账号的两条各约 885 秒的全表扫描语句（`NOT EXISTS` 子查询没走索引）。慢日志和上涨排查的数据是齐的——该语句家族的增长份额 94%、相关度 r=0.97、运行重叠占 89%——但 1.30.0 的增长排序把它排在 118 个 SQL 家族里的第 87 位，每节点只显示前 10 条，页面上看不到。两个原因：

- 排序第一级是「扫描行比昨日同窗有增加（哪怕只多几行）」，86 条几乎没成本的语句靠它排到前面。
- DAS 对含物化子查询或递归 CTE 的语句，记录的 `Rows_examined` 可能远小于真实扫描量：这两条各只记了几百行和几行。**慢日志的扫描行不是真实 IO 的下界。** 1.30.0 的起因事件里三条全表扫描的扫描行记全了，所以当时没暴露。

**排序（`method = growth_overlap_net_of_lock_v4`）**

- 每节点内按增长份额降序；扫描行增量只在份额相同（份额为零的尾部）时决定先后，仍用作 `attribution.status` 和「扫描 / 锁等待增量」列的证据，不再是门槛。`order=growth` / `executions` / `correlation` 三种取值和响应字段（`rank`、`growth_share`…）不变，告警卡片等调用方无需改动。
- 增长份额的口径不变（每分钟「执行耗时 × 高于窗口 P20 基线的指标超额」乘以相比昨日同窗的正向增长比例，节点内占比），但**增长是扣掉慢日志记录的锁等待之后的**：每次执行按「锁等待 / 耗时」平均摊在整个执行区间里扣除，窗口和昨日同窗同样处理。没有记录锁等待（缺失或 0）时，与旧口径逐位相同；只因等锁变慢的语句份额为零，响应里 `lock_wait_excluded_us` 给出被扣掉的增长。这保住了回归用例「扫描量真增长的语句排在只因锁等待变慢的语句之前」。
- 回测（离线，用真实慢日志事件和云监控指标序列重放）：同一次事故的两个备节点上，元凶都从第 87 位升到第 1；另一次事故里三条全表扫描仍是前三，份额数字与旧口径一致。脚本和原始证据留在运维私有目录，不进公共仓库。
- 单测覆盖：长耗时语句只记了几行扫描时不被三十条各扫百万行的语句压后；扫描增量只分同分；锁等待不算增长且缺失锁时间不当成 0；无锁等待时份额与旧公式逐位相同。

**用户列**

- 审计查询表、上涨排查表、「更多视图 → 慢日志」表各加一列「用户」：数据库账号来自慢日志的 `AccountName`（索引里的 `database_account`）和本地执行日志。审计表里 Binlog 行显示「—」并说明 Binlog 不记录执行账号；写入 SQL / 事务 / 锁争用三个 Binlog 页签没有账号数据，不加这一列。MongoDB 的采集里没有账号字段，也不加。
- 一个 SQL 家族有多个账号时，上涨排查和慢日志表显示执行次数最多的账号加「+N」，悬停看全部。接口：`/api/slowlog-impact` 的每条语句多 `accounts`（最多 3 个，次数多的在前）和 `account_count`（去重后的总数）；`/api/analytics?source=slowlog` 的每条语句同样多这两个字段（ClickHouse 路径在同一次汇总里用 `topKIf` / `uniqExactIf`，回退到 SQLite 时对选出的指纹补一次分组查询）。ClickHouse 的规范事件元组追加第 13 个元素 `database_account`，没有第二次扫描。
- 审计表的 CSV 导出本来就含 `database_account`，不变。

**在途长查询**

慢日志要等语句结束才出现，事故进行中看不到还在跑的元凶。上涨排查每个节点多一个「看正在跑的查询」按钮：点一下才调用 DAS `GetMySQLAllSessionAsync`（只读的异步会话快照，约 2–3 秒），列出该节点正在执行的语句（用户、来源 IP、库、已运行时间、状态、SQL），空闲连接和复制线程不列。

- 接口：`GET /api/live-sessions?instance=<实例 ID>&nodeId=<节点 ID>`；只接受本服务已登记采集慢日志的 (实例, 节点)，其余返回 400 `LIVE_SESSIONS_UNKNOWN_NODE`，不会拿服务的云身份去问任意实例。凭据与慢日志采集同源（ECS 角色），不新增权限。
- 不新增定时采集、不落盘：每个节点同一时刻只读一次（并发点击合并），10 秒内重复点击用缓存，同时最多读两个节点，单次最长等 25 秒。
- 失败有原因：缺权限 403 `DAS_PERMISSION_DENIED`，超时或接口失败 502 `DAS_TIMEOUT` / `DAS_UNREACHABLE` / `DAS_FAILED`，都写 WARNING 日志。
- 返回的 SQL 文本最多 2000 字符（`sql_chars` 给出原长）；它和慢日志样本 SQL 属同一类数据，访问控制与其它接口一致。

验证：`python -m unittest tests.test_slowlog_impact tests.test_live_sessions tests.test_clickhouse_slowlog tests.test_slowlog_index tests.test_server_roles_and_windows -q`；`node --test tests/test_workspace.cjs tests/test_workspace_impact.cjs tests/test_indexed_range.cjs`。

## 审计页操作类型误带 operation=on（1.30.2）

1.30.0 的分析洞察「证据来源」两个勾选框复用了审计页「操作类型」的样式类 `operation-filter`，而审计查询用全局选择器 `.operation-filter input:checked` 收集操作类型：证据来源的勾选框默认勾选、没有 `value`（浏览器取值 `on`），于是默认每次审计查询都带 `operation=on`，什么都匹配不上，结果 0 条（手动勾上某个操作类型时才有结果）。1.30.2 起审计页读取和清空操作类型都限定在 `#view-audit` 内（审计查询、事务钻取、重置条件三处），分析洞察的勾选框不再被审计页读到或清掉。回归测试用「按选择器返回匹配项」的假 `document` 复现旧行为，并检查源码里每处 `operation-filter` 查找都带 `#view-audit` 前缀。

## 复制延迟与延迟溯源（1.31.0）

起因：集群版备节点 `Cluster_DataDelay` 告警（≥30 秒）。一个备节点的复制线程在一条 `ALTER TABLE … ADD INDEX`（主库 11:17:57 记入 binlog，毫秒级完成）上卡了 4077 秒：这条 DDL 在备节点回放时要这张表的元数据锁，一个没在慢日志里出现的持锁者一直不放手，延迟每秒涨 1 秒，到放行才回落。慢日志里看得到的只有被堵的 15 条 `Prepare` 和一条 `SELECT`（各运行约 4077 / 1696 秒，全是受害者），元凶 DDL 本身几毫秒就执行完，**不会出现在任何慢日志里**。上涨排查原有的五个指标（cpu / iops / rows_read / row_lock / threads）都是「瞬时负载」，对这类事件没有答案，所以加了第六个：复制延迟。

- **指标**：`metric=replication_lag`，云监控 `Cluster_DataDelay`（单位秒，按节点，一分钟点，取值与告警同源）。页面「性能指标」下拉里叫「复制延迟 秒（备节点）」。主节点没有复制延迟，不在结果里；只取到主节点的点时返回 `no_replica_lag_samples`。
- **候选 SQL 是主库的语句**：备节点的延迟出在主库写入或备节点的回放线程，不在备节点自己的读查询。所以对每个有延迟样本的备节点，候选集 = **主节点**的慢日志语句（主节点由 `DescribeDBInstanceAttribute` 的节点角色确定，角色未知时记 WARNING、候选为空，溯源卡照常给出）；每条候选带 `origin = "primary"` 和 `source_node_id`，`sample_event_id` 仍是真实事件 ID（详情抽屉可打开）。备节点自己的语句不参与排名，只作为溯源里的「被堵 / 持锁」证据。
- **打分用「延迟增长」而不是「延迟水平」**：延迟是累积量，水位高不代表这几分钟里有什么在写。排名取 `max(lag(t) − lag(t−1 分钟), 0)`（每分钟新增的延迟秒数）作为「指标超额」序列：增长份额、相关度 r 都按它算，节点的曲线与 `metric_values` 仍是延迟水平，增长序列在 `score_values` / `score_scale` / `score_baseline`。窗口前多取一个样本来算第一分钟的增长，缺样本的分钟不补零（沿用「缺点不补零」）。昨日同窗对照、锁等待扣除、20% 分位基线与其他指标完全一致。
- **`lag_origin`（每个备节点一块）**：只用延迟序列本身做算术。一个复制线程卡在同一个事件上时，延迟每秒涨 1 秒，而它正在回放的事件在主库的时间 `origin(t) = t − lag(t)` 不动；只是追不上写入时涨得比 1 慢、起点一直在移。
  - 延迟段（episode）：延迟 ≥30 秒的连续分钟，向前取到延迟从 0.5 秒以上开始涨的那一分钟，向后取到回落到 0.5 秒以下；每个节点最多返回峰值最大的 3 段（`episode_count` 给总数）。
  - `kind`：`stall`（峰值前有 ≥3 分钟斜率 ≥0.85 秒/秒，且起点 `origin_us` 的偏差在一分钟内；个别样本因分钟均值横跨起点/放行而偏离时，只截掉这几个样本，不丢整段）、`behind`（涨得慢，≥3 分钟正增长，没有稳定起点：回放速度跟不上写入）、`spike`（短时尖峰）。另给 `growth_rate`、`recovery_rate`（秒/秒，中位数）。缺一分钟样本时不跨缺口取斜率。
  - `stall.origin_us`：卡住的那个事件在主库的时间（取该段的中位数），按「样本分钟的中点 − 延迟」估算。云监控的分钟点以该分钟的**结束**为时间戳（用 10-14 的事件核过：11:19:00 的点是 11:18:30 的延迟，ALTER 记在 11:17:57，估算与它相差 0.5 秒），一般情况下误差在 ±30 秒内。
  - `stall.release_us`：放行时刻。卡住的线程在最后一个「起点不变」的样本之后的那一分钟里放行，所以默认取该样本的时间戳（10-14 的事件里与实际放行差 6 秒）；爬升段比峰值早结束超过一分钟（最后几个样本被别的东西带偏）但不超过 5 分钟时改取峰值。`queued` / `holders` 都以它为准。
  - `trigger`：用 `origin_us ± 90 秒` 在 Binlog 行存储里查 DDL（聚合表里 `operation = 'DDL'` 的行，事件时间精确到秒，`sample_sql` 是语句文本），取离起点最近且在 90 秒以内的一条：`{type: "ddl", event_us, sql, database, table, offset_s}`；没有则 `{type: "none"}`（卡住的是一个事件的回放：超大事务或回放时等锁，行存储没有逐事务明细，需看那一刻前后的大事务）；行存储查询失败为 `{type: "unavailable"}` 并写 WARNING，其余内容照常返回。
  - `queued` / `holders`：该备节点自己的慢日志里，**随卡顿一起结束**（结束时刻与放行相差 ≤90 秒）且运行 ≥60 秒的语句：开始于起点之后的是被堵在同一把锁后面的受害者（`queued`，运行最久的在前，最多列 5 条，另给总数），开始于起点之前的是持锁者候选（`holders`）。`holders.count = 0` 的含义是**持锁者不在慢日志里**：慢日志要等语句结束才出现，空闲事务 / 连接永远不会出现。只统计窗口内开始且已执行完的慢日志。
  - `node_local`：其他备节点在同一窗口里的峰值都低于 30 秒为 `true`（问题在这个节点上，不是主库写入量）；有别的备节点也过阈值为 `false`；没有其他备节点为 `null`。
- **接口**：`GET /api/slowlog-impact?metric=replication_lag&order=growth`（`order` 三种取值都可用，其余参数不变）；响应多 `candidate_scope = "primary_statements"`、`primary_nodes`，每个备节点多 `lag_origin`。服务端新增只读的 `BinlogRows.ddl_events(instance, start_us, end_us)`（一条 ClickHouse 聚合表查询），`/api/slowlog-impact` 通过 `ddl_lookup` 注入给 `query_resource_overlap`；没有行存储时 `trigger` 为 `none`。不新增权限、不新增采集、不落盘；云监控仍只调用 `DescribeMetricList`。
- **页面**：节点卡片里，曲线下面是「复制延迟溯源」：每段延迟一块，写明峰值、是否卡在同一个事件上、事件在主库的时间、那一刻的 DDL（语句文本转义显示）、被堵语句（可点「明细」打开慢日志详情）、没有持锁者时直说「持锁的不在慢日志里」，以及其他备节点是否也延迟；其后才是主库候选语句表（列名「SQL（主库执行，按增长份额排序）」）。节点里仍保留「看正在跑的查询」。延迟段是卡住型的节点，候选表上方多一行说明：下表只是同期主库上跑得久的慢 SQL，份额和相关度说明不了原因。
- **回归**：`tests/test_lag_origin.py` 用 10-14 事件的真实云监控分钟序列（实例级均值×5，含其他节点的噪声和 12:23 的删除尖峰）复现：一段 `stall`、起点落在 ALTER 的 90 秒内、命中那条 `ALTER TABLE … ADD INDEX` 的 DDL；另有合成序列覆盖慢速落后、短尖峰、缺分钟、两段延迟、DDL 查询失败、持锁者在 / 不在慢日志里。`tests/test_slowlog_impact.py::LagMetricTests` 覆盖增长序列打分（节点仍报水位）、主库候选按备节点复制且不与备节点自己的同 SQL 合并、主节点不出现在结果里、角色未知、只有主节点有点。节点测试覆盖溯源卡各分支与转义。

- **1.31.1**：`active_minutes`（页面「n / m 分钟有执行」的 n）以前只有相关度排序会算，页面默认的增长排序一直显示 0 / m；现在每种排序的每条语句都带它。

验证：`python -m unittest tests.test_lag_origin tests.test_slowlog_impact tests.test_server_roles_and_windows tests.test_binlog_rows -q`；`node --test tests/test_workspace.cjs tests/test_workspace_impact.cjs tests/test_indexed_range.cjs`。

## 复制延迟：和其他备节点比、同期主库写入（1.32.1）

起因：10-14 下午同一个备节点连续落后（14:00 起，峰值 93 秒，拖到 15:36 才回落），其余备节点全程不到 3 秒。1.31.1 的卡片对这种「追不上」只写「备节点的回放速度跟不上主库的写入，下表是同期主库上的慢语句」：没说只有这个节点落后（那句话只在卡死型里显示），下表列的是 COMMIT 和订单表 INSERT，那是主库最忙的几条，没延迟的节点同样在回放，不是原因；真正的触发（对一张表的大批量删除加插入重建，每条不到 100 毫秒，进不了慢日志）和「这个节点自己比兄弟节点慢」都看不到。

- **`lag_origin.peer_health`（每个有延迟段的备节点一块）**：把这个节点和同实例其他备节点逐分钟比，数据只用云监控已有的节点指标，不新增权限、不连数据库。每个答案对五个指标各发一次云监控请求（`Cluster_SlowQueries`、`Cluster_ComSelect`、`Cluster_ThreadsRunning`、`Cluster_CpuUsage`、`Cluster_IOPSUsage`，并行，每个请求取全部节点），回看第一段延迟之前 3 小时；没有任何节点有延迟段时一个请求都不发；读不到时 `peer_health = {status: "unavailable"}` 并写 WARNING，不影响排名。
  - 判定只看一个量：**每 1000 次 SELECT 的慢查询数**（`Cluster_SlowQueries` ÷ `Cluster_ComSelect`），延迟段内、两边都有读流量的分钟合计；节点是兄弟节点中位数的 3 倍以上、且这些分钟里它自己记了至少 60 条慢查询才算 `diverged`（兄弟节点比值低于 0.05 时按 0.05 算，免得一个零慢查询的兄弟让任何节点都显得慢）。阈值依据：在一个生产集群的平静上午和「所有备节点一起延迟」的晚上，每个节点相对其余节点的这个比值（5 分钟平滑）中位数在 0.85–1.2 之间，最大 1.76。
  - `ms_per_read`（并发线程 ÷ 读速率，毫秒）、读 QPS、CPU、IOPS 使用率一并给出，但**不参与判定**：运行线程数里含复制线程自己，回放忙的节点会虚高。页面把它标成「粗估」。
  - **读流量被摘除的分钟单独列出**（`reads_removed`，≥3 分钟一段）：延迟越过读写分离阈值后，代理把节点从读路由里摘掉（它的 SELECT 速率塌到兄弟节点的 5% 以下，而兄弟节点每秒还有 50 次以上）。这些分钟的数字说明不了节点的健康，不进任何比值；缺样本的分钟是 unknown，不是摘除。延迟段内可比的分钟少于 5 个时 `status = "no_reads"`，页面直说「不足以判断」。
  - `diverged_since_us`：判定为慢时，回溯到「到最后一段延迟结束时仍在持续」的那一段慢的起点（5 个可比分钟平滑，容忍 2 个好分钟），告诉你它是不是在第一段延迟之前就已经比兄弟节点慢；中间被摘除的分钟不打断它。
  - `status`：`ok`、`no_peers`、`no_data`（云监控没有这个节点的读指标）、`no_reads`、`unavailable`；`peers` 是参与比较的兄弟节点数。
- **`episodes[].writes`（`behind` 与 `spike` 型的每段延迟）**：主库在这段延迟（向前多取 5 分钟）里写得最多的 5 张表，取自 Binlog 行存储的 5 分钟聚合表（`binlog_agg_5m_v1`）：行事件数、删/插/改拆分、负载字节、占全部行事件的比例，以及**紧挨在前面的同长时间段**里同一张表的行事件数（`before_events`），页面据此写「是前一段的 N 倍」或「前一段几乎没有写入」。按行事件算：一条删一千行的 DELETE 记一千。DDL 不计入（有自己的列表）。查询窗口按 5 分钟取整（与聚合粒度一致，各节点相差几分钟的延迟段共用一次），同一答案最多查 12 个不同窗口，卡死型延迟段不查。行存储读不到时 `writes = {status: "unavailable"}`（WARNING），实例没有行存储则不出现这个字段（WARNING 一条）。`stall` 型不显示：卡在一个事件上与写入量无关。
- **页面**：`behind` 型多一句「同一时段其他备节点的延迟都没到阈值：只有这个节点落后」（或「其他备节点也出现了延迟」）；延迟段下面列写入热点；卡片底部「和其他备节点比」给一句结论（「这个节点自己比其他备节点慢」/「读表现和其他备节点一致」）、一张五行小表、比较依据和被摘除的时段。只有「只有这个节点落后 + 判为更慢」时，主库慢语句表上方才写「这个节点的延迟是它自己比其他备节点慢，不是主库写得太多」，与卡死型的同类提示并列。CPU 和 IOPS 都不高于兄弟节点却更慢时，多一句「多半是在等（存储、锁或宿主机），不是在算」，并说明这里看不到盘延迟。
- **接口**：`GET /api/slowlog-impact?metric=replication_lag` 的响应在 `nodes[].lag_origin` 里多 `peer_health`，在 `episodes[]` 里多 `writes`；老字段不变。服务端新增 `BinlogRows.write_hotspots(instance, start_us, end_us)`（三条 ClickHouse 聚合查询，读已有聚合表）和注入函数 `query_resource_overlap(peer_loader=…, writes_lookup=…)`，默认不传，不传就不读。
- **做不到的**：节点为什么慢（存储延迟、限流、宿主机）；没有复制线程状态（调度线程在等什么、worker 空闲数）；两者都需要连到具体节点或看云厂商的盘指标，这里不做。结论停在「这个节点自己慢，且在等」。
- **回归**：`tests/test_lag_peers.py` 用合成序列覆盖：节点在某一分钟后慢查询率变成数倍（判慢、起点在 5 分钟内）、所有备节点一起延迟（不冤枉）、延迟段内读流量被摘除（单列、不进比值、不打断起点）、整段无读流量（`no_reads`）、小样本不判、缺样本不当零、无兄弟/无数据、指标读取失败只报告。`tests/test_slowlog_impact.py` 的 `LagPeerEvidenceTests` 覆盖按需读取、失败只记日志、写入热点按窗口去重；`tests/test_binlog_rows.py` 覆盖聚合查询的窗口与前一段；`tests/test_workspace.cjs` 覆盖卡片文案与转义。真实事件离线回放（云监控分钟序列，只含数值）：慢查询率每千次读 3.44 对 0.10（×35），起点与人工读曲线得到的时刻相差 3 分钟内；「所有备节点一起延迟」的晚上五个节点比值 0.64–1.23，都不判慢。

验证：`python -m unittest tests.test_lag_peers tests.test_lag_origin tests.test_slowlog_impact tests.test_server_roles_and_windows tests.test_binlog_rows -q`；`node --test tests/test_workspace.cjs`。
