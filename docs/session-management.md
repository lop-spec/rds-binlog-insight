# 会话管理

对齐阿里云 DAS 的「会话管理」：实时看每个节点的会话、异常会话和统计；**结束会话换成 KILL 语句**——服务只有云上只读身份、没有数据库账号，所以只生成语句（连同核对语句）让人在节点上执行，自己不执行任何 kill。另外后台每分钟留存异常会话，事后还能看到「当时谁持锁」。

## 页面

左侧「会话管理」。选实例和节点（默认主实例、全部节点），点刷新才读；首次打开自动读一次——页面比服务状态先打开时，等状态回来、实例列表填好后再读这一次。状态每几秒轮询一次，只在实例或节点有变化时才重写下拉列表，不会收起正在展开的选项。

- **实例会话**：异常会话、运行中会话（命令是 Query / Execute）、会话总数、运行中最长；下面每个节点一行，读取失败的节点写明原因（没权限 / DAS 限流 / 超时），不影响其他节点。
- **会话统计**：按用户、按访问来源、按数据库，各给会话总数、活跃数（运行中或在未结束事务里）、异常数；点一行只看它，再点取消。
- **会话列表**：「异常会话 / 活跃会话 / 全部会话」三个范围，关键词和最短执行时间筛选，按执行时间 / 事务时长 / 会话号排序，100 条一页。每个节点的列表保留全部异常会话和疑似持锁会话，其余各按时长最多 1000 / 1500 条，更多的在统计里仍计入（响应的 `truncated` 为真时页面说明）。
- **导出活跃会话**：当前快照里活跃会话的 CSV（单元格以 `= + - @` 开头时加引号前缀，表格软件不会把 SQL 当公式）。
- **历史（留存）**：选时间窗，列出该窗口里留存到的异常会话——首次 → 最后看到、最长执行、最长事务、原因、命令 / 状态、SQL；「疑似持锁」的排最前；「同时段」按钮以某一行为中心看前后 2 分钟。窗口顶部给每个节点的快照数和应有数，少于 70% 标「留存有缺口」，缺口里的空白不等于没有异常。

## 异常会话（口径同阿里云，阈值可改）

阿里云对 RDS MySQL 的定义：含阻塞型 SQL（执行超过 30 秒，如在等 `Waiting for table metadata lock`）、事务持续超过 30 秒、事务开启后超过 10 秒没再执行语句（可能漏了 COMMIT）。本页用同一接口 `GetMySQLAllSessionAsync` 的字段自己判定（页面「异常判定阈值」可改，接口参数 `blockingSeconds` / `trxSeconds` / `idleTrxSeconds`，1–86400）：

| 原因 | 判定 |
|---|---|
| 语句运行过久 `long_running` | 命令是 Query / Execute 且已运行超过阈值（默认 30 秒） |
| 等待锁 `lock_wait` | 上一条成立，且状态是等元数据锁 / 全局读锁 / 备份锁 / 提交锁 / table flush |
| 事务过长 `long_trx` | 有 `TrxId`，`TrxDuration` 超过阈值（默认 30 秒） |
| 未提交事务 `idle_trx` | 有 `TrxId`，命令是 Sleep，空闲 `Time` 超过阈值（默认 10 秒） |

接口对不在事务里的会话不返回 `TrxId` / `TrxDuration`（在线上节点核对过；`TrxId` 为 `"0"` 同样当作不在事务里），所以整个节点没有这两个字段只代表它没有事务。阿里云「活跃」的口径：Query / Execute，或在未结束事务里；接口的 `ActiveSessionCount` 只算前者，页面分开写「运行中」和「活跃」。

### 疑似持锁（阿里云没有）

有会话在等元数据锁等锁时，持锁者一定比最久的等待者更早开始。快照里满足下面之一、自己没在等锁的会话标「疑似持锁」，按时长从长到短最多 20 个：

- 有未结束事务，事务时长 ≥ 最久等待者已等的时间（`holder_basis = transaction`）；
- 或是正在运行的语句，执行时间 ≥ 最久等待者已等的时间（`holder_basis = statement`）。

这只是按时长推断，DAS 的接口不给阻塞关系。要确切知道是谁，用生成 KILL 语句时附带的核对语句在节点上查 `sys.schema_table_lock_waits`（`performance_schema.metadata_locks`）。**先杀持锁者，不是等待者。** 10-14 事件里那个持锁的空闲事务就是这一类：它不在慢日志里（慢日志要等语句结束），在会话快照里是一个 Sleep 会话，事务时长超过那条 DDL 已等的时间。

## KILL 语句

勾选会话 →「生成 KILL 语句」，按节点各生成一段（`POST /api/sessions/kill-statements`，请求带页面当时显示的那一行、快照时间和模式，服务端不会再读 DAS，也不连数据库）。一段文本包含：

1. 头注释：生成时间、快照时间、节点角色 / 可用区和「会话号只在本节点有效」，以及这段语句用的路由 Hint（见下面「路由 Hint」）；
2. 自检路由（带同一个 Hint）：`SELECT @@server_id, @@read_only`，备节点应是 `read_only=1`、主节点应是 `0`，对不上说明这条连接是主地址或 Hint 没生效，别往下执行；
3. 核对语句：`SELECT … FROM information_schema.PROCESSLIST WHERE ID IN (…)`，确认会话还在、仍是快照里的那个；
4. 持锁核对：`SELECT * FROM sys.schema_table_lock_waits;`；
5. `KILL <id>;`（结束连接并回滚它没提交的事务）或 `KILL QUERY <id>;`（只终止当前语句，对空闲会话无效），每条后面的注释写用户 @ 来源、库、命令、时长、事务时长和原因。

规则：只有整数会话号变成代码，其余值（用户名、来源、库……）只写进注释，并先清掉换行 / 控制字符 / `/*` `*/`，所以一个带换行的用户名塞不进第二条语句；一次最多 200 个、重复会话号合并；**受保护的会话不生成**——复制线程（`Binlog Dump*`）、`system user` / `Daemon` / `Connect` 系统线程、`event_scheduler`、`aliyun_` 开头和 `aliyun_root` 的云内部账号、没有会话号的行——文本里以注释写明跳过的会话号和原因。主节点的段落多一句红字提醒。生成动作只在服务日志留一行（实例、节点、模式、个数，不含 SQL；写明「nothing was executed」）。

### 路由 Hint

集群系列的实例没有每个节点各自的地址，平时只连读写分离地址：普通语句落到代理挑的节点，而会话号只在它所在的节点有意义——核对语句查不到会话、KILL 报 `Unknown thread id`，都是这个原因。所以每条语句都带阿里云读写分离的 Hint：备节点 `/*FORCE_NODE='<节点 ID>'*/`，主节点 `/*FORCE_MASTER*/`。直连节点或主地址时它只是注释；用 mysql 命令行要加 `-c`，否则 Hint 会被命令行工具过滤；节点 ID 只有符合 `[A-Za-z0-9][A-Za-z0-9_-]{0,63}` 才会写进 Hint，否则不带 Hint、文本里写明「请连到这个节点执行」。

实测过的（2026-10-14 / 15，只读账号和 DBX）：

- 查询带 `FORCE_NODE` 能落到指定节点：生产实例的只读代理地址上，试过的两个备节点（`FORCE_NODE` 写节点 ID）都落到了各自的节点：`@@server_id` 与目标一致、`read_only=1`（阿里云文档写「Hint 仅支持读写分离地址」，只读代理地址实测也认）。
- 经代理地址，带 hint 的 `KILL <id>` 能杀掉备节点上的会话，带 hint 的 `KILL QUERY <id>` 只中断语句、连接保留：在测试实例的代理地址上用 `/*FORCE_SLAVE*/` 实测（该实例只有一个备节点）。`FORCE_NODE` 本身对 KILL 没单独实测，它和 `FORCE_SLAVE` 同属代理的 Hint 路由。
- 同一次测试里不带 hint 的 `KILL <id>` 也成功了（阿里云文档写的是「SQL 形式的 KILL 只发往主实例」），可能是代理把它送到了刚才那条查询所在的节点；不依赖这个行为，所以文本里的 KILL 仍带 hint。
- 主地址的连接上 Hint 只是注释：测试实例主地址连接上带 hint 的 `SELECT @@server_id` 返回的是主节点（`read_only=0`），所以文本先让你自检。
- 测试实例的代理地址不认 `FORCE_NODE`（两个节点 ID 都报 `force hint server node is not found`，原因不明），但认 `FORCE_SLAVE` / `FORCE_MASTER`；该实例只有一个备节点，可把备节点语句的 Hint 换成 `FORCE_SLAVE`。生产实例上试过的两个备节点没有这个问题。
- 备选：阿里云 RDS 控制台「自治服务 → 一键诊断 → 会话管理」可以直接结束会话。

## 留存

后台线程 `session-retention`，随服务启动，默认开启：**每 60 秒、每个节点读一次**（一个节点每 `interval / 节点数` 秒轮到，至少间隔 5 秒），只留主实例的节点，异常会话和疑似持锁会话存 7 天。它和页面共用同一个读取器（`LiveSessions`），所以共用 10 秒缓存和「同时最多读两个节点」的限制；读的是已登记采集慢日志的节点，不会对别的实例用云身份。

- **存什么**：`data/session-history.sqlite3`（WAL）。`snapshot` 每次读一行（会话总数、运行中、活跃、异常数、最长运行、等锁数）；`abnormal` 每个异常会话一行，跨快照合并——同实例、节点、会话号、用户、来源，且距上次看到不超过 5 分钟，就更新 `last_us` / 最大值 / 原因并集 / 最新状态和 SQL，一个异常一小时的会话是一行而不是六十行。普通会话只计数、不存。
- **清理**：每小时删 `last_us`（快照 `ts_us`）早于保留天数的行。
- **开关与参数**：`data/session-retention.json`，每个周期重读，不用重启——`{"enabled": false}` 关掉；`intervalSeconds`（30–3600，默认 60）、`retentionDays`（1–60，默认 7）、`instances`（实例 ID 数组，空表示主实例）。文件没有就用默认值；文件写错时按默认值运行并写 WARNING。
- **失败**：DAS 拒绝（限流、没权限、超时）时这个节点按 2×、4× … 最多 10 分钟退避重试，一段失败只在第一次写一条 WARNING（带错误码），恢复时写一条 INFO；别的节点不受影响，页面顶部的留存条显示「N / M 个节点最近读取失败（原因）」。`GET /api/sessions/status` 给开关、参数、每个节点最近一次成功时间、连续失败数和存储规模。
- **成本**：阿里云「会话管理」（实时看会话、结束会话、SQL 限流等）属 DAS 基础服务，文档没有按调用次数收费的条目；本功能只调只读的 `GetMySQLAllSessionAsync`，不用收费的「会话管理与分析」（实时事务、锁等待、全量锁分析，运维服务）。6 个节点每分钟一次约 1.7 万次读取 / 天。本机成本是 yy-ecs 上每天几十 KB 到几 MB 的 SQLite。

## 接口

| 接口 | 说明 |
|---|---|
| `GET /api/sessions?instance=&nodeId=&blockingSeconds=&trxSeconds=&idleTrxSeconds=` | 实例（或一个节点）的会话视图；`nodeId` 留空读该实例全部已登记节点，最多 3 个并行。每个节点 `status: ok` 带 `overview` / `holders` / `stats` / `sessions` / `truncated` / `observed_at_us` / `cached`，失败的带 `code` / `message`；`partial` 表示有节点失败 |
| `GET /api/sessions/history?instance=&nodeId=&startEpochUs=&endEpochUs=&limit=` | 留存的异常会话（默认最近 1 小时，窗口不超过 31 天，最多 500 条）、每个节点的快照覆盖、节点标签和留存状态 |
| `GET /api/sessions/status` | 留存线程状态 |
| `POST /api/sessions/kill-statements` | 生成 KILL 语句，`{instance, node_id, mode: connection|query, snapshot_us, sessions: [...]}`，返回 `text` / `statements` / `skipped` |

所有接口只接受已登记采集慢日志的（实例，节点），否则 400（`SESSIONS_UNKNOWN_INSTANCE` / `SESSIONS_UNKNOWN_NODE`）；错误码 `DAS_PERMISSION_DENIED`（403 语义，节点级）、`DAS_THROTTLED`、`DAS_TIMEOUT` / `DAS_UNREACHABLE` / `DAS_FAILED` 写在节点的 `code` 里。

## 边界

- 不调用 DAS 的结束会话 API（`CreateKillInstanceSessionTask`）：它必填数据库账号和密码，RAM 动作是写权限且不能限定到某个实例，`NodeId` 文档只写给 PolarDB 用，而 8769 没有登录；要做得先有鉴权、专用的最小权限库账号和审计。
- 不做 SQL 限流、自动限流（DAS 的写操作，本服务只有只读身份）、10 秒 SQL 分析和 SQL 优化（DAS 云端能力；慢日志和分析洞察里已有对应的证据）。
- 不加访问保护：8769 没有登录，页面会显示实时 SQL 和用户 / 来源，留存也会把异常会话的 SQL 存 7 天，与审计查询、慢日志页同等暴露。
- 会话号只在同一个节点、同一次服务进程里有意义：节点重启后号会重来，所以核对语句必须先跑。8.0.28 上有过被 KILL 的空闲会话没释放元数据锁的情况（实例现为 8.0.36），杀完用 `sys.schema_table_lock_waits` 确认锁真的没了。
- 留存是每分钟一次的采样：不到一分钟的异常可能漏掉；留存刚开启或某节点读取失败的时间窗有缺口，页面会标出来。

## 验证

`python -m unittest tests.test_session_view tests.test_session_store tests.test_session_collector tests.test_session_service tests.test_live_sessions tests.test_server_roles_and_windows -q`；`node --test tests/test_sessions_ui.cjs tests/test_workspace.cjs`。用线上节点的真实 DAS 快照回放过判定（事务字段只在有事务的会话上出现、`TrxId` 为 `"0"` 的 Sleep 不算事务、`Binlog Dump GTID` 被保护、运行中会话数与接口的 `ActiveSessionCount` 一致）。
