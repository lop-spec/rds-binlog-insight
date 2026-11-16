# 会话管理

对齐阿里云 DAS 的「会话管理」：实时看每个节点的会话、异常会话和统计；**结束会话换成 KILL 语句**——服务没有能 kill 的账号，所以只生成 KILL 语句（没有别的 SQL）让人在节点上执行，自己不执行任何 kill。另外后台每分钟留存异常会话，事后还能看到「当时谁持锁」。

## 页面

左侧「会话管理」。选实例和节点（默认主实例、全部节点），点刷新才读；首次打开自动读一次——页面比服务状态先打开时，等状态回来、实例列表填好后再读这一次。状态每几秒轮询一次，只在实例或节点有变化时才重写下拉列表，不会收起正在展开的选项。

- **实例会话**：异常会话、运行中会话（命令是 Query / Execute）、会话总数、运行中最长；下面每个节点一行，读取失败的节点写明原因（没权限 / DAS 限流 / 超时），不影响其他节点。
- **会话统计**：按用户、按访问来源、按数据库，各给会话总数、活跃数（运行中或在未结束事务里）、异常数；点一行只看它，再点取消。
- **会话列表**：「异常会话 / 活跃会话 / 全部会话」三个范围，关键词和最短执行时间筛选，按执行时间 / 事务时长 / 会话号排序，100 条一页；表头的复选框一次选中 / 取消本页全部可结束的会话（受保护的不选，本页只选了一部分时显示半选）。每个节点的列表保留全部异常会话和疑似持锁会话，其余各按时长最多 1000 / 1500 条，更多的在统计里仍计入（响应的 `truncated` 为真时页面说明）。
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

这只是按时长推断，DAS 的接口不给阻塞关系。要确切知道是谁，得在节点上自己查（本页不生成查询语句；2026-11-16 在生产实例实测，6 个节点的 `performance_schema` 都是关的，`metadata_locks` 和 `sys.schema_table_lock_waits` 都查不到）。**先杀持锁者，不是等待者。** 10-14 事件里那个持锁的空闲事务就是这一类：它不在慢日志里（慢日志要等语句结束），在会话快照里是一个 Sleep 会话，事务时长超过那条 DDL 已等的时间。

## KILL 语句

勾选会话（表头的复选框一次选中 / 取消本页全部可结束的会话，受保护的不选）→「生成 KILL 语句」，不管会话在几个节点上，都合成**一段**文本（`POST /api/sessions/kill-statements` 一次请求，`nodes` 里每个节点带页面当时显示的那些行和该节点的快照时间；老页面还会带 `lock_waiters`，服务端忽略它。服务端按这些会话号重新读一次节点，只写仍在该节点上、用户和来源地址与你选中时一致的会话，不是你选中的那个连接就拒绝）。勾选只对屏幕上的列表有效：换页签、改关键词或最短时长、排序、翻页、刷新后，不在屏幕上的勾选会被去掉，所以文本里只有看得见的会话。文本只有 KILL 语句（每个会话一行，没有检查查询、没有注释），主节点的在前，每条自带所属节点的 Hint，所以在同一个读写分离地址的连接上从头到尾执行即可：

- `KILL <id>;`（结束连接并回滚它没提交的事务），或 `KILL QUERY <id>;`（只终止当前语句，连接和事务还在，对空闲事务无效）；全部节点的连在一起，别的语句一条都没有。

1.41.3 起去掉了原来的检查语句（`@@read_only` 与 `information_schema.PROCESSLIST` 核对、`sys.schema_table_lock_waits`）：只要 KILL。读写分离地址上 Hint 没生效时，`KILL` 可能落到别的节点；想确认落点，可以在同一连接上自己查一次 `@@server_id`、`@@read_only`。

规则：文本里只有整数会话号和 Hint，用户名、来源、库等不进文本（页面表格里看），所以塞不进第二条语句；复制按钮在 HTTP 页面上（没有异步剪贴板）改为选中文本再让浏览器复制，浏览器拒绝时文本保持选中、提示按 Ctrl+C；一次最多 200 个、重复会话号合并；**受保护的会话不生成**——复制线程（`Binlog Dump*`）、`system user` / `Daemon` / `Connect` 系统线程、`event_scheduler`、`aliyun_` 开头和 `aliyun_root` 的云内部账号、没有会话号的行——页面在文本下面写明跳过了哪些、为什么（接口返回的 `skipped`）。选中的会话含主节点的时多一句红字提醒。生成动作只在服务日志留一行（实例、节点列表、模式、个数，不含 SQL；写明「nothing was executed」）。

### 路由 Hint

集群系列的实例没有每个节点各自的地址，平时只连读写分离地址：普通语句落到代理挑的节点，而会话号只在它所在的节点有意义——KILL 报 `Unknown thread id`，是这个原因。所以每条语句都带阿里云读写分离的 Hint：备节点 `/*FORCE_NODE='<节点 ID>'*/`，主节点 `/*FORCE_MASTER*/`。直连节点或主地址时它只是注释；用 mysql 命令行要加 `-c`，否则 Hint 会被命令行工具过滤；节点 ID 只有符合 `[A-Za-z0-9][A-Za-z0-9_-]{0,63}` 才会写进 Hint，否则不带 Hint、文本里写明「请连到这个节点执行」。

实测过的（2026-10-14 / 15，只读账号和 DBX）：

- 查询带 `FORCE_NODE` 能落到指定节点：生产实例的只读代理地址上，试过的两个备节点（`FORCE_NODE` 写节点 ID）都落到了各自的节点：`@@server_id` 与目标一致、`read_only=1`（阿里云文档写「Hint 仅支持读写分离地址」，只读代理地址实测也认）。
- 经代理地址，带 hint 的 `KILL <id>` 能杀掉备节点上的会话，带 hint 的 `KILL QUERY <id>` 只中断语句、连接保留：在测试实例的代理地址上用 `/*FORCE_SLAVE*/` 实测（该实例只有一个备节点）。`FORCE_NODE` 本身对 KILL 没单独实测，它和 `FORCE_SLAVE` 同属代理的 Hint 路由。
- 同一次测试里不带 hint 的 `KILL <id>` 也成功了（阿里云文档写的是「SQL 形式的 KILL 只发往主实例」），可能是代理把它送到了刚才那条查询所在的节点；不依赖这个行为，所以文本里的 KILL 仍带 hint。
- 主地址的连接上 Hint 只是注释：测试实例主地址连接上带 hint 的 `SELECT @@server_id` 返回的是主节点（`read_only=0`），所以检查语句带 `ro`。
- 测试实例的代理地址不认 `FORCE_NODE`（两个节点 ID 都报 `force hint server node is not found`），但认 `FORCE_SLAVE` / `FORCE_MASTER`（2026-11-17 在代理上实测：`FORCE_MASTER` 落主节点、`FORCE_SLAVE` 落备节点）。1.42.1 起，`session-direct.json` 里写了 `"standbyHint": "FORCE_SLAVE"` 的实例，KILL 文本自动用 `/*FORCE_MASTER*/`（主）和 `/*FORCE_SLAVE*/`（备）；这个写法只在实例恰好一个备节点时才对（`FORCE_SLAVE` 指的是「那个」备节点）。生产实例上试过的两个备节点没有这个问题，仍写 `FORCE_NODE`。
- 备选：阿里云 RDS 控制台「自治服务 → 一键诊断 → 会话管理」可以直接结束会话。

## MongoDB（1.34.6 起）

`mongo-instances.json` 里启用的 MongoDB 实例（`dds-…`）出现在同一个实例下拉框里，标「（MongoDB）」。页面、统计、列表、勾选、导出、留存和历史都是同一套，只是数据源、列和结束语句不同。

**成员**：后台线程每 5 分钟用 DDS 只读接口 `DescribeRoleZoneInfo`（角色 ID、主 / 备 / 隐藏、可用区）和 `DescribeReplicaSetRole`（每个角色 ID 的 VPC 地址）刷新，查到之前下拉框里没有这个实例；成员列表经 `GET /api/sessions/status` 的 `mongo` 字段给页面。隐藏节点没有连接地址。

**读取**：1.42.0 起生产实例直连成员读 `$currentOp`（见下「读取方式」）；隐藏成员没有地址，读 DAS。DAS 的 `GetMongoDBCurrentOp`（`InstanceId` + `NodeId`=角色 ID，不带过滤条件，与 DAS 控制台一致：每个客户端连接一行（生产主节点约 1300、备节点约 1200，空闲连接没有 opid）加后台线程）。2026-10-22 实测：`NodeId` 写一个不存在的号时接口**不报错、直接返回主节点的操作**，所以服务只发成员表里的角色 ID（其他一律 400 `SESSIONS_UNKNOWN_NODE`）；隐藏节点的角色 ID 可用。缓存、并发、失败码与 RDS 共用。

**一行是一个操作**，按 RDS 行的字段放，页面换了列名：

| 列 | 来源 |
|---|---|
| opid（下面一行是连接号 `connNNN`） | `OpId`（32 位整数，可能为负）、`Desc` |
| 驱动 | `Driver`（DAS 不给 MongoDB 的用户名） |
| 来源 | `Client` 的 IP（端口不参与统计） |
| 集合 | `Ns` |
| 操作 / 计划 | `Op`、`PlanSummary` |
| 命令 | `Command`，去掉 `lsid`、`$clusterTime`、`$db`、`txnNumber` 等噪声字段；过长时是 DAS 截断后的文本 |

异常只有一类「操作运行过久」：`Active` 且 `SecsRunning` 超过「语句运行超过」阈值（默认 30 秒）。DAS 对 MongoDB 不返回等锁、事务字段，所以没有等待锁 / 事务两类、没有疑似持锁，两个事务阈值输入框在选 MongoDB 时隐藏。「运行中」=`Active`。

**受保护（不生成语句）**：空闲连接 / 没有 opid（没有在跑的操作，killOp 无对象）；`Desc` 不是 `connNNN`（`TTLMonitor` 等内部线程）；集合在 `local.`（oplog 读取）或驱动是 `NetworkInterfaceTL` / `MongoDB Internal Client`（副本集内部连接）；`hello` / `isMaster` / `ping` / 认证命令（驱动探活，长轮询的 `hello` 也不算异常）。

**killOp 语句**（1.42.0 起只有 killOp，没有核对脚本）：opid 是每个成员各自的计数器，同一个数在别的成员上是别人的操作，所以每个成员单独一段，用 mongosh **直连该成员**（`mongosh "mongodb://<成员 VPC 地址>:3717/admin?directConnection=true" -u <账号> -p`，页面写在每段上方，有单独的复制按钮）；每个操作一行：`db.adminCommand({killOp: 1, op: <opid>});`。连错成员会结束别人的操作，所以服务端在写这些行之前先重新读成员，确认每个 opid 仍在跑、仍是你选中时的那条连接（`connNNN`），不是就拒绝（`SESSIONS_CHANGED`）。


文本里只有整数 opid，客户端发来的内容进不了代码。killOp 只结束操作、连接保留，没有 KILL / KILL QUERY 两种模式；执行账号要有 `killop` 权限（只读账号不行）。隐藏节点没有地址，不生成，提示到 DAS 控制台的会话管理里结束。

## 读取方式：直连（RDS MySQL 1.41.3 起，MongoDB 1.42.0 起）

DAS 的 `GetMySQLAllSessionAsync` 是异步接口（先发起、再每 1.5 秒轮询），一个节点 2–3 秒，六个节点并发只开 2 路，冷读一次约 7 秒。同样的事实一条 `information_schema` 语句就能查到，所以 RDS MySQL 节点改为**直连数据库读取**，DAS 只做回落。线上实测（生产实例，6 节点、每节点约 2100 个会话）：一次查询 20–60 ms，6 节点并行约 0.2 秒。MongoDB 成员同理：DAS 每个成员 1.5–2.2 s，直连 `$currentOp` 20–70 ms。

- **语句**：每个节点一条 `SELECT`（没有 KILL、DDL、SET）：`information_schema.PROCESSLIST` 左连 `INNODB_TRX`（`trx_mysql_thread_id = ID` 且 `trx_is_read_only = 0`：InnoDB 的事务一开始都是只读，第一次写或加锁读才升级成读写；只读事务（单条自动提交 SELECT、BEGIN 后只查询就空闲的连接池连接）什么都没占，不算事务，DAS 也不报（在生产实例上对过：备节点上约十个这样的空闲只读事务，DAS 一个都没有）），带 `@@server_id`、`@@read_only`，去掉本连接和 RDS 平台账号（`system user` / `event_scheduler` / `aliyun_root` / `replicator` / `rdsadmin` / `rdsinternal` / `rdsrepl`，再加配置文件 `excludeUsers` 里登记的本部署专有账号——DAS 也不列它们，在生产实例上对过；账号名作为绑定参数传入，不拼进语句），加 `MAX_EXECUTION_TIME(5000)`。结果整理成和 DAS 快照一样的 `Data`（`SessionList` 的 `SessionId / User / Client / DbName / Command / State / Time / SqlText`，在事务里才有 `TrxId / TrxDuration`），实时视图、会话管理、留存、KILL 文本都不用改。
- **账号和地址**：复用 `schema-instances.json`（`RDS_BINLOG_SCHEMA_CONFIG`）里该实例的只读账号（权限 `SELECT, PROCESS, SHOW VIEW`）和读写分离地址；备节点走该地址并带 `/*FORCE_NODE='<节点 ID>'*/`，主节点不在这个只读代理的节点列表里（`FORCE_NODE` 报 `force hint server node is not found`），改连实例自己的地址 `primaryHost`。不新增凭据、不存任何新口令。
- **开关**：`data/session-direct.json`，没有这个文件就不直连。每次改动后自动重读，不用重启：`{"enabled": true, "instances": {"<实例 ID>": {"primaryHost": "<实例主地址>"}}, "excludeUsers": ["<本部署专有的平台账号>"], "cacheSeconds": 3, "connectTimeoutSeconds": 3, "readTimeoutSeconds": 8, "maxExecutionMs": 5000}`（`excludeUsers` 和后四项可省，范围见 `app/direct_sessions.py`）。`{"enabled": false}` 或删文件立即回到 DAS；只列出的实例才直连。MongoDB 实例写 `"dds-…": {}` 即可（成员和账号来自 MongoDB 登记）；代理不认节点 ID 的实例可加 `"standbyHint": "FORCE_SLAVE"`（只在它恰好一个备节点时有效）。
- **读不到就回落**：没有凭据 / 角色未知 / 连不上 / 查询失败 / 落点不对（`@@read_only` 与角色不符，或同实例另一个节点已用过这个 `@@server_id`——说明 Hint 没送到）都读 DAS，节点的 `fallback` 写原因码，页面快照行写「N 个节点直连失败，已回落 DAS」。该节点之后 30 秒内不再试直连（连续失败翻倍，最长 10 分钟），日志只在失败那一刻写一条 WARNING（`direct read failed, reading DAS instead`）。
- **缓存**：直连结果缓存 3 秒（DAS 仍是 10 秒），页面、实时视图、留存循环和 KILL 核对共用。
- **没有直连通道的成员**（MongoDB 隐藏成员，没有地址）：实例按直连读取时，页面不为它等 DAS（约 1 秒），而是沿用留存循环每分钟取的快照（最多 90 秒前），快照行写「N 个节点没有直连通道，沿用 X 秒前的快照」，头部时间只算当次读到的节点；留存循环停了或快照更旧就照常读 DAS。
- **MongoDB 直连**（`app/direct_mongo_sessions.py`）：账号是 MongoDB 采集器已在用的只读账号（`mongo-instances.json` 的 `credentialsFile` / `readonlyUsername`，角色 `read` / `readAnyDatabase` / `clusterMonitor`，含 `$currentOp` 要的 `inprog`）；建连后和采集器一样核对认证身份与角色，不符就拒绝并回落；一次读 = `hello`（核对角色：登记为主的必须是 `isWritablePrimary`、登记为备的必须是 `secondary`，同一个成员名也不能被两个节点认领）+ 一条 `$currentOp`（`allUsers`、`idleConnections`，后接 `$project` 只留页面用的字段，`maxTimeMS` 5000）。只读，不发写命令。每成员保持一个客户端（认证一次），读失败就丢弃重建，关直连时关闭。
- **MongoDB 与 DAS 的差别**：直连列出成员自己的全部 `connNNN` 连接（和 `serverStatus.connections.current` 一致，DAS 少约 20%）；驱动的监控流（`hello` / `isMaster`，每个客户端一条长连接，生产上每成员约 230 条）算连接不算「运行中」操作（DAS 也只显示 1–8 个运行中）；成员内部线程和本服务自己的操作不列。
- **和 DAS 的差别**：事务字段来自 `INNODB_TRX`，只有真正在事务里的会话才有 `TrxId`；DAS 和库内读取不在同一时刻，时长 0–1 秒的瞬时事务两边可能不一致，不影响三类异常的判定。`performance_schema` 在 6 个节点都是关的，所以仍然没有精确的元数据锁持有者。
- **测试实例**：测试实例的只读账号只有 `SELECT, SHOW VIEW`，没有 `PROCESS`，直连只看得到自己的线程（`INNODB_TRX` 直接拒绝），所以该实例的直连读取会报 `direct_no_process_privilege` 并回落 DAS。DBA 在测试实例的主节点执行 `GRANT PROCESS ON *.* TO '<只读账号>'@'%';` 之后，在 `session-direct.json` 的 `instances` 里加上该实例（`primaryHost` 是实例自己的地址；测试实例的代理不认节点 ID，加 `"standbyHint": "FORCE_SLAVE"`）即可，不用重启。

## 留存

后台线程 `session-retention`，随服务启动，默认开启（MongoDB 实例的每个成员同样每分钟读一次、存「操作运行过久」的操作）：**每 60 秒、每个节点读一次**（一个节点每 `interval / 节点数` 秒轮到，至少间隔 5 秒），只留主实例的节点，异常会话和疑似持锁会话存 7 天。它和页面共用同一个读取器（`LiveSessions`），所以共用 10 秒缓存和「同时最多读两个节点」的限制；读的是已登记采集慢日志的节点，不会对别的实例用云身份。

- **存什么**：`data/session-history.sqlite3`（WAL）。`snapshot` 每次读一行（会话总数、运行中、活跃、异常数、最长运行、等锁数）；`abnormal` 每个异常会话一行，跨快照合并——同实例、节点、会话号、用户、来源，且距上次看到不超过 5 分钟，就更新 `last_us` / 最大值 / 原因并集 / 最新状态和 SQL，一个异常一小时的会话是一行而不是六十行。普通会话只计数、不存。
- **SQL 原文**：1.40.3 起去掉采集视图和留存层的 2000 字符裁剪，保存上游返回的全部文本；历史列表保留摘要，点击可查看、复制原文，实时列表也有「原文」入口。`sql_chars` 是上游返回文本的字符数，`sql_truncated` 表示已知截断（如 MongoDB 的 `$truncated`），不保证上游本身返回完整。原文可能含敏感值，访问范围沿用现有策略。
- **旧版兼容**：启动时只给留存 SQLite 添加可空元数据列，不回填、猜补已丢失文本；旧记录提示「旧版记录可能截断」。跨快照保留首条语句，只允许同一前缀的更长原文补齐它，不用连接上的其他语句覆盖证据。
- **清理**：每小时删 `last_us`（快照 `ts_us`）早于保留天数的行。
- **开关与参数**：`data/session-retention.json`，每个周期重读，不用重启——`{"enabled": false}` 关掉；`intervalSeconds`（30–3600，默认 60）、`retentionDays`（1–60，默认 7）、`instances`（实例 ID 数组，空表示主实例加已启用的 MongoDB 实例）。文件没有就用默认值；文件写错时按默认值运行并写 WARNING。
- **失败**：DAS 拒绝（限流、没权限、超时）时这个节点按 2×、4× … 最多 10 分钟退避重试，一段失败只在第一次写一条 WARNING（带错误码），恢复时写一条 INFO；别的节点不受影响，页面顶部的留存条显示「N / M 个节点最近读取失败（原因）」。`GET /api/sessions/status` 给开关、参数、每个节点最近一次成功时间、连续失败数和存储规模。
- **成本**：阿里云「会话管理」（实时看会话、结束会话、SQL 限流等）属 DAS 基础服务，文档没有按调用次数收费的条目；本功能只调只读的 `GetMySQLAllSessionAsync`，不用收费的「会话管理与分析」（实时事务、锁等待、全量锁分析，运维服务）。6 个节点每分钟一次约 1.7 万次读取 / 天。本机成本是 yy-ecs 上每天几十 KB 到几 MB 的 SQLite。

## 接口

| 接口 | 说明 |
|---|---|
| `GET /api/sessions?instance=&nodeId=&blockingSeconds=&trxSeconds=&idleTrxSeconds=&source=` | 实例（或一个节点）的会话视图；`source=das` 强制读 DAS（默认自动：开了直连的实例读库，失败回落 DAS），每个节点带 `source`（`mysql` / `das`）和直连失败时的 `fallback` 原因码；`nodeId` 留空读该实例全部已登记节点，最多 3 个并行。每个节点 `status: ok` 带 `overview` / `holders` / `stats` / `sessions` / `truncated` / `observed_at_us` / `cached`，失败的带 `code` / `message`；`partial` 表示有节点失败 |
| `GET /api/sessions/history?instance=&nodeId=&startEpochUs=&endEpochUs=&limit=` | 留存的异常会话（默认最近 1 小时，窗口不超过 31 天，最多 500 条）、每个节点的快照覆盖、节点标签和留存状态 |
| `GET /api/sessions/status` | 留存线程状态，和 `direct`（直连读取是否开、哪些实例、哪些节点刚回落到 DAS 及原因） |
| `POST /api/sessions/kill-statements` | 生成 KILL 语句，`{instance, mode: connection|query, nodes: [{node_id, snapshot_us, sessions: [...]}, …]}`（顶层直接给 `node_id` / `snapshot_us` / `sessions` 是单节点写法），所有节点合成一段：返回 `text`（全部 KILL，主节点在前）/ `checks`（恒为空）/ `statements` / `session_ids` / `skipped`（每条带 `node_id`）/ `nodes`（每个节点的 `role` / `hint`（节点 ID 不能写进 Hint 时为空，页面提示直连）/ `snapshot_us` / `session_ids` / `statements` / `skipped`） |

所有接口只接受已登记采集慢日志的（实例，节点），否则 400（`SESSIONS_UNKNOWN_INSTANCE` / `SESSIONS_UNKNOWN_NODE`）；错误码 `DAS_PERMISSION_DENIED`（403 语义，节点级）、`DAS_THROTTLED`、`DAS_TIMEOUT` / `DAS_UNREACHABLE` / `DAS_FAILED` 写在节点的 `code` 里。

## 边界

- 不调用 DAS 的结束会话 API（`CreateKillInstanceSessionTask`）：它必填数据库账号和密码，RAM 动作是写权限且不能限定到某个实例，`NodeId` 文档只写给 PolarDB 用，而 8769 没有登录；要做得先有鉴权、专用的最小权限库账号和审计。
- 不做 SQL 限流、自动限流（DAS 的写操作，本服务只有只读身份）、10 秒 SQL 分析和 SQL 优化（DAS 云端能力；慢日志和分析洞察里已有对应的证据）。
- 访问保护：1.39.2 起敏感 API 默认只允许回环 TCP 对端；部署可在私有 `data/http-access.json` 显式登记隔离 worker 网络。公网匿名请求一律拒绝，Host、Origin 和 X-Forwarded-For 不能替代身份。公网登录方式和 HTTPS 入口需由部署方另行审定；没有该入口时只用受控 SSH 转发。此边界不授予任何数据库写入或 KILL 权限。
- 会话号只在同一个节点、同一次服务进程里有意义：节点重启后号会重来，服务端因此在生成前按「会话号 + 用户 + 来源地址」重新核对。8.0.28 上有过被 KILL 的空闲会话没释放元数据锁的情况（实例现为 8.0.36），杀完用 `sys.schema_table_lock_waits` 确认锁真的没了。
- 留存是每分钟一次的采样：不到一分钟的异常可能漏掉；留存刚开启或某节点读取失败的时间窗有缺口，页面会标出来。

## 验证

`python -m unittest tests.test_direct_sessions tests.test_session_view tests.test_session_store tests.test_session_collector tests.test_session_service tests.test_live_sessions tests.test_mongo_sessions tests.test_server_roles_and_windows -q`；`node --test tests/test_sessions_ui.cjs tests/test_workspace.cjs`。用线上节点的真实 DAS 快照回放过判定（事务字段只在有事务的会话上出现、`TrxId` 为 `"0"` 的 Sleep 不算事务、`Binlog Dump GTID` 被保护、运行中会话数与接口的 `ActiveSessionCount` 一致）。
