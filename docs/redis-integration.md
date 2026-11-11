# Redis 五层持续记录

**⚠️ 风险：新增只读采集和本系统诊断表会消耗少量网络、磁盘与API配额；沿用容量与主机空间熔断。回滚主应用镜像即可停止新增记录，保留历史；不改业务Redis、ACK工作负载或云侧告警参数。**

入口仍为「分析洞察 → Redis」。所有上涨排查（MySQL、MongoDB、Redis）默认对照紧邻之前的等长窗口：当前13:20–14:20，基线12:20–13:20；Mongo仍可手选昨日或自定义。尾部截短后对照随实际等长重算，不静默挪动事件。一天内窗口查20秒差分与五层证据；一天以上自动查小时汇总，超过31天用日视图，最长400天。长期总量不是上涨根因。

## 持续记录

| 内容 | 频率 | 存放与保留 |
|---|---|---|
| 分片IINFO：server、commandstats、cpu、stats、memory、clients、persistence、keyspace、latencystats、errorstats | 约20秒 | 原SQLite，30天 |
| 命令、CPU、网络/连接/过期淘汰计数、内存区间及最新分位快照 | 分钟/小时汇总 | SQLite，90/400天 |
| 指定Deployment、HPA、镜像、Pod启动、worker数字/布尔白名单 | 每分钟，状态变更去重 | SQLite，长期；快照差分事件不是精确发生时刻 |
| 云侧历史事件/任务 | 每5分钟 | SQLite，长期；只保留类型、时间、状态和标识 |
| 热Key、大Key、重流量Key榜单 | 每分片每分钟 | SQLite，90天；仅阈值榜单，不代表全部Key |
| 已登记outbox四阶段ZSet、payload/版本Hash长度 | 每分钟 | SQLite，90天；只读长度，不读内容 |
| 游标前缀画像 | 每20秒轮一分片，COUNT16，最多16键×TYPE/TTL/MEMORY | SQLite短期及CH30天、小时末快照180天 |
| 凌晨03:00–05:00基线 | 首次补最近两日，之后幂等更新 | SQLite，400天；云侧只含实际可读速率字段，不编历史耗时 |

物理表：原 `redis-history.sqlite3`；CH新增 `redis_client_counts`、`redis_client_hours`、`redis_prefix_profiles`、`redis_prefix_hours`。不使用OSS，不启动MONITOR、审计、实例诊断或修改慢日志阈值。

分钟以差分段结束时间归桶，不按比例把段内调用拆成真实秒；边界和断采不外推。内存保存区间最小/最大/末值，分位只保留供应商累计直方图快照，不对分位相减、求均值。小时汇总与分钟各有水位，原差分未汇总前不提前删除。CH按实际写入小时持久化脏窗口，迟到/断联补传/重启后重算；两类小时表都成功才清标记，失败保留，不局限最近两小时。

配置仍在私有 `data/redis-instances.json`：`sources` 固定ACK集群、namespace及Deployment名单；`queueEnvironments` 只允许空前缀/pre/alpha；`queues` 只登记ws/order_buyer_match的确定键和长度命令；`prefixProfile` 显式开启低速画像。私有地址、账号、令牌不进入仓库。

SQLite沿用实例 `maxMiB`；CH诊断表默认8GiB总预算，可用 `maxClickHouseMiB` 设置。主机可用空间低于120GiB或命中容量预算即暂停，不提前删除证据伪装达标。保留天数是目标，受容量和实际采集覆盖制约。

## 业务进程全量计数

SDK保持在 `tools/redis_sample_hook/`，已从0.1%抽样改成全部逻辑调用聚合，接收口为 `POST /api/ingest/redis-aggregates`。旧 `/redis-samples` 仅兼容原数据，不代表新接入。

业务正常发版时：

1. 每进程只创建一个 `redissample.New(instance, endpoint, token, deployment, pod)`，同一个Hook注册到该目标Redis的**全部**client实例，只运行一次 `hook.Run(shutdownContext)`。统计客户端逻辑调用，不声称覆盖库内所有网络尝试或Lua内部调用。
2. 关键worker/请求分支用 `WithPath(ctx, "静态代码路径")` 标注；跟单任务可再用 `WithCategory` 标注。没有标注就记 `unclassified`，不能假装已定位代码来源。
3. 端点必须是受控HTTPS的 `/api/ingest/redis-aggregates`，禁止明文公网和跳转。token引用既有受控注入渠道，不写源码、日志或命令行。
4. 每20秒上报一个不可变批次；最多499个普通形状＋1个other。ID换成星号、fo桶号保留；只保留固定词汇表命名空间，未知字段也是星号，不把随机英文ID当命名空间。
5. 汇总同时保留逐命令总调用，形状溢出不丢调用。Pipeline逐命令计数，但只记一次Pipeline客户端耗时，不摊到每条命令。收发字节是已知参数/返回类型的RESP估算，`bytes_known_calls` 标记覆盖，不是网卡流量；不传Value、ARGV、Lua正文或错误正文。
6. 网络发送不阻塞业务请求；一个待确认批次用同ID重试，队列有界。未保留窗口的调用进入后续 `dropped_calls`；进程崩溃/退出的未确认尾窗无法保证送达，最后水位缺口不能当零。

接收端先落有界SQLite去重队列，成功返回202，再批量写CH。相同ID不同内容拒绝；CH `ReplacingMergeTree` 加 `FINAL` 防止重试放大。CH异常时明确显示排队/不可用，满队列返回503。上报窗口精确计数，不按比例重建窗口内真实秒或墙钟分钟。

Redis结果默认先展示高相关且有增量的命令，按同名命令合并相关分片；每分片独立计算零时滞带符号Pearson，至少6个有效分钟、r≥0.7，调用/执行成本分别保留相关系数和差分相关反证。高相关但没增长、缺基线的命令单独折叠；所有原始节点/来源/配置证据放在“原始证据与明细”。命令执行耗时不是CPU，不按相关系数分摊资源。

高相关Key只来自实际持续保存的热点QPS或已接入业务形状计数；热点不在榜上不补零，大Key体积/队列长度不当作访问次数。业务计数不能定位分片时只比较完整实例CPU；本窗没有足够Key流量证据就明确说明，不造Key榜。

客户端排行比较“已收到相同形状”的前一等长窗调用变化；缺基线、截断或已报告丢失不造增量。各Deployment/进程/Pod的接入与传输覆盖仍须核对，不把已收到批次自动称为全部业务。

## 离线画像

`POST /api/ingest/redis-profiles` 接收预计算脱敏统计：`instance,node,time,rdb_sha256,complete,total_keys,groups`。每组仅 `keys,estimated_bytes,expires,types`，前缀必须已规范化；禁止RDB内容、原始ID、下载链接与值。完整性是提供者声明，后端校验分组计数但不假装独立认证过RDB。

可运行入口：`python tools/import-redis-profile.py --help`。令牌只从0600文件读取，只允许受控HTTPS或本机环回HTTP，不跟随重定向。统计由离线分析器产生后交此入口；**本次不创建或启用云控制台的每日离线分析定时任务**。

低速ISCAN不是均匀采样，可能重复、漏项和遇到键消失，不外推全库前缀数量、真实内存或增长；MEMORY USAGE使用SAMPLES5，内存是估算。在线样本与离线统计按来源分别保留，小时快照不互相覆盖。

## 归因与缺口

- CPU差分、已处理命令数和客户端往返是不同分母；未证明Lua/EXEC计时包含关系前，不用“进程CPU减命令耗时”认定网络开销。
- 单次成本指数按覆盖相近、至少100次的同种命令对标；Key大小/参数与节点因素都可能影响它，不单凭指数确认宿主机。
- ConfigMap只读字面数字/布尔值，不等于运行进程已加载；Pod就绪不等于worker已执行。三段轮询模型不涵盖waiting_online等其他路径；拟合只属机制证据，不单凭比例锁定根因。
- 服务端计数可能含Lua内部、事务控制、连接与采集器命令；客户端是逻辑调用。只列各自计数与数值差，口径未对齐时不输出伪精确的“未覆盖比例”。
- `persistence/errorstats`及事件循环、主动过期、缓冲、命令失败/拒绝等字段由供应商实际返回决定；未返回就标缺失，不补0。Proxy的connected_clients也不自动等同真实连接数。
- 生产业务SDK未发版、其他客户端未注册、动态队列未登记及采集前历史，必须在页面标明，不冒充覆盖或恢复。

原运维巡检可直接读 `/api/redis/status` 与 `/api/redis/investigate`：CPU等已有异常自动带五层证据；新增可比单位成本抬升与持续记录缺口检查复用原5分钟状态机、模型、收件群与去重通知队列，不新建机器人，不发送探针。
