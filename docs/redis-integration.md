# Redis 持续记录与上涨排查

在「分析洞察 → 数据来源 → Redis」选择实例、分片和一天内窗口。主按钮读取历史记录，不重放业务命令。

## 存在哪里

选择采集主机的数据盘：`data/redis-history.sqlite3`，WAL、独立表和索引，快照与差分压缩保存。默认20秒采样、保留30天、有效数据上限2GiB；采集/应用采样写入均检查容量，上限或主机可用空间低于120GiB时明确熔断，不自动删其他数据。每小时有界清理过期记录。不是逐条全量命令录制，数据量远小于MONITOR；当前不上传OSS、不修改桶生命周期或权限，也不依赖外部对象存储才能回查。

## 持续采什么

- 从云侧刷新实例拓扑，保留完整物理节点标识。每个节点经Proxy读取 `IINFO index server commandstats cpu`，保存调用和usec累计计数。账号由私有配置限定，云侧实时核验全部权限为RoleReadOnly；固定实例FQDN，私有凭据0600、拒绝符号链接。
- 每20秒差分；首次只有基线。节点标识/run_id变化、uptime不连续、计数回退、超过2.5倍采样周期均断开，缺口不外推。Tair没有数据节点run_id时使用物理节点＋uptime，**不把proxy_run_id当节点身份**。
- 20秒为下次采集的最短等待；云请求、故障与调度会加长实际间隔，按实际时间差计算，不把配置周期当实测周期。
- 每分钟读取已存在的节点云监控CPU/QPS与过去10分钟最多100条慢日志，按事件去重。云侧慢日志超过页上限会标部分，API故障进入状态；不调整慢阈值。截断命令只提取脱敏键形状与来源，不保存Value/ARGV/Lua正文。
- 每10分钟只读 `INFO KEY` 记录64个outbox桶的当前槽位与节点索引。查询只使用不晚于窗口结束的已记录快照，不把当前分配套回历史；Proxy索引是节点索引而非逻辑数据库号。
- 不启用MONITOR、审计、诊断任务，不发送CONFIG、写命令、用户脚本或读取业务值。

## 上涨排查口径

当前窗口与昨日同长度窗口分别汇总。完整落在窗口内的差分段才参与；两窗该命令覆盖均≥95%时显示调用量和累计执行耗时增量，否则显示“基线不足”。命令usec是服务器命令执行耗时，不是精确CPU；EVAL/EXEC与内部命令可能嵌套，严禁相加推算总CPU。进程CPU秒差分与云监控曲线是不同证据。缺基线仅展示已记录成本，不给根因分数；新启用采集不能补出旧窗口commandstats。

## 私有配置

数据目录`redis-instances.json`（0600）为显式实例白名单：

```json
[{"instanceId":"r-example01","region":"cn-hangzhou","label":"示例缓存","enabled":true,
  "host":"r-example01.redis.cn-hangzhou.rds.aliyuncs.com","port":6379,"username":"fixture_ro",
  "credentialsFile":".credentials/redis.json","intervalSeconds":20,"retentionDays":30,"maxMiB":2048}]
```

凭据文件只含`username`与`password`。云查询复用应用ECS角色，不回退静态AK。无配置不创建记录库、不启动线程。隔离预览设置`RDS_REDIS_COLLECT=0`，禁止与正式应用重复采集。回滚旧镜像会停止该采集，历史文件保留。

## 键级采样接入

接收入口`POST /api/ingest/redis-samples`，用私有`.credentials/redis-ingest.json`中的至少32字符令牌校验Bearer；未配置令牌默认拒绝。每批最多200条，最多120批/分钟；实例、时间、字段、采样率与耗时有限制，按实例＋样本ID去重。只接受声明字段，拒绝value/ARGV/raw等额外字段。

`tools/redis_sample_hook/hook.go`适用于go-redis/v8。在业务初始化时以0.1%采样率创建Hook并`client.AddHook(hook)`，用应用关闭context运行`go hook.Run(ctx)`。endpoint必须是已有受控HTTPS入口，令牌仅通过业务现有凭据机制提供，禁止改为公共明文HTTP。采样有界512条队列、每秒最多100条批量发送、2秒HTTP超时、不跟随重定向；失败丢弃并累计`Dropped()`，不阻塞或重试业务请求。Pipeline只记录一次整体客户端耗时，不复制到每条子命令。采样率不是完整性证明，Dropped必须进入原有监控。

在领取到期与恢复超时两处分别使用`redissample.WithCategory(ctx,"claim_due")`、`redissample.WithCategory(ctx,"recover_timeout")`。EVAL只记录脚本SHA1与声明的KEYS；不保存脚本正文和ARGV。非outbox键在客户端和接收端双重脱敏，最多保留16个键形状。标记的是客户端端到端耗时，`server_usec`始终为空，无法据此计算服务端CPU占比。样本按实例展示，不猜测当前分片归属。

Hook属于业务代码，需要由该业务的正常发布链路接入；SQL Insight版本上线不等于业务Hook已上线。没有样本时页面明确显示未接入，不伪造键级榜。

## 回归

```text
python -m unittest tests.test_redis_insight
node --test tests/test_redis_workspace.cjs tests/test_starrocks_workspace.cjs tests/test_mongo_workspace.cjs tests/test_workspace.cjs
```

前端只扩展现有数据来源；旧RDS/Mongo/StarRocks、ClickHouse和索引worker不随Redis改动重建。生产数据、实例配置和凭据不进入源码、镜像、测试或跨工作站同步。
