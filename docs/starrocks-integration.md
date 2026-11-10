# StarRocks 只读接入

StarRocks 已登记到首页共用实例目录。打开「审计查询」选择实例即可直接查询源SQL审计和详情；「分析洞察」选择同一实例自动切换为StarRocks，查看SQL负载、慢查询、导入作业和节点监控。会话页也可选择该实例；权限不足明确提示，不冒充零会话。

使用实例既有只读账号，不新增授权、不修改集群参数。数据库身份和云监控ECS角色分别校验。读取模式为源端直查，不是持续归档：当前没有将SR审计复制到本地/OSS，默认保留范围由源表决定。主审计页默认15分钟，支持SQL原文片段、完整库名、账号、来源和成功/失败筛选；不支持的主键历史、表级行变更和导出控件不会误发给MySQL索引。

## 数据与范围

- SQL 审计直接只读查询源表 `_starrocks_audit_db_.starrocks_audit_tbl`，不复制整库、不在源端建表。可选择源端默认保留的近30天，每次不超过一小时。
- 慢查询按 `queryTime >= 1000` 毫秒筛选，不依赖另开慢日志。审计表实际列需与部署版本一致。
- SQL 负载对有界审计快照进行本地参数化、去重和成本汇总，展示账号、来源、CPU、扫描量、耗时及单次峰值内存；不是上涨根因排名，不将 JOIN 的查询总成本重复分摊给每张表。
- 导入作业读取 `information_schema.loads` 的作业状态和行数，不代表完整逐行变更。
- 云监控读取 CMS `acs_emr` 的 FE/BE CPU、内存、在线状态和 FE 查询速率、排队；按返回的节点展示，缺点不补零。
- 可选每30秒刷新；只有预设时间范围随当前时间更新，自定义故障窗口不会移动。页面不可见或切换来源即停止刷新。

审计按查询开始时间筛选、完成后才入表；插件延迟、源端遗漏、保留到期均可能影响结果。返回成功仅表示这次读取成功，不证明全 FE 无缺失。默认新到旧快照，审计明细最多100条，导入最多200条，SQL负载最多读取3万条或32MiB。达到上限会明确标部分，不外推全窗口次数或零活动。查询量大时应缩短时间或指定库和账号。

审计 SQL 不包含逐行 before/after；内部 binlog 的存在不等于托管产品开放了可用的 CDC。全局在途查询需要 SYSTEM OPERATE：只读账号被拒时页面直接说明权限缺口，不将自身 PROCESSLIST 冒充全集群、不临时提权或回退管理员。

## 受控配置

部署环境数据目录的 `starrocks-instances.json` 是固定实例白名单。配置与密码不进入源码、镜像、测试样本或跨工作站同步。

```json
[{
  "instanceId":"c-example01",
  "region":"cn-hangzhou",
  "label":"示例集群",
  "enabled":true,
  "host":"fe-c-example01-internal.starrocks.aliyuncs.com",
  "port":9030,
  "readonlyUsername":"mcp-ro",
  "credentialsFile":".credentials/starrocks.json"
}]
```

凭据为仅服务用户可读的普通 JSON 文件（Linux 0600，拒绝符号链接），字段为 `username`、`password`。连接时核验固定 FE、CURRENT_USER、CURRENT_ROLE、SHOW GRANTS 和源时区；直接权限只接受 SELECT/USAGE，不接受转授权。超时10秒、每BE执行内存256MiB，审计计划最多扫描两个日分区；并发一个数据库查询，超限关闭 socket，不发 KILL、不排空结果。所有过滤值均为绑定参数，无用户SQL入口。

云监控只允许应用的 ECS 角色认证，不回退静态 AccessKey。跨地域使用目标地域公网 CMS 端点。它与数据库查询相互独立，数据面不可用时仍能查看云监控。

## 回归入口

```text
python -m unittest tests.test_starrocks_metrics tests.test_starrocks_service
node --test tests/test_instances_ui.cjs tests/test_starrocks_workspace.cjs tests/test_mongo_workspace.cjs tests/test_workspace.cjs tests/test_sessions_ui.cjs
```

发版只更新主应用容器，既有 MySQL/Mongo 采集器、ClickHouse 和索引worker不随本功能重建。保留旧主应用容器与原镜像作为回滚；回退后本功能配置保持私有但不再被旧版读取。
