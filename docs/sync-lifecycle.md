# 同步暂停与运行检查

临时维护使用 `POST /api/sync/pause`，请求体 `{"resumeAfterSeconds":900}`；附加实例增加 `instanceId`。支持 1–86400 秒，截止时间从请求时刻计算，当前文件仍在原子提交边界停下。若文件提交前期限已到，暂停被取消，不会强行中断文件。截止时间跨服务重启保留，调度器到期后恢复原来的自动调度，**不修改 autoSync**。

不带期限的暂停仍需手动 `POST /api/sync/start` 恢复，且跨服务重启保留。关闭 `autoSync` 是无限期禁用：从开启改为关闭必须同时提交 `confirmDisableAutoSync:true`；界面提供确认。单次手动启动不会开启原先禁用的自动同步。

`/healthz` 只检查进程存活，继续用于容器探活，不要拿它证明同步追平。`GET /api/sync/health` 分实例返回采集状态；暂停、禁用、启动失败及进度长期未推进返回 HTTP 503，正常返回 200。此端点用于告警，**不要用作自动重启条件**。运行状态也不证明历史完整；缺口需按源文件时间和归档/索引实际覆盖核对。服务以 `SYNC_HEALTH` 记录状态变化，并每 5 分钟重报；控制动作另有 `SYNC_PAUSE`、`SYNC_MAINTENANCE_EXPIRED`、`AUTO_SYNC_CHANGED` 日志。

主采集服务启动前持有数据目录内 `collector.lock` 的内核排他锁，第二个端口或容器不能同时打开同一采集目录。独立索引/ClickHouse worker 不持有主服务锁，不受影响。崩溃自动释放锁，禁止删除锁文件规避冲突。该锁要求所有主采集服务均为支持锁的版本；旧版候选实例必须退出生产目录，隔离验证使用独立目录和独立端口。

升级只切换主服务镜像，不修改业务数据库、不覆盖凭据或实例配置，也不重启独立 worker/ClickHouse。先用隔离目录验证新镜像，再让旧服务的所有 Binlog 任务在文件边界暂停，确认无活动查询后切换；切换失败恢复原镜像并按原自动同步设置恢复。不要为维护将自动同步改为 false。

回归入口：`python -m unittest tests.test_sync_lifecycle` 和 `node --test tests/test_sync_lifecycle.cjs`。云端发布回归同时覆盖下载校验、凭据刷新、文件原子发布和已有查询行为。
