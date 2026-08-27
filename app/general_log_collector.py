"""General log 采集器：把 RDS 的 mysql.general_log(TABLE 输出)增量灌入现有
事件管道，补齐 binlog 缺失的 SELECT 类语句。

设计约束：
- 轮询连接使用只读账号；写操作只有夜间对 mysql.general_log 的 TRUNCATE，
  使用独立高权限账号，且本模块只允许发出这一条写语句。
- 采集批以"虚拟 binlog 文件"的形式复用 EventStorage.ingest_ndjson_file →
  finalize_file_parts → metadata 状态机，Parquet/索引/OSS 链路零改动。
- instance_id 使用 general log 来源实例，与 binlog 同步实例天然隔离，
  自动同步的 discover/recover 均按 settings.db_instance_id 过滤，不会
  触碰本模块写入的文件记录。
- 水位线为 event_time 的微秒 epoch，持久化在 data 目录 JSON 文件中；
  TRUNCATE 不回退水位线(event_time 单调向前)。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import utc_now_text
from .rds_api import RemoteBinlog

if TYPE_CHECKING:
    from .metadata import MetadataStore
    from .storage import EventStorage

LOGGER = logging.getLogger(__name__)

try:
    _BATCH_ROW_LIMIT = min(
        max(int(os.environ.get("RDS_BINLOG_GLOG_BATCH_ROWS", "200000")), 10_000),
        1_000_000,
    )
except ValueError:
    _BATCH_ROW_LIMIT = 200_000
_READ_STATEMENT_PREFIXES = ("select", "show", "with", "desc", "describe", "explain")
# 真实执行的两类记录：直接执行(Query)与预处理语句执行(Execute，带绑定后的
# 参数值)。应用普遍使用预处理语句，业务 SELECT 绝大多数只出现在 Execute 里。
# 刻意排除 Prepare——它与 Execute 文本相同但只是预编译、不是一次执行，
# 收进来会让每条语句的执行次数翻倍。
_EXECUTION_COMMAND_TYPES = ("Query", "Execute")
# 只用于维护 thread_id → 当前库 的会话轨迹，不产出事件：
# Init DB 的 argument 是库名；Connect 的 argument 形如 "user@host on <db> using TCP/IP"。
_SESSION_DB_COMMAND_TYPES = ("Init DB", "Connect")
_CONNECT_DB_RE = re.compile(r" on (\S+) using ")
# 会话轨迹上限：超过后整体清空重建（线程断开不会发事件，靠自然淘汰）。
_THREAD_DB_LIMIT = 50_000
_TRUNCATE_SQL = "TRUNCATE TABLE mysql.general_log"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)


class GeneralLogConfig:
    """采集配置。默认从环境变量读；多实例时由 data/general-log-instances.json
    逐项覆盖(见 config.load_general_log_instances)。"""

    def __init__(self, overrides: dict[str, Any] | None = None) -> None:
        self.enabled = _env("RDS_BINLOG_GLOG_ENABLED") == "1"
        self.host = _env("RDS_BINLOG_GLOG_HOST")
        self.port = _env_int("RDS_BINLOG_GLOG_PORT", 3306, 1, 65535)
        self.user = _env("RDS_BINLOG_GLOG_USER")
        self.password = os.environ.get("RDS_BINLOG_GLOG_PASSWORD", "")
        self.instance_id = _env("RDS_BINLOG_GLOG_INSTANCE_ID")
        self.poll_seconds = _env_int("RDS_BINLOG_GLOG_POLL_SECONDS", 120, 30, 3600)
        self.truncate_hour = _env_int("RDS_BINLOG_GLOG_TRUNCATE_HOUR", 3, 0, 23)
        # 清理节奏与采集对齐：只在「本轮已追平」时清，间隔仅作下限。源表是
        # CSV 无索引，每轮 WHERE event_time > 水位 都是全表扫，成本与表内行
        # 数线性相关；清理间隔过长会让单轮扫描持续膨胀并挤占采集周期。
        # 设为 0 退回原来的「每天 truncate_hour 点清一次」。
        self.truncate_min_interval_s = _env_int(
            "RDS_BINLOG_GLOG_TRUNCATE_MIN_INTERVAL_SECONDS", 3600, 0, 86400
        )
        self.truncate_user = _env("RDS_BINLOG_GLOG_TRUNCATE_USER")
        self.truncate_password = os.environ.get(
            "RDS_BINLOG_GLOG_TRUNCATE_PASSWORD", ""
        )
        # 标注到事件 database_name，让「全量 SQL」页按 database 筛出 general
        # log 语句（否则会淹没在 binlog 指纹里翻不到）。
        self.label = _env("RDS_BINLOG_GLOG_LABEL", "secondary-db")
        # OSS 对象前缀与 binlog 分开：binlog 走 mysql-binlog/<实例>/，
        # general log 走 mysql-general-log/<实例>/，避免两类来源混在一个目录。
        self.oss_prefix = _env("RDS_BINLOG_GLOG_OSS_PREFIX") or (
            f"mysql-general-log/{self.instance_id}/" if self.instance_id else ""
        )
        # 默认值仅作示例；生产应通过环境变量排除采集器自身与平台内部账号。
        exclude = _env(
            "RDS_BINLOG_GLOG_EXCLUDE_USERS", "readonly_user,service_admin,automation_user"
        )
        self.exclude_users = tuple(
            item.strip().lower() for item in exclude.split(",") if item.strip()
        )
        # 强制清理兜底：源端写入速度长期高于采集速度时，「未追平不清」会让源表
        # 一直涨、每轮全表扫描更慢，形成追不上→更慢→更追不上的死循环，最终把
        # 实例磁盘写满。距上次成功 TRUNCATE 超过这个秒数就强制清一次(丢弃这段
        # 没采到的记录并记 warning)——保住实例可用性优先于日志完整性。0=不启用。
        self.force_truncate_after_s = _env_int(
            "RDS_BINLOG_GLOG_FORCE_TRUNCATE_AFTER_SECONDS", 0, 0, 86400
        )
        # 采样窗口上限(分钟)：到点自动停止本实例采集，用于生产库上「开一小段
        # 时间取样本」的用法，避免忘记关。0=长期采集。
        self.sample_max_minutes = _env_int(
            "RDS_BINLOG_GLOG_SAMPLE_MAX_MINUTES", 0, 0, 10080
        )
        self.state_suffix = ""
        if overrides:
            self._apply_overrides(overrides)

    def _apply_overrides(self, item: dict[str, Any]) -> None:
        """用 JSON 配置覆盖环境变量默认值。只覆盖显式给出的键。"""

        def pick(*names: str) -> Any:
            for name in names:
                if name in item and item[name] not in (None, ""):
                    return item[name]
            return None

        text_fields = {
            "host": ("host",),
            "user": ("user",),
            "password": ("password",),
            "instance_id": ("instanceId", "instance_id"),
            "label": ("label",),
            "truncate_user": ("truncateUser", "truncate_user"),
            "truncate_password": ("truncatePassword", "truncate_password"),
            "oss_prefix": ("ossPrefix", "oss_prefix"),
        }
        for attr, names in text_fields.items():
            value = pick(*names)
            if value is not None:
                setattr(self, attr, str(value).strip())
        int_fields = {
            "port": ("port",),
            "poll_seconds": ("pollSeconds", "poll_seconds"),
            "truncate_hour": ("truncateHour", "truncate_hour"),
            "truncate_min_interval_s": (
                "truncateMinIntervalSeconds",
                "truncate_min_interval_s",
            ),
            "force_truncate_after_s": (
                "forceTruncateAfterSeconds",
                "force_truncate_after_s",
            ),
            "sample_max_minutes": ("sampleMaxMinutes", "sample_max_minutes"),
        }
        for attr, names in int_fields.items():
            value = pick(*names)
            if value is not None:
                setattr(self, attr, int(value))
        enabled = pick("enabled")
        if enabled is not None:
            if isinstance(enabled, str):
                self.enabled = enabled.strip().lower() in {"1", "true", "yes"}
            else:
                self.enabled = bool(enabled)
        exclude = pick("excludeUsers", "exclude_users")
        if exclude is not None:
            if isinstance(exclude, str):
                exclude = exclude.split(",")
            self.exclude_users = tuple(
                str(one).strip().lower() for one in exclude if str(one).strip()
            )
        # 下面两项必须按本实例重算，不能继承环境变量里那一份：环境变量的
        # oss_prefix 与 label 是照 RDS_BINLOG_GLOG_INSTANCE_ID(secondary-db)派生
        # 的，直接继承会把本实例的数据归档到别人的 OSS 目录、并标成别人的库名。
        if not str(item.get("ossPrefix") or item.get("oss_prefix") or "").strip():
            self.oss_prefix = (
                f"mysql-general-log/{self.instance_id}/" if self.instance_id else ""
            )
        if not str(item.get("label") or "").strip():
            self.label = self.instance_id
        # 多实例各自一份水位文件，否则互相覆盖会造成重复采集或漏采。
        self.state_suffix = self.instance_id

    def validate(self) -> list[str]:
        problems: list[str] = []
        if not self.host:
            problems.append("RDS_BINLOG_GLOG_HOST 缺失")
        if not self.user or not self.password:
            problems.append("RDS_BINLOG_GLOG_USER/PASSWORD 缺失")
        if not self.instance_id:
            problems.append("RDS_BINLOG_GLOG_INSTANCE_ID 缺失")
        return problems


def _read_operation(sql: str) -> str:
    """读语句的操作分类：SELECT / SHOW / QUERY(其他只读)。"""

    text = sql.lstrip().lower()
    while text.startswith("/*"):
        closing = text.find("*/")
        if closing < 0:
            return "QUERY"
        text = text[closing + 2 :].lstrip()
    if text.startswith(("select", "with")):
        return "SELECT"
    if text.startswith("show"):
        return "SHOW"
    return "QUERY"


def _is_read_statement(sql: str) -> bool:
    text = sql.lstrip()
    # 剥掉前导注释 /*...*/ 与 -- 行注释后再判定首关键字。
    while True:
        if text.startswith("/*"):
            closing = text.find("*/")
            if closing < 0:
                return False
            text = text[closing + 2 :].lstrip()
            continue
        if text.startswith("--"):
            newline = text.find("\n")
            if newline < 0:
                return False
            text = text[newline + 1 :].lstrip()
            continue
        break
    lowered = text.lower()
    return lowered.startswith(_READ_STATEMENT_PREFIXES)


class GeneralLogCollector:
    """驻留线程：轮询 general_log → NDJSON → 现有 ingest 链路。"""

    def __init__(
        self,
        metadata: "MetadataStore",
        storage: "EventStorage",
        config: GeneralLogConfig | None = None,
        archiver: Any | None = None,
    ) -> None:
        self.metadata = metadata
        self.storage = storage
        self.archiver = archiver
        self.config = config or GeneralLogConfig()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # thread_id → 当前库。由 Init DB / Connect 记录维护，采集重启后为空，
        # 只影响重启后老连接的定库（读不出就置空，与旧行为一致）。
        self._thread_db: dict[int, str] = {}
        suffix = getattr(self.config, "state_suffix", "")
        self._state_path = storage.paths["root"] / (
            f"general_log_state-{suffix}.json" if suffix else "general_log_state.json"
        )
        self._started_monotonic = 0.0
        self._last_truncate_monotonic = 0.0
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "enabled": self.config.enabled,
            "instanceId": self.config.instance_id,
            "label": self.config.label,
            "stopped": False,
            "lastPollUtc": "",
            "lastError": "",
            "ingestedTotal": 0,
            "lastBatchRows": 0,
            "lastTruncateUtc": "",
            "watermarkEpochUs": 0,
        }

    # ---------------- 生命周期 ----------------

    def start(self) -> None:
        if not self.config.enabled:
            LOGGER.info("general log 采集未启用(RDS_BINLOG_GLOG_ENABLED!=1)")
            return
        problems = self.config.validate()
        if problems:
            LOGGER.error("general log 采集配置不完整: %s", "; ".join(problems))
            self._set_status(lastError="; ".join(problems))
            return
        self._thread = threading.Thread(
            target=self._run,
            name=(
                f"general-log-collector-{self.config.instance_id}"
                if self.config.instance_id
                else "general-log-collector"
            ),
            daemon=True,
        )
        self._thread.start()

    def shutdown(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10)

    def status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._status)

    def _set_status(self, **values: Any) -> None:
        with self._status_lock:
            self._status.update(values)

    # ---------------- 水位线 ----------------

    def _load_watermark(self) -> tuple[int, str]:
        try:
            raw = json.loads(self._state_path.read_text("utf-8"))
            return int(raw.get("watermark_epoch_us") or 0), str(
                raw.get("last_truncate_date") or ""
            )
        except (OSError, ValueError):
            return 0, ""

    def _save_watermark(self, epoch_us: int, last_truncate_date: str) -> None:
        payload = json.dumps(
            {
                "watermark_epoch_us": int(epoch_us),
                "last_truncate_date": last_truncate_date,
                "updated_utc": utc_now_text(),
            }
        )
        tmp = self._state_path.with_suffix(".json.tmp")
        tmp.write_text(payload, "utf-8")
        os.replace(tmp, self._state_path)

    # ---------------- 主循环 ----------------

    def _run(self) -> None:
        LOGGER.info(
            "general log 采集启动: %s:%s instance=%s poll=%ss",
            self.config.host,
            self.config.port,
            self.config.instance_id,
            self.config.poll_seconds,
        )
        watermark, last_truncate_date = self._load_watermark()
        self._set_status(watermarkEpochUs=watermark)
        self._started_monotonic = time.monotonic()
        self._last_truncate_monotonic = self._started_monotonic
        while not self._stop.is_set():
            started = time.monotonic()
            backlog = False
            if self._sample_window_expired(started):
                LOGGER.warning(
                    "general log 采样窗口已到(%s 分钟)，停止采集：instance=%s",
                    self.config.sample_max_minutes,
                    self.config.instance_id,
                )
                self._set_status(
                    stopped=True,
                    lastError=(
                        f"采样窗口 {self.config.sample_max_minutes} 分钟已用完，"
                        "采集已停止；请在 RDS 控制台关闭 general_log"
                    ),
                )
                return
            try:
                watermark, ingested, raw_count = self._poll_once(watermark)
                if ingested:
                    self._save_watermark(watermark, last_truncate_date)
                # 满批说明源端还有积压，立即连续拉取直到追平，
                # 否则积压增长速度可能永远超过固定节拍的消费速度。
                backlog = raw_count >= _BATCH_ROW_LIMIT
                # 清理必须在追平之后：本轮满批时源表里还有没读到的记录，
                # 此时 TRUNCATE 会把它们直接删掉。
                last_truncate_date, watermark = self._maybe_truncate(
                    watermark, last_truncate_date, caught_up=not backlog
                )
                self._set_status(
                    lastPollUtc=utc_now_text(),
                    lastError="",
                    watermarkEpochUs=watermark,
                )
            except Exception as exc:  # noqa: BLE001 - 驻留线程必须自愈
                LOGGER.exception("general log 采集循环失败")
                self._set_status(lastError=f"{type(exc).__name__}: {exc}")
            elapsed = time.monotonic() - started
            self._stop.wait(
                1.0 if backlog else max(self.config.poll_seconds - elapsed, 5.0)
            )

    def _sample_window_expired(self, now_monotonic: float) -> bool:
        limit = self.config.sample_max_minutes
        if limit <= 0:
            return False
        return now_monotonic - self._started_monotonic >= limit * 60

    # ---------------- 采集 ----------------

    def _connect(self, user: str, password: str):
        import pymysql

        return pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=user,
            password=password,
            database="mysql",
            connect_timeout=10,
            read_timeout=120,
            write_timeout=30,
            charset="utf8mb4",
            autocommit=True,
        )

    def _poll_once(self, watermark_epoch_us: int) -> tuple[int, int, int]:
        rows = self._fetch_rows(watermark_epoch_us)
        if not rows:
            self._set_status(lastBatchRows=0)
            return watermark_epoch_us, 0, 0
        events, max_epoch_us = self._build_events(rows)
        if events:
            self._ingest_batch(events)
            with self._status_lock:
                self._status["ingestedTotal"] += len(events)
                self._status["lastBatchRows"] = len(events)
        else:
            self._set_status(lastBatchRows=0)
        # 水位线推进到本批最大 event_time，无论是否有可入库语句，
        # 否则被过滤的批会导致重复扫描同一段。
        return max(max_epoch_us, watermark_epoch_us), len(events), len(rows)

    def _fetch_rows(self, watermark_epoch_us: int) -> list[dict[str, Any]]:
        connection = self._connect(self.config.user, self.config.password)
        try:
            with connection.cursor() as cursor:
                # 时间基准自校准：general_log 会记录本连接刚执行的语句，用
                # 「自己这行日志的 UNIX_TIMESTAMP 与本机真实时间的差」现场测出
                # 偏移（正常为 0；若 RDS 侧写入语义变化会自动适应），按半小时
                # 取整消除采样抖动。禁止硬编码时区假设——2026-08-05 教训。
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.execute(
                    "SELECT MAX(UNIX_TIMESTAMP(event_time))"
                    "  FROM mysql.general_log"
                    " WHERE thread_id = CONNECTION_ID()"
                )
                probe = cursor.fetchone()[0]
                tz_offset_s = 0
                if probe is not None:
                    drift = time.time() - float(probe)
                    tz_offset_s = int(round(drift / 1800.0) * 1800)
                    tz_offset_s = max(min(tz_offset_s, 15 * 3600), -15 * 3600)
                cursor.execute(
                    "SELECT CAST((UNIX_TIMESTAMP(event_time) + %s) * 1000000"
                    "            AS SIGNED) AS epoch_us,"
                    "       user_host, thread_id, server_id, command_type,"
                    "       CONVERT(argument USING utf8mb4) AS sql_text"
                    "  FROM mysql.general_log"
                    # GREATEST 钳位：水位线为 0 时减去时区偏移会变负数，
                    # FROM_UNIXTIME(负数) 返回 NULL 导致 WHERE 恒假。
                    " WHERE event_time >"
                    "       FROM_UNIXTIME(GREATEST(%s / 1000000.0 - %s, 0))"
                    # command_type 下推到 SQL：Prepare/Close stmt/Quit 占源表
                    # 约 59%，在库侧丢弃可省掉这部分传输与解码。Init DB 与
                    # Connect 不产出事件，只用于还原各连接的当前库（general
                    # log 的语句行不带库名，定库只能靠会话轨迹）。
                    "   AND command_type IN (%s, %s, %s, %s)"
                    " ORDER BY event_time"
                    " LIMIT %s",
                    (
                        tz_offset_s,
                        watermark_epoch_us,
                        tz_offset_s,
                        *_EXECUTION_COMMAND_TYPES,
                        *_SESSION_DB_COMMAND_TYPES,
                        _BATCH_ROW_LIMIT,
                    ),
                )
                columns = [item[0] for item in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            connection.close()

    def _build_events(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        events: list[dict[str, Any]] = []
        max_epoch_us = 0
        for index, row in enumerate(rows):
            epoch_us = int(row.get("epoch_us") or 0)
            max_epoch_us = max(max_epoch_us, epoch_us)
            command_type = str(row.get("command_type") or "")
            sql_text = row.get("sql_text") or ""
            if isinstance(sql_text, bytes):
                sql_text = sql_text.decode("utf-8", "replace")
            # 会话轨迹行：更新 thread→库 映射后跳过，不产出事件。行序就是
            # event_time 序，所以同线程后续语句一定拿到最新的库。
            if command_type in _SESSION_DB_COMMAND_TYPES:
                thread_id = int(row.get("thread_id") or 0)
                if command_type == "Init DB":
                    database = sql_text.strip().strip("`")
                else:
                    matched = _CONNECT_DB_RE.search(sql_text)
                    database = matched.group(1) if matched else ""
                if thread_id and database:
                    if len(self._thread_db) >= _THREAD_DB_LIMIT:
                        self._thread_db.clear()
                    self._thread_db[thread_id] = database
                continue
            if command_type not in _EXECUTION_COMMAND_TYPES:
                continue
            user_host = str(row.get("user_host") or "")
            lowered_user = user_host.lower()
            if any(item in lowered_user for item in self.config.exclude_users):
                continue
            if not _is_read_statement(sql_text):
                continue
            digest = hashlib.sha256(
                "\x1f".join(
                    (
                        self.config.instance_id,
                        str(epoch_us),
                        str(row.get("thread_id") or 0),
                        str(index),
                        sql_text[:512],
                    )
                ).encode("utf-8", "replace")
            ).hexdigest()
            # general log 的语句行不带库名，用会话轨迹（Init DB/Connect）还原；
            # 采集重启后老连接读不到，置空退回旧行为。
            session_db = self._thread_db.get(int(row.get("thread_id") or 0), "")
            events.append(
                {
                    "event_id": f"glog-{digest[:40]}",
                    "event_epoch_us": epoch_us,
                    "raw_event_type": "GENERAL_LOG",
                    "operation": _read_operation(sql_text),
                    # 实例维度由分区归属(instance_id)承载，analytics 已支持
                    # instance 过滤；database 维度是会话当前库。
                    "database_name": session_db,
                    "table_name": "",
                    "table_map_id": 0,
                    "schema_version_id": "",
                    "server_id": int(row.get("server_id") or 0),
                    "thread_id": int(row.get("thread_id") or 0),
                    "transaction_id": "",
                    "gtid": "",
                    "xid": "",
                    "start_position": 0,
                    "end_position": 0,
                    "row_index": 0,
                    "execution_time_ms": 0,
                    "error_code": 0,
                    # ORIGINAL = QueryEvent 原文语义，statement_profile 才会把
                    # sql_text 当真实语句进入「全量 SQL」聚合；来源账号以块注释
                    # 前缀嵌入，normalize_sql 会剥掉它，不影响指纹归并。
                    "sql_kind": "ORIGINAL",
                    # db= 供 select_explain 定库使用；normalize_sql 会剥掉整个
                    # 注释，不影响指纹归并。
                    "sql_text": (
                        f"/* {user_host} db={session_db} */ {sql_text}"
                        if session_db
                        else f"/* {user_host} */ {sql_text}"
                    ),
                    "sql_bytes_base64": "",
                    "before_json": "",
                    "after_json": "",
                    "columns_json": "",
                    # row_query 必须留空：statement_profile 在 sql_kind 不是
                    # ORIGINAL 时会把 row_query 当语句原文，账号串会污染聚合。
                    "row_query": "",
                    "header_epoch_us": epoch_us,
                    "commit_epoch_us": epoch_us,
                    "txn_last_committed": 0,
                    "txn_sequence_number": 0,
                    "txn_length_bytes": 0,
                }
            )
        return events, max_epoch_us

    def _ingest_batch(self, events: list[dict[str, Any]]) -> None:
        begin_us = min(int(item["event_epoch_us"]) for item in events)
        end_us = max(int(item["event_epoch_us"]) for item in events)

        def iso(epoch_us: int) -> str:
            return (
                datetime.fromtimestamp(epoch_us / 1_000_000, UTC)
                .strftime("%Y-%m-%dT%H:%M:%SZ")
            )

        # 文件名由批次时间区间确定性生成：宕机后重跑同一批会命中同一条
        # 文件记录(stable_id 相同)，state=done 直接跳过，天然幂等去重。
        stamp = f"{begin_us}-{end_us}"
        log_file_name = f"general-log/{self.config.instance_id}/{stamp}"
        staging = self.storage.paths["staging"]
        staging.mkdir(parents=True, exist_ok=True)
        ndjson_path = staging / f"glog-{stamp}-{os.getpid()}.ndjson"
        body = "\n".join(
            json.dumps(item, ensure_ascii=False, separators=(",", ":"))
            for item in events
        )
        ndjson_path.write_text(body + "\n", "utf-8")
        try:
            item = RemoteBinlog(
                log_file_name=log_file_name,
                log_begin_utc=iso(begin_us),
                log_end_utc=iso(end_us),
                file_size=ndjson_path.stat().st_size,
                checksum_crc64="",
                download_link="",
                intranet_download_link="",
                link_expired_utc="",
                remote_status="Completed",
                host_instance_id="general-log",
            )
            settings = self.metadata.load_settings()
            glog_settings = replace(settings, db_instance_id=self.config.instance_id)
            file_id, state = self.metadata.upsert_remote(glog_settings, item)
            if state == "done":
                return
            self.metadata.set_file_state(
                file_id,
                "parsing",
                query_visible=False,
            )
            count, parts = self.storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=self.config.instance_id,
                host_instance_id="general-log",
                source_file_name=log_file_name,
                ndjson_path=ndjson_path,
                part_key="000000",
                append=False,
            )
            self.storage.finalize_file_parts(
                file_id, {str(part["path"]) for part in parts}
            )
            self.metadata.set_file_state(file_id, "stored", event_count=count)
            self.metadata.set_file_state(file_id, "done", event_count=count)
            # 立即归档到 OSS：后台索引器只处理已归档分区，不归档等于这批
            # 数据永远进不了「全量 SQL」。归档失败不阻断采集，下一轮由
            # 同步任务的存量归档兜底。
            if self.archiver is not None:
                try:
                    archived = self.archiver.archive_parts_now(
                        parts, self.config.oss_prefix
                    )
                    self._set_status(lastArchivedParts=archived)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("general log 分区归档失败: %s", exc)
                    self._set_status(lastArchiveError=f"{type(exc).__name__}: {exc}")
            LOGGER.info(
                "general log 批已入库: %s events=%s parts=%s",
                log_file_name,
                count,
                len(parts),
            )
        finally:
            if ndjson_path.exists():
                try:
                    ndjson_path.unlink()
                except OSError:
                    pass

    # ---------------- 源表清理 ----------------

    def _truncate_slot(self, now_local: datetime) -> str:
        """本次清理所属的槽位；同一槽内只清一次。

        间隔模式下用 epoch 整除定槽，不依赖挂钟对齐到整点——清理时机由采集
        追平决定，槽位只负责「同一段时间内不重复清」。
        """

        interval = self.config.truncate_min_interval_s
        if interval <= 0:
            return now_local.strftime("%Y-%m-%d")
        return f"s{int(now_local.timestamp()) // interval}"

    def _maybe_truncate(
        self,
        watermark_epoch_us: int,
        last_slot: str,
        *,
        caught_up: bool,
    ) -> tuple[str, int]:
        if not self.config.truncate_user or not self.config.truncate_password:
            return last_slot, watermark_epoch_us
        # 未追平时清理等于丢数据，直接跳过等下一轮。但源端写入长期快过采集时
        # 这条规则会让源表无限增长，最终写满实例磁盘——超过强制清理阈值就不再
        # 等追平，宁可丢这段日志也要保住实例。
        forced = False
        if not caught_up:
            if not self._force_truncate_due():
                return last_slot, watermark_epoch_us
            forced = True
        now_local = datetime.now()
        if self.config.truncate_min_interval_s <= 0 and (
            now_local.hour != self.config.truncate_hour
        ):
            return last_slot, watermark_epoch_us
        slot = self._truncate_slot(now_local)
        if last_slot == slot and not forced:
            return last_slot, watermark_epoch_us
        # 收尾补采：把「本轮读取完成 → 现在」这段时间里新写入的记录先收掉，
        # 否则 _ingest_batch 落盘与归档的这几秒会被 TRUNCATE 一并删除。补采
        # 又满批说明源端仍在积压，本次不清，等追平(强制清理时不适用)。
        watermark_epoch_us, _, tail_rows = self._poll_once(watermark_epoch_us)
        if tail_rows >= _BATCH_ROW_LIMIT and not forced:
            return last_slot, watermark_epoch_us
        connection = self._connect(
            self.config.truncate_user, self.config.truncate_password
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(_TRUNCATE_SQL)
        finally:
            connection.close()
        self._last_truncate_monotonic = time.monotonic()
        if forced:
            LOGGER.warning(
                "mysql.general_log 强制 TRUNCATE(超过 %s 秒未清、采集未追平)："
                "这段未采集的日志已丢弃，instance=%s",
                self.config.force_truncate_after_s,
                self.config.instance_id,
            )
            self._set_status(
                lastTruncateUtc=utc_now_text(),
                lastForcedTruncateUtc=utc_now_text(),
                lastError=(
                    "采集速度跟不上源端写入，已强制清理 general_log("
                    "该时段日志不完整)"
                ),
            )
        else:
            LOGGER.info(
                "mysql.general_log 已 TRUNCATE(槽 %s, 收尾补采 %s 行)",
                slot,
                tail_rows,
            )
            self._set_status(lastTruncateUtc=utc_now_text())
        self._save_watermark(watermark_epoch_us, slot)
        return slot, watermark_epoch_us

    def _force_truncate_due(self) -> bool:
        limit = self.config.force_truncate_after_s
        if limit <= 0:
            return False
        return time.monotonic() - self._last_truncate_monotonic >= limit
