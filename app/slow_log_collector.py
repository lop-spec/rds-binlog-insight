"""慢日志采集器：把 DAS 的 DescribeSlowLogRecords 增量灌入现有事件管道。

与 general log 的关系：两者都为了补齐 binlog 缺失的 SELECT，但 general log 的源表
没有索引、采集与清理成本随吞吐线性增长；慢日志由 RDS 自己存储和轮转，采集端只发
只读 API，覆盖面由 long_query_time 决定。

**走 DAS 不走 RDS**：两个产品有同名 Action，但 DAS 返回更完整的慢日志记录与字段，
包括 TableName、SqlType、AccountName、RowsExamined 和 ThreadId。

设计约束：
- 只用管控面只读接口，凭据复用 binlog 同步那一份 AccessKey，不需要任何数据库账号，
  也不在目标实例上执行任何语句。
- 采集批以"虚拟 binlog 文件"的形式复用 EventStorage.ingest_ndjson_file →
  finalize_file_parts → metadata 状态机，Parquet/索引/OSS 链路零改动。
- **水位线只推进到 now - lag_seconds**：慢日志落库有分钟级延迟，水位线贴着当前时间
  走会让晚到的记录永久落在窗口之外。lag 期内的重复由 event_id 去重兜底。
- **每轮必须翻完窗口内所有页**：DAS 的 OrderBy 只支持耗时/行数，没有按时间排序，
  拿不到"最后一条的时间"，所以水位线按窗口末尾推进，而不是按最大事件时间。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import ssl
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import NODE_ID_RE, Settings, utc_now_text
from .rds_api import RemoteBinlog, RdsRpcClient
from .sql_fingerprint import FINGERPRINT_FORMAT_VERSION, statement_profile

if TYPE_CHECKING:
    from .metadata import MetadataStore
    from .storage import EventStorage

LOGGER = logging.getLogger(__name__)

_API_ACTION = "DescribeSlowLogRecords"
_API_PAGE_SIZE = 100
# 单轮页数有硬上限；超出时保留水位并在下一轮继续。
_MAX_PAGES = 300
DAS_ENDPOINT = "https://das.cn-shanghai.aliyuncs.com"
_OPERATIONS = {
    "select": "SELECT",
    "insert": "INSERT",
    "update": "UPDATE",
    "delete": "DELETE",
    "replace": "REPLACE",
    "with": "SELECT",
    "call": "CALL",
    "alter": "DDL",
    "create": "DDL",
    "drop": "DDL",
    "truncate": "DDL",
    "rename": "DDL",
}


class DasRpcClient(RdsRpcClient):
    """DAS 的 RPC 客户端：签名与 RDS 相同，只换 Version 和响应判定。

    不能直接用 RdsRpcClient.call —— DAS **成功**时也会返回 Code/Message
    (Message="Successful")，那个实现见到 Code+Message 就抛错，实测三次调用全被
    误判成失败。这里改用 Success 字段判定。
    """

    VERSION = "2020-01-16"

    def call(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        signed = self._signed_params(action, params)
        url = self.endpoint + "/?" + urllib.parse.urlencode(
            signed, quote_via=urllib.parse.quote
        )
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "User-Agent": "RDS-Binlog-Insight/1.0",
            },
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout, context=ssl.create_default_context()
            ) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read()[:300].decode("utf-8", "replace")
            raise RuntimeError(f"DAS HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError(f"DAS 网络错误：{exc}") from exc
        result = json.loads(body.decode("utf-8"))
        if str(result.get("Success", "true")).lower() == "false":
            raise RuntimeError(f"DAS 调用失败：{result.get('Message')}")
        return result


def _operation_of(sql_text: str) -> str:
    head = sql_text.lstrip().lstrip("(").lstrip()
    if head.startswith("/*"):
        end = head.find("*/")
        if end >= 0:
            head = head[end + 2 :].lstrip()
    word = head.split(None, 1)[0].lower() if head.split(None, 1) else ""
    return _OPERATIONS.get(word, "OTHER")


def _epoch_us(record: dict[str, Any]) -> int:
    """优先用 Timestamp(毫秒)，退回 QueryStartTime(2026-08-18T05:10:49Z)。"""
    raw = record.get("Timestamp")
    try:
        millis = int(raw)
    except (TypeError, ValueError):
        millis = 0
    if millis > 0:
        return millis * 1000
    value = str(record.get("QueryStartTime") or "").strip()
    if not value:
        return 0
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return 0
    return int(parsed.timestamp() * 1_000_000)


class SlowLogConfig:
    """单实例的慢日志采集配置，来自 data/slow-log-instances.json 的一项。"""

    def __init__(self, overrides: dict[str, Any] | None = None) -> None:
        data = dict(overrides or {})
        self.instance_id = str(
            data.get("instanceId") or data.get("instance_id") or ""
        ).strip()
        self.node_id = str(data.get("nodeId") or data.get("node_id") or "").strip()
        default_label = (
            f"{self.instance_id} / {self.node_id}"
            if self.node_id
            else self.instance_id
        )
        self.label = str(data.get("label") or default_label).strip()
        self.enabled = bool(data.get("enabled", False))
        self.poll_seconds = self._int(data, "pollSeconds", 120, 30, 3600)
        # RDS 侧落库延迟裕量：水位线只推进到 now - lag，避免晚到记录被跳过。
        self.lag_seconds = self._int(data, "lagSeconds", 300, 60, 3600)
        # 首次启动回溯窗口，之后由水位线接管。
        self.initial_lookback_minutes = self._int(
            data, "initialLookbackMinutes", 60, 1, 7 * 24 * 60
        )
        # 单轮最多覆盖多长的时间窗，防止长时间停机后一轮拉爆。
        self.max_window_minutes = self._int(data, "maxWindowMinutes", 120, 5, 1440)
        exclude = str(data.get("excludeAccounts") or "").strip()
        self.exclude_accounts = tuple(
            item.strip().lower() for item in exclude.split(",") if item.strip()
        )
        default_scope = "/".join(
            part for part in (self.instance_id, self.node_id) if part
        )
        self.oss_prefix = str(data.get("ossPrefix") or "").strip() or (
            f"mysql-slow-log/{default_scope}/" if default_scope else ""
        )
        self.state_suffix = "-".join(
            part for part in (self.instance_id, self.node_id) if part
        )

    @staticmethod
    def _int(data: dict[str, Any], key: str, default: int, low: int, high: int) -> int:
        try:
            value = int(data.get(key, default))
        except (TypeError, ValueError):
            value = default
        return min(max(value, low), high)

    def validate(self) -> list[str]:
        problems: list[str] = []
        if not self.instance_id:
            problems.append("instanceId 为空")
        if self.node_id and not NODE_ID_RE.fullmatch(self.node_id):
            problems.append("nodeId 格式无效")
        if not self.oss_prefix:
            problems.append("ossPrefix 为空")
        return problems


class SlowLogCollector:
    """驻留线程：轮询 DescribeSlowLogRecords → NDJSON → 现有 ingest 链路。"""

    def __init__(
        self,
        metadata: "MetadataStore",
        storage: "EventStorage",
        config: SlowLogConfig,
        credential_loader: Any,
        archiver: Any | None = None,
    ) -> None:
        self.metadata = metadata
        self.storage = storage
        self.config = config
        self.credential_loader = credential_loader
        self.archiver = archiver
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_path = (
            storage.paths["root"] / f"slow_log_state-{config.state_suffix}.json"
        )
        # 只保留水位线附近的 event_id，用于跨轮去重（lag 窗口内会重复拉到）。
        self._seen_ids: set[str] = set()
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "enabled": config.enabled,
            "instanceId": config.instance_id,
            "nodeId": config.node_id,
            "label": config.label,
            "lastPollUtc": "",
            "lastError": "",
            "ingestedTotal": 0,
            "lastBatchRows": 0,
            "watermarkEpochUs": 0,
            "lastWindow": "",
            "lastQueuedParts": 0,
            "lastIndexedParts": 0,
            "lastIndexError": "",
        }

    # ---------------- 生命周期 ----------------

    def start(self) -> None:
        if not self.config.enabled:
            LOGGER.info(
                "慢日志采集未启用: instance=%s", self.config.instance_id or "(未配置)"
            )
            return
        problems = self.config.validate()
        if problems:
            LOGGER.error("慢日志采集配置不完整: %s", "; ".join(problems))
            self._set_status(lastError="; ".join(problems))
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"slow-log-collector-{self.config.state_suffix}",
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

    def _load_watermark(self) -> int:
        try:
            raw = json.loads(self._state_path.read_text("utf-8"))
            self._seen_ids = set(raw.get("recent_event_ids") or [])
            return int(raw.get("watermark_epoch_us") or 0)
        except (OSError, ValueError):
            return 0

    def _save_watermark(self, epoch_us: int) -> None:
        payload = json.dumps(
            {
                "watermark_epoch_us": int(epoch_us),
                # 只落最近一批的 id，文件不会无限增长。
                "recent_event_ids": sorted(self._seen_ids)[-20000:],
                "updated_utc": utc_now_text(),
            }
        )
        tmp = self._state_path.with_suffix(".json.tmp")
        tmp.write_text(payload, "utf-8")
        os.replace(tmp, self._state_path)

    # ---------------- 主循环 ----------------

    def _run(self) -> None:
        LOGGER.info(
            "慢日志采集启动: instance=%s node=%s poll=%ss lag=%ss",
            self.config.instance_id,
            self.config.node_id or "(默认)",
            self.config.poll_seconds,
            self.config.lag_seconds,
        )
        watermark = self._load_watermark()
        if watermark <= 0:
            watermark = int(
                (time.time() - self.config.initial_lookback_minutes * 60) * 1_000_000
            )
        self._set_status(watermarkEpochUs=watermark)
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                watermark, ingested = self._poll_once(watermark)
                if ingested:
                    self._save_watermark(watermark)
                self._set_status(
                    lastPollUtc=utc_now_text(),
                    lastError="",
                    watermarkEpochUs=watermark,
                )
            except Exception as exc:  # noqa: BLE001 - 驻留线程必须自愈
                LOGGER.exception("慢日志采集循环失败")
                self._set_status(lastError=f"{type(exc).__name__}: {exc}")
            elapsed = time.monotonic() - started
            self._stop.wait(max(self.config.poll_seconds - elapsed, 5.0))

    def _client(self) -> DasRpcClient:
        base = self.metadata.load_settings()
        credential = self.credential_loader(base.credential_target)
        if credential is None:
            raise RuntimeError(f"读不到凭据: target={base.credential_target}")
        settings = replace(
            base, db_instance_id=self.config.instance_id, endpoint=DAS_ENDPOINT
        )
        return DasRpcClient(settings, credential)

    def _request_params(
        self, start_ms: int, end_ms: int, page: int
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "InstanceId": self.config.instance_id,
            "StartTime": int(start_ms),
            "EndTime": int(end_ms),
            "PageSize": _API_PAGE_SIZE,
            "PageNumber": int(page),
        }
        if self.config.node_id:
            params["NodeId"] = self.config.node_id
        return params

    def _poll_once(self, watermark_epoch_us: int) -> tuple[int, int]:
        now = time.time()
        window_end = now - self.config.lag_seconds
        window_start = watermark_epoch_us / 1_000_000
        if window_end <= window_start:
            return watermark_epoch_us, 0
        limit = self.config.max_window_minutes * 60
        window_end = min(window_end, window_start + limit)
        start_ms = int(window_start * 1000)
        end_ms = int(window_end * 1000)
        self._set_status(
            lastWindow=(
                f"{datetime.fromtimestamp(window_start, UTC):%Y-%m-%dT%H:%M:%SZ} → "
                f"{datetime.fromtimestamp(window_end, UTC):%Y-%m-%dT%H:%M:%SZ}"
            )
        )

        client = self._client()
        records: list[dict[str, Any]] = []
        page = 1
        truncated = False
        while not self._stop.is_set():
            if page > _MAX_PAGES:
                truncated = True
                LOGGER.warning(
                    "慢日志单轮页数触顶(%s 页)，本窗口剩余记录留给下一轮：instance=%s",
                    _MAX_PAGES,
                    self.config.instance_id,
                )
                break
            response = client.call(
                _API_ACTION,
                self._request_params(start_ms, end_ms, page),
            )
            data = response.get("Data")
            if not isinstance(data, dict):
                data = response
            items = data.get("Logs") or []
            response_node_id = str(data.get("NodeId") or self.config.node_id).strip()
            for item in items:
                if not isinstance(item, dict):
                    continue
                record = dict(item)
                node_id = str(record.get("NodeId") or response_node_id).strip()
                if node_id:
                    record["NodeId"] = node_id
                records.append(record)
            total = int(data.get("TotalRecords") or 0)
            if len(items) < _API_PAGE_SIZE or len(records) >= total:
                break
            page += 1

        events = self._build_events(records)
        if events:
            self._ingest_batch(events)
            with self._status_lock:
                self._status["ingestedTotal"] += len(events)
                self._status["lastBatchRows"] = len(events)
        else:
            self._set_status(lastBatchRows=0)
        # DAS 的 OrderBy 不支持按时间排序，拿不到"最后一条的时间"，所以水位线按
        # 窗口末尾推进——本轮已经翻完窗口内所有页。页数触顶时不推进，下一轮重来。
        if truncated:
            return watermark_epoch_us, len(events)
        return int(window_end * 1_000_000), len(events)

    def _build_events(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        seen_now: set[str] = set()
        for index, record in enumerate(records):
            sql_text = str(record.get("SQLText") or "").strip()
            if not sql_text:
                continue
            epoch_us = _epoch_us(record)
            if epoch_us <= 0:
                continue
            account = str(record.get("AccountName") or "").strip()
            client_ip = str(record.get("HostAddress") or "").strip()
            node_id = str(record.get("NodeId") or self.config.node_id).strip()
            if account and account.lower() in self.config.exclude_accounts:
                continue
            sql_hash = str(record.get("SqlId") or "")
            digest = hashlib.sha256(
                "\x1f".join(
                    (
                        self.config.instance_id,
                        node_id,
                        str(epoch_us),
                        sql_hash,
                        client_ip,
                        str(record.get("ThreadId") or ""),
                        str(record.get("QueryTime") or 0),
                        sql_text[:512],
                    )
                ).encode("utf-8", "replace")
            ).hexdigest()
            event_id = f"slow-{digest[:40]}"
            if event_id in self._seen_ids or event_id in seen_now:
                continue
            seen_now.add(event_id)
            database = str(record.get("DBName") or "")
            prefix_parts = [part for part in (account, client_ip) if part]
            prefix = f"/* {'@'.join(prefix_parts)}" if prefix_parts else "/* slow-log"
            if database:
                prefix += f" db={database}"
            prefix += " */ "
            try:
                thread_id = int(record.get("ThreadId") or 0)
            except (TypeError, ValueError):
                thread_id = 0
            table_name = str(record.get("TableName") or "")
            operation = (
                _OPERATIONS.get(str(record.get("SqlType") or "").lower())
                or _operation_of(sql_text)
            )
            profile = statement_profile(
                sql_kind="ORIGINAL",
                sql_text=sql_text,
                row_query="",
                operation=operation,
                database=database,
                table=table_name,
            )
            events.append(
                {
                    "event_id": event_id,
                    "event_epoch_us": epoch_us,
                    "raw_event_type": "SLOW_LOG",
                    # SqlType 是 DAS 给的语句类型(select/delete/...)，比解析文本可靠；
                    # 缺失时才回退到按首词判断。
                    "operation": operation,
                    "database_name": database,
                    "table_name": table_name,
                    "table_map_id": 0,
                    "schema_version_id": "",
                    "server_id": 0,
                    "thread_id": thread_id,
                    "transaction_id": "",
                    "gtid": "",
                    "xid": "",
                    "start_position": 0,
                    "end_position": 0,
                    "row_index": 0,
                    "execution_time_ms": int(float(record.get("QueryTime") or 0)),
                    "error_code": 0,
                    # ORIGINAL：statement_profile 只在这个取值下把 sql_text 当
                    # 真实语句进入「全量 SQL」聚合；注释前缀会被 normalize 剥掉。
                    "sql_kind": "ORIGINAL",
                    "sql_text": prefix + sql_text,
                    "sql_bytes_base64": "",
                    "before_json": "",
                    "after_json": "",
                    # binlog 语境的列元数据，慢日志没有，用来承载它独有的指标。
                    "columns_json": json.dumps(
                        {
                            "rows_examined": int(record.get("RowsExamined") or 0),
                            "rows_sent": int(record.get("RowsSent") or 0),
                            "lock_time_ms": int(float(record.get("LockTime") or 0)),
                            "sql_id": sql_hash,
                            "node_id": node_id,
                            "statement_profile": {
                                "format_version": FINGERPRINT_FORMAT_VERSION,
                                **profile,
                            },
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    "row_query": "",
                    "header_epoch_us": epoch_us,
                    "commit_epoch_us": epoch_us,
                    "txn_last_committed": 0,
                    "txn_sequence_number": 0,
                    "txn_length_bytes": 0,
                    "connection_id": "",
                    "connection_name": client_ip,
                    "database_account": account,
                    "execution_status": "success",
                    "error_message": "",
                    "affected_rows": int(record.get("RowsSent") or 0),
                    "started_epoch_us": epoch_us,
                    "finished_epoch_us": epoch_us
                    + int(float(record.get("QueryTime") or 0)) * 1000,
                    "batch_id": "",
                    "statement_index": -1,
                    "transaction_context_id": "",
                }
            )
        if seen_now:
            # 只保留本轮见到的 id：水位线之前的窗口不会再被拉到，留着只会无限增长。
            self._seen_ids = seen_now
        return events

    def _ingest_batch(self, events: list[dict[str, Any]]) -> None:
        begin_us = min(int(item["event_epoch_us"]) for item in events)
        end_us = max(int(item["event_epoch_us"]) for item in events)

        def iso(epoch_us: int) -> str:
            return datetime.fromtimestamp(epoch_us / 1_000_000, UTC).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        stamp = f"{begin_us}-{end_us}"
        source_scope = "/".join(
            part for part in (self.config.instance_id, self.config.node_id) if part
        )
        log_file_name = f"slow-log/{source_scope}/{stamp}"
        staging = self.storage.paths["staging"]
        staging.mkdir(parents=True, exist_ok=True)
        body = "\n".join(
            json.dumps(item, ensure_ascii=False, separators=(",", ":"))
            for item in events
        )
        temporary = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=f"slowlog-{stamp}-",
            suffix=".ndjson",
            dir=staging,
            delete=False,
        )
        ndjson_path = Path(temporary.name)
        try:
            with temporary:
                temporary.write(body + "\n")
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
                host_instance_id="slow-log",
            )
            settings = self.metadata.load_settings()
            scoped = replace(settings, db_instance_id=self.config.instance_id)
            file_id, state = self.metadata.upsert_remote(scoped, item)
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
                host_instance_id="slow-log",
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
            queued_parts = self.storage.slowlog_index.enqueue_parts(parts)
            self._set_status(
                lastQueuedParts=queued_parts,
                # 建索引属于独立 indexer 进程；主服务只负责持久化和入队。
                lastIndexedParts=0,
                lastIndexError="",
            )
            if self.archiver is not None:
                try:
                    archived = self.archiver.archive_parts_now(
                        parts, self.config.oss_prefix
                    )
                    self._set_status(lastArchivedParts=archived)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("慢日志分区归档失败: %s", exc)
                    self._set_status(lastArchiveError=f"{type(exc).__name__}: {exc}")
            LOGGER.info(
                "慢日志批已入库: %s events=%s parts=%s",
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
