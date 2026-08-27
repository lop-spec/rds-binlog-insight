from __future__ import annotations

import argparse
import faulthandler
import json
import logging
import logging.handlers
import mimetypes
import os
import signal
import shutil
import sys
import threading
import urllib.parse
from datetime import UTC, date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from . import __version__
from .analytics_index import DEFAULT_SQL_ORDER, SQL_ORDERS
from .config import (
    APP_ID,
    APP_NAME,
    APP_VERSION,
    NODE_ID_RE,
    Settings,
    app_root,
    data_root,
    ensure_data_dirs,
    load_general_log_instances,
    load_slow_log_instances,
    load_secondary_instances,
    parse_settings_payload,
    utc_now_text,
)
from .credentials import (
    CloudCredential,
    credential_status,
    delete_credential,
    load_credential,
    save_credential,
)
from .general_log_collector import GeneralLogCollector, GeneralLogConfig
from .slow_log_collector import SlowLogCollector, SlowLogConfig
from .metadata import MetadataStore
from .pipeline import PipelineError, SyncManager
from .query_tasks import QueryTaskManager
from .rds_api import RdsRpcClient
from .schema_diff import SchemaDiffError, SchemaDiffService
from .storage import EventStorage
from .tabularis_audit import AuditIngestError, TabularisAuditIngest

LOGGER = logging.getLogger(__name__)
_LOCAL_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _normalize_host(value: str) -> str:
    return value.strip().lower().rstrip(".")


def _allowed_hosts(raw: str | None = None) -> frozenset[str]:
    configured = (
        os.environ.get("RDS_BINLOG_ALLOWED_HOSTS", "") if raw is None else raw
    )
    return frozenset(
        _LOCAL_ALLOWED_HOSTS
        | {
            normalized
            for item in configured.split(",")
            if (normalized := _normalize_host(item))
        }
    )


def _host_allowed(authority: str, allowed_hosts: frozenset[str]) -> bool:
    try:
        hostname = urllib.parse.urlsplit(f"//{authority}").hostname
    except ValueError:
        return False
    return bool(hostname and _normalize_host(hostname) in allowed_hosts)


def _origin_allowed(
    origin: str, allowed_hosts: frozenset[str], server_port: int
) -> bool:
    if not origin:
        return True
    parsed = urllib.parse.urlparse(origin)
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError:
        return False
    return bool(
        parsed.scheme == "http"
        and parsed.hostname
        and _normalize_host(parsed.hostname) in allowed_hosts
        and port == server_port
    )


def _json_default(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _query_value(query: dict[str, list[str]], key: str, default: str = "") -> str:
    values = query.get(key)
    return values[0] if values else default


def _event_query(query: dict[str, list[str]]) -> dict[str, Any]:
    operations = [
        item.strip().upper()
        for value in query.get("operation", [])
        for item in value.split(",")
        if item.strip()
    ]
    result: dict[str, Any] = {
        "keyword": _query_value(query, "keyword"),
        "keyword_mode": _query_value(query, "keywordMode", "AND"),
        # all（默认）/ audit（Tabularis 执行）/ database（除审计外的全部）
        # / slowlog（只看慢日志）/ binlog（只看 binlog 与 general log）
        "source": _query_value(query, "source"),
        # 事务钻取：GTID / XID / transaction_id 精确匹配，供「最长事务」等榜单点开看经过
        "transaction": _query_value(query, "transaction"),
        "fingerprint": _query_value(query, "fingerprint"),
        "instance": _query_value(query, "instance"),
        "database": _query_value(query, "database"),
        "table": _query_value(query, "table"),
        "connection": _query_value(query, "connection"),
        "account": _query_value(query, "account"),
        "status": _query_value(query, "status"),
        "operations": operations,
        "limit": int(_query_value(query, "limit", "100")),
        "offset": int(_query_value(query, "offset", "0")),
    }
    for source, target in (
        ("startEpochUs", "start_epoch_us"),
        ("endEpochUs", "end_epoch_us"),
    ):
        value = _query_value(query, source)
        if value:
            result[target] = int(value)
    return result


def _analytics_query(query: dict[str, list[str]]) -> dict[str, Any]:
    """分析请求参数。

    `scan` 控制未覆盖分区的即时扫描兜底数量。它有上界：单次请求不允许把整个
    保留窗口都拉下来扫描，否则会挤占同容器内的同步与查询。
    """

    scan_value = _query_value(query, "scan", "auto")
    node_id = _query_value(query, "nodeId").strip()
    if node_id and not NODE_ID_RE.fullmatch(node_id):
        raise ValueError("Node ID 格式无效")
    result: dict[str, Any] = {
        "source": _query_value(query, "source", "binlog").strip().lower(),
        "instance": _query_value(query, "instance"),
        "node_id": node_id,
        "database": _query_value(query, "database"),
        "table": _query_value(query, "table"),
        "connection": _query_value(query, "connection"),
        "account": _query_value(query, "account"),
        "status": _query_value(query, "status"),
        "operation": _query_value(query, "operation").upper(),
        "limit": int(_query_value(query, "limit", "50")),
        # auto（默认）= 由服务端按缺口大小自行决定即时补建数量；
        # 显式数字仅保留给 API 调用方兼容。
        "scan_limit": -1 if scan_value == "auto" else int(scan_value),
        "order": _query_value(query, "order", DEFAULT_SQL_ORDER),
    }
    if result["order"] not in SQL_ORDERS:
        raise ValueError("order 取值不受支持")
    if result["source"] not in {"", "binlog", "database", "slowlog"}:
        raise ValueError("source 取值不受支持")
    if not 1 <= result["limit"] <= 500:
        raise ValueError("limit 必须在 1 到 500 之间")
    if not -1 <= result["scan_limit"] <= 32:
        raise ValueError("scan 必须是 auto 或 0 到 32 之间")
    for source, target in (
        ("startEpochUs", "start_epoch_us"),
        ("endEpochUs", "end_epoch_us"),
    ):
        value = _query_value(query, source)
        if value:
            result[target] = int(value)
    return result


def _event_query_payload(payload: dict[str, Any]) -> dict[str, Any]:
    operations_value = payload.get("operations", payload.get("operation", []))
    if isinstance(operations_value, str):
        operations = [
            item.strip().upper()
            for item in operations_value.split(",")
            if item.strip()
        ]
    elif isinstance(operations_value, list):
        operations = [
            str(item).strip().upper()
            for item in operations_value
            if str(item).strip()
        ]
    else:
        raise ValueError("operations 必须是字符串或数组")
    result: dict[str, Any] = {
        "keyword": str(payload.get("keyword") or "").strip(),
        "keyword_mode": str(
            payload.get("keyword_mode", payload.get("keywordMode", "AND"))
            or "AND"
        ).upper(),
        "source": str(payload.get("source") or "").strip(),
        "transaction": str(payload.get("transaction") or "").strip(),
        "fingerprint": str(payload.get("fingerprint") or "").strip(),
        "instance": str(payload.get("instance") or "").strip(),
        "database": str(payload.get("database") or "").strip(),
        "table": str(payload.get("table") or "").strip(),
        "connection": str(payload.get("connection") or "").strip(),
        "account": str(payload.get("account") or "").strip(),
        "status": str(payload.get("status") or "").strip(),
        "operations": operations,
        "limit": int(payload.get("limit") or 100),
        "offset": int(payload.get("offset") or 0),
    }
    if result["keyword_mode"] not in {"AND", "OR"}:
        raise ValueError("keywordMode 只能是 AND 或 OR")
    for source, target in (
        ("startEpochUs", "start_epoch_us"),
        ("start_epoch_us", "start_epoch_us"),
        ("endEpochUs", "end_epoch_us"),
        ("end_epoch_us", "end_epoch_us"),
    ):
        value = payload.get(source)
        if value not in {None, ""}:
            result[target] = int(value)
    if not 1 <= result["limit"] <= 1000:
        raise ValueError("limit 必须在 1 到 1000 之间")
    if not 0 <= result["offset"] <= 100_000:
        raise ValueError("offset 必须在 0 到 100000 之间")
    exact_value = payload.get("exact")
    if exact_value is not None:
        if not isinstance(exact_value, dict):
            raise ValueError("exact 必须是对象")
        kind = str(exact_value.get("kind") or "").strip().upper()
        value = exact_value.get("value")
        fallback = str(exact_value.get("fallback") or "error").strip().lower()
        if kind != "PRIMARY_KEY":
            raise ValueError("exact.kind 目前只支持 PRIMARY_KEY")
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValueError("exact.value 不能为空")
        if fallback not in {"scan", "error"}:
            raise ValueError("exact.fallback 只能是 scan 或 error")
        if not result["database"] or not result["table"]:
            raise ValueError("主键精确查询必须指定完整数据库名和表名")
        if result["keyword"]:
            raise ValueError("主键精确查询不能同时使用关键词")
        result["exact"] = {
            "kind": kind,
            "value": value.strip() if isinstance(value, str) else value,
            "fallback": fallback,
        }
    return result


class Application:
    def __init__(self, root: Path | None = None):
        self.data_dir = root or data_root()
        self.paths = ensure_data_dirs(self.data_dir)
        self.metadata = MetadataStore(
            self.data_dir / "metadata.sqlite3",
            run_migrations=False,
        )
        # Do not leak a root file handler when the explicit metadata migration
        # has not run yet.  The supervisor still reports that startup error on
        # stderr; the rotating application log begins after schema validation.
        self._configure_logging()
        self.storage = EventStorage(self.metadata, self.data_dir)
        # The status page is backed by a last-good snapshot. Build it off the
        # startup critical path so transient SQLite or disk contention cannot
        # delay either service readiness or the first /api/storage request.
        self.storage.start_storage_stats_snapshot_refresh()
        client_factory = None
        credential_loader = load_credential
        if os.environ.get("RDS_BINLOG_TEST_MODE") == "1":
            manifest = os.environ.get("RDS_BINLOG_MOCK_MANIFEST", "").strip()
            if manifest:
                from .mock_api import ManifestRdsClient

                client_factory = lambda _settings, _credential: ManifestRdsClient(
                    Path(manifest)
                )
                credential_loader = lambda _target: CloudCredential("test-ak", "test-secret")
        self.sync = SyncManager(
            self.metadata,
            self.storage,
            client_factory=client_factory,
            credential_loader=credential_loader,
        )
        self.queries = QueryTaskManager(
            self.metadata,
            self.storage,
            settings_loader=self.metadata.load_settings,
            archive_loader=self.sync.archive_for_settings,
        )
        self.audit_ingest = TabularisAuditIngest(
            self.metadata,
            self.storage,
            archiver=self.sync,
        )
        self.secondary_syncs: list[SyncManager] = self._start_secondary_syncs(
            client_factory, credential_loader
        )
        self.general_logs = self._start_general_log_collectors()
        # 兼容既有调用与状态字段：第一个采集器仍作为 general_log 暴露。
        self.general_log = self.general_logs[0]
        self.slow_logs = self._start_slow_log_collectors(credential_loader)
        try:
            self.schema_diff = SchemaDiffService(name_resolver=self._rds_instance_name)
        except SchemaDiffError as exc:
            LOGGER.warning("结构对比配置无效，该功能已禁用：%s", exc)
            self.schema_diff = SchemaDiffService([])
        self.httpd: ThreadingHTTPServer | None = None

    def _start_secondary_syncs(
        self,
        client_factory: Any,
        credential_loader: Any,
    ) -> list[SyncManager]:
        """为 data/binlog-instances.json 里的每个附加实例起一个同步器。

        配置文件不存在时返回空列表，运行形态与单实例时期完全一致。附加实例的
        Settings 每次现算(继承当前主设置)，这样在服务设置页改了 OSS 或凭据后，
        附加实例会跟着变，不需要在两个地方各配一遍。
        """

        try:
            configured = load_secondary_instances(self.data_dir)
        except Exception as exc:  # noqa: BLE001 - 附加配置异常不能挡住主实例启动
            LOGGER.warning("附加 binlog 实例配置读取失败，已忽略：%s", exc)
            return []
        if not configured:
            return []
        primary_id = str(self.metadata.load_settings().db_instance_id or "").strip()
        managers: list[SyncManager] = []
        for item in configured:
            if item.instance_id == primary_id:
                LOGGER.warning(
                    "附加实例 %s 与主实例相同，已跳过", item.instance_id
                )
                continue
            try:
                item.resolve(self.metadata.load_settings())
            except ValueError as exc:
                LOGGER.warning("附加实例 %s 配置无效：%s", item.instance_id, exc)
                continue
            managers.append(
                SyncManager(
                    self.metadata,
                    self.storage,
                    client_factory=client_factory,
                    credential_loader=credential_loader,
                    settings_loader=(
                        lambda entry=item: entry.resolve(
                            self.metadata.load_settings()
                        )
                    ),
                    role="secondary",
                    scope_instance_id=item.instance_id,
                    display_name=item.display_name(),
                )
            )
            LOGGER.info(
                "附加 binlog 实例已启用：%s(%s)",
                item.display_name(),
                item.instance_id,
            )
        return managers

    def _start_general_log_collectors(self) -> list[GeneralLogCollector]:
        """起 general log 采集器：环境变量那一份 + JSON 里配置的其余实例。

        JSON 文件不存在时只有环境变量那一个，与本功能上线前一致。任一实例配置
        坏掉只跳过它自己，不影响其他实例。
        """

        collectors = [
            GeneralLogCollector(self.metadata, self.storage, archiver=self.sync)
        ]
        try:
            extra = load_general_log_instances(self.data_dir)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("general log 多实例配置读取失败，已忽略：%s", exc)
            extra = []
        env_instance = collectors[0].config.instance_id
        for item in extra:
            instance_id = str(item.get("instanceId") or "")
            if instance_id == env_instance:
                LOGGER.warning(
                    "general log 实例 %s 已由环境变量配置，JSON 中的同名项已跳过",
                    instance_id,
                )
                continue
            try:
                config = GeneralLogConfig(item)
            except (TypeError, ValueError) as exc:
                LOGGER.warning("general log 实例 %s 配置无效：%s", instance_id, exc)
                continue
            collectors.append(
                GeneralLogCollector(
                    self.metadata, self.storage, config=config, archiver=self.sync
                )
            )
            LOGGER.info(
                "general log 实例已登记：%s enabled=%s 采样上限=%s 分钟",
                instance_id,
                config.enabled,
                config.sample_max_minutes or "无",
            )
        for collector in collectors:
            collector.start()
        return collectors

    def _start_slow_log_collectors(
        self, credential_loader: Any
    ) -> list[SlowLogCollector]:
        """按 data/slow-log-instances.json 起慢日志采集器。

        文件不存在时返回空列表，与本功能上线前完全一致；任一实例配置坏掉只跳过
        它自己。凭据复用 binlog 同步的 AccessKey，配置文件里不含密码。
        """

        try:
            configured = load_slow_log_instances(self.data_dir)
        except Exception as exc:  # noqa: BLE001 - 附加配置异常不能挡住主实例启动
            LOGGER.warning("慢日志实例配置读取失败，已忽略：%s", exc)
            return []
        collectors: list[SlowLogCollector] = []
        for entry in configured:
            instance_id = str(entry.get("instanceId") or "")
            try:
                config = SlowLogConfig(entry)
            except (TypeError, ValueError) as exc:
                LOGGER.warning("慢日志实例 %s 配置无效：%s", instance_id, exc)
                continue
            collector = SlowLogCollector(
                self.metadata,
                self.storage,
                config=config,
                credential_loader=credential_loader,
                archiver=self.sync,
            )
            collectors.append(collector)
            LOGGER.info(
                "慢日志实例已登记：%s node=%s enabled=%s poll=%ss lag=%ss",
                instance_id,
                config.node_id or "(默认)",
                config.enabled,
                config.poll_seconds,
                config.lag_seconds,
            )
        for collector in collectors:
            collector.start()
        return collectors

    def secondary_sync(self, instance_id: str) -> SyncManager | None:
        target = str(instance_id or "").strip()
        for manager in self.secondary_syncs:
            if manager.scope_instance_id == target:
                return manager
        return None

    def _rds_instance_name(self, instance_id: str) -> str | None:
        """取 RDS 控制台里的实例名称(DBInstanceDescription)作为界面显示名。

        取不到时返回 None，由 SchemaDiffService 回退到配置里的名字。
        """
        settings = self.metadata.load_settings()
        credential = load_credential(settings.credential_target)
        if credential is None:
            return None
        payload = RdsRpcClient(settings, credential, timeout=15).call(
            "DescribeDBInstanceAttribute", {"DBInstanceId": instance_id}
        )
        items = payload.get("Items", {}).get("DBInstanceAttribute", [])
        if isinstance(items, dict):
            items = [items]
        if not items:
            return None
        return str(items[0].get("DBInstanceDescription") or "").strip() or None

    def _configure_logging(self) -> None:
        log_path = self.paths["logs"] / "app.log"
        handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        already_configured = any(
            isinstance(item, logging.handlers.RotatingFileHandler)
            and getattr(item, "baseFilename", "") == str(log_path.resolve())
            for item in root.handlers
        )
        if already_configured:
            handler.close()
        else:
            root.addHandler(handler)

    def public_status(self) -> dict[str, Any]:
        settings = self.metadata.load_settings()
        stats = self.metadata.storage_metadata_stats()
        sync_status = self.sync.status()
        index_stats = sync_status.get("index") or {}
        catalog_stats = index_stats.get("catalog") or {}
        part_count = int(stats.get("part_count") or 0)
        indexed_parts = int(
            index_stats.get("part_count")
            or index_stats.get("indexedParts")
            or 0
        )
        structural_indexed_parts = int(
            index_stats.get("structural_part_count") or indexed_parts
        )
        cataloged_parts = int(
            catalog_stats.get("catalogedParts")
            or catalog_stats.get("cataloged_parts")
            or 0
        )
        local_body_bytes = int(index_stats.get("localBodyBytes") or 0)
        return {
            "app": APP_ID,
            "name": APP_NAME,
            "version": APP_VERSION,
            "timeUtc": utc_now_text(),
            "sync": sync_status,
            "credential": credential_status(settings.credential_target),
            "summary": {
                "eventCount": stats.get("event_count", 0),
                "parquetBytes": stats.get("parquet_bytes", 0),
                "localParquetBytes": local_body_bytes,
                "ossArchivedBytes": stats.get("archived_bytes", 0),
                "oldestEpochUs": stats.get("oldest_epoch_us"),
                "latestEpochUs": stats.get("latest_epoch_us"),
                "partCount": stats.get("part_count", 0),
                "retentionDays": settings.retention_days,
                "localBodyBytes": local_body_bytes,
                "indexBytes": index_stats.get("size_bytes", 0),
                "indexedParts": indexed_parts,
                "structuralIndexedParts": structural_indexed_parts,
                "indexBlocks": index_stats.get("block_count", 0),
                "indexCoverage": (
                    indexed_parts / part_count
                    if part_count
                    else 1.0
                ),
                "structuralIndexCoverage": (
                    structural_indexed_parts / part_count
                    if part_count
                    else 1.0
                ),
                "catalogedParts": cataloged_parts,
                "catalogCoverage": (
                    cataloged_parts / part_count
                    if part_count
                    else 1.0
                ),
                "ossRetentionDays": settings.oss_retention_days,
            },
            "generalLog": (
                self.general_log.status()
                if getattr(self, "general_log", None) is not None
                else {}
            ),
            "generalLogs": [
                item.status() for item in getattr(self, "general_logs", [])
            ],
            "slowLogs": [item.status() for item in getattr(self, "slow_logs", [])],
            "instances": (
                self.metadata.known_instances()
                if callable(getattr(self.metadata, "known_instances", None))
                else []
            ),
            "configured": bool(settings.db_instance_id),
            "primaryInstance": {"instanceId": settings.db_instance_id},
            "secondarySyncs": [
                {
                    "instanceId": manager.scope_instance_id,
                    "label": manager.display_name,
                    "sync": manager.status(),
                }
                for manager in getattr(self, "secondary_syncs", [])
            ],
        }

    def stop(self) -> None:
        for collector in self.general_logs:
            collector.shutdown()
        self.queries.shutdown()
        for manager in self.secondary_syncs:
            manager.shutdown()
        self.sync.shutdown()
        if self.httpd:
            threading.Thread(target=self.httpd.shutdown, daemon=True).start()


class AppHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], application: Application):
        self.application = application
        self.allowed_hosts = _allowed_hosts()
        super().__init__(address, RequestHandler)


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "RDSBinlogInsight/1.0"

    @property
    def app(self) -> Application:
        return self.server.application  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        # Liveness must not depend on Docker's log delivery path.  Under host
        # I/O saturation a blocking stdout logger can otherwise hold Python's
        # logging lock long enough for every supervisor probe to time out.
        if str(getattr(self, "path", "")).partition("?")[0] == "/healthz":
            return
        LOGGER.info("%s %s", self.address_string(), format % args)

    def _valid_host(self) -> bool:
        return _host_allowed(
            self.headers.get("Host", ""),
            self.server.allowed_hosts,  # type: ignore[attr-defined]
        )

    def _valid_origin(self) -> bool:
        return _origin_allowed(
            self.headers.get("Origin", ""),
            self.server.allowed_hosts,  # type: ignore[attr-defined]
            int(self.server.server_address[1]),  # type: ignore[attr-defined]
        )

    def _security_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        self.send_header("Cache-Control", "no-store")

    def _json(self, value: Any, status: int = 200) -> None:
        body = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_default,
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, code: str, message: str) -> None:
        try:
            self._json(
                {"ok": False, "error": {"code": code, "message": message}}, status
            )
        except (BrokenPipeError, ConnectionResetError):
            # Error handling must not emit a second socket exception after a
            # timed-out client has already closed the connection.
            return

    def _body_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("Content-Length 无效") from exc
        if length < 0 or length > 1024 * 1024:
            raise ValueError("请求体过大")
        raw = self.rfile.read(length)
        value = json.loads(raw.decode("utf-8")) if raw else {}
        if not isinstance(value, dict):
            raise ValueError("请求 JSON 必须是对象")
        return value

    def _serve_static(self, relative: str) -> None:
        static_root = (app_root() / "web").resolve()
        path = (static_root / relative).resolve()
        try:
            path.relative_to(static_root)
        except ValueError:
            self._error(404, "NOT_FOUND", "资源不存在")
            return
        if not path.is_file():
            self._error(404, "NOT_FOUND", "资源不存在")
            return
        body = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header(
            "Content-Type",
            content_type + ("; charset=utf-8" if content_type.startswith("text/") else ""),
        )
        self.send_header("Content-Length", str(len(body)))
        self._security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, download_name: str) -> None:
        if not path.is_file():
            self._error(404, "FILE_NOT_FOUND", "导出文件不存在")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header(
            "Content-Disposition",
            "attachment; filename*=UTF-8''"
            + urllib.parse.quote(download_name, safe=""),
        )
        self.send_header("Content-Length", str(path.stat().st_size))
        self._security_headers()
        self.end_headers()
        with path.open("rb") as handle:
            shutil.copyfileobj(handle, self.wfile, length=1024 * 1024)

    def do_GET(self) -> None:
        if not self._valid_host():
            self._error(421, "INVALID_HOST", "仅允许本机访问")
            return
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        try:
            if parsed.path in {"/", "/index.html"}:
                self._serve_static("index.html")
            elif parsed.path == "/assets/app.css":
                self._serve_static("app.css")
            elif parsed.path == "/assets/app.js":
                self._serve_static("app.js")
            elif parsed.path == "/favicon.svg":
                self._serve_static("favicon.svg")
            elif parsed.path == "/healthz":
                self._json(
                    {
                        "ok": True,
                        "app": APP_ID,
                        "version": __version__,
                        "pid": os.getpid(),
                    }
                )
            elif parsed.path == "/api/status":
                self._json({"ok": True, "data": self.app.public_status()})
            elif parsed.path == "/api/settings":
                settings = self.app.metadata.load_settings()
                self._json(
                    {
                        "ok": True,
                        "data": {
                            **settings.public_dict(),
                            "credential": credential_status(
                                settings.credential_target
                            ),
                        },
                    }
                )
            elif parsed.path == "/api/jobs":
                self._json(
                    {"ok": True, "data": self.app.metadata.jobs(limit=50)}
                )
            elif parsed.path == "/api/query-tasks":
                limit = int(_query_value(query, "limit", "50"))
                self._json(
                    {"ok": True, "data": self.app.queries.list(limit=limit)}
                )
            elif parsed.path == "/api/query-task":
                task_id = _query_value(query, "id")
                result = self.app.queries.get(task_id)
                if result is None:
                    self._error(404, "QUERY_TASK_NOT_FOUND", "查询任务不存在")
                else:
                    self._json({"ok": True, "data": result})
            elif parsed.path == "/api/events":
                settings = self.app.metadata.load_settings()
                event_query = _event_query(query)
                archive = None
                if not (
                    str(event_query.get("source") or "").lower() == "slowlog"
                    and self.app.storage.slowlog_query_coverage(
                        event_query, settings
                    ).get("complete")
                ):
                    archive = self.app.sync.archive_for_settings(settings)
                result = self.app.storage.query_events_tiered(
                    event_query,
                    settings,
                    archive,
                )
                self._json({"ok": True, "data": result})
            elif parsed.path == "/api/event":
                settings = self.app.metadata.load_settings()
                event_id = _query_value(query, "id")
                locator = _query_value(query, "locator")
                instance = _query_value(query, "instance")
                result = self.app.storage.local_execution_event_detail(
                    event_id, instance
                )
                if result is None:
                    result = self.app.storage.slowlog_event_detail(
                        event_id, settings, instance
                    )
                if result is None:
                    archive = self.app.sync.archive_for_settings(settings)
                    result = self.app.storage.event_detail_tiered(
                        event_id,
                        settings,
                        archive,
                        locator,
                        instance,
                    )
                if result is None:
                    self._error(404, "EVENT_NOT_FOUND", "事件不存在或已超过保留期")
                else:
                    self._json({"ok": True, "data": result})
            elif parsed.path == "/api/analytics":
                settings = self.app.metadata.load_settings()
                analytics_query = _analytics_query(query)
                archive = None
                if not (
                    str(analytics_query.get("source") or "").lower()
                    == "slowlog"
                    and self.app.storage.slowlog_query_coverage(
                        analytics_query, settings
                    ).get("complete")
                ):
                    archive = self.app.sync.archive_for_settings(settings)
                result = self.app.storage.analytics_summary(
                    analytics_query,
                    settings,
                    archive,
                    scan_limit=int(analytics_query.pop("scan_limit")),
                )
                self._json({"ok": True, "data": result})
            elif parsed.path == "/api/storage":
                settings = self.app.metadata.load_settings()
                self._json(
                    {
                        "ok": True,
                        "data": {
                            **self.app.storage.stats(settings.retention_days),
                            "oss_retention_days": settings.oss_retention_days,
                            "oss_enabled": settings.oss_enabled,
                        },
                    }
                )
            elif parsed.path == "/api/export":
                settings = self.app.metadata.load_settings()
                event_query = _event_query(query)
                archive = self.app.sync.archive_for_settings(settings)
                try:
                    path, count = self.app.storage.export_csv_tiered(
                        event_query,
                        settings,
                        archive,
                    )
                except Exception as exc:
                    if getattr(exc, "code", "") != "TIER_COVERAGE_MISSING":
                        raise
                    start_us, end_us = self.app.storage._query_window(
                        event_query,
                        settings.retention_days,
                    )
                    backfill = self.app.sync.request_backfill_for_range(
                        start_us,
                        end_us,
                    )
                    raise PipelineError(
                        backfill["message"] + "；解析完成后再导出",
                        "BINLOG_BACKFILL_QUEUED",
                    ) from exc
                self._send_file(path, f"binlog-events-{count}.csv")
            elif parsed.path == "/api/schema/instances":
                self._json(
                    {
                        "ok": True,
                        "data": {
                            "enabled": self.app.schema_diff.enabled,
                            "instances": self.app.schema_diff.instances(),
                            "defaultCompare": self.app.schema_diff.default_compare(),
                        },
                    }
                )
            elif parsed.path == "/api/schema/databases":
                instance = _query_value(query, "instance")
                self._json(
                    {"ok": True, "data": {"databases": self.app.schema_diff.databases(instance)}}
                )
            elif parsed.path == "/api/schema/tables":
                instance = _query_value(query, "instance")
                database = _query_value(query, "database")
                self._json(
                    {"ok": True, "data": {"tables": self.app.schema_diff.tables(instance, database)}}
                )
            else:
                self._error(404, "NOT_FOUND", "接口或资源不存在")
        except (BrokenPipeError, ConnectionResetError):
            # The client already went away. Sending a JSON error would only
            # raise a second socket exception and amplify log I/O pressure.
            return
        except SchemaDiffError as exc:
            self._error(400, "SCHEMA_DIFF_ERROR", str(exc))
        except (ValueError, TypeError) as exc:
            self._error(400, "INVALID_REQUEST", str(exc))
        except Exception as exc:
            LOGGER.exception("GET request failed")
            code = getattr(exc, "code", "INTERNAL_ERROR")
            status = (
                404
                if code == "BINLOG_RANGE_NOT_FOUND"
                else 422
                if code == "QUERY_END_AFTER_LATEST"
                else 409
                if code == "BINLOG_BACKFILL_QUEUED"
                else 503
                if code == "CLICKHOUSE_RAW_OSS_QUERY_UNAVAILABLE"
                else 500
            )
            self._error(status, code, str(exc))

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/ingest/tabularis-audit":
            if not self._valid_host():
                self._error(421, "INVALID_HOST", "请求 Host 不受信任")
                return
            audit_ingest = self.app.audit_ingest
            authorization = self.headers.get("Authorization", "")
            authorize = getattr(audit_ingest, "authorized", None)
            if not authorization or not callable(authorize) or not authorize(authorization):
                self._error(401, "AUDIT_AUTH_REQUIRED", "Tabularis audit authentication failed")
                return
            try:
                payload = self._body_json()
                result = audit_ingest.ingest(payload)
                self._json({"ok": True, "data": result}, 202)
            except (AuditIngestError, ValueError, TypeError) as exc:
                self._error(400, getattr(exc, "code", "AUDIT_INVALID_PAYLOAD"), str(exc))
            except Exception as exc:
                LOGGER.exception("Tabularis audit ingest failed")
                self._error(503, getattr(exc, "code", "AUDIT_INGEST_FAILED"), str(exc))
            return
        if not self._valid_host() or not self._valid_origin():
            self._error(403, "LOCAL_ORIGIN_REQUIRED", "仅允许本机同源请求")
            return
        try:
            payload = self._body_json()
            if parsed.path == "/api/settings":
                current = self.app.metadata.load_settings()
                updated = parse_settings_payload(payload, current)
                if payload.get("clearCredentials"):
                    delete_credential(current.credential_target)
                access_id = str(payload.get("accessKeyId") or "").strip()
                access_secret = str(payload.get("accessKeySecret") or "").strip()
                token = str(payload.get("securityToken") or "").strip()
                if access_id or access_secret or token:
                    save_credential(
                        updated.credential_target,
                        CloudCredential(access_id, access_secret, token),
                    )
                self.app.metadata.save_settings(updated)
                query_cache = self.app.storage.enforce_query_cache_limit(0)
                if query_cache["errors"]:
                    LOGGER.warning(
                        "Query cache limit applied with %d errors",
                        len(query_cache["errors"]),
                    )
                self._json(
                    {
                        "ok": True,
                        "data": {
                            **updated.public_dict(),
                            "credential": credential_status(
                                updated.credential_target
                            ),
                        },
                    }
                )
            elif parsed.path == "/api/settings/test":
                settings = self.app.metadata.load_settings()
                settings.validate(require_identity=True)
                credential = load_credential(settings.credential_target)
                if os.environ.get("RDS_BINLOG_TEST_MODE") == "1":
                    credential = credential or CloudCredential("test-ak", "test-secret")
                if not credential:
                    raise PipelineError("未找到阿里云凭据", "CREDENTIAL_MISSING")
                if os.environ.get("RDS_BINLOG_TEST_MODE") == "1" and os.environ.get(
                    "RDS_BINLOG_MOCK_MANIFEST"
                ):
                    from .mock_api import ManifestRdsClient

                    client = ManifestRdsClient(
                        Path(os.environ["RDS_BINLOG_MOCK_MANIFEST"])
                    )
                else:
                    client = RdsRpcClient(settings, credential)
                identity = client.verify_instance()
                primary_host_instance_id = client.primary_host_instance_id()
                now = datetime.now(UTC)
                logs = [
                    item
                    for item in client.list_binlogs(
                        (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    )
                    if item.host_instance_id == primary_host_instance_id
                ]
                oss_result = None
                if settings.oss_enabled:
                    archive = self.app.sync.archive_for_settings(settings)
                    if archive is None:
                        raise PipelineError("OSS 客户端未就绪", "OSS_DISABLED")
                    oss_result = archive.ensure_ready()
                self._json(
                    {
                        "ok": True,
                        "data": {
                            "identity": identity,
                            "primaryHostInstanceId": primary_host_instance_id,
                            "recentBinlogCount": len(logs),
                            "oss": oss_result,
                            "message": (
                                "实例身份、Binlog 列表、OSS 访问和生命周期均已验证"
                                if oss_result
                                else "实例身份和 Binlog 列表权限均已验证"
                            ),
                        },
                    }
                )
            elif parsed.path == "/api/sync/start":
                # instanceId 省略即主实例，与单实例时期一致。
                target_id = str(payload.get("instanceId") or "").strip()
                manager = self.app.sync
                if target_id:
                    found = self.app.secondary_sync(target_id)
                    if found is None:
                        primary_id = str(
                            self.app.metadata.load_settings().db_instance_id or ""
                        )
                        if target_id != primary_id:
                            raise ValueError(f"未配置该同步实例：{target_id}")
                    else:
                        manager = found
                job_id = manager.start(
                    reason="manual",
                    start_utc=str(payload.get("startTimeUtc") or ""),
                    end_utc=str(payload.get("endTimeUtc") or ""),
                )
                self._json({"ok": True, "data": {"jobId": job_id}}, 202)
            elif parsed.path == "/api/query-tasks":
                event_query = _event_query_payload(payload)
                task_id = self.app.queries.submit(event_query)
                self._json(
                    {
                        "ok": True,
                        "data": {
                            "taskId": task_id,
                            "status": "queued",
                            "message": "查询任务已创建",
                        },
                    },
                    202,
                )
            elif parsed.path == "/api/query-task/cancel":
                task_id = str(payload.get("id") or "").strip()
                if not task_id:
                    raise ValueError("缺少查询任务 ID")
                try:
                    result = self.app.queries.cancel(task_id)
                except KeyError:
                    self._error(404, "QUERY_TASK_NOT_FOUND", "查询任务不存在")
                else:
                    self._json({"ok": True, "data": result})
            elif parsed.path == "/api/sync/pause":
                pause_id = str(payload.get("instanceId") or "").strip()
                pause_target = self.app.secondary_sync(pause_id) if pause_id else None
                running = (pause_target or self.app.sync).request_pause()
                self._json(
                    {
                        "ok": True,
                        "data": {
                            "requested": running,
                            "message": (
                                "将在当前文件完成后暂停"
                                if running
                                else "当前没有运行中的任务"
                            ),
                        },
                    }
                )
            elif parsed.path == "/api/storage/cleanup":
                settings = self.app.metadata.load_settings()
                result = self.app.storage.cleanup(
                    settings.retention_days,
                    archive_enabled=settings.oss_enabled,
                )
                cache = self.app.storage.enforce_local_cache_limit(0)
                query_cache = self.app.storage.enforce_query_cache_limit(0)
                result["cache"] = cache
                result["query_cache"] = query_cache
                result["errors"].extend(cache["errors"])
                result["errors"].extend(query_cache["errors"])
                self._json({"ok": not bool(result["errors"]), "data": result})
            elif parsed.path == "/api/schema/diff":
                result = self.app.schema_diff.compare(
                    source_instance=str(payload.get("sourceInstance") or "").strip(),
                    source_database=str(payload.get("sourceDatabase") or "").strip(),
                    target_instance=str(payload.get("targetInstance") or "").strip(),
                    target_database=str(payload.get("targetDatabase") or "").strip(),
                    scope=str(payload.get("scope") or "all").strip(),
                    include_views=bool(payload.get("includeViews")),
                    generated_at=utc_now_text(),
                )
                self._json({"ok": True, "data": result})
            elif parsed.path == "/api/system/stop":
                self._json({"ok": True, "data": {"message": "后台服务正在停止"}})
                self.app.stop()
            else:
                self._error(404, "NOT_FOUND", "接口不存在")
        except SchemaDiffError as exc:
            self._error(400, "SCHEMA_DIFF_ERROR", str(exc))
        except (ValueError, TypeError) as exc:
            self._error(400, "INVALID_REQUEST", str(exc))
        except Exception as exc:
            code = getattr(exc, "code", "INTERNAL_ERROR")
            status = 409 if code in {"JOB_ALREADY_RUNNING"} else 400
            LOGGER.exception("POST request failed")
            self._error(status, code, str(exc))


def run_server(
    port: int,
    *,
    root: Path | None = None,
    host: str = "127.0.0.1",
) -> None:
    try:
        faulthandler.enable(file=sys.stderr, all_threads=True)
        diagnostic_signal = getattr(signal, "SIGUSR1", None)
        if diagnostic_signal is not None:
            faulthandler.register(
                diagnostic_signal,
                file=sys.stderr,
                all_threads=True,
                chain=False,
            )
    except (OSError, RuntimeError, ValueError):
        LOGGER.exception("Unable to enable service stack dumps")
    application = Application(root)
    httpd = AppHTTPServer((host, port), application)
    application.httpd = httpd
    interrupted = application.metadata.reconcile_interrupted_jobs()
    if interrupted:
        LOGGER.warning("Reconciled %d interrupted job(s)", interrupted)
    runtime = application.data_dir / "runtime.json"
    runtime.write_text(
        json.dumps(
            {
                "app": APP_ID,
                "pid": os.getpid(),
                "host": host,
                "port": port,
                "startedAt": utc_now_text(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    LOGGER.info("%s %s listening on %s:%s", APP_NAME, APP_VERSION, host, port)
    try:
        httpd.serve_forever(poll_interval=0.25)
    finally:
        application.queries.shutdown()
        application.sync.shutdown()
        httpd.server_close()
        try:
            current = json.loads(runtime.read_text(encoding="utf-8"))
            if int(current.get("pid") or 0) == os.getpid():
                runtime.unlink()
        except (OSError, ValueError, json.JSONDecodeError):
            pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", choices=("127.0.0.1", "0.0.0.0"), default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8769)
    parser.add_argument("--data-dir", type=Path)
    args = parser.parse_args()
    run_server(args.port, root=args.data_dir, host=args.host)


if __name__ == "__main__":
    main()
