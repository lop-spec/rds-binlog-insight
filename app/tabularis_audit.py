from __future__ import annotations

import hmac
import os
import json
import logging
import threading
from .audit_redaction import redact_secrets

LOGGER = logging.getLogger(__name__)
from datetime import UTC, datetime
from typing import Any

from .config import Settings
from .metadata import MetadataStore
from .rds_api import RemoteBinlog
from .storage import EventStorage

# 投递方必须用 RDS 实例 ID，与 binlog / general log 两条链路同一口径。允许列表
# 来自运行时配置，避免把任何部署环境的实例标识写入镜像或源代码。
_ALLOWED_STATUSES = frozenset(
    {"success", "failed", "cancelled", "skipped", "unknown"}
)
_MAX_EVENTS = 1000
_MAX_SQL_LENGTH = 1_000_000
_MAX_TEXT_LENGTH = 4000


class AuditIngestError(ValueError):
    def __init__(self, message: str, code: str = "AUDIT_INVALID_PAYLOAD") -> None:
        super().__init__(message)
        self.code = code


class TabularisAuditIngest:
    def __init__(
        self,
        metadata: MetadataStore,
        storage: EventStorage,
        *,
        archiver: Any,
        token: str | None = None,
        oss_prefix: str | None = None,
        allowed_instances: Any | None = None,
    ) -> None:
        self.metadata = metadata
        self.storage = storage
        self.archiver = archiver
        self.token = (
            token
            if token is not None
            else os.environ.get("RDS_BINLOG_TABULARIS_AUDIT_TOKEN", "")
        ).strip()
        prefix = (
            oss_prefix
            if oss_prefix is not None
            else os.environ.get(
                "RDS_BINLOG_TABULARIS_AUDIT_OSS_PREFIX",
                "tabularis-audit/",
            )
        )
        self.oss_prefix = self._validate_prefix(prefix)
        if allowed_instances is None:
            configured = {
                item.strip()
                for item in os.environ.get(
                    "RDS_BINLOG_TABULARIS_AUDIT_ALLOWED_INSTANCES", ""
                ).split(",")
                if item.strip()
            }
            if not configured:
                settings = metadata.load_settings()
                configured.update(
                    value
                    for value in (
                        str(settings.db_instance_id or "").strip(),
                        os.environ.get("RDS_BINLOG_GLOG_INSTANCE_ID", "").strip(),
                    )
                    if value
                )
        elif isinstance(allowed_instances, str):
            configured = {
                item.strip() for item in allowed_instances.split(",") if item.strip()
            }
        else:
            configured = {str(item).strip() for item in allowed_instances if str(item).strip()}
        self.allowed_instances = frozenset(configured)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._archive_loop, name='audit-archive', daemon=True)
            self._thread.start()

    def shutdown(self) -> None:
        self._stop.set()
        self._wake.set()

    def status(self) -> dict[str, Any]:
        return self.metadata.audit_delivery_status()

    def _archive_loop(self) -> None:
        while not self._stop.is_set():
            try:
                for record in self.metadata.pending_audit_events(limit=20):
                    if self._stop.is_set():
                        break
                    event = self.metadata.local_execution_event_detail(record['event_id'])
                    if event is None:
                        continue
                    source_id = record['source_file_id']
                    if not self.metadata.parts_for_file(source_id):
                        self._store_new_event(event, source_id)
                    self.metadata.set_tabularis_audit_state([record['event_id']], 'stored')
                    self._archive_pending(record['event_id'], source_id)
            except Exception:
                LOGGER.exception('Audit background archive failed; durable index remains retryable')
            self._wake.wait(30)
            self._wake.clear()

    @staticmethod
    def _validate_prefix(value: str) -> str:
        prefix = str(value or "").strip().lstrip("/")
        if not prefix or ".." in prefix.split("/") or "//" in prefix:
            raise AuditIngestError("Tabularis audit OSS prefix is invalid")
        return prefix.rstrip("/") + "/"

    def authorized(self, authorization: str) -> bool:
        if not self.token:
            return False
        scheme, separator, value = str(authorization or "").partition(" ")
        return bool(
            separator
            and scheme.lower() == "bearer"
            and hmac.compare_digest(value.strip(), self.token)
        )

    @staticmethod
    def _text(event: dict[str, Any], key: str, maximum: int) -> str:
        value = str(event.get(key) or "")
        if len(value) > maximum:
            raise AuditIngestError(f"{key} exceeds the maximum length")
        return value

    def _validate_event(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise AuditIngestError("Each audit event must be an object")
        event_id = self._text(value, "event_id", 256).strip()
        if not event_id:
            raise AuditIngestError("event_id is required")
        instance_id = self._text(value, "instance_id", 64).strip()
        if instance_id not in self.allowed_instances:
            raise AuditIngestError("instance_id is not allowed")
        status = self._text(value, "execution_status", 32).strip().lower()
        if status not in _ALLOWED_STATUSES:
            raise AuditIngestError("execution_status is not allowed")
        started_epoch_us = int(value.get("started_epoch_us") or 0)
        finished_epoch_us = int(value.get("finished_epoch_us") or 0)
        if started_epoch_us <= 0 or finished_epoch_us < started_epoch_us:
            raise AuditIngestError("Audit event time range is invalid")
        execution_time_ms = max(int(value.get("execution_time_ms") or 0), 0)
        operation = self._text(value, "operation", 32).strip().upper() or "OTHER"
        sql_text, _ = redact_secrets(self._text(value, "sql_text", _MAX_SQL_LENGTH))
        return {
            "event_id": event_id,
            "event_epoch_us": finished_epoch_us,
            "raw_event_type": "TABULARIS_AUDIT",
            "operation": operation,
            "database_name": self._text(value, "database_name", 256),
            "table_name": self._text(value, "table_name", 256),
            "transaction_id": self._text(value, "transaction_context_id", 256),
            "execution_time_ms": execution_time_ms,
            "error_code": int(value.get('error_code') or (1 if status == 'failed' else 0)),
            "sql_kind": self._text(value, "sql_kind", 32) or operation,
            "sql_text": sql_text,
            "connection_id": self._text(value, "connection_id", 256),
            "connection_name": self._text(value, "connection_name", 256),
            "database_account": self._text(value, "database_account", 256),
            "execution_status": status,
            "error_message": redact_secrets(self._text(value, "error_message", _MAX_TEXT_LENGTH))[0],
            "affected_rows": max(int(value.get("affected_rows") or 0), 0),
            "started_epoch_us": started_epoch_us,
            "finished_epoch_us": finished_epoch_us,
            "batch_id": self._text(value, "batch_id", 256),
            "statement_index": int(
                value.get("statement_index")
                if value.get("statement_index") is not None
                else -1
            ),
            "transaction_context_id": self._text(
                value, "transaction_context_id", 256
            ),
            "instance_id": instance_id,
        }

    @staticmethod
    def _source(event: dict[str, Any]) -> tuple[Settings, RemoteBinlog, str]:
        epoch_us = int(event["event_epoch_us"])
        instant = datetime.fromtimestamp(epoch_us / 1_000_000, UTC)
        timestamp = instant.strftime("%Y-%m-%dT%H:%M:%SZ")
        settings = Settings(db_instance_id=str(event["instance_id"]), auto_sync=False)
        item = RemoteBinlog(
            log_file_name=f"tabularis-audit-{event['event_id']}.ndjson",
            log_begin_utc=timestamp,
            log_end_utc=timestamp,
            file_size=len(str(event.get("sql_text") or "").encode("utf-8")),
            checksum_crc64="",
            download_link="",
            intranet_download_link="",
            link_expired_utc="",
            remote_status="Completed",
            host_instance_id="tabularis",
        )
        return settings, item, MetadataStore.file_id(settings.db_instance_id, item)

    def _ensure_source_file(self, event: dict[str, Any], source_file_id: str) -> None:
        settings, item, expected_id = self._source(event)
        if expected_id != source_file_id:
            raise RuntimeError("Tabularis audit source identity mismatch")
        actual_id, _state = self.metadata.upsert_remote(settings, item)
        if actual_id != source_file_id:
            raise RuntimeError("Tabularis audit source identity mismatch")
        self.metadata.set_file_visibility(source_file_id, True)

    def _store_new_event(self, event: dict[str, Any], source_file_id: str) -> list[dict[str, Any]]:
        self._ensure_source_file(event, source_file_id)
        self.metadata.set_file_state(source_file_id, "parsing")
        count, parts = self.storage.ingest_file(
            file_id=source_file_id,
            instance_id=str(event["instance_id"]),
            host_instance_id="tabularis",
            source_file_name=f"tabularis-audit-{event['event_id']}.ndjson",
            events=[event],
        )
        if count != 1 or not parts:
            raise RuntimeError("Tabularis audit event was not stored")
        self.metadata.set_file_state(source_file_id, "done", event_count=count)
        return parts

    def _archive_pending(self, event_id: str, source_file_id: str) -> bool:
        settings = self.metadata.load_settings()
        parts = self.metadata.parts_for_file(source_file_id)
        if not parts:
            return False
        if not settings.oss_enabled:
            return False
        self.archiver.archive_parts_now(
            parts,
            self.oss_prefix,
            event_code="OSS_TABULARIS_AUDIT_ARCHIVED",
        )
        if not self.metadata.file_archive_complete(source_file_id):
            return False
        self.metadata.set_tabularis_audit_state([event_id], "archived")
        return True

    def ingest(self, payload: dict[str, Any]) -> dict[str, Any]:
        events_value = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events_value, list) or not events_value:
            raise AuditIngestError("events must be a non-empty array")
        if len(events_value) > _MAX_EVENTS:
            raise AuditIngestError("Too many audit events")
        events, rejected = [], []
        for index, value in enumerate(events_value):
            try:
                events.append(self._validate_event(value))
            except (AuditIngestError, ValueError, TypeError, OverflowError) as exc:
                rejected.append({'index': index, 'event_id': str(value.get('event_id') or '')[:256] if isinstance(value, dict) else '',
                                 'code': getattr(exc, 'code', 'AUDIT_INVALID_PAYLOAD')})
        acknowledged, accepted, duplicates = [], 0, 0
        # Record and conflict-check under one ingest lock. The DB itself also
        # enforces INSERT-only semantics, including backfill callers.
        with self._lock:
            for event in events:
                event_id = event['event_id']
                existing = self.metadata.local_execution_event_detail(event_id)
                if existing is not None and any(existing.get(key) != event.get(key) for key in self.metadata.TABULARIS_AUDIT_LOG_COLUMNS if key != 'source_file_name'):
                    rejected.append({'event_id': event_id, 'code': 'AUDIT_ID_CONFLICT'})
                    LOGGER.warning('Audit immutable identity conflict')
                    continue
                if existing is None:
                    event['source_file_name'] = f'tabularis-audit-{event_id}.ndjson'
                    self.metadata.record_tabularis_audit_log([event])
                    accepted += 1
                else:
                    duplicates += 1
                self.metadata.claim_tabularis_audit_events([(event_id, self._source(event)[2])])
                acknowledged.append(event_id)
        self._wake.set()
        return {'accepted': accepted, 'duplicates': duplicates,
                'acknowledged': not rejected, 'acknowledged_event_ids': list(dict.fromkeys(acknowledged)),
                'rejected': rejected, 'acknowledgement_scope': 'durable_local_index',
                'archive_async': True}

    def acknowledged(self, event_ids: list[str]) -> list[str]:
        normalized = [str(value) for value in event_ids if str(value)]
        return self.metadata.acknowledged_tabularis_audit_events(normalized)
