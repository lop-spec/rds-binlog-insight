from __future__ import annotations

import hashlib
import io
import logging
import os
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any, Callable

import oss2
from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_credentials.models import Config as CredentialConfig
from oss2.credentials import Credentials
from oss2.models import BucketLifecycle, LifecycleExpiration, LifecycleRule

from .config import Settings
from .credentials import CloudCredential

LOGGER = logging.getLogger(__name__)

OSS_PACK_TARGET_BYTES = 128 * 1024 * 1024

# 整桶生命周期规则的 read-modify-write 必须进程内串行，详见 ensure_lifecycle。
_LIFECYCLE_LOCK = threading.Lock()


class OssArchiveError(RuntimeError):
    def __init__(self, message: str, code: str = "OSS_ARCHIVE_ERROR"):
        super().__init__(message)
        self.code = code


class OssRangeReader(io.RawIOBase):
    """Seekable OSS reader with bounded read-ahead for Parquet's small reads."""

    def __init__(
        self,
        bucket: Any,
        key: str,
        size_bytes: int,
        expected_etag: str = "",
        base_offset: int = 0,
        read_ahead_bytes: int = 16 * 1024,
        cache_blocks: int = 8,
        fetch_attempts: int = 3,
        retry_delay_seconds: float = 0.05,
    ):
        super().__init__()
        self.bucket = bucket
        self.key = key
        self.size_bytes = max(int(size_bytes), 0)
        self.expected_etag = str(expected_etag or "").strip('"')
        self.base_offset = max(int(base_offset), 0)
        self.read_ahead_bytes = max(int(read_ahead_bytes), 1)
        self.cache_blocks = max(int(cache_blocks), 0)
        self.fetch_attempts = max(int(fetch_attempts), 1)
        self.retry_delay_seconds = max(float(retry_delay_seconds), 0.0)
        self.position = 0
        self.request_count = 0
        self.bytes_read = 0
        self._range_cache: OrderedDict[int, bytes] = OrderedDict()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            position = int(offset)
        elif whence == io.SEEK_CUR:
            position = self.position + int(offset)
        elif whence == io.SEEK_END:
            position = self.size_bytes + int(offset)
        else:
            raise ValueError("无效的 OSS Range seek 模式")
        if position < 0:
            raise ValueError("OSS Range seek 不能小于 0")
        self.position = min(position, self.size_bytes)
        return self.position

    def _fetch(self, logical_start: int, length: int) -> bytes:
        start = self.base_offset + logical_start
        end = start + length - 1
        last_error: Exception | None = None
        for attempt in range(self.fetch_attempts):
            try:
                result = self.bucket.get_object(
                    self.key,
                    byte_range=(start, end),
                )
                payload = result.read()
                break
            except Exception as exc:
                last_error = exc
                if attempt + 1 < self.fetch_attempts and self.retry_delay_seconds:
                    time.sleep(self.retry_delay_seconds * (2**attempt))
        else:
            raise OssArchiveError(
                f"OSS Range 读取失败：{self.key}",
                "OSS_RANGE_READ_FAILED",
            ) from last_error
        if len(payload) != length:
            raise OssArchiveError(
                f"OSS Range 长度不一致：{self.key}",
                "OSS_RANGE_VERIFY_FAILED",
            )
        actual_etag = str(
            getattr(result, "headers", {}).get("ETag", "")
            or getattr(result, "etag", "")
            or ""
        ).strip('"')
        if self.expected_etag and actual_etag and actual_etag != self.expected_etag:
            raise OssArchiveError(
                f"OSS Range ETag 不一致：{self.key}",
                "OSS_RANGE_VERIFY_FAILED",
            )
        self.request_count += 1
        self.bytes_read += length
        return payload

    def _cached(self, start: int, length: int) -> bytes | None:
        end = start + length
        hit: int | None = None
        for cache_start, payload in self._range_cache.items():
            if cache_start <= start and end <= cache_start + len(payload):
                hit = cache_start
                break
        if hit is None:
            return None
        payload = self._range_cache[hit]
        self._range_cache.move_to_end(hit)
        offset = start - hit
        return payload[offset : offset + length]

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("OSS Range reader 已关闭")
        remaining = self.size_bytes - self.position
        if remaining <= 0 or size == 0:
            return b""
        length = remaining if size is None or int(size) < 0 else min(int(size), remaining)
        start = self.position
        payload = self._cached(start, length)
        if payload is None and length < self.read_ahead_bytes:
            block = self.read_ahead_bytes
            fetch_start = (start // block) * block
            fetch_end = min(
                self.size_bytes,
                ((start + length + block - 1) // block) * block,
            )
            fetched = self._fetch(fetch_start, fetch_end - fetch_start)
            if self.cache_blocks:
                self._range_cache[fetch_start] = fetched
                self._range_cache.move_to_end(fetch_start)
                while len(self._range_cache) > self.cache_blocks:
                    self._range_cache.popitem(last=False)
            offset = start - fetch_start
            payload = fetched[offset : offset + length]
        elif payload is None:
            payload = self._fetch(start, length)
        self.position += length
        return payload

    def close(self) -> None:
        getattr(self, "_range_cache", {}).clear()
        super().close()

    def stats(self) -> dict[str, int]:
        return {
            "range_requests": self.request_count,
            "range_bytes": self.bytes_read,
        }


class _EcsRamRoleCredentialsProvider(oss2.CredentialsProvider):
    def __init__(self, role_name: str = ""):
        options: dict[str, Any] = {
            "type": "ecs_ram_role",
            "disable_imds_v1": True,
            "metadata_token_duration": 21600,
            "connect_timeout": 3000,
            "timeout": 3000,
        }
        if role_name:
            options["role_name"] = role_name
        self.client = CredentialClient(CredentialConfig(**options))

    def get_credentials(self) -> Credentials:
        credential = self.client.get_credential()
        return Credentials(
            credential.access_key_id,
            credential.access_key_secret,
            credential.security_token,
        )


class _AccessKeyCredentialsProvider(oss2.CredentialsProvider):
    def __init__(self, credential: CloudCredential):
        credential.validate()
        self.credential = credential

    def get_credentials(self) -> Credentials:
        return Credentials(
            self.credential.access_key_id,
            self.credential.access_key_secret,
            self.credential.security_token,
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _endpoint_url(endpoint: str) -> str:
    value = endpoint.strip().rstrip("/")
    if not value.startswith(("https://", "http://")):
        value = "https://" + value
    return value


class OssArchive:
    RULE_ID_PREFIX = "rds-binlog-insight-"

    def __init__(
        self,
        settings: Settings,
        *,
        bucket: Any | None = None,
        credential: CloudCredential | None = None,
        upload_attempts: int = 4,
        upload_retry_delay_seconds: float = 0.5,
    ):
        settings.validate(require_identity=False)
        if not settings.oss_enabled:
            raise OssArchiveError("OSS 归档未启用", "OSS_DISABLED")
        self.settings = settings
        self.upload_attempts = max(int(upload_attempts), 1)
        self.upload_retry_delay_seconds = max(
            float(upload_retry_delay_seconds),
            0.0,
        )
        self.prefix = settings.oss_prefix.strip().lstrip("/")
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"
        if bucket is not None:
            self.bucket = bucket
            return
        try:
            if settings.oss_auth_mode == "access_key":
                if credential is None:
                    raise OssArchiveError(
                        "未找到受保护 AccessKey，无法初始化 OSS 客户端",
                        "OSS_CREDENTIAL_MISSING",
                    )
                provider = _AccessKeyCredentialsProvider(credential)
            else:
                provider = _EcsRamRoleCredentialsProvider(settings.oss_role_name)
            auth = oss2.ProviderAuthV4(provider)
            self.bucket = oss2.Bucket(
                auth,
                _endpoint_url(settings.oss_endpoint),
                settings.oss_bucket,
                region=settings.oss_region_id,
                connect_timeout=10,
                enable_crc=True,
            )
        except OssArchiveError:
            raise
        except Exception as exc:
            raise OssArchiveError(
                f"初始化 OSS 客户端失败：{exc}",
                "OSS_CREDENTIAL_ERROR",
            ) from exc

    @property
    def lifecycle_rule_id(self) -> str:
        digest = hashlib.sha256(self.prefix.encode("utf-8")).hexdigest()[:16]
        return f"{self.RULE_ID_PREFIX}{digest}"

    def object_key(self, part: dict[str, Any]) -> str:
        event_date = str(part["event_date"])
        file_name = Path(str(part["path"])).stem
        sha256 = str(part["sha256"])
        return (
            f"{self.prefix}parquet/event_date={event_date}/"
            f"{file_name}-{sha256[:16]}.parquet"
        )

    def pack_object_key(self, event_date: str, sha256: str) -> str:
        return (
            f"{self.prefix}packs/event_date={event_date}/"
            f"{sha256}.parquet-pack"
        )

    @staticmethod
    def pack_batches(
        parts: list[dict[str, Any]],
        target_bytes: int = OSS_PACK_TARGET_BYTES,
    ) -> list[list[dict[str, Any]]]:
        target = max(int(target_bytes), 1)
        batches: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_bytes = 0
        current_date = ""
        for part in parts:
            part_date = str(part.get("event_date") or "")
            part_bytes = max(int(part.get("size_bytes") or 0), 0)
            if current and part_date != current_date:
                batches.append(current)
                current = []
                current_bytes = 0
            if not current:
                current_date = part_date
            current.append(part)
            current_bytes += part_bytes
            if current_bytes >= target:
                batches.append(current)
                current = []
                current_bytes = 0
                current_date = ""
        if current:
            batches.append(current)
        return batches

    def _head_pack_verified(
        self,
        key: str,
        *,
        size_bytes: int,
        sha256: str,
        part_count: int,
    ) -> dict[str, Any] | None:
        try:
            result = self.bucket.head_object(key)
        except oss2.exceptions.NotFound:
            return None
        except Exception as exc:
            if int(getattr(exc, "status", 0) or 0) == 404:
                return None
            raise self._server_error(exc, "OSS_HEAD_FAILED") from exc
        actual_sha256 = str(
            result.headers.get("x-oss-meta-pack-sha256", "")
            or result.headers.get("x-oss-meta-sha256", "")
        ).lower()
        actual_parts = int(
            result.headers.get("x-oss-meta-part-count", 0) or 0
        )
        if (
            int(result.content_length or -1) != int(size_bytes)
            or actual_sha256 != str(sha256).lower()
            or actual_parts != int(part_count)
        ):
            raise OssArchiveError(
                f"OSS 聚合对象校验不一致：{key}",
                "OSS_PACK_VERIFY_FAILED",
            )
        return {
            "oss_key": key,
            "oss_etag": str(result.etag or ""),
            "size_bytes": int(size_bytes),
            "sha256": str(sha256),
        }

    def _upload_pack(
        self,
        parts: list[dict[str, Any]],
        *,
        scratch_dir: Path,
    ) -> list[dict[str, Any]]:
        if not parts:
            return []
        scratch_dir.mkdir(parents=True, exist_ok=True)
        temporary = scratch_dir / f".oss-pack-{uuid.uuid4().hex}.part"
        pack_digest = hashlib.sha256()
        locators: list[tuple[int, int]] = []
        try:
            with temporary.open("wb") as output:
                for part in parts:
                    path = Path(str(part["path"]))
                    if not path.is_file():
                        raise OssArchiveError(
                            f"待归档 Parquet 不存在：{path.name}",
                            "OSS_UPLOAD_SOURCE_MISSING",
                        )
                    expected_size = int(part["size_bytes"])
                    if path.stat().st_size != expected_size:
                        raise OssArchiveError(
                            f"待归档 Parquet 大小变化：{path.name}",
                            "OSS_UPLOAD_SOURCE_CHANGED",
                        )
                    offset = output.tell()
                    part_digest = hashlib.sha256()
                    with path.open("rb") as source:
                        while chunk := source.read(4 * 1024 * 1024):
                            output.write(chunk)
                            pack_digest.update(chunk)
                            part_digest.update(chunk)
                    length = output.tell() - offset
                    if (
                        length != expected_size
                        or part_digest.hexdigest().lower()
                        != str(part["sha256"]).lower()
                    ):
                        raise OssArchiveError(
                            f"待归档 Parquet SHA-256 变化：{path.name}",
                            "OSS_UPLOAD_SOURCE_CHANGED",
                        )
                    locators.append((offset, length))
                output.flush()
                os.fsync(output.fileno())

            pack_size = temporary.stat().st_size
            pack_sha256 = pack_digest.hexdigest()
            event_date = str(parts[0].get("event_date") or "unknown")
            key = self.pack_object_key(event_date, pack_sha256)
            verified = self._head_pack_verified(
                key,
                size_bytes=pack_size,
                sha256=pack_sha256,
                part_count=len(parts),
            )
            if verified is None:
                headers = {
                    "Content-Type": "application/octet-stream",
                    "x-oss-forbid-overwrite": "true",
                    "x-oss-meta-sha256": pack_sha256,
                    "x-oss-meta-pack-sha256": pack_sha256,
                    "x-oss-meta-part-count": str(len(parts)),
                    "x-oss-meta-schema-version": "2",
                }
                verified = self._put_object_from_file_verified(
                    key,
                    temporary,
                    headers,
                    verify=lambda: self._head_pack_verified(
                        key,
                        size_bytes=pack_size,
                        sha256=pack_sha256,
                        part_count=len(parts),
                    ),
                    missing_message=f"OSS 上传后聚合对象不存在：{key}",
                    missing_code="OSS_UPLOAD_VERIFY_MISSING",
                )
            return [
                {
                    "oss_key": key,
                    "oss_etag": str(verified["oss_etag"]),
                    "oss_offset": offset,
                    "oss_length": length,
                    "oss_object_sha256": pack_sha256,
                    "size_bytes": int(part["size_bytes"]),
                    "sha256": str(part["sha256"]),
                }
                for part, (offset, length) in zip(parts, locators, strict=True)
            ]
        finally:
            temporary.unlink(missing_ok=True)

    def upload_parts(
        self,
        parts: list[dict[str, Any]],
        *,
        scratch_dir: Path,
        target_bytes: int = OSS_PACK_TARGET_BYTES,
        fresh: bool = False,
    ) -> list[dict[str, Any]]:
        if fresh:
            # Fresh Parquet files are already content-addressed and verified.
            # Avoid rereading, concatenating, hashing and fsyncing their bytes
            # before OSS reads the same bytes again.
            return [self.upload_part(part, fresh=True) for part in parts]
        results: list[dict[str, Any]] = []
        for batch in self.pack_batches(parts, target_bytes):
            results.extend(self._upload_pack(batch, scratch_dir=scratch_dir))
        return results

    @staticmethod
    def _server_error(exc: Exception, default_code: str) -> OssArchiveError:
        code = str(getattr(exc, "code", "") or default_code)
        request_id = str(getattr(exc, "request_id", "") or "")
        message = str(getattr(exc, "message", "") or exc)
        suffix = f"（RequestId={request_id}）" if request_id else ""
        return OssArchiveError(f"OSS 请求失败：{message}{suffix}", code)

    @staticmethod
    def _retryable_request_error(exc: BaseException) -> bool:
        candidate: BaseException | None = exc
        while (
            isinstance(candidate, OssArchiveError)
            and candidate.__cause__ is not None
        ):
            candidate = candidate.__cause__
        try:
            status = int(getattr(candidate, "status", 0) or 0)
        except (TypeError, ValueError):
            status = 0
        if status in {408, 429, 500, 502, 503, 504} or status < 0:
            return True
        return isinstance(candidate, (TimeoutError, ConnectionError, OSError))

    def _put_object_from_file_verified(
        self,
        key: str,
        path: Path,
        headers: dict[str, str],
        *,
        verify: Callable[[], dict[str, Any] | None],
        missing_message: str,
        missing_code: str,
    ) -> dict[str, Any]:
        """Upload a content-addressed object with bounded, idempotent retries.

        A request can reach OSS and still end with a local TLS EOF.  Every
        failed attempt therefore verifies the immutable destination first;
        only an absent object plus a transient transport/server failure is
        retried.  Permanent 4xx failures are returned immediately.
        """

        last_error: BaseException | None = None
        for attempt in range(self.upload_attempts):
            request_error: BaseException | None = None
            try:
                self.bucket.put_object_from_file(
                    key,
                    str(path),
                    headers=headers,
                )
            except Exception as exc:  # noqa: BLE001 - SDK error hierarchy varies
                request_error = exc
                last_error = exc
            try:
                verified = verify()
            except OssArchiveError as exc:
                if exc.code in {
                    "OSS_OBJECT_VERIFY_FAILED",
                    "OSS_PACK_VERIFY_FAILED",
                }:
                    raise
                verified = None
                last_error = exc
            if verified is not None:
                return dict(verified)
            if request_error is None:
                last_error = OssArchiveError(missing_message, missing_code)
            elif not self._retryable_request_error(request_error):
                raise self._server_error(
                    request_error,
                    "OSS_UPLOAD_FAILED",
                ) from request_error
            if attempt + 1 >= self.upload_attempts:
                break
            LOGGER.warning(
                "Transient OSS upload failure; retrying key=%s attempt=%s/%s "
                "error=%s status=%s",
                key,
                attempt + 1,
                self.upload_attempts,
                type(request_error or last_error).__name__,
                getattr(request_error or last_error, "status", ""),
            )
            if self.upload_retry_delay_seconds:
                time.sleep(self.upload_retry_delay_seconds * (2**attempt))
        if isinstance(last_error, OssArchiveError):
            raise last_error
        if isinstance(last_error, Exception):
            raise self._server_error(
                last_error,
                "OSS_UPLOAD_FAILED",
            ) from last_error
        raise OssArchiveError(missing_message, missing_code)

    def probe(self) -> dict[str, Any]:
        try:
            result = self.bucket.list_objects_v2(prefix=self.prefix, max_keys=1)
        except Exception as exc:
            raise self._server_error(exc, "OSS_ACCESS_DENIED") from exc
        return {
            "ok": True,
            "bucket": self.settings.oss_bucket,
            "regionId": self.settings.oss_region_id,
            "endpoint": self.settings.oss_endpoint,
            "prefix": self.prefix,
            "authMode": self.settings.oss_auth_mode,
            "sampleObjectCount": len(getattr(result, "object_list", []) or []),
        }

    def ensure_lifecycle(self) -> dict[str, Any]:
        # 生命周期规则是整桶一份，写入必须先读全量再整体 put。多实例同步时两个
        # SyncManager 会同时走这条路径：各自基于自己读到的旧列表 put，后写的那
        # 个会把先写的规则删掉——轻则本次回读校验失败，重则某个前缀的规则被抹
        # 掉、对象永不过期。进程内串行化 + 一次重试兜住这个竞态。
        with _LIFECYCLE_LOCK:
            try:
                return self._ensure_lifecycle_once()
            except OssArchiveError as exc:
                if getattr(exc, "code", "") != "OSS_LIFECYCLE_VERIFY_FAILED":
                    raise
                LOGGER.warning(
                    "OSS 生命周期规则回读不一致，重试一次：prefix=%s", self.prefix
                )
            return self._ensure_lifecycle_once()

    def _ensure_lifecycle_once(self) -> dict[str, Any]:
        try:
            try:
                current = self.bucket.get_bucket_lifecycle()
                rules = list(current.rules or [])
            except oss2.exceptions.NoSuchLifecycle:
                rules = []
            wanted = LifecycleRule(
                self.lifecycle_rule_id,
                self.prefix,
                status=LifecycleRule.ENABLED,
                expiration=LifecycleExpiration(
                    days=int(self.settings.oss_retention_days)
                ),
            )
            existing = next(
                (
                    rule
                    for rule in rules
                    if rule.id == self.lifecycle_rule_id
                    or str(rule.prefix or "") == self.prefix
                ),
                None,
            )
            if (
                existing is not None
                and existing.id == wanted.id
                and str(existing.prefix or "") == self.prefix
                and existing.status == LifecycleRule.ENABLED
                and existing.expiration is not None
                and int(existing.expiration.days or 0)
                == int(self.settings.oss_retention_days)
            ):
                changed = False
            else:
                preserved = [
                    rule
                    for rule in rules
                    if rule.id != self.lifecycle_rule_id
                    and str(rule.prefix or "") != self.prefix
                ]
                self.bucket.put_bucket_lifecycle(BucketLifecycle(preserved + [wanted]))
                changed = True
            verified = self.bucket.get_bucket_lifecycle()
            match = next(
                (
                    rule
                    for rule in (verified.rules or [])
                    if rule.id == self.lifecycle_rule_id
                ),
                None,
            )
            if (
                match is None
                or str(match.prefix or "") != self.prefix
                or match.expiration is None
                or int(match.expiration.days or 0)
                != int(self.settings.oss_retention_days)
            ):
                raise OssArchiveError(
                    "OSS 生命周期规则回读不一致",
                    "OSS_LIFECYCLE_VERIFY_FAILED",
                )
            return {
                "ruleId": self.lifecycle_rule_id,
                "prefix": self.prefix,
                "expirationDays": int(self.settings.oss_retention_days),
                "changed": changed,
            }
        except OssArchiveError:
            raise
        except Exception as exc:
            raise self._server_error(exc, "OSS_LIFECYCLE_ERROR") from exc

    def ensure_ready(self) -> dict[str, Any]:
        access = self.probe()
        lifecycle = self.ensure_lifecycle()
        return {"access": access, "lifecycle": lifecycle}

    def _head_verified(
        self, key: str, part: dict[str, Any]
    ) -> dict[str, Any] | None:
        try:
            result = self.bucket.head_object(key)
        except oss2.exceptions.NotFound:
            return None
        except Exception as exc:
            if int(getattr(exc, "status", 0) or 0) == 404:
                return None
            raise self._server_error(exc, "OSS_HEAD_FAILED") from exc
        expected_size = int(part["size_bytes"])
        expected_sha256 = str(part["sha256"])
        actual_sha256 = str(
            result.headers.get("x-oss-meta-sha256", "")
        ).lower()
        if (
            int(result.content_length or -1) != expected_size
            or actual_sha256 != expected_sha256.lower()
        ):
            raise OssArchiveError(
                f"OSS 对象校验不一致：{key}",
                "OSS_OBJECT_VERIFY_FAILED",
            )
        return {
            "oss_key": key,
            "oss_etag": str(result.etag or ""),
            "oss_offset": 0,
            "oss_length": 0,
            "oss_object_sha256": expected_sha256,
            "size_bytes": expected_size,
            "sha256": expected_sha256,
        }

    def upload_part(
        self,
        part: dict[str, Any],
        *,
        source_path: Path | None = None,
        fresh: bool = False,
    ) -> dict[str, Any]:
        path = Path(source_path) if source_path is not None else Path(str(part["path"]))
        if not path.is_file():
            raise OssArchiveError(
                f"待归档 Parquet 不存在：{path.name}",
                "OSS_UPLOAD_SOURCE_MISSING",
            )
        if path.stat().st_size != int(part["size_bytes"]):
            raise OssArchiveError(
                f"待归档 Parquet 大小变化：{path.name}",
                "OSS_UPLOAD_SOURCE_CHANGED",
            )
        if not fresh and _sha256_file(path) != str(part["sha256"]):
            raise OssArchiveError(
                f"待归档 Parquet SHA-256 变化：{path.name}",
                "OSS_UPLOAD_SOURCE_CHANGED",
            )
        key = self.object_key(part)
        if not fresh:
            verified = self._head_verified(key, part)
            if verified:
                return verified
        headers = {
            "Content-Type": "application/vnd.apache.parquet",
            "x-oss-forbid-overwrite": "true",
            "x-oss-meta-sha256": str(part["sha256"]),
            "x-oss-meta-row-count": str(int(part["row_count"])),
            "x-oss-meta-min-event-epoch-us": str(
                int(part["min_event_epoch_us"])
            ),
            "x-oss-meta-max-event-epoch-us": str(
                int(part["max_event_epoch_us"])
            ),
            "x-oss-meta-schema-version": "1",
        }
        return self._put_object_from_file_verified(
            key,
            path,
            headers,
            verify=lambda: self._head_verified(key, part),
            missing_message=f"OSS 上传后对象不存在：{key}",
            missing_code="OSS_UPLOAD_VERIFY_MISSING",
        )

    def delete_object(self, key: str) -> None:
        normalized = str(key or "").lstrip("/")
        if not normalized or not normalized.startswith(self.prefix):
            raise OssArchiveError(
                "拒绝删除 OSS 前缀之外的对象",
                "OSS_DELETE_OUTSIDE_PREFIX",
            )
        try:
            self.bucket.delete_object(normalized)
        except Exception as exc:
            if int(getattr(exc, "status", 0) or 0) == 404:
                return
            raise self._server_error(exc, "OSS_DELETE_FAILED") from exc

    def download_part(
        self,
        part: dict[str, Any],
        destination: Path,
    ) -> Path:
        key = str(part.get("oss_key") or "")
        if not key:
            raise OssArchiveError("分区缺少 OSS 对象键", "OSS_KEY_MISSING")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex[:12]}.part"
        )
        try:
            offset = max(int(part.get("oss_offset") or 0), 0)
            length = max(int(part.get("oss_length") or 0), 0)
            if length:
                if length != int(part["size_bytes"]):
                    raise OssArchiveError(
                        f"OSS 聚合分区长度不一致：{key}",
                        "OSS_DOWNLOAD_VERIFY_FAILED",
                    )
                result = self.bucket.get_object(
                    key,
                    byte_range=(offset, offset + length - 1),
                )
                with temporary.open("wb") as output:
                    while chunk := result.read(4 * 1024 * 1024):
                        output.write(chunk)
            else:
                self.bucket.get_object_to_file(key, str(temporary))
            if temporary.stat().st_size != int(part["size_bytes"]):
                raise OssArchiveError(
                    f"OSS 下载大小不一致：{key}",
                    "OSS_DOWNLOAD_VERIFY_FAILED",
                )
            if _sha256_file(temporary) != str(part["sha256"]):
                raise OssArchiveError(
                    f"OSS 下载 SHA-256 不一致：{key}",
                    "OSS_DOWNLOAD_VERIFY_FAILED",
                )
            os.replace(temporary, destination)
            return destination
        except OssArchiveError:
            raise
        except Exception as exc:
            raise self._server_error(exc, "OSS_DOWNLOAD_FAILED") from exc
        finally:
            temporary.unlink(missing_ok=True)

    def open_part_reader(self, part: dict[str, Any]) -> OssRangeReader:
        key = str(part.get("oss_key") or "")
        if not key:
            raise OssArchiveError("分区缺少 OSS 对象键", "OSS_KEY_MISSING")
        offset = max(int(part.get("oss_offset") or 0), 0)
        length = max(int(part.get("oss_length") or 0), 0)
        logical_size = length or int(part["size_bytes"])
        if length and length != int(part["size_bytes"]):
            raise OssArchiveError(
                f"OSS 聚合分区长度不一致：{key}",
                "OSS_RANGE_VERIFY_FAILED",
            )
        return OssRangeReader(
            self.bucket,
            key,
            logical_size,
            str(part.get("oss_etag") or ""),
            base_offset=offset,
        )
