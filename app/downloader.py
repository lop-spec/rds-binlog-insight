from __future__ import annotations

import hashlib
import logging
import os
import re
import ssl
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

from .parser_bridge import (
    NativeChecksumResult,
    NativeChecksumStream,
    ParserError,
    checksum_file,
)
from .raw_cache import (
    END,
    HEADER,
    RawCacheError,
    RawCacheWriter,
    inspect_cache,
    iter_raw,
    iter_verified_prefix,
)


try:
    import fcntl
except ImportError:  # pragma: no cover - Windows test path
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - Linux production path
    msvcrt = None


LOGGER = logging.getLogger(__name__)
INTEGRITY_MISMATCH_CODES = frozenset({"SIZE_MISMATCH", "CRC64_MISMATCH"})
_CONTENT_RANGE = re.compile(r"^bytes ([0-9]+)-([0-9]+)/([0-9]+)$")
MAX_RAW_CACHE_RECOVERY_ASSETS = 16
_RAW_CACHE_LOCKS = [threading.Lock() for _ in range(256)]


class DownloadError(RuntimeError):
    def __init__(self, message: str, code: str = "DOWNLOAD_ERROR"):
        super().__init__(message)
        self.code = code


@dataclass(slots=True, frozen=True)
class DownloadResult:
    path: Path
    size_bytes: int
    sha256: str
    crc64: str


def _validated_result(
    path: Path,
    result: NativeChecksumResult,
    expected_size: int,
    expected_crc64: str,
) -> DownloadResult:
    if expected_size > 0 and result.size_bytes != expected_size:
        raise DownloadError(
            f"文件大小校验失败：期望 {expected_size}，实际 {result.size_bytes}",
            "SIZE_MISMATCH",
        )
    expected = expected_crc64.strip()
    if (
        expected
        and expected not in {"0", "None", "null"}
        and result.crc64 != expected
    ):
        raise DownloadError(
            f"CRC64 校验失败：期望 {expected}，实际 {result.crc64}",
            "CRC64_MISMATCH",
        )
    return DownloadResult(
        path,
        result.size_bytes,
        result.sha256,
        result.crc64,
    )


def verify_file(path: Path, expected_size: int, expected_crc64: str) -> DownloadResult:
    try:
        result = checksum_file(path)
    except ParserError as exc:
        raise DownloadError(str(exc), exc.code) from exc
    return _validated_result(path, result, expected_size, expected_crc64)


def _response_status(response) -> int:
    return int(getattr(response, "status", None) or response.getcode())


def _validate_response_range(response, *, offset: int, expected_size: int) -> bool:
    """Return whether the response is an exact append response.

    A resumed request may restart from byte zero only when the server explicitly
    returns 200. A 206 is never trusted without an exact original-byte range.
    """
    status = _response_status(response)
    headers = getattr(response, "headers", {})
    content_length = str(headers.get("Content-Length", "")).strip()
    length: int | None = None
    if content_length:
        try:
            length = int(content_length)
        except ValueError as exc:
            raise DownloadError(
                "Binlog 响应 Content-Length 无效", "RANGE_RESPONSE_INVALID"
            ) from exc
        if length < 0:
            raise DownloadError(
                "Binlog 响应 Content-Length 无效", "RANGE_RESPONSE_INVALID"
            )
    if status == 200:
        if length is not None and expected_size > 0 and length != expected_size:
            raise DownloadError(
                "Binlog 完整响应长度与原始文件大小不一致",
                "RANGE_RESPONSE_INVALID",
            )
        return False
    if status != 206:
        raise DownloadError(f"Binlog 下载失败：HTTP {status}", f"HTTP_{status}")
    value = str(headers.get("Content-Range", "")).strip()
    match = _CONTENT_RANGE.fullmatch(value)
    if match is None:
        raise DownloadError("Binlog 断点响应缺少合法 Content-Range", "RANGE_RESPONSE_INVALID")
    start, end, total = (int(part) for part in match.groups())
    if start != offset or end < start or (expected_size > 0 and total != expected_size):
        raise DownloadError("Binlog 断点响应与原始字节偏移不一致", "RANGE_RESPONSE_INVALID")
    if end != total - 1:
        raise DownloadError("Binlog 断点响应未精确覆盖原始文件末尾", "RANGE_RESPONSE_INVALID")
    if length is not None and length != end - start + 1:
        raise DownloadError("Binlog 断点响应长度与 Content-Range 不一致", "RANGE_RESPONSE_INVALID")
    return True


def download_file(
    url: str,
    destination: Path,
    *,
    expected_size: int,
    expected_crc64: str,
    progress: Callable[[int], None] | None = None,
    timeout: int = 60,
) -> DownloadResult:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    if destination.exists():
        try:
            return verify_file(destination, expected_size, expected_crc64)
        except DownloadError as exc:
            # Missing/crashed checksum tools are not evidence of damaged bytes.
            # Keep the original and propagate the cause; do not redownload or
            # mask it as a missing/expired URL (also true on crash recovery).
            if exc.code not in INTEGRITY_MISMATCH_CODES:
                raise
            forensic = destination.with_suffix(
                destination.suffix + f".corrupt-{int(time.time())}"
            )
            os.replace(destination, forensic)
            LOGGER.warning("Cached binlog %s; preserved as %s", exc.code, forensic.name)
    if not url:
        raise DownloadError("RDS 未提供可下载 URL", "DOWNLOAD_LINK_MISSING")
    offset = partial.stat().st_size if partial.exists() else 0
    if expected_size > 0 and offset > expected_size:
        forensic = partial.with_suffix(partial.suffix + f".oversize-{int(time.time())}")
        os.replace(partial, forensic)
        offset = 0
    headers = {
        "Accept": "application/octet-stream",
        "User-Agent": "RDS-Binlog-Insight/1.0",
    }
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        response = urllib.request.urlopen(
            request, timeout=timeout, context=ssl.create_default_context()
        )
    except urllib.error.HTTPError as exc:
        exc.close()
        if exc.code == 416 and partial.exists() and (
            expected_size <= 0 or partial.stat().st_size == expected_size
        ):
            try:
                verified = verify_file(partial, expected_size, expected_crc64)
            except DownloadError as verification_error:
                if verification_error.code not in INTEGRITY_MISMATCH_CODES:
                    raise
                forensic = partial.with_suffix(
                    partial.suffix + f".corrupt-{int(time.time())}"
                )
                os.replace(partial, forensic)
                LOGGER.warning("Partial binlog %s; preserved as %s", verification_error.code, forensic.name)
                raise
            os.replace(partial, destination)
            return DownloadResult(
                destination,
                verified.size_bytes,
                verified.sha256,
                verified.crc64,
            )
        code = "LINK_EXPIRED" if exc.code in {401, 403} else f"HTTP_{exc.code}"
        raise DownloadError(f"Binlog 下载失败：HTTP {exc.code}", code) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise DownloadError(f"Binlog 下载网络错误：{exc}", "NETWORK_ERROR") from exc
    try:
        append = offset > 0 and _validate_response_range(
            response, offset=offset, expected_size=expected_size
        )
        if offset == 0:
            _validate_response_range(response, offset=0, expected_size=expected_size)
    except BaseException:
        response.close()
        raise
    mode = "ab" if append else "wb"
    current = offset if append else 0
    last_report = 0.0
    try:
        checksum = NativeChecksumStream()
    except BaseException as exc:
        response.close()
        if isinstance(exc, ParserError):
            raise DownloadError(str(exc), exc.code) from exc
        raise
    try:
        if append:
            with partial.open("rb") as existing:
                while chunk := existing.read(4 * 1024 * 1024):
                    checksum.update(chunk)
        with response, partial.open(mode) as handle:
            while chunk := response.read(1024 * 1024):
                handle.write(chunk)
                checksum.update(chunk)
                current += len(chunk)
                now = time.monotonic()
                if progress and now - last_report >= 0.5:
                    progress(current)
                    last_report = now
            handle.flush()
            os.fsync(handle.fileno())
        native_result = checksum.finish()
    except ParserError as exc:
        checksum.abort()
        raise DownloadError(str(exc), exc.code) from exc
    except (OSError, urllib.error.URLError, TimeoutError) as exc:
        checksum.abort()
        raise DownloadError(f"Binlog 下载中断：{exc}", "DOWNLOAD_INTERRUPTED") from exc
    except BaseException:
        checksum.abort()
        raise
    finally:
        response.close()
    if progress:
        progress(current)
    try:
        verified = _validated_result(
            partial,
            native_result,
            expected_size,
            expected_crc64,
        )
    except DownloadError:
        forensic = partial.with_suffix(
            partial.suffix + f".corrupt-{int(time.time())}"
        )
        os.replace(partial, forensic)
        raise
    os.replace(partial, destination)
    return DownloadResult(
        destination,
        verified.size_bytes,
        verified.sha256,
        verified.crc64,
    )


def verify_raw_cache(
    path: Path,
    *,
    source_id: str,
    expected_size: int,
    expected_crc64: str,
) -> DownloadResult:
    """Replay one complete cache through the native source checksum oracle."""
    checksum: NativeChecksumStream | None = None
    try:
        checksum = NativeChecksumStream()
        for chunk in iter_raw(path, source_id=source_id, expected_size=expected_size):
            checksum.update(chunk)
        result = checksum.finish()
    except RawCacheError as exc:
        if checksum is not None:
            checksum.abort()
        raise DownloadError(str(exc), "RAW_CACHE_INVALID") from exc
    except ParserError as exc:
        if checksum is not None:
            checksum.abort()
        raise DownloadError(str(exc), exc.code) from exc
    except BaseException:
        if checksum is not None:
            checksum.abort()
        raise
    return _validated_result(path, result, expected_size, expected_crc64)


def _sync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_cache_exclusive(source: Path, destination: Path) -> None:
    """Publish by an exclusive hard link; never replace another complete asset."""
    os.link(source, destination)
    _sync_directory(destination.parent)


def _cache_recovery_assets(destination: Path) -> list[Path]:
    assets = sorted(destination.parent.glob(destination.name + ".part-*"))
    for path in assets:
        if path.is_symlink() or not path.is_file():
            raise DownloadError("原文件压缩缓存恢复资产类型无效", "RAW_CACHE_RECOVERY_ASSET_INVALID")
    return assets


def _cache_forensic_assets(destination: Path) -> list[Path]:
    assets = sorted(destination.parent.glob(destination.name + ".corrupt-*"))
    for path in assets:
        if path.is_symlink() or not path.is_file():
            raise DownloadError("原文件压缩缓存取证资产类型无效", "RAW_CACHE_RECOVERY_ASSET_INVALID")
    return assets


def _bounded_cache_assets(destination: Path) -> tuple[list[Path], list[Path], int]:
    recoveries = _cache_recovery_assets(destination)
    forensics = _cache_forensic_assets(destination)
    retained = recoveries + forensics
    if len(retained) > MAX_RAW_CACHE_RECOVERY_ASSETS:
        raise DownloadError(
            "原文件压缩缓存恢复/取证资产过多，拒绝无界扫描或继续占用空间",
            "RAW_CACHE_RECOVERY_ASSET_LIMIT",
        )
    try:
        retained_bytes = sum(path.stat().st_size for path in retained)
    except OSError as exc:
        raise DownloadError(
            "原文件压缩缓存恢复/取证资产状态读取失败",
            "RAW_CACHE_RECOVERY_ASSET_INVALID",
        ) from exc
    return recoveries, forensics, retained_bytes


def _cleanup_recovery_assets(assets: list[Path]) -> None:
    for path in assets:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            LOGGER.warning("RAW_CACHE_RECOVERY_CLEANUP_FAILED path=%s error=%s", path, exc)
    if assets:
        try:
            _sync_directory(assets[0].parent)
        except OSError as exc:
            LOGGER.warning("RAW_CACHE_RECOVERY_DIRECTORY_SYNC_FAILED path=%s error=%s", assets[0].parent, exc)


@contextmanager
def _raw_cache_destination_lock(destination: Path) -> Iterator[None]:
    """Bound recovery accounting and exclusive publication across processes."""
    digest = hashlib.sha256(str(destination.resolve()).encode("utf-8")).digest()
    shard = int.from_bytes(digest[:4], "big") % len(_RAW_CACHE_LOCKS)
    lock_path = destination.parent / f".raw-cache-{shard:03d}.lock"
    local_lock = _RAW_CACHE_LOCKS[shard]
    with local_lock:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            elif msvcrt is not None:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                elif msvcrt is not None:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def _finish_raw_cache_recovery(
    source: Path,
    destination: Path,
    *,
    recoveries: list[Path],
    source_id: str,
    expected_size: int,
    expected_crc64: str,
    physical_budget: int,
    retained_bytes: int,
    progress: Callable[[int], None] | None,
) -> DownloadResult:
    """Finalize complete verified frames without requiring another signed URL."""
    attempt = destination.parent / f"{destination.name}.part-{uuid.uuid4().hex}"
    writer: RawCacheWriter | None = None
    checksum: NativeChecksumStream | None = None
    try:
        source_physical = source.stat().st_size
        writer = RawCacheWriter.resume(
            source,
            attempt,
            source_id=source_id,
            expected_size=expected_size,
            physical_budget=physical_budget - (retained_bytes - source_physical),
        )
        checksum = NativeChecksumStream()
        for chunk in iter_verified_prefix(
            source, source_id=source_id, expected_size=expected_size
        ):
            checksum.update(chunk)
        native_result = checksum.finish()
        checksum = None
        verified = _validated_result(
            attempt,
            native_result,
            expected_size,
            expected_crc64,
        )
        writer.finish(expected_sha256=verified.sha256)
        writer = None
        if progress:
            progress(verified.size_bytes)
        try:
            _publish_cache_exclusive(attempt, destination)
        except FileExistsError:
            winner = verify_raw_cache(
                destination,
                source_id=source_id,
                expected_size=expected_size,
                expected_crc64=expected_crc64,
            )
            _cleanup_recovery_assets(recoveries + [attempt])
            return winner
        _cleanup_recovery_assets(recoveries + [attempt])
        return DownloadResult(
            destination,
            verified.size_bytes,
            verified.sha256,
            verified.crc64,
        )
    except RawCacheError as exc:
        raise DownloadError(str(exc), "RAW_CACHE_WRITE_FAILED") from exc
    except ParserError as exc:
        raise DownloadError(str(exc), exc.code) from exc
    except OSError as exc:
        raise DownloadError(
            f"Binlog 压缩缓存恢复中断：{exc}", "DOWNLOAD_INTERRUPTED"
        ) from exc
    finally:
        if checksum is not None:
            checksum.abort()
        if writer is not None:
            writer.close()


def download_raw_cache(
    url: str,
    destination: Path,
    *,
    source_id: str,
    expected_size: int,
    expected_crc64: str,
    physical_budget: int,
    progress: Callable[[int], None] | None = None,
    timeout: int = 60,
) -> DownloadResult:
    with _raw_cache_destination_lock(destination):
        return _download_raw_cache_locked(
            url,
            destination,
            source_id=source_id,
            expected_size=expected_size,
            expected_crc64=expected_crc64,
            physical_budget=physical_budget,
            progress=progress,
            timeout=timeout,
        )


def _download_raw_cache_locked(
    url: str,
    destination: Path,
    *,
    source_id: str,
    expected_size: int,
    expected_crc64: str,
    physical_budget: int,
    progress: Callable[[int], None] | None = None,
    timeout: int = 60,
) -> DownloadResult:
    """Download original bytes once into an immutable, framed compressed cache.

    HTTP Range always uses verified *raw* bytes. A cache footer is written only
    after native size/CRC64/SHA verification, and exclusive publication happens
    only after file and directory durability. Failed attempts remain private.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    if physical_budget < HEADER.size + END.size:
        raise DownloadError("原文件压缩缓存物理预算过小", "RAW_CACHE_BUDGET_INVALID")
    if destination.exists():
        try:
            verified_destination = verify_raw_cache(
                destination,
                source_id=source_id,
                expected_size=expected_size,
                expected_crc64=expected_crc64,
            )
            _cleanup_recovery_assets(_cache_recovery_assets(destination))
            return verified_destination
        except DownloadError as exc:
            if exc.code not in INTEGRITY_MISMATCH_CODES | {"RAW_CACHE_INVALID"}:
                raise
            forensic = destination.with_suffix(
                destination.suffix + f".corrupt-{time.time_ns()}"
            )
            os.replace(destination, forensic)
            _sync_directory(destination.parent)
            LOGGER.warning("Cached raw source %s; preserved as %s", exc.code, forensic.name)
    assets, forensics, retained_bytes = _bounded_cache_assets(destination)
    if retained_bytes >= physical_budget:
        raise DownloadError("原文件压缩缓存恢复/取证资产已耗尽物理预算", "RAW_CACHE_BUDGET_EXCEEDED")
    recoveries: list[tuple[int, Path, object]] = []
    for path in assets:
        try:
            state = inspect_cache(path, source_id=source_id, expected_size=expected_size)
        except RawCacheError as exc:
            LOGGER.warning("RAW_CACHE_RECOVERY_REJECTED path=%s error=%s", path, exc)
            continue
        if state.complete:
            verified = verify_raw_cache(
                path,
                source_id=source_id,
                expected_size=expected_size,
                expected_crc64=expected_crc64,
            )
            try:
                _publish_cache_exclusive(path, destination)
            except FileExistsError:
                winner = verify_raw_cache(
                    destination,
                    source_id=source_id,
                    expected_size=expected_size,
                    expected_crc64=expected_crc64,
                )
                _cleanup_recovery_assets(assets)
                return winner
            _cleanup_recovery_assets(assets)
            return DownloadResult(destination, verified.size_bytes, verified.sha256, verified.crc64)
        recoveries.append((state.raw_bytes, path, state))
    recovery = max(recoveries, default=None, key=lambda value: (value[0], str(value[1])))
    offset = recovery[0] if recovery is not None else 0
    retained_count = len(assets) + len(forensics)
    if retained_count >= MAX_RAW_CACHE_RECOVERY_ASSETS:
        raise DownloadError(
            "原文件压缩缓存恢复/取证资产已达数量上限",
            "RAW_CACHE_RECOVERY_ASSET_LIMIT",
        )
    if recovery is not None and offset == expected_size:
        return _finish_raw_cache_recovery(
            recovery[1],
            destination,
            recoveries=assets,
            source_id=source_id,
            expected_size=expected_size,
            expected_crc64=expected_crc64,
            physical_budget=physical_budget,
            retained_bytes=retained_bytes,
            progress=progress,
        )
    if not url:
        raise DownloadError("RDS 未提供可下载 URL", "DOWNLOAD_LINK_MISSING")
    headers = {
        "Accept": "application/octet-stream",
        "User-Agent": "RDS-Binlog-Insight/1.0",
    }
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        response = urllib.request.urlopen(
            request, timeout=timeout, context=ssl.create_default_context()
        )
    except urllib.error.HTTPError as exc:
        exc.close()
        code = "LINK_EXPIRED" if exc.code in {401, 403} else f"HTTP_{exc.code}"
        raise DownloadError(f"Binlog 下载失败：HTTP {exc.code}", code) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise DownloadError(f"Binlog 下载网络错误：{exc}", "NETWORK_ERROR") from exc

    writer: RawCacheWriter | None = None
    checksum: NativeChecksumStream | None = None
    attempt = destination.parent / f"{destination.name}.part-{uuid.uuid4().hex}"
    current = 0
    last_report = 0.0
    try:
        append = _validate_response_range(
            response, offset=offset, expected_size=expected_size
        )
        selected = recovery if append and recovery is not None else None
        if selected is not None:
            source = selected[1]
            source_physical = source.stat().st_size
            combined_budget = physical_budget - (retained_bytes - source_physical)
            writer = RawCacheWriter.resume(
                source,
                attempt,
                source_id=source_id,
                expected_size=expected_size,
                physical_budget=combined_budget,
            )
            current = writer.raw_offset
        else:
            writer = RawCacheWriter(
                attempt,
                source_id=source_id,
                expected_size=expected_size,
                physical_budget=physical_budget - retained_bytes,
            )
        checksum = NativeChecksumStream()
        if selected is not None:
            for chunk in iter_verified_prefix(
                selected[1], source_id=source_id, expected_size=expected_size
            ):
                checksum.update(chunk)
        with response:
            while chunk := response.read(1024 * 1024):
                writer.write(chunk)
                checksum.update(chunk)
                current += len(chunk)
                now = time.monotonic()
                if progress and now - last_report >= 0.5:
                    progress(current)
                    last_report = now
        native_result = checksum.finish()
        checksum = None
        verified = _validated_result(
            attempt,
            native_result,
            expected_size,
            expected_crc64,
        )
        writer.finish(expected_sha256=verified.sha256)
        writer = None
        if progress:
            progress(current)
        try:
            _publish_cache_exclusive(attempt, destination)
        except FileExistsError:
            winner = verify_raw_cache(
                destination,
                source_id=source_id,
                expected_size=expected_size,
                expected_crc64=expected_crc64,
            )
            _cleanup_recovery_assets(assets + [attempt])
            return winner
        _cleanup_recovery_assets(assets + [attempt])
        return DownloadResult(destination, verified.size_bytes, verified.sha256, verified.crc64)
    except RawCacheError as exc:
        raise DownloadError(str(exc), "RAW_CACHE_WRITE_FAILED") from exc
    except ParserError as exc:
        raise DownloadError(str(exc), exc.code) from exc
    except DownloadError:
        raise
    except (OSError, urllib.error.URLError, TimeoutError) as exc:
        raise DownloadError(f"Binlog 压缩缓存下载中断：{exc}", "DOWNLOAD_INTERRUPTED") from exc
    finally:
        response.close()
        if checksum is not None:
            checksum.abort()
        if writer is not None:
            writer.close()
