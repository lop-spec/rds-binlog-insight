from __future__ import annotations

import os
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .parser_bridge import (
    NativeChecksumResult,
    NativeChecksumStream,
    ParserError,
    checksum_file,
)


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
        except DownloadError:
            forensic = destination.with_suffix(
                destination.suffix + f".corrupt-{int(time.time())}"
            )
            os.replace(destination, forensic)
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
        if exc.code == 416 and partial.exists() and (
            expected_size <= 0 or partial.stat().st_size == expected_size
        ):
            try:
                verified = verify_file(partial, expected_size, expected_crc64)
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
        code = "LINK_EXPIRED" if exc.code in {401, 403} else f"HTTP_{exc.code}"
        raise DownloadError(f"Binlog 下载失败：HTTP {exc.code}", code) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise DownloadError(f"Binlog 下载网络错误：{exc}", "NETWORK_ERROR") from exc
    status = getattr(response, "status", None) or response.getcode()
    append = offset > 0 and status == 206
    mode = "ab" if append else "wb"
    current = offset if append else 0
    last_report = 0.0
    try:
        checksum = NativeChecksumStream()
    except ParserError as exc:
        raise DownloadError(str(exc), exc.code) from exc
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
