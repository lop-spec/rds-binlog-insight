"""Remove scratch leftovers of processes that were killed mid-write.

Temporary bodies are deleted in `finally` blocks, which a SIGKILL, OOM kill or container recreate skips.
On 10-20 the shared scratch held 15.4 GiB of such files (2008 analytics downloads back to 09-17, three
binlog-rows lanes from 09-25) while the disk sat below the ClickHouse safety floor. Only names this
service creates are touched, and only once nothing has written to them for hours, so a slow build in
another container is never pulled from under it.
"""
from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

STALE_SECONDS = 6 * 3600
SWEEP_INTERVAL_SECONDS = 3600
STAMP_NAME = ".scratch-sweep-stamp"
# EventStorage's per-part bodies (f".{kind}-{uuid}.parquet"), the partial files OssArchive.download_part
# writes next to them (f"..{kind}-{uuid}.parquet.{hex}.part"), and OssArchive._upload_pack temporaries.
BODY_KINDS = ("query", "structure", "index", "catalog", "exact", "analytics", "slowlog", "detail",
              "zstd-source", "zstd9")
FILE_PATTERNS = tuple(f".{kind}-*.parquet" for kind in BODY_KINDS) + tuple(
    f"..{kind}-*.part" for kind in BODY_KINDS) + (".oss-pack-*.part",)
# binlog_rows_worker lanes (tempfile.TemporaryDirectory(prefix=f"lane{n}-")).
DIRECTORY_PATTERNS = ("binlog-rows/lane*-*",)


def _newest_mtime(path: Path) -> float:
    newest = path.lstat().st_mtime
    if path.is_dir() and not path.is_symlink():
        for child in path.rglob("*"):
            try:
                newest = max(newest, child.lstat().st_mtime)
            except OSError:
                continue
    return newest


def sweep_stale_scratch(
    root: Path,
    *,
    stale_seconds: float = STALE_SECONDS,
    min_interval_seconds: float = SWEEP_INTERVAL_SECONDS,
    now: float | None = None,
) -> dict[str, Any]:
    root = Path(root)
    current = time.time() if now is None else float(now)
    stamp = root / STAMP_NAME
    result: dict[str, Any] = {"skipped": False, "files": 0, "directories": 0, "bytes": 0, "errors": []}
    try:
        if min_interval_seconds and current - stamp.stat().st_mtime < min_interval_seconds:
            result["skipped"] = True
            return result
    except FileNotFoundError:
        pass
    if not root.is_dir():
        return result
    resolved_root = root.resolve()
    cutoff = current - max(float(stale_seconds), 0.0)
    candidates = [(path, False) for pattern in FILE_PATTERNS for path in root.glob(pattern)]
    candidates += [(path, True) for pattern in DIRECTORY_PATTERNS for path in root.glob(pattern)]
    for path, directory in candidates:
        try:
            if path.is_symlink() or not path.resolve().is_relative_to(resolved_root):
                continue
            if directory != path.is_dir() or _newest_mtime(path) >= cutoff:
                continue
            if directory:
                size = sum(child.stat().st_size for child in path.rglob("*") if child.is_file())
                shutil.rmtree(path)
                result["directories"] += 1
            else:
                size = path.stat().st_size
                path.unlink()
                result["files"] += 1
            result["bytes"] += size
        except FileNotFoundError:
            continue
        except OSError as exc:
            result["errors"].append(f"{path.name}: {exc}")
    try:
        stamp.touch()
        os.utime(stamp, (current, current))
    except OSError as exc:
        result["errors"].append(f"{STAMP_NAME}: {exc}")
    if result["files"] or result["directories"] or result["errors"]:
        LOGGER.warning(
            "SCRATCH_SWEEP files=%s directories=%s bytes=%s errors=%s",
            result["files"], result["directories"], result["bytes"], result["errors"][:3],
        )
    return result
