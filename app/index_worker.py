from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

# Bound native runtimes before importing Arrow/DuckDB through EventStorage.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("ARROW_NUM_THREADS", "1")

from .config import data_root, ensure_data_dirs
from .credentials import load_credential
from .maintenance_status import WORKER_PROGRESS_NAME, write_json_status
from .metadata import MetadataStore, SLOW_LOG_FILE_PREFIX
from .oss_store import OssArchive
from .storage import EventStorage


IDLE_EXIT_CODE = 20
INDEX_BATCH_SIZE = 64
INDEX_SCAN_PAGE_SIZE = 256
STRUCTURAL_INDEX_WORKERS = 4
FULL_INDEX_BATCH_SIZE = 4
CATALOG_BATCH_SIZE = 256
CATALOG_INDEX_WORKERS = 16
CATALOG_RECONCILE_BATCH_SIZE = 2048
CATALOG_STORE_BACKFILL_BATCH_SIZE = 256
CATALOG_STATS_CACHE_SECONDS = 5.0
EXACT_INDEX_BATCH_SIZE = 4
EXACT_INDEX_WORKERS = 4
# 分析聚合要读几乎所有列并整对象下载，单批保持小量；两个工作线程用于让
# DuckDB 聚合与下一个对象的下载重叠，仍受容器 1 CPU 限制。
def _env_int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return min(max(value, low), high)


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


# 历史回填吞吐可按宿主机余量临时调大（compose 环境变量），默认保持保守值。
ANALYTICS_BATCH_SIZE = _env_int("RDS_BINLOG_ANALYTICS_BATCH", 2, 1, 32)
ANALYTICS_WORKERS = _env_int("RDS_BINLOG_ANALYTICS_WORKERS", 2, 1, 12)
# 每轮 analytics 批次里留给「历史欠账」的名额，其余给新采集的分区。
# 设 0 则完全按原来的最新优先（历史将不再推进）。
ANALYTICS_HISTORY_QUOTA = _env_int("RDS_BINLOG_ANALYTICS_HISTORY_QUOTA", 4, 0, 16)
SLOWLOG_INDEX_BATCH_SIZE = _env_int("RDS_BINLOG_SLOWLOG_BATCH", 128, 1, 512)
SLOWLOG_INDEX_WORKERS = _env_int("RDS_BINLOG_SLOWLOG_WORKERS", 8, 1, 16)
SLOWLOG_RECONCILE_BATCH_SIZE = _env_int(
    "RDS_BINLOG_SLOWLOG_RECONCILE_BATCH",
    2048,
    32,
    2048,
)
SLOWLOG_RECONCILE_REFRESH_SECONDS = _env_int(
    "RDS_BINLOG_SLOWLOG_RECONCILE_SECONDS",
    60,
    30,
    3600,
)
SLOWLOG_INDEX_ENABLED = _env_bool("RDS_BINLOG_SLOWLOG_INDEX_ENABLED", True)


def _bound_arrow() -> None:
    try:
        import pyarrow as pa

        pa.set_cpu_count(1)
        pa.set_io_thread_count(1)
    except (AttributeError, ImportError):
        pass


def _part_name(part: dict[str, Any]) -> str:
    return Path(str(part.get("path") or "")).name


def _missing_index_parts(
    metadata: MetadataStore,
    storage: EventStorage,
    *,
    limit: int = INDEX_BATCH_SIZE,
    page_size: int = INDEX_SCAN_PAGE_SIZE,
) -> list[dict[str, Any]]:
    target = max(int(limit), 0)
    page_size = max(int(page_size), 1)
    if target == 0:
        return []

    local = [
        part
        for part in storage.local_body_parts()
        if str(part.get("oss_key") or "")
    ]
    missing = storage.search_index.missing_parts(local, limit=target)
    selected_paths = {str(part["path"]) for part in missing}
    offset = 0
    while len(missing) < target:
        page = metadata.list_parts(
            limit=page_size,
            visible_only=False,
            offset=offset,
        )
        if not page:
            break
        offset += len(page)
        archived = [
            part
            for part in page
            if str(part.get("oss_key") or "")
            and str(part["path"]) not in selected_paths
        ]
        candidates = storage.search_index.missing_parts(
            archived,
            limit=target - len(missing),
        )
        missing.extend(candidates)
        selected_paths.update(str(part["path"]) for part in candidates)
        if len(page) < page_size:
            break
    return missing


def _missing_structural_parts(
    metadata: MetadataStore,
    storage: EventStorage,
    *,
    limit: int = INDEX_BATCH_SIZE,
    page_size: int = INDEX_SCAN_PAGE_SIZE,
) -> list[dict[str, Any]]:
    target = max(int(limit), 0)
    page_size = max(int(page_size), 1)
    if target == 0:
        return []
    local = [
        part
        for part in storage.local_body_parts()
        if str(part.get("oss_key") or "")
    ]
    missing = storage.search_index.missing_structural_parts(local, limit=target)
    selected_paths = {str(part["path"]) for part in missing}
    offset = 0
    while len(missing) < target:
        page = metadata.list_parts(
            limit=page_size,
            visible_only=False,
            offset=offset,
        )
        if not page:
            break
        offset += len(page)
        archived = [
            part
            for part in page
            if str(part.get("oss_key") or "")
            and str(part["path"]) not in selected_paths
        ]
        candidates = storage.search_index.missing_structural_parts(
            archived,
            limit=target - len(missing),
        )
        missing.extend(candidates)
        selected_paths.update(str(part["path"]) for part in candidates)
        if len(page) < page_size:
            break
    return missing


def _missing_exact_parts(
    metadata: MetadataStore,
    storage: EventStorage,
    *,
    limit: int = EXACT_INDEX_BATCH_SIZE,
    page_size: int = INDEX_SCAN_PAGE_SIZE,
) -> list[dict[str, Any]]:
    target = max(int(limit), 0)
    page_size = max(int(page_size), 1)
    if target == 0:
        return []
    local = [
        part
        for part in storage.local_body_parts()
        if str(part.get("oss_key") or "")
    ]
    local_catalogs = metadata.part_catalogs([str(part["path"]) for part in local])
    local = storage.exact_index.catalog_relevant_parts(local, local_catalogs)
    missing = storage.exact_index.missing_parts(local, limit=target)
    selected_paths = {str(part["path"]) for part in missing}
    offset = 0
    while len(missing) < target:
        page = metadata.list_parts(
            limit=page_size,
            visible_only=False,
            offset=offset,
        )
        if not page:
            break
        offset += len(page)
        archived = [
            part
            for part in page
            if str(part.get("oss_key") or "")
            and str(part["path"]) not in selected_paths
        ]
        archived_catalogs = metadata.part_catalogs(
            [str(part["path"]) for part in archived]
        )
        archived = storage.exact_index.catalog_relevant_parts(
            archived,
            archived_catalogs,
        )
        candidates = storage.exact_index.missing_parts(
            archived,
            limit=target - len(missing),
        )
        missing.extend(candidates)
        selected_paths.update(str(part["path"]) for part in candidates)
        if len(page) < page_size:
            break
    return missing


def _advance_rollup(
    metadata: MetadataStore,
    storage: Any,
    *,
    budget_buckets: int = 48,
) -> int:
    """把时间桶 rollup 往前推。返回本轮重算的小时桶数。

    每轮从水位往后推 budget 个小时桶；当前小时那个桶不封存（分区还在增），
    每轮重算。历史桶只有在发现新分区落进来时才回退重算。
    """

    from .rollup_index import DAY_US, HOUR_US, RollupIndex, bucket_floor

    rollup: RollupIndex = storage.rollup_index
    analytics = storage.analytics_index
    rollup.ensure_schema()
    now_us = int(time.time() * 1_000_000)
    current_hour = bucket_floor(now_us, HOUR_US)

    conn = rollup.connection()
    try:
        # 待办直接用差集算：analytics_parts（已聚合）− rollup_state（已并入）。
        # 两张表同库，都有主键，很便宜；而且历史补录（指定时间同步拉回旧数据）
        # 会自动出现在差集里，不需要额外的回退逻辑。
        pending_buckets = [
            int(row[0])
            for row in conn.execute(
                """
                SELECT DISTINCT (a.min_event_epoch_us / :hour) * :hour AS b
                FROM analytics_parts a
                LEFT JOIN rollup_state r ON r.part_path = a.part_path
                WHERE r.part_path IS NULL
                ORDER BY b
                LIMIT :budget
                """,
                {"hour": HOUR_US, "budget": budget_buckets},
            ).fetchall()
        ]
        # 当前小时的分区还在持续写入，每轮都重算，不然最新数据进不了长窗口。
        targets = sorted(set(pending_buckets) | {current_hour})

        processed = 0
        touched_days: set[int] = set()
        for bucket in targets:
            parts = metadata.parts_in_range(
                start_epoch_us=bucket,
                end_epoch_us=bucket + HOUR_US - 1,
            )
            if parts:
                # 只用「已完成分析聚合」的分区：没聚合的分区在 sql_stat 里根本
                # 没有数据，纳入只会把它们错误标记成已 rollup。它们补齐后会重新
                # 出现在上面的差集里，触发本桶重算。这与精确路径只用 covered
                # 分区是同一口径，两条路径结论一致。
                missing = set(analytics.coverage(parts).get("missing_parts") or [])
                parts = [p for p in parts if str(p["path"]) not in missing]
            conn.execute("BEGIN IMMEDIATE")
            try:
                rollup.rebuild_bucket(
                    conn, bucket_us=bucket, width_us=HOUR_US, parts=parts
                )
                if parts:
                    rollup.mark_parts(conn, parts)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            touched_days.add(bucket_floor(bucket, DAY_US))
            processed += 1

        for day in sorted(touched_days):
            conn.execute("BEGIN IMMEDIATE")
            try:
                rollup.rebuild_day(conn, day)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return processed
    finally:
        conn.close()


def _missing_analytics_parts(
    metadata: MetadataStore,
    storage: EventStorage,
    *,
    limit: int = ANALYTICS_BATCH_SIZE,
    page_size: int = INDEX_SCAN_PAGE_SIZE,
) -> list[dict[str, Any]]:
    target = max(int(limit), 0)
    page_size = max(int(page_size), 1)
    index = getattr(storage, "analytics_index", None)
    if not target or index is None:
        return []
    local = [
        part
        for part in storage.local_body_parts()
        if str(part.get("oss_key") or "")
        and not str(part.get("log_file_name") or "").startswith(
            SLOW_LOG_FILE_PREFIX
        )
    ]
    # 给历史欠账留固定名额。本地正文（新采集的分区）几乎每轮都能凑满整个
    # batch，历史分区就永远排不上队——2026-08-11 实测：待办队列 12 个全是当天
    # 的，07-22~07-30 九天的 8.9 万个分区覆盖率一直是 0%，而下面那段分页扫描
    # 因为 len(missing) 已达 target 从来没执行过（实测翻页数 = 0）。
    local_limit = max(target - ANALYTICS_HISTORY_QUOTA, 1)
    missing = index.missing_parts(local, limit=local_limit)
    selected_paths = {str(part["path"]) for part in missing}
    offset = 0
    while len(missing) < target:
        page = metadata.list_parts(
            limit=page_size,
            visible_only=False,
            offset=offset,
            # 欠账都堆在最早那几天，从旧往新扫第一页就命中；倒序要空翻
            # 一百多页（已完成的分区）才碰得到一个待办的。
            oldest_first=True,
        )
        if not page:
            break
        offset += len(page)
        archived = [
            part
            for part in page
            if str(part.get("oss_key") or "")
            and str(part["path"]) not in selected_paths
            and not str(part.get("log_file_name") or "").startswith(
                SLOW_LOG_FILE_PREFIX
            )
        ]
        candidates = index.missing_parts(archived, limit=target - len(missing))
        missing.extend(candidates)
        selected_paths.update(str(part["path"]) for part in candidates)
        if len(page) < page_size:
            break
    return missing


def _advance_slowlog_index(
    metadata: MetadataStore,
    storage: EventStorage,
    archive: OssArchive | None,
    *,
    publish: Any,
    generation: str,
    admission_check: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Bounded historical reconcile plus durable queue processing."""

    now_us = int(time.time() * 1_000_000)
    state = storage.slowlog_index.reconcile_state()
    if state["complete"] and (
        now_us - int(state.get("updated_at_us") or 0)
        >= SLOWLOG_RECONCILE_REFRESH_SECONDS * 1_000_000
    ):
        storage.slowlog_index.advance_reconcile(after_path="", complete=False)
        state = storage.slowlog_index.reconcile_state()
    reconcile_scanned = 0
    reconcile_advanced = False
    if not state["complete"]:
        page = metadata.slowlog_parts_page(
            after_path=str(state.get("after_path") or ""),
            limit=SLOWLOG_RECONCILE_BATCH_SIZE,
        )
        reconcile_scanned = len(page)
        # Reconciliation is a crash-gap repair, not a rewrite of the live
        # queue.  Compare the two small part registries first so an idle sweep
        # remains read-only for already covered history.
        missing = storage.slowlog_index.missing_parts(page, limit=len(page))
        if missing:
            storage.slowlog_index.enqueue_parts(missing)
        complete = len(page) < SLOWLOG_RECONCILE_BATCH_SIZE
        after_path = (
            str(page[-1]["path"])
            if page
            else str(state.get("after_path") or "")
        )
        storage.slowlog_index.advance_reconcile(
            after_path=after_path,
            complete=complete,
        )
        reconcile_advanced = True

    parts: list[dict[str, Any]] = []
    for path in storage.slowlog_index.ready_paths(SLOWLOG_INDEX_BATCH_SIZE):
        part = metadata.part_by_path(path)
        if part is None:
            storage.slowlog_index.remove_path(path)
            continue
        if not str(part.get("log_file_name") or "").startswith(
            SLOW_LOG_FILE_PREFIX
        ):
            storage.slowlog_index.remove_path(path)
            continue
        if archive is None and not Path(path).is_file():
            continue
        parts.append(part)
    if not parts:
        return {
            "parts": 0,
            "rows": 0,
            "failedParts": 0,
            "reconcileScanned": reconcile_scanned,
            "reconcileAdvanced": reconcile_advanced,
        }

    # The dedicated source worker supplies a serving-path canary here.  This
    # boundary is intentionally after the small reconciliation page and before
    # any Parquet read/index write, so an unhealthy online API vetoes the next
    # expensive unit without recording a false part failure.
    if admission_check is not None:
        admission_check()

    totals = {
        "parts": 0,
        "rows": 0,
        "failedParts": 0,
        "reconcileScanned": reconcile_scanned,
        "reconcileAdvanced": reconcile_advanced,
    }
    errors: list[str] = []
    publish(
        "running",
        phase="slowlog",
        part=parts[0],
        token=f"{generation}:slowlog:0:{parts[0]['sha256']}",
        result={**totals, "batchSize": len(parts)},
    )
    with ThreadPoolExecutor(
        max_workers=min(SLOWLOG_INDEX_WORKERS, len(parts)),
        thread_name_prefix="slowlog-index-worker",
    ) as executor:
        futures = {
            executor.submit(
                storage.ensure_slowlog_part,
                part,
                archive,
                already_queued=True,
            ): part
            for part in parts
        }
        for position, future in enumerate(as_completed(futures), start=1):
            part = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                totals["failedParts"] += 1
                errors.append(f"{_part_name(part)}：{exc}")
                continue
            totals["parts"] += int(result.get("built") or 0)
            totals["rows"] += int(result.get("indexed_rows") or 0)
            publish(
                "running",
                phase="slowlog",
                part=part,
                token=f"{generation}:slowlog:{position}:{part['sha256']}",
                result={**totals, "batchSize": len(parts)},
            )
    publish(
        "completed",
        phase="slowlog",
        part=parts[-1],
        token=f"{generation}:slowlog-indexed:{totals['parts']}",
        error="；".join(errors[:3]),
        result={
            **totals,
            "batchSize": len(parts),
            "slowlogIndexStats": storage.slowlog_index.stats(),
        },
    )
    return totals


def run_one(data_dir: Path, generation: str) -> int:
    _bound_arrow()
    paths = ensure_data_dirs(data_dir)
    progress_path = paths["index"] / WORKER_PROGRESS_NAME

    def publish(
        state: str,
        *,
        phase: str = "",
        part: dict[str, Any] | None = None,
        token: str = "",
        error: str = "",
        result: dict[str, Any] | None = None,
    ) -> None:
        write_json_status(
            progress_path,
            {
                "generation": generation,
                "pid": os.getpid(),
                "state": state,
                "phase": phase,
                "currentPart": _part_name(part or {}),
                "currentPath": str((part or {}).get("path") or ""),
                "partSha256": str((part or {}).get("sha256") or ""),
                "progressToken": token,
                "lastError": error,
                "result": result or {},
            },
        )

    publish("starting", token=f"{generation}:starting")
    metadata = MetadataStore(
        data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    storage = EventStorage(metadata, data_dir)
    catalog_stats_cache: dict[str, Any] = {
        "deadline": 0.0,
        "value": {},
        "compressed": {},
    }

    def snapshots() -> dict[str, Any]:
        exact = getattr(storage, "exact_index", None)
        analytics = getattr(storage, "analytics_index", None)
        slowlog = getattr(storage, "slowlog_index", None)
        now = time.monotonic()
        if now >= float(catalog_stats_cache["deadline"]):
            catalog_stats_cache["value"] = metadata.part_catalog_stats()
            catalog_stats_cache["compressed"] = metadata.catalog_store_stats()
            catalog_stats_cache["deadline"] = (
                now + CATALOG_STATS_CACHE_SECONDS
            )
        return {
            "indexStats": storage.search_index.stats(),
            "exactIndexStats": exact.stats() if exact is not None else {},
            "analyticsStats": analytics.stats() if analytics is not None else {},
            "slowlogIndexStats": slowlog.stats() if slowlog is not None else {},
            "catalogStats": dict(catalog_stats_cache["value"]),
            "compressedCatalogStats": dict(
                catalog_stats_cache["compressed"]
            ),
            "localBodyStats": storage.local_body_stats(),
        }

    settings = metadata.load_settings()
    archive: OssArchive | None = None
    if settings.oss_enabled:
        credential = load_credential(settings.credential_target)
        archive = OssArchive(settings, credential=credential)
    if SLOWLOG_INDEX_ENABLED:
        try:
            _advance_slowlog_index(
                metadata,
                storage,
                archive,
                publish=publish,
                generation=generation,
            )
        except Exception as exc:  # queue owns retries; other indexes must progress
            traceback.print_exc()
            publish(
                "running",
                phase="slowlog",
                token=f"{generation}:slowlog-error",
                error=str(exc),
                result=snapshots(),
            )
    if not settings.oss_enabled:
        publish(
            "idle",
            token=f"{generation}:oss-disabled",
            result=snapshots(),
        )
        return IDLE_EXIT_CODE
    metadata.reconcile_part_catalog_pending(
        limit=CATALOG_RECONCILE_BATCH_SIZE
    )
    try:
        metadata.backfill_catalog_store(
            limit=CATALOG_STORE_BACKFILL_BATCH_SIZE
        )
    except Exception as exc:
        traceback.print_exc()
        publish(
            "running",
            phase="catalog-store-backfill",
            token=f"{generation}:catalog-store-backfill-error",
            error=str(exc),
        )
    assert archive is not None
    # Only immutable, committed OSS objects are eligible. EventStorage still
    # opens a matching local body first, under the shared part lock, so the
    # normal hot path avoids OSS Range requests.
    #
    # Build the compact L0 catalog before any heavier index. A catalog miss
    # forces every query to open the Parquet body, while a completed table
    # catalog rejects the great majority of unrelated objects without an OSS
    # request. Returning after this batch also prevents the always-present
    # full-text backlog from starving catalog coverage indefinitely.
    catalogs = [
        part
        for part in metadata.missing_part_catalogs(limit=CATALOG_BATCH_SIZE)
        if str(part.get("oss_key") or "")
    ]
    if catalogs:
        totals = {"cataloged": 0, "rows": 0, "parts": 0}
        errors: list[str] = []
        last_part = catalogs[-1]
        publish(
            "running",
            phase="catalog",
            part=catalogs[0],
            token=f"{generation}:catalog:0:{catalogs[0]['sha256']}",
            result={**totals, "batchSize": len(catalogs)},
        )
        with ThreadPoolExecutor(
            max_workers=min(CATALOG_INDEX_WORKERS, len(catalogs)),
            thread_name_prefix="catalog-index-worker",
        ) as executor:
            futures = {
                executor.submit(
                    storage.ensure_part_catalog,
                    part,
                    archive,
                ): part
                for part in catalogs
            }
            for position, future in enumerate(as_completed(futures), start=1):
                part = futures[future]
                publish(
                    "running",
                    phase="catalog",
                    part=part,
                    token=(
                        f"{generation}:catalog:{position}:"
                        f"{part['sha256']}"
                    ),
                    result={**totals, "batchSize": len(catalogs)},
                )
                try:
                    result = future.result()
                except Exception as exc:
                    errors.append(f"{_part_name(part)}：{exc}")
                    continue
                totals["cataloged"] += int(result.get("cataloged") or 0)
                totals["rows"] += int(result.get("rows") or 0)
                totals["parts"] += 1
                last_part = part
        catalog_stats_cache["deadline"] = 0.0
        publish(
            "completed",
            phase="catalog",
            part=last_part,
            token=f"{generation}:cataloged-batch:{totals['parts']}",
            error="；".join(errors[:3]),
            result={
                **totals,
                "batchSize": len(catalogs),
                "failedParts": len(errors),
                **snapshots(),
            },
        )
        if not totals["parts"] and errors:
            raise RuntimeError("；".join(errors[:3]))
        # Continue into one exact-index batch in the same cycle so a large
        # catalog backlog cannot starve primary-key coverage.

    # 分析聚合排在 exact 之前：exact 阶段每轮都会 return，只要它还有积压，
    # 排在其后的阶段就永远拿不到调度，SQL / 事务 / 锁分析会被无限期饿死。
    # 单批只有 2 个分区，不会反过来饿死 exact。
    analytics_parts = _missing_analytics_parts(metadata, storage)
    if analytics_parts:
        totals = {"parts": 0, "rows": 0, "transactions": 0, "degraded": 0}
        errors: list[str] = []
        last_part = analytics_parts[-1]
        publish(
            "running",
            phase="analytics",
            part=analytics_parts[0],
            token=f"{generation}:analytics:0:{analytics_parts[0]['sha256']}",
            result={**totals, "batchSize": len(analytics_parts)},
        )
        with ThreadPoolExecutor(
            max_workers=min(ANALYTICS_WORKERS, len(analytics_parts)),
            thread_name_prefix="analytics-index-worker",
        ) as executor:
            futures = {
                executor.submit(
                    storage.ensure_part_analytics,
                    part,
                    archive,
                ): part
                for part in analytics_parts
            }
            for position, future in enumerate(as_completed(futures), start=1):
                part = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    errors.append(f"{_part_name(part)}：{exc}")
                    continue
                totals["parts"] += int(result.get("built") or 0)
                totals["rows"] += int(result.get("row_count") or 0)
                totals["transactions"] += int(result.get("txn_count") or 0)
                if str(result.get("sql_mode") or "statement") != "statement":
                    totals["degraded"] += 1
                last_part = part
                publish(
                    "running",
                    phase="analytics",
                    part=part,
                    token=f"{generation}:analytics:{position}:{part['sha256']}",
                    result={**totals, "batchSize": len(analytics_parts)},
                )
        publish(
            "completed",
            phase="analytics",
            part=last_part,
            token=f"{generation}:analytics-batch:{totals['parts']}",
            error="；".join(errors[:3]),
            result={
                **totals,
                "batchSize": len(analytics_parts),
                "failedParts": len(errors),
                **snapshots(),
            },
        )
        if not totals["parts"] and errors:
            raise RuntimeError("；".join(errors[:3]))
        # 不 return：本批分析聚合完成后继续推进 exact / structural /
        # 全文索引，避免任何一个重活阶段独占整轮把其它阶段饿死。

    # SELECT 扫描行数估算：每周期一小批（有条数与秒数双预算），放在早期
    # 返回的 exact 阶段之前，保证积压期间也能持续推进。失败不阻塞索引。
    analytics = getattr(storage, "analytics_index", None)
    if analytics is not None:
        try:
            from .select_explain import SelectExplainWorker

            SelectExplainWorker(str(analytics.manifest_path)).run_topup()
        except Exception:  # noqa: BLE001 - 估算是旁路，绝不拖垮索引
            traceback.print_exc()

    # 时间桶 rollup：长窗口分析的数据源，同样是旁路，失败不阻塞索引。
    try:
        rolled = _advance_rollup(metadata, storage)
        if rolled:
            publish(
                "completed",
                phase="rollup",
                part=None,
                token=f"{generation}:rollup:{rolled}",
                result={"buckets": rolled, **snapshots()},
            )
    except Exception:  # noqa: BLE001
        traceback.print_exc()

    exact_parts = (
        _missing_exact_parts(metadata, storage)
        if getattr(storage, "exact_index", None) is not None
        else []
    )
    if exact_parts:
        totals = {"segments": 0, "parts": 0, "rows": 0, "exact_docs": 0}
        errors: list[str] = []
        last_part = exact_parts[-1]
        publish(
            "running",
            phase="exact",
            part=exact_parts[0],
            token=f"{generation}:exact:0:{exact_parts[0]['sha256']}",
            result={**totals, "batchSize": len(exact_parts)},
        )
        with ThreadPoolExecutor(
            max_workers=min(EXACT_INDEX_WORKERS, len(exact_parts)),
            thread_name_prefix="exact-index-worker",
        ) as executor:
            futures = {
                executor.submit(
                    storage.ensure_exact_segment,
                    [part],
                    archive,
                ): part
                for part in exact_parts
            }
            for position, future in enumerate(as_completed(futures), start=1):
                part = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    errors.append(f"{_part_name(part)}：{exc}")
                    continue
                totals["segments"] += int(result.get("built") or 0)
                totals["parts"] += int(result.get("part_count") or 0)
                totals["rows"] += int(result.get("row_count") or 0)
                totals["exact_docs"] += int(result.get("exact_docs") or 0)
                last_part = part
                publish(
                    "running",
                    phase="exact",
                    part=part,
                    token=f"{generation}:exact:{position}:{part['sha256']}",
                    result={**totals, "batchSize": len(exact_parts)},
                )
        publish(
            "completed",
            phase="exact",
            part=last_part,
            token=f"{generation}:exact-indexed-batch:{totals['parts']}",
            error="；".join(errors[:3]),
            result={
                **totals,
                "batchSize": len(exact_parts),
                "failedParts": len(errors),
                **snapshots(),
            },
        )
        if not totals["parts"] and errors:
            raise RuntimeError("；".join(errors[:3]))
        # Continue into structural and full-text quotas in the same cycle.

    structural = _missing_structural_parts(metadata, storage)
    if structural:
        totals = {
            "indexed": 0,
            "row_groups": 0,
            "rows": 0,
            "parts": 0,
            "released_bytes": 0,
        }
        errors: list[str] = []
        last_part = structural[-1]
        publish(
            "running",
            phase="structure",
            part=structural[0],
            token=f"{generation}:structure:0:{structural[0]['sha256']}",
            result={**totals, "batchSize": len(structural)},
        )
        with ThreadPoolExecutor(
            max_workers=min(STRUCTURAL_INDEX_WORKERS, len(structural)),
            thread_name_prefix="structural-index-worker",
        ) as executor:
            futures = {
                executor.submit(
                    storage.ensure_part_structural_index,
                    part,
                    archive,
                ): part
                for part in structural
            }
            for position, future in enumerate(as_completed(futures), start=1):
                part = futures[future]
                publish(
                    "running",
                    phase="structure",
                    part=part,
                    token=(
                        f"{generation}:structure:{position}:"
                        f"{part['sha256']}"
                    ),
                    result={**totals, "batchSize": len(structural)},
                )
                try:
                    result = future.result()
                except Exception as exc:
                    errors.append(f"{_part_name(part)}：{exc}")
                    continue
                for key in ("indexed", "row_groups", "rows"):
                    totals[key] += int(result.get(key) or 0)
                totals["parts"] += 1
                totals["released_bytes"] += storage.release_archived_body(part)
                last_part = part
        publish(
            "completed",
            phase="structure",
            part=last_part,
            token=f"{generation}:structured-batch:{totals['parts']}",
            error="；".join(errors[:3]),
            result={
                **totals,
                "batchSize": len(structural),
                "failedParts": len(errors),
                **snapshots(),
            },
        )
        if not totals["parts"] and errors:
            raise RuntimeError("；".join(errors[:3]))
        # Keep the compact structural directory advancing quickly, but also
        # build a few complete newest indexes every cycle. Otherwise a large
        # structural backlog can starve keyword coverage for hours.


    missing = _missing_index_parts(
        metadata,
        storage,
        limit=FULL_INDEX_BATCH_SIZE,
    )
    if missing:
        totals = {
            "indexed": 0,
            "row_groups": 0,
            "rows": 0,
            "parts": 0,
            "released_bytes": 0,
        }
        errors: list[str] = []
        last_part = missing[-1]
        for position, part in enumerate(missing, start=1):
            publish(
                "running",
                phase="index",
                part=part,
                token=(
                    f"{generation}:index:{position}:"
                    f"{part['sha256']}"
                ),
                result={**totals, "batchSize": len(missing)},
            )
            try:
                result = storage.ensure_part_index(part, archive)
            except Exception as exc:
                errors.append(f"{_part_name(part)}：{exc}")
                continue
            for key in ("indexed", "row_groups", "rows"):
                totals[key] += int(result.get(key) or 0)
            totals["parts"] += 1
            totals["released_bytes"] += storage.release_archived_body(part)
            last_part = part
        publish(
            "completed",
            phase="index",
            part=last_part,
            token=f"{generation}:indexed-batch:{totals['parts']}",
            error="；".join(errors[:3]),
            result={
                **totals,
                "batchSize": len(missing),
                "failedParts": len(errors),
                **snapshots(),
            },
        )
        if not totals["parts"] and errors:
            raise RuntimeError("；".join(errors[:3]))
        return 0

    publish("idle", token=f"{generation}:idle", result=snapshots())
    return IDLE_EXIT_CODE


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=data_root())
    parser.add_argument(
        "--generation",
        default=os.environ.get("RDS_BINLOG_INDEX_GENERATION", ""),
    )
    args = parser.parse_args()
    generation = str(args.generation or os.getpid())
    try:
        return run_one(args.data_dir.resolve(), generation)
    except BaseException as exc:
        paths = ensure_data_dirs(args.data_dir.resolve())
        write_json_status(
            paths["index"] / WORKER_PROGRESS_NAME,
            {
                "generation": generation,
                "pid": os.getpid(),
                "state": "failed",
                "phase": "",
                "currentPart": "",
                "progressToken": f"{generation}:failed",
                "lastError": str(exc),
                "traceback": traceback.format_exc(limit=12),
            },
        )
        print(f"index worker failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
