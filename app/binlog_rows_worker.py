"""Continuously ingest collected binlog files into ClickHouse ``binlog_rows_v1``.

Lane 0 walks newest-first so fresh data becomes queryable within minutes.
Optional backfill lanes walk a configured window. Raw-archived files are
downloaded, verified (size/SHA256/CRC64) and parsed once with the native
parser; the NDJSON chunks go straight into ClickHouse (no per-row Python).
Parquet-backed files are loaded with ``s3()``. Every file goes through a
lane-local buffer, is row-count verified, moved through the materialized
views and only then recorded in the manifest that proves coverage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .binlog_lite import RawBinlogError
from .binlog_rows import (
    NON_BINLOG_HOSTS, STAGE_COLUMN_NAMES, WORKER_BUFFER_TABLE, WORKER_LANES_MAX, ChHttp, RowsIngestor, _epoch_us,
)
from .config import data_root, ensure_data_dirs

LOGGER = logging.getLogger("binlog_rows_worker")
STATUS_NAME = "binlog-rows-worker-status.json"
NEWLINE = bytes([10])
PARQUET_STRUCTURE_OVERRIDES = {"row_query": "LowCardinality(String)"}


def _env_windows(value: str) -> list[tuple[str, str, str]]:
    """``instance|startISO|endISO;...`` -> list; used for backfill lanes and external Parquet windows."""
    out = []
    for item in (value or "").split(";"):
        parts = [p.strip() for p in item.split("|")]
        if len(parts) == 3 and all(parts):
            out.append((parts[0], parts[1], parts[2]))
    return out


DOWNLOAD_STREAMS = max(1, min(int(os.environ.get("RDS_BINLOG_ROWS_DOWNLOAD_STREAMS", "4") or 4), 8))


def download_ranges(bucket: Any, key: str, size: int, path: Path, stop: threading.Event) -> None:
    """Parallel ranged GETs into a preallocated file; a single stream is ~11 MB/s on this network."""
    from concurrent.futures import ThreadPoolExecutor
    with path.open("wb") as handle:
        handle.truncate(size)
    if size == 0:
        return
    step = -(-size // DOWNLOAD_STREAMS)
    ranges = [(start, min(start + step, size)) for start in range(0, size, step)]

    def fetch(bounds: tuple[int, int]) -> None:
        start, end = bounds
        response = bucket.get_object(key, byte_range=(start, end - 1))
        written = 0
        try:
            with path.open("r+b") as out:
                out.seek(start)
                while chunk := response.read(1024 ** 2):
                    if stop.is_set():
                        raise RawBinlogError("写入进程停止", "BINLOG_ROWS_STOPPED")
                    written += len(chunk)
                    if written > end - start:
                        raise RawBinlogError("分段下载超过请求长度", "RAW_INDEX_VERIFY_FAILED")
                    out.write(chunk)
        finally:
            response.close()
        if written != end - start:
            raise RawBinlogError("分段下载长度不符", "RAW_INDEX_VERIFY_FAILED")

    with ThreadPoolExecutor(max_workers=len(ranges), thread_name_prefix="oss-range") as pool:
        for future in [pool.submit(fetch, r) for r in ranges]:
            future.result()


def parser_command() -> list[str]:
    """Row-store mode of the collector's parser: same event ids and row images, ~40% less CPU."""
    from .parser_bridge import parser_executable
    override = os.environ.get("RDS_BINLOG_ROWS_PARSER", "").strip()
    return [override or str(parser_executable()), "--slim"]


def slice_memory_limits() -> tuple[str, int]:
    """memory.stat of the cgroup slice that holds every service (host path bind-mounted read-only).

    When anonymous memory leaves too little room under the slice's memory.high for the hot file pages,
    the kernel refaults them from disk and the read cap stalls the whole host. memory.current is no
    signal: clean page cache fills it up to the line and is cheap to reclaim. Empty path disables the
    check, which is logged at start.
    """
    path = os.environ.get("RDS_BINLOG_ROWS_SLICE_STAT_FILE", "").strip()
    limit = int(float(os.environ.get("RDS_BINLOG_ROWS_SLICE_ANON_MAX_GIB", "8") or 8) * 1024 ** 3)
    return path, limit


def read_slice_memory(path: str) -> int | None:
    """Anonymous bytes from a cgroup memory.stat file."""
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith("anon "):
                return int(line.split()[1])
    except (OSError, ValueError):
        return None
    return None


class LineCountingStream:
    """File-like view of parser stdout that counts NDJSON records while ClickHouse reads it."""

    def __init__(self, raw: Any, stop: threading.Event):
        self.raw, self.stop, self.lines, self._last = raw, stop, 0, NEWLINE

    def read(self, size: int = -1) -> bytes:
        if self.stop.is_set():
            raise RawBinlogError("写入进程停止", "BINLOG_ROWS_STOPPED")
        data = self.raw.read(size if size and size > 0 else 1024 * 1024)
        if data:
            self.lines += data.count(NEWLINE)
            self._last = data[-1:]
        elif self._last != NEWLINE:
            self.lines += 1  # final record without trailing newline
            self._last = NEWLINE
        return data


class Claims:
    """In-process ownership. A committed file is never handed out again, even to a lane whose
    candidate list was computed before the commit."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: set[str] = set()
        self._done: set[str] = set()

    def take(self, file_id: str) -> bool:
        with self._lock:
            if file_id in self._ids or file_id in self._done:
                return False
            self._ids.add(file_id)
            return True

    def release(self, file_id: str) -> None:
        with self._lock:
            self._ids.discard(file_id)

    def finish(self, file_id: str) -> None:
        with self._lock:
            self._ids.discard(file_id)
            self._done.add(file_id)


class Worker:
    def __init__(self, data_dir: Path, *, lanes: int, backfill: list[tuple[str, str, str]],
                 external_parquet: list[tuple[str, str, str]]):
        from .metadata import MetadataStore
        from .storage import EventStorage
        self.data_dir = data_dir
        self.metadata = MetadataStore(data_dir / "metadata.sqlite3", run_migrations=False)
        self.storage = EventStorage(self.metadata, data_dir)
        client = ChHttp.from_env()
        if client is None:
            raise SystemExit("binlog rows worker needs CLICKHOUSE_USER/CLICKHOUSE_PASSWORD")
        self.ch = client
        self.lane_specs: list[tuple[str, Any]] = [("live", None)] * max(1, lanes) + [("window", w) for w in backfill]
        self.lanes = len(self.lane_specs)
        self.backfill = backfill
        self.external_parquet = external_parquet
        self.claims = Claims()
        self.stop = threading.Event()
        self.status: dict[str, Any] = {"lanes": {}, "startedAt": datetime.now(UTC).isoformat()}
        self.status_lock = threading.Lock()
        self._ingested: dict[str, tuple[float, set[str]]] = {}
        self._archive = None
        self._archive_at = 0.0

    # -- shared helpers -------------------------------------------------------
    def archive(self):
        from .credentials import load_credential
        from .oss_store import OssArchive
        if self._archive is None or time.monotonic() - self._archive_at > 600:
            settings = self.metadata.load_settings()
            self._archive = OssArchive(settings, credential=load_credential(settings.credential_target)) \
                if settings.oss_enabled else None
            self._archive_at = time.monotonic()
        return self._archive

    def ingested(self, instance: str, *, fresh: bool = False) -> set[str]:
        cached = self._ingested.get(instance)
        if cached and not fresh and time.monotonic() - cached[0] < 60:
            return cached[1]
        text = self.ch.execute("SELECT file_id FROM insight.binlog_rows_files_v1 FINAL WHERE instance_id = {i:String} FORMAT TSV",
                               params={"i": instance}, settings={"max_execution_time": 60}, timeout=80)
        ids = set(text.split())
        self._ingested[instance] = (time.monotonic(), ids)
        return ids

    def mark_ingested(self, instance: str, file_id: str) -> None:
        cached = self._ingested.get(instance)
        if cached:
            cached[1].add(file_id)

    def publish(self, lane: int, **fields: Any) -> None:
        with self.status_lock:
            state = self.status["lanes"].setdefault(str(lane), {})
            state.update(fields, updatedAt=datetime.now(UTC).isoformat())
            path = self.storage.paths["index"] / STATUS_NAME
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.status, ensure_ascii=False, sort_keys=True))
            os.replace(tmp, path)

    def inflight_dir(self) -> Path:
        path = self.storage.paths["index"] / "binlog-rows-inflight"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def commit(self, ingestor: RowsIngestor, entry: dict[str, Any], part_keys: list[str], rows: int, source: str,
               rq_complete: bool, lane: int) -> None:
        """Move + manifest behind a durable marker, so an interrupted move is purged on the next start."""
        marker = self.inflight_dir() / f"{entry['file_id']}.json"
        tmp = marker.with_suffix(".tmp")
        tmp.write_text(json.dumps({"instance_id": entry["instance_id"], "file_id": entry["file_id"],
                                   "part_keys": part_keys, "file": entry["source_file_name"]}))
        os.replace(tmp, marker)
        ingestor.move(part_keys, f"binlog-rows-l{lane}")  # purges its own partial rows on failure
        ingestor.record(entry, rows, source, rq_complete)
        marker.unlink(missing_ok=True)

    def recover_inflight(self) -> int:
        """Purge rows of moves that never reached the manifest (crash, kill, container stop)."""
        purged = 0
        for marker in sorted(self.inflight_dir().glob("*.json")):
            try:
                info = json.loads(marker.read_text())
            except ValueError:
                LOGGER.error("BINLOG_ROWS_INFLIGHT_UNREADABLE marker=%s", marker.name)
                continue
            if info["file_id"] in self.ingested(info["instance_id"], fresh=True):
                marker.unlink(missing_ok=True)
                continue
            LOGGER.warning("BINLOG_ROWS_INFLIGHT_PURGE file=%s parts=%s", info.get("file"), len(info["part_keys"]))
            self.ch.execute("DELETE FROM insight.binlog_rows_v1 WHERE _source_part_key IN "
                            "(SELECT arrayJoin(JSONExtract({k:String}, 'Array(String)')))",
                            params={"k": json.dumps(info["part_keys"])},
                            settings={"lightweight_deletes_sync": 2, "max_execution_time": 1800}, timeout=1900)
            marker.unlink(missing_ok=True)
            purged += 1
        return purged

    def collector_lag_seconds(self) -> int:
        with self.metadata.connection() as conn:
            row = conn.execute(
                "SELECT max(log_end_utc) AS e FROM binlog_files WHERE state = 'done' AND host_instance_id NOT IN ("
                + ", ".join("?" * len(NON_BINLOG_HOSTS)) + ") AND instr(log_file_name, '/') = 0 "
                "AND log_begin_utc >= ?", (*NON_BINLOG_HOSTS,
                                          (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"))
            ).fetchone()
        if not row or not row["e"]:
            return 0
        return int(time.time() - _epoch_us(row["e"]) / 1e6)

    def clickhouse_memory(self) -> int:
        text = self.ch.execute("SELECT value FROM system.metrics WHERE metric = 'MemoryTracking' FORMAT TSV",
                               settings={"max_execution_time": 20}, timeout=30)
        return int(text.strip() or 0)

    def wait_for_capacity(self, lane: int) -> bool:
        """Yield to the collector and interactive queries; every pause reason is logged."""
        limit = int(float(os.environ.get("RDS_BINLOG_ROWS_CH_MEMORY_LIMIT_GIB", "4.2")) * 1024 ** 3)
        slice_path, slice_limit = slice_memory_limits()
        while not self.stop.is_set():
            reason = ""
            used = read_slice_memory(slice_path) if slice_path else None
            if slice_path and used is None:
                reason = f"slice-memory-unreadable: {slice_path}"
            elif used is not None and used > slice_limit:
                reason = "slice-anon-memory"
            if not reason:
                try:
                    if self.clickhouse_memory() > limit:
                        reason = "clickhouse-memory"
                except RawBinlogError as exc:
                    reason = f"clickhouse-unavailable: {exc}"
            if not reason and self.lane_specs[lane][0] == "window" and self.collector_lag_seconds() > 1200:
                reason = "collector-lag"
            if not reason:
                return True
            LOGGER.warning("BINLOG_ROWS_PAUSED lane=%s reason=%s", lane, reason)
            self.publish(lane, state="paused", reason=reason)
            self.stop.wait(15)
        return False

    # -- candidate selection --------------------------------------------------
    def candidates(self, lane: int, limit: int = 200) -> list[dict[str, Any]]:
        settings = self.metadata.load_settings()
        floor = (datetime.now(UTC) - timedelta(days=max(int(settings.retention_days or 60) - 1, 1))
                 ).strftime("%Y-%m-%dT%H:%M:%SZ")
        kind, window = self.lane_specs[lane]
        sql = ("SELECT b.id, b.instance_id, b.host_instance_id, b.log_file_name, b.log_begin_utc, b.log_end_utc, "
               "r.descriptor AS raw_descriptor FROM binlog_files b LEFT JOIN raw_binlog_archives r ON r.file_id = b.id "
               "WHERE b.state = 'done' AND b.host_instance_id NOT IN (" + ", ".join("?" * len(NON_BINLOG_HOSTS)) + ") "
               "AND instr(b.log_file_name, '/') = 0 AND b.log_end_utc >= ?")
        args: list[Any] = [*NON_BINLOG_HOSTS, floor]
        if window:
            sql += " AND b.instance_id = ? AND b.log_end_utc >= ? AND b.log_begin_utc < ?"
            args += [window[0], window[1], window[2]]
        sql += " ORDER BY b.log_begin_utc DESC LIMIT ?"
        args.append(5000)
        out = []
        with self.metadata.connection() as conn:
            for row in conn.execute(sql, args):
                entry = {"file_id": row["id"], "instance_id": row["instance_id"], "host_instance_id": row["host_instance_id"],
                         "source_file_name": row["log_file_name"], "lo": _epoch_us(row["log_begin_utc"]),
                         "hi": _epoch_us(row["log_end_utc"]), "begin": row["log_begin_utc"]}
                if entry["file_id"] in self.ingested(entry["instance_id"]):
                    continue
                if row["raw_descriptor"]:
                    entry["kind"] = "raw"
                    entry["descriptor"] = json.loads(row["raw_descriptor"])
                else:
                    if any(i == entry["instance_id"] and s <= entry["begin"] < e for i, s, e in self.external_parquet):
                        continue  # another backfill owns this Parquet window
                    parts = conn.execute("SELECT oss_key, row_count, oss_length FROM parquet_parts WHERE binlog_id = ? "
                                         "ORDER BY path", (entry["file_id"],)).fetchall()
                    if not parts or any(p["oss_length"] or not p["oss_key"] for p in parts):
                        continue
                    entry["kind"] = "parquet"
                    entry["parts"] = [(p["oss_key"], int(p["row_count"])) for p in parts]
                out.append(entry)
                if len(out) >= limit:
                    break
        return out

    # -- ingestion ------------------------------------------------------------
    def ingest_raw(self, ingestor: RowsIngestor, entry: dict[str, Any], lane: int) -> int:
        from oss2.utils import Crc64
        archive = self.archive()
        descriptor = entry["descriptor"]
        raw = descriptor["raw"]
        self.storage.raw_binlogs.verify(archive, descriptor)
        scratch_root = self.storage.paths["scratch"] / "binlog-rows"
        scratch_root.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(scratch_root).free < int(raw["size_bytes"]) + 20 * 1024 ** 3:
            raise RawBinlogError("本地磁盘余量不足 20GiB，暂缓解析", "RAW_INDEX_DISK_RESERVE")
        with tempfile.TemporaryDirectory(prefix=f"lane{lane}-", dir=scratch_root) as scratch:
            root = Path(scratch)
            source = root / "source.binlog"
            download_ranges(archive.bucket, raw["oss_key"], int(raw["size_bytes"]), source, self.stop)
            digest, crc, size = hashlib.sha256(), Crc64(), 0
            with source.open("rb") as handle:
                while chunk := handle.read(8 * 1024 ** 2):
                    size += len(chunk)
                    digest.update(chunk)
                    crc(chunk)
            if size != int(raw["size_bytes"]) or digest.hexdigest() != raw["sha256"] or str(crc.crc) != str(raw["crc64"]):
                raise RawBinlogError("原档长度/SHA256/CRC64校验失败", "RAW_INDEX_VERIFY_FAILED")
            ingestor.truncate()
            stderr_path = root / "parser.stderr"
            with stderr_path.open("wb") as stderr:
                process = subprocess.Popen(
                    [*parser_command(), "--input", str(source), "--source-file-id", entry["file_id"],
                     "--flavor", descriptor.get("flavor") or "mysql"],
                    stdout=subprocess.PIPE, stderr=stderr, stdin=subprocess.DEVNULL)
                stream = LineCountingStream(process.stdout, self.stop)
                try:
                    ingestor.load_ndjson_stream(stream, entry)
                except BaseException:
                    process.kill()
                    raise
                finally:
                    returncode = process.wait(timeout=600)
            if returncode != 0:
                tail = stderr_path.read_bytes()[-400:].decode("utf-8", errors="replace")
                raise RawBinlogError(f"解析器退出码 {returncode}：{tail}", "PARSER_FAILED")
            total = stream.lines
        buffered = ingestor.buffered_rows()
        if buffered != total:
            ingestor.truncate()
            raise RawBinlogError(f"缓冲行数 {buffered} 与解析行数 {total} 不一致", "BINLOG_ROWS_VERIFY_FAILED")
        self.commit(ingestor, entry, ["raw:" + entry["file_id"]], total, "raw", True, lane)
        ingestor.truncate()
        return total

    def ingest_parquet(self, ingestor: RowsIngestor, entry: dict[str, Any], lane: int) -> int:
        from .clickhouse_client import SOURCE_COLUMN_TYPES
        types = dict(SOURCE_COLUMN_TYPES)
        wanted = [n for n in STAGE_COLUMN_NAMES if n != "_source_part_key"]
        sources = [*wanted, "sql_text"]
        settings = self.metadata.load_settings()
        base = f"https://{settings.oss_bucket.strip().lower()}.oss-{settings.oss_region_id.strip().lower()}-internal.aliyuncs.com/"
        keys = [k for k, _ in entry["parts"]]
        if any(ch in k for k in keys for ch in "{},'"):
            raise RawBinlogError("Parquet 对象名含不安全字符", "BINLOG_ROWS_UNSAFE_KEY")
        expect = {f"{settings.oss_bucket.strip().lower()}/{k}": r for k, r in entry["parts"]}
        url = base + ("{" + ",".join(keys) + "}" if len(keys) > 1 else keys[0])
        rq_complete = True
        ingestor.truncate()
        for with_rq in (True, False):
            names = [n for n in sources if with_rq or n != "row_query"]
            structure = ", ".join(f"{n} {PARQUET_STRUCTURE_OVERRIDES.get(n, types[n])}" for n in names)
            row_query = ("toLowCardinality(if(row_query = '' AND sql_kind = 'ORIGINAL', sql_text, row_query)) AS row_query"
                         if with_rq else "toLowCardinality(if(sql_kind = 'ORIGINAL', sql_text, '')) AS row_query")
            select = ", ".join(row_query if n == "row_query" else n for n in wanted)
            try:
                ingestor.ch.execute(
                    f"INSERT INTO {ingestor.buffer} ({', '.join(STAGE_COLUMN_NAMES)}) SELECT {select}, _path "
                    f"FROM s3('{url}', 'Parquet', '{structure}')",
                    settings={**RowsIngestor.LOAD_SETTINGS, "input_format_parquet_memory_high_watermark": 201326592,
                              "input_format_parquet_enable_row_group_prefetch": 0, "max_download_threads": 1,
                              "input_format_parquet_max_block_size": 4096, "log_comment": f"binlog-rows-l{lane}-parquet"},
                    timeout=3600)
                break
            except RawBinlogError as exc:
                ingestor.truncate()
                if not with_rq:
                    raise
                LOGGER.warning("BINLOG_ROWS_PARQUET_NORQ file=%s reason=%s", entry["source_file_name"], str(exc)[:200])
                rq_complete = False
        counts = ingestor.ch.rows(f"SELECT _source_part_key AS k, count() AS c FROM {ingestor.buffer} GROUP BY k",
                                  settings={"max_execution_time": 120}, timeout=140)
        got = {r["k"]: int(r["c"]) for r in counts}
        if got != expect:
            ingestor.truncate()
            raise RawBinlogError("Parquet 行数与元数据不一致", "BINLOG_ROWS_VERIFY_FAILED")
        self.commit(ingestor, entry, list(expect), sum(expect.values()), "parquet", rq_complete, lane)
        ingestor.truncate()
        return sum(expect.values())

    def run_lane(self, lane: int) -> None:
        ingestor = RowsIngestor(self.ch, buffer_table=f"{WORKER_BUFFER_TABLE}_{lane}")
        failures: dict[str, int] = {}
        while not self.stop.is_set():
            try:
                todo = self.candidates(lane)
            except Exception as exc:  # metadata busy etc.; retried, always logged
                LOGGER.warning("BINLOG_ROWS_CANDIDATES_FAILED lane=%s error=%s", lane, exc)
                self.stop.wait(30)
                continue
            todo = [e for e in todo if failures.get(e["file_id"], 0) < 3]
            if not todo:
                self.publish(lane, state="idle", reason="caught-up" if self.lane_specs[lane][0] == "live" else "window-complete")
                self.stop.wait(30)
                continue
            for entry in todo:
                if self.stop.is_set() or not self.wait_for_capacity(lane):
                    break
                if not self.claims.take(entry["file_id"]):
                    continue
                if entry["file_id"] in self.ingested(entry["instance_id"]):
                    self.claims.finish(entry["file_id"])  # committed by another lane after this list was built
                    continue
                started = time.monotonic()
                committed = False
                try:
                    self.publish(lane, state="running", file=entry["source_file_name"], kind=entry["kind"])
                    rows = (self.ingest_raw if entry["kind"] == "raw" else self.ingest_parquet)(ingestor, entry, lane)
                    self.mark_ingested(entry["instance_id"], entry["file_id"])
                    committed = True
                    seconds = round(time.monotonic() - started, 1)
                    LOGGER.info("BINLOG_ROWS_INGESTED lane=%s file=%s kind=%s rows=%s seconds=%s",
                                lane, entry["source_file_name"], entry["kind"], rows, seconds)
                    self.publish(lane, state="running", lastFile=entry["source_file_name"], lastRows=rows,
                                 lastSeconds=seconds, lastEventEnd=entry["hi"])
                except Exception as exc:
                    failures[entry["file_id"]] = failures.get(entry["file_id"], 0) + 1
                    LOGGER.error("BINLOG_ROWS_INGEST_FAILED lane=%s file=%s kind=%s attempt=%s error=%s", lane,
                                 entry["source_file_name"], entry["kind"], failures[entry["file_id"]], str(exc)[:300])
                    self.publish(lane, state="error", lastError=str(exc)[:300], errorFile=entry["source_file_name"])
                    try:
                        ingestor.truncate()
                    except RawBinlogError:
                        pass
                    self.stop.wait(10)
                finally:
                    (self.claims.finish if committed else self.claims.release)(entry["file_id"])
                if self.lane_specs[lane][0] == "live":
                    break  # re-rank so the newest unclaimed file is always next

    def run(self) -> None:
        purged = self.recover_inflight()
        if purged:
            LOGGER.warning("BINLOG_ROWS_INFLIGHT_RECOVERED files=%s", purged)
        threads = [threading.Thread(target=self.run_lane, args=(lane,), name=f"lane-{lane}", daemon=True)
                   for lane in range(self.lanes)]
        for thread in threads:
            thread.start()
        try:
            while any(t.is_alive() for t in threads):
                time.sleep(5)
        except KeyboardInterrupt:
            self.stop.set()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=data_root())
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ensure_data_dirs(args.data_dir)
    slice_path, slice_limit = slice_memory_limits()
    if slice_path:
        LOGGER.info("BINLOG_ROWS_SLICE_GUARD path=%s max_bytes=%s", slice_path, slice_limit)
    else:
        LOGGER.warning("BINLOG_ROWS_SLICE_GUARD_OFF reason=RDS_BINLOG_ROWS_SLICE_STAT_FILE unset")
    backfill = _env_windows(os.environ.get("RDS_BINLOG_ROWS_BACKFILL_WINDOWS", ""))
    live = max(1, int(os.environ.get("RDS_BINLOG_ROWS_LIVE_LANES", "2") or 2))
    if live + len(backfill) > WORKER_LANES_MAX:
        raise SystemExit(f"at most {WORKER_LANES_MAX} lanes (live + backfill windows)")
    worker = Worker(args.data_dir, lanes=live, backfill=backfill,
                    external_parquet=_env_windows(os.environ.get("RDS_BINLOG_ROWS_EXTERNAL_PARQUET_WINDOWS", "")))
    # docker stop: finish the move in progress (bounded by stop_grace_period), start nothing new.
    signal.signal(signal.SIGTERM, lambda _signum, _frame: worker.stop.set())
    LOGGER.info("BINLOG_ROWS_WORKER_START live_lanes=%s backfill=%s", live, backfill)
    worker.run()
    LOGGER.info("BINLOG_ROWS_WORKER_STOPPED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
