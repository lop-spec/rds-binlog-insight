from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb
import pyarrow.parquet as parquet

try:
    import pysqlite3 as sqlite3
except ImportError:  # pragma: no cover - Windows development fallback
    import sqlite3

from app.config import Settings
from app.credentials import load_credential
from app.metadata import SLOW_LOG_FILE_PREFIX, TABULARIS_AUDIT_FILE_PREFIX
from app.oss_store import OssArchive


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TOP_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "source_file_name",
    "end_position",
    "row_index",
)


def _identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _canonical_rows(rows: list[tuple[Any, ...]]) -> list[list[Any]]:
    return [
        [str(row[0]), int(row[1]), str(row[2]), int(row[3]), int(row[4])]
        for row in rows
    ]


def _canonical_hash(rows: list[tuple[Any, ...]]) -> str:
    payload = json.dumps(
        _canonical_rows(rows),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _top_key(row: tuple[Any, ...]) -> tuple[int, str, int, int]:
    return int(row[1]), str(row[2]), int(row[3]), int(row[4])


def _probe_health(url: str, max_seconds: float, host_header: str = "") -> float:
    started = time.monotonic()
    request = Request(url, headers={"Host": host_header} if host_header else {})
    with urlopen(request, timeout=max(max_seconds + 1.0, 2.0)) as response:
        body = response.read(16 * 1024)
        status = int(response.status)
    elapsed = time.monotonic() - started
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Production health returned invalid JSON: {exc}") from exc
    if status != 200 or not bool(payload.get("ok")):
        raise RuntimeError(f"Production health failed: HTTP {status}")
    if elapsed > max_seconds:
        raise RuntimeError(
            f"Production health exceeded {max_seconds:.3f}s: {elapsed:.3f}s"
        )
    return elapsed


def _open_metadata(path: Path):
    uri = f"file:{path.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _candidate_parts(
    metadata_path: Path,
    *,
    start_epoch_us: int,
    end_epoch_us: int,
    instance: str,
    database: str,
    table: str,
) -> tuple[Settings, list[dict[str, Any]], int]:
    with _open_metadata(metadata_path) as connection:
        settings_row = connection.execute(
            "SELECT value_json FROM app_settings WHERE singleton = 1"
        ).fetchone()
        if settings_row is None:
            raise RuntimeError("app_settings singleton is missing")
        settings = Settings.from_mapping(json.loads(str(settings_row["value_json"])))
        rows = connection.execute(
            """
            SELECT p.*, b.instance_id, b.log_file_name,
                   b.log_begin_utc, b.log_end_utc
            FROM parquet_parts p
            JOIN binlog_files b ON b.id = p.binlog_id
            WHERE p.max_event_epoch_us >= ?
              AND p.min_event_epoch_us <= ?
              AND b.query_visible = 1
              AND b.instance_id = ?
              AND b.log_file_name NOT LIKE ?
              AND b.log_file_name NOT LIKE ?
            ORDER BY p.min_event_epoch_us, p.max_event_epoch_us, p.path
            """,
            (
                int(start_epoch_us),
                int(end_epoch_us),
                instance,
                TABULARIS_AUDIT_FILE_PREFIX + "%",
                SLOW_LOG_FILE_PREFIX + "%",
            ),
        ).fetchall()
        raw_parts = [dict(row) for row in rows]
        catalogs: dict[str, dict[str, Any]] = {}
        for offset in range(0, len(raw_parts), 300):
            chunk = raw_parts[offset : offset + 300]
            placeholders = ",".join("?" for _ in chunk)
            catalog_rows = connection.execute(
                "SELECT path, sha256, databases_json, tables_json "
                f"FROM parquet_part_catalog WHERE path IN ({placeholders})",
                [str(part["path"]) for part in chunk],
            ).fetchall()
            for row in catalog_rows:
                catalogs[str(row["path"])] = {
                    "sha256": str(row["sha256"]),
                    "databases": json.loads(str(row["databases_json"])),
                    "tables": json.loads(str(row["tables_json"])),
                }

    database_lower = database.lower()
    table_lower = table.lower()
    selected: list[dict[str, Any]] = []
    for part in raw_parts:
        catalog = catalogs.get(str(part["path"]))
        if not catalog or catalog["sha256"] != str(part["sha256"]):
            continue
        if database_lower not in {
            str(value).strip().lower() for value in catalog["databases"]
        }:
            continue
        if table_lower not in {
            str(value).strip().lower() for value in catalog["tables"]
        }:
            continue
        selected.append(part)
    return settings, selected, len(raw_parts)


class ClickHouseHttp:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def _headers(self) -> dict[str, str]:
        return {
            "X-ClickHouse-User": self.user,
            "X-ClickHouse-Key": self.password,
        }

    def query(
        self,
        sql: str,
        *,
        parameters: dict[str, str | int] | None = None,
        timeout: int = 900,
    ) -> str:
        query_parameters: dict[str, str | int] = {
            "query": sql,
            "max_threads": 2,
            "use_query_cache": 0,
        }
        for key, value in (parameters or {}).items():
            query_parameters[f"param_{key}"] = value
        connection = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            connection.request(
                "POST",
                "/?" + urlencode(query_parameters),
                body=b"",
                headers=self._headers(),
            )
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise RuntimeError(
                    f"ClickHouse query failed: HTTP {response.status}: {body[:1000]}"
                )
            return body
        finally:
            connection.close()

    def insert_parquet(self, table: str, path: Path, token: str) -> None:
        query_parameters: dict[str, str | int] = {
            "query": f"INSERT INTO {table} FORMAT Parquet",
            "max_threads": 2,
            "max_insert_threads": 1,
            "input_format_parallel_parsing": 0,
            "input_format_parquet_allow_missing_columns": 1,
            "input_format_defaults_for_omitted_fields": 1,
            "input_format_null_as_default": 1,
            "insert_deduplication_token": token,
            "wait_end_of_query": 1,
        }
        connection = http.client.HTTPConnection(self.host, self.port, timeout=1800)
        try:
            connection.putrequest("POST", "/?" + urlencode(query_parameters))
            for name, value in self._headers().items():
                connection.putheader(name, value)
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Content-Length", str(path.stat().st_size))
            connection.endheaders()
            with path.open("rb") as source:
                while chunk := source.read(4 * 1024 * 1024):
                    connection.send(chunk)
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise RuntimeError(
                    f"ClickHouse insert failed: HTTP {response.status}: {body[:1000]}"
                )
        finally:
            connection.close()


def _scan_source_part(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    start_epoch_us: int,
    end_epoch_us: int,
    instance: str,
    database: str,
    table: str,
) -> tuple[int, int, list[tuple[Any, ...]]]:
    metadata = parquet.ParquetFile(path).metadata
    if metadata is None:
        raise RuntimeError(f"Parquet metadata is missing: {path.name}")
    parquet_rows = int(metadata.num_rows)
    rows = connection.execute(
        """
        SELECT event_id, event_epoch_us, source_file_name, end_position,
               row_index, count(*) OVER() AS filtered_count
        FROM read_parquet(?)
        WHERE event_epoch_us >= ? AND event_epoch_us <= ?
          AND instance_id = ? AND database_name = ? AND table_name = ?
        ORDER BY event_epoch_us DESC, source_file_name DESC,
                 end_position DESC, row_index DESC
        LIMIT 100
        """,
        [
            str(path),
            int(start_epoch_us),
            int(end_epoch_us),
            instance,
            database,
            table,
        ],
    ).fetchall()
    filtered_count = int(rows[0][5]) if rows else 0
    return parquet_rows, filtered_count, [tuple(row[:5]) for row in rows]


def _clickhouse_result(
    client: ClickHouseHttp,
    qualified_table: str,
    *,
    start_epoch_us: int,
    end_epoch_us: int,
    instance: str,
    database: str,
    table: str,
) -> tuple[int, int, list[tuple[Any, ...]]]:
    parameters = {
        "start": int(start_epoch_us),
        "end": int(end_epoch_us),
        "instance": instance,
        "database": database,
        "table": table,
    }
    total_rows = int(
        client.query(f"SELECT count() FROM {qualified_table} FORMAT TabSeparated").strip()
    )
    filtered_rows = int(
        client.query(
            f"""
            SELECT count() FROM {qualified_table}
            WHERE event_epoch_us >= {{start:Int64}}
              AND event_epoch_us <= {{end:Int64}}
              AND instance_id = {{instance:String}}
              AND database_name = {{database:String}}
              AND table_name = {{table:String}}
            FORMAT TabSeparated
            """,
            parameters=parameters,
        ).strip()
    )
    body = client.query(
        f"""
        SELECT {', '.join(TOP_COLUMNS)} FROM {qualified_table}
        WHERE event_epoch_us >= {{start:Int64}}
          AND event_epoch_us <= {{end:Int64}}
          AND instance_id = {{instance:String}}
          AND database_name = {{database:String}}
          AND table_name = {{table:String}}
        ORDER BY event_epoch_us DESC, source_file_name DESC,
                 end_position DESC, row_index DESC
        LIMIT 100 FORMAT JSONEachRow
        """,
        parameters=parameters,
    )
    rows = []
    for line in body.splitlines():
        value = json.loads(line)
        rows.append(tuple(value[column] for column in TOP_COLUMNS))
    return total_rows, filtered_rows, rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load a bounded production Parquet window into isolated ClickHouse."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--start-epoch-us", type=int, required=True)
    parser.add_argument("--end-epoch-us", type=int, required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="insight_poc")
    parser.add_argument("--clickhouse-table", default="events_poc")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--max-source-bytes", type=int, default=0)
    parser.add_argument("--health-url", default="")
    parser.add_argument("--health-host-header", default="")
    parser.add_argument("--health-max-seconds", type=float, default=2.0)
    parser.add_argument("--part-delay-seconds", type=float, default=0.0)
    args = parser.parse_args()

    database_identifier = _identifier(args.clickhouse_database)
    table_identifier = _identifier(args.clickhouse_table)
    qualified_table = f"{database_identifier}.{table_identifier}"
    clickhouse_user = os.environ.get("CLICKHOUSE_USER", "").strip()
    clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    if not clickhouse_user or not clickhouse_password:
        raise RuntimeError("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD are required")

    started = time.monotonic()
    metadata_path = args.data_dir / "metadata.sqlite3"
    settings, parts, range_parts = _candidate_parts(
        metadata_path,
        start_epoch_us=args.start_epoch_us,
        end_epoch_us=args.end_epoch_us,
        instance=args.instance,
        database=args.database,
        table=args.table,
    )
    if not parts:
        raise RuntimeError("The frozen window has no catalog-matching Parquet parts")
    if any(not str(part.get("oss_key") or "") for part in parts):
        raise RuntimeError("The frozen window contains a part without an OSS object")
    source_bytes = sum(int(part["size_bytes"]) for part in parts)
    plan = {
        "phase": "planned",
        "window": {
            "start_epoch_us": args.start_epoch_us,
            "end_epoch_us": args.end_epoch_us,
            "instance": args.instance,
            "database": args.database,
            "table": args.table,
        },
        "range_parts": range_parts,
        "selected_parts": len(parts),
        "source_bytes": source_bytes,
        "source_rows_metadata": sum(int(part["row_count"]) for part in parts),
    }
    if args.plan_only:
        _write_json(args.status, plan)
        print(json.dumps(plan, ensure_ascii=False, separators=(",", ":")))
        return 0
    if args.max_source_bytes and source_bytes > args.max_source_bytes:
        raise RuntimeError(
            f"Planned source bytes {source_bytes} exceed hard limit "
            f"{args.max_source_bytes}"
        )
    if args.health_url:
        _probe_health(
            args.health_url,
            args.health_max_seconds,
            args.health_host_header,
        )

    credential = load_credential(settings.credential_target)
    archive = OssArchive(settings, credential=credential)
    client = ClickHouseHttp(
        args.clickhouse_host,
        args.clickhouse_port,
        clickhouse_user,
        clickhouse_password,
    )
    version = client.query("SELECT version() FORMAT TabSeparated").strip()
    scratch = args.data_dir / "scratch" / "clickhouse-poc"
    scratch.mkdir(parents=True, exist_ok=True)
    duck = duckdb.connect(":memory:")
    duck.execute("SET threads = 1")
    duck.execute("SET preserve_insertion_order = false")

    state: dict[str, Any] = {
        "phase": "loading",
        "clickhouse_version": version,
        "window": {
            "start_epoch_us": args.start_epoch_us,
            "end_epoch_us": args.end_epoch_us,
            "instance": args.instance,
            "database": args.database,
            "table": args.table,
        },
        "range_parts": range_parts,
        "selected_parts": len(parts),
        "completed_parts": 0,
        "source_bytes": source_bytes,
        "source_rows": 0,
        "source_filtered_rows": 0,
        "source_top100_hash": "",
        "clickhouse_total_rows": 0,
        "clickhouse_filtered_rows": 0,
        "clickhouse_top100_hash": "",
        "exact_match": False,
        "elapsed_seconds": 0.0,
    }
    _write_json(args.status, state)
    source_top: list[tuple[Any, ...]] = []
    try:
        for index, part in enumerate(parts, start=1):
            if args.health_url:
                state["health_probe_seconds"] = round(
                    _probe_health(
                        args.health_url,
                        args.health_max_seconds,
                        args.health_host_header,
                    ),
                    3,
                )
            destination = scratch / f"{index:04d}-{str(part['sha256'])[:20]}.parquet"
            destination.unlink(missing_ok=True)
            archive.download_part(part, destination)
            try:
                parquet_rows, filtered_rows, top_rows = _scan_source_part(
                    duck,
                    destination,
                    start_epoch_us=args.start_epoch_us,
                    end_epoch_us=args.end_epoch_us,
                    instance=args.instance,
                    database=args.database,
                    table=args.table,
                )
                client.insert_parquet(
                    qualified_table,
                    destination,
                    str(part["sha256"]),
                )
            finally:
                destination.unlink(missing_ok=True)
            if args.health_url:
                state["health_probe_seconds"] = round(
                    _probe_health(
                        args.health_url,
                        args.health_max_seconds,
                        args.health_host_header,
                    ),
                    3,
                )
            state["source_rows"] += parquet_rows
            state["source_filtered_rows"] += filtered_rows
            source_top.extend(top_rows)
            source_top.sort(key=_top_key, reverse=True)
            del source_top[100:]
            state["completed_parts"] = index
            state["current_part_sha256"] = str(part["sha256"])
            state["elapsed_seconds"] = round(time.monotonic() - started, 3)
            _write_json(args.status, state)
            print(
                json.dumps(
                    {
                        "completed": index,
                        "total": len(parts),
                        "source_rows": state["source_rows"],
                        "filtered_rows": state["source_filtered_rows"],
                        "elapsed_seconds": state["elapsed_seconds"],
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
            if args.part_delay_seconds > 0:
                time.sleep(args.part_delay_seconds)

        source_hash = _canonical_hash(source_top)
        clickhouse_rows, clickhouse_filtered, clickhouse_top = _clickhouse_result(
            client,
            qualified_table,
            start_epoch_us=args.start_epoch_us,
            end_epoch_us=args.end_epoch_us,
            instance=args.instance,
            database=args.database,
            table=args.table,
        )
        clickhouse_hash = _canonical_hash(clickhouse_top)
        exact_match = (
            clickhouse_rows == int(state["source_rows"])
            and clickhouse_filtered == int(state["source_filtered_rows"])
            and clickhouse_hash == source_hash
        )
        state.update(
            {
                "phase": "complete" if exact_match else "mismatch",
                "source_top100_hash": source_hash,
                "clickhouse_total_rows": clickhouse_rows,
                "clickhouse_filtered_rows": clickhouse_filtered,
                "clickhouse_top100_hash": clickhouse_hash,
                "exact_match": exact_match,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
        _write_json(args.status, state)
        print(json.dumps(state, ensure_ascii=False, separators=(",", ":")))
        return 0 if exact_match else 2
    except Exception as exc:
        state.update(
            {
                "phase": "failed",
                "last_error": str(exc),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
        _write_json(args.status, state)
        raise
    finally:
        duck.close()


if __name__ == "__main__":
    raise SystemExit(main())
