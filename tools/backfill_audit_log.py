"""把已归档的本地执行日志回填进 tabularis_audit_log 索引表。

事件本体一直在 Parquet 里（每条一个单行分区），这个脚本只是把它们的可查询字段
复制进带索引的表，不改动任何 Parquet 或 OSS 对象。可重复执行（按 event_id 覆盖）。

用法（在容器内）：
    python -m tools.backfill_audit_log /data/metadata.sqlite3
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import pyarrow.parquet as pq

COLUMNS = (
    "event_id",
    "event_epoch_us",
    "instance_id",
    "connection_id",
    "connection_name",
    "database_name",
    "table_name",
    "database_account",
    "execution_status",
    "operation",
    "sql_kind",
    "sql_text",
    "execution_time_ms",
    "affected_rows",
    "error_code",
    "error_message",
    "started_epoch_us",
    "finished_epoch_us",
    "batch_id",
    "statement_index",
    "transaction_context_id",
    "source_file_name",
)

INTEGER_COLUMNS = {
    "event_epoch_us",
    "execution_time_ms",
    "affected_rows",
    "error_code",
    "started_epoch_us",
    "finished_epoch_us",
    "statement_index",
}

def _instance_aliases() -> dict[str, str]:
    """读取部署方提供的别名映射，不把实例标识固化进发布包。"""
    raw = os.environ.get("RDS_BINLOG_AUDIT_INSTANCE_ALIASES", "").strip()
    if not raw:
        return {}
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("RDS_BINLOG_AUDIT_INSTANCE_ALIASES must be a JSON object")
    return {
        str(alias).strip(): str(instance_id).strip()
        for alias, instance_id in payload.items()
        if str(alias).strip() and str(instance_id).strip()
    }


def main(db_path: str) -> int:
    instance_aliases = _instance_aliases()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    parts = con.execute(
        """
        SELECT DISTINCT p.path
        FROM parquet_parts p
        JOIN binlog_files b ON b.id = p.binlog_id
        WHERE b.log_file_name LIKE 'tabularis-audit-%'
        """
    ).fetchall()
    print(f"audit parquet parts: {len(parts)}", flush=True)

    inserted = 0
    missing = 0
    batch: list[tuple] = []
    placeholders = ",".join("?" for _ in COLUMNS)
    statement = (
        f"INSERT OR REPLACE INTO tabularis_audit_log({','.join(COLUMNS)}) "
        f"VALUES({placeholders})"
    )

    for index, row in enumerate(parts, 1):
        path = Path(str(row["path"]))
        if not path.exists():
            missing += 1
            continue
        table = pq.read_table(path)
        data = table.to_pydict()
        count = table.num_rows
        for i in range(count):
            if str(data.get("raw_event_type", [""] * count)[i]) != "TABULARIS_AUDIT":
                continue
            values = []
            for column in COLUMNS:
                series = data.get(column)
                value = series[i] if series is not None else None
                if column in INTEGER_COLUMNS:
                    values.append(int(value or 0))
                elif column == "instance_id":
                    text = str(value or "")
                    values.append(instance_aliases.get(text, text))
                else:
                    values.append(str(value or ""))
            batch.append(tuple(values))
        if len(batch) >= 500:
            con.executemany(statement, batch)
            con.commit()
            inserted += len(batch)
            batch.clear()
            print(f"  {index}/{len(parts)} parts, {inserted} rows", flush=True)

    if batch:
        con.executemany(statement, batch)
        con.commit()
        inserted += len(batch)

    total = con.execute("SELECT count(*) FROM tabularis_audit_log").fetchone()[0]
    print(f"backfilled rows: {inserted}, missing part files: {missing}")
    print(f"tabularis_audit_log total: {total}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/data/metadata.sqlite3"))
