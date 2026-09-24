"""Independent known-input value gate for tools.parser_contract_fixture.

A passing Arrow/NDJSON transport comparison does not prove the legacy decoder
is lossless. This oracle separately checks the values of the SQL we generated.
It is deliberately scoped to that fixture, not arbitrary production binlogs.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.columnar_input import parser_schema
from app.storage import EVENT_COLUMNS, PARSER_JSON_COLUMNS, EventStorage, ensure_data_dirs


BIG_ID = 18446744073709551615


def expected_images():
    first = dict(id=BIG_ID, txt="中文🙂", amount={"$decimal": "1234567890123456789012.12345678"},
                 payload=b"\x00\xff\xfe", document={"n": 123, "s": "中文"}, nullable=None,
                 flag=5, happened={"$time_rfc3339_nano": "2026-09-20T12:34:56.123456Z"},
                 duration="-10:02:03.123456", category=2, options=3, small=-128,
                 score={"$float64": "0.125", "$ieee754": "3fc0000000000000"})
    second = dict(id=2, txt="", amount={"$decimal": "-123.00000001"}, payload=b"",
                  document=[None, True, False], nullable=-2147483648, flag=0,
                  happened={"$time_rfc3339_nano": "2001-01-01T00:00:00.000001Z"},
                  duration="00:00:00", category=1, options=0, small=127,
                  score={"$float64": "-1.5", "$ieee754": "bff8000000000000"})
    return [("INSERT", {}, first), ("INSERT", {}, second),
            ("UPDATE", first, dict(first, txt="after", nullable=2147483647)),
            ("DELETE", second, {})]


def check_values(rows):
    """Return all mismatches; never bless legacy replacement characters.

    Binary-safe candidates can express bytes as {"$bytes_base64": "..."}.
    A legacy text value is valid only if its UTF-8 bytes equal the SQL literal.
    """
    failures = []
    actual = [r for r in rows if r.get("operation") in {"INSERT", "UPDATE", "DELETE"}]
    expected = expected_images()
    if len(actual) != len(expected):
        return [{"field": "mutation_count", "expected": len(expected), "actual": len(actual)}]
    for index, (row, (operation, before, after)) in enumerate(zip(actual, expected)):
        for key, value in {"operation": operation, "database_name": "fixture", "table_name": "rows_abi"}.items():
            if row.get(key) != value:
                failures.append(dict(row=index, field=key, expected=value, actual=row.get(key)))
        for field, image in (("before_json", before), ("after_json", after)):
            try:
                decoded = json.loads(row[field]) if row.get(field) else {}
                if not isinstance(decoded, dict):
                    raise ValueError("row image is not an object")
            except (ValueError, TypeError, KeyError) as exc:
                failures.append(dict(row=index, field=field, error=str(exc)))
                continue
            if set(decoded) != set(image):
                failures.append(dict(row=index, field=field + ".keys", expected=sorted(image), actual=sorted(decoded)))
            for name, wanted in image.items():
                found = decoded.get(name)
                try:
                    if name == "payload":
                        if isinstance(found, dict) and set(found) == {"$bytes_base64"}:
                            found = base64.b64decode(found["$bytes_base64"], validate=True)
                        elif isinstance(found, dict) and set(found) == {"$binary_base64", "$length"}:
                            found = base64.b64decode(found["$binary_base64"], validate=True)
                            if type(decoded[name]["$length"]) is not int or decoded[name]["$length"] != len(found):
                                raise ValueError("binary length does not match decoded bytes")
                        elif isinstance(found, str):
                            found = found.encode("utf-8", "strict")
                        else:
                            raise ValueError("unrecognized binary representation")
                        wanted, found = wanted.hex(), found.hex()
                    elif name == "document" and isinstance(found, str):
                        found = json.loads(found)
                    # Python equality treats True == 1 and 1 == 1.0; the
                    # source-value gate must not erase those type differences.
                    if json.dumps(found, sort_keys=True) != json.dumps(wanted, sort_keys=True):
                        failures.append(dict(row=index, field=f"{field}.{name}", expected=wanted, actual=found))
                except (ValueError, TypeError, UnicodeError) as exc:
                    failures.append(dict(row=index, field=f"{field}.{name}", error=str(exc)))
    return failures


def compare_columnar(ndjson: Path, output: Path, source_id: str):
    """Full 47-field/order/type/catalog transport check, not a native producer."""
    rows = [json.loads(line) for line in ndjson.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows or any(set(row) - set(PARSER_JSON_COLUMNS) for row in rows):
        raise ValueError("empty or unknown native parser fields")
    output.mkdir(exist_ok=False)
    table = pa.Table.from_pylist(rows, schema=parser_schema(PARSER_JSON_COLUMNS))
    ipc = output / "input.arrow"
    with pa.OSFile(str(ipc), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=2)
    tables, catalogs = [], []
    for mode in ("ndjson", "arrow"):
        storage = EventStorage.__new__(EventStorage)
        storage.paths = ensure_data_dirs(output / mode)
        storage._part_body_locks = [threading.RLock() for _ in range(256)]
        common = dict(file_id=source_id, instance_id="synthetic-fixture", host_instance_id="fixture-host",
                      source_file_name="mysql-bin.fixture", publish_metadata=False, append=True)
        if mode == "ndjson":
            count, parts = storage.ingest_ndjson_file(ndjson_path=ndjson, **common)
        else:
            count, parts = storage.ingest_arrow_file(arrow_path=ipc, **common)
        actual = pa.concat_tables([pq.ParquetFile(p["path"]).read() for p in parts])
        if count != len(rows) or actual.column_names != list(EVENT_COLUMNS):
            raise ValueError("row count or full output schema changed")
        tables.append(actual); catalogs.append([p["catalog"] for p in parts])
    if not tables[0].equals(tables[1], check_metadata=True) or catalogs[0] != catalogs[1]:
        raise ValueError("Arrow/NDJSON values, order, types or catalogs differ")
    return dict(rows=len(rows), fields=len(EVENT_COLUMNS), transport_equal=True,
                native_arrow_producer=False, independent_value_oracle=False)


def compare_decoders(legacy: Path, candidate: Path, output: Path, source_id: str, *, require_binary_repair=False):
    """Compare every stored field, allowing only independently checked byte-body repairs."""
    output.mkdir(exist_ok=False)
    tables, catalogs = {}, {}
    for label, ndjson in (("legacy", legacy), ("candidate", candidate)):
        storage = EventStorage.__new__(EventStorage)
        storage.paths = ensure_data_dirs(output / label)
        storage._part_body_locks = [threading.RLock() for _ in range(256)]
        count, parts = storage.ingest_ndjson_file(
            ndjson_path=ndjson, file_id=source_id, instance_id="synthetic-fixture",
            host_instance_id="fixture-host", source_file_name="mysql-bin.fixture",
            publish_metadata=False, append=True)
        table = pa.concat_tables([pq.ParquetFile(part["path"]).read() for part in parts])
        if count != table.num_rows or table.column_names != list(EVENT_COLUMNS):
            raise ValueError(label + " decoder did not materialize the complete 47-field schema")
        tables[label] = table
        catalogs[label] = [part["catalog"] for part in parts]
    if tables["legacy"].schema != tables["candidate"].schema:
        raise ValueError("legacy/candidate stored schemas differ")
    if catalogs["legacy"] != catalogs["candidate"]:
        raise ValueError("legacy/candidate semantic catalogs differ")
    legacy_rows = tables["legacy"].to_pylist()
    candidate_rows = tables["candidate"].to_pylist()
    if len(legacy_rows) != len(candidate_rows):
        raise ValueError("legacy/candidate row counts differ")
    allowed = {"before_json", "after_json", "sql_text"}
    differing = set()
    for index, (old, new) in enumerate(zip(legacy_rows, candidate_rows)):
        for field in EVENT_COLUMNS:
            if old[field] == new[field]:
                continue
            differing.add(field)
            if field not in allowed:
                raise ValueError(f"decoder drift at row {index} field {field}: {old[field]!r} != {new[field]!r}")
        if "\ufffd" in str(new["before_json"]) + str(new["after_json"]) + str(new["sql_text"]):
            raise ValueError(f"candidate retained a Unicode replacement character at row {index}")
    expected_differences = allowed if require_binary_repair else set()
    if differing != expected_differences:
        raise ValueError(f"candidate differences are not the intended binary body repair: {sorted(differing)}")
    if require_binary_repair:
        mutation_sql = [str(row["sql_text"]) for row in candidate_rows
                        if row["operation"] in {"INSERT", "UPDATE", "DELETE"}]
        if not mutation_sql or not all("FROM_BASE64(" in sql for sql in mutation_sql):
            raise ValueError("candidate pseudo SQL does not preserve binary values")
    return {"rows": len(candidate_rows), "fields": len(EVENT_COLUMNS),
            "identity_fields_equal": True, "semantic_catalog_equal": True,
            "only_intended_value_differences": True,
            "differing_fields": sorted(differing)}


def verify(root: Path):
    contract = json.loads((root / "contract.json").read_text(encoding="utf-8"))
    result = {"scope": "synthetic fixture only", "source_sha": contract["source_sha"],
              "transport": {}, "decoder_differential": {},
              "independent_values_equal": False, "failures": [], "legacy_failures": []}
    report = root / "independent-oracle.json"
    if report.exists():
        raise FileExistsError("retain existing oracle evidence; use a separate fixture directory")
    try:
        for mode in ("STATEMENT", "ROW"):
            case = contract["cases"][mode]; directory = root / mode.lower()
            legacy = directory / "legacy.ndjson"
            candidate = directory / "candidate.ndjson"
            for path, wanted in ((directory / "source.binlog", case["raw_sha256"]),
                                 (legacy, case["legacy_sha256"]),
                                 (candidate, case["candidate_sha256"])):
                if hashlib.sha256(path.read_bytes()).hexdigest() != wanted:
                    raise ValueError("frozen fixture identity mismatch: " + path.name)
            result["transport"][mode] = {
                "legacy": compare_columnar(legacy, directory / "legacy-columnar", case["source_id"]),
                "candidate": compare_columnar(candidate, directory / "candidate-columnar", case["source_id"]),
            }
            result["decoder_differential"][mode] = compare_decoders(
                legacy, candidate, directory / "decoder-differential", case["source_id"],
                require_binary_repair=mode == "ROW")
            if mode == "ROW":
                legacy_rows = [json.loads(line) for line in legacy.read_text(encoding="utf-8").splitlines() if line.strip()]
                candidate_rows = [json.loads(line) for line in candidate.read_text(encoding="utf-8").splitlines() if line.strip()]
                result["legacy_failures"] = check_values(legacy_rows)
                result["failures"] = check_values(candidate_rows)
                if len(result["legacy_failures"]) != 3:
                    raise ValueError("legacy oracle sensitivity changed; expected three known byte-loss failures")
        result["independent_values_equal"] = not result["failures"]
        return result
    except BaseException as exc:
        result["execution_failure"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        with report.open("x", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.fixture)
    print(json.dumps(result, ensure_ascii=False))
    if not result["independent_values_equal"]:
        raise SystemExit("independent source-value oracle failed; do not promote this decoder")


if __name__ == "__main__":
    main()
