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


def compare_columnar(ndjson: Path, output: Path, source_id: str, *, native_arrow: Path | None = None):
    """Compare parser transport and final 47 fields; native Arrow must match all 41 inputs."""
    rows = [json.loads(line) for line in ndjson.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows or any(set(row) - set(PARSER_JSON_COLUMNS) for row in rows):
        raise ValueError("empty or unknown native parser fields")
    output.mkdir(exist_ok=False)
    expected = pa.Table.from_pylist(rows, schema=parser_schema(PARSER_JSON_COLUMNS))
    if native_arrow is None:
        ipc = output / "input.arrow"
        with pa.OSFile(str(ipc), "wb") as sink, pa.ipc.new_file(sink, expected.schema) as writer:
            writer.write_table(expected, max_chunksize=2)
        record_batches = None
    else:
        ipc = native_arrow
        if not ipc.is_file():
            raise ValueError("native Arrow parser output is missing")
        with pa.memory_map(str(ipc), "r") as source:
            reader = pa.ipc.open_file(source)
            record_batches = reader.num_record_batches
            direct = reader.read_all()
        if (direct.column_names != list(PARSER_JSON_COLUMNS)
                or not direct.schema.equals(expected.schema, check_metadata=True)):
            raise ValueError("native Arrow producer changed the strict 41-field parser schema")
        if not direct.equals(expected, check_metadata=True):
            raise ValueError("native Arrow values or row order differ from candidate NDJSON")
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
    return dict(rows=len(rows), transport_fields=len(PARSER_JSON_COLUMNS), fields=len(EVENT_COLUMNS),
                transport_equal=True, native_arrow_producer=native_arrow is not None,
                arrow_record_batches=record_batches, arrow_bytes=ipc.stat().st_size,
                arrow_sha256=hashlib.sha256(ipc.read_bytes()).hexdigest(),
                independent_value_oracle=False)


def compare_chunked_arrow(ndjson: Path, case_dir: Path, output: Path, source_id: str,
                          contract: dict):
    """Independently decode the retained strict manifests, ACKs and IPC chunks."""
    required_contract = {
        "protocol", "format", "ack_protocol", "manifests_sha256",
        "acks_sha256", "stderr_sha256", "chunks",
    }
    if set(contract) != required_contract:
        raise ValueError("Arrow chunk contract fields changed")
    if (contract["protocol"] != "parser-chunk-v1"
            or contract["format"] != "arrow-ipc-file-v1"
            or contract["ack_protocol"] != "parser-chunk-ack-v1"
            or not isinstance(contract["chunks"], list)
            or not contract["chunks"]):
        raise ValueError("Arrow chunk contract identity changed")
    manifests_raw = (case_dir / "candidate-arrow-chunks.manifests.ndjson").read_bytes()
    acknowledgements_raw = (case_dir / "candidate-arrow-chunks.acks.ndjson").read_bytes()
    stderr_raw = (case_dir / "candidate-arrow-chunks.stderr").read_bytes()
    if (hashlib.sha256(manifests_raw).hexdigest() != contract["manifests_sha256"]
            or hashlib.sha256(acknowledgements_raw).hexdigest()
            != contract["acks_sha256"]
            or hashlib.sha256(stderr_raw).hexdigest() != contract["stderr_sha256"]):
        raise ValueError("Arrow chunk transcript identity mismatch")
    if not manifests_raw.endswith(b"\n") or not acknowledgements_raw.endswith(b"\n"):
        raise ValueError("Arrow chunk transcript is not line framed")
    try:
        manifests = [json.loads(line) for line in manifests_raw.splitlines()]
        acknowledgements = [json.loads(line) for line in acknowledgements_raw.splitlines()]
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Arrow chunk transcript is malformed") from exc
    if not (len(manifests) == len(acknowledgements) == len(contract["chunks"])):
        raise ValueError("Arrow chunk transcript length mismatch")
    if stderr_raw.decode("utf-8", "strict").strip() != (
            f"parsed {sum(chunk['rows'] for chunk in contract['chunks'])} audit records"):
        raise ValueError("Arrow chunk completion record changed")

    rows = [json.loads(line) for line in ndjson.read_text(encoding="utf-8").splitlines()
            if line.strip()]
    expected = pa.Table.from_pylist(rows, schema=parser_schema(PARSER_JSON_COLUMNS))
    chunk_tables = []
    manifest_fields = {
        "protocol", "format", "sequence", "path", "rows", "bytes",
        "decoded_bytes",
    }
    contract_fields = {"sequence", "name", "rows", "bytes", "decoded_bytes", "sha256"}
    for sequence, (manifest, acknowledgement, retained) in enumerate(
            zip(manifests, acknowledgements, contract["chunks"])):
        if (not isinstance(manifest, dict) or set(manifest) != manifest_fields
                or not isinstance(acknowledgement, dict)
                or set(acknowledgement) != {"protocol", "sequence", "status"}
                or not isinstance(retained, dict) or set(retained) != contract_fields):
            raise ValueError("Arrow chunk transcript has unknown or missing fields")
        if (manifest["protocol"] != contract["protocol"]
                or manifest["format"] != contract["format"]
                or acknowledgement["protocol"] != contract["ack_protocol"]
                or acknowledgement["status"] != "ok"
                or type(manifest["sequence"]) is not int
                or type(acknowledgement["sequence"]) is not int
                or type(retained["sequence"]) is not int
                or manifest["sequence"] != sequence
                or acknowledgement["sequence"] != sequence
                or retained["sequence"] != sequence
                or type(manifest["path"]) is not str
                or not Path(manifest["path"]).is_absolute()):
            raise ValueError("Arrow chunk transcript sequence or identity mismatch")
        for field in ("rows", "bytes", "decoded_bytes"):
            if (type(manifest[field]) is not int or type(retained[field]) is not int
                    or manifest[field] != retained[field] or manifest[field] <= 0):
                raise ValueError("Arrow chunk bounds are malformed")
        if (manifest["rows"] > 3 or manifest["bytes"] > 128 * 1024 * 1024
                or manifest["decoded_bytes"] > 128 * 1024 * 1024):
            raise ValueError("Arrow chunk exceeded its retained hard bounds")
        name = retained["name"]
        if (type(name) is not str or Path(name).name != name
                or Path(manifest["path"]).name != name
                or name != f"{source_id}-{sequence:06d}.arrow"
                or type(retained["sha256"]) is not str):
            raise ValueError("Arrow chunk path binding changed")
        chunk = case_dir / "candidate-arrow-chunks" / name
        content = chunk.read_bytes()
        if (len(content) != manifest["bytes"]
                or hashlib.sha256(content).hexdigest() != retained["sha256"]):
            raise ValueError("Arrow chunk byte identity mismatch")
        with pa.OSFile(str(chunk), "rb") as source:
            reader = pa.ipc.open_file(source)
            table = reader.read_all()
            del reader
        if (table.column_names != list(PARSER_JSON_COLUMNS)
                or not table.schema.equals(expected.schema, check_metadata=True)
                or table.num_rows != manifest["rows"]):
            raise ValueError("Arrow chunk schema or manifest row count changed")
        chunk_tables.append(table)
    combined = pa.concat_tables(chunk_tables)
    if not combined.equals(expected, check_metadata=True):
        raise ValueError("Arrow chunks changed parser values or row order")

    output.mkdir(exist_ok=False)
    materialized = []
    storage = EventStorage.__new__(EventStorage)
    storage.paths = ensure_data_dirs(output / "chunks")
    storage._part_body_locks = [threading.RLock() for _ in range(256)]
    common = dict(file_id=source_id, instance_id="synthetic-fixture",
                  host_instance_id="fixture-host", source_file_name="mysql-bin.fixture",
                  publish_metadata=False, append=True)
    count = 0
    for sequence, retained in enumerate(contract["chunks"]):
        rows_written, parts = storage.ingest_arrow_file(
            arrow_path=case_dir / "candidate-arrow-chunks" / retained["name"],
            expected_rows=retained["rows"],
            part_key=f"{sequence:06d}",
            **common,
        )
        count += rows_written
        materialized.extend(pq.ParquetFile(part["path"]).read() for part in parts)
    actual = pa.concat_tables(materialized)
    baseline_storage = EventStorage.__new__(EventStorage)
    baseline_storage.paths = ensure_data_dirs(output / "ndjson")
    baseline_storage._part_body_locks = [threading.RLock() for _ in range(256)]
    baseline_count, baseline_parts = baseline_storage.ingest_ndjson_file(
        ndjson_path=ndjson, **common
    )
    baseline = pa.concat_tables([
        pq.ParquetFile(part["path"]).read() for part in baseline_parts
    ])
    expected_ids = [row["event_id"] for row in rows]

    def parser_order(table):
        actual_ids = table["event_id"].to_pylist()
        positions = {event_id: index for index, event_id in enumerate(actual_ids)}
        if (len(positions) != len(actual_ids)
                or set(positions) != set(expected_ids)):
            raise ValueError("47-field materialization changed event identities")
        return table.take(pa.array([positions[event_id] for event_id in expected_ids]))

    actual = parser_order(actual)
    baseline = parser_order(baseline)
    if (count != len(rows) or baseline_count != len(rows)
            or actual.column_names != list(EVENT_COLUMNS)
            or not actual.equals(baseline, check_metadata=True)):
        differing = [
            name for name in EVENT_COLUMNS
            if not actual[name].equals(baseline[name])
        ] if actual.num_rows == baseline.num_rows else []
        raise ValueError(
            "Arrow chunks changed the final 47-field materialization: "
            + ",".join(differing[:10])
        )
    return {
        "protocol": contract["protocol"],
        "ack_protocol": contract["ack_protocol"],
        "chunks": len(chunk_tables),
        "rows": len(rows),
        "transport_fields": len(PARSER_JSON_COLUMNS),
        "fields": len(EVENT_COLUMNS),
        "transport_equal": True,
        "materialization_equal": True,
    }


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
        candidate_binary = root / "candidate-binlog-parser-linux-amd64"
        if (not candidate_binary.is_file()
                or hashlib.sha256(candidate_binary.read_bytes()).hexdigest()
                != contract["candidate_native_sha256"]):
            raise ValueError("candidate binary evidence identity mismatch")
        for mode in ("STATEMENT", "ROW"):
            case = contract["cases"][mode]; directory = root / mode.lower()
            legacy = directory / "legacy.ndjson"
            candidate = directory / "candidate.ndjson"
            candidate_arrow = directory / "candidate.arrow"
            for path, wanted in ((directory / "source.binlog", case["raw_sha256"]),
                                 (legacy, case["legacy_sha256"]),
                                 (candidate, case["candidate_sha256"]),
                                 (candidate_arrow, case["candidate_arrow_sha256"])):
                if hashlib.sha256(path.read_bytes()).hexdigest() != wanted:
                    raise ValueError("frozen fixture identity mismatch: " + path.name)
            if candidate_arrow.stat().st_size != case["candidate_arrow_bytes"]:
                raise ValueError("native Arrow parser output size changed")
            result["transport"][mode] = {
                "legacy": compare_columnar(legacy, directory / "legacy-columnar", case["source_id"]),
                "candidate": compare_columnar(candidate, directory / "candidate-columnar", case["source_id"],
                                                native_arrow=candidate_arrow),
                "candidate_arrow_chunks": compare_chunked_arrow(
                    candidate,
                    directory,
                    directory / "candidate-arrow-chunks-columnar",
                    case["source_id"],
                    case["candidate_arrow_chunks"],
                ),
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
