"""Cloud-CI-only native parser contract fixtures; no production/network source.

Retains full, synthetic binlog + legacy output for source-rebuild differential
checks. A fixture is NOT a production throughput or full-engine coverage claim.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import time
import uuid
import zlib
from pathlib import Path

SCOPE = "sql-insight-parser-contract"


def run(args, *, stdin=None, seconds=30):
    result = subprocess.run(args, input=stdin, capture_output=True, timeout=seconds)
    if result.returncode:
        raise RuntimeError(f"{args[:3]} failed {result.returncode}: "
                           + result.stderr.decode("utf-8", "replace")[-3000:])
    return result.stdout


def mysql_query(container, query):
    # Applies to DDL as well as mutations: SET NAMES only in the latter would
    # create mojibake ENUM labels before the Unicode INSERT is ever tested.
    return run(["docker", "exec", "-i", container, "mysql", "-uroot", "-pfixture-only",
                "--default-character-set=utf8mb4", "--batch", "--skip-column-names"],
               stdin=query.encode("utf-8"), seconds=20).decode("utf-8")


def read_headers(raw):
    """Independent framing/CRC oracle, not the decoder under test."""
    if raw[:4] != b"\xfebin":
        raise ValueError("binlog magic mismatch")
    at = 4
    rows = []
    while at < len(raw):
        if len(raw) - at < 19:
            raise ValueError("truncated event header")
        timestamp, kind, server, size, end, flags = struct.unpack_from("<IBIIIH", raw, at)
        if size < 23 or at + size > len(raw) or end != at + size:
            raise ValueError("event bounds mismatch")
        event = raw[at:at + size]
        if zlib.crc32(event[:-4]) != struct.unpack_from("<I", event, size-4)[0]:
            raise ValueError("event CRC32 mismatch")
        rows.append(dict(start=at, end=end, event_type=kind, server_id=server,
                         timestamp=timestamp, flags=flags, bytes=size))
        at += size
    return rows


def split_events(raw):
    headers = read_headers(raw)
    return [bytearray(raw[row["start"]:row["end"]]) for row in headers]


def rewrite_events(events):
    """Rebuild positions and CRCs after deleting a context event."""
    output = bytearray(b"\xfebin")
    for source in events:
        event = bytearray(source)
        struct.pack_into("<I", event, 13, len(output) + len(event))
        struct.pack_into("<I", event, len(event) - 4, zlib.crc32(event[:-4]))
        output.extend(event)
    read_headers(bytes(output))
    return bytes(output)


def negative_streams(raw):
    events = split_events(raw)
    without_fde = rewrite_events([event for event in events if event[4] != 15])
    without_table_map = rewrite_events([event for event in events if event[4] != 19])
    without_gtid = rewrite_events([event for event in events if event[4] not in {33, 34}])
    unknown_events = [bytearray(event) for event in events]
    unknown_events[-1][4] = 0x7f
    unknown = rewrite_events(unknown_events)
    bad_crc = bytearray(raw); bad_crc[-5] ^= 0x01
    bad_size = bytearray(raw); struct.pack_into("<I", bad_size, 4 + 9, 18)
    bad_position = bytearray(raw); struct.pack_into("<I", bad_position, 4 + 13,
                                                    struct.unpack_from("<I", bad_position, 4 + 13)[0] + 1)
    return {
        "missing-fde": (without_fde, "FormatDescriptionEvent"),
        "missing-table-map": (without_table_map, "table map"),
        "missing-gtid": (without_gtid, "GTID context"),
        "unknown-event": (unknown, "unknown binlog event"),
        "partial-header": (raw + b"\x00" * 7, "partial binlog event header"),
        "partial-body": (raw[:-1], "partial binlog event body"),
        "bad-crc": (bytes(bad_crc), "checksum"),
        "bad-size": (bytes(bad_size), "invalid binlog event size"),
        "bad-position": (bytes(bad_position), "position mismatch"),
    }


def candidate_command(binary, raw_path, source_id, mode):
    command = [str(binary), "--input", str(raw_path), "--source-file-id", source_id,
               "--flavor", "mysql", "--require-gtid"]
    if mode == "ROW":
        command.append("--require-table-map")
    return command


def verify_negative_streams(root, binary, raw, source_id):
    negative = root / "negative"
    negative.mkdir()
    proof = {}
    for name, (content, marker) in negative_streams(raw).items():
        source = negative / f"{name}.binlog"
        source.write_bytes(content)
        output_dir = negative / f"{name}-output"
        output_dir.mkdir()
        result = subprocess.run(candidate_command(binary, source, source_id, "ROW")
                                + ["--output-dir", str(output_dir)],
                                capture_output=True, timeout=20)
        stderr = result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.stderr").write_text(stderr, encoding="utf-8")
        if result.returncode == 0:
            raise RuntimeError(f"candidate accepted corrupt stream {name}")
        if marker.lower() not in stderr.lower():
            raise RuntimeError(f"candidate {name} failure lacks {marker!r}: {stderr[-1000:]}")
        if output_dir.exists() and any(output_dir.iterdir()):
            raise RuntimeError(f"candidate published partial output for {name}")
        proof[name] = {"returncode": result.returncode, "stderr_marker": marker,
                       "published_files": 0, "sha256": hashlib.sha256(content).hexdigest()}
    return proof


def prepare(root):
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("synthetic parser fixture requires disposable cloud CI")
    root.mkdir(exist_ok=False)
    binary = Path("tools/binlog-parser-linux-amd64").resolve()
    candidate = Path(os.environ.get("PARSER_CANDIDATE", "parser/build/binlog-parser-linux-amd64")).resolve()
    build_info = Path("parser/candidate-build-info.txt").resolve()
    if not candidate.is_file():
        raise RuntimeError("candidate parser binary is missing: " + str(candidate))
    if not build_info.is_file():
        raise RuntimeError("candidate build identity is missing: " + str(build_info))
    (root / "candidate-build-info.txt").write_bytes(build_info.read_bytes())
    binary.chmod(0o555)
    candidate.chmod(0o555)
    source_id = hashlib.sha256(b"sql-insight synthetic parser ABI").hexdigest()
    name = "parser-contract-" + uuid.uuid4().hex[:12]
    run(["docker", "pull", "mysql:8.0"], seconds=240)
    run(["docker", "run", "-d", "--name", name, "--label", "scope=" + SCOPE,
         "--network", "none", "--memory", "1g", "--memory-swap", "1g", "--cpus", "1",
         "-e", "MYSQL_ROOT_PASSWORD=fixture-only", "mysql:8.0", "--server-id=77",
         "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-checksum=CRC32",
         "--binlog-row-metadata=FULL", "--innodb-buffer-pool-size=64M",
         "--performance-schema=OFF"], seconds=30)

    def sql(query):
        return mysql_query(name, query)

    try:
        deadline = time.monotonic() + 120
        while True:
            try:
                sql("SELECT 1;")
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("fixture MySQL readiness timed out")
                time.sleep(2)
        sql("""CREATE DATABASE fixture CHARACTER SET utf8mb4;
CREATE TABLE fixture.rows_abi (
 id BIGINT UNSIGNED PRIMARY KEY, txt VARCHAR(80), amount DECIMAL(30,8),
 payload VARBINARY(80), document JSON, nullable INT NULL, flag BIT(3),
 happened DATETIME(6), duration TIME(6), category ENUM('a','中文'),
 options SET('x','y'), small TINYINT, score DOUBLE
) ENGINE=InnoDB;
CREATE TABLE fixture.statements (id INT PRIMARY KEY, txt VARCHAR(80)) ENGINE=InnoDB;
""")
        cases = {}
        for mode in ("STATEMENT", "ROW"):
            sql("FLUSH BINARY LOGS;")
            filename = sql("SHOW MASTER STATUS;").split("\t")[0].strip()
            if mode == "STATEMENT":
                changes = """INSERT INTO statements VALUES (1,'中文🙂');
UPDATE statements SET txt='changed' WHERE id=1;
DELETE FROM statements WHERE id=1;"""
            else:
                changes = """INSERT INTO rows_abi VALUES
(18446744073709551615,'中文🙂',1234567890123456789012.12345678,X'00FFFE',JSON_OBJECT('n',123,'s','中文'),NULL,b'101','2026-09-20 12:34:56.123456','-10:02:03.123456','中文','x,y',-128,0.125),
(2,'',-123.00000001,X'',JSON_ARRAY(NULL,TRUE,FALSE),-2147483648,b'000','2001-01-01 00:00:00.000001','00:00:00','a','',127,-1.5);
UPDATE rows_abi SET txt='after',nullable=2147483647 WHERE id=18446744073709551615;
DELETE FROM rows_abi WHERE id=2;"""
            script = f"SET NAMES utf8mb4; SET SESSION binlog_format='{mode}'; USE fixture; BEGIN;\n{changes}\nCOMMIT;"
            case_dir = root / mode.lower()
            case_dir.mkdir()
            (case_dir / "input.sql").write_text(script, encoding="utf-8")
            sql(script)
            sql("FLUSH BINARY LOGS;")
            raw_path = case_dir / "source.binlog"
            run(["docker", "cp", f"{name}:/var/lib/mysql/{filename}", str(raw_path)])
            raw = raw_path.read_bytes()
            headers = read_headers(raw)
            output = run([str(binary), "--input", str(raw_path), "--source-file-id", source_id,
                          "--flavor", "mysql"], seconds=20)
            (case_dir / "legacy.ndjson").write_bytes(output)
            candidate_output = run(candidate_command(candidate, raw_path, source_id, mode), seconds=20)
            (case_dir / "candidate.ndjson").write_bytes(candidate_output)
            parsed = [json.loads(line) for line in output.splitlines() if line.strip()]
            candidate_parsed = [json.loads(line) for line in candidate_output.splitlines() if line.strip()]
            if not parsed or not candidate_parsed:
                raise RuntimeError("native parser emitted no fixture rows")
            bounds = {(r["start"], r["end"]) for r in headers}
            if any((r["start_position"], r["end_position"]) not in bounds for r in parsed):
                raise RuntimeError("native emitted positions outside independent event bounds")
            if len({r["event_id"] for r in parsed}) != len(parsed):
                raise RuntimeError("native fixture contains duplicate identities")
            if len({r["event_id"] for r in candidate_parsed}) != len(candidate_parsed):
                raise RuntimeError("candidate fixture contains duplicate identities")
            if not {"INSERT", "UPDATE", "DELETE"}.issubset({r["operation"] for r in parsed}):
                raise RuntimeError("native fixture lost a mutation operation")
            if any((r["start_position"], r["end_position"]) not in bounds for r in candidate_parsed):
                raise RuntimeError("candidate emitted positions outside independent event bounds")
            from tools.benchmark_raw_cache import measure
            measure(raw_path, case_dir / "cache-mechanisms", sha256=hashlib.sha256(raw).hexdigest(),
                    source_id=source_id, source_size=len(raw), physical_budget=16 * 1024 * 1024,
                    deadline_seconds=30)
            cases[mode] = dict(raw_bytes=len(raw), raw_sha256=hashlib.sha256(raw).hexdigest(),
                               source_id=source_id, header_events=headers, emitted_rows=len(parsed),
                               candidate_rows=len(candidate_parsed),
                               legacy_sha256=hashlib.sha256(output).hexdigest(),
                               candidate_sha256=hashlib.sha256(candidate_output).hexdigest())
            if mode == "ROW":
                cases[mode]["negative_streams"] = verify_negative_streams(
                    root, candidate, raw, source_id)
        proof = dict(cases=cases, source_sha=os.environ["GITHUB_SHA"],
                     native_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                     candidate_native_sha256=hashlib.sha256(candidate.read_bytes()).hexdigest(),
                     limitation="Synthetic semantic fixture only; not a throughput or production coverage gate")
        (root / "contract.json").write_text(json.dumps(proof, indent=2), encoding="utf-8")
        print(json.dumps({"fixture": "native-parser-contract", "cases": {
            k: {a: v[a] for a in ("raw_bytes", "emitted_rows", "candidate_rows", "raw_sha256")}
            for k, v in cases.items()}}))
    finally:
        details = json.loads(run(["docker", "inspect", name]))[0]
        if details["Config"]["Labels"].get("scope") != SCOPE:
            raise RuntimeError("refuse cleanup outside parser fixture ownership")
        (root / "mysql.log").write_bytes(run(["docker", "logs", name]))
        run(["docker", "stop", "--time", "10", name], seconds=20)
        run(["docker", "rm", name])


if __name__ == "__main__":
    prepare(Path("parser-contract-evidence"))
