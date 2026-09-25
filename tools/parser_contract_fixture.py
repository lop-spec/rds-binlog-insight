"""Cloud-CI-only native parser contract fixtures; no production/network source.

Retains full, synthetic binlog + legacy output for source-rebuild differential
checks. A fixture is NOT a production throughput or full-engine coverage claim.
"""
from __future__ import annotations

import hashlib
import json
import os
import queue
import shutil
import struct
import subprocess
import threading
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


def run_candidate_arrow_chunks(
    binary,
    raw_path,
    source_id,
    mode,
    case_dir,
    *,
    output_label="candidate-arrow-chunks",
    extra_args=(),
):
    """Exercise the real manifest/ACK transport and retain byte-for-byte evidence."""
    output_dir = (case_dir / output_label).resolve()
    output_dir.mkdir()
    manifest_path = case_dir / f"{output_label}.manifests.ndjson"
    ack_path = case_dir / f"{output_label}.acks.ndjson"
    stderr_path = case_dir / f"{output_label}.stderr"
    command = candidate_command(binary, raw_path, source_id, mode) + list(extra_args) + [
        "--output-dir", str(output_dir),
        "--chunk-format", "arrow",
        "--chunk-max-lines", "3",
        "--chunk-max-bytes", str(128 * 1024 * 1024),
    ]
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        process.kill()
        raise RuntimeError("candidate Arrow chunk pipes are unavailable")
    stdout_items = queue.Queue()

    def read_stdout():
        try:
            for line in process.stdout:
                stdout_items.put(line)
        except BaseException as exc:
            stdout_items.put(exc)
        finally:
            stdout_items.put(None)

    stdout_thread = threading.Thread(
        target=read_stdout,
        name="parser-contract-arrow-manifests",
        daemon=True,
    )
    stdout_thread.start()
    manifests = bytearray()
    acknowledgements = bytearray()
    chunks = []
    expected_sequence = 0
    deadline = time.monotonic() + 30
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("candidate Arrow chunk protocol timed out")
            try:
                item = stdout_items.get(timeout=remaining)
            except queue.Empty as exc:
                raise TimeoutError("candidate Arrow chunk manifest stalled") from exc
            if item is None:
                break
            if isinstance(item, BaseException):
                raise RuntimeError("candidate Arrow manifest reader failed") from item
            line = item
            manifests.extend(line)
            try:
                manifest = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("candidate emitted malformed Arrow manifest") from exc
            expected_fields = {
                "protocol", "format", "sequence", "path", "rows", "bytes",
                "decoded_bytes",
            }
            if not isinstance(manifest, dict) or set(manifest) != expected_fields:
                raise RuntimeError("candidate emitted a non-strict Arrow manifest")
            if (manifest["protocol"] != "parser-chunk-v1"
                    or manifest["format"] != "arrow-ipc-file-v1"
                    or type(manifest["sequence"]) is not int
                    or manifest["sequence"] != expected_sequence
                    or type(manifest["path"]) is not str
                    or type(manifest["rows"]) is not int
                    or not 0 < manifest["rows"] <= 3
                    or type(manifest["bytes"]) is not int
                    or not 0 < manifest["bytes"] <= 128 * 1024 * 1024
                    or type(manifest["decoded_bytes"]) is not int
                    or not 0 < manifest["decoded_bytes"] <= 128 * 1024 * 1024):
                raise RuntimeError("candidate Arrow manifest violates its bounded protocol")
            chunk = Path(manifest["path"])
            if (not chunk.is_absolute() or chunk.parent != output_dir
                    or chunk.name != f"{source_id}-{expected_sequence:06d}.arrow"
                    or not chunk.is_file()
                    or chunk.stat().st_size != manifest["bytes"]):
                raise RuntimeError("candidate Arrow manifest does not bind its publication")
            content = chunk.read_bytes()
            chunks.append({
                "sequence": expected_sequence,
                "name": chunk.name,
                "rows": manifest["rows"],
                "bytes": manifest["bytes"],
                "decoded_bytes": manifest["decoded_bytes"],
                "sha256": hashlib.sha256(content).hexdigest(),
            })
            acknowledgement = (json.dumps({
                "protocol": "parser-chunk-ack-v1",
                "sequence": expected_sequence,
                "status": "ok",
            }, separators=(",", ":")) + "\n").encode("utf-8")
            acknowledgements.extend(acknowledgement)
            process.stdin.write(acknowledgement)
            process.stdin.flush()
            expected_sequence += 1
        process.stdin.close()
        stderr_bytes = process.stderr.read()
        stderr = stderr_bytes.decode("utf-8", "replace")
        returncode = process.wait(timeout=max(deadline - time.monotonic(), 0.1))
        if returncode:
            raise RuntimeError(
                f"candidate Arrow chunk producer failed {returncode}: {stderr[-3000:]}"
            )
        expected_stderr = f"parsed {sum(chunk['rows'] for chunk in chunks)} audit records"
        if stderr.strip() != expected_stderr:
            raise RuntimeError("candidate Arrow chunk completion record changed")
        if not chunks:
            raise RuntimeError("candidate Arrow chunk producer emitted no chunks")
        if list(output_dir.glob("*.part")):
            raise RuntimeError("candidate Arrow chunk producer retained partial files")
        manifest_path.write_bytes(manifests)
        ack_path.write_bytes(acknowledgements)
        stderr_path.write_bytes(stderr_bytes)
        return {
            "protocol": "parser-chunk-v1",
            "format": "arrow-ipc-file-v1",
            "ack_protocol": "parser-chunk-ack-v1",
            "manifests_sha256": hashlib.sha256(manifests).hexdigest(),
            "acks_sha256": hashlib.sha256(acknowledgements).hexdigest(),
            "stderr_sha256": hashlib.sha256(stderr_bytes).hexdigest(),
            "chunks": chunks,
        }
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        raise
    finally:
        stdout_thread.join(timeout=5)


def run_failed_chunk_transport(
    binary,
    source,
    source_id,
    mode,
    output_dir,
    *,
    chunk_format,
    extra_args=(),
):
    """ACK every published one-row chunk, then clean collector-owned files."""
    output_dir.mkdir()
    command = candidate_command(binary, source, source_id, mode) + list(extra_args) + [
        "--output-dir", str(output_dir),
        "--chunk-max-lines", "1",
        "--chunk-max-bytes", str(128 * 1024 * 1024),
    ]
    if chunk_format == "arrow":
        command += ["--chunk-format", "arrow"]
        supplied_acknowledgements = [
            (json.dumps({
                "protocol": "parser-chunk-ack-v1",
                "sequence": sequence,
                "status": "ok",
            }, separators=(",", ":")) + "\n").encode("utf-8")
            for sequence in range(256)
        ]
        expected_fields = {
            "protocol", "format", "sequence", "path", "rows", "bytes",
            "decoded_bytes",
        }
        suffix = ".arrow"
    elif chunk_format == "ndjson":
        supplied_acknowledgements = [b"ok\n"] * 256
        expected_fields = {
            "protocol", "format", "sequence", "path", "rows", "bytes",
        }
        suffix = ".ndjson"
    else:
        raise ValueError("unsupported failed chunk transport")

    result = subprocess.run(
        command,
        input=b"".join(supplied_acknowledgements),
        capture_output=True,
        timeout=20,
    )
    raw_lines = result.stdout.splitlines(keepends=True)
    if any(not line.endswith(b"\n") for line in raw_lines):
        raise RuntimeError("failed chunk manifest transcript is not line framed")
    for sequence, line in enumerate(raw_lines):
        try:
            manifest = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("failed chunk manifest transcript is malformed") from exc
        expected_format = (
            "arrow-ipc-file-v1" if chunk_format == "arrow" else "ndjson-v1"
        )
        if (
            not isinstance(manifest, dict)
            or set(manifest) != expected_fields
            or manifest.get("protocol") != "parser-chunk-v1"
            or manifest.get("format") != expected_format
            or type(manifest.get("sequence")) is not int
            or manifest["sequence"] != sequence
            or type(manifest.get("path")) is not str
            or type(manifest.get("rows")) is not int
            or manifest["rows"] != 1
            or type(manifest.get("bytes")) is not int
            or not 0 < manifest["bytes"] <= 128 * 1024 * 1024
        ):
            raise RuntimeError("failed chunk manifest violates its strict contract")
        if chunk_format == "arrow" and (
            type(manifest.get("decoded_bytes")) is not int
            or not 0 < manifest["decoded_bytes"] <= 128 * 1024 * 1024
        ):
            raise RuntimeError("failed Arrow chunk decoded bound is malformed")
        chunk = Path(manifest["path"])
        if (
            not chunk.is_absolute()
            or chunk.parent != output_dir.resolve()
            or chunk.name != f"{source_id}-{sequence:06d}{suffix}"
            or not chunk.is_file()
            or chunk.stat().st_size != manifest["bytes"]
        ):
            raise RuntimeError("failed chunk manifest does not bind its publication")
        # Every manifest was followed by the corresponding pre-supplied ACK;
        # after the later parse failure these finals belong to the collector.
        chunk.unlink()
    if any(output_dir.iterdir()):
        raise RuntimeError("failed chunk transport retained final or partial output")
    acknowledged = b"".join(supplied_acknowledgements[:len(raw_lines)])
    return result, {
        "acknowledged_files": len(raw_lines),
        "manifests_sha256": hashlib.sha256(result.stdout).hexdigest(),
        "acks_sha256": hashlib.sha256(acknowledged).hexdigest(),
    }, acknowledged


def raw_cache_negative_streams(cache, expected_size):
    """Mutate each strict RDSRAW1 boundary without using the Go reader."""
    content = bytes(cache)
    if len(content) < 53 + 44 + 44 or content[:8] != b"RDSRAW1\n":
        raise ValueError("valid raw-cache fixture is too short")
    frame_at = 53
    if content[frame_at : frame_at + 4] != b"FRM1":
        raise ValueError("raw-cache fixture has no first frame")
    compressed_size = struct.unpack_from("<I", content, frame_at + 8)[0]
    footer_at = frame_at + 44 + compressed_size
    if content[footer_at : footer_at + 4] != b"END1" or footer_at + 44 != len(content):
        raise ValueError("raw-cache fixture has a non-canonical footer")

    def changed(offset, value):
        result = bytearray(content)
        result[offset] = value
        return bytes(result)

    wrong_size = bytearray(content)
    struct.pack_into("<Q", wrong_size, 9, expected_size + 1)
    bad_frame_marker = bytearray(content)
    bad_frame_marker[frame_at : frame_at + 4] = b"BAD1"
    bad_footer_marker = bytearray(content)
    bad_footer_marker[footer_at : footer_at + 4] = b"BAD1"
    return {
        "truncated-header": (content[:52], expected_size, "header"),
        "bad-magic": (changed(0, content[0] ^ 0x01), expected_size, "magic"),
        "unsupported-codec": (changed(8, 0x7F), expected_size, "codec"),
        "wrong-identity": (changed(17, content[17] ^ 0x01), expected_size, "identity"),
        "wrong-declared-size": (bytes(wrong_size), expected_size, "expected size"),
        "wrong-expected-size-argument": (content, expected_size + 1, "expected size"),
        "bad-frame-marker": (bytes(bad_frame_marker), expected_size, "frame marker"),
        "bad-frame-sha": (changed(frame_at + 12, content[frame_at + 12] ^ 0x01), expected_size, "size/SHA256"),
        "bad-footer-marker": (bytes(bad_footer_marker), expected_size, "frame marker"),
        "truncated-footer": (content[:-1], expected_size, "record"),
        "bad-final-sha": (changed(len(content) - 1, content[-1] ^ 0x01), expected_size, "final SHA256"),
        "trailing-data": (content + b"x", expected_size, "trailing data"),
    }


def verify_negative_raw_cache(root, binary, cache_path, source_id, expected_size):
    """Require raw-cache failures before any NDJSON/Arrow publication or ACK."""
    negative = root / "negative" / "raw-cache"
    negative.mkdir()
    proof = {}
    for name, (content, supplied_size, marker) in raw_cache_negative_streams(
        cache_path.read_bytes(), expected_size
    ).items():
        source = negative / f"{name}.cache"
        source.write_bytes(content)
        raw_args = [
            "--raw-cache-source-id",
            source_id,
            "--raw-cache-expected-size",
            str(supplied_size),
        ]
        result = subprocess.run(
            candidate_command(binary, source, source_id, "ROW") + raw_args,
            capture_output=True,
            timeout=20,
        )
        stderr = result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.ndjson.stdout").write_bytes(result.stdout)
        (negative / f"{name}.ndjson.stderr").write_bytes(result.stderr)
        if result.returncode == 0 or result.stdout:
            raise RuntimeError(f"candidate raw-cache NDJSON accepted or emitted {name}")
        if marker.lower() not in stderr.lower():
            raise RuntimeError(
                f"candidate raw-cache NDJSON {name} lacks {marker!r}: {stderr[-1000:]}"
            )

        arrow_output = negative / f"{name}.arrow"
        arrow_result = subprocess.run(
            candidate_command(binary, source, source_id, "ROW")
            + raw_args
            + ["--arrow-output", str(arrow_output)],
            capture_output=True,
            timeout=20,
        )
        arrow_stderr = arrow_result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.arrow.stdout").write_bytes(arrow_result.stdout)
        (negative / f"{name}.arrow.stderr").write_bytes(arrow_result.stderr)
        if arrow_result.returncode == 0 or arrow_result.stdout:
            raise RuntimeError(f"candidate raw-cache Arrow accepted or emitted {name}")
        if marker.lower() not in arrow_stderr.lower():
            raise RuntimeError(
                f"candidate raw-cache Arrow {name} lacks {marker!r}: "
                f"{arrow_stderr[-1000:]}"
            )
        if arrow_output.exists() or arrow_output.with_name(
            arrow_output.name + ".part"
        ).exists():
            raise RuntimeError(f"candidate raw-cache Arrow published partial {name}")

        chunk_output = negative / f"{name}-arrow-chunk-output"
        chunk_result, chunk_proof, chunk_acks = run_failed_chunk_transport(
            binary,
            source,
            source_id,
            "ROW",
            chunk_output,
            chunk_format="arrow",
            extra_args=raw_args,
        )
        chunk_stderr = chunk_result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.arrow-chunk.manifests.ndjson").write_bytes(
            chunk_result.stdout
        )
        (negative / f"{name}.arrow-chunk.acks.ndjson").write_bytes(chunk_acks)
        (negative / f"{name}.arrow-chunk.stderr").write_bytes(chunk_result.stderr)
        if chunk_result.returncode == 0:
            raise RuntimeError(f"candidate raw-cache Arrow chunk accepted {name}")
        if marker.lower() not in chunk_stderr.lower():
            raise RuntimeError(
                f"candidate raw-cache Arrow chunk {name} lacks {marker!r}: "
                f"{chunk_stderr[-1000:]}"
            )
        if chunk_proof["acknowledged_files"] != 0 or any(chunk_output.iterdir()):
            raise RuntimeError(
                f"candidate raw-cache Arrow chunk published before validation: {name}"
            )
        proof[name] = {
            "cache_bytes": len(content),
            "cache_sha256": hashlib.sha256(content).hexdigest(),
            "expected_size_argument": supplied_size,
            "stderr_marker": marker,
            "ndjson_returncode": result.returncode,
            "ndjson_stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
            "arrow_returncode": arrow_result.returncode,
            "arrow_stderr_sha256": hashlib.sha256(arrow_result.stderr).hexdigest(),
            "arrow_chunk_returncode": chunk_result.returncode,
            "arrow_chunk_stderr_sha256": hashlib.sha256(
                chunk_result.stderr
            ).hexdigest(),
            "arrow_chunk_manifests_sha256": chunk_proof["manifests_sha256"],
            "arrow_chunk_acks_sha256": chunk_proof["acks_sha256"],
            "arrow_chunk_acknowledged_files": 0,
            "published_files": 0,
        }
    return proof


def verify_negative_streams(root, binary, raw, source_id):
    negative = root / "negative"
    negative.mkdir()
    proof = {}
    for name, (content, marker) in negative_streams(raw).items():
        source = negative / f"{name}.binlog"
        source.write_bytes(content)
        output_dir = negative / f"{name}-output"
        result, ndjson_chunk_proof, ndjson_acks = run_failed_chunk_transport(
            binary, source, source_id, "ROW", output_dir, chunk_format="ndjson"
        )
        (negative / f"{name}.ndjson-chunk.manifests.ndjson").write_bytes(
            result.stdout
        )
        (negative / f"{name}.ndjson-chunk.acks.ndjson").write_bytes(ndjson_acks)
        stderr = result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.stderr").write_text(stderr, encoding="utf-8")
        if result.returncode == 0:
            raise RuntimeError(f"candidate accepted corrupt stream {name}")
        if marker.lower() not in stderr.lower():
            raise RuntimeError(f"candidate {name} failure lacks {marker!r}: {stderr[-1000:]}")
        if output_dir.exists() and any(output_dir.iterdir()):
            raise RuntimeError(f"candidate published partial NDJSON output for {name}")

        arrow_output = negative / f"{name}.arrow"
        arrow_result = subprocess.run(candidate_command(binary, source, source_id, "ROW")
                                      + ["--arrow-output", str(arrow_output)],
                                      capture_output=True, timeout=20)
        arrow_stderr = arrow_result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.arrow.stderr").write_text(arrow_stderr, encoding="utf-8")
        if arrow_result.returncode == 0:
            raise RuntimeError(f"candidate Arrow producer accepted corrupt stream {name}")
        if marker.lower() not in arrow_stderr.lower():
            raise RuntimeError(f"candidate Arrow {name} failure lacks {marker!r}: {arrow_stderr[-1000:]}")
        if arrow_output.exists() or arrow_output.with_name(arrow_output.name + ".part").exists():
            raise RuntimeError(f"candidate published partial Arrow output for {name}")

        arrow_chunk_output = negative / f"{name}-arrow-chunk-output"
        arrow_chunk_result, arrow_chunk_proof, arrow_chunk_acks = (
            run_failed_chunk_transport(
                binary,
                source,
                source_id,
                "ROW",
                arrow_chunk_output,
                chunk_format="arrow",
            )
        )
        (negative / f"{name}.arrow-chunk.manifests.ndjson").write_bytes(
            arrow_chunk_result.stdout
        )
        (negative / f"{name}.arrow-chunk.acks.ndjson").write_bytes(
            arrow_chunk_acks
        )
        arrow_chunk_stderr = arrow_chunk_result.stderr.decode("utf-8", "replace")
        (negative / f"{name}.arrow-chunk.stderr").write_text(
            arrow_chunk_stderr, encoding="utf-8"
        )
        if arrow_chunk_result.returncode == 0:
            raise RuntimeError(
                f"candidate Arrow chunk producer accepted corrupt stream {name}"
            )
        if marker.lower() not in arrow_chunk_stderr.lower():
            raise RuntimeError(
                f"candidate Arrow chunk {name} failure lacks {marker!r}: "
                f"{arrow_chunk_stderr[-1000:]}"
            )
        if any(arrow_chunk_output.iterdir()):
            raise RuntimeError(
                f"candidate retained Arrow chunk output for {name}"
            )
        proof[name] = {
            "returncode": result.returncode,
            "stderr_marker": marker,
            "published_files": 0,
            "ndjson_chunk_acknowledged_files": ndjson_chunk_proof[
                "acknowledged_files"
            ],
            "ndjson_chunk_manifests_sha256": ndjson_chunk_proof[
                "manifests_sha256"
            ],
            "ndjson_chunk_acks_sha256": ndjson_chunk_proof["acks_sha256"],
            "arrow_returncode": arrow_result.returncode,
            "arrow_stderr_marker": marker,
            "arrow_published_files": 0,
            "arrow_chunk_returncode": arrow_chunk_result.returncode,
            "arrow_chunk_stderr_marker": marker,
            "arrow_chunk_published_files": 0,
            "arrow_chunk_acknowledged_files": arrow_chunk_proof[
                "acknowledged_files"
            ],
            "arrow_chunk_manifests_sha256": arrow_chunk_proof[
                "manifests_sha256"
            ],
            "arrow_chunk_acks_sha256": arrow_chunk_proof["acks_sha256"],
            "sha256": hashlib.sha256(content).hexdigest(),
        }
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
    candidate_evidence = root / "candidate-binlog-parser-linux-amd64"
    shutil.copyfile(candidate, candidate_evidence)
    candidate_evidence.chmod(0o555)
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
            candidate_arrow = case_dir / "candidate.arrow"
            arrow_stdout = run(candidate_command(candidate, raw_path, source_id, mode)
                               + ["--arrow-output", str(candidate_arrow)], seconds=20)
            if arrow_stdout.strip():
                raise RuntimeError("candidate Arrow producer leaked NDJSON to stdout")
            if not candidate_arrow.is_file() or candidate_arrow.with_name(candidate_arrow.name + ".part").exists():
                raise RuntimeError("candidate Arrow producer did not atomically publish its IPC file")
            chunk_contract = run_candidate_arrow_chunks(
                candidate, raw_path, source_id, mode, case_dir
            )
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
            cache_dir = case_dir / "cache-mechanisms"
            measure(raw_path, cache_dir, sha256=hashlib.sha256(raw).hexdigest(),
                    source_id=source_id, source_size=len(raw), physical_budget=16 * 1024 * 1024,
                    deadline_seconds=30)
            raw_cache_contract = {}
            for codec in ("zstd", "lz4_frame"):
                cache_path = cache_dir / f"{codec}.cache"
                cache_output = run(
                    candidate_command(candidate, cache_path, source_id, mode) + [
                        "--raw-cache-source-id", source_id,
                        "--raw-cache-expected-size", str(len(raw)),
                    ],
                    seconds=20,
                )
                output_path = case_dir / f"candidate-{codec}-cache.ndjson"
                output_path.write_bytes(cache_output)
                if cache_output != candidate_output:
                    raise RuntimeError(
                        f"candidate {codec} raw-cache replay changed parser output"
                    )
                raw_args = [
                    "--raw-cache-source-id",
                    source_id,
                    "--raw-cache-expected-size",
                    str(len(raw)),
                ]
                cache_arrow = case_dir / f"candidate-{codec}-cache.arrow"
                cache_arrow_stdout = run(
                    candidate_command(candidate, cache_path, source_id, mode)
                    + raw_args
                    + ["--arrow-output", str(cache_arrow)],
                    seconds=20,
                )
                if cache_arrow_stdout or not cache_arrow.is_file():
                    raise RuntimeError(
                        f"candidate {codec} raw-cache Arrow publication failed"
                    )
                cache_chunk_contract = run_candidate_arrow_chunks(
                    candidate,
                    cache_path,
                    source_id,
                    mode,
                    case_dir,
                    output_label=f"candidate-{codec}-cache-arrow-chunks",
                    extra_args=raw_args,
                )
                raw_cache_contract[codec] = {
                    "cache_bytes": cache_path.stat().st_size,
                    "cache_sha256": hashlib.sha256(cache_path.read_bytes()).hexdigest(),
                    "output_sha256": hashlib.sha256(cache_output).hexdigest(),
                    "arrow_bytes": cache_arrow.stat().st_size,
                    "arrow_sha256": hashlib.sha256(cache_arrow.read_bytes()).hexdigest(),
                    "arrow_chunks": cache_chunk_contract,
                }
            cases[mode] = dict(raw_bytes=len(raw), raw_sha256=hashlib.sha256(raw).hexdigest(),
                               source_id=source_id, header_events=headers, emitted_rows=len(parsed),
                               candidate_rows=len(candidate_parsed),
                               legacy_sha256=hashlib.sha256(output).hexdigest(),
                               candidate_sha256=hashlib.sha256(candidate_output).hexdigest(),
                               candidate_arrow_bytes=candidate_arrow.stat().st_size,
                               candidate_arrow_sha256=hashlib.sha256(candidate_arrow.read_bytes()).hexdigest(),
                               candidate_arrow_chunks=chunk_contract,
                               candidate_raw_cache=raw_cache_contract)
            if mode == "ROW":
                cases[mode]["negative_streams"] = verify_negative_streams(
                    root, candidate, raw, source_id
                )
                cases[mode]["negative_raw_cache"] = verify_negative_raw_cache(
                    root,
                    candidate,
                    cache_dir / "zstd.cache",
                    source_id,
                    len(raw),
                )
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
