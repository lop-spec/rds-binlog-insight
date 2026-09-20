"""Measure lossless cache mechanisms on a frozen local input, not E2E throughput.

Run only in an explicitly bounded disposable runner/container. The caller owns
CPU/RSS/I/O hard limits; this module enforces source identity, output space and a
cooperative deadline. It never contacts RDS/OSS, deletes input, or overwrites an
output. Retained results and partial failures count against the space budget.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

from app.raw_cache import RawCacheWriter, iter_raw


def measure(source: Path, destination: Path, *, sha256: str, source_id: str,
            source_size: int, physical_budget: int, deadline_seconds: float,
            codecs=("zstd", "lz4_frame")):
    if not codecs or len(set(codecs)) != len(codecs) or set(codecs) - {"zstd", "lz4_frame"}:
        raise ValueError("invalid or duplicate codec list")
    if source.stat().st_size != source_size or source_size <= 0:
        raise ValueError("frozen input size mismatch/empty source")
    if deadline_seconds <= 0:
        raise ValueError("deadline must be positive")
    if any(len(v) != 64 or any(c not in "0123456789abcdef" for c in v)
           for v in (sha256, source_id)):
        raise ValueError("frozen SHA/source identity must be lowercase SHA256")
    report_reserve = 16 * 1024
    if physical_budget < report_reserve + 1024:
        raise ValueError("physical budget must also cover retained measurement/failure output")
    destination.mkdir(exist_ok=False)
    started = time.monotonic()
    stop_at = started + deadline_seconds
    results = []
    proof = {"scope": "raw-cache mechanism only, not end-to-end throughput",
             "source_id": source_id, "source_sha256": sha256, "source_bytes": source_size,
             "physical_budget": physical_budget, "codecs": results, "complete": False}

    def check():
        if time.monotonic() >= stop_at:
            raise TimeoutError("raw cache experiment deadline exceeded")

    try:
        retained = 0
        for codec in codecs:
            check()
            target = destination / (codec + ".cache")
            wall = time.monotonic(); cpu = time.process_time()
            with source.open("rb") as input_file, RawCacheWriter(
                    target, source_id=source_id, expected_size=source_size,
                    physical_budget=physical_budget-report_reserve-retained, codec=codec) as writer:
                before = os.fstat(input_file.fileno())
                while data := input_file.read(1024 * 1024):
                    check(); writer.write(data)
                after = os.fstat(input_file.fileno())
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                        after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise ValueError("source changed while reading")
                writer.finish(expected_sha256=sha256)
            write_cpu = time.process_time()-cpu
            write_seconds = time.monotonic()-wall
            physical = target.stat().st_size
            retained += physical
            wall = time.monotonic(); cpu = time.process_time()
            recovered = hashlib.sha256(); count = 0
            for raw in iter_raw(target, source_id=source_id, expected_size=source_size):
                check(); recovered.update(raw); count += len(raw)
            if count != source_size or recovered.hexdigest() != sha256:
                raise ValueError("complete decompressed source differs from frozen oracle")
            results.append({"codec": codec, "cache_bytes": physical,
                            "cache_to_raw_ratio": physical/source_size,
                            "write_seconds": write_seconds, "write_process_cpu_seconds": write_cpu,
                            "replay_seconds": time.monotonic()-wall,
                            "replay_process_cpu_seconds": time.process_time()-cpu,
                            "full_roundtrip_sha_verified": True})
        check()
        proof["retained_cache_bytes"] = retained
        proof["complete"] = True
        return proof
    except BaseException as exc:
        proof["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        proof["wall_seconds"] = time.monotonic()-started
        with (destination / "measurement.json").open("x", encoding="utf-8") as handle:
            json.dump(proof, handle, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-size", type=int, required=True)
    parser.add_argument("--physical-budget", type=int, required=True)
    parser.add_argument("--deadline-seconds", type=float, default=120)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("CLI is cloud-fixture only until real-input resource/authorization gates exist")
    result = measure(args.input, args.output_dir, sha256=args.sha256, source_id=args.source_id,
                     source_size=args.source_size, physical_budget=args.physical_budget,
                     deadline_seconds=args.deadline_seconds)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
