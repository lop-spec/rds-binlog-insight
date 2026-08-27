from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path("/opt/rds-binlog-insight").resolve()
CURRENT_VERSION = "1.6.3"
CURRENT_IMAGE = f"local/rds-binlog-insight:{CURRENT_VERSION}"
NEW_IMAGE = "local/rds-binlog-insight:1.6.4"
BACKUP_IMAGE = "local/rds-binlog-insight:backup-v1.6.3-pre-v1.6.4"
RESULT_FILE = ROOT / "deploy-v1.6.4-result.json"
SOURCE_FILES = (
    ".dockerignore",
    ".env.example",
    "Dockerfile",
    "README.md",
    "requirements.txt",
    "compose.yaml",
)
SOURCE_DIRS = ("app", "web", "deploy")


def run(
    argv: list[str],
    *,
    cwd: Path | None = None,
    stdin: str | None = None,
) -> str:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        input=stdin,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return completed.stdout


def run_optional(argv: list[str], *, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return completed.stdout


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def request_json(
    path: str,
    payload: dict | None = None,
    *,
    timeout: float = 10.0,
) -> dict:
    body = (
        json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if payload is not None
        else None
    )
    request = urllib.request.Request(
        f"http://127.0.0.1:8769{path}",
        data=body,
        headers={"Content-Type": "application/json"} if body is not None else {},
        method="POST" if body is not None else "GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def health() -> dict:
    return request_json("/healthz")


def status() -> dict:
    return request_json("/api/status", timeout=30)["data"]


def settings() -> dict:
    return request_json("/api/settings")["data"]


def jobs() -> list[dict]:
    return request_json("/api/jobs", timeout=30)["data"]


def directory_bytes(path: Path) -> int:
    if not path.is_dir():
        return 0
    total = 0
    for candidate in path.rglob("*"):
        try:
            if candidate.is_file():
                total += candidate.stat().st_size
        except OSError:
            continue
    return total


def safe_extract(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        for member in members:
            if member.issym() or member.islnk():
                raise RuntimeError(f"archive links are not allowed: {member.name}")
            target = (destination / member.name).resolve()
            if not target.is_relative_to(destination):
                raise RuntimeError(f"archive path escapes staging: {member.name}")
        bundle.extractall(destination, members=members)


def atomic_copy(source: Path, target: Path, mode: int | None = None) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.deploy-v160.tmp")
    temporary.unlink(missing_ok=True)
    try:
        shutil.copy2(source, temporary)
        if mode is not None:
            temporary.chmod(mode)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def copy_source(source: Path, destination: Path) -> None:
    for name in SOURCE_FILES:
        candidate = source / name
        if candidate.is_file():
            atomic_copy(candidate, destination / name)
    for name in SOURCE_DIRS:
        candidate = source / name
        if candidate.is_dir():
            shutil.copytree(candidate, destination / name, dirs_exist_ok=True)
    parser = source / "tools" / "binlog-parser-linux-amd64"
    if parser.is_file():
        atomic_copy(
            parser,
            destination / "tools" / parser.name,
            0o555,
        )


def verify_candidate(staging: Path) -> None:
    required = (
        staging / "Dockerfile",
        staging / "compose.yaml",
        staging / "app" / "server.py",
        staging / "app" / "search_index.py",
        staging / "app" / "index_worker.py",
        staging / "app" / "index_supervisor.py",
        staging / "web" / "index.html",
        staging / "tools" / "binlog-parser-linux-amd64",
    )
    missing = [
        str(path.relative_to(staging))
        for path in required
        if not path.is_file()
    ]
    if missing:
        raise RuntimeError("candidate archive is incomplete: " + ", ".join(missing))
    compose = (staging / "compose.yaml").read_text(encoding="utf-8")
    if NEW_IMAGE not in compose:
        raise RuntimeError(f"candidate compose does not use {NEW_IMAGE}")
    if '0.0.0.0:${RDS_BINLOG_PORT:-8769}:8769' not in compose:
        raise RuntimeError("candidate would remove the public port binding")
    if "RDS_BINLOG_ALLOWED_HOSTS" not in compose:
        raise RuntimeError("candidate would remove the Host allowlist")
    if "app.index_supervisor" not in compose or "mem_limit: 3g" not in compose:
        raise RuntimeError("candidate would remove the isolated bounded indexer")
    pipeline = (staging / "app" / "pipeline.py").read_text(encoding="utf-8")
    metadata = (staging / "app" / "metadata.py").read_text(encoding="utf-8")
    storage = (staging / "app" / "storage.py").read_text(encoding="utf-8")
    if (
        "FILE_PIPELINE_WORKERS = 6" not in pipeline
        or "DOWNLOAD_PIPELINE_WORKERS = 6" not in pipeline
        or "OSS_ARCHIVE_WORKERS = 9" not in pipeline
        or "def _run_pending_parallel" not in pipeline
    ):
        raise RuntimeError("candidate would remove the bounded 6-lane pipeline")
    if "query_visible INTEGER NOT NULL DEFAULT 1" not in metadata:
        raise RuntimeError("candidate would remove the ordered visibility gate")
    if (
        "def _duckdb_connect" not in storage
        or "SET temp_directory = " not in storage
        or compose.count("TMPDIR: /data/scratch") != 2
        or compose.count("RDS_BINLOG_STAGING_DIR: /data/staging") != 2
    ):
        raise RuntimeError("candidate would return DuckDB spills to the 256 MiB tmpfs")


def candidate_runtime_probe() -> dict:
    code = """
import json
import tempfile
from pathlib import Path
import duckdb
import pyarrow
import pysqlite3
from app.pipeline import (
    DOWNLOAD_PIPELINE_WORKERS,
    FILE_PIPELINE_WORKERS,
    OSS_ARCHIVE_WORKERS,
)
from app.metadata import MetadataStore
from app.storage import EventStorage
p = Path(tempfile.mkdtemp()) / "probe.sqlite3"
c = pysqlite3.connect(p)
c.execute("CREATE VIRTUAL TABLE f USING fts5(text, content='', contentless_delete=1, detail=none, tokenize='trigram')")
c.execute("INSERT INTO f(rowid, text) VALUES(1, 'abcdef')")
c.execute("DELETE FROM f WHERE rowid=1")
remaining = c.execute("SELECT count(*) FROM f").fetchone()[0]
c.close()
data_root = p.parent / "data"
storage = EventStorage(MetadataStore(p.parent / "metadata.sqlite3"), data_root)
duck = storage._duckdb_connect()
try:
    spill = Path(duck.execute("SELECT current_setting('temp_directory')").fetchone()[0]).resolve()
finally:
    duck.close()
print(json.dumps({"sqlite": pysqlite3.sqlite_version, "pyarrow": pyarrow.__version__, "duckdb": duckdb.__version__, "ftsDeleteRemaining": remaining, "fileWorkers": FILE_PIPELINE_WORKERS, "downloadWorkers": DOWNLOAD_PIPELINE_WORKERS, "archiveWorkers": OSS_ARCHIVE_WORKERS, "duckdbSpillUnderData": spill.is_relative_to((data_root / "scratch").resolve())}))
""".strip()
    value = json.loads(
        run(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "python",
                NEW_IMAGE,
                "-c",
                code,
            ]
        )
    )
    if int(value["ftsDeleteRemaining"]) != 0:
        raise RuntimeError(f"candidate FTS delete probe failed: {value}")
    if (
        int(value["fileWorkers"]) != 6
        or int(value["downloadWorkers"]) != 6
        or int(value["archiveWorkers"]) != 9
    ):
        raise RuntimeError(f"candidate parallel pipeline probe failed: {value}")
    if not bool(value["duckdbSpillUnderData"]):
        raise RuntimeError(f"candidate DuckDB spill probe failed: {value}")
    parser_output = run(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "/app/tools/binlog-parser",
            NEW_IMAGE,
            "--checksum-stdin",
        ],
        stdin="",
    )
    parser_value = json.loads(parser_output)
    if parser_value != {
        "size_bytes": 0,
        "sha256": hashlib.sha256(b"").hexdigest(),
        "crc64": "0",
    }:
        raise RuntimeError(f"candidate parser probe failed: {parser_value}")
    return {**value, "parser": parser_value}


def pause_at_file_boundary(timeout_seconds: int = 1200) -> dict:
    request_json("/api/settings", {"autoSync": False})
    request_json("/api/sync/pause", {})
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = status()
        if not bool(current["sync"]["running"]):
            if settings().get("autoSync"):
                raise RuntimeError("auto sync was re-enabled before activation")
            return current
        if not bool(current["sync"].get("pauseRequested")):
            # A scheduler start racing between the two requests above clears
            # the pause event. Reassert it while autoSync remains disabled.
            request_json("/api/sync/pause", {})
        time.sleep(3)
    current = status()
    latest = current.get("sync", {}).get("latestJob") or {}
    raise RuntimeError(
        "sync did not reach a file boundary: "
        + str(latest.get("current_file") or "unknown")
    )


def snapshot_before() -> dict:
    try:
        health_before: object = health()
    except Exception as exc:
        unavailable = {"unavailable": True, "error": str(exc)}
        return {
            "healthBefore": unavailable,
            "settingsBefore": unavailable,
            "statusBefore": unavailable,
            "jobsBefore": unavailable,
        }
    result: dict[str, object] = {"healthBefore": health_before}
    for name, reader in (
        ("settingsBefore", settings),
        ("statusBefore", status),
        ("jobsBefore", jobs),
    ):
        try:
            result[name] = reader()
        except Exception as exc:
            result[name] = {"unavailable": True, "error": str(exc)}
    return result


def backup_current(
    backup: Path,
    archive_digest: str,
    before: dict,
) -> dict:
    backup.mkdir(mode=0o700)
    copy_source(ROOT, backup)
    env_source = ROOT / ".env"
    if env_source.is_file():
        atomic_copy(env_source, backup / ".env", 0o600)
    database = ROOT / "data" / "metadata.sqlite3"
    if database.is_file():
        source_db = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        target_db = sqlite3.connect(backup / "metadata.sqlite3")
        try:
            source_db.backup(target_db)
        finally:
            target_db.close()
            source_db.close()
        (backup / "metadata.sqlite3").chmod(0o600)
    credential_source = ROOT / "data" / ".credentials"
    if credential_source.is_dir():
        credential_backup = backup / ".credentials"
        shutil.copytree(credential_source, credential_backup)
        credential_backup.chmod(0o700)
        for path in credential_backup.iterdir():
            if path.is_file():
                path.chmod(0o600)
    run(["docker", "image", "tag", CURRENT_IMAGE, BACKUP_IMAGE])
    manifest = {
        "archiveSha256": archive_digest,
        **before,
        "currentImage": CURRENT_IMAGE,
        "backupImage": BACKUP_IMAGE,
    }
    (backup / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def restore_source(backup: Path) -> None:
    run_optional(["docker", "stop", "--time", "15", "rds-binlog-insight-indexer"])
    copy_source(backup, ROOT)
    run(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "--no-build",
            "--force-recreate",
            "insight",
        ],
        cwd=ROOT,
    )


def wait_health(version: str, timeout_seconds: int = 120) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            value = health()
            if value.get("version") == version:
                return value
            last_error = f"unexpected health: {value}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"health {version} timed out: {last_error}")


def wait_index_started(timeout_seconds: int = 120) -> dict:
    deadline = time.monotonic() + timeout_seconds
    last = {}
    while time.monotonic() < deadline:
        last = status()
        index = (last.get("sync") or {}).get("index") or {}
        supervisor = index.get("supervisor") or {}
        if bool(index.get("external")) and bool(supervisor.get("running")):
            return last
        time.sleep(2)
    raise RuntimeError(f"index backfill did not start: {last}")


def resume_sync() -> dict:
    request_json("/api/settings", {"autoSync": True})
    try:
        request_json("/api/sync/start", {})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise
    deadline = time.monotonic() + 60
    latest = {}
    while time.monotonic() < deadline:
        latest = status()
        if bool(latest.get("sync", {}).get("running")):
            return latest
        time.sleep(2)
    return latest


def emit_result(value: dict, *, error: bool = False) -> None:
    temporary = RESULT_FILE.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temporary, RESULT_FILE)
    print(
        json.dumps(value, ensure_ascii=False),
        file=sys.stderr if error else sys.stdout,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--backup-name", required=True)
    args = parser.parse_args()
    RESULT_FILE.unlink(missing_ok=True)
    if ROOT != Path("/opt/rds-binlog-insight"):
        raise RuntimeError(f"unexpected deployment root: {ROOT}")
    archive = args.archive.resolve()
    if not archive.is_file() or not archive.is_relative_to(ROOT):
        raise RuntimeError("archive must be a file inside the deployment root")
    actual_digest = sha256(archive)
    if actual_digest.lower() != args.sha256.lower():
        raise RuntimeError(
            f"archive SHA-256 mismatch: expected={args.sha256} actual={actual_digest}"
        )
    if "/" in args.backup_name or "\\" in args.backup_name:
        raise RuntimeError("backup name must be one directory name")
    backup = (ROOT / args.backup_name).resolve()
    if not backup.is_relative_to(ROOT) or backup.exists():
        raise RuntimeError(f"invalid backup target: {backup}")
    staging = (ROOT / ".deploy-v1.6.4-staging").resolve()
    if not staging.is_relative_to(ROOT) or staging.exists():
        raise RuntimeError(f"invalid staging target: {staging}")

    staging.mkdir(mode=0o700)
    activated = False
    backup_created = False
    paused = False
    current_stopped = False
    try:
        safe_extract(archive, staging)
        verify_candidate(staging)
        build_output = run(
            ["docker", "build", "--pull=false", "-t", NEW_IMAGE, "."],
            cwd=staging,
        )
        image_id = run(
            ["docker", "image", "inspect", NEW_IMAGE, "--format", "{{.Id}}"]
        ).strip()
        runtime_probe = candidate_runtime_probe()

        cache_before = {
            "queryCacheBytes": directory_bytes(ROOT / "data" / "query-cache"),
            "cacheBytes": directory_bytes(ROOT / "data" / "cache"),
        }
        before = snapshot_before()
        try:
            boundary = pause_at_file_boundary()
            paused = True
        except Exception as exc:
            boundary = {
                "unhealthyRecovery": True,
                "pauseError": str(exc),
            }
            # The persisted pipeline is chunk-atomic. Quiescing an unresponsive
            # container leaves the raw Binlog and SQLite checkpoint recoverable.
            run(["docker", "stop", "--time", "30", "rds-binlog-insight"])
            current_stopped = True
        manifest = backup_current(backup, actual_digest, before)
        backup_created = True
        copy_source(staging, ROOT)
        current_stopped = True
        run(
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--no-build",
                "--force-recreate",
                "insight",
                "indexer",
            ],
            cwd=ROOT,
        )
        activated = True
        current_health = wait_health("1.6.4")
        indexed_status = wait_index_started()
        inspect = json.loads(run(["docker", "inspect", "rds-binlog-insight"]))[0]
        indexer_inspect = json.loads(
            run(["docker", "inspect", "rds-binlog-insight-indexer"])
        )[0]
        ports = inspect["NetworkSettings"]["Ports"]["8769/tcp"]
        if not any(item.get("HostIp") == "0.0.0.0" for item in ports or []):
            raise RuntimeError(f"public port verification failed: {ports}")
        if inspect["Config"]["Image"] != NEW_IMAGE:
            raise RuntimeError(f"running image mismatch: {inspect['Config']['Image']}")
        if indexer_inspect["Config"]["Image"] != NEW_IMAGE:
            raise RuntimeError(
                f"indexer image mismatch: {indexer_inspect['Config']['Image']}"
            )
        if int(indexer_inspect["HostConfig"].get("Memory") or 0) != 3 * 1024**3:
            raise RuntimeError("indexer memory limit was not applied")
        for label, inspected in (
            ("insight", inspect),
            ("indexer", indexer_inspect),
        ):
            if "TMPDIR=/data/scratch" not in (inspected["Config"].get("Env") or []):
                raise RuntimeError(
                    f"{label} persistent spill directory was not applied"
                )
            if "RDS_BINLOG_STAGING_DIR=/data/staging" not in (
                inspected["Config"].get("Env") or []
            ):
                raise RuntimeError(
                    f"{label} persistent staging directory was not applied"
                )
        active_settings = settings()
        if not active_settings.get("ossEnabled"):
            raise RuntimeError("active OSS settings were not preserved")
        if "queryCacheBytes" in active_settings or "localCacheBytes" in active_settings:
            raise RuntimeError("legacy body cache settings are still exposed")
        cache_after = {
            "queryCacheBytes": directory_bytes(ROOT / "data" / "query-cache"),
            "cacheBytes": directory_bytes(ROOT / "data" / "cache"),
        }
        resumed = resume_sync()
        pipeline_status = (resumed.get("sync") or {}).get("pipeline") or {}
        if (
            int(pipeline_status.get("fileWorkers") or 0) != 6
            or int(pipeline_status.get("downloadWorkers") or 0) != 6
            or int(pipeline_status.get("archiveWorkers") or 0) != 9
        ):
            raise RuntimeError(
                f"parallel pipeline status mismatch: {pipeline_status}"
            )
        result = {
            "ok": True,
            "health": current_health,
            "image": NEW_IMAGE,
            "imageId": image_id,
            "ports": ports,
            "indexerMemory": indexer_inspect["HostConfig"].get("Memory"),
            "backup": str(backup),
            "backupImage": BACKUP_IMAGE,
            "runtimeProbe": runtime_probe,
            "cacheBefore": cache_before,
            "cacheAfter": cache_after,
            "boundary": boundary.get("sync"),
            "index": indexed_status.get("summary"),
            "syncResumed": bool(resumed.get("sync", {}).get("running")),
            "pipeline": pipeline_status,
            "buildTail": build_output.splitlines()[-10:],
            "before": manifest.get("healthBefore"),
        }
        emit_result(result)
        return 0
    except Exception as exc:
        rollback_error = ""
        if backup_created and (activated or current_stopped):
            try:
                restore_source(backup)
                wait_health(CURRENT_VERSION)
            except Exception as rollback_exc:
                rollback_error = str(rollback_exc)
        if paused:
            try:
                request_json("/api/settings", {"autoSync": True})
            except Exception as resume_exc:
                rollback_error = (
                    rollback_error + "; " if rollback_error else ""
                ) + f"resume failed: {resume_exc}"
        emit_result(
            {
                "ok": False,
                "error": str(exc),
                "rollbackError": rollback_error,
            },
            error=True,
        )
        return 1
    finally:
        if staging.exists():
            shutil.rmtree(staging)


if __name__ == "__main__":
    raise SystemExit(main())
