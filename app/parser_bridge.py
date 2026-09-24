from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import app_root


NATIVE_CHUNK_MAX_LINES = 200_000
NATIVE_CHUNK_MAX_BYTES = 128 * 1024 * 1024
NATIVE_CHUNK_PREFETCH = 1
NATIVE_CHUNK_MAX_OUTSTANDING = NATIVE_CHUNK_PREFETCH + 1
NATIVE_MANIFEST_MAX_BYTES = 64 * 1024
PARSER_TRANSPORT_NDJSON = "ndjson"
PARSER_TRANSPORT_ARROW = "arrow"
PARSER_CHUNK_PROTOCOL = "parser-chunk-v1"
PARSER_ARROW_MANIFEST_FORMAT = "arrow-ipc-file-v1"
PARSER_NDJSON_MANIFEST_FORMAT = "ndjson-v1"
PARSER_ARROW_ACK_PROTOCOL = "parser-chunk-ack-v1"
# Shared by primary/secondary managers in the collector process. Physical and
# decoded chunk sizes are both bounded; the remaining tmpfs capacity covers
# manifests, stderr, DuckDB scratch and atomic-publication overlap.
NATIVE_STAGING_BUDGET_BYTES = 768 * 1024 * 1024
_NATIVE_STAGING_LOCK = threading.Condition()
_NATIVE_STAGING_ACTIVE: dict[Path, dict[str, int]] = {}


def _cleanup_native_chunk_artifacts(
    staging_root: Path,
    source_file_id: str | None = None,
    *,
    keep_source_ids: set[str] | None = None,
) -> None:
    """Remove parser-owned orphans, never another admitted lane's chunks."""

    for candidate in staging_root.iterdir():
        name = candidate.name
        suffix = next(
            (
                value
                for value in (
                    ".ndjson.part", ".arrow.part", ".ndjson", ".arrow"
                )
                if name.endswith(value)
            ),
            "",
        )
        if not suffix:
            continue
        stem = name[: -len(suffix)]
        prefix, separator, index = stem.rpartition("-")
        if (
            not separator
            or not prefix
            or len(index) < 6
            or not index.isascii()
            or not index.isdecimal()
            or (source_file_id is not None and prefix != source_file_id)
            or (keep_source_ids is not None and prefix in keep_source_ids)
        ):
            continue
        if candidate.is_file() or candidate.is_symlink():
            candidate.unlink(missing_ok=True)


class ParserError(RuntimeError):
    def __init__(self, message: str, code: str = "PARSER_ERROR"):
        super().__init__(message)
        self.code = code


def _validate_chunk_source_file_id(source_file_id: str) -> str:
    value = str(source_file_id)
    safe_characters = frozenset(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    )
    if (
        not 1 <= len(value) <= 128
        or value != value.strip()
        or value in {".", ".."}
        or Path(value).name != value
        or any(character not in safe_characters for character in value)
    ):
        raise ParserError(
            "解析器源文件标识不能用于安全的分块路径",
            "PARSER_SOURCE_FILE_ID_INVALID",
        )
    return value


@dataclass(slots=True, frozen=True)
class ParserChunk:
    path: Path
    transport_format: str
    sequence: int
    rows: int
    size_bytes: int
    decoded_bytes: int | None = None


def parser_transport_format(value: str | None = None) -> str:
    selected = (
        value
        if value is not None
        else os.environ.get("RDS_BINLOG_PARSER_TRANSPORT", PARSER_TRANSPORT_ARROW)
    )
    normalized = str(selected).strip().lower()
    if normalized not in {PARSER_TRANSPORT_NDJSON, PARSER_TRANSPORT_ARROW}:
        raise ParserError(
            "解析器传输格式必须是 arrow 或 ndjson",
            "PARSER_TRANSPORT_INVALID",
        )
    return normalized


@dataclass(slots=True, frozen=True)
class NativeChecksumResult:
    size_bytes: int
    sha256: str
    crc64: str


def parser_executable() -> Path:
    override = os.environ.get("RDS_BINLOG_PARSER", "").strip()
    binary_name = "binlog-parser.exe" if os.name == "nt" else "binlog-parser"
    path = Path(override) if override else app_root() / "tools" / binary_name
    if not path.is_file():
        raise ParserError(
            f"未找到 Binlog 解析器：{path}", "PARSER_EXECUTABLE_MISSING"
        )
    return path


def _parser_command(
    path: Path,
    source_file_id: str,
    flavor: str,
    *,
    output_dir: Path | None = None,
    chunk_format: str = PARSER_TRANSPORT_NDJSON,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
) -> list[str]:
    command = [
        str(parser_executable()),
        "--input",
        str(path),
        "--source-file-id",
        source_file_id,
        "--flavor",
        flavor,
    ]
    if output_dir is not None:
        selected_format = parser_transport_format(chunk_format)
        command.extend(["--output-dir", str(output_dir)])
        # Omit the new flag for NDJSON so the explicit rollback path can still
        # operate an older deployed parser; the exact legacy manifest is gated
        # separately below. New parser builds also default to NDJSON.
        if selected_format == PARSER_TRANSPORT_ARROW:
            command.extend(["--chunk-format", selected_format])
        command.extend(
            [
                "--chunk-max-lines",
                str(max_lines),
                "--chunk-max-bytes",
                str(max_bytes),
            ]
        )
    return command


def _hidden_process_options() -> tuple[int, subprocess.STARTUPINFO | None]:
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    startupinfo = None
    if os.name == "nt":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0
    return creation_flags, startupinfo


def _parser_no_progress_seconds(value: float | None = None) -> float:
    if value is not None:
        return max(float(value), 0.05)
    try:
        return max(
            float(os.environ.get("RDS_BINLOG_PARSER_NO_PROGRESS_SECONDS", "300")),
            30.0,
        )
    except ValueError:
        return 300.0


class NativeChecksumStream:
    def __init__(self) -> None:
        creation_flags, startupinfo = _hidden_process_options()
        self.process = subprocess.Popen(
            [str(parser_executable()), "--checksum-stdin"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=creation_flags,
            startupinfo=startupinfo,
        )
        self._finished = False

    def update(self, data: bytes) -> None:
        if self._finished or self.process.stdin is None:
            raise ParserError("原生校验流已经关闭", "CHECKSUM_STREAM_CLOSED")
        try:
            self.process.stdin.write(data)
        except (BrokenPipeError, OSError) as exc:
            self.abort()
            raise ParserError(
                f"原生校验流写入失败：{exc}", "CHECKSUM_STREAM_FAILED"
            ) from exc

    def finish(self) -> NativeChecksumResult:
        if self._finished:
            raise ParserError("原生校验流已经关闭", "CHECKSUM_STREAM_CLOSED")
        self._finished = True
        assert self.process.stdin is not None
        assert self.process.stdout is not None
        assert self.process.stderr is not None
        self.process.stdin.close()
        output = self.process.stdout.read()
        detail = self.process.stderr.read().decode("utf-8", errors="replace").strip()
        return_code = self.process.wait()
        self.process.stdout.close()
        self.process.stderr.close()
        if return_code != 0:
            raise ParserError(
                f"原生校验失败：{detail[-4000:] if detail else f'退出码 {return_code}'}",
                "CHECKSUM_PROCESS_FAILED",
            )
        try:
            value = json.loads(output.decode("utf-8"))
            result = NativeChecksumResult(
                size_bytes=int(value["size_bytes"]),
                sha256=str(value["sha256"]).lower(),
                crc64=str(value["crc64"]),
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise ParserError(
                "原生校验器输出无效", "CHECKSUM_OUTPUT_INVALID"
            ) from exc
        if (
            result.size_bytes < 0
            or len(result.sha256) != 64
            or not result.crc64.isdigit()
        ):
            raise ParserError("原生校验器输出无效", "CHECKSUM_OUTPUT_INVALID")
        return result

    def abort(self) -> None:
        if self._finished:
            return
        self._finished = True
        if self.process.stdin is not None:
            try:
                self.process.stdin.close()
            except OSError:
                pass
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        for stream in (self.process.stdout, self.process.stderr):
            if stream is not None:
                stream.close()


def checksum_file(path: Path) -> NativeChecksumResult:
    stream = NativeChecksumStream()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(4 * 1024 * 1024):
                stream.update(chunk)
        return stream.finish()
    except BaseException:
        stream.abort()
        raise


def parse_to_ndjson(
    path: Path,
    source_file_id: str,
    output_path: Path,
    flavor: str = "mysql",
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_suffix(output_path.suffix + ".part")
    stderr_path = output_path.with_suffix(output_path.suffix + ".stderr")
    for stale_path in (output_path, partial_path, stderr_path):
        if stale_path.exists():
            stale_path.unlink()
    creation_flags, startupinfo = _hidden_process_options()
    process: subprocess.Popen[bytes] | None = None
    try:
        with partial_path.open("wb") as output, stderr_path.open("wb") as error:
            process = subprocess.Popen(
                _parser_command(path, source_file_id, flavor),
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=error,
                creationflags=creation_flags,
                startupinfo=startupinfo,
            )
            return_code = process.wait()
        if return_code != 0:
            detail = ""
            if stderr_path.is_file():
                with stderr_path.open("rb") as error:
                    size = error.seek(0, os.SEEK_END)
                    error.seek(max(0, size - 16_000))
                    detail = error.read().decode("utf-8", errors="replace").strip()
            raise ParserError(
                f"Binlog 解析失败：{detail[-4000:] if detail else f'退出码 {return_code}'}",
                "PARSER_FAILED",
            )
        os.replace(partial_path, output_path)
        return output_path
    except BaseException:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if partial_path.exists():
            partial_path.unlink()
        raise
    finally:
        if stderr_path.exists():
            stderr_path.unlink()


def parse_ndjson_chunks(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    cancel_event: threading.Event | None = None,
    no_progress_seconds: float | None = None,
) -> Iterator[Path]:
    staging_dir.mkdir(parents=True, exist_ok=True)
    stderr_path = staging_dir / f"{source_file_id}.stream.stderr"
    if stderr_path.exists():
        stderr_path.unlink()
    creation_flags, startupinfo = _hidden_process_options()
    process: subprocess.Popen[bytes] | None = None
    output = None
    partial_path: Path | None = None
    try:
        with stderr_path.open("wb") as error:
            process = subprocess.Popen(
                _parser_command(path, source_file_id, flavor),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=error,
                creationflags=creation_flags,
                startupinfo=startupinfo,
            )
            assert process.stdout is not None
            stdout_messages: queue.Queue[tuple[str, bytes | BaseException | None]] = (
                queue.Queue(maxsize=1024)
            )
            stdout_cancel = threading.Event()

            def enqueue_stdout(
                kind: str,
                value: bytes | BaseException | None,
            ) -> None:
                while not stdout_cancel.is_set():
                    try:
                        stdout_messages.put((kind, value), timeout=0.1)
                        return
                    except queue.Full:
                        continue

            def read_stdout() -> None:
                try:
                    for value in iter(process.stdout.readline, b""):
                        if stdout_cancel.is_set():
                            break
                        enqueue_stdout("line", value)
                except BaseException as exc:
                    enqueue_stdout("error", exc)
                finally:
                    enqueue_stdout("done", None)

            stdout_reader = threading.Thread(
                target=read_stdout,
                name=f"binlog-parser-stdout-{source_file_id[:8]}",
                daemon=True,
            )
            stdout_reader.start()
            chunk_index = 0
            line_count = 0
            byte_count = 0
            progress_timeout = _parser_no_progress_seconds(no_progress_seconds)
            last_progress = time.monotonic()

            def open_chunk(index: int):
                nonlocal partial_path
                final = staging_dir / f"{source_file_id}-{index:06d}.ndjson"
                partial_path = final.with_suffix(final.suffix + ".part")
                for stale in (final, partial_path):
                    if stale.exists():
                        stale.unlink()
                return final, partial_path.open("wb")

            final_path, output = open_chunk(chunk_index)
            while True:
                try:
                    kind, value = stdout_messages.get(
                        timeout=min(1.0, progress_timeout)
                    )
                except queue.Empty:
                    if time.monotonic() - last_progress >= progress_timeout:
                        raise ParserError(
                            f"Binlog 解析器连续 {progress_timeout:g} 秒无输出，已终止并保留原文件重试",
                            "PARSER_NO_PROGRESS",
                        )
                    continue
                if kind == "done":
                    break
                if kind == "error":
                    assert isinstance(value, BaseException)
                    raise ParserError(
                        f"读取 Binlog 解析器输出失败：{value}",
                        "PARSER_OUTPUT_READ_FAILED",
                    ) from value
                assert isinstance(value, bytes)
                line = value
                last_progress = time.monotonic()
                if cancel_event is not None and cancel_event.is_set():
                    raise ParserError("Binlog 解析已取消", "PARSER_CANCELLED")
                output.write(line)
                line_count += 1
                byte_count += len(line)
                if line_count < max_lines and byte_count < max_bytes:
                    continue
                output.close()
                output = None
                assert partial_path is not None
                os.replace(partial_path, final_path)
                partial_path = None
                yield final_path
                chunk_index += 1
                line_count = 0
                byte_count = 0
                final_path, output = open_chunk(chunk_index)

            stdout_cancel.set()
            output.close()
            output = None
            assert partial_path is not None
            if line_count:
                os.replace(partial_path, final_path)
                partial_path = None
                yield final_path
            else:
                partial_path.unlink()
                partial_path = None

            return_code = process.wait()
            stdout_reader.join(timeout=5)
        if return_code != 0:
            detail = ""
            if stderr_path.is_file():
                with stderr_path.open("rb") as error:
                    size = error.seek(0, os.SEEK_END)
                    error.seek(max(0, size - 16_000))
                    detail = error.read().decode("utf-8", errors="replace").strip()
            raise ParserError(
                f"Binlog 解析失败：{detail[-4000:] if detail else f'退出码 {return_code}'}",
                "PARSER_FAILED",
            )
    except BaseException:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise
    finally:
        if output is not None:
            output.close()
        if partial_path is not None and partial_path.exists():
            partial_path.unlink()
        if process is not None and process.stdout is not None:
            process.stdout.close()
        for attempt in range(10):
            try:
                stderr_path.unlink(missing_ok=True)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05)


def parse_native_parser_chunks(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    chunk_format: str = PARSER_TRANSPORT_ARROW,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    cancel_event: threading.Event | None = None,
    no_progress_seconds: float | None = None,
) -> Iterator[ParserChunk]:
    """Consume atomic Go-published chunks under the manifest/ACK protocol."""

    selected_format = parser_transport_format(chunk_format)
    source_file_id = _validate_chunk_source_file_id(source_file_id)
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_root = staging_dir.resolve(strict=True)
    stderr_path = staging_root / f"{source_file_id}.native.stderr"
    stderr_path.unlink(missing_ok=True)
    creation_flags, startupinfo = _hidden_process_options()
    process: subprocess.Popen[bytes] | None = None
    stdout_reader: threading.Thread | None = None
    stdout_cancel = threading.Event()
    stdout_messages: queue.Queue[tuple[str, bytes | BaseException | None]] = (
        queue.Queue(maxsize=32)
    )

    def enqueue_stdout(
        kind: str,
        value: bytes | BaseException | None,
    ) -> None:
        while not stdout_cancel.is_set():
            try:
                stdout_messages.put((kind, value), timeout=0.1)
                return
            except queue.Full:
                continue

    try:
        with stderr_path.open("wb") as error:
            process = subprocess.Popen(
                _parser_command(
                    path,
                    source_file_id,
                    flavor,
                    output_dir=staging_root,
                    chunk_format=selected_format,
                    max_lines=max_lines,
                    max_bytes=max_bytes,
                ),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=error,
                creationflags=creation_flags,
                startupinfo=startupinfo,
            )
            assert process.stdin is not None
            assert process.stdout is not None

            def read_stdout() -> None:
                try:
                    while not stdout_cancel.is_set():
                        value = process.stdout.readline(NATIVE_MANIFEST_MAX_BYTES + 1)
                        if value == b"":
                            break
                        if (
                            len(value) > NATIVE_MANIFEST_MAX_BYTES
                            or not value.endswith(b"\n")
                        ):
                            raise ValueError(
                                "parser manifest exceeds its line bound or is not line framed"
                            )
                        enqueue_stdout("manifest", value)
                except BaseException as exc:
                    enqueue_stdout("error", exc)
                finally:
                    enqueue_stdout("done", None)

            stdout_reader = threading.Thread(
                target=read_stdout,
                name=f"binlog-parser-manifest-{source_file_id[:8]}",
                daemon=True,
            )
            stdout_reader.start()
            expected_index = 0
            progress_timeout = _parser_no_progress_seconds(no_progress_seconds)
            last_progress = time.monotonic()

            while True:
                if cancel_event is not None and cancel_event.is_set():
                    raise ParserError("Binlog 解析已取消", "PARSER_CANCELLED")
                try:
                    kind, raw_value = stdout_messages.get(
                        timeout=min(1.0, progress_timeout)
                    )
                except queue.Empty:
                    if time.monotonic() - last_progress >= progress_timeout:
                        raise ParserError(
                            f"Binlog 解析器连续 {progress_timeout:g} 秒无分块，已终止并保留原文件重试",
                            "PARSER_NO_PROGRESS",
                        )
                    continue
                if kind == "done":
                    break
                if kind == "error":
                    assert isinstance(raw_value, BaseException)
                    raise ParserError(
                        f"读取 Binlog 分块清单失败：{raw_value}",
                        "PARSER_OUTPUT_READ_FAILED",
                    ) from raw_value
                assert isinstance(raw_value, bytes)
                last_progress = time.monotonic()
                try:
                    manifest = json.loads(raw_value.decode("utf-8"))
                    if not isinstance(manifest, dict):
                        raise TypeError("manifest must be an object")
                    candidate = Path(str(manifest["path"]))
                    rows = int(manifest["rows"])
                    bytes_written = int(manifest["bytes"])
                    strict_protocol = "protocol" in manifest
                    sequence = int(manifest.get("sequence", expected_index))
                    decoded_bytes = (
                        int(manifest["decoded_bytes"])
                        if "decoded_bytes" in manifest
                        else None
                    )
                except (
                    KeyError,
                    TypeError,
                    ValueError,
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                ) as exc:
                    raise ParserError(
                        "Binlog 分块清单无效", "PARSER_CHUNK_MANIFEST_INVALID"
                    ) from exc
                suffix = (
                    ".arrow"
                    if selected_format == PARSER_TRANSPORT_ARROW
                    else ".ndjson"
                )
                expected_name = f"{source_file_id}-{expected_index:06d}{suffix}"
                expected_manifest_format = (
                    PARSER_ARROW_MANIFEST_FORMAT
                    if selected_format == PARSER_TRANSPORT_ARROW
                    else PARSER_NDJSON_MANIFEST_FORMAT
                )
                if selected_format == PARSER_TRANSPORT_ARROW:
                    expected_keys = {
                        "protocol", "format", "sequence", "path", "rows",
                        "bytes", "decoded_bytes",
                    }
                    protocol_valid = (
                        set(manifest) == expected_keys
                        and manifest.get("protocol") == PARSER_CHUNK_PROTOCOL
                        and manifest.get("format") == expected_manifest_format
                        and isinstance(manifest.get("path"), str)
                        and all(
                            type(manifest.get(key)) is int
                            for key in ("sequence", "rows", "bytes", "decoded_bytes")
                        )
                        and decoded_bytes is not None
                        and 0 < decoded_bytes <= max_bytes
                    )
                elif strict_protocol:
                    expected_keys = {
                        "protocol", "format", "sequence", "path", "rows", "bytes"
                    }
                    protocol_valid = (
                        set(manifest) == expected_keys
                        and manifest.get("protocol") == PARSER_CHUNK_PROTOCOL
                        and manifest.get("format") == expected_manifest_format
                        and isinstance(manifest.get("path"), str)
                        and all(
                            type(manifest.get(key)) is int
                            for key in ("sequence", "rows", "bytes")
                        )
                        and decoded_bytes is None
                    )
                else:
                    # Keep only the exact pre-v1 three-field NDJSON protocol
                    # available for rollback; do not turn it into an extension
                    # point that bypasses the versioned manifest contract.
                    protocol_valid = (
                        set(manifest) == {"path", "rows", "bytes"}
                        and isinstance(manifest.get("path"), str)
                        and all(
                            type(manifest.get(key)) is int
                            for key in ("rows", "bytes")
                        )
                        and decoded_bytes is None
                    )
                if (
                    not protocol_valid
                    or sequence != expected_index
                    or not candidate.is_absolute()
                    or candidate.name != expected_name
                    or rows <= 0
                    or rows > max_lines
                    or bytes_written <= 0
                    or bytes_written > max_bytes
                ):
                    raise ParserError(
                        "Binlog 分块清单越界", "PARSER_CHUNK_MANIFEST_INVALID"
                    )
                try:
                    resolved = candidate.resolve(strict=True)
                    resolved.relative_to(staging_root)
                    actual_size = resolved.stat().st_size
                except (OSError, ValueError) as exc:
                    raise ParserError(
                        "Binlog 分块路径不在暂存目录内",
                        "PARSER_CHUNK_PATH_INVALID",
                    ) from exc
                if (
                    resolved.parent != staging_root
                    or not resolved.is_file()
                    or actual_size != bytes_written
                ):
                    raise ParserError(
                        "Binlog 分块文件与清单不一致",
                        "PARSER_CHUNK_MANIFEST_INVALID",
                    )
                yield ParserChunk(
                    path=resolved,
                    transport_format=selected_format,
                    sequence=sequence,
                    rows=rows,
                    size_bytes=bytes_written,
                    decoded_bytes=decoded_bytes,
                )
                if cancel_event is not None and cancel_event.is_set():
                    raise ParserError("Binlog 解析已取消", "PARSER_CANCELLED")
                try:
                    if selected_format == PARSER_TRANSPORT_ARROW:
                        ack = json.dumps(
                            {
                                "protocol": PARSER_ARROW_ACK_PROTOCOL,
                                "sequence": expected_index,
                                "status": "ok",
                            },
                            separators=(",", ":"),
                        ).encode("utf-8") + b"\n"
                        process.stdin.write(ack)
                    else:
                        # Keep the deployed line ACK byte-for-byte compatible.
                        process.stdin.write(b"ok\n")
                    process.stdin.flush()
                    # Consumer time belongs to the acknowledged chunk, not to
                    # the parser's next-chunk no-progress window.
                    last_progress = time.monotonic()
                except (BrokenPipeError, OSError) as exc:
                    raise ParserError(
                        f"确认 Binlog 分块失败：{exc}", "PARSER_CHUNK_ACK_FAILED"
                    ) from exc
                expected_index += 1

            return_code = process.wait(timeout=30)
            stdout_cancel.set()
            stdout_reader.join(timeout=5)
        if return_code != 0:
            detail = ""
            if stderr_path.is_file():
                with stderr_path.open("rb") as error:
                    size = error.seek(0, os.SEEK_END)
                    error.seek(max(0, size - 16_000))
                    detail = error.read().decode("utf-8", errors="replace").strip()
            raise ParserError(
                f"Binlog 解析失败：{detail[-4000:] if detail else f'退出码 {return_code}'}",
                "PARSER_FAILED",
            )
    except subprocess.TimeoutExpired as exc:
        raise ParserError(
            "Binlog 解析器退出超时，已终止并保留原文件重试",
            "PARSER_EXIT_TIMEOUT",
        ) from exc
    finally:
        stdout_cancel.set()
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process is not None:
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass
        if stdout_reader is not None:
            stdout_reader.join(timeout=5)
        for attempt in range(10):
            try:
                stderr_path.unlink(missing_ok=True)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05)


def parse_native_ndjson_chunks(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    cancel_event: threading.Event | None = None,
    no_progress_seconds: float | None = None,
) -> Iterator[Path]:
    """Compatibility wrapper for the legacy NDJSON chunk transport."""

    for chunk in parse_native_parser_chunks(
        path,
        source_file_id,
        staging_dir,
        flavor,
        chunk_format=PARSER_TRANSPORT_NDJSON,
        max_lines=max_lines,
        max_bytes=max_bytes,
        cancel_event=cancel_event,
        no_progress_seconds=no_progress_seconds,
    ):
        yield chunk.path


def _parse_parser_chunks_buffered(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    chunk_format: str,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    max_prefetch: int = NATIVE_CHUNK_PREFETCH,
    no_progress_seconds: float | None = None,
) -> Iterator[ParserChunk]:
    messages: queue.Queue[tuple[str, object]] = queue.Queue(
        maxsize=max(1, int(max_prefetch))
    )
    cancel = threading.Event()
    outstanding = threading.Semaphore(max(1, int(max_prefetch)) + 1)

    def enqueue(kind: str, value: object) -> bool:
        while not cancel.is_set():
            try:
                messages.put((kind, value), timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    def acquire_outstanding_slot() -> bool:
        while not cancel.is_set():
            if outstanding.acquire(timeout=0.1):
                return True
        return False

    def produce() -> None:
        chunks = parse_native_parser_chunks(
            path,
            source_file_id,
            staging_dir,
            flavor,
            chunk_format=chunk_format,
            max_lines=max_lines,
            max_bytes=max_bytes,
            cancel_event=cancel,
            no_progress_seconds=no_progress_seconds,
        )
        try:
            while acquire_outstanding_slot():
                try:
                    chunk = next(chunks)
                except StopIteration:
                    outstanding.release()
                    break
                except BaseException:
                    outstanding.release()
                    raise
                if not enqueue("chunk", chunk):
                    chunk.path.unlink(missing_ok=True)
                    outstanding.release()
                    break
        except BaseException as exc:
            if not cancel.is_set():
                enqueue("error", exc)
        finally:
            try:
                chunks.close()
            except BaseException as exc:
                if not cancel.is_set():
                    enqueue("error", exc)
            if not cancel.is_set():
                enqueue("done", None)

    producer = threading.Thread(
        target=produce,
        name=f"binlog-parser-prefetch-{source_file_id[:8]}",
        daemon=True,
    )
    producer.start()
    try:
        while True:
            kind, value = messages.get()
            if kind == "chunk":
                assert isinstance(value, ParserChunk)
                try:
                    yield value
                finally:
                    # Releasing this slot means the caller finished the chunk
                    # body. The producer may ACK/admit one successor, while the
                    # total ready + in-flight set remains bounded.
                    outstanding.release()
                continue
            if kind == "error":
                assert isinstance(value, BaseException)
                raise value
            break
    finally:
        cancel.set()
        producer.join(timeout=10)
        while True:
            try:
                kind, value = messages.get_nowait()
            except queue.Empty:
                break
            if kind == "chunk":
                assert isinstance(value, ParserChunk)
                value.path.unlink(missing_ok=True)
                outstanding.release()
        if producer.is_alive():
            raise ParserError(
                "解析预取线程未能安全停止", "PARSER_PREFETCH_STOP_TIMEOUT"
            )


def _parse_ndjson_chunks_buffered(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    max_prefetch: int = NATIVE_CHUNK_PREFETCH,
    no_progress_seconds: float | None = None,
) -> Iterator[Path]:
    """Compatibility wrapper over the single buffered chunk implementation."""

    for chunk in _parse_parser_chunks_buffered(
        path,
        source_file_id,
        staging_dir,
        flavor,
        chunk_format=PARSER_TRANSPORT_NDJSON,
        max_lines=max_lines,
        max_bytes=max_bytes,
        max_prefetch=max_prefetch,
        no_progress_seconds=no_progress_seconds,
    ):
        yield chunk.path


def parse_ndjson_chunks_buffered(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    max_prefetch: int = NATIVE_CHUNK_PREFETCH,
    no_progress_seconds: float | None = None,
) -> Iterator[Path]:
    """Compatibility wrapper preserving the deployed NDJSON call contract."""

    for chunk in parse_parser_chunks_buffered(
        path,
        source_file_id,
        staging_dir,
        flavor,
        chunk_format=PARSER_TRANSPORT_NDJSON,
        max_lines=max_lines,
        max_bytes=max_bytes,
        max_prefetch=max_prefetch,
        no_progress_seconds=no_progress_seconds,
    ):
        yield chunk.path


def parse_parser_chunks_buffered(
    path: Path,
    source_file_id: str,
    staging_dir: Path,
    flavor: str = "mysql",
    *,
    chunk_format: str | None = None,
    max_lines: int = NATIVE_CHUNK_MAX_LINES,
    max_bytes: int = NATIVE_CHUNK_MAX_BYTES,
    max_prefetch: int = NATIVE_CHUNK_PREFETCH,
    no_progress_seconds: float | None = None,
) -> Iterator[ParserChunk]:
    selected_format = parser_transport_format(chunk_format)
    source_file_id = _validate_chunk_source_file_id(source_file_id)
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_root = staging_dir.resolve(strict=True)
    reservation = max_bytes * (max(1, int(max_prefetch)) + 1)
    if max_bytes <= 0 or reservation > NATIVE_STAGING_BUDGET_BYTES:
        raise ParserError("解析分块预取超出暂存预算", "PARSER_STAGING_BUDGET")
    with _NATIVE_STAGING_LOCK:
        active = _NATIVE_STAGING_ACTIVE.setdefault(staging_root, {})
        _NATIVE_STAGING_LOCK.wait_for(
            lambda: source_file_id not in active
            and len(active) < 2
            and sum(active.values()) + reservation <= NATIVE_STAGING_BUDGET_BYTES
        )
        # Clean both formats so a crash followed by a transport rollback cannot
        # retain an unowned chunk. Same-file admission is serialized above.
        _cleanup_native_chunk_artifacts(staging_root, keep_source_ids=set(active))
        active[source_file_id] = reservation
    completed = False
    try:
        yield from _parse_parser_chunks_buffered(
            path,
            source_file_id,
            staging_root,
            flavor,
            chunk_format=selected_format,
            max_lines=max_lines,
            max_bytes=max_bytes,
            max_prefetch=max_prefetch,
            no_progress_seconds=no_progress_seconds,
        )
        completed = True
    finally:
        with _NATIVE_STAGING_LOCK:
            try:
                if not completed:
                    _cleanup_native_chunk_artifacts(staging_root, source_file_id)
            finally:
                del active[source_file_id]
                _NATIVE_STAGING_LOCK.notify_all()


def parse_events(
    path: Path, source_file_id: str, flavor: str = "mysql"
) -> Iterator[dict[str, Any]]:
    creation_flags, startupinfo = _hidden_process_options()
    process = subprocess.Popen(
        _parser_command(path, source_file_id, flavor),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="strict",
        creationflags=creation_flags,
        startupinfo=startupinfo,
    )
    assert process.stdout is not None
    assert process.stderr is not None
    try:
        for line_number, line in enumerate(process.stdout, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ParserError(
                    f"解析器第 {line_number} 行输出不是有效 JSON",
                    "PARSER_INVALID_JSON",
                ) from exc
            if not isinstance(value, dict):
                raise ParserError("解析器输出结构无效", "PARSER_INVALID_RECORD")
            yield value
        stderr = process.stderr.read().strip()
        return_code = process.wait()
        if return_code != 0:
            detail = stderr[-4000:] if stderr else f"退出码 {return_code}"
            raise ParserError(f"Binlog 解析失败：{detail}", "PARSER_FAILED")
    except BaseException:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        raise
    finally:
        process.stdout.close()
        process.stderr.close()
