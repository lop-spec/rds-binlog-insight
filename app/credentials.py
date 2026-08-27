from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import tempfile
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path

CRED_TYPE_GENERIC = 1
CRED_PERSIST_LOCAL_MACHINE = 2
FILE_SCHEMA_VERSION = 1
MAX_CREDENTIAL_FILE_BYTES = 16 * 1024


@dataclass(slots=True)
class CloudCredential:
    access_key_id: str
    access_key_secret: str
    security_token: str = ""

    def validate(self) -> None:
        if not self.access_key_id or not self.access_key_secret:
            raise ValueError("AccessKey ID 和 AccessKey Secret 均不能为空")
        if len(self.access_key_id) > 256 or len(self.access_key_secret) > 1024:
            raise ValueError("凭据字段长度异常")


class _CREDENTIALW(ctypes.Structure):
    _fields_ = [
        ("Flags", wintypes.DWORD),
        ("Type", wintypes.DWORD),
        ("TargetName", wintypes.LPWSTR),
        ("Comment", wintypes.LPWSTR),
        ("LastWritten", wintypes.FILETIME),
        ("CredentialBlobSize", wintypes.DWORD),
        ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
        ("Persist", wintypes.DWORD),
        ("AttributeCount", wintypes.DWORD),
        ("Attributes", wintypes.LPVOID),
        ("TargetAlias", wintypes.LPWSTR),
        ("UserName", wintypes.LPWSTR),
    ]


def _uses_windows_credential_manager() -> bool:
    return os.name == "nt"


def _credential_payload(credential: CloudCredential) -> dict[str, str | int]:
    return {
        "schema_version": FILE_SCHEMA_VERSION,
        "access_key_id": credential.access_key_id,
        "access_key_secret": credential.access_key_secret,
        "security_token": credential.security_token,
    }


def _credential_from_payload(value: object) -> CloudCredential:
    if not isinstance(value, dict):
        raise RuntimeError("凭据文件格式无效")
    if int(value.get("schema_version") or 0) != FILE_SCHEMA_VERSION:
        raise RuntimeError("凭据文件版本不受支持")
    credential = CloudCredential(
        str(value.get("access_key_id") or ""),
        str(value.get("access_key_secret") or ""),
        str(value.get("security_token") or ""),
    )
    credential.validate()
    return credential


def _credential_directory() -> Path:
    override = os.environ.get("RDS_BINLOG_CREDENTIAL_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    from .config import data_root

    return data_root() / ".credentials"


def _credential_path(target: str) -> Path:
    digest = hashlib.sha256(target.encode("utf-8")).hexdigest()
    return _credential_directory() / f"{digest}.json"


def _save_file_credential(target: str, credential: CloudCredential) -> None:
    directory = _credential_directory()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    path = _credential_path(target)
    fd, temporary_name = tempfile.mkstemp(
        dir=directory,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, 0o600)
        handle = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        fd = -1
        with handle:
            json.dump(
                _credential_payload(credential),
                handle,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)


def _load_file_credential(target: str) -> CloudCredential | None:
    path = _credential_path(target)
    try:
        details = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise RuntimeError("凭据路径必须是普通文件")
    if os.name != "nt" and stat.S_IMODE(details.st_mode) & 0o077:
        raise RuntimeError("Linux 凭据文件权限必须为 0600")
    if details.st_size > MAX_CREDENTIAL_FILE_BYTES:
        raise RuntimeError("凭据文件大小异常")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"无法读取凭据文件: {exc}") from exc
    return _credential_from_payload(value)


def _delete_file_credential(target: str) -> bool:
    try:
        _credential_path(target).unlink()
        return True
    except FileNotFoundError:
        return False


def _environment_credential() -> CloudCredential | None:
    env_id = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "").strip()
    env_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "").strip()
    if not env_id or not env_secret:
        return None
    return CloudCredential(
        env_id,
        env_secret,
        os.environ.get("ALIBABA_CLOUD_SECURITY_TOKEN", "").strip(),
    )


def _advapi32():
    if not _uses_windows_credential_manager():
        raise RuntimeError("Windows Credential Manager 仅在 Windows 可用")
    dll = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
    dll.CredWriteW.argtypes = [ctypes.POINTER(_CREDENTIALW), wintypes.DWORD]
    dll.CredWriteW.restype = wintypes.BOOL
    dll.CredReadW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(_CREDENTIALW)),
    ]
    dll.CredReadW.restype = wintypes.BOOL
    dll.CredFree.argtypes = [wintypes.LPVOID]
    dll.CredFree.restype = None
    dll.CredDeleteW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
    dll.CredDeleteW.restype = wintypes.BOOL
    return dll


def save_credential(target: str, credential: CloudCredential) -> None:
    credential.validate()
    if not _uses_windows_credential_manager():
        _save_file_credential(target, credential)
        return
    payload = json.dumps(
        _credential_payload(credential),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-16-le")
    if len(payload) > 2560:
        raise ValueError("凭据超出 Windows Credential Manager 大小限制")
    blob = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
    item = _CREDENTIALW()
    item.Type = CRED_TYPE_GENERIC
    item.TargetName = target
    item.CredentialBlobSize = len(payload)
    item.CredentialBlob = ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte))
    item.Persist = CRED_PERSIST_LOCAL_MACHINE
    item.UserName = credential.access_key_id
    dll = _advapi32()
    if not dll.CredWriteW(ctypes.byref(item), 0):
        raise ctypes.WinError(ctypes.get_last_error())


def load_credential(target: str) -> CloudCredential | None:
    environment = _environment_credential()
    if environment:
        return environment
    if not _uses_windows_credential_manager():
        return _load_file_credential(target)
    pointer = ctypes.POINTER(_CREDENTIALW)()
    dll = _advapi32()
    if not dll.CredReadW(target, CRED_TYPE_GENERIC, 0, ctypes.byref(pointer)):
        error = ctypes.get_last_error()
        if error == 1168:
            return None
        raise ctypes.WinError(error)
    try:
        item = pointer.contents
        raw = ctypes.string_at(item.CredentialBlob, item.CredentialBlobSize)
        value = json.loads(raw.decode("utf-16-le"))
        return CloudCredential(
            str(value.get("access_key_id", "")),
            str(value.get("access_key_secret", "")),
            str(value.get("security_token", "")),
        )
    finally:
        dll.CredFree(pointer)


def delete_credential(target: str) -> bool:
    if not _uses_windows_credential_manager():
        return _delete_file_credential(target)
    dll = _advapi32()
    if dll.CredDeleteW(target, CRED_TYPE_GENERIC, 0):
        return True
    error = ctypes.get_last_error()
    if error == 1168:
        return False
    raise ctypes.WinError(error)


def credential_status(target: str) -> dict[str, str | bool]:
    credential = load_credential(target)
    if not credential:
        return {"present": False, "source": "none", "maskedAccessKeyId": ""}
    source = (
        "environment"
        if _environment_credential()
        else (
            "windows-credential-manager"
            if _uses_windows_credential_manager()
            else "protected-file"
        )
    )
    key = credential.access_key_id
    masked = key[:4] + "••••" + key[-4:] if len(key) > 8 else "••••"
    return {"present": True, "source": source, "maskedAccessKeyId": masked}
