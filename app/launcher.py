from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from .config import APP_ID, DEFAULT_PORT, MAX_PORT, data_root
from .server import run_server


def _health(port: int, timeout: float = 0.6) -> bool:
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/healthz", timeout=timeout
        ) as response:
            value = json.loads(response.read().decode("utf-8"))
            return value.get("app") == APP_ID
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError):
        return False


def _find_port() -> int:
    runtime = data_root() / "runtime.json"
    if runtime.is_file():
        try:
            port = int(json.loads(runtime.read_text(encoding="utf-8")).get("port"))
            if _health(port):
                return -port
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
    for port in range(DEFAULT_PORT, MAX_PORT + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            try:
                probe.bind(("127.0.0.1", port))
                return port
            except OSError:
                if _health(port):
                    return -port
    raise RuntimeError("没有可用的本地端口（8769-8799）")


def _browser_path() -> Path | None:
    if os.name == "nt":
        try:
            import winreg

            for executable in ("msedge.exe", "chrome.exe"):
                for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                    try:
                        with winreg.OpenKey(
                            hive,
                            "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\"
                            f"App Paths\\{executable}",
                        ) as key:
                            value, _ = winreg.QueryValueEx(key, None)
                            path = Path(value)
                            if path.is_file():
                                return path
                    except OSError:
                        continue
        except ImportError:
            pass
    candidates = [
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    ]
    local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
    if local_app_data:
        candidates.extend(
            [
                Path(local_app_data)
                / "Microsoft"
                / "Edge"
                / "Application"
                / "msedge.exe",
                Path(local_app_data)
                / "Google"
                / "Chrome"
                / "Application"
                / "chrome.exe",
            ]
        )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _open_window(port: int) -> None:
    url = f"http://127.0.0.1:{port}/"
    browser = _browser_path()
    if not browser:
        raise RuntimeError("未找到 Microsoft Edge 或 Google Chrome，无法创建应用窗口")
    profile = data_root() / "browser-profile"
    profile.mkdir(parents=True, exist_ok=True)
    startupinfo = subprocess.STARTUPINFO() if os.name == "nt" else None
    if startupinfo:
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 4  # SW_SHOWNOACTIVATE
    subprocess.Popen(
        [
            str(browser),
            f"--app={url}",
            f"--user-data-dir={profile}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-background-mode",
            "--disable-background-networking",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        startupinfo=startupinfo,
    )


def main() -> None:
    selected = _find_port()
    if selected < 0:
        _open_window(-selected)
        return
    port = selected

    def opener() -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if _health(port):
                _open_window(port)
                return
            time.sleep(0.1)

    import threading

    threading.Thread(target=opener, name="open-ui", daemon=True).start()
    run_server(port)


if __name__ == "__main__":
    main()
