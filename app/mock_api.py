from __future__ import annotations

import json
from pathlib import Path

from .rds_api import RemoteBinlog


class ManifestRdsClient:
    """Offline test adapter. It is enabled only by explicit test environment flags."""

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path

    def _load(self) -> dict:
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def verify_instance(self) -> dict[str, str]:
        return dict(self._load()["identity"])

    def list_binlogs(self, start_utc: str, end_utc: str) -> list[RemoteBinlog]:
        del start_utc, end_utc
        return [RemoteBinlog(**item) for item in self._load().get("binlogs", [])]

