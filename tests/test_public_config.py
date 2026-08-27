from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.config import Settings
from app.exact_index import _load_schema_registry
from app.tabularis_audit import AuditIngestError, TabularisAuditIngest
from tools.backfill_audit_log import _instance_aliases


def _audit_event(instance_id: str) -> dict[str, object]:
    return {
        "event_id": "event-1",
        "instance_id": instance_id,
        "execution_status": "success",
        "started_epoch_us": 1,
        "finished_epoch_us": 2,
        "operation": "SELECT",
    }


class RuntimeIdentifierConfigTests(unittest.TestCase):
    def _ingest(self, *, instance_id: str, allowed_instances=None) -> TabularisAuditIngest:
        metadata = SimpleNamespace(
            load_settings=lambda: Settings(db_instance_id=instance_id)
        )
        return TabularisAuditIngest(
            metadata,
            SimpleNamespace(),
            archiver=SimpleNamespace(),
            token="test-token",
            allowed_instances=allowed_instances,
        )

    def test_audit_allowlist_falls_back_to_runtime_instances(self) -> None:
        with patch.dict(os.environ, {"RDS_BINLOG_GLOG_INSTANCE_ID": "rm-example-secondary"}, clear=False):
            ingest = self._ingest(instance_id="rm-example-primary")
        self.assertEqual(
            ingest.allowed_instances,
            {"rm-example-primary", "rm-example-secondary"},
        )
        self.assertEqual(
            ingest._validate_event(_audit_event("rm-example-primary"))["instance_id"],
            "rm-example-primary",
        )
        with self.assertRaises(AuditIngestError):
            ingest._validate_event(_audit_event("rm-example-other"))

    def test_explicit_audit_allowlist_does_not_use_embedded_defaults(self) -> None:
        ingest = self._ingest(
            instance_id="rm-example-primary",
            allowed_instances="rm-example-a,rm-example-b",
        )
        self.assertEqual(ingest.allowed_instances, {"rm-example-a", "rm-example-b"})

    def test_exact_registry_uses_external_runtime_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_text(
                json.dumps({"format_version": 1, "mappings": []}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"RDS_BINLOG_EXACT_SCHEMA_REGISTRY": str(path)},
                clear=False,
            ):
                mappings, digest = _load_schema_registry(None)
            self.assertEqual(mappings, {})
            self.assertEqual(len(digest), 64)

    def test_missing_external_registry_fails_closed(self) -> None:
        with patch.dict(
            os.environ,
            {"RDS_BINLOG_EXACT_SCHEMA_REGISTRY": "missing-registry.json"},
            clear=False,
        ):
            with self.assertRaises(FileNotFoundError):
                _load_schema_registry(None)

    def test_audit_aliases_are_runtime_json(self) -> None:
        with patch.dict(
            os.environ,
            {
                "RDS_BINLOG_AUDIT_INSTANCE_ALIASES": (
                    '{"primary-db":"rm-example-primary"}'
                )
            },
            clear=False,
        ):
            self.assertEqual(
                _instance_aliases(),
                {"primary-db": "rm-example-primary"},
            )


if __name__ == "__main__":
    unittest.main()
