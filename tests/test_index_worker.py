from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from app.index_worker import (
    _missing_index_parts,
    _missing_structural_parts,
    run_one,
)


class _FakeIndex:
    @staticmethod
    def missing_parts(
        parts: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        return [part for part in parts if part.get("missing")][:limit]

    @staticmethod
    def missing_structural_parts(
        parts: list[dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        return [part for part in parts if part.get("structure_missing")][:limit]


class _FakeStorage:
    def __init__(self, local: list[dict[str, Any]]) -> None:
        self._local = local
        self.search_index = _FakeIndex()

    def local_body_parts(self) -> list[dict[str, Any]]:
        return list(self._local)


class _FakeMetadata:
    def __init__(self, parts: list[dict[str, Any]]) -> None:
        self.parts = parts
        self.calls: list[tuple[int, bool, int]] = []

    def list_parts(
        self,
        limit: int,
        *,
        visible_only: bool,
        offset: int,
    ) -> list[dict[str, Any]]:
        self.calls.append((limit, visible_only, offset))
        return self.parts[offset : offset + limit]


class IndexWorkerCandidateTests(unittest.TestCase):
    def test_missing_candidates_prioritize_local_and_page_metadata(self) -> None:
        local = {"path": "local", "oss_key": "oss/local", "missing": True}
        metadata = _FakeMetadata(
            [
                local,
                {"path": "unarchived", "oss_key": "", "missing": True},
                {"path": "indexed", "oss_key": "oss/indexed", "missing": False},
                {"path": "remote-a", "oss_key": "oss/a", "missing": True},
                {"path": "remote-b", "oss_key": "oss/b", "missing": True},
            ]
        )

        result = _missing_index_parts(
            metadata,
            _FakeStorage([local]),
            limit=3,
            page_size=2,
        )

        self.assertEqual([part["path"] for part in result], ["local", "remote-a", "remote-b"])
        self.assertEqual(
            metadata.calls,
            [(2, False, 0), (2, False, 2), (2, False, 4)],
        )

    def test_structural_candidates_use_the_same_local_first_paging(self) -> None:
        local = {
            "path": "local",
            "oss_key": "oss/local",
            "structure_missing": True,
        }
        metadata = _FakeMetadata(
            [
                local,
                {"path": "covered", "oss_key": "oss/covered"},
                {
                    "path": "remote",
                    "oss_key": "oss/remote",
                    "structure_missing": True,
                },
            ]
        )

        result = _missing_structural_parts(
            metadata,
            _FakeStorage([local]),
            limit=2,
            page_size=2,
        )

        self.assertEqual([part["path"] for part in result], ["local", "remote"])

    def test_catalog_and_full_index_both_progress_in_one_cycle(self) -> None:
        catalog_part = {
            "path": "/data/events/catalog.parquet",
            "sha256": "catalog-sha",
            "oss_key": "oss/catalog.parquet",
        }
        full_part = {
            "path": "/data/events/full.parquet",
            "sha256": "full-sha",
            "oss_key": "oss/full.parquet",
        }

        class FakeMetadata:
            def load_settings(self):
                return SimpleNamespace(
                    oss_enabled=True,
                    credential_target="credential",
                )

            @staticmethod
            def missing_part_catalogs(limit: int):
                return [catalog_part][:limit]

            @staticmethod
            def part_catalog_stats():
                return {"total_parts": 2, "cataloged_parts": 1}

            @staticmethod
            def catalog_store_stats():
                return {}

            @staticmethod
            def reconcile_part_catalog_pending(limit: int):
                return 0

            @staticmethod
            def backfill_catalog_store(limit: int):
                return {"parts": 0}

        class FakeSearchIndex:
            @staticmethod
            def stats():
                return {"part_count": 1}

        class FakeStorage:
            def __init__(self) -> None:
                self.search_index = FakeSearchIndex()
                self.cataloged: list[str] = []
                self.indexed: list[str] = []

            @staticmethod
            def local_body_stats():
                return {"parts": 0, "bytes": 0}

            def ensure_part_catalog(self, part, archive):
                self.cataloged.append(str(part["path"]))
                return {"cataloged": 1, "rows": 1}

            def ensure_part_index(self, part, archive):
                self.indexed.append(str(part["path"]))
                return {"indexed": 1, "row_groups": 1, "rows": 1}

            @staticmethod
            def release_archived_body(part):
                return 0

        metadata = FakeMetadata()
        storage = FakeStorage()
        with (
            patch(
                "app.index_worker.ensure_data_dirs",
                return_value={"index": Path("index")},
            ),
            patch("app.index_worker.write_json_status"),
            patch("app.index_worker.MetadataStore", return_value=metadata),
            patch("app.index_worker.EventStorage", return_value=storage),
            patch("app.index_worker.load_credential", return_value=object()),
            patch("app.index_worker.OssArchive", return_value=object()),
            patch("app.index_worker._advance_slowlog_index", return_value={}),
            patch("app.index_worker._advance_rollup", return_value=0),
            patch("app.index_worker._missing_structural_parts", return_value=[]),
            patch(
                "app.index_worker._missing_index_parts",
                return_value=[full_part],
            ),
        ):
            self.assertEqual(run_one(Path("data"), "test"), 0)

        self.assertEqual(storage.cataloged, [catalog_part["path"]])
        self.assertEqual(storage.indexed, [full_part["path"]])


if __name__ == "__main__":
    unittest.main()
