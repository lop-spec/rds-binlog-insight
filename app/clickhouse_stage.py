from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .clickhouse_client import ClickHouseClient, ClickHouseConfig
from .clickhouse_manifest import ClickHouseManifest, part_identity
from .clickhouse_oss import (
    ClickHouseOssConfig,
    build_direct_s3_insert_sql,
)
from .config import Settings
from .maintenance_status import read_json_status, write_json_status


LOGGER = logging.getLogger(__name__)


class StagedBatchError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StageTables:
    final_time: str
    final_name: str
    stage_time: str
    stage_name: str

    @classmethod
    def from_configs(
        cls,
        config: ClickHouseConfig,
        oss_config: ClickHouseOssConfig,
    ) -> "StageTables":
        database = config.database
        return cls(
            final_time=f"{database}.{oss_config.query_table}",
            final_name=f"{database}.{oss_config.name_query_table}",
            stage_time=f"{database}.{oss_config.stage_query_table}",
            stage_name=f"{database}.{oss_config.stage_name_query_table}",
        )


def _time_matches(part: dict[str, Any], state: dict[str, Any]) -> bool:
    expected_rows = int(part.get("row_count") or 0)
    if expected_rows == 0:
        return int(state.get("rows") or 0) == 0
    revision = int(part.get("content_revision") or 0)
    return bool(
        int(state.get("rows") or 0) == expected_rows
        and int(state.get("sha_count") or 0) == 1
        and str(state.get("sha256") or "") == str(part.get("sha256") or "")
        and int(state.get("min_revision") or 0) == revision
        and int(state.get("max_revision") or 0) == revision
    )


def _name_matches(part: dict[str, Any], state: dict[str, Any]) -> bool:
    expected_rows = int(part.get("row_count") or 0)
    if expected_rows == 0:
        return int(state.get("name_rows") or 0) == 0
    revision = int(part.get("content_revision") or 0)
    return bool(
        int(state.get("name_rows") or 0) == expected_rows
        and int(state.get("name_sha_count") or 0) == 1
        and str(state.get("name_sha256") or "")
        == str(part.get("sha256") or "")
        and int(state.get("name_min_revision") or 0) == revision
        and int(state.get("name_max_revision") or 0) == revision
    )


def staged_remote_matches(
    part: dict[str, Any], state: dict[str, Any]
) -> bool:
    return _time_matches(part, state) and _name_matches(part, state)


def _time_present(state: dict[str, Any]) -> bool:
    return int(state.get("rows") or 0) > 0


def _name_present(state: dict[str, Any]) -> bool:
    return int(state.get("name_rows") or 0) > 0


def _journal_part(part: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": str(part.get("path") or ""),
        "logical_part_id": part_identity(part),
        "sha256": str(part.get("sha256") or ""),
        "content_revision": int(part.get("content_revision") or 0),
        "row_count": int(part.get("row_count") or 0),
    }


class StagedBatchLoader:
    """Crash-recoverable two-table loader using disposable staging parts."""

    def __init__(
        self,
        *,
        client: ClickHouseClient,
        manifest: ClickHouseManifest,
        settings: Settings,
        config: ClickHouseConfig,
        oss_config: ClickHouseOssConfig,
        journal_path: Path,
    ) -> None:
        if not oss_config.staged_backfill_enabled:
            raise ValueError("ClickHouse OSS staged backfill is not enabled")
        if oss_config.incremental_mv_enabled:
            raise ValueError(
                "Incremental materialized view must stay disabled during "
                "staged history backfill"
            )
        self.client = client
        self.manifest = manifest
        self.settings = settings
        self.config = config
        self.oss_config = oss_config
        self.tables = StageTables.from_configs(config, oss_config)
        self.journal_path = Path(journal_path)

    def _states(
        self,
        parts: list[dict[str, Any]],
        *,
        staged: bool,
    ) -> dict[str, dict[str, Any]]:
        tables = self.tables
        return self.client.paired_part_states_for_tables(
            [part_identity(part) for part in parts],
            time_table=tables.stage_time if staged else tables.final_time,
            name_table=tables.stage_name if staged else tables.final_name,
        )

    @staticmethod
    def _all_match(
        parts: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
    ) -> bool:
        return all(
            staged_remote_matches(part, states.get(part_identity(part), {}))
            for part in parts
        )

    def _summary_matches(
        self,
        parts: list[dict[str, Any]],
        *,
        table: str,
        side: str,
        states: dict[str, dict[str, Any]],
    ) -> bool:
        if side not in {"time", "name"}:
            raise ValueError("side must be time or name")
        selected = [
            part
            for part in parts
            if (
                _time_matches(part, states.get(part_identity(part), {}))
                if side == "time"
                else _name_matches(part, states.get(part_identity(part), {}))
            )
            and int(part.get("row_count") or 0) > 0
        ]
        summary = self.client.table_part_summary(table)
        return bool(
            summary["rows"]
            == sum(int(part.get("row_count") or 0) for part in selected)
            and summary["part_count"] == len(selected)
        )

    def _staging_is_exact(
        self,
        parts: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
    ) -> bool:
        return bool(
            self._all_match(parts, states)
            and self._summary_matches(
                parts,
                table=self.tables.stage_time,
                side="time",
                states=states,
            )
            and self._summary_matches(
                parts,
                table=self.tables.stage_name,
                side="name",
                states=states,
            )
        )

    def _staging_time_is_exact(
        self,
        parts: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
    ) -> bool:
        return bool(
            all(
                _time_matches(part, states.get(part_identity(part), {}))
                and not _name_present(states.get(part_identity(part), {}))
                for part in parts
            )
            and self._summary_matches(
                parts,
                table=self.tables.stage_time,
                side="time",
                states=states,
            )
            and self.client.table_storage_summary(self.tables.stage_name)["rows"]
            == 0
        )

    @staticmethod
    def _single_table_states_are_exact(
        parts: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
    ) -> bool:
        expected = {part_identity(part) for part in parts}
        return bool(
            set(states) == expected
            and all(
                _time_matches(part, states.get(part_identity(part), {}))
                for part in parts
            )
        )

    def _write_journal(
        self,
        state: str,
        parts: list[dict[str, Any]],
        **extra: Any,
    ) -> None:
        value: dict[str, Any] = {
            "schema": 1,
            "state": state,
            "parts": [_journal_part(part) for part in parts],
        }
        value.update(extra)
        write_json_status(self.journal_path, value)

    def _clear_journal(self) -> None:
        self.journal_path.unlink(missing_ok=True)

    def _truncate_staging(self) -> None:
        for table in (self.tables.stage_name, self.tables.stage_time):
            if self.client.table_storage_summary(table)["rows"]:
                self.client.truncate_table(table)

    def _mark_ready(self, parts: list[dict[str, Any]]) -> None:
        for part in parts:
            self.manifest.mark_ready(
                str(part["path"]),
                part_identity(part),
                int(part.get("row_count") or 0),
            )

    def _move_remaining_staging(self) -> None:
        for source, destination in (
            (self.tables.stage_name, self.tables.final_name),
            (self.tables.stage_time, self.tables.final_time),
        ):
            partitions = self.client.active_partitions(source)
            if partitions:
                self.client.move_partitions(
                    source=source,
                    destination=destination,
                    partitions=partitions,
                )

    def recover(self) -> int:
        journal = read_json_status(self.journal_path)
        raw_parts = journal.get("parts") if isinstance(journal, dict) else None
        if not isinstance(raw_parts, list) or not raw_parts:
            return 0
        parts = [dict(part) for part in raw_parts if isinstance(part, dict)]
        if len(parts) != len(raw_parts) or any(
            not part_identity(part) or not str(part.get("path") or "")
            for part in parts
        ):
            raise StagedBatchError("OSS stage journal is invalid")

        final_states = self._states(parts, staged=False)
        if self._all_match(parts, final_states):
            self._truncate_staging()
            self._mark_ready(parts)
            self._clear_journal()
            return len(parts)

        stage_states = self._states(parts, staged=True)
        can_finish = True
        final_has_rows = False
        for part in parts:
            identity = part_identity(part)
            final = final_states.get(identity, {})
            stage = stage_states.get(identity, {})
            final_time = _time_matches(part, final)
            final_name = _name_matches(part, final)
            stage_time = _time_matches(part, stage) and _time_present(stage)
            stage_name = _name_matches(part, stage) and _name_present(stage)
            final_has_rows = final_has_rows or _time_present(final) or _name_present(final)
            if (_time_present(final) and not final_time) or (
                _name_present(final) and not final_name
            ):
                raise StagedBatchError(
                    f"Final OSS stage recovery state is inexact: {identity}"
                )
            if (final_time and stage_time) or (final_name and stage_name):
                raise StagedBatchError(
                    f"Final and staging tables both contain: {identity}"
                )
            can_finish = can_finish and (final_time or stage_time) and (
                final_name or stage_name
            )

        if can_finish:
            if not (
                self._summary_matches(
                    parts,
                    table=self.tables.stage_time,
                    side="time",
                    states=stage_states,
                )
                and self._summary_matches(
                    parts,
                    table=self.tables.stage_name,
                    side="name",
                    states=stage_states,
                )
            ):
                raise StagedBatchError(
                    "OSS recovery staging tables contain unexpected rows"
                )
            self._move_remaining_staging()
            verified = self._states(parts, staged=False)
            if not self._all_match(parts, verified):
                raise StagedBatchError(
                    "OSS staged partition recovery did not reach exact final state"
                )
            self._mark_ready(parts)
            self._clear_journal()
            return len(parts)

        if final_has_rows:
            raise StagedBatchError(
                "OSS stage cannot discard data after a partial final move"
            )
        self._truncate_staging()
        self._clear_journal()
        return 0

    def load(self, parts: list[dict[str, Any]]) -> tuple[int, int]:
        final_states = self._states(parts, staged=False)
        ready_parts: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        for part in parts:
            state = final_states.get(part_identity(part), {})
            if staged_remote_matches(part, state):
                ready_parts.append(part)
                continue
            if _time_present(state) or _name_present(state):
                raise StagedBatchError(
                    "Staged v3 final table contains an inexact source part: "
                    f"{part_identity(part)}"
                )
            if int(part.get("row_count") or 0) == 0:
                ready_parts.append(part)
            else:
                missing.append(part)
        self._mark_ready(ready_parts)
        if not missing:
            return len(ready_parts), 0

        if self.journal_path.exists():
            raise StagedBatchError(
                "OSS stage journal must be recovered before loading a new batch"
            )
        if any(
            self.client.table_storage_summary(table)["rows"]
            for table in (self.tables.stage_time, self.tables.stage_name)
        ):
            raise StagedBatchError(
                "OSS staging tables are non-empty without a recovery journal"
            )

        self._write_journal("prepared", missing)
        try:
            sql = build_direct_s3_insert_sql(
                self.settings,
                database=self.config.database,
                table=self.oss_config.stage_query_table,
                parts=missing,
            )
            self.client.query(
                sql,
                settings={
                    "max_threads": self.oss_config.backfill_threads,
                    "max_insert_threads": (
                        self.oss_config.backfill_insert_threads
                    ),
                    "max_execution_time": 1800,
                    "max_memory_usage": 3_000_000_000,
                    "wait_end_of_query": 1,
                },
                timeout=1800,
            )
            stage_time_states = self.client.all_part_states_for_table(
                self.tables.stage_time
            )
            if not (
                self._single_table_states_are_exact(missing, stage_time_states)
                and self.client.table_storage_summary(self.tables.stage_name)[
                    "rows"
                ]
                == 0
            ):
                raise StagedBatchError(
                    "OSS staged time-table verification failed"
                )
            self._write_journal("time-loaded", missing)

            self.client.copy_table(
                source=self.tables.stage_time,
                destination=self.tables.stage_name,
            )
            stage_name_states = self.client.all_part_states_for_table(
                self.tables.stage_name
            )
            if not (
                self._single_table_states_are_exact(missing, stage_time_states)
                and self._single_table_states_are_exact(
                    missing, stage_name_states
                )
            ):
                raise StagedBatchError(
                    "OSS staged two-table verification failed"
                )
            time_partitions = self.client.active_partitions(
                self.tables.stage_time
            )
            name_partitions = self.client.active_partitions(
                self.tables.stage_name
            )
            if time_partitions != name_partitions or not time_partitions:
                raise StagedBatchError(
                    "OSS staging partition sets are not identical"
                )
            self._write_journal(
                "verified",
                missing,
                partitions=time_partitions,
            )

            self.client.move_partitions(
                source=self.tables.stage_name,
                destination=self.tables.final_name,
                partitions=name_partitions,
            )
            self._write_journal(
                "name-moved",
                missing,
                partitions=time_partitions,
            )
            self.client.move_partitions(
                source=self.tables.stage_time,
                destination=self.tables.final_time,
                partitions=time_partitions,
            )
            verified = self._states(missing, staged=False)
            if not self._all_match(missing, verified):
                raise StagedBatchError(
                    "OSS staged final-table verification failed"
                )
            self._mark_ready(missing)
            self._clear_journal()
            return len(ready_parts) + len(missing), 0
        except Exception as exc:
            LOGGER.warning(
                "ClickHouse staged OSS batch failed; parts=%s error=%s",
                len(missing),
                exc,
            )
            recovered = self.recover()
            if recovered:
                return len(ready_parts) + recovered, 0
            if len(missing) > 1:
                midpoint = len(missing) // 2
                left_ready, left_failed = self.load(missing[:midpoint])
                right_ready, right_failed = self.load(missing[midpoint:])
                return (
                    len(ready_parts) + left_ready + right_ready,
                    left_failed + right_failed,
                )
            part = missing[0]
            self.manifest.mark_failed(
                str(part["path"]), part_identity(part), str(exc)
            )
            LOGGER.exception(
                "ClickHouse staged OSS object failed: %s",
                part_identity(part),
            )
            return len(ready_parts), 1
