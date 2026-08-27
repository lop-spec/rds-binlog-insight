from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


class SqliteOnlineBackupToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1]
        self.tool = self.root / "tools" / "sqlite_online_backup.py"

    @staticmethod
    def write_pressure(path: Path, full_avg10: float) -> None:
        path.write_text(
            "some avg10=1.00 avg60=1.00 avg300=1.00 total=1\n"
            f"full avg10={full_avg10:.2f} avg60=1.00 avg300=1.00 total=1\n",
            encoding="utf-8",
        )

    def test_backs_up_multiple_databases_and_verifies_them(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            source = workspace / "source"
            destination = workspace / "backup"
            pressure = workspace / "io.pressure"
            self.write_pressure(pressure, 1.0)
            (source / "index").mkdir(parents=True)
            for relative, value in (
                ("metadata.sqlite3", "metadata"),
                ("index/slowlog.sqlite3", "slowlog"),
            ):
                database = source / relative
                with closing(sqlite3.connect(database)) as connection:
                    connection.execute("PRAGMA journal_mode=WAL")
                    connection.execute("CREATE TABLE sample(value TEXT NOT NULL)")
                    connection.execute("INSERT INTO sample(value) VALUES(?)", (value,))
                    connection.commit()

            result = subprocess.run(
                [
                    sys.executable,
                    str(self.tool),
                    "--source-root",
                    str(source),
                    "--destination-root",
                    str(destination),
                    "--database",
                    "metadata.sqlite3",
                    "--database",
                    "index/slowlog.sqlite3",
                    "--io-pressure-file",
                    str(pressure),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            records = [json.loads(line) for line in result.stdout.splitlines()]
            self.assertEqual(len(records), 2)
            self.assertTrue(all(record["quick_check"] == "ok" for record in records))
            for relative, value in (
                ("metadata.sqlite3", "metadata"),
                ("index/slowlog.sqlite3", "slowlog"),
            ):
                with closing(sqlite3.connect(destination / relative)) as connection:
                    row = connection.execute("SELECT value FROM sample").fetchone()
                self.assertEqual(row, (value,))

    def test_refuses_to_overwrite_an_existing_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            source = workspace / "source"
            destination = workspace / "backup"
            pressure = workspace / "io.pressure"
            self.write_pressure(pressure, 1.0)
            source.mkdir()
            destination.mkdir()
            with closing(sqlite3.connect(source / "metadata.sqlite3")) as connection:
                connection.execute("CREATE TABLE sample(value TEXT)")
                connection.commit()
            existing = destination / "metadata.sqlite3"
            existing.write_bytes(b"preserve-me")

            result = subprocess.run(
                [
                    sys.executable,
                    str(self.tool),
                    "--source-root",
                    str(source),
                    "--destination-root",
                    str(destination),
                    "--database",
                    "metadata.sqlite3",
                    "--io-pressure-file",
                    str(pressure),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("refusing to overwrite", result.stderr)
            self.assertEqual(existing.read_bytes(), b"preserve-me")

    def test_pressure_fuse_aborts_without_publishing_a_partial_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            source = workspace / "source"
            destination = workspace / "backup"
            pressure = workspace / "io.pressure"
            source.mkdir()
            with closing(sqlite3.connect(source / "metadata.sqlite3")) as connection:
                connection.execute("CREATE TABLE sample(value TEXT)")
                connection.execute("INSERT INTO sample VALUES('value')")
                connection.commit()
            self.write_pressure(pressure, 21.0)

            result = subprocess.run(
                [
                    sys.executable,
                    str(self.tool),
                    "--source-root",
                    str(source),
                    "--destination-root",
                    str(destination),
                    "--database",
                    "metadata.sqlite3",
                    "--io-pressure-file",
                    str(pressure),
                    "--io-full-avg10-max",
                    "10",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("I/O pressure", result.stderr)
            self.assertFalse((destination / "metadata.sqlite3").exists())
            self.assertEqual(list(destination.glob("*.partial-*")), [])


if __name__ == "__main__":
    unittest.main()
