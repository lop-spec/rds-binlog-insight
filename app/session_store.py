"""History of abnormal sessions, kept so that "who was holding the lock at 11:30" can still be answered at noon.

DAS shows only the sessions of this moment. The retention loop (``session_collector``) reads every node's session list once
a minute and hands each snapshot here: one summary row per snapshot, and one row per abnormal session (or lock-holder
candidate) that is merged across snapshots, so a session that stays abnormal for an hour is one row with ``first_us`` and
``last_us``, not sixty. Only abnormal sessions are stored; ordinary ones are counted in the summary and forgotten.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from typing import Any

from .session_view import KIND_LABELS

LOGGER = logging.getLogger(__name__)

SQL_LIMIT = 2000
DEFAULT_GAP_US = 5 * 60 * 1_000_000   # a gap longer than this between two sightings makes the same session id a new row
MAX_HISTORY_ROWS = 500

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
  instance TEXT NOT NULL, node TEXT NOT NULL, ts_us INTEGER NOT NULL,
  total INTEGER NOT NULL, running INTEGER NOT NULL, active INTEGER NOT NULL, abnormal INTEGER NOT NULL,
  max_running_s INTEGER NOT NULL, waiters INTEGER NOT NULL,
  PRIMARY KEY (instance, node, ts_us)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS abnormal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance TEXT NOT NULL, node TEXT NOT NULL, session_id INTEGER NOT NULL,
  first_us INTEGER NOT NULL, last_us INTEGER NOT NULL, seen INTEGER NOT NULL,
  user TEXT NOT NULL, client TEXT NOT NULL, db TEXT NOT NULL, command TEXT NOT NULL, state TEXT NOT NULL,
  max_seconds INTEGER NOT NULL, max_trx_seconds INTEGER NOT NULL, kinds TEXT NOT NULL,
  holder INTEGER NOT NULL, holder_basis TEXT NOT NULL, sql TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS abnormal_span ON abnormal (instance, node, last_us);
CREATE INDEX IF NOT EXISTS abnormal_session ON abnormal (instance, node, session_id, last_us);
"""


class SessionStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        return conn

    def record(self, instance: str, node: str, view: dict[str, Any], *, gap_us: int = DEFAULT_GAP_US) -> int:
        """Store one snapshot's summary and its abnormal sessions; the number of abnormal sessions written."""
        ts_us = int(view["observed_at_us"])
        overview, holders = view["overview"], view.get("holders") or {}
        flagged = [row for row in view["sessions"] if (row["kinds"] or row.get("holder")) and isinstance(row.get("session_id"), int)]
        with self._lock, closing(self._connect()) as conn, conn:
            conn.execute("INSERT OR REPLACE INTO snapshot VALUES (?,?,?,?,?,?,?,?,?)",
                         (instance, node, ts_us, overview["total"], overview["running"], overview["active"], overview["abnormal"],
                          overview["max_active_seconds"], holders.get("waiters", 0)))
            for row in flagged:
                self._merge(conn, instance, node, ts_us, row, gap_us)
        return len(flagged)

    @staticmethod
    def _merge(conn: sqlite3.Connection, instance: str, node: str, ts_us: int, row: dict[str, Any], gap_us: int) -> None:
        kinds = list(row["kinds"])
        sql = str(row.get("sql") or "")[:SQL_LIMIT]
        previous = conn.execute(
            "SELECT * FROM abnormal WHERE instance=? AND node=? AND session_id=? AND user=? AND client=? AND last_us>=? "
            "ORDER BY last_us DESC LIMIT 1", (instance, node, row["session_id"], row["user"], row["client"], ts_us - gap_us)).fetchone()
        if previous is None:
            conn.execute(
                "INSERT INTO abnormal (instance,node,session_id,first_us,last_us,seen,user,client,db,command,state,max_seconds,"
                "max_trx_seconds,kinds,holder,holder_basis,sql) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (instance, node, row["session_id"], ts_us, ts_us, 1, row["user"], row["client"], row["database"], row["command"],
                 row["state"], row["seconds"], row["trx_seconds"], json.dumps(kinds), int(bool(row.get("holder"))),
                 str(row.get("holder_basis") or ""), sql))
            return
        known = set(json.loads(previous["kinds"])) | set(kinds)
        merged = [kind for kind in KIND_LABELS if kind in known]   # the page's order, not alphabetical
        conn.execute(
            "UPDATE abnormal SET last_us=MAX(last_us,?), seen=seen+1, db=?, command=?, state=?, max_seconds=MAX(max_seconds,?), "
            "max_trx_seconds=MAX(max_trx_seconds,?), kinds=?, holder=MAX(holder,?), holder_basis=?, sql=? WHERE id=?",
            (ts_us, row["database"] or previous["db"], row["command"], row["state"], row["seconds"], row["trx_seconds"],
             json.dumps(merged), int(bool(row.get("holder"))), str(row.get("holder_basis") or previous["holder_basis"]),
             sql or previous["sql"], previous["id"]))

    def history(self, instance: str, node: str, start_us: int, end_us: int, limit: int = MAX_HISTORY_ROWS) -> dict[str, Any]:
        """Abnormal sessions seen between two instants (one node, or every node with ``node=""``), lock-holder candidates first and then newest first, and how
        many snapshots were taken in that window per node (so a gap in collection is visible, not read as "all quiet")."""
        limit = max(1, min(int(limit), MAX_HISTORY_ROWS))
        where, args = "instance=? AND first_us<=? AND last_us>=?", [instance, end_us, start_us]
        if node:
            where, args = where + " AND node=?", args + [node]
        with closing(self._connect()) as conn:
            rows = conn.execute(f"SELECT * FROM abnormal WHERE {where} ORDER BY holder DESC, first_us DESC, id DESC LIMIT ?", args + [limit + 1]).fetchall()
            cover_where, cover_args = "instance=? AND ts_us BETWEEN ? AND ?", [instance, start_us, end_us]
            if node:
                cover_where, cover_args = cover_where + " AND node=?", cover_args + [node]
            coverage = conn.execute(
                f"SELECT node, COUNT(*) AS snapshots, MIN(ts_us) AS first_us, MAX(ts_us) AS last_us, MAX(abnormal) AS most_abnormal "
                f"FROM snapshot WHERE {cover_where} GROUP BY node ORDER BY node", cover_args).fetchall()
        shown = rows[:limit]
        return {
            "sessions": [{
                "node_id": row["node"], "session_id": row["session_id"], "first_us": row["first_us"], "last_us": row["last_us"],
                "seen": row["seen"], "user": row["user"], "client": row["client"], "database": row["db"], "command": row["command"],
                "state": row["state"], "max_seconds": row["max_seconds"], "max_trx_seconds": row["max_trx_seconds"],
                "kinds": json.loads(row["kinds"]), "holder": bool(row["holder"]), "holder_basis": row["holder_basis"], "sql": row["sql"],
            } for row in shown],
            "truncated": len(rows) > limit,
            "coverage": [{"node_id": row["node"], "snapshots": row["snapshots"], "first_us": row["first_us"], "last_us": row["last_us"],
                          "most_abnormal": row["most_abnormal"]} for row in coverage],
        }

    def purge(self, before_us: int) -> int:
        """Forget everything last seen before ``before_us``; the number of rows removed."""
        with self._lock, closing(self._connect()) as conn, conn:
            removed = conn.execute("DELETE FROM abnormal WHERE last_us<?", (before_us,)).rowcount
            removed += conn.execute("DELETE FROM snapshot WHERE ts_us<?", (before_us,)).rowcount
        return removed

    def stats(self) -> dict[str, Any]:
        with closing(self._connect()) as conn:
            snapshots = conn.execute("SELECT COUNT(*) AS n, MIN(ts_us) AS oldest, MAX(ts_us) AS newest FROM snapshot").fetchone()
            abnormal = conn.execute("SELECT COUNT(*) AS n FROM abnormal").fetchone()
        try:
            size = self.path.stat().st_size
        except OSError:
            size = 0
        return {"snapshots": snapshots["n"], "abnormal_sessions": abnormal["n"], "oldest_us": snapshots["oldest"],
                "newest_us": snapshots["newest"], "bytes": size}
