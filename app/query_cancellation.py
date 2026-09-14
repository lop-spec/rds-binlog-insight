"""Interrupt SQLite planning as well as the subsequent object scan."""
from contextlib import contextmanager
import sqlite3


@contextmanager
def cancellable_sqlite(conn, control=None):
    if control is None:
        yield conn
        return
    control.check_cancelled()

    def progress():
        try:
            control.check_cancelled()
            return 0
        except Exception:
            # sqlite callbacks cannot propagate the original cancellation.
            return 1

    conn.set_progress_handler(progress, 1000)
    try:
        yield conn
    except sqlite3.OperationalError:
        control.check_cancelled()
        raise
    finally:
        conn.set_progress_handler(None, 0)
    control.check_cancelled()
