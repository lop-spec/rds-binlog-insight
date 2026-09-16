"""Owned query cleanup; native timeout regression runs only in isolated cloud CI."""
from __future__ import annotations

import os
import threading
import unittest
from unittest.mock import patch

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig, ClickHouseError
from app.clickhouse_query import query_rows_with_cancel


class Cancelled(RuntimeError):
    pass


class Control:
    def __init__(self):
        self.cancelled = threading.Event()

    def check_cancelled(self):
        if self.cancelled.is_set():
            raise Cancelled("owned task cancelled")


class Client:
    def __init__(self, error=None):
        self.error = error
        self.calls = []
        self.kills = []
        self.kill_error = None
        self.killed = threading.Event()
        self.on_read = None

    def json_rows(self, sql, **kwargs):
        self.calls.append((sql, kwargs))
        if self.on_read:
            self.on_read()
        if self.error:
            raise self.error
        return [{"value": 7}]

    def query(self, sql, **kwargs):
        self.kills.append((sql, kwargs))
        self.killed.set()
        if self.kill_error:
            raise self.kill_error
        return ""


class ClickHouseQueryLifecycle(unittest.TestCase):
    def assert_owned_cleanup(self, client):
        self.assertEqual(len(client.kills), 1)
        sql, kwargs = client.kills[0]
        self.assertEqual(sql, "KILL QUERY WHERE query_id = {query_id:String} SYNC")
        self.assertEqual(kwargs["parameters"], {
            "query_id": client.calls[0][1]["settings"]["query_id"],
        })
        self.assertEqual(kwargs["timeout"], 10)

    def test_transport_failure_cancels_only_the_owned_query_and_preserves_error(self):
        error = ClickHouseError("transport timed out")
        client = Client(error)
        with self.assertRaises(ClickHouseError) as raised:
            query_rows_with_cancel(client, "SELECT 1", {}, Control())
        self.assertIs(raised.exception, error)
        self.assert_owned_cleanup(client)

    def test_success_enables_disconnect_cancellation_without_mutating_settings(self):
        client = Client()
        settings = {"max_threads": 1}
        rows = query_rows_with_cancel(client, "SELECT 7", {}, Control(), settings=settings)
        self.assertEqual(rows, [{"value": 7}])
        self.assertEqual(client.kills, [])
        self.assertEqual(settings, {"max_threads": 1})
        self.assertEqual(client.calls[0][1]["settings"]["cancel_http_readonly_queries_on_client_close"], 1)

    def test_failed_cleanup_is_logged_and_cannot_mask_original_error(self):
        original = ClickHouseError("read failed")
        client = Client(original)
        client.kill_error = OSError("endpoint unavailable")
        with self.assertLogs("app.clickhouse_query", level="ERROR") as logs:
            with self.assertRaises(ClickHouseError) as raised:
                query_rows_with_cancel(client, "SELECT 1", {}, Control())
        self.assertIs(raised.exception, original)
        self.assert_owned_cleanup(client)
        self.assertIn("Failed to cancel ClickHouse query", " ".join(logs.output))

    def test_watcher_and_error_path_share_one_completed_cleanup(self):
        control = Control()
        client = Client(ClickHouseError("server cancelled"))
        def cancel_during_read():
            control.cancelled.set()
            self.assertTrue(client.killed.wait(2), "watcher did not cancel")
        client.on_read = cancel_during_read
        with self.assertRaises(ClickHouseError):
            query_rows_with_cancel(client, "SELECT 1", {}, control)
        self.assert_owned_cleanup(client)

    def test_error_waits_for_watcher_cleanup_acknowledgement(self):
        control = Control()
        original = ClickHouseError("transport failed during cancellation")
        client = Client(original)
        allow_ack = threading.Event()
        read_failed = threading.Event()
        finished = threading.Event()
        failures = []
        normal_kill = client.query
        def delayed_kill(*args, **kwargs):
            normal_kill(*args, **kwargs)
            if not allow_ack.wait(5):
                raise TimeoutError("test cleanup was not released")
        client.query = delayed_kill
        def cancel_during_read():
            control.cancelled.set()
            if not client.killed.wait(2):
                raise AssertionError("watcher did not start cleanup")
            read_failed.set()
        client.on_read = cancel_during_read
        def run():
            try:
                query_rows_with_cancel(client, "SELECT 1", {}, control)
            except BaseException as exc:
                failures.append(exc)
            finally:
                finished.set()
        worker = threading.Thread(target=run)
        worker.start()
        try:
            self.assertTrue(read_failed.wait(2))
            self.assertFalse(finished.wait(1.1), "returned before native cleanup acknowledged")
        finally:
            allow_ack.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [original])
        self.assert_owned_cleanup(client)

    def test_pre_cancelled_task_does_not_submit_or_kill_any_query(self):
        client = Client()
        control = Control()
        control.cancelled.set()
        with self.assertRaises(Cancelled):
            query_rows_with_cancel(client, "SELECT 1", {}, control)
        self.assertEqual(client.calls, [])
        self.assertEqual(client.kills, [])

    def test_cancellation_after_read_uses_owned_cleanup(self):
        client = Client()
        control = Control()
        client.on_read = control.cancelled.set
        with self.assertRaises(Cancelled):
            query_rows_with_cancel(client, "SELECT 1", {}, control)
        self.assert_owned_cleanup(client)

    def test_unmanaged_call_keeps_existing_contract(self):
        client = Client()
        self.assertEqual(query_rows_with_cancel(client, "SELECT 7", {}, None), [{"value": 7}])
        self.assertEqual(client.calls[0][1]["settings"], None)
        self.assertEqual(client.kills, [])


@unittest.skipUnless(os.environ.get("RAW_CANDIDATE_CI_FIXTURE") == "1",
                     "isolated CI ClickHouse fixture only")
class ClickHouseQueryLifecycleIntegration(unittest.TestCase):
    def test_http_timeout_returns_only_after_owned_native_query_is_gone(self):
        config = ClickHouseConfig.from_env()
        self.assertEqual((config.host, config.port, config.database),
                         ("127.0.0.1", 18123, "mongo_ci_fixture"))
        client = ClickHouseClient(config)
        query_id = ""
        original = client.json_rows
        def capture(sql, **kwargs):
            nonlocal query_id
            query_id = kwargs["settings"]["query_id"]
            # Exercise explicit cleanup even if disconnect cancellation is off.
            return original(sql, **{**kwargs, "settings": {
                **kwargs["settings"], "cancel_http_readonly_queries_on_client_close": 0,
            }})
        try:
            with patch.object(client, "json_rows", side_effect=capture):
                with self.assertRaisesRegex(ClickHouseError, "timed out"):
                    query_rows_with_cancel(client, "SELECT sleep(3)", {}, Control(), timeout=1)
            self.assertTrue(query_id.startswith("rds-insight-"))
            rows = original("SELECT count() AS n FROM system.processes WHERE query_id={id:String}",
                            parameters={"id": query_id}, timeout=5)
            self.assertEqual(int(rows[0]["n"]), 0, "HTTP failure left a native query running")
        finally:
            if query_id:
                client.query("KILL QUERY WHERE query_id={id:String} SYNC",
                             parameters={"id": query_id}, timeout=10)


if __name__ == "__main__":
    unittest.main()
