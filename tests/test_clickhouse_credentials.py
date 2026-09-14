from __future__ import annotations

import io
import json
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from unittest.mock import patch

from clickhouse.credential_entrypoint import (
    CredentialServer, CredentialUnavailable, MetadataCredentials, NoRedirect, child_environment,
)


class Reply(io.BytesIO):
    pass


class Opener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        assert timeout == 3
        self.requests.append(request)
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        if isinstance(value, dict):
            value = json.dumps(value)
        return Reply(value.encode())


def credential(expiry=3000, key="fixture-key"):
    return {"Code": "Success", "AccessKeyId": key, "AccessKeySecret": "fixture-secret",
            "SecurityToken": "fixture-token", "Expiration": datetime.fromtimestamp(
                expiry, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


class MetadataCredentialTests(unittest.TestCase):
    def test_sanitization_only_allows_documented_metadata_endpoint(self):
        from tools.public_sanitization_check import _allowed_public_ip
        from clickhouse.credential_entrypoint import METADATA
        address = METADATA.removeprefix("http://")
        self.assertTrue(_allowed_public_ip(address))
        neighbor = address.rsplit(".", 1)[0] + ".201"
        self.assertFalse(_allowed_public_ip(neighbor))

    def test_exact_translation_expiration_and_cached_requests(self):
        op = Opener(["metadata-token", credential()])
        p = MetadataCredentials("fixture-role", opener=op, clock=lambda: 1000)
        value = p.get()
        self.assertEqual(value, {"AccessKeyId": "fixture-key", "SecretAccessKey": "fixture-secret",
                                 "Token": "fixture-token", "Expiration": "1970-01-01T00:50:00Z"})
        value["Token"] = "changed"
        self.assertEqual(p.get()["Token"], "fixture-token")
        self.assertEqual(len(op.requests), 2)
        self.assertEqual(op.requests[0].method, "PUT")
        self.assertEqual(op.requests[1].get_header("X-aliyun-ecs-metadata-token"), "metadata-token")

    def test_rotation_without_process_restart(self):
        now = [1000]
        op = Opener(["metadata-token", credential(), credential(4000, "rotated-key")])
        p = MetadataCredentials("role", opener=op, clock=lambda: now[0])
        self.assertEqual(p.get()["AccessKeyId"], "fixture-key")
        now[0] += 301
        self.assertEqual(p.get()["AccessKeyId"], "rotated-key")
        self.assertEqual(len(op.requests), 3)

    def test_metadata_token_rotates(self):
        now = [1000]
        op = Opener(["old-token", credential(50000), "new-token", credential(80000)])
        p = MetadataCredentials("role", opener=op, clock=lambda: now[0])
        p.get()
        now[0] = 23000
        p.get()
        self.assertEqual(op.requests[-1].get_header("X-aliyun-ecs-metadata-token"), "new-token")

    def test_401_renews_token_once(self):
        op = Opener(["old", urllib.error.HTTPError("metadata", 401, "fixture", {}, None),
                     "new", credential()])
        p = MetadataCredentials("role", opener=op, clock=lambda: 1000)
        p.get()
        self.assertEqual([r.method for r in op.requests], ["PUT", "GET", "PUT", "GET"])

    def test_repeated_401_fails_closed(self):
        error = urllib.error.HTTPError("metadata", 401, "fixture", {}, None)
        op = Opener(["old", error, "new", error])
        with self.assertRaises(CredentialUnavailable):
            MetadataCredentials("role", opener=op, clock=lambda: 1000).get()
        self.assertEqual(len(op.requests), 4)

    def test_auto_discovers_single_role(self):
        op = Opener(["token", "fixture-role\n", credential()])
        p = MetadataCredentials(opener=op, clock=lambda: 1000)
        p.get()
        self.assertEqual(p.role, "fixture-role")

    def test_ambiguous_role_refused(self):
        op = Opener(["token", "role-a\nrole-b"])
        with self.assertRaises(CredentialUnavailable):
            MetadataCredentials(opener=op, clock=lambda: 1000).get()

    def test_bad_role_refused(self):
        for value in ("../other", "a/b", "a\nb"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                MetadataCredentials(value)

    def test_failed_refresh_never_returns_old_snapshot_or_logs_secrets(self):
        now = [1000]
        op = Opener(["token", credential(), ValueError("fixture-secret fixture-token")])
        p = MetadataCredentials("role", opener=op, clock=lambda: now[0])
        p.get()
        now[0] += 301
        with self.assertLogs("oss_credentials", level="ERROR") as logs:
            with self.assertRaises(CredentialUnavailable):
                p.get()
        self.assertIn("no_static_fallback", " ".join(logs.output))
        self.assertNotIn("fixture-secret", " ".join(logs.output))
        self.assertNotIn("fixture-token", " ".join(logs.output))

    def test_invalid_expired_and_incomplete_credentials_refused(self):
        cases = [credential(999), credential(1029), {**credential(), "SecurityToken": ""},
                 {**credential(), "Code": "Denied"}, {**credential(), "Expiration": "bad"},
                 {**credential(), "Expiration": "2027-01-01T00:00:00"}, "not-json"]
        for source in cases:
            with self.subTest(source=source), self.assertRaises(CredentialUnavailable):
                MetadataCredentials("role", opener=Opener(["token", source]), clock=lambda: 1000).get()

    def test_metadata_response_size_is_bounded(self):
        with self.assertRaises(CredentialUnavailable):
            MetadataCredentials("role", opener=Opener(["x" * 16385])).get()

    def test_redirects_and_environment_proxy_are_not_used(self):
        with self.assertRaises(CredentialUnavailable):
            NoRedirect().redirect_request(None, None, 302, "", {}, "https://example.org")
        with patch.dict("os.environ", {"HTTP_PROXY": "http://bad-proxy:9999"}):
            p = MetadataCredentials("role")
            for handler in p.opener.handlers:
                if isinstance(handler, urllib.request.ProxyHandler):
                    self.assertEqual(handler.proxies, {})

    def test_missing_imds_v2_fails_without_get_or_static_fallback(self):
        op = Opener([urllib.error.HTTPError("metadata", 403, "fixture", {}, None)])
        with patch.dict("os.environ", {"AWS_ACCESS_KEY_ID": "old-key"}):
            with self.assertRaises(CredentialUnavailable):
                MetadataCredentials("role", opener=op).get()
        self.assertEqual([r.method for r in op.requests], ["PUT"])


class CredentialHTTPTests(unittest.TestCase):
    def setUp(self):
        self.provider = MetadataCredentials("role", opener=Opener(["token", credential()]), clock=lambda: 1000)
        self.server = CredentialServer(self.provider, "fixture-private-auth")
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/credentials"
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_only_loopback_bound(self):
        self.assertEqual(self.server.server_address[0], "127.0.0.1")

    def test_authenticated_response_not_cacheable(self):
        req = urllib.request.Request(self.url, headers={"Authorization": self.server.token})
        with self.opener.open(req, timeout=2) as r:
            self.assertEqual(r.headers["Cache-Control"], "no-store")
            self.assertEqual(json.load(r)["Token"], "fixture-token")

    def test_unauthenticated_rejected_without_fetching_credentials(self):
        with self.assertRaises(urllib.error.HTTPError) as cm:
            self.opener.open(self.url, timeout=2)
        self.assertEqual(cm.exception.code, 403)
        self.assertEqual(self.provider.opener.requests, [])

    def test_unknown_path_rejected(self):
        with self.assertRaises(urllib.error.HTTPError) as cm:
            self.opener.open(self.url + "?other", timeout=2)
        self.assertEqual(cm.exception.code, 404)

    def test_failure_is_503_not_stale_credentials(self):
        req = urllib.request.Request(self.url, headers={"Authorization": self.server.token})
        with patch.object(self.provider, "get", side_effect=CredentialUnavailable()):
            with self.assertRaises(urllib.error.HTTPError) as cm:
                self.opener.open(req, timeout=2)
        self.assertEqual(cm.exception.code, 503)

    def test_child_environment_excludes_legacy_sources(self):
        original = {"AWS_ACCESS_KEY_ID": "old-key", "AWS_SECRET_ACCESS_KEY": "old-secret",
                    "AWS_SESSION_TOKEN": "old-token", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/old",
                    "AWS_WEB_IDENTITY_TOKEN_FILE": "old-file", "AWS_REGION": "fixture-region", "TZ": "UTC"}
        env = child_environment(original, self.server)
        for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_WEB_IDENTITY_TOKEN_FILE"):
            self.assertNotIn(key, env)
        self.assertEqual(env["AWS_EC2_METADATA_DISABLED"], "true")
        self.assertEqual(env["AWS_CONFIG_FILE"], "/dev/null")
        self.assertEqual(env["AWS_SHARED_CREDENTIALS_FILE"], "/dev/null")
        self.assertEqual(env["AWS_CONTAINER_CREDENTIALS_FULL_URI"], self.url)
        self.assertEqual(env["AWS_REGION"], "fixture-region")
        self.assertEqual(env["TZ"], "UTC")


if __name__ == "__main__":
    unittest.main()
