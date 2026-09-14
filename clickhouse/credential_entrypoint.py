"""IMDSv2 -> AWS container credentials, private to the ClickHouse process.

No static key/file fallback, credential persistence, proxy, or metadata redirect.
The SDK consumes the real STS expiration and refreshes without a server restart.
"""
from __future__ import annotations

import hmac
import http.server
import json
import logging
import os
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

LOG = logging.getLogger("oss_credentials")
METADATA = "http://100.100.100.200"
MAX_BODY = 16384


class CredentialUnavailable(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise CredentialUnavailable("metadata_redirect_refused")


class MetadataCredentials:
    def __init__(self, role="", *, opener=None, clock=time.time):
        if role and not re.fullmatch(r"[A-Za-z0-9._@-]{1,64}", role):
            raise ValueError("invalid_role_name")
        self.role = role
        self.opener = opener or urllib.request.build_opener(
            urllib.request.ProxyHandler({}), NoRedirect()
        )
        self.clock = clock
        self.token = ""
        self.token_until = 0.0
        self.refresh_at = 0.0
        self.cached = None

    def _request(self, path, *, method="GET", headers=None):
        request = urllib.request.Request(METADATA + path, method=method, headers=headers or {})
        with self.opener.open(request, timeout=3) as response:
            body = response.read(MAX_BODY + 1)
            if len(body) > MAX_BODY:
                raise CredentialUnavailable("metadata_response_too_large")
            return body.decode("utf-8")

    def _metadata(self, path):
        # Retry once, and only for an invalid IMDSv2 token. Never try IMDSv1.
        for attempt in range(2):
            if not self.token or self.clock() >= self.token_until:
                token = self._request("/latest/api/token", method="PUT", headers={
                    "X-aliyun-ecs-metadata-token-ttl-seconds": "21600"
                }).strip()
                if not token or any(ord(c) < 33 or ord(c) > 126 for c in token):
                    raise CredentialUnavailable("invalid_metadata_token")
                self.token = token
                self.token_until = self.clock() + 21540
            try:
                return self._request(path, headers={"X-aliyun-ecs-metadata-token": self.token})
            except urllib.error.HTTPError as exc:
                if exc.code != 401 or attempt:
                    raise
                self.token = ""
                LOG.warning("OSS_IMDS_TOKEN_REJECTED renewing IMDSv2 token once")
        raise CredentialUnavailable("metadata_token_rejected")

    def get(self):
        if self.cached is not None and self.clock() < self.refresh_at:
            return dict(self.cached)
        try:
            if not self.role:
                roles = self._metadata("/latest/meta-data/ram/security-credentials/").split()
                if len(roles) != 1 or not re.fullmatch(r"[A-Za-z0-9._@-]{1,64}", roles[0]):
                    raise CredentialUnavailable("ambiguous_metadata_role")
                self.role = roles[0]
            source = json.loads(self._metadata("/latest/meta-data/ram/security-credentials/" + self.role))
            if source.get("Code") != "Success":
                raise CredentialUnavailable("metadata_unsuccessful")
            fields = {key: source.get(key) for key in (
                "AccessKeyId", "AccessKeySecret", "SecurityToken", "Expiration"
            )}
            if any(not isinstance(v, str) or not v.strip() for v in fields.values()):
                raise CredentialUnavailable("incomplete_metadata_credential")
            expires = datetime.fromisoformat(fields["Expiration"].replace("Z", "+00:00"))
            if expires.tzinfo is None or expires.timestamp() <= self.clock() + 30:
                raise CredentialUnavailable("expired_metadata_credential")
            self.cached = {
                "AccessKeyId": fields["AccessKeyId"],
                "SecretAccessKey": fields["AccessKeySecret"],
                "Token": fields["SecurityToken"],
                "Expiration": expires.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            self.refresh_at = min(self.clock() + 300, expires.timestamp() - 300)
            LOG.info("OSS_CREDENTIAL_REFRESHED expires=%s", self.cached["Expiration"])
            return dict(self.cached)
        except Exception as exc:
            # Exception text can contain signed headers or upstream payloads.
            LOG.error("OSS_CREDENTIAL_REFRESH_FAILED type=%s no_static_fallback", type(exc).__name__)
            raise CredentialUnavailable("metadata_refresh_failed") from None


class CredentialServer(http.server.HTTPServer):
    allow_reuse_address = False

    def __init__(self, provider, token):
        self.provider = provider
        self.token = token
        # Ephemeral loopback port; not reachable from Docker peers or the host.
        super().__init__(("127.0.0.1", 0), CredentialHandler)

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(3)
        return sock, address

    def handle_error(self, request, client_address):
        LOG.error("OSS_CREDENTIAL_HTTP_FAILED response_not_completed")


class CredentialHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass  # Never log request headers, URLs, or credential response bodies.

    def do_GET(self):
        if self.path != "/credentials":
            LOG.warning("OSS_CREDENTIAL_REQUEST_REJECTED path")
            self.send_error(404)
            return
        if not hmac.compare_digest(self.headers.get("Authorization", ""), self.server.token):
            LOG.warning("OSS_CREDENTIAL_REQUEST_REJECTED authorization")
            self.send_error(403)
            return
        try:
            data = json.dumps(self.server.provider.get()).encode()
        except CredentialUnavailable:
            self.send_error(503, "credential refresh unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def child_environment(original, server):
    # Exclude static env/profile/web-identity/IMDS providers, including inherited
    # partial keys. Preserve only explicitly configured signing regions.
    env = {k: v for k, v in original.items() if not k.startswith("AWS_") or k in (
        "AWS_REGION", "AWS_DEFAULT_REGION"
    )}
    env.update({
        "AWS_CONTAINER_CREDENTIALS_FULL_URI": f"http://127.0.0.1:{server.server_port}/credentials",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN": server.token,
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_CONFIG_FILE": "/dev/null",
    })
    return env


def run(argv):
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda _sig, _frame: stopping.set())
    provider = MetadataCredentials(os.environ.get("RDS_BINLOG_OSS_ROLE_NAME", "") or
                                   os.environ.get("ALIBABA_CLOUD_ECS_METADATA", ""))
    provider.get()  # Fail before starting ClickHouse if the role chain is unavailable.
    if stopping.is_set():
        return 143
    server = CredentialServer(provider, secrets.token_hex(32))
    thread = threading.Thread(target=server.serve_forever, name="oss-credentials", daemon=True)
    thread.start()
    child = None
    stop_started = None
    try:
        child = subprocess.Popen(argv, env=child_environment(os.environ, server), start_new_session=True)
        LOG.info("OSS_CREDENTIAL_BRIDGE_READY imdsv2=true loopback=true authenticated=true")
        while child.poll() is None:
            if not thread.is_alive():
                LOG.error("OSS_CREDENTIAL_SERVER_STOPPED stopping dependent ClickHouse")
                stopping.set()
            if stopping.is_set() and stop_started is None:
                LOG.info("OSS_CREDENTIAL_BRIDGE_STOPPING forwarding SIGTERM")
                os.killpg(child.pid, signal.SIGTERM)
                stop_started = time.monotonic()
            if stop_started is not None and time.monotonic() - stop_started >= 55:
                LOG.error("OSS_CREDENTIAL_CHILD_STOP_TIMEOUT escalating after 55 seconds")
                os.killpg(child.pid, signal.SIGKILL)
            time.sleep(0.2)
        code = child.returncode
        return code if code >= 0 else 128 - code
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        sys.exit(run(sys.argv[1:] or ["/entrypoint.sh"]))
    except Exception as error:
        LOG.error("OSS_CREDENTIAL_BRIDGE_FATAL type=%s", type(error).__name__)
        sys.exit(1)
