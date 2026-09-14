"""Cloud CI only: rotate synthetic STS credentials under the real ClickHouse SDK."""
import http.server
import json
import logging
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from clickhouse import credential_entrypoint as bridge

STATE = {"phase": 1, "reads": [], "failures": 0}


class Provider:
    def get(self):
        phase = STATE["phase"]
        return {"AccessKeyId": f"fixture-key-{phase}", "SecretAccessKey": f"fixture-secret-{phase}",
                "Token": f"fixture-token-{phase}", "Expiration": datetime.fromtimestamp(
                    time.time() + 2, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


class Source(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        if self.path == "/rotate":
            STATE["phase"] = 2
            body = b"ok"
        elif self.path == "/evidence":
            body = json.dumps(STATE).encode()
        else:
            phase = STATE["phase"]
            if (f"Credential=fixture-key-{phase}/" not in self.headers.get("Authorization", "") or
                    self.headers.get("x-amz-security-token") != f"fixture-token-{phase}"):
                STATE["failures"] += 1
                self.send_error(403, "fixture credential mismatch")
                return
            if self.command == "GET":
                STATE["reads"].append(phase)
            body = b"7\n"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    Path('/etc/clickhouse-server/config.d/credentials-ci.xml').write_text(
        '<clickhouse><s3><use_environment_credentials>true</use_environment_credentials></s3></clickhouse>')
    server = http.server.HTTPServer(("127.0.0.1", 31812), Source)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    bridge.MetadataCredentials = lambda *_args: Provider()
    raise SystemExit(bridge.run(["/entrypoint.sh"]))
