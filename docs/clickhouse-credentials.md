# Refreshable ClickHouse OSS credentials

`RDS_BINLOG_CLICKHOUSE_OSS_AUTH_MODE=ecs_ram_role` selects the credential-enabled
ClickHouse image (`clickhouse-<release>` tag in the existing GHCR package).
The upstream engine remains pinned to the existing 26.3 LTS digest; only Python's standard library
and the credential entrypoint are added. Production deployments pin the CI digest.

The entrypoint obtains temporary credentials from Alibaba ECS **IMDSv2 only**,
then serves the AWS container credential JSON format on an ephemeral loopback
port. ClickHouse uses its native `GeneralHTTPCredentialsProvider` and the real STS
expiration. An in-memory random authorization token protects the endpoint.
No temporary credential or endpoint token is written to disk or published on a
container/host port. Metadata access bypasses proxies and refuses redirects.

The bridge polls the metadata credential at most every five minutes during normal
use and refreshes early near expiry. On refresh failure it logs the reason category
and returns 503, not an old key. Static environment/profile/web-identity/EC2
credential fallbacks are removed from the ClickHouse child environment. Existing
static credential assets may be retained for rollback but are not read in role
mode. `access_key` remains an explicit legacy mode; unknown modes fail closed.

The credential HTTP server and ClickHouse have one supervised container lifecycle.
SIGTERM is forwarded to the server process group; its existing 60-second Docker
stop grace period is retained. A failed credential server stops its dependent
ClickHouse process so the existing restart policy can recover both together.
The collector and independent workers do not share this lifecycle.

## Acceptance gates

- `python -m unittest tests.test_clickhouse_credentials`: exact expiration, STS
  and IMDS token rotation, no static fallback, fail-closed 503, secret-safe logging,
  authenticated loopback access, proxy/redirect refusal.
- Cloud CI starts the actual engine with a synthetic STS provider/S3 endpoint:
  both credential generations must read `7`, no rejected signature identity is
  allowed, container start time must remain unchanged, and SIGTERM must exit 0.
- Before production recreation, use an isolated data directory and the published
  digest to verify the real ECS role and real OSS reads. Never mount production
  ClickHouse data into a simultaneously running candidate.
- Preserve production data/config mounts, engine version, table identities and
  collector identity. Only then recreate ClickHouse and roll existing workers.
- Real column reads (not metadata-only `count()`) and advancing worker completion
  are required. Liveness alone is insufficient; historical coverage is separate.

## Upstream contracts

- ClickHouse 26.3 LTS, `src/IO/S3/Credentials.cpp`: native full-URI
  container provider and authorization token support.
- Alibaba ECS instance metadata: IMDSv2 token PUT, RAM role credential GET,
  `AccessKeyId`, `AccessKeySecret`, `SecurityToken`, `Expiration`.
- AWS container credential response: `AccessKeyId`, `SecretAccessKey`, `Token`,
  `Expiration`.
