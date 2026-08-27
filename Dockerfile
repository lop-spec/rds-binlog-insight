FROM python:3.12-slim-bookworm@sha256:b64e9d3a71eddaa1b3f80c04abf292b3139e3b7c4dd272d19c31dc1f91194d1b AS sqlite-builder

ARG SQLITE_AUTOCONF_VERSION=3530400
ARG SQLITE_AUTOCONF_SHA3=454e45f61c6bd75b7420e7190732dea03ce6639c63ada47bbc592f67fc340338

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/sqlite-build
RUN curl --fail --show-error --silent --location \
        --connect-timeout 10 --max-time 120 \
        --output sqlite-autoconf.tar.gz \
        "https://www2.sqlite.org/2026/sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}.tar.gz" \
    && SQLITE_AUTOCONF_SHA3="${SQLITE_AUTOCONF_SHA3}" python -c \
        "import hashlib, os, pathlib; p = pathlib.Path('sqlite-autoconf.tar.gz'); actual = hashlib.sha3_256(p.read_bytes()).hexdigest(); expected = os.environ['SQLITE_AUTOCONF_SHA3']; assert actual == expected, f'SQLite source SHA3 mismatch: {actual}'" \
    && mkdir source \
    && tar -xzf sqlite-autoconf.tar.gz --strip-components=1 -C source \
    && cd source \
    && ./configure \
        --prefix=/usr/local \
        --disable-static \
        --disable-readline \
        --disable-static-shell \
        --soname=legacy \
        --all \
    && make -j2 \
    && make install

FROM python:3.12-slim-bookworm@sha256:b64e9d3a71eddaa1b3f80c04abf292b3139e3b7c4dd272d19c31dc1f91194d1b

LABEL org.opencontainers.image.title="RDS Binlog Insight" \
      org.opencontainers.image.version="1.26.1-rawoss" \
      org.opencontainers.image.sqlite.version="3.53.4"

COPY --from=sqlite-builder /usr/local/lib/ /usr/local/lib/
RUN ldconfig \
    && python -c "import sqlite3; assert sqlite3.sqlite_version == '3.53.4', sqlite3.sqlite_version; connection = sqlite3.connect(':memory:'); connection.execute('CREATE VIRTUAL TABLE probe USING fts5(value)'); assert connection.execute(\"SELECT count(*) FROM pragma_compile_options WHERE compile_options = 'ENABLE_DBSTAT_VTAB'\").fetchone()[0] == 1"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RDS_BINLOG_DATA_DIR=/data \
    RDS_BINLOG_STAGING_DIR=/tmp/rds-binlog-staging \
    RDS_BINLOG_PARSER=/app/tools/binlog-parser

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --disable-pip-version-check -r /app/requirements.txt

COPY requirements-migrate.txt /app/requirements-migrate.txt
RUN python -m pip install --no-cache-dir --disable-pip-version-check -r /app/requirements-migrate.txt

COPY app /app/app
COPY clickhouse /app/clickhouse
COPY web /app/web
COPY tools/clickhouse_oss_backfill.py /app/tools/clickhouse_oss_backfill.py
COPY tools/clickhouse_oss_verify.py /app/tools/clickhouse_oss_verify.py
COPY tools/clickhouse_poc_benchmark.py /app/tools/clickhouse_poc_benchmark.py
COPY tools/clickhouse_slowlog_verify.py /app/tools/clickhouse_slowlog_verify.py
COPY tools/binlog-parser-linux-amd64 /app/tools/binlog-parser
RUN chmod 0555 /app/tools/binlog-parser

EXPOSE 8769

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8769/healthz', timeout=3).read()"]

CMD ["python", "-m", "app.service_supervisor", "--host", "0.0.0.0", "--port", "8769", "--data-dir", "/data"]
