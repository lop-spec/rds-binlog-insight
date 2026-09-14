# Preserve the deployed upstream ClickHouse engine; add only a stdlib credential
# adapter. This image is built, tested and published exclusively by cloud CI.
FROM clickhouse/clickhouse-server@sha256:2ef11bbe2e44ab7022f37ff3019b3f2125ed09e919ea6194660be6130b7ca4b7
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY clickhouse/oss-entrypoint.sh clickhouse/credential_entrypoint.py /opt/sql-insight/
ENTRYPOINT ["sh", "/opt/sql-insight/oss-entrypoint.sh"]
