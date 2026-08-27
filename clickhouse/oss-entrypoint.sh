#!/bin/sh
set -eu

case "${RDS_BINLOG_CLICKHOUSE_OSS_ENABLED:-0}" in
    1|true|TRUE|yes|YES|on|ON)
        credential_file="${RDS_BINLOG_OSS_CREDENTIAL_FILE:-/run/secrets/clickhouse_oss_credentials}"
        if [ ! -r "$credential_file" ]; then
            echo "ClickHouse OSS credential file is not readable" >&2
            exit 1
        fi

        json_value() {
            sed -n \
                "s/.*\"${1}\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" \
                "$credential_file" | sed -n '1p'
        }

        AWS_ACCESS_KEY_ID="$(json_value access_key_id)"
        AWS_SECRET_ACCESS_KEY="$(json_value access_key_secret)"
        AWS_SESSION_TOKEN="$(json_value security_token)"
        if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
            echo "ClickHouse OSS credential file is incomplete" >&2
            exit 1
        fi
        export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
        if [ -n "$AWS_SESSION_TOKEN" ]; then
            export AWS_SESSION_TOKEN
        else
            unset AWS_SESSION_TOKEN
        fi
        ;;
esac

exec /entrypoint.sh "$@"
