#!/usr/bin/env bash
# Runs once on an empty data directory (docker-entrypoint-initdb.d). Passwords come from the environment.
set -euo pipefail

for var in LAKEFLOW_APP_PASSWORD DEBEZIUM_PASSWORD ICEBERG_CATALOG_PASSWORD CONTROL_DB_PASSWORD AIRFLOW_DB_PASSWORD TRINO_PG_PASSWORD; do
  if [ -z "${!var:-}" ]; then
    echo "FATAL: $var is not set. Run 'make bootstrap' to generate .env" >&2
    exit 1
  fi
done

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v app_pw="$LAKEFLOW_APP_PASSWORD" \
  -v dbz_pw="$DEBEZIUM_PASSWORD" \
  -v ice_pw="$ICEBERG_CATALOG_PASSWORD" \
  -v ctl_pw="$CONTROL_DB_PASSWORD" \
  -v af_pw="$AIRFLOW_DB_PASSWORD" \
  -v trino_pw="$TRINO_PG_PASSWORD" <<'SQL'
CREATE ROLE lakeflow_app LOGIN PASSWORD :'app_pw';
CREATE ROLE debezium LOGIN REPLICATION PASSWORD :'dbz_pw';
CREATE ROLE iceberg_catalog LOGIN PASSWORD :'ice_pw';
CREATE ROLE lakeflow_control LOGIN PASSWORD :'ctl_pw';
CREATE ROLE airflow LOGIN PASSWORD :'af_pw';
CREATE ROLE trino_reader LOGIN PASSWORD :'trino_pw';
GRANT pg_monitor TO lakeflow_app;

CREATE DATABASE lakeflow OWNER lakeflow_app;
CREATE DATABASE iceberg_catalog OWNER iceberg_catalog;
CREATE DATABASE lakeflow_control OWNER lakeflow_control;
CREATE DATABASE airflow OWNER airflow;
REVOKE ALL ON DATABASE lakeflow FROM PUBLIC;
GRANT CONNECT ON DATABASE lakeflow TO debezium, trino_reader;
SQL
