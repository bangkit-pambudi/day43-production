#!/usr/bin/env bash
# configs/airflow_connections.sh
# ==============================
# Register all Airflow connections required by the Day 43 DAG.
# Run this once after `airflow db migrate` and before triggering the DAG.
#
# Usage:
#   bash configs/airflow_connections.sh
#
# Environment variables used (from .env):
#   JDBC_URL, DB_USER, DB_PASSWORD
#   HDFS_BASE, HIVE_METASTORE_URI

set -euo pipefail

# Load .env if present
if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

echo "=== Registering Airflow Connections ==="

# ── 1. spark_local ────────────────────────────────────────────────────────────
# Used by SparkSubmitOperator in the DAG.
# conn_type = spark, host = local[2]
echo "[1/3] spark_local (SparkSubmitOperator connection)"
airflow connections delete spark_local 2>/dev/null || true
airflow connections add spark_local \
    --conn-type    spark \
    --conn-host    "local[2]" \
    --conn-extra   "{
        \"spark-home\": \"\",
        \"queue\": \"root.default\",
        \"deploy-mode\": \"client\",
        \"spark_binary\": \"spark-submit\"
    }"

# ── 2. postgres_adventureworks ────────────────────────────────────────────────
# Used for monitoring or manual SQL checks — NOT used by the pipeline jobs
# directly (they load creds from .env via pipeline.yaml).
echo "[2/3] postgres_adventureworks (monitoring / DBeaver reference)"
JDBC_HOST=$(echo "${JDBC_URL:-jdbc:postgresql://adventureworks-postgres:5432/postgres}" \
    | sed 's|jdbc:postgresql://||' | cut -d: -f1)
JDBC_PORT=$(echo "${JDBC_URL:-jdbc:postgresql://adventureworks-postgres:5432/postgres}" \
    | sed 's|jdbc:postgresql://||' | cut -d: -f2 | cut -d/ -f1)
JDBC_SCHEMA=$(echo "${JDBC_URL:-jdbc:postgresql://adventureworks-postgres:5432/postgres}" \
    | rev | cut -d/ -f1 | rev)

airflow connections delete postgres_adventureworks 2>/dev/null || true
airflow connections add postgres_adventureworks \
    --conn-type   postgres \
    --conn-host   "${JDBC_HOST:-adventureworks-postgres}" \
    --conn-port   "${JDBC_PORT:-5432}" \
    --conn-schema "${JDBC_SCHEMA:-postgres}" \
    --conn-login  "${DB_USER:-postgres}" \
    --conn-password "${DB_PASSWORD:-My_password1}"

# ── 3. hive_cli ───────────────────────────────────────────────────────────────
# Used for HiveServer2 direct queries (optional — e.g. monitoring tasks).
echo "[3/3] hive_cli (HiveServer2 connection)"
HIVE_HOST=$(echo "${HIVE_METASTORE_URI:-thrift://hive-metastore:9083}" \
    | sed 's|thrift://||' | cut -d: -f1)

airflow connections delete hive_cli 2>/dev/null || true
airflow connections add hive_cli \
    --conn-type  hiveserver2 \
    --conn-host  "${HIVE_HOST:-hive-metastore}" \
    --conn-port  10000 \
    --conn-login hive

echo ""
echo "=== Connections registered. Verify with: ==="
echo "    airflow connections list"
echo ""
airflow connections list --output table 2>/dev/null | grep -E "spark_local|postgres_advent|hive_cli" || true
