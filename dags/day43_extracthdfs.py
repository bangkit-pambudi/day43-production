"""
dags/day43_batch_pipeline.py
============================
AdventureWorks Full Batch Pipeline — Day 43 Production DAG

Architecture
------------
PostgreSQL (AdventureWorks)
    ↓  [extract_to_hdfs]        jobs/hive_to_parquet.py --step extract
HDFS Parquet (raw layer)
    ↓  [register_hive_tables]   jobs/hive_to_parquet.py --step register
Hive External Tables (adventureworks.*)
    ↓  [verify_raw_tables]      jobs/hive_to_parquet.py --step verify
    ↓
    ├─ [transform_sales]    →   adventureworks_curated.fact_sales_performance
    │                           adventureworks_curated.monthly_sales_summary
    │                           adventureworks_curated.territory_yoy
    │                           adventureworks_curated.top_products
    │
    ├─ [transform_hr]       →   adventureworks_curated.fact_hr_workforce
    │
    ├─ [transform_vendor]   →   adventureworks_curated.fact_vendor_performance
    │                           adventureworks_curated.vendor_overall_ranking
    │
    └─ [transform_rfm]      →   adventureworks_curated.fact_customer_rfm
         ↓ (all 4 parallel tasks must succeed)
    [validate_curated]      — row-count check on all 8 curated tables
         ↓
    [notify_complete]       — log pipeline summary

Schedule  : 0 2 * * *  (daily at 02:00 UTC)
Retries   : 2 per task, 5-minute delay
Timeout   : per-task timeouts defined below
Catchup   : False (no backfill on first deploy)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_utils import (
    make_spark_task,
    make_validation_task,
    make_notify_task,
)

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": False,
    "email_on_failure":  False,
    "email_on_retry":    False,
}

# ─── Curated tables validated after all transforms ────────────────────────────

CURATED_TABLES = [
    # Sales pipeline
    "adventureworks_curated.fact_sales_performance",
    "adventureworks_curated.monthly_sales_summary",
    "adventureworks_curated.territory_yoy",
    "adventureworks_curated.top_products",
    # HR pipeline
    "adventureworks_curated.fact_hr_workforce",
    # Vendor pipeline
    "adventureworks_curated.fact_vendor_performance",
    "adventureworks_curated.vendor_overall_ranking",
    # RFM pipeline
    "adventureworks_curated.fact_customer_rfm",
]

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="day43_extracthdfs",
    default_args=default_args,
    description=(
        "AdventureWorks full batch pipeline: "
        "PostgreSQL → HDFS → Hive → Curated Layer (Sales, HR, Vendor, RFM)"
    ),
    start_date=datetime(2026, 5, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["day43", "batch", "adventureworks", "pyspark"],
    doc_md=__doc__,
) as dag:

    # ── 1. Extract: PostgreSQL → HDFS Parquet ─────────────────────────────────
    extract_to_hdfs = make_spark_task(
        task_id="extract_to_hdfs",
        job_file="hive_to_parquet.py",
        extra_args=["--step", "extract"],
        driver_memory="1g",
        timeout_minutes=30,
    )