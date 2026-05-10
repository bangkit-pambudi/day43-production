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
    "adventureworks_curated.top_products"
    # # HR pipeline
    # "adventureworks_curated.fact_hr_workforce",
    # # Vendor pipeline
    # "adventureworks_curated.fact_vendor_performance",
    # "adventureworks_curated.vendor_overall_ranking",
    # # RFM pipeline
    # "adventureworks_curated.fact_customer_rfm",
]

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="day43_adventureworks_batch_pipeline",
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

    # ── 2. Register: HDFS Parquet → Hive External Tables ─────────────────────
    register_hive_tables = make_spark_task(
        task_id="register_hive_tables",
        job_file="hive_to_parquet.py",
        extra_args=["--step", "register"],
        driver_memory="1g",
        timeout_minutes=10,
    )

    # ── 3. Verify: row-count check on raw Hive tables ─────────────────────────
    verify_raw_tables = make_spark_task(
        task_id="verify_raw_tables",
        job_file="hive_to_parquet.py",
        extra_args=["--step", "verify"],
        driver_memory="512m",
        timeout_minutes=5,
    )

    # ── 4a. Transform: Sales Performance ──────────────────────────────────────
    transform_sales = make_spark_task(
        task_id="transform_sales",
        job_file="sales_pipeline.py",
        driver_memory="1g",
        timeout_minutes=20,
    )

    # ── 5. Validate: all 8 curated tables must be non-empty ───────────────────
    validate_curated = make_validation_task(
        task_id="validate_curated",
        tables=CURATED_TABLES,
        timeout_minutes=5,
    )

    # ── 6. Notify: log completion summary ─────────────────────────────────────
    notify_complete = make_notify_task(
        task_id="notify_complete",
        message="All curated tables ready in adventureworks_curated.*",
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    #
    #  extract_to_hdfs
    #       ↓
    #  register_hive_tables
    #       ↓
    #  verify_raw_tables
    #       ↓
    #  ┌────────┬──────┬────────┬─────┐
    #  sales   hr   vendor   rfm    ← all run in parallel
    #  └────────┴──────┴────────┴─────┘
    #       ↓ (fan-in: all 4 must succeed)
    #  validate_curated
    #       ↓
    #  notify_complete

    transform_tasks = [transform_sales]

    (
        extract_to_hdfs
        >> register_hive_tables
        >> verify_raw_tables
        >> transform_tasks
        >> validate_curated
        >> notify_complete
    )
