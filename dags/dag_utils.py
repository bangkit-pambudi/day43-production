"""
dags/dag_utils.py
=================
Shared helpers for all Day 43 Airflow DAGs.

Provides:
  - _make_spark_submit()  : factory for SparkSubmitOperator / PythonOperator fallback
  - _make_python_runner() : subprocess-based fallback when SparkSubmit unavailable
  - _validate_curated()   : row-count check on curated Hive tables
  - _notify_complete()    : pipeline completion notification
"""

import os
import subprocess
import sys
from datetime import timedelta

from airflow.operators.python import PythonOperator

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    SPARK_SUBMIT_AVAILABLE = True
except ImportError:
    SPARK_SUBMIT_AVAILABLE = False

# ─── Paths ────────────────────────────────────────────────────────────────────

ROOT_DIR  = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JOBS_DIR  = os.path.join(ROOT_DIR, "jobs")
CONFIG    = os.path.join(ROOT_DIR, "configs", "pipeline.yaml")
JARS      = os.path.join(ROOT_DIR, "..", "Day-42", "jars", "postgresql-42.7.3.jar")

SPARK_CONF = {
    "spark.sql.shuffle.partitions":     "4",
    "spark.ui.showConsoleProgress":     "false",
    "spark.sql.adaptive.enabled":       "true",
}


# ─── Factory ──────────────────────────────────────────────────────────────────

def make_spark_task(task_id: str, job_file: str,
                    extra_args: list = None,
                    driver_memory: str = "1g",
                    timeout_minutes: int = 20):
    """
    Return a SparkSubmitOperator if the provider is installed,
    otherwise fall back to a PythonOperator that calls the job via subprocess.

    This lets the DAG work in both full Airflow+Spark environments
    and lightweight local/Codespace setups.
    """
    application = os.path.join(JOBS_DIR, job_file)
    args = ["--config", CONFIG] + (extra_args or [])

    if SPARK_SUBMIT_AVAILABLE:
        return SparkSubmitOperator(
            task_id=task_id,
            application=application,
            application_args=args,
            jars=JARS,
            conn_id="spark_local",
            driver_memory=driver_memory,
            executor_memory=driver_memory,
            conf=SPARK_CONF,
            execution_timeout=timedelta(minutes=timeout_minutes),
        )

    # Fallback: run as a plain Python subprocess
    def _run(**context):
        cmd = [sys.executable, application] + args
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(
                f"Job {job_file} failed with exit code {result.returncode}"
            )

    return PythonOperator(
        task_id=task_id,
        python_callable=_run,
        execution_timeout=timedelta(minutes=timeout_minutes),
    )


# ─── Validation callable ──────────────────────────────────────────────────────

def make_validation_task(task_id: str, tables: list, timeout_minutes: int = 5):
    """
    Return a PythonOperator that verifies all listed Hive tables are non-empty.
    tables: list of fully-qualified table names, e.g. 'adventureworks_curated.fact_hr_workforce'
    """
    def _validate(**context):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .master("local[1]") \
            .appName(f"ValidateCurated-{task_id}") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.ui.showConsoleProgress", "false") \
            .enableHiveSupport() \
            .getOrCreate()

        failed = []
        print(f"\n{'─'*60}")
        print(f"Validation: {task_id}")
        print(f"{'─'*60}")
        for table in tables:
            try:
                cnt = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
                status = "✓ OK   " if cnt > 0 else "✗ EMPTY"
                print(f"  {status} | {table} | {cnt:,} rows")
                if cnt == 0:
                    failed.append(table)
            except Exception as e:
                print(f"  ✗ ERROR | {table} | {e}")
                failed.append(table)

        spark.stop()

        if failed:
            raise AssertionError(
                f"Validation FAILED — empty or missing tables: {failed}"
            )
        print(f"\nAll {len(tables)} tables validated successfully.\n")

    return PythonOperator(
        task_id=task_id,
        python_callable=_validate,
        execution_timeout=timedelta(minutes=timeout_minutes),
    )


# ─── Notification callable ────────────────────────────────────────────────────

def make_notify_task(task_id: str = "notify_complete", message: str = None):
    """Return a PythonOperator that logs pipeline completion details."""

    def _notify(**context):
        dag_run    = context.get("dag_run")
        exec_date  = context.get("logical_date") or context.get("execution_date")
        dag_id     = dag_run.dag_id if dag_run else "unknown"

        print(f"\n{'═'*60}")
        print(f"  PIPELINE COMPLETE")
        print(f"  DAG        : {dag_id}")
        print(f"  Run date   : {exec_date}")
        if message:
            print(f"  Message    : {message}")
        print(f"{'═'*60}\n")

    return PythonOperator(
        task_id=task_id,
        python_callable=_notify,
    )
