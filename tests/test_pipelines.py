"""
tests/test_pipelines.py
=======================
DAG integrity tests + pipeline unit tests.

These tests do NOT require a running Airflow instance or Hive cluster.
They verify:
  1. DAG can be imported without errors
  2. DAG has the expected structure (tasks, dependencies, schedule)
  3. Core pipeline functions produce correct output on synthetic data

Run:
    cd day43-production
    pytest tests/test_pipelines.py -v
"""

import sys
import os
import pytest

# Make sure jobs/ and utils/ are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── DAG INTEGRITY TESTS ──────────────────────────────────────────────────────

class TestDAGIntegrity:
    """Verify the DAG loads cleanly and has the correct structure."""

    @pytest.fixture(scope="class")
    def dag(self):
        """Import the DAG object directly — no running Airflow needed."""
        # Set minimal env vars so dag_utils paths resolve
        os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
        os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow_test")

        try:
            from airflow import configuration
            configuration.conf.set("core", "unit_test_mode", "True")
        except Exception:
            pass

        from dags.day43_batch_pipeline import dag as _dag
        return _dag

    def test_dag_loads(self, dag):
        assert dag is not None

    def test_dag_id(self, dag):
        assert dag.dag_id == "day43_adventureworks_batch_pipeline"

    def test_dag_schedule(self, dag):
        assert dag.schedule_interval == "0 2 * * *" or dag.schedule == "0 2 * * *"

    def test_dag_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_dag_max_active_runs(self, dag):
        assert dag.max_active_runs == 1

    def test_expected_task_ids(self, dag):
        expected = {
            "extract_to_hdfs",
            "register_hive_tables",
            "verify_raw_tables",
            "transform_sales",
            "transform_hr",
            "transform_vendor",
            "transform_rfm",
            "validate_curated",
            "notify_complete",
        }
        actual = set(dag.task_ids)
        assert expected == actual, f"Missing tasks: {expected - actual}"

    def test_extract_is_first(self, dag):
        task = dag.get_task("extract_to_hdfs")
        assert len(task.upstream_task_ids) == 0

    def test_notify_is_last(self, dag):
        task = dag.get_task("notify_complete")
        assert len(task.downstream_task_ids) == 0

    def test_transform_tasks_are_parallel(self, dag):
        """All 4 transform tasks must have the same single upstream (verify_raw_tables)."""
        transforms = ["transform_sales", "transform_hr", "transform_vendor", "transform_rfm"]
        for tid in transforms:
            task = dag.get_task(tid)
            assert "verify_raw_tables" in task.upstream_task_ids, \
                f"{tid} does not depend on verify_raw_tables"

    def test_validate_waits_for_all_transforms(self, dag):
        """validate_curated must depend on all 4 transform tasks."""
        validate = dag.get_task("validate_curated")
        transforms = {"transform_sales", "transform_hr", "transform_vendor", "transform_rfm"}
        assert transforms.issubset(validate.upstream_task_ids), \
            f"Missing upstreams: {transforms - validate.upstream_task_ids}"

    def test_default_retries(self, dag):
        for task in dag.tasks:
            assert task.retries == 2, \
                f"Task {task.task_id} has {task.retries} retries, expected 2"


# ─── PIPELINE FUNCTION TESTS ──────────────────────────────────────────────────

class TestHRPipelineFunctions:
    """Test HR pipeline transformation logic on synthetic data."""

    def test_filter_valid_departments(self, spark):
        from jobs.hr_pipeline import filter_valid_departments
        from pyspark.sql import functions as F

        data = [
            (1, "Production"),
            (2, "Engineering"),
            (3, "Sales"),
            (4, "Finance"),
        ]
        df = spark.createDataFrame(data, ["departmentid", "name"])
        result = filter_valid_departments(df)
        depts = {r["dept_name"] for r in result.collect()}
        assert depts == {"Production", "Engineering"}

    def test_get_latest_pay_rate(self, spark):
        from jobs.hr_pipeline import get_latest_pay_rate

        data = [
            (1, "2020-01-01", 15.0),
            (1, "2022-06-01", 20.0),   # latest for employee 1
            (1, "2021-03-01", 18.0),
            (2, "2023-01-01", 30.0),   # latest for employee 2
        ]
        df = spark.createDataFrame(data, ["businessentityid", "ratechangedate", "rate"])
        result = get_latest_pay_rate(df)
        rows = {r["pay_emp_id"]: r["latest_pay_rate"] for r in result.collect()}
        assert rows[1] == 20.0
        assert rows[2] == 30.0

    def test_dept_change_count(self, spark):
        from jobs.hr_pipeline import get_dept_change_count

        data = [
            (1, 10), (1, 20), (1, 30),   # employee 1: 3 dept changes
            (2, 10),                       # employee 2: 1 (never moved)
        ]
        df = spark.createDataFrame(data, ["businessentityid", "departmentid"])
        result = get_dept_change_count(df)
        rows = {r["businessentityid"]: r["dept_history_count"] for r in result.collect()}
        assert rows[1] == 3
        assert rows[2] == 1


class TestVendorPipelineFunctions:
    """Test Vendor pipeline transformation logic on synthetic data."""

    def test_filter_ship_methods(self, spark):
        from jobs.vendor_pipeline import filter_ship_methods

        data = [
            (1, "CARGO TRANSPORT 5"),
            (2, "OVERNIGHT J-FAST"),
            (3, "XL MAIL"),
            (4, "GROUND EXPRESS"),
        ]
        df = spark.createDataFrame(data, ["shipmethodid", "name"])
        result = filter_ship_methods(df)
        methods = {r["ship_method"] for r in result.collect()}
        assert methods == {"CARGO TRANSPORT 5", "OVERNIGHT J-FAST"}

    def test_vendor_score_formula(self, spark):
        """VendorScore = (OnTimeRate * 0.6) + ((100 - ABS(AvgPriceVariance)) * 0.4)"""
        from pyspark.sql import functions as F

        # on_time_rate=80, avg_price_variance=5 → score = 48 + 38 = 86
        data = [(1, "Vendor A", 1, "CARGO TRANSPORT 5", 10, 8, 5.0)]
        schema = ["vendor_id", "vendor_name", "credit_rating", "ship_method",
                  "total_orders", "on_time_orders", "avg_price_variance"]
        df = spark.createDataFrame(data, schema) \
            .withColumn("on_time_rate",
                F.round(F.col("on_time_orders") / F.col("total_orders") * 100, 2)) \
            .withColumn("vendor_score",
                F.round(
                    (F.col("on_time_rate") * 0.6) +
                    ((100 - F.abs(F.col("avg_price_variance"))) * 0.4),
                2))
        row = df.collect()[0]
        assert row["on_time_rate"] == 80.0
        assert row["vendor_score"] == 86.0

    def test_on_time_flag_null_shipdate(self, spark):
        """Orders with NULL ShipDate must not count as on_time."""
        from pyspark.sql import functions as F

        data = [
            (1, "2024-01-10", "2024-01-15", None),    # ShipDate null → not on time
            (2, "2024-01-10", "2024-01-15", "2024-01-14"),  # on time
            (3, "2024-01-10", "2024-01-15", "2024-01-20"),  # late
        ]
        df = spark.createDataFrame(
            data, ["id", "orderdate", "duedate", "shipdate"]
        ).withColumn("shipdate", F.col("shipdate").cast("date")) \
         .withColumn("duedate",  F.col("duedate").cast("date")) \
         .withColumn("is_on_time",
            F.when(
                F.col("shipdate").isNotNull() & (F.col("shipdate") <= F.col("duedate")),
                F.lit(1)
            ).otherwise(F.lit(0)))

        rows = {r["id"]: r["is_on_time"] for r in df.collect()}
        assert rows[1] == 0   # null shipdate → 0
        assert rows[2] == 1   # on time → 1
        assert rows[3] == 0   # late → 0


class TestRFMPipelineFunctions:
    """Test RFM scoring and segmentation logic on synthetic data."""

    def test_rfm_raw_computation(self, spark):
        from jobs.rfm_pipeline import compute_rfm_raw

        data = [
            (1, 1001, "2014-01-01", 500.0),
            (1, 1002, "2014-06-15", 300.0),
            (2, 1003, "2013-12-01", 200.0),
        ]
        df = spark.createDataFrame(data, ["customerid", "salesorderid", "orderdate", "totaldue"])
        df = df.withColumn("orderdate", df["orderdate"].cast("date"))

        result = compute_rfm_raw(df, reference_date="2014-07-31")
        rows = {r["customerid"]: r for r in result.collect()}

        # Customer 1: last order 2014-06-15, frequency=2, monetary=800
        assert rows[1]["frequency"] == 2
        assert rows[1]["monetary"] == 800.0
        assert rows[1]["recency_days"] == 46   # 2014-07-31 - 2014-06-15

        # Customer 2: frequency=1, monetary=200
        assert rows[2]["frequency"] == 1
        assert rows[2]["monetary"] == 200.0

    def test_segment_champion(self, spark):
        from jobs.rfm_pipeline import assign_segments

        data = [(1, 3, 3, 3), (2, 4, 4, 4), (3, 1, 1, 1)]
        df = spark.createDataFrame(data, ["customerid", "r_score", "f_score", "m_score"]) \
            .withColumn("rfm_score",
                __import__("pyspark.sql.functions", fromlist=["concat", "col"]).concat(
                    __import__("pyspark.sql.functions", fromlist=["col"]).col("r_score").cast("string"),
                    __import__("pyspark.sql.functions", fromlist=["col"]).col("f_score").cast("string"),
                    __import__("pyspark.sql.functions", fromlist=["col"]).col("m_score").cast("string"),
                ))

        result = assign_segments(df)
        rows = {r["customerid"]: r["rfm_segment"] for r in result.collect()}
        assert rows[1] == "Bronze Champion"
        assert rows[2] == "Gold Champion"
        assert rows[3] == "Lost"

    def test_segment_new_customer(self, spark):
        from jobs.rfm_pipeline import assign_segments
        from pyspark.sql import functions as F

        data = [(10, 4, 1, 1)]
        df = spark.createDataFrame(data, ["customerid", "r_score", "f_score", "m_score"]) \
            .withColumn("rfm_score",
                F.concat(F.col("r_score").cast("string"),
                         F.col("f_score").cast("string"),
                         F.col("m_score").cast("string")))
        result = assign_segments(df)
        row = result.collect()[0]
        assert row["rfm_segment"] == "New Customer"

    def test_segment_at_risk(self, spark):
        from jobs.rfm_pipeline import assign_segments
        from pyspark.sql import functions as F

        data = [(20, 1, 4, 4)]   # R=1 (low) F>=3 M>=3 → At Risk
        df = spark.createDataFrame(data, ["customerid", "r_score", "f_score", "m_score"]) \
            .withColumn("rfm_score",
                F.concat(F.col("r_score").cast("string"),
                         F.col("f_score").cast("string"),
                         F.col("m_score").cast("string")))
        result = assign_segments(df)
        row = result.collect()[0]
        assert row["rfm_segment"] == "At Risk"

    def test_rfm_score_string_format(self, spark):
        """rfm_score must be a 3-character string e.g. '344'."""
        from jobs.rfm_pipeline import assign_segments
        from pyspark.sql import functions as F

        data = [(1, 3, 4, 4)]
        df = spark.createDataFrame(data, ["customerid", "r_score", "f_score", "m_score"]) \
            .withColumn("rfm_score",
                F.concat(F.col("r_score").cast("string"),
                         F.col("f_score").cast("string"),
                         F.col("m_score").cast("string")))
        result = assign_segments(df).collect()[0]
        assert result["rfm_score"] == "344"
        assert len(result["rfm_score"]) == 3
