"""
jobs/hr_pipeline.py
===================
Pipeline: Hive (adventureworks.*)  →  Spark Transform  →  Hive Curated

Business Case A — HR Attrition & Workforce Analytics

Rules:
  - ONLY departments: Production and Engineering
  - tenure_years  = months_between(current_date, hire_date) / 12
  - is_mover      = dept_history_count > 1
  - dept_change_count = GREATEST(dept_history_count - 1, 0)
  - latest_pay_rate   = most recent Rate from EmployeePayHistory

Target table: adventureworks_curated.fact_hr_workforce

Usage:
  spark-submit --jars jars/postgresql-42.7.3.jar \\
      jobs/hr_pipeline.py --config configs/pipeline.yaml
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.config_loader import load_config
from utils.io import read_hive, write_hive_table, validate
from utils.logger import get_logger
from utils.spark_factory import get_spark_with_hive

logger = get_logger(__name__)

VALID_DEPARTMENTS = ["Production", "Engineering"]


# ─── R1-R2 ──────────────────────────────────────────────────────────────────

def extract(spark: SparkSession, config: dict) -> dict:
    db = config["hive"]["raw_database"]
    logger.info("=== EXTRACT | database=%s ===", db)

    employees = read_hive(spark, db, "dim_employee")
    logger.info("Extracted %d employees", employees.count())
    return {
        "employee":    employees,
        "edh":         read_hive(spark, db, "dim_emp_dept_history"),
        "pay":         read_hive(spark, db, "dim_emp_pay_history"),
        "department":  read_hive(spark, db, "dim_department")
    }

# ─── R3 ────────────────────────────────────────────────────────────────

def filter_valid_departments(df_dept):
    """Keep only Production and Engineering departments."""
    df = df_dept.filter(F.col("Name").isin(VALID_DEPARTMENTS)) \
                .select("DepartmentID", F.col("Name").alias("dept_name"))
    logger.info("Valid departments: %d", df.count())
    return df


def get_active_dept_per_employee(df_edh):
    """Return the currently active department per employee (EndDate IS NULL)."""
    return df_edh.filter(F.col("enddate").isNull()) \
                 .select("businessentityid", "departmentid", "startdate")
# ─── R4 ────────────────────────────────────────────────────────────────

def get_latest_pay_rate(df_pay):
    """Pick the most recent pay rate per employee using ROW_NUMBER window."""
    win = Window.partitionBy("businessentityid").orderBy(F.desc("ratechangedate"))
    return df_pay \
        .withColumn("rn", F.row_number().over(win)) \
        .filter(F.col("rn") == 1) \
        .select(
            F.col("businessentityid").alias("pay_emp_id"),
            F.round(F.col("rate"), 2).alias("latest_pay_rate"),
        )

# ─── R5 ────────────────────────────────────────────────────────────────
def get_dept_change_count(df_edh):
    """Count total department history records per employee (all history, not just active)."""
    return df_edh \
        .groupBy("businessentityid") \
        .agg(F.count("*").alias("dept_history_count"))

# ─── R6 ────────────────────────────────────────────────────────────────

def enrich(dfs: dict):
    """Join all source tables and compute derived columns."""
    logger.info("Enriching HR data")

    df_dept_valid  = filter_valid_departments(dfs["department"])
    df_edh_active  = get_active_dept_per_employee(dfs["edh"])
    df_latest_pay  = get_latest_pay_rate(dfs["pay"])
    df_hist_count  = get_dept_change_count(dfs["edh"])

    df = dfs["employee"].alias("e") \
        .join(df_edh_active.alias("edh"),
              F.col("e.businessentityid") == F.col("edh.businessentityid"), "inner") \
        .join(df_dept_valid.alias("d"),
              F.col("edh.departmentid") == F.col("d.departmentid"), "inner") \
        .join(df_latest_pay.alias("pay"),
              F.col("e.businessentityid") == F.col("pay.pay_emp_id"), "left") \
        .join(df_hist_count.alias("h"),
              F.col("e.businessentityid") == F.col("h.businessentityid"), "left") \
        .select(
            F.col("e.businessentityid").alias("emp_id"),
            F.col("e.jobtitle").alias("job_title"),
            F.col("d.dept_name"),
            F.col("e.hiredate").alias("hire_date"),
            F.col("pay.latest_pay_rate"),
            F.col("h.dept_history_count"),
            F.col("e.gender"),
            F.col("e.maritalstatus").alias("marital_status"),
        )

    return df

def transform(dfs: dict) -> dict:
    logger.info("=== TRANSFORM ===")

    df_enriched = enrich(dfs)

    df_result = df_enriched \
        .withColumn("tenure_years",
            F.round(F.months_between(F.current_date(), F.col("hire_date")) / 12, 2)) \
        .withColumn("is_mover",
            F.col("dept_history_count") > 1) \
        .withColumn("dept_change_count",
            F.greatest(F.col("dept_history_count") - 1, F.lit(0)))

    # Verify department filter held
    depts = [r[0] for r in df_result.select("dept_name").distinct().collect()]
    logger.info("Departments in result: %s", depts)
    assert all(d in VALID_DEPARTMENTS for d in depts), \
        f"Invalid department found: {set(depts) - set(VALID_DEPARTMENTS)}"

    logger.info("Transform complete | rows=%d", df_result.count())
    return {"fact_hr_workforce": df_result}

# ─── LOAD ─────────────────────────────────────────────────────────────────────

def load(results: dict, config: dict):
    db   = config["hive"]["curated_database"]
    mode = config["pipeline"]["write_mode"]
    logger.info("=== LOAD | database=%s ===", db)

    validate(results["fact_hr_workforce"], "fact_hr_workforce")
    write_hive_table(results["fact_hr_workforce"], db, "fact_hr_workforce", mode=mode)

    logger.info("fact_hr_workforce written to Hive")


# ─── ANALYTICS (logged, not printed) ──────────────────────────────────────────

def run_analytics(spark: SparkSession, config: dict):
    """Run the 3 mandatory business queries and log results."""
    db = config["hive"]["curated_database"]
    logger.info("=== ANALYTICS ===")

    # BQ1: Average tenure per department
    logger.info("BQ1: Avg tenure per department")
    spark.sql(f"""
        SELECT department_name,
               COUNT(employee_id)              AS headcount,
               ROUND(AVG(tenure_years), 2)     AS avg_tenure_years,
               ROUND(AVG(latest_pay_rate), 2)  AS avg_pay_rate
        FROM {db}.fact_hr_workforce
        GROUP BY department_name
        ORDER BY avg_tenure_years DESC
    """).show()

    # BQ2: Correlation proxy — avg tenure per pay bracket
    logger.info("BQ2: Avg tenure per pay bracket")
    spark.sql(f"""
        SELECT pay_bracket,
               COUNT(employee_id)              AS headcount,
               ROUND(AVG(tenure_years), 2)     AS avg_tenure_years,
               ROUND(AVG(latest_pay_rate), 2)  AS avg_pay_rate
        FROM {db}.fact_hr_workforce
        GROUP BY pay_bracket
        ORDER BY avg_pay_rate
    """).show()

    # BQ3: % movers per department
    logger.info("BQ3: Mover percentage per department")
    spark.sql(f"""
        SELECT department_name,
               COUNT(employee_id)                                          AS total,
               SUM(CASE WHEN is_mover THEN 1 ELSE 0 END)                  AS movers,
               ROUND(SUM(CASE WHEN is_mover THEN 1 ELSE 0 END)
                     / COUNT(employee_id) * 100, 1)                        AS mover_pct
        FROM {db}.fact_hr_workforce
        GROUP BY department_name
        ORDER BY mover_pct DESC
    """).show()


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HR Attrition Pipeline")
    parser.add_argument("--config",    required=True)
    parser.add_argument("--analytics", action="store_true",
                        help="Run analytic queries after load")
    args = parser.parse_args()

    config = load_config(args.config)
    spark  = get_spark_with_hive("HRPipeline", config.get("spark", {}))
    logger.info("Pipeline started | config=%s", args.config)

    try:
        dfs     = extract(spark, config)
        results = transform(dfs)
        load(results, config)
        if args.analytics:
            run_analytics(spark, config)
        logger.info("Pipeline complete")
    except SystemExit:
        raise
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
