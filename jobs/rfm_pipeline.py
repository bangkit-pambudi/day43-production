"""
jobs/rfm_pipeline.py
====================
Pipeline: Hive (adventureworks.*)  →  Spark Transform  →  Hive Curated

Business Case C — Customer RFM Segmentation

RFM Framework:
  R (Recency)   = days since last order     (lower  = better)
  F (Frequency) = number of orders          (higher = better)
  M (Monetary)  = total spending (totaldue) (higher = better)

Scoring (ntile quartile 1–4):
  R-score: INVERTED — small recency gets score 4
  F-score: normal   — high frequency gets score 4
  M-score: normal   — high monetary gets score 4

Segmentation rules (evaluated in priority order):
  Champion       : R>=3 AND F>=3 AND M>=3
  Loyal          : F>=3 AND M>=3
  Potential Loyal: R>=3 AND F>=2
  At Risk        : R<=2 AND F>=3 AND M>=3
  Lost           : R=1  AND F<=2
  New Customer   : R=4  AND F=1
  Others         : everything else

Reference date: configurable via pipeline.yaml (default: max order date in dataset)

Target table: adventureworks_curated.fact_customer_rfm

Usage:
  spark-submit --jars jars/postgresql-42.7.3.jar \\
      jobs/rfm_pipeline.py --config configs/pipeline.yaml
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


# ─── EXTRACT ──────────────────────────────────────────────────────────────────

def extract(spark: SparkSession, config: dict) -> dict:
    db = config["hive"]["raw_database"]
    logger.info("=== EXTRACT | database=%s ===", db)
    return {
        "orders":    read_hive(spark, db, "fact_sales_orders"),
        "customer":  read_hive(spark, db, "dim_customer"),
        "person":    read_hive(spark, db, "dim_person"),
        "territory": read_hive(spark, db, "dim_territory"),
    }


# ─── TRANSFORM ────────────────────────────────────────────────────────────────

def compute_rfm_raw(df_orders, reference_date: str):
    """
    Aggregate orders per customer to get raw R, F, M values.
    reference_date: ISO string 'YYYY-MM-DD' used as 'today' for recency calculation.
    Using dataset max date (2014-07-31) rather than current_date() so results
    are reproducible on historical AdventureWorks data.
    """
    logger.info("Computing raw RFM values | reference_date=%s", reference_date)

    df = df_orders.groupBy("customerid").agg(
        F.datediff(
            F.lit(reference_date).cast("date"),
            F.max("orderdate")
        ).alias("recency_days"),
        F.count("salesorderid").alias("frequency"),
        F.round(F.sum("totaldue"), 2).alias("monetary"),
        F.max("orderdate").alias("last_order_date"),
    )

    monetary_perc = Window.orderBy("monetary")
    df = df.withColumn("monetary_percentile", F.percent_rank().over(monetary_perc))

    logger.info("RFM raw | customers=%d", df.count())
    return df


def score_rfm(df_rfm):
    """
    Apply ntile(4) quartile scoring.
    R-score is inverted: small recency (recent buyer) → score 4.
    """
    logger.info("Scoring RFM with ntile(4)")

    # Window specs — no partition; global quartile across all customers
    win_r = Window.orderBy(F.asc("recency_days"))  # desc → ntile 1 = smallest recency
    win_f = Window.orderBy(F.asc("frequency"))
    win_m = Window.orderBy(F.asc("monetary"))

    df = df_rfm \
        .withColumn("r_raw",   F.ntile(4).over(win_r)) \
        .withColumn("f_score", F.ntile(4).over(win_f)) \
        .withColumn("m_score", F.ntile(4).over(win_m)) \
        .withColumn("r_score", F.lit(5) - F.col("r_raw")) \
        .drop("r_raw") \
        .withColumn("rfm_score",
            F.concat(
                F.col("r_score").cast("string"),
                F.col("f_score").cast("string"),
                F.col("m_score").cast("string"),
            ))

    logger.info("Scoring complete | customers=%d", df.count())
    return df


def assign_segments(df_scored):
    """
    Assign RFM segment labels based on R, F, M score combinations.
    Rules evaluated in priority order — first match wins.
    """
    logger.info("Assigning RFM segments")

    r = F.col("r_score")
    f = F.col("f_score")
    m = F.col("m_score")
    rfm = F.col("rfm_score")

    df = df_scored.withColumn(
        "rfm_segment",
        F.when((r >= 3) & (f >= 3) & (m >= 3),
                F.when(rfm == "444", "Gold Champion")
                 .when(rfm.startswith("4"), "Silver Champion")
                 .otherwise("Bronze Champion"))
         .when((r <= 2) & (f >= 3) & (m >= 3),               "At Risk")
         .when((f >= 3) & (m >= 3),                          "Loyal")
         .when((r >= 3) & (f >= 2),                          "Potential Loyal")
         .when((r == 1) & (f <= 2),                          "Lost")
         .when((r == 4) & (f == 1),                          "New Customer")
         .otherwise("Others")
    )

    # Log segment distribution
    logger.info("Segment distribution:")
    for row in df.groupBy("rfm_segment").count().orderBy(F.desc("count")).collect():
        logger.info("  %-20s : %d", row["rfm_segment"], row["count"])

    return df


def enrich_with_customer_info(df_segmented, dfs: dict):
    """Join RFM results with customer name and territory."""
    logger.info("Enriching RFM with customer dimension")

    customer_slim = dfs["customer"].select(
        "customerid", "personid", "territoryid"
    )
    person_slim = dfs["person"].select(
        F.col("businessentityid").alias("personid"),
        F.concat_ws(" ", "firstname", "lastname").alias("customer_name"),
    )
    territory_slim = dfs["territory"].select(
        "territoryid",
        F.col("name").alias("territory_name"),
    )

    df = df_segmented \
        .join(customer_slim,  "customerid", "left") \
        .join(person_slim,    "personid",   "left") \
        .join(F.broadcast(territory_slim), "territoryid", "left") \
        .select(
            "customerid", "customer_name", "territory_name",
            "last_order_date", "recency_days", "frequency", "monetary",
            "r_score", "f_score", "m_score", "rfm_score", "rfm_segment",
        )

    logger.info("Enriched RFM rows: %d", df.count())
    return df


def transform(dfs: dict, config: dict) -> dict:
    logger.info("=== TRANSFORM ===")

    # Use configured reference date, fallback to dataset max date
    reference_date = config["pipeline"].get("reference_date", "2014-07-31")

    df_rfm_raw   = compute_rfm_raw(dfs["orders"], reference_date)
    df_scored    = score_rfm(df_rfm_raw)
    df_segmented = assign_segments(df_scored)
    df_enriched  = enrich_with_customer_info(df_segmented, dfs)

    df_final = df_enriched.withColumn("load_timestamp", F.current_timestamp()) \
                          .withColumn("segment_date", F.current_date())

    logger.info("Transform complete | rows=%d", df_final.count())
    return {"fact_customer_rfm": df_final}


# ─── LOAD ─────────────────────────────────────────────────────────────────────

def load(results: dict, config: dict):
    db   = config["hive"]["curated_database"]
    mode = config["pipeline"]["write_mode"]
    logger.info("=== LOAD | database=%s ===", db)

    validate(results["fact_customer_rfm"], "fact_customer_rfm")
    write_hive_table(results["fact_customer_rfm"], db, "fact_customer_rfm", mode=mode)
    logger.info("fact_customer_rfm written to Hive")


# ─── ANALYTICS ────────────────────────────────────────────────────────────────

def run_analytics(spark: SparkSession, config: dict):
    db = config["hive"]["curated_database"]
    logger.info("=== ANALYTICS ===")

    # BQ1: Top 20 Champion customers
    logger.info("BQ1: Top 20 Champion customers by monetary")
    spark.sql(f"""
        SELECT customer_name, territory_name,
               recency_days, frequency, monetary, rfm_score,
               rfm_segment, segment_date,
               round(monetary_percentile, 2) monetary_pct
        FROM {db}.fact_customer_rfm
        WHERE rfm_segment LIKE '%Champion%'
        ORDER BY monetary DESC
        LIMIT 20
    """).show(truncate=False)

    # BQ2: Segment distribution + revenue contribution
    logger.info("BQ2: Segment distribution & revenue share")
    spark.sql(f"""
        SELECT rfm_segment,
               COUNT(customerid)                         AS customer_count,
               ROUND(COUNT(customerid) /
                   SUM(COUNT(customerid)) OVER() * 100, 1) AS pct_customers,
               ROUND(SUM(monetary), 2)                   AS total_revenue,
               ROUND(SUM(monetary) /
                   SUM(SUM(monetary)) OVER() * 100, 1)   AS pct_revenue
        FROM {db}.fact_customer_rfm
        GROUP BY rfm_segment
        ORDER BY total_revenue DESC
    """).show()

    # BQ3: At Risk & Lost per territory
    logger.info("BQ3: At Risk & Lost customers per territory")
    spark.sql(f"""
        SELECT territory_name, rfm_segment,
               COUNT(customerid)              AS customer_count,
               ROUND(AVG(monetary), 2)        AS avg_monetary,
               ROUND(AVG(recency_days), 0)    AS avg_recency_days
        FROM {db}.fact_customer_rfm
        WHERE rfm_segment IN ('At Risk', 'Lost')
        GROUP BY territory_name, rfm_segment
        ORDER BY territory_name, rfm_segment
    """).show(30, truncate=False)

    # BQ4: Avg RFM profile per segment
    logger.info("BQ4: Average RFM metrics per segment")
    spark.sql(f"""
        SELECT rfm_segment,
               COUNT(customerid)              AS customers,
               ROUND(AVG(recency_days), 1)     AS avg_recency,
               ROUND(AVG(frequency), 1)        AS avg_frequency,
               ROUND(AVG(monetary), 2)         AS avg_monetary
        FROM {db}.fact_customer_rfm
        GROUP BY rfm_segment
        ORDER BY avg_monetary DESC
    """).show()


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Customer RFM Pipeline")
    parser.add_argument("--config",    required=True)
    parser.add_argument("--analytics", action="store_true")
    parser.add_argument("--referance-date", type=str, help="Override reference date (YYYY-MM-DD)")
    args = parser.parse_args()

    config = load_config(args.config)
    if args.referance_date:
        if "pipeline" not in config:
            config["pipeline"] = {}
        config["pipeline"]["referance_date"] = args.referance_date

    spark  = get_spark_with_hive("RFMPipeline", config.get("spark", {}))
    logger.info("Pipeline started | config=%s", args.config)

    try:
        dfs     = extract(spark, config)
        results = transform(dfs, config)
        load(results, config)
        if args.analytics:
            run_analytics(spark, config)

        print("\n===== RFM SEGMENT DISTRIBUTION =====")
        spark.table("adventureworks_curated.fact_customer_rfm") \
             .groupBy("rfm_segment").count().orderBy("count", ascending=False).show()

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
