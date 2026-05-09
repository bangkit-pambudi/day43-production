"""
jobs/vendor_pipeline.py
=======================
Pipeline: Hive (adventureworks.*)  →  Spark Transform  →  Hive Curated

Business Case B — Vendor Performance & Procurement Analytics

Rules:
  - ONLY ship methods: CARGO TRANSPORT 5 and OVERNIGHT J-FAST
  - on_time_orders     = ShipDate <= DueDate AND ShipDate IS NOT NULL
  - on_time_rate       = on_time_orders / total_orders * 100
  - avg_lead_time_days = AVG(datediff(ShipDate, OrderDate)) where ShipDate IS NOT NULL
  - avg_price_variance = AVG(UnitPrice - StandardCost) from PO detail
  - VendorScore        = (OnTimeRate * 0.6) + ((100 - ABS(AvgPriceVariance)) * 0.4)

Target table: adventureworks_curated.fact_vendor_performance

Usage:
  spark-submit --jars jars/postgresql-42.7.3.jar \\
      jobs/vendor_pipeline.py --config configs/pipeline.yaml
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

VALID_SHIP_METHODS = ["CARGO TRANSPORT 5", "OVERNIGHT J-FAST"]


# ─── EXTRACT ──────────────────────────────────────────────────────────────────

def extract(spark: SparkSession, config: dict) -> dict:
    db = config["hive"]["raw_database"]
    logger.info("=== EXTRACT | database=%s ===", db)
    return {
        "po_header":  read_hive(spark, db, "fact_purchase_orders"),
        "po_detail":  read_hive(spark, db, "fact_purchase_details"),
        "vendor":     read_hive(spark, db, "dim_vendor"),
        "shipmethod": read_hive(spark, db, "dim_ship_method"),
        "product":    read_hive(spark, db, "dim_product"),
    }


# ─── TRANSFORM ────────────────────────────────────────────────────────────────

def filter_ship_methods(df_shipmethod):
    """Keep only CARGO TRANSPORT 5 and OVERNIGHT J-FAST."""
    df = df_shipmethod \
        .filter(F.col("name").isin(VALID_SHIP_METHODS)) \
        .select("shipmethodid", F.col("name").alias("ship_method"))
    logger.info("Valid ship methods: %d", df.count())
    return df


def enrich(dfs: dict):
    """
    Join flow:
      fact_purchase_orders (filtered by ship method)
        INNER JOIN dim_ship_method (filtered)  ON shipmethodid
        INNER JOIN dim_vendor                  ON vendorid == businessentityid
        INNER JOIN fact_purchase_details       ON purchaseorderid
        LEFT  JOIN dim_product                 ON productid  (for standardcost)

    Adds:
      - is_on_time      : ShipDate <= DueDate AND ShipDate IS NOT NULL  (1/0)
      - lead_time_days  : datediff(ShipDate, OrderDate) when ShipDate not null
      - price_variance  : UnitPrice (detail) - StandardCost (product)
    """
    logger.info("Enriching vendor data")

    df_methods = filter_ship_methods(dfs["shipmethod"])

    vendor_slim = dfs["vendor"].select(
        F.col("businessentityid").alias("vendor_bid"),
        F.col("name").alias("vendor_name"),
        F.col("creditrating").alias("credit_rating"),
    )

    product_slim = dfs["product"].select(
        "productid",
        F.col("standardcost").alias("standard_cost"),
    )

    # Header filtered by ship method
    df_header = dfs["po_header"].alias("h") \
        .join(F.broadcast(df_methods).alias("sm"),
              F.col("h.shipmethodid") == F.col("sm.shipmethodid"), "inner") \
        .select(
            F.col("h.purchaseorderid"),
            F.col("h.vendorid"),
            F.col("h.orderdate"),
            F.col("h.shipdate"),
            F.col("h.duedate"),
            F.col("sm.ship_method"),
        )

    # Join detail + product for price variance
    df_detail = dfs["po_detail"].alias("d") \
        .join(product_slim.alias("p"),
              F.col("d.productid") == F.col("p.productid"), "left") \
        .select(
            F.col("d.purchaseorderid"),
            F.col("d.productid"),
            F.col("d.unitprice"),
            F.col("p.standard_cost"),
            (F.col("d.unitprice") - F.col("p.standard_cost")).alias("price_variance"),
        )

    df = df_header.alias("h") \
        .join(vendor_slim.alias("v"),
              F.col("h.vendorid") == F.col("v.vendor_bid"), "inner") \
        .join(df_detail.alias("d"), "purchaseorderid", "inner") \
        .withColumn("is_on_time",
            F.when(
                F.col("h.shipdate").isNotNull() & (F.col("h.shipdate") <= F.col("h.duedate")),
                F.lit(1)
            ).otherwise(F.lit(0))) \
        .withColumn("lead_time_days",
            F.when(
                F.col("h.shipdate").isNotNull(),
                F.datediff(F.col("h.shipdate"), F.col("h.orderdate"))
            ).otherwise(None)) \
        .select(
            F.col("h.purchaseorderid"),
            F.col("h.vendorid"),
            F.col("v.vendor_name"),
            F.col("v.credit_rating"),
            F.col("h.ship_method"),
            F.col("is_on_time"),
            F.col("lead_time_days"),
            F.col("d.price_variance"),
        )

    logger.info("Enriched rows: %d", df.count())
    return df


def aggregate_vendor_performance(df_enriched):
    """
    Grain: (vendor_id, vendor_name, credit_rating, ship_method)
    Computes on_time_rate, avg_lead_time, avg_price_variance, and VendorScore.
    """
    logger.info("Aggregating vendor performance")

    df_agg = df_enriched.groupBy(
        "vendorid", "vendor_name", "credit_rating", "ship_method"
    ).agg(
        F.count("purchaseorderid").alias("total_orders"),
        F.sum("is_on_time").alias("on_time_orders"),
        F.round(F.avg(
            F.when(F.col("lead_time_days").isNotNull(), F.col("lead_time_days"))
        ), 1).alias("avg_lead_time_days"),
        F.round(F.avg("price_variance"), 2).alias("avg_price_variance"),
    ).withColumn("on_time_rate",
        F.round(F.col("on_time_orders") / F.col("total_orders") * 100, 2)
    ).withColumn("vendor_score",
        F.round(
            (F.col("on_time_rate") * 0.6) +
            ((100 - F.abs(F.col("avg_price_variance"))) * 0.4),
        2)
    ).withColumnRenamed("vendorid", "vendor_id")

    logger.info("Vendor summary rows: %d", df_agg.count())
    return df_agg


def build_overall_ranking(df_perf):
    """Overall vendor ranking aggregated across ship methods."""
    win_overall = Window.orderBy(F.desc("avg_vendor_score"))
    return df_perf.groupBy("vendor_id", "vendor_name", "credit_rating") \
        .agg(
            F.round(F.avg("vendor_score"), 2).alias("avg_vendor_score"),
            F.round(F.avg("on_time_rate"), 2).alias("avg_on_time_rate"),
            F.sum("total_orders").alias("total_orders"),
        ) \
        .withColumn("overall_rank", F.rank().over(win_overall))


def rank_within_ship_method(df_perf):
    """Rank vendors within each ship method by vendor_score."""
    win = Window.partitionBy("ship_method").orderBy(F.desc("vendor_score"))
    return df_perf.withColumn("rank_in_method", F.rank().over(win))


def transform(dfs: dict) -> dict:
    logger.info("=== TRANSFORM ===")

    df_enriched      = enrich(dfs)
    df_perf          = aggregate_vendor_performance(df_enriched)
    df_ranked        = rank_within_ship_method(df_perf)
    df_overall       = build_overall_ranking(df_perf)

    df_final = df_ranked.withColumn("load_timestamp", F.current_timestamp())

    # Verify ship method filter held
    methods = [r[0] for r in df_final.select("ship_method").distinct().collect()]
    logger.info("Ship methods in result: %s", methods)
    assert all(m in VALID_SHIP_METHODS for m in methods), \
        f"Invalid ship method: {set(methods) - set(VALID_SHIP_METHODS)}"

    logger.info("Transform complete | perf_rows=%d | overall_rows=%d",
                df_final.count(), df_overall.count())
    return {
        "fact_vendor_performance": df_final,
        "vendor_overall_ranking":  df_overall,
    }


# ─── LOAD ─────────────────────────────────────────────────────────────────────

def load(results: dict, config: dict):
    db   = config["hive"]["curated_database"]
    mode = config["pipeline"]["write_mode"]
    logger.info("=== LOAD | database=%s ===", db)

    validate(results["fact_vendor_performance"], "fact_vendor_performance")
    write_hive_table(
        results["fact_vendor_performance"], db, "fact_vendor_performance", mode=mode
    )

    validate(results["vendor_overall_ranking"], "vendor_overall_ranking")
    write_hive_table(
        results["vendor_overall_ranking"], db, "vendor_overall_ranking", mode=mode
    )

    logger.info("Vendor tables written to Hive")


# ─── ANALYTICS ────────────────────────────────────────────────────────────────

def run_analytics(spark: SparkSession, config: dict):
    db = config["hive"]["curated_database"]
    logger.info("=== ANALYTICS ===")

    # BQ1: Vendor ranking by VendorScore
    logger.info("BQ1: Vendor ranking by VendorScore")
    spark.sql(f"""
        SELECT vendor_name, ship_method, on_time_rate,
               avg_price_variance, vendor_score, rank_in_method
        FROM {db}.fact_vendor_performance
        ORDER BY ship_method, rank_in_method
    """).show(20, truncate=False)

    # BQ2: OnTimeRate per ship method
    logger.info("BQ2: OnTimeRate per ship method")
    spark.sql(f"""
        SELECT ship_method,
               COUNT(vendor_id)                   AS total_vendors,
               ROUND(AVG(on_time_rate), 2)        AS avg_on_time_rate,
               SUM(total_orders)                  AS total_orders
        FROM {db}.fact_vendor_performance
        GROUP BY ship_method
        ORDER BY avg_on_time_rate DESC
    """).show()

    # BQ3: Vendors cheaper than StandardCost (negative price variance)
    logger.info("BQ3: Vendors with negative avg_price_variance")
    spark.sql(f"""
        SELECT vendor_name, ship_method, avg_price_variance, on_time_rate, vendor_score
        FROM {db}.fact_vendor_performance
        WHERE avg_price_variance < 0
        ORDER BY avg_price_variance ASC
    """).show(20, truncate=False)

    # BQ4: CreditRating vs OnTimeRate correlation proxy
    logger.info("BQ4: CreditRating vs OnTimeRate")
    spark.sql(f"""
        SELECT credit_rating,
               COUNT(vendor_id)              AS vendor_count,
               ROUND(AVG(on_time_rate), 2)  AS avg_on_time_rate,
               ROUND(AVG(vendor_score), 2)  AS avg_vendor_score
        FROM {db}.fact_vendor_performance
        GROUP BY credit_rating
        ORDER BY credit_rating
    """).show()


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Vendor Performance Pipeline")
    parser.add_argument("--config",    required=True)
    parser.add_argument("--analytics", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    spark  = get_spark_with_hive("VendorPipeline", config.get("spark", {}))
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
