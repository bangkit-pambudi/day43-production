"""
jobs/sales_pipeline.py
======================
Pipeline: Hive External Tables  →  Spark transform  →  Hive Curated Layer

Reads from the adventureworks Hive database built by hive_to_parquet.py.
Produces adventureworks_curated.fact_sales_performance.

Business questions answered:
  1. Top products by revenue per territory per year
  2. Monthly revenue trend with MoM growth
  3. Product margin analysis (list_price vs standard_cost)
  4. Category revenue share per year

Usage:
  spark-submit --jars jars/postgresql-42.7.3.jar \\
      jobs/sales_pipeline.py --config configs/pipeline.yaml

  # Individual steps
  spark-submit ... jobs/sales_pipeline.py --config configs/pipeline.yaml --step extract
  spark-submit ... jobs/sales_pipeline.py --config configs/pipeline.yaml --step transform
  spark-submit ... jobs/sales_pipeline.py --config configs/pipeline.yaml --step load
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
from utils.transforms import (
    add_margin_columns,
    add_mom_growth,
    add_yoy_growth,
    add_cumulative_total,
    add_revenue_share,
    rank_within_group,
)

logger = get_logger(__name__)


# ─── STEP 1: EXTRACT ──────────────────────────────────────────────────────────

def extract(spark: SparkSession, config: dict) -> dict:
    """Load all required tables from the Hive adventureworks database."""
    db = config["hive"]["raw_database"]
    logger.info("=== STEP 1: Extract from Hive | database=%s ===", db)

    dfs = {
        "orders":    read_hive(spark, db, "fact_sales_orders"),
        "detail":    read_hive(spark, db, "fact_order_details"),
        "product":   read_hive(spark, db, "dim_product"),
        "subcat":    read_hive(spark, db, "dim_product_subcat"),
        "category":  read_hive(spark, db, "dim_product_category"),
        "territory": read_hive(spark, db, "dim_territory"),
    }
    logger.info("Extract complete | tables=%d", len(dfs))
    return dfs


# ─── STEP 2: TRANSFORM ────────────────────────────────────────────────────────

def enrich_sales_detail(dfs: dict):
    """
    Join fact_order_details with all dimensions.
    Result: one row per order line item, enriched with product + territory + margin.

    Join chain:
      fact_order_details
        INNER JOIN fact_sales_orders  ON salesorderid
        INNER JOIN dim_product        ON productid
        LEFT  JOIN dim_product_subcat ON productsubcategoryid
        LEFT  JOIN dim_product_cat    ON productcategoryid
        LEFT  JOIN dim_territory      ON territoryid  (broadcast — 10 rows)
    """
    logger.info("Enriching sales detail with dimensions")

    orders_slim = dfs["orders"].select(
        "salesorderid", "territoryid", "order_year", "order_month"
    )
    product_slim = dfs["product"].select(
        "productid",
        F.col("name").alias("product_name"),
        "listprice", "standardcost", "productsubcategoryid"
    )
    subcat_slim = dfs["subcat"].select(
        F.col("productsubcategoryid").alias("sub_id"),
        F.col("name").alias("subcategory"),
        F.col("productcategoryid").alias("cat_id")
    )
    cat_slim = dfs["category"].select(
        F.col("productcategoryid").alias("cat_id2"),
        F.col("name").alias("category")
    )
    territory_slim = dfs["territory"].select(
        "territoryid",
        F.col("name").alias("territory_name"),
        F.col("countryregioncode").alias("country_code")
    )

    df = dfs["detail"].alias("od") \
        .join(orders_slim.alias("oh"),
              F.col("od.salesorderid") == F.col("oh.salesorderid"), "inner") \
        .join(product_slim.alias("p"),
              F.col("od.productid") == F.col("p.productid"), "inner") \
        .join(subcat_slim.alias("ps"),
              F.col("p.productsubcategoryid") == F.col("ps.sub_id"), "left") \
        .join(cat_slim.alias("pc"),
              F.col("ps.cat_id") == F.col("pc.cat_id2"), "left") \
        .join(F.broadcast(territory_slim).alias("t"),
              F.col("oh.territoryid") == F.col("t.territoryid"), "left") \
        .select(
            F.col("od.salesorderid"),
            F.col("od.productid"),
            F.col("od.orderqty"),
            F.col("od.unitprice"),
            F.col("od.linetotal"),
            F.col("oh.territoryid"),
            F.col("oh.order_year"),
            F.col("oh.order_month"),
            F.col("p.product_name"),
            F.col("p.listprice"),
            F.col("p.standardcost"),
            F.col("ps.subcategory"),
            F.col("pc.category"),
            F.col("t.territory_name"),
            F.col("t.country_code"),
        )

    # Add margin columns — null-safe via F.when()
    df = add_margin_columns(df, list_price_col="listprice", cost_col="standardcost")

    logger.info("Enrich complete | rows=%d", df.count())
    return df


def aggregate_performance(df_enriched):
    """
    Aggregate to grain: (product, territory, year).
    Produces the fact_sales_performance gold table.
    """
    logger.info("Aggregating sales performance")

    df_perf = df_enriched.groupBy(
        "productid", "product_name", "category", "subcategory",
        "territoryid", "territory_name", "country_code", "order_year"
    ).agg(
        F.round(F.sum("linetotal"), 2).alias("total_revenue"),
        F.sum("orderqty").alias("total_qty"),
        F.round(F.avg("unitprice"), 2).alias("avg_unit_price"),
        F.round(F.first("listprice"), 2).alias("list_price"),
        F.round(F.first("standardcost"), 2).alias("standard_cost"),
        F.round(F.first("margin"), 2).alias("margin"),
        F.round(F.first("margin_pct"), 2).alias("margin_pct"),
    ).withColumnRenamed("productid", "product_id") \
     .withColumnRenamed("territoryid", "territory_id") \
     .withColumn("load_timestamp", F.current_timestamp())

    logger.info("Aggregation complete | rows=%d", df_perf.count())
    return df_perf


def build_monthly_trend(dfs: dict):
    """
    Monthly revenue trend with cumulative total and MoM growth.
    Grain: (order_year, order_month)
    """
    logger.info("Building monthly revenue trend")

    df_monthly = dfs["orders"] \
        .groupBy("order_year", "order_month") \
        .agg(
            F.count("salesorderid").alias("total_orders"),
            F.countDistinct("customerid").alias("unique_customers"),
            F.round(F.sum("totaldue"), 2).alias("monthly_revenue"),
            F.round(F.avg("totaldue"), 2).alias("avg_order_value"),
        )

    df_monthly = add_cumulative_total(
        df_monthly, "monthly_revenue", "order_year", "order_month", "cumulative_revenue"
    )
    df_monthly = add_revenue_share(
        df_monthly, "monthly_revenue", "order_year", "monthly_revenue_share_pct"
    )
    df_monthly = add_mom_growth(
        df_monthly, "monthly_revenue", "order_year", "order_month"
    )
    df_monthly = df_monthly.withColumn("load_timestamp", F.current_timestamp())

    logger.info("Monthly trend complete | rows=%d", df_monthly.count())
    return df_monthly


def build_territory_yoy(df_perf):
    """
    Territory-level yearly revenue with YoY growth.
    Grain: (territory_name, order_year)
    """
    logger.info("Building territory YoY growth")

    df_terr = df_perf.groupBy("territory_name", "country_code", "order_year") \
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("yearly_revenue"),
            F.sum("total_qty").alias("yearly_qty"),
        )

    df_terr = add_yoy_growth(df_terr, "yearly_revenue", "territory_name")
    df_terr = df_terr.withColumn("load_timestamp", F.current_timestamp())

    logger.info("Territory YoY complete | rows=%d", df_terr.count())
    return df_terr


def build_top_products(df_perf, top_n: int = 5):
    """Top N products per territory per year by revenue."""
    logger.info("Building top %d products per territory per year", top_n)
    df = rank_within_group(
        df_perf, "total_revenue",
        partition_cols=["order_year", "territory_name"],
        top_n=top_n,
    )
    logger.info("Top products complete | rows=%d", df.count())
    return df


def transform(dfs: dict, config: dict) -> dict:
    """Orchestrate all transformation steps. Returns dict of result DataFrames."""
    logger.info("=== STEP 2: Transform ===")
    top_n = config["pipeline"].get("top_n_products", 5)

    df_enriched    = enrich_sales_detail(dfs)
    df_perf        = aggregate_performance(df_enriched)
    df_monthly     = build_monthly_trend(dfs)
    df_terr_yoy    = build_territory_yoy(df_perf)
    df_top_products = build_top_products(df_perf, top_n)

    return {
        "fact_sales_performance": df_perf,
        "monthly_sales_summary":  df_monthly,
        "territory_yoy":          df_terr_yoy,
        "top_products":           df_top_products,
    }


# ─── STEP 3: LOAD ─────────────────────────────────────────────────────────────

def load(results: dict, config: dict):
    """Validate and write all result DataFrames to Hive curated database."""
    db   = config["hive"]["curated_database"]
    mode = config["pipeline"]["write_mode"]

    logger.info("=== STEP 3: Load to Hive | database=%s ===", db)

    # fact_sales_performance — partitioned by order_year
    validate(results["fact_sales_performance"], "fact_sales_performance")
    write_hive_table(
        results["fact_sales_performance"], db, "fact_sales_performance",
        partition_by=["order_year"], mode=mode
    )

    # monthly_sales_summary — partitioned by order_year
    validate(results["monthly_sales_summary"], "monthly_sales_summary")
    write_hive_table(
        results["monthly_sales_summary"], db, "monthly_sales_summary",
        partition_by=["order_year"], mode=mode
    )

    # territory_yoy — no partition (small table)
    validate(results["territory_yoy"], "territory_yoy")
    write_hive_table(results["territory_yoy"], db, "territory_yoy", mode=mode)

    # top_products — partitioned by order_year
    validate(results["top_products"], "top_products")
    write_hive_table(
        results["top_products"], db, "top_products",
        partition_by=["order_year"], mode=mode
    )

    logger.info("All curated tables written to Hive database: %s", db)

    # Final summary
    spark_active = SparkSession.getActiveSession()
    if spark_active:
        spark_active.sql(f"SHOW TABLES IN {db}").show(truncate=False)


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sales Performance Pipeline: Hive → Transform → Curated Hive"
    )
    parser.add_argument("--config", required=True,
                        help="Path to pipeline YAML config")
    parser.add_argument("--step", default="all",
                        choices=["all", "extract", "transform", "load"],
                        help="Pipeline step to run (default: all)")
    args = parser.parse_args()

    config = load_config(args.config)
    spark  = get_spark_with_hive("SalesPipeline", config.get("spark", {}))

    logger.info("Pipeline started | step=%s | config=%s", args.step, args.config)

    try:
        dfs     = extract(spark, config)
        results = transform(dfs, config)
        load(results, config)
        logger.info("Pipeline complete | step=%s", args.step)

    except SystemExit:
        raise
    except Exception as e:
        logger.error("Unexpected error | step=%s | error=%s", args.step, e)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
