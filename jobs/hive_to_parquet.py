"""
jobs/hive_to_parquet.py
=======================
Pipeline: AdventureWorks PostgreSQL  →  HDFS Parquet (raw layer)  →  Hive External Tables

Flow:
  1. extract_dimensions()  — JDBC read all dim tables  →  HDFS Parquet (no partition)
  2. extract_facts()       — JDBC read fact tables     →  HDFS Parquet (partitioned by year/month)
  3. register_hive_tables()— CREATE EXTERNAL TABLE in Hive Metastore + MSCK REPAIR
  4. verify()              — Row count check on every Hive table

Usage:
  # Full pipeline (extract + register + verify)
  spark-submit --jars jars/postgresql-42.7.3.jar jobs/hive_to_parquet.py --config configs/pipeline.yaml

  # Individual steps
  spark-submit ... jobs/hive_to_parquet.py --config configs/pipeline.yaml --step extract
  spark-submit ... jobs/hive_to_parquet.py --config configs/pipeline.yaml --step register
  spark-submit ... jobs/hive_to_parquet.py --config configs/pipeline.yaml --step verify
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.config_loader import load_config
from utils.logger import get_logger
from utils.spark_factory import get_spark_with_hive

logger = get_logger(__name__)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _jdbc_read(spark: SparkSession, jdbc_url: str, schema: str, table: str,
               user: str, password: str, driver: str,
               partition_column: str = None,
               lower_bound: int = None, upper_bound: int = None,
               num_partitions: int = 4):
    """Generic JDBC reader. Supports parallel reads via partition_column."""
    props = {"user": user, "password": password, "driver": driver}
    src = f'"{schema}"."{table}"'
    logger.info("JDBC read | source=%s", src)
    try:
        if partition_column and lower_bound is not None and upper_bound is not None:
            df = spark.read.jdbc(
                url=jdbc_url, table=src,
                column=partition_column,
                lowerBound=lower_bound, upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=props,
            )
        else:
            df = spark.read.jdbc(url=jdbc_url, table=src, properties=props)
        cnt = df.count()
        logger.info("JDBC read complete | source=%s | rows=%d", src, cnt)
        return df
    except Exception as e:
        logger.error("JDBC read failed | source=%s | error=%s", src, e)
        sys.exit(1)


def _write_parquet(df, path: str, partition_by: list = None, mode: str = "overwrite"):
    logger.info("Writing parquet | path=%s | partition_by=%s", path, partition_by)
    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
        logger.info("Parquet write complete | path=%s", path)
    except Exception as e:
        logger.error("Parquet write failed | path=%s | error=%s", path, e)
        sys.exit(1)


def _hive_ddl(spark: SparkSession, database: str, table: str, hdfs_path: str,
              partition_spec: str = None):
    """
    Create (or recreate) a Hive External Table pointing at an HDFS Parquet path.
    partition_spec example: "PARTITIONED BY (order_year INT, order_month INT)"
    """
    full = f"{database}.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {full}")

    if partition_spec:
        spark.sql(f"""
            CREATE EXTERNAL TABLE {full}
            {partition_spec}
            STORED AS PARQUET
            LOCATION '{hdfs_path}'
        """)
        spark.sql(f"MSCK REPAIR TABLE {full}")
    else:
        spark.sql(f"""
            CREATE EXTERNAL TABLE {full}
            USING PARQUET
            LOCATION '{hdfs_path}'
        """)

    cnt = spark.sql(f"SELECT COUNT(*) FROM {full}").collect()[0][0]
    logger.info("Hive table ready | table=%s | rows=%d", full, cnt)
    return cnt


# ─── STEP 1: EXTRACT DIMENSIONS ───────────────────────────────────────────────

def extract_dimensions(spark: SparkSession, config: dict):
    """
    Read every dimension table from PostgreSQL via JDBC.
    Write each as Parquet to HDFS raw layer.
    """
    src   = config["source"]
    hdfs  = config["hdfs"]["base"]
    mode  = config["pipeline"]["write_mode"]

    logger.info("=== STEP 1: Extract Dimension Tables ===")

    for entry in config["source"]["dim_tables"]:
        schema = entry["schema"]
        table  = entry["table"]

        df = _jdbc_read(
            spark, src["jdbc_url"], schema, table,
            src["user"], src["password"], src["driver"]
        )

        hdfs_path = f"{hdfs}/{schema}/{table}"
        _write_parquet(df, hdfs_path, mode=mode)

    logger.info("All dimension tables extracted to HDFS")


# ─── STEP 2: EXTRACT FACT TABLES ──────────────────────────────────────────────

def extract_facts(spark: SparkSession, config: dict):
    """
    Read fact tables from PostgreSQL with parallel JDBC reads.
    SalesOrderHeader  → partitioned by order_year / order_month
    SalesOrderDetail  → partitioned by order_year (joined with header for date)
    """
    src   = config["source"]
    hdfs  = config["hdfs"]["base"]
    mode  = config["pipeline"]["write_mode"]

    logger.info("=== STEP 2: Extract Fact Tables ===")

    # ── SalesOrderHeader ──────────────────────────────────────────────────────
    logger.info("Extracting SalesOrderHeader")
    fact_cfg = next(f for f in src["fact_tables"] if f["table"] == "SalesOrderHeader")

    df_orders = _jdbc_read(
        spark, src["jdbc_url"], "Sales", "SalesOrderHeader",
        src["user"], src["password"], src["driver"],
        partition_column=fact_cfg["partition_column"],
        lower_bound=fact_cfg["lower_bound"],
        upper_bound=fact_cfg["upper_bound"],
        num_partitions=fact_cfg["num_partitions"],
    )

    # Derive year / month partition columns from orderdate
    df_orders_partitioned = df_orders \
        .withColumn("order_year",  F.year(F.col("orderdate"))) \
        .withColumn("order_month", F.month(F.col("orderdate")))

    orders_path = f"{hdfs}/Sales/SalesOrderHeader"
    _write_parquet(df_orders_partitioned, orders_path,
                   partition_by=["order_year", "order_month"], mode=mode)

    # ── SalesOrderDetail ──────────────────────────────────────────────────────
    logger.info("Extracting SalesOrderDetail")
    detail_cfg = next(f for f in src["fact_tables"] if f["table"] == "SalesOrderDetail")

    df_detail = _jdbc_read(
        spark, src["jdbc_url"], "Sales", "SalesOrderDetail",
        src["user"], src["password"], src["driver"],
        partition_column=detail_cfg["partition_column"],
        lower_bound=detail_cfg["lower_bound"],
        upper_bound=detail_cfg["upper_bound"],
        num_partitions=detail_cfg["num_partitions"],
    )

    # Join with orders to get order_year for partitioning
    df_detail_year = df_detail \
        .join(
            df_orders_partitioned.select("salesorderid", "order_year"),
            on="salesorderid", how="inner"
        )

    detail_path = f"{hdfs}/Sales/SalesOrderDetail"
    _write_parquet(df_detail_year, detail_path, partition_by=["order_year"], mode=mode)

    # ── PurchaseOrderHeader ───────────────────────────────────────────────────
    logger.info("Extracting PurchaseOrderHeader")
    df_po_header = _jdbc_read(
        spark, src["jdbc_url"], "Purchasing", "PurchaseOrderHeader",
        src["user"], src["password"], src["driver"]
    )
    df_po_header = df_po_header \
        .withColumn("order_year",  F.year(F.col("orderdate"))) \
        .withColumn("order_month", F.month(F.col("orderdate")))
    _write_parquet(df_po_header,
                   f"{hdfs}/Purchasing/PurchaseOrderHeader",
                   partition_by=["order_year", "order_month"], mode=mode)

    # ── PurchaseOrderDetail ───────────────────────────────────────────────────
    logger.info("Extracting PurchaseOrderDetail")
    df_po_detail = _jdbc_read(
        spark, src["jdbc_url"], "Purchasing", "PurchaseOrderDetail",
        src["user"], src["password"], src["driver"]
    )
    df_po_detail = df_po_detail \
        .join(
            df_po_header.select("purchaseorderid", "order_year"),
            on="purchaseorderid", how="inner"
        )
    _write_parquet(df_po_detail,
                   f"{hdfs}/Purchasing/PurchaseOrderDetail",
                   partition_by=["order_year"], mode=mode)

    logger.info("All fact tables extracted to HDFS")


# ─── STEP 3: REGISTER HIVE EXTERNAL TABLES ────────────────────────────────────

def register_hive_tables(spark: SparkSession, config: dict):
    """
    Create Hive databases and register every HDFS Parquet path as an External Table.
    Uses MSCK REPAIR TABLE for partitioned tables to discover partition directories.
    """
    hdfs     = config["hdfs"]["base"]
    raw_db   = config["hive"]["raw_database"]          # adventureworks
    cur_db   = config["hive"]["curated_database"]      # adventureworks_curated

    logger.info("=== STEP 3: Register Hive External Tables ===")

    # Create databases
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {raw_db} COMMENT 'AdventureWorks Data Warehouse'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cur_db} COMMENT 'AdventureWorks Curated Layer'")
    logger.info("Hive databases ready: %s, %s", raw_db, cur_db)

    # ── Dimension tables (no partition) ──────────────────────────────────────
    dim_map = {
        "dim_product":          f"{hdfs}/Production/Product",
        "dim_product_category": f"{hdfs}/Production/ProductCategory",
        "dim_product_subcat":   f"{hdfs}/Production/ProductSubcategory",
        "dim_person":           f"{hdfs}/Person/Person",
        "dim_employee":         f"{hdfs}/HumanResources/Employee",
        "dim_department":       f"{hdfs}/HumanResources/Department",
        "dim_emp_dept_history": f"{hdfs}/HumanResources/EmployeeDepartmentHistory",
        "dim_emp_pay_history":  f"{hdfs}/HumanResources/EmployeePayHistory",
        "dim_customer":         f"{hdfs}/Sales/Customer",
        "dim_territory":        f"{hdfs}/Sales/SalesTerritory",
        "dim_vendor":           f"{hdfs}/Purchasing/Vendor",
        "dim_ship_method":      f"{hdfs}/Purchasing/ShipMethod",
    }
    for table_name, path in dim_map.items():
        _hive_ddl(spark, raw_db, table_name, path)

    # ── Fact tables (partitioned — need explicit DDL + MSCK REPAIR) ───────────

    # fact_sales_orders — partitioned by order_year / order_month
    spark.sql(f"DROP TABLE IF EXISTS {raw_db}.fact_sales_orders")
    spark.sql(f"""
        CREATE EXTERNAL TABLE {raw_db}.fact_sales_orders (
            salesorderid           INT,
            revisionnumber         SMALLINT,
            orderdate              TIMESTAMP,
            duedate                TIMESTAMP,
            shipdate               TIMESTAMP,
            status                 SMALLINT,
            onlineorderflag        BOOLEAN,
            salesordernumber       STRING,
            purchaseordernumber    STRING,
            accountnumber          STRING,
            customerid             INT,
            salespersonid          INT,
            territoryid            INT,
            billtoaddressid        INT,
            shiptoaddressid        INT,
            shipmethodid           INT,
            creditcardid           INT,
            creditcardapprovalcode STRING,
            currencyrateid         INT,
            subtotal               DECIMAL(19,4),
            taxamt                 DECIMAL(19,4),
            freight                DECIMAL(19,4),
            totaldue               DECIMAL(19,4),
            comment                STRING,
            rowguid                STRING,
            modifieddate           TIMESTAMP
        )
        PARTITIONED BY (order_year INT, order_month INT)
        STORED AS PARQUET
        LOCATION '{hdfs}/Sales/SalesOrderHeader'
    """)
    spark.sql(f"MSCK REPAIR TABLE {raw_db}.fact_sales_orders")
    cnt = spark.sql(f"SELECT COUNT(*) FROM {raw_db}.fact_sales_orders").collect()[0][0]
    logger.info("Hive table ready | table=%s.fact_sales_orders | rows=%d", raw_db, cnt)

    # fact_order_details — partitioned by order_year
    spark.sql(f"DROP TABLE IF EXISTS {raw_db}.fact_order_details")
    spark.sql(f"""
        CREATE EXTERNAL TABLE {raw_db}.fact_order_details (
            salesorderid          INT,
            salesorderdetailid    INT,
            carriertrackingnumber STRING,
            orderqty              SMALLINT,
            productid             INT,
            specialofferid        INT,
            unitprice             DECIMAL(19,4),
            unitpricediscount     DECIMAL(19,4),
            linetotal             DECIMAL(38,6),
            rowguid               STRING,
            modifieddate          TIMESTAMP
        )
        PARTITIONED BY (order_year INT)
        STORED AS PARQUET
        LOCATION '{hdfs}/Sales/SalesOrderDetail'
    """)
    spark.sql(f"MSCK REPAIR TABLE {raw_db}.fact_order_details")
    cnt = spark.sql(f"SELECT COUNT(*) FROM {raw_db}.fact_order_details").collect()[0][0]
    logger.info("Hive table ready | table=%s.fact_order_details | rows=%d", raw_db, cnt)

    # fact_purchase_orders — partitioned by order_year / order_month
    spark.sql(f"DROP TABLE IF EXISTS {raw_db}.fact_purchase_orders")
    spark.sql(f"""
        CREATE EXTERNAL TABLE {raw_db}.fact_purchase_orders (
            purchaseorderid   INT,
            revisionnumber    SMALLINT,
            status            SMALLINT,
            employeeid        INT,
            vendorid          INT,
            shipmethodid      INT,
            orderdate         TIMESTAMP,
            shipdate          TIMESTAMP,
            subtotal          DECIMAL(19,4),
            taxamt            DECIMAL(19,4),
            freight           DECIMAL(19,4),
            totaldue          DECIMAL(19,4),
            modifieddate      TIMESTAMP
        )
        PARTITIONED BY (order_year INT, order_month INT)
        STORED AS PARQUET
        LOCATION '{hdfs}/Purchasing/PurchaseOrderHeader'
    """)
    spark.sql(f"MSCK REPAIR TABLE {raw_db}.fact_purchase_orders")
    cnt = spark.sql(f"SELECT COUNT(*) FROM {raw_db}.fact_purchase_orders").collect()[0][0]
    logger.info("Hive table ready | table=%s.fact_purchase_orders | rows=%d", raw_db, cnt)

    # fact_purchase_details — partitioned by order_year
    spark.sql(f"DROP TABLE IF EXISTS {raw_db}.fact_purchase_details")
    spark.sql(f"""
        CREATE EXTERNAL TABLE {raw_db}.fact_purchase_details (
            purchaseorderid       INT,
            purchaseorderdetailid INT,
            duedate               TIMESTAMP,
            orderqty              SMALLINT,
            productid             INT,
            unitprice             DECIMAL(19,4),
            linetotal             DECIMAL(19,4),
            receivedqty           DECIMAL(8,2),
            rejectedqty           DECIMAL(8,2),
            stockedqty            DECIMAL(9,2),
            modifieddate          TIMESTAMP
        )
        PARTITIONED BY (order_year INT)
        STORED AS PARQUET
        LOCATION '{hdfs}/Purchasing/PurchaseOrderDetail'
    """)
    spark.sql(f"MSCK REPAIR TABLE {raw_db}.fact_purchase_details")
    cnt = spark.sql(f"SELECT COUNT(*) FROM {raw_db}.fact_purchase_details").collect()[0][0]
    logger.info("Hive table ready | table=%s.fact_purchase_details | rows=%d", raw_db, cnt)

    logger.info("All Hive External Tables registered in database: %s", raw_db)


# ─── STEP 4: VERIFY ───────────────────────────────────────────────────────────

def verify(spark: SparkSession, config: dict):
    """
    Row-count verification on every registered Hive table.
    Fails fast if any table returns 0 rows.
    """
    raw_db = config["hive"]["raw_database"]

    logger.info("=== STEP 4: Verification ===")

    tables = [
        "dim_product", "dim_product_category", "dim_product_subcat",
        "dim_person", "dim_employee", "dim_department",
        "dim_emp_dept_history", "dim_emp_pay_history",
        "dim_customer", "dim_territory",
        "dim_vendor", "dim_ship_method",
        "fact_sales_orders", "fact_order_details",
        "fact_purchase_orders", "fact_purchase_details",
    ]

    failed = []
    for table in tables:
        full = f"{raw_db}.{table}"
        try:
            cnt = spark.sql(f"SELECT COUNT(*) FROM {full}").collect()[0][0]
            status = "OK" if cnt > 0 else "EMPTY"
            logger.info("%-45s | %-5s | %8d rows", full, status, cnt)
            if cnt == 0:
                failed.append(full)
        except Exception as e:
            logger.error("Verify failed | table=%s | error=%s", full, e)
            failed.append(full)

    if failed:
        logger.error("Verification FAILED for: %s", failed)
        sys.exit(1)

    logger.info("All tables verified successfully")

    # Show database overview
    logger.info("=== Hive Database Summary ===")
    spark.sql(f"SHOW TABLES IN {raw_db}").show(30, truncate=False)


# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="AdventureWorks PostgreSQL → HDFS Parquet → Hive External Tables"
    )
    parser.add_argument("--config", required=True,
                        help="Path to pipeline YAML config (e.g. configs/pipeline.yaml)")
    parser.add_argument("--step", default="all",
                        choices=["all", "extract", "register", "verify"],
                        help="Pipeline step to run (default: all)")
    args = parser.parse_args()

    config = load_config(args.config)
    spark  = get_spark_with_hive("HiveToParquet", config.get("spark", {}))

    logger.info("Job started | step=%s | config=%s", args.step, args.config)

    try:
        if args.step in ("all", "extract"):
            extract_dimensions(spark, config)
            extract_facts(spark, config)

        if args.step in ("all", "register"):
            register_hive_tables(spark, config)

        if args.step in ("all", "verify"):
            verify(spark, config)

        logger.info("Job complete | step=%s", args.step)

    except SystemExit:
        raise
    except Exception as e:
        logger.error("Unexpected error | step=%s | error=%s", args.step, e)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
