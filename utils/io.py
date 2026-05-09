import sys
from pyspark.sql import SparkSession, DataFrame
from utils.logger import get_logger

logger = get_logger(__name__)


# ─── READ ──────────────────────────────────────────────────────────────────────

def read_jdbc(spark: SparkSession, jdbc_url: str, table_or_query: str,
              user: str, password: str, driver: str,
              partition_column: str = None, num_partitions: int = 4,
              lower_bound: int = None, upper_bound: int = None) -> DataFrame:
    """
    Read a table or push-down query from a JDBC source.
    Supports parallel reads via partition_column / num_partitions.
    """
    props = {"user": user, "password": password, "driver": driver}
    logger.info("JDBC read | source=%s", table_or_query)
    try:
        if partition_column and lower_bound is not None and upper_bound is not None:
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_or_query,
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=props,
            )
        else:
            df = spark.read.jdbc(url=jdbc_url, table=table_or_query, properties=props)
        logger.info("JDBC read complete | source=%s | rows=%d", table_or_query, df.count())
        return df
    except Exception as e:
        logger.error("JDBC read failed | source=%s | error=%s", table_or_query, e)
        sys.exit(1)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    logger.info("Reading parquet | path=%s", path)
    try:
        df = spark.read.parquet(path)
        logger.info("Parquet read complete | path=%s | rows=%d", path, df.count())
        return df
    except Exception as e:
        logger.error("Parquet read failed | path=%s | error=%s", path, e)
        sys.exit(1)


def read_hive(spark: SparkSession, database: str, table: str) -> DataFrame:
    full = f"{database}.{table}"
    logger.info("Reading Hive table | table=%s", full)
    try:
        df = spark.table(full)
        logger.info("Hive read complete | table=%s | rows=%d", full, df.count())
        return df
    except Exception as e:
        logger.error("Hive read failed | table=%s | error=%s", full, e)
        sys.exit(1)


# ─── WRITE ─────────────────────────────────────────────────────────────────────

def write_parquet(df: DataFrame, path: str,
                  partition_by: list = None, mode: str = "overwrite") -> None:
    logger.info("Writing parquet | path=%s | mode=%s | partition_by=%s",
                path, mode, partition_by)
    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(path)
        logger.info("Parquet write complete | path=%s", path)
    except Exception as e:
        logger.error("Parquet write failed | path=%s | error=%s", path, e)
        sys.exit(1)


def write_hive_table(df: DataFrame, database: str, table: str,
                     partition_by: list = None, mode: str = "overwrite") -> None:
    full = f"{database}.{table}"
    logger.info("Writing Hive table | table=%s | mode=%s", full, mode)
    try:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(full)
        logger.info("Hive write complete | table=%s", full)
    except Exception as e:
        logger.error("Hive write failed | table=%s | error=%s", full, e)
        sys.exit(1)


# ─── VALIDATE ──────────────────────────────────────────────────────────────────

def validate(df: DataFrame, name: str, min_rows: int = 1) -> None:
    count = df.count()
    assert count >= min_rows, \
        f"[VALIDATE FAILED] {name}: expected >= {min_rows} rows, got {count}"
    logger.info("Validation passed | table=%s | rows=%d", name, count)
