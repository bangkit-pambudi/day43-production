import os
from pyspark.sql import SparkSession


def get_spark(app_name: str, config: dict = None) -> SparkSession:
    """
    Build a SparkSession for local execution.
    Accepts an optional config dict to override defaults (from pipeline.yaml spark section).
    """
    cfg = config or {}

    master         = cfg.get("master", "local[2]")
    driver_memory  = cfg.get("driver_memory", "1g")
    shuffle_parts  = str(cfg.get("shuffle_partitions", 4))
    jars           = cfg.get("jars", "")

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.driver.memory",              driver_memory)
        .config("spark.sql.shuffle.partitions",     shuffle_parts)
        .config("spark.sql.adaptive.enabled",       "true")
        .config("spark.ui.showConsoleProgress",     "false")
        .config("spark.sql.parquet.compression.codec",      "snappy")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )

    if jars:
        builder = builder.config("spark.jars", jars)

    return builder.getOrCreate()


def get_spark_with_hive(app_name: str, config: dict = None) -> SparkSession:
    """
    Build a SparkSession with Hive support enabled.
    Used by hive_to_parquet.py which needs to register External Tables.
    Requires hive.metastore.uris and spark.sql.warehouse.dir in config.
    """
    cfg = config or {}

    master          = cfg.get("master", "local[2]")
    driver_memory   = cfg.get("driver_memory", "1g")
    shuffle_parts   = str(cfg.get("shuffle_partitions", 4))
    jars            = cfg.get("jars", "")
    metastore_uri   = cfg.get("hive_metastore_uri",  os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
    warehouse_dir   = cfg.get("warehouse_dir",        os.environ.get("HIVE_WAREHOUSE_DIR", "hdfs://namenode:9000/user/hive/warehouse"))

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.driver.memory",                  driver_memory)
        .config("spark.sql.shuffle.partitions",         shuffle_parts)
        .config("spark.sql.adaptive.enabled",           "true")
        .config("spark.ui.showConsoleProgress",         "false")
        .config("spark.sql.parquet.compression.codec",      "snappy")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("hive.metastore.uris",                  metastore_uri)
        .config("spark.sql.warehouse.dir",              warehouse_dir)
        .enableHiveSupport()
    )

    if jars:
        builder = builder.config("spark.jars", jars)

    return builder.getOrCreate()
