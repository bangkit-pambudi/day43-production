import pytest
import os
import sys
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests. Uses local[1] and minimal config to save RAM."""
    # Workaround for Java 21+ compatibility issues with Hadoop
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["_JAVA_OPTIONS"] = "-Dcom.sun.security.auth.useSubjectCredsOnly=false"
    
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-day43") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "768m") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.local.dir", "/tmp/spark-local") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "-Dcom.sun.security.auth.useSubjectCredsOnly=false -XX:+UseG1GC") \
        .config("spark.executor.extraJavaOptions", "-Dcom.sun.security.auth.useSubjectCredsOnly=false") \
        .getOrCreate()
    
    yield session
    session.stop()
