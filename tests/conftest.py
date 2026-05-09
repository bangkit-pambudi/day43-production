import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests. Uses local[1] and minimal config to save RAM."""
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-day43") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "512m") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    yield session
    session.stop()
