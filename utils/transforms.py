from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_margin_columns(df: DataFrame,
                       list_price_col: str = "listprice",
                       cost_col: str = "standardcost") -> DataFrame:
    """Margin = list_price - standard_cost. Handles zero list_price (no division by zero)."""
    return df \
        .withColumn("margin",
            F.round(F.col(list_price_col) - F.col(cost_col), 2)) \
        .withColumn("margin_pct",
            F.round(
                F.when(F.col(list_price_col) > 0,
                    (F.col(list_price_col) - F.col(cost_col)) / F.col(list_price_col) * 100
                ).otherwise(0),
            2))


def add_mom_growth(df: DataFrame,
                   metric_col: str,
                   partition_col: str,
                   order_col: str,
                   alias: str = "mom_growth_pct") -> DataFrame:
    """Month-over-month growth %. Null-safe and zero-safe."""
    win = Window.partitionBy(partition_col).orderBy(order_col)
    return df \
        .withColumn("_prev", F.lag(metric_col, 1).over(win)) \
        .withColumn(alias,
            F.round(
                F.when(
                    F.col("_prev").isNotNull() & (F.col("_prev") > 0),
                    (F.col(metric_col) - F.col("_prev")) / F.col("_prev") * 100
                ).otherwise(None),
            2)) \
        .drop("_prev")


def add_yoy_growth(df: DataFrame,
                   metric_col: str,
                   partition_col: str,
                   alias: str = "yoy_growth_pct") -> DataFrame:
    """Year-over-year growth %. Null-safe and zero-safe."""
    win = Window.partitionBy(partition_col).orderBy("order_year")
    return df \
        .withColumn("_prev", F.lag(metric_col, 1).over(win)) \
        .withColumn(alias,
            F.round(
                F.when(
                    F.col("_prev").isNotNull() & (F.col("_prev") > 0),
                    (F.col(metric_col) - F.col("_prev")) / F.col("_prev") * 100
                ).otherwise(None),
            2)) \
        .drop("_prev")


def add_cumulative_total(df: DataFrame,
                         metric_col: str,
                         partition_col: str,
                         order_col: str,
                         alias: str = "cumulative_total") -> DataFrame:
    """Running total within partition, ordered by order_col."""
    win = Window.partitionBy(partition_col) \
        .orderBy(order_col) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn(alias, F.round(F.sum(metric_col).over(win), 2))


def add_revenue_share(df: DataFrame,
                      metric_col: str,
                      partition_col: str,
                      alias: str = "revenue_share_pct") -> DataFrame:
    """Percentage share of metric within partition."""
    win = Window.partitionBy(partition_col)
    return df \
        .withColumn("_total", F.sum(metric_col).over(win)) \
        .withColumn(alias, F.round(F.col(metric_col) / F.col("_total") * 100, 2)) \
        .drop("_total")


def rank_within_group(df: DataFrame,
                      metric_col: str,
                      partition_cols: list,
                      top_n: int = 5,
                      rank_col: str = "rank") -> DataFrame:
    """Rank rows within partition by metric_col descending, filter to top_n."""
    win = Window.partitionBy(*partition_cols).orderBy(F.desc(metric_col))
    return df \
        .withColumn(rank_col, F.rank().over(win)) \
        .filter(F.col(rank_col) <= top_n)
