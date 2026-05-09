"""
Unit tests for utils/transforms.py

Run:
    cd day43-production
    pytest tests/test_transforms.py -v
"""

import pytest
from pyspark.sql import functions as F
from utils.transforms import (
    add_margin_columns,
    add_mom_growth,
    add_yoy_growth,
    add_cumulative_total,
    add_revenue_share,
    rank_within_group,
)


# ─── add_margin_columns ────────────────────────────────────────────────────────

class TestMarginColumns:

    def test_positive_margin(self, spark):
        df = spark.createDataFrame(
            [(1, "Product A", 100.0, 60.0)],
            ["product_id", "name", "listprice", "standardcost"]
        )
        result = add_margin_columns(df).collect()[0]
        assert result["margin"] == 40.0
        assert result["margin_pct"] == 40.0

    def test_zero_margin(self, spark):
        df = spark.createDataFrame(
            [(2, "Break Even", 100.0, 100.0)],
            ["product_id", "name", "listprice", "standardcost"]
        )
        result = add_margin_columns(df).collect()[0]
        assert result["margin"] == 0.0
        assert result["margin_pct"] == 0.0

    def test_zero_list_price_no_division_error(self, spark):
        """When list_price is 0, margin_pct must be 0 — not ZeroDivisionError."""
        df = spark.createDataFrame(
            [(3, "Free Item", 0.0, 0.0)],
            ["product_id", "name", "listprice", "standardcost"]
        )
        result = add_margin_columns(df).collect()[0]
        assert result["margin_pct"] == 0.0

    def test_high_margin_product(self, spark):
        df = spark.createDataFrame(
            [(4, "Mountain Bike", 3578.27, 1912.15)],
            ["product_id", "name", "listprice", "standardcost"]
        )
        result = add_margin_columns(df).collect()[0]
        assert result["margin"] == round(3578.27 - 1912.15, 2)
        assert 0 < result["margin_pct"] < 100


# ─── add_mom_growth ────────────────────────────────────────────────────────────

class TestMomGrowth:

    def test_positive_growth(self, spark):
        data = [
            (2014, 1, 1000.0),
            (2014, 2, 1200.0),
            (2014, 3, 1100.0),
        ]
        df = spark.createDataFrame(data, ["order_year", "order_month", "monthly_revenue"])
        result = add_mom_growth(df, "monthly_revenue", "order_year", "order_month")
        rows = {r["order_month"]: r for r in result.collect()}
        assert rows[1]["mom_growth_pct"] is None   # first month — no prior
        assert rows[2]["mom_growth_pct"] == 20.0   # (1200-1000)/1000*100
        assert rows[3]["mom_growth_pct"] == round((1100 - 1200) / 1200 * 100, 2)

    def test_no_growth_from_zero(self, spark):
        """Month with prev=0 must return None, not infinity."""
        data = [(2014, 1, 0.0), (2014, 2, 500.0)]
        df = spark.createDataFrame(data, ["order_year", "order_month", "monthly_revenue"])
        result = add_mom_growth(df, "monthly_revenue", "order_year", "order_month")
        rows = {r["order_month"]: r for r in result.collect()}
        assert rows[2]["mom_growth_pct"] is None


# ─── add_cumulative_total ──────────────────────────────────────────────────────

class TestCumulativeTotal:

    def test_running_sum(self, spark):
        data = [(2014, 1, 100.0), (2014, 2, 200.0), (2014, 3, 150.0)]
        df = spark.createDataFrame(data, ["order_year", "order_month", "revenue"])
        result = add_cumulative_total(df, "revenue", "order_year", "order_month", "cum_rev")
        rows = sorted(result.collect(), key=lambda r: r["order_month"])
        assert rows[0]["cum_rev"] == 100.0
        assert rows[1]["cum_rev"] == 300.0
        assert rows[2]["cum_rev"] == 450.0

    def test_resets_per_partition(self, spark):
        """Cumulative total must reset for each year partition."""
        data = [
            (2013, 1, 100.0), (2013, 2, 200.0),
            (2014, 1, 50.0),  (2014, 2, 75.0),
        ]
        df = spark.createDataFrame(data, ["order_year", "order_month", "revenue"])
        result = add_cumulative_total(df, "revenue", "order_year", "order_month", "cum_rev")
        rows = {(r["order_year"], r["order_month"]): r["cum_rev"] for r in result.collect()}
        assert rows[(2013, 2)] == 300.0
        assert rows[(2014, 2)] == 125.0   # resets at 2014


# ─── rank_within_group ─────────────────────────────────────────────────────────

class TestRankWithinGroup:

    def test_top_2_per_year(self, spark):
        data = [
            (2014, "Bike A",    5000.0),
            (2014, "Helmet B",  3000.0),
            (2014, "Glove C",   1000.0),
            (2013, "Bike A",    4000.0),
            (2013, "Jersey D",  2000.0),
        ]
        df = spark.createDataFrame(data, ["order_year", "product_name", "total_revenue"])
        result = rank_within_group(df, "total_revenue", ["order_year"], top_n=2)
        assert result.count() == 4   # 2 per year × 2 years

    def test_rank_column_present(self, spark):
        data = [(2014, "A", 100.0), (2014, "B", 200.0)]
        df = spark.createDataFrame(data, ["order_year", "name", "revenue"])
        result = rank_within_group(df, "revenue", ["order_year"], top_n=5)
        assert "rank" in result.columns


# ─── add_revenue_share ─────────────────────────────────────────────────────────

class TestRevenueShare:

    def test_share_sums_to_100(self, spark):
        data = [
            (2014, "Bikes",       6000.0),
            (2014, "Accessories", 3000.0),
            (2014, "Clothing",    1000.0),
        ]
        df = spark.createDataFrame(data, ["order_year", "category", "cat_revenue"])
        result = add_revenue_share(df, "cat_revenue", "order_year")
        total_share = sum(r["revenue_share_pct"] for r in result.collect())
        assert abs(total_share - 100.0) < 0.1   # floating point tolerance
