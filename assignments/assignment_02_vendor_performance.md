# Assignment 2 — Vendor Performance Pipeline
### Day 43: Batch Processing with PySpark

---

## Business Case

The **Procurement team** at AdventureWorks evaluates vendors quarterly to decide which
suppliers to renew contracts with. Their current process: a senior analyst manually
exports CSV from PostgreSQL, opens it in Excel, writes VLOOKUP formulas for ship-method
filtering, and calculates delivery performance by hand. One mistake caused a bad contract
renewal last quarter.

Your task is to automate this entirely as a production `spark-submit` job that reads
from Hive External Tables and writes two curated output tables:
- `adventureworks_curated.fact_vendor_performance` — per-vendor metrics
- `adventureworks_curated.vendor_overall_ranking` — ranked vendor leaderboard

---

## Dataset

All tables are in the Hive database `adventureworks`.

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `fact_purchase_orders` | `PurchaseOrderID`, `VendorID`, `ShipMethodID`, `OrderDate`, `SubTotal` | One row per PO header |
| `fact_purchase_details` | `PurchaseOrderID`, `PurchaseOrderDetailID`, `OrderQty`, `UnitPrice`, `LineTotal` | Line items |
| `dim_vendor` | `BusinessEntityID` (=VendorID), `Name`, `CreditRating`, `ActiveFlag` | Vendor master |
| `dim_ship_method` | `ShipMethodID`, `Name` | 5 ship methods total |

Key columns in `fact_purchase_orders`:

| Column | Type | Description |
|--------|------|-------------|
| `DueDate` | date | When the PO was due |
| `ShipDate` | date | When it actually shipped (can be NULL if not yet shipped) |
| `SubTotal` | decimal | Order value |

Sample row counts:
- `fact_purchase_orders`: ~4,012 rows
- `dim_vendor`: 104 rows
- `dim_ship_method`: 5 rows (only 2 are relevant for this report)

---

## Technical Requirements

Build `jobs/vendor_pipeline.py` as a production `spark-submit` job.

### R1 — Entry point

```bash
spark-submit jobs/vendor_pipeline.py --config configs/pipeline.yaml
```

Use `argparse`. Load config via `utils/config_loader.load_config()`.

---

### R2 — Extract

Read all four tables from Hive:

```python
purchase_orders  = read_hive(spark, "adventureworks.fact_purchase_orders")
purchase_details = read_hive(spark, "adventureworks.fact_purchase_details")
vendors          = read_hive(spark, "adventureworks.dim_vendor")
ship_methods     = read_hive(spark, "adventureworks.dim_ship_method")
```

---

### R3 — Filter ship methods

**Only include purchase orders shipped via "CARGO TRANSPORT 5" or "OVERNIGHT J-FAST".**

```python
VALID_SHIP_METHODS = {"CARGO TRANSPORT 5", "OVERNIGHT J-FAST"}
```

Expected signature:
```python
def filter_ship_methods(ship_df: DataFrame) -> DataFrame:
    # Input:  dim_ship_method with [shipmethodid, name]
    # Output: filtered DataFrame with column renamed to ship_method
```

Join `fact_purchase_orders` to this filtered result on `ShipMethodID` to get only
relevant purchase orders.

> **Why this filter?** The Procurement team only manages these two premium shipping
> contracts. Ground and standard mail are handled by the logistics team separately.

---

### R4 — On-time delivery flag

For each purchase order, compute `is_on_time`:

```
is_on_time = 1  if ShipDate IS NOT NULL AND ShipDate <= DueDate
is_on_time = 0  otherwise (late OR not yet shipped)
```

Use `F.when()`:
```python
F.when(
    F.col("shipdate").isNotNull() & (F.col("shipdate") <= F.col("duedate")),
    F.lit(1)
).otherwise(F.lit(0))
```

> **Critical:** NULL ShipDate must NOT count as on-time. An unshipped order
> is worse than a late one from the Procurement team's perspective.

---

### R5 — Vendor-level aggregation

Group by `(vendor_id, vendor_name, credit_rating, ship_method)` and aggregate:

| Column | Aggregation |
|--------|------------|
| `total_orders` | `count(PurchaseOrderID)` |
| `on_time_orders` | `sum(is_on_time)` |
| `total_spend` | `sum(SubTotal)` rounded to 2 dp |
| `avg_price_variance` | average of `(UnitPrice - StandardCost)` per line item |

For `avg_price_variance`, join `fact_purchase_details` on `PurchaseOrderID` first,
then join `dim_product` (if available) for `StandardCost`. If `StandardCost` is not
available, use `UnitPrice - (UnitPrice * 0.85)` as a proxy (assume 15% standard margin).

---

### R6 — Vendor score formula

After aggregation, compute derived columns:

```python
on_time_rate = ROUND(on_time_orders / total_orders * 100, 2)

vendor_score = ROUND(
    (on_time_rate * 0.6) + ((100 - ABS(avg_price_variance)) * 0.4),
    2
)
```

**Important edge cases:**
- `total_orders = 0` → use `F.when(total_orders > 0, formula).otherwise(F.lit(0.0))`
- `avg_price_variance > 100` → vendor score could go negative; clamp to 0 with `F.greatest(score, F.lit(0.0))`

---

### R7 — Output: fact_vendor_performance

Write the per-vendor-per-shipmethod table:

| Column | Type | Description |
|--------|------|-------------|
| `vendor_id` | integer | VendorID |
| `vendor_name` | string | Vendor name |
| `credit_rating` | integer | 1 (best) to 5 (worst) |
| `ship_method` | string | "CARGO TRANSPORT 5" or "OVERNIGHT J-FAST" |
| `total_orders` | long | Total POs in period |
| `on_time_orders` | long | POs shipped on time |
| `on_time_rate` | double | % on time (0–100) |
| `total_spend` | double | Total SubTotal |
| `avg_price_variance` | double | Avg unit price deviation |
| `vendor_score` | double | Composite score (0–100) |

Write to `adventureworks_curated.fact_vendor_performance`.

---

### R8 — Output: vendor_overall_ranking

Aggregate `fact_vendor_performance` to one row per vendor (across all ship methods),
then add a ranking:

```python
overall_score = ROUND(AVG(vendor_score), 2)  # average across ship methods
```

Add `overall_rank` using `F.rank()` Window function, ordered by `overall_score DESC`.

| Column | Type |
|--------|------|
| `vendor_id` | integer |
| `vendor_name` | string |
| `credit_rating` | integer |
| `overall_score` | double |
| `overall_rank` | integer |

Write to `adventureworks_curated.vendor_overall_ranking`.

---

### R9 — Validation and error handling

- Call `utils/io.validate()` on both output DataFrames before writing
- Wrap `main()` in `try/except` with `sys.exit(1)` on failure
- Log final summary:
  ```
  Vendor pipeline complete | vendors=<N> | fact_vendor_performance rows=<N>
  ```

---

## Expected Output

```bash
bash run_demo.sh vendor
```

Then verify:
```python
spark.table("adventureworks_curated.fact_vendor_performance") \
    .groupBy("ship_method").count().show()
# Expected: 2 rows — "CARGO TRANSPORT 5" and "OVERNIGHT J-FAST"

spark.table("adventureworks_curated.vendor_overall_ranking") \
    .orderBy("overall_rank").show(10)
# Expected: vendors ranked 1..N by overall_score descending
```

---

## Acceptance Criteria

- [ ] `spark-submit jobs/vendor_pipeline.py --config configs/pipeline.yaml` exits 0
- [ ] `fact_vendor_performance` has > 0 rows
- [ ] Only `CARGO TRANSPORT 5` and `OVERNIGHT J-FAST` appear in `ship_method`
- [ ] `on_time_rate` is between 0 and 100 for every row
- [ ] `vendor_score` is between 0 and 100 for every row
- [ ] `vendor_overall_ranking` has exactly one row per vendor
- [ ] `overall_rank` values are consecutive integers starting at 1
- [ ] `pytest tests/test_pipelines.py::TestVendorPipelineFunctions -v` — all 3 tests pass

---

## Bonus Challenges

**B1 — Monthly trend per vendor** *(+10 pts)*  
Add a third output table `adventureworks_curated.vendor_monthly_trend` with
`(vendor_id, order_year, order_month, total_orders, on_time_rate)`. Add a
`mom_change` column (MoM change in `on_time_rate`) using `utils/transforms.add_mom_growth()`.

**B2 — Credit risk flag** *(+10 pts)*  
Add `credit_risk` column to `vendor_overall_ranking`:
- `credit_rating >= 4 AND on_time_rate < 80` → "High Risk"
- `credit_rating == 3 OR on_time_rate < 90` → "Watch"
- otherwise → "Good Standing"

**B3 — Vendor comparison broadcast join** *(+15 pts)*  
`dim_vendor` has only 104 rows. Explicitly broadcast it using
`F.broadcast(vendors)` and add a comment explaining *why* this is the correct
choice here (not just a performance trick, but a correctness guarantee in skewed
data environments).

**B4 — Parameterized ship methods** *(+15 pts)*  
Read `VALID_SHIP_METHODS` from `configs/pipeline.yaml` instead of hardcoding it.
Add a `vendor.valid_ship_methods` key to the YAML and read it in the job.

---

## Scoring Rubric

| Criterion | Points |
|-----------|--------|
| R1–R3: Entry point, extract, ship method filter | 20 |
| R4: On-time flag (NULL-safe) | 15 |
| R5: Vendor-level aggregation | 20 |
| R6: Score formula (edge-case safe) | 20 |
| R7–R9: Output tables, validation, error handling | 15 |
| Code quality (logging, modular functions, no hardcoded paths) | 10 |
| **Total** | **100** |
| Bonus challenges | up to +50 |

---

## Submission

1. `jobs/vendor_pipeline.py` — complete production job
2. `tests/test_pipelines.py` — TestVendorPipelineFunctions must pass
3. Screenshot or log file from `bash run_demo.sh vendor`

> **Tip:** Test the on-time flag first with `test_on_time_flag_null_shipdate` —
> it's the trickiest edge case. NULL must evaluate to 0, not be skipped.
