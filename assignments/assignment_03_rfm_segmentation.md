# Assignment 3 — RFM Customer Segmentation Pipeline
### Day 43: Batch Processing with PySpark

---

## Business Case

The **Marketing team** at AdventureWorks runs 4 targeted email campaigns per year.
Their biggest problem: they blast the same offer to all customers regardless of purchase
history, resulting in high unsubscribe rates for loyal buyers and zero re-engagement
for dormant ones.

The solution is RFM segmentation — a classical marketing analytics framework that
scores each customer on three dimensions and assigns them to one of 7 behavioral segments.
Marketing can then tailor messages per segment (e.g., "Champion" customers get early
access; "Lost" customers get a win-back offer).

Your task is to build a production `spark-submit` job that computes RFM scores and
segments for all AdventureWorks customers.

---

## Background: RFM Framework

| Dimension | Meaning | Higher = Better? |
|-----------|---------|-----------------|
| **R** — Recency | Days since last purchase | Lower days = better |
| **F** — Frequency | Number of orders | Higher = better |
| **M** — Monetary | Total spend | Higher = better |

**Scoring:** Each dimension is divided into 4 quartiles using `ntile(4)`:
- Score 4 = best quartile
- Score 1 = worst quartile

**R-score inversion:** Because lower recency_days is *better*, invert the score:
```
r_score = 5 - ntile(4) OVER (ORDER BY recency_days ASC)
```
Wait — this means ORDER BY `recency_days` ascending means the smallest recency_days
gets ntile=1, so after `5 - ntile`: score = 4 (best). Correct.

Actually the standard approach is:
```
r_score = 5 - ntile(4) OVER (ORDER BY recency_days DESC)
```
Where ORDER BY DESC puts largest recency (most stale) in ntile=1, then `5-1=4`...
that would be wrong. Think carefully.

The correct formula:
```python
# Smaller recency_days → more recent → should get score 4 (best)
# ntile ordered ASC: smallest recency_days → ntile=1
# So: r_score = 5 - ntile gives 4 ✓
window_r = Window.orderBy(F.col("recency_days").asc())
r_ntile  = F.ntile(4).over(window_r)
r_score  = (F.lit(5) - r_ntile).alias("r_score")
```

> **Instructor note:** This is an intentional trap. Students who copy `F-score` logic
> verbatim will get inverted R scores. The test `test_rfm_raw_computation` will catch it.

---

## Dataset

All tables are in the Hive database `adventureworks`.

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `fact_sales_orders` | `SalesOrderID`, `CustomerID`, `OrderDate`, `TotalDue` | One row per order header |
| `dim_customer` | `CustomerID`, `PersonID`, `StoreID` | Customer master |

Sample:
- `fact_sales_orders`: ~31,465 rows
- `dim_customer`: ~19,820 rows
- Distinct customers with at least one order: ~19,119

**Reference date:** Use `"2014-07-31"` (the last date of the AdventureWorks dataset).
This makes results deterministic and matches test expectations.

Read from `configs/pipeline.yaml`:
```yaml
pipeline:
  reference_date: "2014-07-31"
```

---

## Technical Requirements

Build `jobs/rfm_pipeline.py` as a production `spark-submit` job.

### R1 — Entry point

```bash
spark-submit jobs/rfm_pipeline.py --config configs/pipeline.yaml
```

---

### R2 — Extract

```python
sales_orders = read_hive(spark, "adventureworks.fact_sales_orders")
customers    = read_hive(spark, "adventureworks.dim_customer")
```

---

### R3 — Compute raw RFM metrics

Expected signature:
```python
def compute_rfm_raw(orders_df: DataFrame, reference_date: str) -> DataFrame:
    # Input:  fact_sales_orders with [customerid, salesorderid, orderdate, totaldue]
    # Output: one row per customer with [customerid, recency_days, frequency, monetary]
```

Formulas:

| Column | Formula |
|--------|---------|
| `recency_days` | `datediff(lit(reference_date).cast("date"), max(orderdate))` |
| `frequency` | `count(salesorderid)` |
| `monetary` | `sum(totaldue)` rounded to 2 dp |

Group by `customerid`.

> Note: `recency_days` = how many days since the customer last ordered, relative
> to the reference date. Customer who ordered on the reference date → `recency_days = 0`.

---

### R4 — Score RFM with ntile(4)

Expected signature:
```python
def score_rfm(rfm_df: DataFrame) -> DataFrame:
    # Input:  [customerid, recency_days, frequency, monetary]
    # Output: adds [r_score, f_score, m_score] columns (each 1–4)
```

```python
# R: smaller recency = more recent = score 4
window_r = Window.orderBy(F.col("recency_days").asc())
r_score  = (F.lit(5) - F.ntile(4).over(window_r)).alias("r_score")

# F and M: higher = better = score 4 (no inversion needed)
window_f = Window.orderBy(F.col("frequency").asc())
f_score  = F.ntile(4).over(window_f).alias("f_score")

window_m = Window.orderBy(F.col("monetary").asc())
m_score  = F.ntile(4).over(window_m).alias("m_score")
```

Then add `rfm_score` as a 3-character string concatenation:
```python
rfm_score = F.concat(
    F.col("r_score").cast("string"),
    F.col("f_score").cast("string"),
    F.col("m_score").cast("string")
)
```

Example: customer with R=3, F=4, M=4 → `rfm_score = "344"`.

---

### R5 — Assign segments

Expected signature:
```python
def assign_segments(scored_df: DataFrame) -> DataFrame:
    # Input:  DataFrame with [r_score, f_score, m_score, rfm_score]
    # Output: adds [rfm_segment] column
```

Apply these rules **in priority order** using `F.when().when()...otherwise()`:

| Segment | Rule | Priority |
|---------|------|----------|
| `Champion` | `r_score >= 3 AND f_score >= 3 AND m_score >= 3` | 1 (highest) |
| `Loyal` | `f_score >= 3 AND m_score >= 3` | 2 |
| `Potential Loyal` | `r_score >= 3 AND f_score >= 2` | 3 |
| `At Risk` | `r_score <= 2 AND f_score >= 3 AND m_score >= 3` | 4 |
| `Lost` | `r_score == 1 AND f_score == 1 AND m_score == 1` | 5 |
| `New Customer` | `r_score >= 4 AND f_score <= 1` | 6 |
| `Others` | all remaining | 7 (fallback) |

> **Priority matters:** A customer who qualifies for both "Champion" and "Loyal"
> must be labeled "Champion". Spark's `F.when()` evaluates conditions in order —
> the first match wins.

---

### R6 — Output schema

The final DataFrame written to Hive must have these columns:

| Column | Type | Description |
|--------|------|-------------|
| `customerid` | integer | CustomerID |
| `recency_days` | integer | Days since last order |
| `frequency` | long | Number of orders |
| `monetary` | double | Total spend |
| `r_score` | integer | Recency score (1–4) |
| `f_score` | integer | Frequency score (1–4) |
| `m_score` | integer | Monetary score (1–4) |
| `rfm_score` | string | 3-char string e.g. "344" |
| `rfm_segment` | string | One of the 7 segment labels |

Write to `adventureworks_curated.fact_customer_rfm`.

---

### R7 — Validation and error handling

- Call `validate()` before writing (row count > 0)
- Log a segment distribution summary before exiting:

```python
logger.info("Segment distribution:")
spark.table("adventureworks_curated.fact_customer_rfm") \
    .groupBy("rfm_segment").count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)
```

- Wrap `main()` in `try/except` with `sys.exit(1)` on failure

---

## Expected Output

```bash
bash run_demo.sh rfm
```

Verify:
```python
spark.table("adventureworks_curated.fact_customer_rfm") \
    .groupBy("rfm_segment").count().orderBy("count", ascending=False).show()
```

Expected: all 7 segments present. Typical distribution for AdventureWorks:

| Segment | Approx count |
|---------|-------------|
| Others | largest group |
| Champion | medium |
| Loyal | medium |
| At Risk | small |
| Lost | small |
| Potential Loyal | small |
| New Customer | smallest |

Verify score format:
```python
spark.table("adventureworks_curated.fact_customer_rfm") \
    .select("rfm_score").distinct().orderBy("rfm_score").show(20)
# All values should be 3-char strings like "111", "344", "444"
```

---

## Acceptance Criteria

- [ ] `spark-submit jobs/rfm_pipeline.py --config configs/pipeline.yaml` exits 0
- [ ] `fact_customer_rfm` has > 0 rows
- [ ] All 7 segment labels appear in `rfm_segment`
- [ ] `rfm_score` is always exactly 3 characters
- [ ] `r_score`, `f_score`, `m_score` values are only 1, 2, 3, or 4
- [ ] No customer appears twice
- [ ] `recency_days` >= 0 for all rows (no future orders)
- [ ] `pytest tests/test_pipelines.py::TestRFMPipelineFunctions -v` — all 5 tests pass

---

## Bonus Challenges

**B1 — Monetary percentile** *(+10 pts)*  
Add `monetary_percentile` column using `F.percent_rank()` over `Window.orderBy("monetary")`.
This gives the relative spend position (0.0 to 1.0) for each customer.

**B2 — Segment migration tracking** *(+15 pts)*  
The pipeline runs daily. Add a `segment_date` column = `F.current_date()` and
write in **append mode** instead of overwrite. This lets the Marketing team see
how segments shift over time.  
*Note: requires changing `write_mode` to "append" for this table only.*

**B3 — Champion breakdown** *(+15 pts)*  
For the `Champion` segment, add a sub-classification:
- `rfm_score == "444"` → "Gold Champion"
- `rfm_score` starts with "4" (but not "444") → "Silver Champion"
- otherwise → "Bronze Champion"

**B4 — Configurable reference date** *(+10 pts)*  
Read `reference_date` from `configs/pipeline.yaml` and add a CLI override:
```bash
spark-submit jobs/rfm_pipeline.py --config configs/pipeline.yaml --reference-date 2014-12-31
```
Use `argparse` with `--reference-date` defaulting to the YAML value.

---

## Scoring Rubric

| Criterion | Points |
|-----------|--------|
| R1–R3: Entry point, extract, raw RFM computation | 25 |
| R4: ntile scoring with correct R inversion | 25 |
| R5: Segment assignment (priority order) | 25 |
| R6–R7: Output schema, validation, error handling | 15 |
| Code quality (logging, modular functions, no hardcoded paths) | 10 |
| **Total** | **100** |
| Bonus challenges | up to +50 |

---

## Submission

1. `jobs/rfm_pipeline.py` — complete production job
2. `tests/test_pipelines.py` — TestRFMPipelineFunctions must pass
3. Screenshot of segment distribution table from `bash run_demo.sh rfm`

> **The hardest part is R4.** Before writing `score_rfm()`, draw out the ntile
> ordering on paper for 8 hypothetical customers. Verify that the customer who
> bought yesterday gets `r_score=4`, not `r_score=1`.
>
> Run `pytest tests/test_pipelines.py::TestRFMPipelineFunctions::test_rfm_raw_computation -v`
> first — it checks `recency_days=46` for a specific customer, which pins the reference
> date math and catches off-by-one errors early.
