# Assignment 1 — HR Workforce Analysis Pipeline
### Day 43: Batch Processing with PySpark

---

## Business Case

The **HR Analytics team** at AdventureWorks needs a daily-refreshed workforce report to
support the quarterly talent review. They currently pull data manually from PostgreSQL and
paste it into Excel — a process that takes 4 hours and is error-prone.

Your task is to build a production `spark-submit` job that automates this report by
reading from Hive External Tables (already loaded by `hive_to_parquet.py`) and writing
curated output to `adventureworks_curated.fact_hr_workforce`.

---

## Dataset

All tables are in the Hive database `adventureworks`. Schema reference:

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `dim_employee` | `BusinessEntityID`, `HireDate`, `JobTitle`, `Gender`, `MaritalStatus` | One row per employee |
| `dim_department` | `DepartmentID`, `Name`, `GroupName` | Reference table |
| `dim_dept_history` | `BusinessEntityID`, `DepartmentID`, `StartDate`, `EndDate` | All dept transfers |
| `dim_pay_history` | `BusinessEntityID`, `RateChangeDate`, `Rate`, `PayFrequency` | All pay changes |

Sample row counts (AdventureWorks):
- `dim_employee`: 290 rows
- `dim_dept_history`: 296 rows (some employees appear multiple times)
- `dim_pay_history`: 316 rows (multiple pay rates per employee)

---

## Technical Requirements

Build `jobs/hr_pipeline.py` as a production `spark-submit` job.

### R1 — Entry point

The script must accept CLI arguments:

```bash
spark-submit jobs/hr_pipeline.py --config configs/pipeline.yaml
```

Use `argparse`. Load config via `utils/config_loader.load_config()`.

---

### R2 — Extract

Read all four tables from Hive using `utils/io.read_hive()`:

```python
employees    = read_hive(spark, "adventureworks.dim_employee")
departments  = read_hive(spark, "adventureworks.dim_department")
dept_history = read_hive(spark, "adventureworks.dim_dept_history")
pay_history  = read_hive(spark, "adventureworks.dim_pay_history")
```

---

### R3 — Filter departments

**Only include employees in Production and Engineering departments.**

```python
VALID_DEPARTMENTS = {"Production", "Engineering"}
```

The `dim_department.Name` column contains the department name. You need to join
`dim_dept_history` → `dim_department` to find each employee's *current* department
(where `EndDate IS NULL`).

Expected signature:
```python
def filter_valid_departments(dept_df: DataFrame) -> DataFrame:
    # Input:  dim_department with columns [departmentid, name]
    # Output: filtered DataFrame with column renamed to dept_name
```

> **Why this filter?** The HR director only wants to see the two largest departments
> for the talent review. Other departments submit their own reports separately.

---

### R4 — Latest pay rate

Each employee may have multiple rows in `dim_pay_history`. You need **only the most
recent pay rate** per employee.

Use a Window function:

```python
window = Window.partitionBy("businessentityid").orderBy(F.col("ratechangedate").desc())
# ROW_NUMBER() == 1 → most recent
```

Expected signature:
```python
def get_latest_pay_rate(pay_df: DataFrame) -> DataFrame:
    # Input:  dim_pay_history with [businessentityid, ratechangedate, rate]
    # Output: one row per employee with columns [pay_emp_id, latest_pay_rate]
```

---

### R5 — Department change count

Count **how many department records** each employee has in `dim_dept_history`.
This is the total number of rows (including their original assignment).

```python
def get_dept_change_count(hist_df: DataFrame) -> DataFrame:
    # Input:  dim_dept_history with [businessentityid, departmentid]
    # Output: [businessentityid, dept_history_count]
```

Use `groupBy().count()`.

---

### R6 — Join and enrich

Join all derived DataFrames with the employee base table. Compute derived columns:

| Column | Formula |
|--------|---------|
| `tenure_years` | `months_between(current_date(), hire_date) / 12` rounded to 2 dp |
| `dept_change_count` | `GREATEST(dept_history_count - 1, 0)` — subtract the original assignment |
| `is_mover` | `dept_change_count > 1` → boolean (True/False) |

Use `F.greatest(F.col("dept_history_count") - 1, F.lit(0))` to avoid negative values
for employees who have only one history record.

---

### R7 — Output schema

The final DataFrame written to Hive must have **exactly these columns**:

| Column | Type | Description |
|--------|------|-------------|
| `emp_id` | integer | BusinessEntityID |
| `job_title` | string | JobTitle |
| `dept_name` | string | Current department name |
| `hire_date` | date | Original hire date |
| `tenure_years` | double | Years at company |
| `latest_pay_rate` | double | Most recent hourly rate |
| `dept_history_count` | long | Total dept records |
| `dept_change_count` | long | Transfers (history - 1, min 0) |
| `is_mover` | boolean | True if transferred at least once |
| `gender` | string | M / F |
| `marital_status` | string | S / M |

Write to `adventureworks_curated.fact_hr_workforce` using `utils/io.write_hive_table()`.

---

### R8 — Validation

Before writing, call `utils/io.validate()` to assert row count > 0.

After writing, log:
```
HR pipeline complete | rows=<N> | curated=adventureworks_curated.fact_hr_workforce
```

---

### R9 — Error handling

Wrap the `main()` body in `try/except`. On any exception:
1. Log the error with `logger.error(...)`
2. Call `spark.stop()`
3. Call `sys.exit(1)`

Airflow needs exit code 1 to mark the task as failed.

---

## Expected Output

Run `bash run_demo.sh hr` and verify:

```bash
spark-submit jobs/hr_pipeline.py --config configs/pipeline.yaml
```

Expected log lines:
```
INFO  | hr_pipeline | Extracted 290 employees
INFO  | hr_pipeline | After department filter: ~200 employees (Production + Engineering)
INFO  | hr_pipeline | HR pipeline complete | rows=<N>
```

Then validate in PySpark shell:
```python
spark.table("adventureworks_curated.fact_hr_workforce").show(5)
spark.table("adventureworks_curated.fact_hr_workforce") \
    .groupBy("dept_name").count().show()
```

Expected result: only `Production` and `Engineering` in `dept_name` column.

---

## Acceptance Criteria

Your submission passes when ALL of the following are true:

- [ ] `spark-submit jobs/hr_pipeline.py --config configs/pipeline.yaml` exits with code 0
- [ ] `adventureworks_curated.fact_hr_workforce` exists and has > 0 rows
- [ ] Only `Production` and `Engineering` appear in `dept_name`
- [ ] No employee appears twice (verify: `count() == countDistinct("emp_id")`)
- [ ] `latest_pay_rate` is never null
- [ ] `dept_change_count` is never negative
- [ ] `pytest tests/test_pipelines.py::TestHRPipelineFunctions -v` — all 3 tests pass

---

## Bonus Challenges

**B1 — Pay tier segmentation** *(+10 pts)*  
Add a `pay_tier` column using `F.when()` chains:
- `latest_pay_rate < 15` → "Entry"
- `15 <= latest_pay_rate < 25` → "Mid"
- `latest_pay_rate >= 25` → "Senior"

**B2 — Tenure band** *(+10 pts)*  
Add `tenure_band`:
- `tenure_years < 2` → "New"
- `2 <= tenure_years < 5` → "Established"
- `tenure_years >= 5` → "Veteran"

**B3 — Movers analysis** *(+15 pts)*  
Build a second output table `adventureworks_curated.fact_hr_movers` that contains
only `is_mover=True` employees, with an additional column `avg_years_per_dept`
= `tenure_years / dept_history_count`.

**B4 — Unit test for pay tier** *(+15 pts)*  
Write a `pytest` test in `tests/test_pipelines.py` that verifies the pay tier
boundaries using synthetic data with 3 rows (one per tier).

---

## Scoring Rubric

| Criterion | Points |
|-----------|--------|
| R1–R3: Entry point, extract, department filter | 20 |
| R4: Latest pay rate (Window function) | 20 |
| R5: Department change count | 15 |
| R6: Join + derived columns (tenure, is_mover) | 20 |
| R7–R9: Output schema, validation, error handling | 15 |
| Code quality (logging, modular functions, no hardcoded paths) | 10 |
| **Total** | **100** |
| Bonus challenges | up to +50 |

---

## Submission

1. `jobs/hr_pipeline.py` — complete production job
2. `tests/test_pipelines.py` — TestHRPipelineFunctions must pass
3. Screenshot or log file from `bash run_demo.sh hr`

> **Tip:** Start with `filter_valid_departments()` and test it with
> `pytest tests/test_pipelines.py::TestHRPipelineFunctions::test_filter_valid_departments -v`
> before wiring up the full pipeline.
