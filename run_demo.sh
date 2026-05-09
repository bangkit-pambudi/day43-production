#!/usr/bin/env bash
# run_demo.sh
# ===========
# Run the complete Day 43 AdventureWorks batch pipeline end-to-end.
# Prerequisites: bash setup.sh must have been run first.
#
# Usage:
#   bash run_demo.sh                  # full pipeline (all steps)
#   bash run_demo.sh extract          # Step 1: PostgreSQL → HDFS Parquet
#   bash run_demo.sh register         # Step 2: register Hive External Tables
#   bash run_demo.sh verify           # Step 3: row-count validation (raw)
#   bash run_demo.sh sales            # Step 4a: sales performance transforms
#   bash run_demo.sh hr               # Step 4b: HR workforce transforms
#   bash run_demo.sh vendor           # Step 4c: vendor performance transforms
#   bash run_demo.sh rfm              # Step 4d: RFM customer segmentation
#   bash run_demo.sh transform        # Step 4 (all 4 transforms)
#   bash run_demo.sh validate         # Step 5: validate curated tables
#   bash run_demo.sh test             # run pytest suite only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

STEP="${1:-all}"
CONFIG="configs/pipeline.yaml"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# ─── Colors ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()     { echo -e "${GREEN}  ✓ $*${NC}"; }
warn()   { echo -e "${YELLOW}  ⚠ $*${NC}"; }
fail()   { echo -e "${RED}  ✗ $*${NC}"; exit 1; }
section(){ echo -e "\n${CYAN}══════════════════════════════════════════════════════${NC}"; \
           echo -e "${CYAN}  $1${NC}"; \
           echo -e "${CYAN}  $(date '+%Y-%m-%d %H:%M:%S')${NC}"; \
           echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"; }

# ─── Pre-flight ───────────────────────────────────────────────────────────────

# Activate venv
if [ -d ".venv" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

# Load .env
if [ -f ".env" ]; then
    set -a
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    set +a
else
    warn ".env not found — using environment variables only"
fi

# Resolve PostgreSQL JAR path
JARS=""
JAR_CANDIDATES=(
    "$(pwd)/jars/postgresql-42.7.3.jar"
    "$(dirname "$(pwd)")/Day-42/jars/postgresql-42.7.3.jar"
    "${POSTGRES_JAR:-}"
)
for candidate in "${JAR_CANDIDATES[@]}"; do
    if [ -n "$candidate" ] && [ -f "$candidate" ]; then
        JARS="$candidate"
        break
    fi
done

if [ -z "$JARS" ] && [ "$STEP" != "test" ] && [ "$STEP" != "validate" ]; then
    warn "PostgreSQL JDBC JAR not found in jars/ or ../Day-42/jars/"
    warn "Set POSTGRES_JAR=/path/to/postgresql-42.7.3.jar in .env if extract will run"
fi

# ─── spark-submit wrapper ─────────────────────────────────────────────────────
run_spark() {
    local job="$1"
    shift
    local log_name
    log_name="$LOG_DIR/${TIMESTAMP}_$(basename "$job" .py).log"

    echo "  → spark-submit $job $*"
    echo "  → Log: $log_name"

    local jar_arg=()
    [ -n "$JARS" ] && jar_arg=(--jars "$JARS")

    spark-submit \
        --master "${SPARK_MASTER:-local[2]}" \
        --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
        --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
        --conf "spark.ui.showConsoleProgress=false" \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN,console" \
        "${jar_arg[@]}" \
        "$job" "$@" \
        2>&1 | tee "$log_name"

    local exit_code=${PIPESTATUS[0]}
    if [ $exit_code -eq 0 ]; then
        ok "$(basename "$job" .py) finished"
    else
        fail "$(basename "$job" .py) FAILED (exit $exit_code) — see $log_name"
    fi
}

# ─── Individual step functions ────────────────────────────────────────────────

step_extract() {
    section "Step 1/5 — Extract: PostgreSQL → HDFS Parquet"
    run_spark jobs/hive_to_parquet.py --config "$CONFIG" --step extract
}

step_register() {
    section "Step 2/5 — Register Hive External Tables"
    run_spark jobs/hive_to_parquet.py --config "$CONFIG" --step register
}

step_verify() {
    section "Step 3/5 — Verify Raw Tables (row-count check)"
    run_spark jobs/hive_to_parquet.py --config "$CONFIG" --step verify
}

step_sales() {
    section "Step 4a/5 — Transform: Sales Performance"
    run_spark jobs/sales_pipeline.py --config "$CONFIG"
}

step_hr() {
    section "Step 4b/5 — Transform: HR Workforce"
    run_spark jobs/hr_pipeline.py --config "$CONFIG"
}

step_vendor() {
    section "Step 4c/5 — Transform: Vendor Performance"
    run_spark jobs/vendor_pipeline.py --config "$CONFIG"
}

step_rfm() {
    section "Step 4d/5 — Transform: RFM Customer Segmentation"
    run_spark jobs/rfm_pipeline.py --config "$CONFIG"
}

step_validate() {
    section "Step 5/5 — Validate Curated Tables"
    python - <<'PYEOF'
import os, sys
sys.path.insert(0, os.getcwd())
from utils.spark_factory import get_spark
from utils.config_loader import load_config

cfg = load_config("configs/pipeline.yaml")
spark = get_spark("validate_curated", cfg)

curated_tables = [
    "adventureworks_curated.fact_sales_performance",
    "adventureworks_curated.fact_sales_monthly_trend",
    "adventureworks_curated.fact_territory_yoy",
    "adventureworks_curated.fact_top_products",
    "adventureworks_curated.fact_hr_workforce",
    "adventureworks_curated.fact_vendor_performance",
    "adventureworks_curated.vendor_overall_ranking",
    "adventureworks_curated.fact_customer_rfm",
]

failed = []
for table in curated_tables:
    try:
        count = spark.table(table).count()
        print(f"  ✓ {table:<55} {count:>8,} rows")
    except Exception as e:
        print(f"  ✗ {table:<55} ERROR: {e}")
        failed.append(table)

spark.stop()
if failed:
    print(f"\nFAILED: {len(failed)} table(s) missing or empty")
    sys.exit(1)
else:
    print(f"\nAll {len(curated_tables)} curated tables OK")
PYEOF
}

step_test() {
    section "pytest — Unit & DAG Integrity Tests"
    pytest tests/ -v --tb=short \
        2>&1 | tee "$LOG_DIR/${TIMESTAMP}_pytest.log"
    ok "pytest finished — see $LOG_DIR/${TIMESTAMP}_pytest.log"
}

# ─── Route to requested step(s) ───────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Day 43 — AdventureWorks Batch Pipeline            ║"
echo "║   Mode: ${STEP}$(printf '%*s' $((47 - ${#STEP})) '')║"
echo "╚══════════════════════════════════════════════════════╝"

START_TIME=$(date +%s)

case "$STEP" in
    all)
        step_extract
        step_register
        step_verify
        step_sales
        step_hr
        step_vendor
        step_rfm
        step_validate
        ;;
    extract)   step_extract   ;;
    register)  step_register  ;;
    verify)    step_verify    ;;
    sales)     step_sales     ;;
    hr)        step_hr        ;;
    vendor)    step_vendor    ;;
    rfm)       step_rfm       ;;
    transform)
        step_sales
        step_hr
        step_vendor
        step_rfm
        ;;
    validate)  step_validate  ;;
    test)      step_test      ;;
    *)
        echo "Unknown step: $STEP"
        echo "Valid options: all | extract | register | verify | sales | hr | vendor | rfm | transform | validate | test"
        exit 1
        ;;
esac

END_TIME=$(date +%s)
ELAPSED=$(( END_TIME - START_TIME ))

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Pipeline complete                                 ║"
printf "║   Elapsed: %ds%-40s║\n" "$ELAPSED" ""
echo "║   Logs: logs/${TIMESTAMP}_*.log"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
