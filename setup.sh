#!/usr/bin/env bash
# setup.sh
# =========
# One-shot environment setup for Day 43 — local machine or Codespace.
# Safe to re-run — all steps are idempotent.
#
# Usage:
#   bash setup.sh

set -euo pipefail

# ── Resolve project root (works whether called from inside or outside dir) ──
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ─── Helpers ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}  ✓ $*${NC}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${NC}"; }
fail() { echo -e "${RED}  ✗ $*${NC}"; exit 1; }
step() { echo ""; echo "[$1/$TOTAL_STEPS] $2"; }
TOTAL_STEPS=6

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Day 43 — Batch Processing with PySpark            ║"
echo "║   Environment Setup                                 ║"
echo "╚══════════════════════════════════════════════════════╝"

# ── 1. Prerequisites check ────────────────────────────────────────────────────
step 1 "Checking prerequisites"

python3 --version >/dev/null 2>&1 || fail "Python 3.11+ required. Install from python.org"
PYTHON_MINOR=$(python3 -c "import sys; print(sys.version_info.minor)")
[ "$PYTHON_MINOR" -ge 10 ] || warn "Python 3.10+ recommended (found 3.$PYTHON_MINOR)"
ok "Python $(python3 --version)"

java -version >/dev/null 2>&1 || fail "Java 11+ required for PySpark. Install OpenJDK 11."
ok "Java $(java -version 2>&1 | head -1)"

# ── 2. Virtual environment ────────────────────────────────────────────────────
step 2 "Python virtual environment"

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    ok ".venv created"
else
    ok ".venv already exists"
fi

# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip setuptools wheel --quiet
ok "pip upgraded"

# ── 3. Install dependencies ───────────────────────────────────────────────────
step 3 "Installing Python dependencies"

echo "    Installing from requirements.txt (first run ~3-5 min)..."
pip install -r requirements.txt --quiet
ok "pyspark    $(python -c 'import pyspark; print(pyspark.__version__)')"
ok "airflow    $(airflow version 2>/dev/null | head -1 || echo 'pending init')"
ok "pytest     $(pytest --version 2>/dev/null | head -1)"

# ── 4. Environment file ───────────────────────────────────────────────────────
step 4 "Environment configuration"

if [ ! -f ".env" ]; then
    cp .env.example .env
    ok ".env created from .env.example"
    warn "Edit .env with your actual connection details before running the pipeline"
else
    ok ".env already exists"
fi

# Load .env into current shell
set -a
# shellcheck disable=SC1091
source .env 2>/dev/null || true
set +a

# ── 5. Airflow initialization ─────────────────────────────────────────────────
step 5 "Initializing Airflow"

export AIRFLOW_HOME="${AIRFLOW_HOME:-/tmp/airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////${AIRFLOW_HOME}/airflow.db"
export AIRFLOW__WEBSERVER__SECRET_KEY="day43-dev-secret-key"

mkdir -p "$AIRFLOW_HOME/logs" "$AIRFLOW_HOME/plugins"

# Migrate DB (Airflow 2.7+) or init (older)
if airflow db migrate > /dev/null 2>&1; then
    ok "Airflow DB migrated"
elif airflow db init > /dev/null 2>&1; then
    ok "Airflow DB initialized"
else
    warn "Airflow DB init had warnings — may still work"
fi

# Create admin user (suppress 'already exists' error)
airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@day43.local \
    > /dev/null 2>&1 && ok "Airflow admin user: admin/admin" \
    || ok "Airflow admin user already exists"

# Register Airflow connections
bash configs/airflow_connections.sh > /dev/null 2>&1 \
    && ok "Airflow connections registered (spark_local, postgres, hive_cli)" \
    || warn "Connection registration skipped — run manually: bash configs/airflow_connections.sh"

# ── 6. Data directories & PYTHONPATH ─────────────────────────────────────────
step 6 "Workspace preparation"

mkdir -p data/raw data/curated data/sample logs
ok "data/ and logs/ directories ready"

# Add project root to PYTHONPATH so utils/ and jobs/ resolve in tests
PROJ_ROOT="$(pwd)"
if [ -f "$HOME/.bashrc" ] && ! grep -q "day43-production" "$HOME/.bashrc" 2>/dev/null; then
    {
        echo ""
        echo "# Day 43 PySpark project"
        echo "export PYTHONPATH=\"$PROJ_ROOT:\$PYTHONPATH\""
        echo "[ -f \"$PROJ_ROOT/.venv/bin/activate\" ] && source \"$PROJ_ROOT/.venv/bin/activate\""
    } >> "$HOME/.bashrc"
    ok "PYTHONPATH added to ~/.bashrc"
fi
export PYTHONPATH="$PROJ_ROOT:${PYTHONPATH:-}"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Setup complete!                                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║                                                     ║"
echo "║  Next steps:                                        ║"
echo "║                                                     ║"
echo "║  1. Edit .env with your connection details          ║"
echo "║                                                     ║"
echo "║  2. Run pipeline:                                   ║"
echo "║       bash run_demo.sh                              ║"
echo "║                                                     ║"
echo "║  3. Run tests:                                      ║"
echo "║       pytest tests/ -v                              ║"
echo "║                                                     ║"
echo "║  4. Start Airflow:                                  ║"
echo "║       airflow standalone                            ║"
echo "║       → http://localhost:8080  (admin/admin)        ║"
echo "║                                                     ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
