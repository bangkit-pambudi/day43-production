#!/usr/bin/env bash
# .devcontainer/post_create.sh
# =============================
# Runs automatically after Codespace container is created.
# Installs all dependencies, initializes Airflow, prepares data dirs.
#
# Designed to be idempotent — safe to re-run.

set -euo pipefail

WORKSPACE="/workspaces/day43-production"
cd "$WORKSPACE"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Day 43 — Post-Create Setup                        ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. System packages ────────────────────────────────────────────────────────
echo "[1/6] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
    procps \
    curl \
    wget \
    unzip \
    > /dev/null 2>&1
echo "      System packages OK"

# ── 2. Java check (required by PySpark) ──────────────────────────────────────
echo "[2/6] Checking Java..."
if ! java -version 2>/dev/null; then
    echo "      Java not found — installing OpenJDK 11..."
    sudo apt-get install -y --no-install-recommends openjdk-11-jdk-headless > /dev/null 2>&1
fi
java -version 2>&1 | head -1
echo "      JAVA_HOME=$(java -XshowSettings:all -version 2>&1 | grep java.home | awk '{print $3}')"

# ── 3. Python virtual environment ────────────────────────────────────────────
echo "[3/6] Setting up Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "      .venv created"
else
    echo "      .venv already exists"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

pip install --upgrade pip setuptools wheel --quiet

echo "[3/6] Installing Python dependencies (may take 3-5 min first time)..."
pip install -r requirements.txt --quiet
echo "      Dependencies installed"
echo "      PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"
echo "      Airflow: $(airflow version 2>/dev/null || echo 'not yet configured')"

# ── 4. Environment file ───────────────────────────────────────────────────────
echo "[4/6] Environment configuration..."
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo "      .env created from template"
    echo "      ⚠  Edit .env with your PostgreSQL / HDFS / Hive connection details"
else
    echo "      .env already exists"
fi

# ── 5. Airflow initialization ─────────────────────────────────────────────────
echo "[5/6] Initializing Airflow..."
export AIRFLOW_HOME="${AIRFLOW_HOME:-/tmp/airflow}"
export AIRFLOW__CORE__DAGS_FOLDER="$WORKSPACE/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////${AIRFLOW_HOME}/airflow.db"

mkdir -p "$AIRFLOW_HOME/logs" "$AIRFLOW_HOME/plugins"

# Migrate DB (Airflow 2.7+) or init (older)
airflow db migrate 2>/dev/null || airflow db init 2>/dev/null || true

# Create admin user (ignore if already exists)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@day43.local \
    > /dev/null 2>&1 || true

# Register Airflow connections
bash configs/airflow_connections.sh > /dev/null 2>&1 || true

echo "      Airflow ready | AIRFLOW_HOME=$AIRFLOW_HOME"
echo "      Login: admin / admin"

# ── 6. Data directories & PYTHONPATH ─────────────────────────────────────────
echo "[6/6] Preparing workspace..."
mkdir -p data/raw data/curated data/sample logs

# Add workspace to PYTHONPATH so utils/ and jobs/ are importable in tests
BASHRC="${HOME}/.bashrc"
if ! grep -q "day43-production" "$BASHRC" 2>/dev/null; then
    echo "" >> "$BASHRC"
    echo "# Day 43 — PySpark project path" >> "$BASHRC"
    echo "export PYTHONPATH=\"$WORKSPACE:\$PYTHONPATH\"" >> "$BASHRC"
    echo "source $WORKSPACE/.venv/bin/activate" >> "$BASHRC"
fi
export PYTHONPATH="$WORKSPACE:${PYTHONPATH:-}"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Setup complete!                                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║                                                     ║"
echo "║  Quick start:                                       ║"
echo "║    bash run_demo.sh            # full pipeline      ║"
echo "║    pytest tests/ -v            # run tests          ║"
echo "║    airflow standalone          # start Airflow UI   ║"
echo "║    → http://localhost:8080     # admin / admin      ║"
echo "║                                                     ║"
echo "║  Edit .env with your DB connection details first!   ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
