# ⚡ QUICK COMMANDS - Vendor Pipeline

## 🚀 FIRST TIME SETUP (5-10 menit)

```bash
# 1. Setup Python environment
cd /workspaces/day43-production
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Prepare directories & configs
mkdir -p pg-conf hive jars logs data/{raw,curated,sample}

# 3. Create PostgreSQL config
cat > pg-conf/pg_hba.conf << 'HBA'
local   all  all                trust
host    all  all  127.0.0.1/32  md5
host    all  all  ::1/128       md5
host    all  all  0.0.0.0/0     md5
HBA

# 4. Download PostgreSQL JDBC driver
curl -L -o jars/postgresql-42.7.3.jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
cp jars/postgresql-42.7.3.jar hive/postgresql-42.7.3.jar

# 5. Create Docker network
docker network create day43-net

# 6. Start infrastructure (PostgreSQL + Hadoop)
docker-compose -f docker-compose.infra.yml up -d \
  adventureworks-postgres namenode datanode hive-metastore

sleep 30
```

---

## 🔄 RUNNING VENDOR PIPELINE (Standard)

```bash
# Activate environment
cd /workspaces/day43-production
source .venv/bin/activate

# RUN VENDOR PIPELINE (simplest)
bash run_demo.sh vendor
```

---

## 📊 FULL PIPELINE (Extract → Register → Vendor)

```bash
# Step 1: Extract data PostgreSQL → HDFS
bash run_demo.sh extract

# Step 2: Register Hive tables
bash run_demo.sh register

# Step 3: Run vendor pipeline
bash run_demo.sh vendor

# Step 4: Validate results
bash run_demo.sh validate
```

---

## 🔧 ALTERNATIVE COMMANDS (Manual)

### Extract
```bash
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/hive_to_parquet.py --config configs/pipeline.yaml --step extract
```

### Register Hive Tables
```bash
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/hive_to_parquet.py --config configs/pipeline.yaml --step register
```

### Vendor Pipeline (with analytics)
```bash
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/vendor_pipeline.py --config configs/pipeline.yaml --analytics
```

### Verify Results (pyspark)
```bash
docker-compose exec spark pyspark << 'PYSPARK'
spark.sql("SELECT COUNT(*) FROM adventureworks_curated.fact_vendor_performance").show()
spark.sql("SELECT COUNT(*) FROM adventureworks_curated.vendor_overall_ranking").show()
spark.table("adventureworks_curated.vendor_overall_ranking").show(10)
PYSPARK
```

---

## ✅ QUICK CHECKS

### Check services running
```bash
docker-compose -f docker-compose.infra.yml ps
docker-compose -f docker-compose.apps.yml ps
```

### Check HDFS
```bash
curl http://localhost:9870
# or
docker exec hadoop-namenode hdfs dfs -ls /datalake/raw
```

### Check PostgreSQL
```bash
docker exec adventureworks-postgres psql -U postgres -c "SELECT version();"
```

### Check Hive tables
```bash
docker-compose exec spark pyspark << 'PYSPARK'
spark.sql("SHOW TABLES IN adventureworks").show()
PYSPARK
```

---

## 🛑 STOP EVERYTHING

```bash
# Stop all containers
docker-compose -f docker-compose.apps.yml down
docker-compose -f docker-compose.infra.yml down

# Deactivate Python env
deactivate

# (Optional) Remove network
docker network rm day43-net
```

---

## 📋 WEB INTERFACES

While running:
- **Spark UI**: http://localhost:4040
- **HDFS NameNode**: http://localhost:9870
- **Jupyter**: http://localhost:8888 (token: bigdata2024)
- **Airflow**: http://localhost:8080 (user: admin, pass: admin)

---

## 🎯 TYPICAL WORKFLOW

```bash
# 1. First time (one-time setup)
cd /workspaces/day43-production
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdir -p pg-conf hive jars logs data/{raw,curated,sample}
# ... follow setup steps above ...

# 2. Every time you need to run pipeline
source .venv/bin/activate
bash run_demo.sh vendor

# 3. Check results
docker-compose exec spark pyspark << 'PYSPARK'
spark.table("adventureworks_curated.vendor_overall_ranking").show(10)
PYSPARK
```

---

## ⚠️ COMMON ERRORS & FIXES

**Port already in use:**
```bash
lsof -i :9870  # Check NameNode port
# Kill if needed: kill -9 <PID>
```

**Hive tables not found:**
```bash
# Re-register tables
bash run_demo.sh register
```

**PostgreSQL password error:**
```bash
docker exec adventureworks-postgres psql -U postgres -c \
  "SET password_encryption='md5'; ALTER USER postgres WITH PASSWORD 'My_password1';"
```

**Docker network issue:**
```bash
docker network rm day43-net
docker network create day43-net
```

---

📖 **Full guide**: See `RUN_PROJECT_GUIDE.md`
