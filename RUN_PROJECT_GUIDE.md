# 🚀 Panduan Lengkap Menjalankan Day 43 Production - Vendor Pipeline

## 📋 Daftar Isi
1. [Prasyarat](#prasyarat)
2. [Setup Environment](#setup-environment)
3. [Setup Infrastructure](#setup-infrastructure)
4. [Setup Database](#setup-database)
5. [Jalankan Vendor Pipeline](#jalankan-vendor-pipeline)
6. [Verifikasi Hasil](#verifikasi-hasil)

---

## Prasyarat

Pastikan sudah terinstall:
- Docker & Docker Compose
- Python 3.11+
- Java 11-17
- Git

### Check versi:
```bash
docker --version
docker-compose --version
python3 --version
java -version
```

---

## 1. Setup Environment

### Langkah 1.1: Clone Repository dan Navigate
```bash
cd /workspaces/day43-production
pwd  # Verify lokasi
```

### Langkah 1.2: Copy File Konfigurasi
```bash
# Copy .env dari template
cp .env.example .env

# Edit .env dengan nilai real (opsional untuk testing)
# nano .env  atau vim .env
```

### Langkah 1.3: Setup Python Virtual Environment
```bash
# Create venv
python3 -m venv .venv

# Activate venv
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip setuptools wheel
```

### Langkah 1.4: Install Dependencies
```bash
# Install semua dependencies (first run ~3-5 min)
pip install -r requirements.txt

# Verify instalasi
python3 -c "import pyspark; print('PySpark:', pyspark.__version__)"
python3 -c "import airflow; print('Airflow:', airflow.__version__)"
```

---

## 2. Setup Infrastructure (Docker)

### Langkah 2.1: Create Network
```bash
# Create Docker network untuk day43
docker network create day43-net
```

### Langkah 2.2: Setup Infrastructure Layer (STEP 1)

**Pre-requisites untuk Hive Metastore:**

```bash
# 1. Create directories
mkdir -p pg-conf hive jars logs data/{raw,curated,sample}

# 2. Create PostgreSQL HBA config
cat > pg-conf/pg_hba.conf << 'HBA'
local   all  all                trust
host    all  all  127.0.0.1/32  md5
host    all  all  ::1/128       md5
host    all  all  0.0.0.0/0     md5
HBA

# 3. Download PostgreSQL JDBC Driver
curl -L -o jars/postgresql-42.7.3.jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

# 4. Copy ke Hive folder juga
cp jars/postgresql-42.7.3.jar hive/postgresql-42.7.3.jar
```

**Start Infrastructure (PostgreSQL + Hadoop NameNode):**

```bash
# Start PostgreSQL dan NameNode
docker-compose -f docker-compose.infra.yml up -d adventureworks-postgres namenode

# Wait for PostgreSQL healthy (~10-15 seconds)
sleep 15

# Check status
docker-compose -f docker-compose.infra.yml ps
```

### Langkah 2.3: Initialize PostgreSQL & Hive Metastore

```bash
# 1. Set PostgreSQL password
docker exec adventureworks-postgres psql -U postgres -c \
  "SET password_encryption='md5'; ALTER USER postgres WITH PASSWORD 'My_password1';"

# 2. Create Hive Metastore database
docker exec adventureworks-postgres psql -U postgres -c \
  "CREATE DATABASE IF NOT EXISTS hive_metastore OWNER postgres;"

# 3. Start DataNode dan build Hive Metastore
docker-compose -f docker-compose.infra.yml up -d datanode

# 4. Initialize Hive schema
docker-compose -f docker-compose.infra.yml build hive-metastore

docker run --rm --network day43-net day43-hive-metastore:local \
  /opt/hive/bin/schematool -dbType postgres -initSchema \
  -url "jdbc:postgresql://adventureworks-postgres:5432/hive_metastore" \
  -driver org.postgresql.Driver -userName postgres -passWord My_password1

# 5. Start Hive Metastore
docker-compose -f docker-compose.infra.yml up -d hive-metastore

# Wait for services healthy
sleep 10
docker-compose -f docker-compose.infra.yml ps
```

---

## 3. Setup Application Layer (STEP 2)

### Langkah 3.1: Verify Infrastructure Healthy

```bash
# Check NameNode
curl http://localhost:9870

# Check Hive Metastore
docker-compose -f docker-compose.infra.yml logs hive-metastore | tail -20

# Check PostgreSQL
docker exec adventureworks-postgres psql -U postgres -c "SELECT version();"
```

### Langkah 3.2: Start Spark & Airflow

```bash
# Start Spark notebook dan Airflow
docker-compose -f docker-compose.apps.yml up -d

# Wait untuk startup (~30 detik)
sleep 30

# Check services
docker-compose -f docker-compose.apps.yml ps

# Verify UIs
# - Spark: http://localhost:4040
# - Jupyter: http://localhost:8888 (token: bigdata2024)
# - Airflow: http://localhost:8080 (user: admin, pass: admin)
```

---

## 4. Setup Database & Extract Data

### Langkah 4.1: Extract from PostgreSQL to HDFS

```bash
# Method 1: Run via bash script (recommended)
bash run_demo.sh extract

# Wait untuk complete (~2-3 min depending on data size)
```

**Atau Method 2: Manual spark-submit:**

```bash
# Ensure dalam directory
cd /workspaces/day43-production

# Run extract
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/hive_to_parquet.py \
  --config configs/pipeline.yaml \
  --step extract

# Verify HDFS
# Open: http://localhost:9870
# Or gunakan command: docker exec hadoop-namenode hdfs dfs -ls /datalake/raw
```

### Langkah 4.2: Register Hive External Tables

```bash
# Register Hive tables
bash run_demo.sh register

# Atau manual:
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/hive_to_parquet.py \
  --config configs/pipeline.yaml \
  --step register
```

---

## 5. Jalankan Vendor Pipeline (ASSIGNMENT 2)

### Langkah 5.1: Verify Data Ready

```bash
# Verify Hive tables ada
docker-compose exec spark pyspark << 'PYSPARK'
spark.sql("SHOW TABLES IN adventureworks").show()
spark.sql("SELECT COUNT(*) FROM adventureworks.fact_purchase_orders").show()
spark.sql("SELECT COUNT(*) FROM adventureworks.dim_vendor").show()
PYSPARK
```

### Langkah 5.2: Run Vendor Pipeline

**Method 1: Bash Script (Recommended)**

```bash
# Run vendor pipeline saja
bash run_demo.sh vendor

# Run dengan analytics queries
bash run_demo.sh vendor --analytics
```

**Method 2: Direct Spark Submit**

```bash
# Activate Python env terlebih dahulu
source .venv/bin/activate

# Run vendor pipeline
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/vendor_pipeline.py \
  --config configs/pipeline.yaml

# Run dengan analytics
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/vendor_pipeline.py \
  --config configs/pipeline.yaml \
  --analytics
```

**Method 3: Via Docker Spark Container**

```bash
# Shell into Spark container
docker-compose exec spark bash

# Di dalam container:
cd /app
source .venv/bin/activate
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/vendor_pipeline.py \
  --config configs/pipeline.yaml --analytics
```

---

## 6. Verifikasi Hasil

### 6.1: Check Output Tables di Hive

```bash
# Option 1: Via pyspark
docker-compose exec spark pyspark << 'PYSPARK'
# Check fact_vendor_performance
df = spark.table("adventureworks_curated.fact_vendor_performance")
print(f"Total rows: {df.count()}")
df.show(5)

# Check vendor_overall_ranking
df2 = spark.table("adventureworks_curated.vendor_overall_ranking")
print(f"Total vendors ranked: {df2.count()}")
df2.show(10)
PYSPARK

# Option 2: Via bash script
bash run_demo.sh validate
```

### 6.2: Validate Data Quality

```bash
# Run test suite
pytest tests/test_pipelines.py::TestVendorPipelineFunctions -v

# Expected: 3 tests pass (jika di Docker dengan Java 11-17)
```

### 6.3: Export Results to CSV (Optional)

```bash
# Export vendor_overall_ranking
docker-compose exec spark pyspark << 'PYSPARK'
df = spark.table("adventureworks_curated.vendor_overall_ranking")
df.coalesce(1).write.mode("overwrite").csv("/tmp/vendor_ranking", header=True)
PYSPARK

# Copy dari container ke local
docker cp hadoop-namenode:/tmp/vendor_ranking ./data/results/

# View hasil
cat ./data/results/vendor_ranking/*.csv
```

---

## 📊 Quick Command Summary

### SETUP AWAL (first time):
```bash
# 1. Setup Python
cd /workspaces/day43-production
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Prepare Docker files
mkdir -p pg-conf hive jars logs data/{raw,curated,sample}
cat > pg-conf/pg_hba.conf << 'HBA'
local   all  all                trust
host    all  all  127.0.0.1/32  md5
host    all  all  ::1/128       md5
host    all  all  0.0.0.0/0     md5
HBA

curl -L -o jars/postgresql-42.7.3.jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
cp jars/postgresql-42.7.3.jar hive/postgresql-42.7.3.jar

# 3. Create network
docker network create day43-net

# 4. Start infrastructure
docker-compose -f docker-compose.infra.yml up -d adventureworks-postgres namenode datanode hive-metastore

sleep 30
```

### EVERY TIME (run pipeline):
```bash
# Activate env
cd /workspaces/day43-production
source .venv/bin/activate

# Run vendor pipeline
bash run_demo.sh vendor

# Atau manual:
spark-submit --jars jars/postgresql-42.7.3.jar \
  jobs/vendor_pipeline.py --config configs/pipeline.yaml
```

### STOP EVERYTHING:
```bash
# Stop Docker containers
docker-compose -f docker-compose.apps.yml down
docker-compose -f docker-compose.infra.yml down

# Deactivate venv
deactivate

# Clean Docker network (optional)
docker network rm day43-net
```

---

## 🔧 Troubleshooting

### Issue: Port sudah dipakai
```bash
# Check port
lsof -i :9870   # NameNode
lsof -i :5433   # PostgreSQL
lsof -i :4040   # Spark UI

# Kill process
kill -9 <PID>
```

### Issue: Docker network error
```bash
# Recreate network
docker network rm day43-net
docker network create day43-net
docker-compose down
docker-compose -f docker-compose.infra.yml up -d
```

### Issue: PostgreSQL password error
```bash
# Reset password
docker exec adventureworks-postgres psql -U postgres -c \
  "SET password_encryption='md5'; ALTER USER postgres WITH PASSWORD 'My_password1';"
```

### Issue: Hive schema tidak initialized
```bash
# Re-init Hive schema
docker-compose -f docker-compose.infra.yml build hive-metastore

docker run --rm --network day43-net day43-hive-metastore:local \
  /opt/hive/bin/schematool -dbType postgres -initSchema \
  -url "jdbc:postgresql://adventureworks-postgres:5432/hive_metastore" \
  -driver org.postgresql.Driver -userName postgres -passWord My_password1
```

---

## 📈 Expected Output

**Vendor Pipeline Log Output:**
```
2026-05-19 02:45:30 | INFO | Vendor Pipeline started
2026-05-19 02:45:35 | INFO | Extract complete | tables=5
2026-05-19 02:45:40 | INFO | Enriched rows: 4012
2026-05-19 02:46:00 | INFO | Vendor summary rows: 52
2026-05-19 02:46:05 | INFO | Transform complete | fact_vendor_performance rows=52 | vendor_overall_ranking rows=24
2026-05-19 02:46:10 | INFO | Vendor pipeline complete | vendors=24 | fact_vendor_performance rows=52
```

**Vendor Overall Ranking (Sample):**
```
vendor_id | vendor_name        | credit_rating | overall_score | overall_rank
1         | Vendor A           | 2             | 87.50        | 1
2         | Vendor B           | 3             | 84.25        | 2
3         | Vendor C           | 1             | 81.75        | 3
...
```

---

## ✅ Checklist Completion

- [ ] Python 3.11+ installed
- [ ] Docker & Docker Compose running
- [ ] .env file copied dan configured
- [ ] Virtual environment created
- [ ] Dependencies installed
- [ ] Docker network created
- [ ] PostgreSQL & Hadoop running
- [ ] Hive Metastore initialized
- [ ] Spark & Airflow containers running
- [ ] Data extracted to HDFS
- [ ] Hive tables registered
- [ ] Vendor pipeline executed successfully
- [ ] Output tables verified

---

**✨ Setelah semua selesai, vendor pipeline siap untuk production! ✨**
