-- initdb-extra/create_databases.sql
-- File ini dijalankan oleh Postgres saat container pertama kali start
-- (setelah semua file di /docker-entrypoint-initdb.d/ lainnya selesai)
-- Membuat database untuk Hive Metastore dan Airflow agar tidak race condition.

SELECT 'CREATE DATABASE hive_metastore OWNER postgres'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hive_metastore')\gexec

SELECT 'CREATE DATABASE airflow OWNER postgres'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
