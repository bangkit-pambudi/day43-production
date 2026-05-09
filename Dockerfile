FROM apache/airflow:2.9.1-python3.11

# Switch to root to install system-level Java and procps
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean

# Switch back to the airflow user to install Python packages
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyyaml python-dotenv --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"