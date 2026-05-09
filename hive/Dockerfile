FROM apache/hive:3.1.3

USER root

# Hapus driver lama, ganti dengan 42.7.3 yang support SCRAM-SHA-256
RUN find /opt/hive/lib -name "postgresql-*.jar" -delete && \
    find /opt/hadoop -name "postgresql-*.jar" -delete 2>/dev/null || true

COPY postgresql-42.7.3.jar /opt/hive/lib/postgresql-42.7.3.jar

USER hive