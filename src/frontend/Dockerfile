FROM python:3.14-slim

# Java is required by PySpark
RUN apt update && apt install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Pre-download the Spark-Kafka connector jar so it is available offline at runtime
RUN python3 -c "\
from pyspark.sql import SparkSession; \
s = SparkSession.builder \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0') \
    .getOrCreate(); \
s.stop()"

COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "src/app.py", \
    "--server.port=8501", \
    "--server.address=0.0.0.0", \
    "--server.headless=true"]


