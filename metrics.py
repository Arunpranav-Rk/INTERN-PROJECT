from anomaly import log_anomaly
import subprocess
import time
from datetime import datetime, timedelta
import traceback
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as max_, col

access_key = os.environ["AZURE_STORAGE_KEY"]

# === CONFIG ===
kafka_topic = "ts-stream"
consumer_group = "consumer_group_01"
metrics_path = "abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/metrics/"
sensor_path = "abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/sensor_data/"
interval = 60  # in seconds

# === START SPARK ===
spark = SparkSession.builder \
    .appName("MonitorMetrics") \
    .config("spark.jars.packages", ",".join([
        "io.delta:delta-spark_2.12:3.0.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.kafka:kafka-clients:3.5.1",
        "org.apache.hadoop:hadoop-azure:3.3.2",
        "org.apache.hadoop:hadoop-azure-datalake:3.3.2",
        "com.microsoft.azure:azure-storage:8.6.6",
        "org.apache.commons:commons-pool2:2.11.1"
    ])) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.fs.defaultFS", "abfs://container1@pipelinestorage01.dfs.core.windows.net/") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("fs.azure.account.key.pipelinestorage01.dfs.core.windows.net", access_key) \
    .config("spark.sql.session.timeZone", "Asia/Kolkata") \
    .getOrCreate()

print("✅ Spark session started.")

# === Get Kafka Lag ===
def kafka_lag():
    try:
        cmd = f"/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group {consumer_group}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        for line in result.stdout.strip().split("\n"):
            if kafka_topic in line:
                parts = line.split()
                return int(parts[-1]) if parts[-1].isdigit() else 0
        return 0
    except Exception as e:
        log_anomaly(spark, "Kafka Lag", str(e))
        return -1

# === Rows in last X seconds ===
def recent_rows(df, since):
    try:
        new_df = df.filter(col("ts") >= since)
        return new_df, new_df.count()
    except Exception as e:
        log_anomaly(spark, "Recent Rows", str(e))
        return None, 0

# === Input rate ===
def input_rate(count, interval):
    try:
        return count / interval if count else 0.0
    except Exception as e:
        log_anomaly(spark, "Input Rate", str(e))
        return 0.0

# === Latest timestamp ===
def latest_ts(df):
    try:
        return df.select(max_("ts")).collect()[0][0]
    except Exception as e:
        log_anomaly(spark, "Latest TS", str(e))
        return None

# === Build metrics dataframe ===
def make_metrics_df(lag, new_count, rate, latest):
    now = datetime.now()
    data = {
        "timestamp": [now],
        "kafka_lag": [lag],
        "rows_last_minute": [new_count],
        "input_rate": [rate],
        "latest_event_ts": [latest]
    }
    return spark.createDataFrame(pd.DataFrame(data))

# === MAIN LOOP ===
def monitor():
    while True:
        try:
            lag = kafka_lag()
            df = spark.read.format("delta").load(sensor_path)
            since = datetime.now() - timedelta(seconds=interval)
            new_df, new_count = recent_rows(df, since)
            rate = input_rate(new_count, interval)
            latest = latest_ts(df)

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📊 Lag: {lag}, "
                  f"New Rows: {new_count}, Rate: {rate:.2f}/s, Latest: {latest}")

            try:
                metrics_df = make_metrics_df(lag, new_count, rate, latest)
                metrics_df.repartition(1).write.format("delta").mode("append").save(metrics_path)
                print("✅ Metrics written to Delta.")
            except Exception as e:
                log_anomaly(spark, "Metrics Write", traceback.format_exc())
                print("❌ Write failed:", e)

            time.sleep(interval)

        except KeyboardInterrupt:
            print("\n🛑 Interrupted.")
            break
        except Exception as e:
            log_anomaly(spark, "Main Loop", traceback.format_exc())
            print("❌ Main loop error:", e)

    spark.stop()

if __name__ == "__main__":
    monitor()
