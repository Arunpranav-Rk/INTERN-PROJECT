from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np
import time
import traceback
import os, sys
from anomaly import log_anomaly

# --- CONFIGS ---
access_key = os.environ["AZURE_STORAGE_KEY"]
metrics_path = "abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/metrics/"
latest_n = 30
interval = 60  

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("MLAnomalyDetection") \
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
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "abfs://container1@pipelinestorage01.dfs.core.windows.net/") \
    .config("spark.hadoop.fs.azure.account.key.pipelinestorage01.dfs.core.windows.net", access_key) \
    .getOrCreate()

print("\n✅ Spark ML session started.")

try:
    while True:
        count = 0

        # Load recent N records
        df = spark.read.format("delta").load(metrics_path).orderBy(col("timestamp").desc()).limit(latest_n)
        pdf = df.select(
            "timestamp", "kafka_lag", "rows_last_minute", "input_rate"
        ).toPandas().dropna()

        if len(pdf) < 10:
            print("Not enough data to apply anomaly detection...")
        else:
            features = ["rows_last_minute", "input_rate"]
            model = IsolationForest(contamination=0.1, random_state=42)
            pdf["anomaly"] = model.fit_predict(pdf[features])

            count = (pdf["anomaly"] == -1).sum()
            if count >= 3:
                for i, row in pdf.iterrows():
                    if row["anomaly"] == -1:
                        details = " | ".join([f"{col}: {row[col]}" for col in features])
                        log_anomaly(spark,"Warning: Abrupt change of input values",f"Anomaly at {details}")
                        print(f"\n✅ Anomaly logged at time: {row['timestamp']}")

            print(f"\n✅ Anomaly detection completed...total anomaly count is {count}")
        time.sleep(interval)

except KeyboardInterrupt:
    print("\n🔴 Stopped by user...")

except Exception as e:
    log_anomaly(spark, "ML Error", traceback.format_exc())
    print("\n❌ ML model error:", e)

finally:
    spark.stop()
