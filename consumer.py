from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType
from anomaly import log_anomaly
import os
import traceback
import json

def infer_schema_from_sample(json_sample):
    json_rdd = spark.sparkContext.parallelize([json_sample])
    df = spark.read.json(json_rdd)
    return df.schema

if __name__ == "__main__":
    access_key = os.environ["AZURE_STORAGE_KEY"]

    # Spark session with Delta configs
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

    print("✅ Spark session created.")

    # Read stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ts-stream") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    try:
        sample_json = kafka_df.selectExpr("CAST(value AS STRING)").limit(1).collect()[0][0]
        inferred_schema = infer_schema_from_sample(sample_json)
    except Exception:
        inferred_schema = StructType().add("sensor", "string").add("value", "double")

    # Parse Kafka messages 
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), inferred_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ts", current_timestamp())
    '''
    # Print stream to console
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    '''
    # Write to Delta Lake
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/checkpoints/sensor_data/") \
        .option("path", "abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/sensor_data/") \
        .start()
    
    try:
        print("📶 Streaming started. Waiting for Kafka messages...")
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n🛑 Ctrl+C detected. Stopping the stream...")
        query.stop()

    except Exception as e:
        log_anomaly(spark, "Spark Job Error", traceback.format_exc())
        print("❌ Main Loop Exception:", e)

    finally:
        spark.stop()
        print("✅ Spark session stopped.")

