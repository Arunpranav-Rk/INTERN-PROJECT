from pyspark.sql import Row
from datetime import datetime
import os
import subprocess
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col

def log_anomaly(spark, source, message, path="abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/anomaly/"):
    try:
        now = datetime.now()

        source = str(source)
        message = str(message)

        df = spark.createDataFrame(
            [Row(timestamp=now, source=source, message=message)]
        )
        df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

        df.write.format("delta").mode("append").save(path)

        print("✅ Anomaly logged successfully.")

        # Call llm.py 
        subprocess.Popen(["python3", "llm.py"])
    except Exception as e:
        print("❌ Failed to log anomaly:", e)
