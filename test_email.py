import os
import smtplib
import requests
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

load_dotenv()

access_key = os.environ["AZURE_STORAGE_KEY"]

# === Config ===
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
ANOMALY_PATH = "abfs://container1@pipelinestorage01.dfs.core.windows.net/delta/anomaly/"
RATE_LIMIT_FILE = "last_sent.txt"
EMAIL_INTERVAL_MINUTES = 3

spark = SparkSession.builder.appName("LLM Root Cause Analyzer") \
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

def get_latest_anomaly_row():
    try:
        df = spark.read.format("delta").load(ANOMALY_PATH)
        row = df.orderBy(desc("timestamp")).limit(1).collect()[0]
        return {
            "timestamp": row["timestamp"],
            "source": row["source"],
            "message": row["message"]
        }
    except Exception as e:
        print("❌ Error loading Delta table:", e)
        return None

def analyze_log_with_llama(source, message):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    if source.strip().startswith("Warning: Abrupt change of input values"):
        prompt = f"""
You are an MLOps assistant reviewing anomaly detection results in a real-time data pipeline.

### Pipeline Context:
- Kafka ingests real-time sensor data.
- Spark processes and stores data in Delta Lake on ADLS Gen2.
- Every minute, a metrics collector logs:
  - `input_rate` (normal avg: 0.40 rows/sec)
  - `rows_last_minute` (normal avg: 25 rows/minute)
- An Isolation Forest model flags anomalies based on these metrics.

### Your Task:
Analyze the following anomaly log and generate:
1. Severity (Warning or Critical)
2. Likely reason for anomaly
3. Which metric(s) deviated and how
4. How it differs from expected values
5. Actionable recommendations

### Log:
{message}
"""
    else:
        prompt = f"""
You are a DevOps assistant designed to monitor and debug a real-time data streaming pipeline with the following architecture:

- Data Ingestion: Kafka receives sensor data with a timestamp field (`ts`).
- Processing Engine: Spark Structured Streaming reads Kafka messages, performs transformations, and writes metrics to a Delta Lake table on Azure Data Lake Storage Gen2 (ADLS Gen2).
- Monitoring Layer:
    - Metrics such as Kafka lag, input rate, processing rate, and event delay are calculated every 1 minute.
    - These metrics are analyzed by an Isolation Forest ML model to detect anomalies.
    - If anomalies persist, logs related to the issue are saved in the 'anomaly' folder.
- LLM-based Root Cause Analysis:
    - This log is analyzed by an LLM (you) to extract a human-readable explanation.
    - An email notification with severity, root cause, and suggestions is sent to the DevOps team.

### Your Task:
Analyze the following anomaly log and generate:
1. Identify the severity of the issue (`Warning` or `Critical`).
2. Analyze the root cause of the anomaly within the context of this pipeline.
3. List the affected components (e.g., Kafka, Spark, Delta Lake, ADLS, ML, Metrics, etc.).
4. Extract any important metric values from the log (e.g., Kafka lag, input rate, etc.).
5. Provide actionable recommendations to resolve or investigate the issue.

### Log to analyze:
{message}
"""

    data = {
        "model": "llama3-70b-8192",
        "messages": [
            {"role": "system", "content": "You are a DevOps pipeline assistant."},
            {"role": "user", "content": prompt}
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    else:
        return f"❌ Error from LLM API: {response.text}"

def load_css():
    with open("email_style.css", "r") as f:
        return f.read()

def send_email(subject, body_html):
    msg = MIMEMultipart("alternative")
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())

def check_rate_limit():
    try:
        if not os.path.exists(RATE_LIMIT_FILE):
            return True
        with open(RATE_LIMIT_FILE, 'r') as f:
            last_sent = float(f.read().strip())
        minutes_passed = (time.time() - last_sent) / 60
        return minutes_passed > EMAIL_INTERVAL_MINUTES
    except:
        return True

def update_rate_limit():
    with open(RATE_LIMIT_FILE, 'w') as f:
        f.write(str(time.time()))

def main():
    if not check_rate_limit():
        print("⏱️ Rate limit active. Skipping this run.")
        return

    row = get_latest_anomaly_row()
    if not row:
        print("🚫 No anomalies found.")
        return

    summary = analyze_log_with_llama(row['source'], row['message'])
    css = load_css()

    html_body = f"""
    <html>
    <head>
        <style>{css}</style>
    </head>
    <body>
        <h2>🚨 Streaming Alert Notification</h2>

        <div class="section">
            <div class="section-title">🕒 Timestamp</div>
            <div class="content">{row['timestamp']}</div>
        </div>

        <div class="section">
            <div class="section-title">📦 Source Component</div>
            <div class="content">{row['source']}</div>
        </div>

        <div class="section">
            <div class="section-title">📄 Raw Log Message</div>
            <div class="content"><pre>{row['message']}</pre></div>
        </div>

        <div class="section">
            <div class="section-title">🧠 LLM Diagnostic Summary</div>
            <div class="content"><pre>{summary}</pre></div>
        </div>
    </body>
    </html>
    """

    subject = f"🚨 Streaming Alert from {row['source']} at {row['timestamp']}"
    send_email(subject, html_body)
    update_rate_limit()
    print(f"✅ Alert Email Sent at {datetime.now()}")

    spark.stop()

if __name__ == "__main__":
    main()
