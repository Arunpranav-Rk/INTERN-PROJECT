# Real-Time Streaming Pipeline with AI-Based Monitoring

## 📌 Overview
This project implements a **real-time streaming data pipeline** with automated monitoring, anomaly detection, and AI-based root cause analysis.  
The pipeline ingests sensor data from Kafka, processes it using PySpark Structured Streaming, stores results in **Delta Lake on Azure Data Lake Storage Gen2**, and uses **Isolation Forest** for anomaly detection.  
Detected anomalies are analyzed by an **LLM (Gemini / LLaMA)**, and formatted alert emails are sent to the DevOps team.

---

## ⚙️ Architecture
1. **Data Ingestion** – Kafka producer sends sensor data with timestamps.
2. **Stream Processing** – PySpark Structured Streaming consumes Kafka data, performs transformations, and writes to Delta tables.
3. **Metrics Collector** – Calculates Kafka lag, input rate, rows per minute, and event delays.
4. **Anomaly Detection (ML)** – Isolation Forest identifies abnormal trends.
5. **Root Cause Analysis (LLM)** – Analyzes anomaly logs and generates human-readable summaries.
6. **Alerting** – Sends email notifications with severity level, affected components, and recommended actions.

---

## 🛠️ Technologies Used
- **Apache Kafka** – Message broker for real-time ingestion
- **PySpark Structured Streaming** – Data processing
- **Delta Lake** – Storage format for streaming data
- **Azure Data Lake Storage Gen2 (ADLS Gen2)** – Cloud storage
- **Isolation Forest (scikit-learn)** – Anomaly detection
- **LLM API (Groq / Gemini)** – Root cause analysis
- **SMTP (Gmail)** – Email alerts



###  Install Dependencies
```bash

#Update System Packages
sudo apt update && sudo apt upgrade -y

#Install Java (Required for Spark & Kafka)
#We use OpenJDK 11 for maximum compatibility.
sudo apt install openjdk-11-jdk -y

#Install Python
#Use Python 3.10.x (Python 3.11+ can cause some PySpark dependency issues).
sudo apt install python3.10 python3.10-venv python3-pip -y

#Install Common Utilities
sudo apt install curl wget unzip git -y

#Install Apache Kafka & Zookeeper
#We use Kafka 3.7.x (without Confluent).
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xvzf kafka_2.13-3.7.0.tgz
mv kafka_2.13-3.7.0 kafka

#Install Apache Spark
#We use Spark 3.5.1 with Delta Lake jars.
wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
mv spark-3.5.1-bin-hadoop3 spark

# Add Delta Lake & Azure Integration JARs
#Required JARs (place them in spark/jars/):

delta-core_2.12-3.0.0.jar

delta-storage-3.0.0.jar

spark-sql-kafka-0-10_2.12-3.5.1.jar

kafka-clients-3.5.1.jar

hadoop-azure-3.3.2.jar

hadoop-azure-datalake-3.3.2.jar

azure-storage-8.6.6.jar

pip install -r requirements.txt

# run every file within the virtual env to avoid version errors
source venv/bin/activate

#Start kafka and zookeeper
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties

#Start Kafka producer
python producer.py

#Start spark streaming
python consumer.py

#Run Metrics Collector
python metrics.py

#Start ML Anomaly Detector
python ml.py

#Enable LLM & Email Alerts
python llm.py

```
## 📂 Folder Structure

.
├── kafka_producer.py # Sends sample sensor data to Kafka topic

├── spark_streaming.py # Reads from Kafka, writes to Delta

├── metrics_collector.py # Calculates metrics every 1 min

├── ml.py # Runs Isolation Forest anomaly detection

├── llm.py # Analyzes anomalies and sends emails

├── anomaly.py # Helper functions for anomaly logging

├── requirements.txt # Python dependencies

├── README.md # Project documentation

└── email_style.css # Styling for email alerts




