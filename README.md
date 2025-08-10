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
pip install -r requirements.txt


zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties




