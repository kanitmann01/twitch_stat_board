# Real-Time Twitch Analytics Pipeline 🎮 📊

![Python](https://img.shields.io/badge/Python-3.12+-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Confluent-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-blue)
![dbt](https://img.shields.io/badge/dbt-Transformation-orange)

## 📖 Project Overview
This project is an end-to-end **ELT (Extract, Load, Transform)** streaming pipeline that ingests real-time viewership data from Twitch and visualizes trends in Looker Studio.

The pipeline monitors live streams for **Age of Empires II**, processes events through **Apache Kafka**, loads them into **Snowflake** via a custom Python consumer, and transforms the raw JSON data using **dbt** for analytics.

View it here: [Looker Studio](https://lookerstudio.google.com/s/jyb_uKEUcmo)
### 🎯 Goal
To demonstrate a "Modern Data Stack" architecture capable of handling real-time data ingestion, cloud warehousing, and automated transformation.

---

## 🏗 Architecture
**Twitch API** $\rightarrow$ **Python Producer** $\rightarrow$ **Kafka (Confluent)** $\rightarrow$ **Snowflake (Raw)** $\rightarrow$ **dbt (Transformation)** $\rightarrow$ **Looker Studio**

*(Note: To be added)*

---

## 🛠 Tech Stack
* **Source:** [Twitch API (Helix)](https://dev.twitch.tv/docs/api/)
* **Ingestion:** Python (`twitchAPI`, `confluent_kafka`)
* **Streaming:** Apache Kafka (Confluent Cloud)
* **Warehouse:** Snowflake (Standard Edition)
* **Transformation:** dbt (data build tool)
* **Visualization:** Looker Studio
* **Orchestration:** Python (with potential for Airflow upgrade)

---

## ⚡ How It Works

### 1. Ingestion (Producer)
A Python script (`fetch_data.py`) polls the Twitch API every 60 seconds for live streams of *Age of Empires II*.
* Extracts metadata: `streamer_name`, `viewer_count`, `started_at`, `language`.
* Serializes data to JSON.
* Pushes messages to the `twitch_aoe2_stream_data` Kafka topic.

### 2. Storage (Consumer)
A Python consumer script listens to the Kafka topic.
* Batches messages for efficiency.
* Loads raw JSON directly into a **Snowflake VARIANT** column (`RAW_DATA`) in the `STREAM_LOGS` table.

### 3. Transformation (dbt)
dbt models clean and normalize the data.
* **Model:** `stg_twitch_streams.sql`
* Parses JSON fields into structured columns.
* Calculates stream duration.
* **Data Quality:** Filters out streams with 0 viewers to prevent visualization skew.

---

## 🚀 Setup & Usage

### 1. Prerequisites
* Python 3.12+
* Confluent Cloud Account (Free tier)
* Snowflake Account (Free trial)
* Twitch Developer Account

### 2. Installation
```bash
# Clone the repo
git clone https://github.com/kanitmann01/twitch_stat_board/
cd twitch-streaming-pipeline
```
```bash
# Install dependencies
pip install -r requirements.txt
3. Configuration (.env)
Create a .env file in the root directory

# Twitch
client_id=YOUR_TWITCH_ID
client_secret=YOUR_TWITCH_SECRET

# Kafka
KAFKA_BOOTSTRAP=pkc-xxxx.region.confluent.cloud:9092
KAFKA_KEY=YOUR_API_KEY
KAFKA_SECRET=YOUR_API_SECRET

# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
```

4. Running the Pipeline
Terminal 1: Start the Producer

```bash
python scripts/fetch_data.py
```

Terminal 2: Start the Consumer
```bash
python scripts/consumer.py
```
Terminal 3: Run dbt Transformation

```bash
cd twitch_project
dbt run
```

📊 Dashboard
Visualization Tool: Looker Studio

Key Metrics: Concurrent Viewers, Top Streamers by Popularity, Peak Viewership.

(Screenshshot TBA)

📈 Future Improvements
- [ ] Orchestration: Containerize scripts using Docker and schedule dbt runs with Apache Airflow.
- [ ] Alerting: Add Slack notifications when viewership spikes (e.g., > 10k viewers).
- [ ] CI/CD: Automate dbt testing on GitHub merge.
