# 🚀 Real-Time Sales Analytics Pipeline (Kafka + Snowflake + Power BI)

---

## 📌 Project Overview

This project demonstrates the design and implementation of a **real-time data analytics pipeline** using modern data engineering tools.

The system ingests streaming sales data using Apache Kafka, processes it in Snowflake using a structured data model, and visualizes insights through an interactive Power BI dashboard.

---

## 🧠 Problem Statement

Traditional analytics systems rely on **batch processing**, which leads to delayed insights.

This project solves that problem by building a **real-time event-driven pipeline**, enabling continuous data ingestion, transformation, and visualization.

---

## 🏗️ System Architecture

```
        +----------------------+
        |  Kafka Producer      |
        | (Python - Live Data) |
        +----------+-----------+
                   |
                   v
        +----------------------+
        |   Apache Kafka       |
        |   Topic: sales_topic |
        +----------+-----------+
                   |
                   v
        +----------------------+
        |  Kafka Consumer      |
        | (Python Script)      |
        +----------+-----------+
                   |
                   v
        +----------------------+
        |   Snowflake RAW      |
        | (VARIANT JSON Data)  |
        +----------+-----------+
                   |
                   v
        +----------------------+
        |   STAGING LAYER      |
        | (Clean Structured)   |
        +----------+-----------+
                   |
                   v
        +----------------------+
        |  ANALYTICS LAYER     |
        | (Star Schema Model)  |
        +----------+-----------+
                   |
                   v
        +----------------------+
        |    Power BI          |
        |  Dashboard (Live)    |
        +----------------------+
```

---

## ⚙️ Tech Stack

* **Streaming Platform:** Apache Kafka
* **Programming Language:** Python
* **Data Warehouse:** Snowflake
* **Visualization:** Power BI
* **Query Language:** SQL

---

## 🔄 Data Pipeline Flow

### 1. Data Generation

* Python Kafka Producer generates real-time sales data
* Data is sent to Kafka topic `sales_topic`

### 2. Data Streaming

* Kafka acts as a message broker
* Streams events continuously

### 3. Data Ingestion

* Kafka Consumer reads data
* Inserts JSON into Snowflake RAW table

### 4. Data Processing (Medallion Architecture)

#### 🟫 Bronze Layer (RAW)

* Stores raw JSON data
* Table: `raw_data.sales_stream_raw`

#### 🟨 Silver Layer (STAGING)

* Cleans and transforms data
* Table: `staging.sales_cleaned`

#### 🟩 Gold Layer (ANALYTICS)

* Structured star schema model
* Fact + Dimension tables

---

## 🧱 Data Model (Star Schema)

### ⭐ Fact Table

* `fact_sales`

  * order_id
  * region
  * sales
  * profit
  * order_date

### 📊 Dimension Tables

* `dim_date`
* `dim_region`
* (optional: customer, product)

---

## ⚡ Automation (Snowflake)

* **Streams** track new incoming data
* **Tasks** automate transformation

👉 Enables fully automated pipeline

---

## 📊 Dashboard Features (Power BI)

* 📈 Sales trends over time
* 📊 Region-wise performance
* 💰 Profit analysis
* 🔍 Interactive filters
* 📦 Order-level insights

---

## 📁 Project Structure

```
sales-analytics-pipeline/
│
├── kafka/
│   ├── producer.py
│   ├── consumer.py
│
├── snowflake/
│   ├── setup.sql
│   ├── schema.sql
│   ├── pipeline.sql
│
├── dashboard/
│   └── sales_dashboard.pbix
│
├── screenshots/
│
└── README.md
```

---

## 🚀 How to Run the Project

### 1. Start Kafka (Docker)

```bash
docker compose up -d
```

---

### 2. Run Producer

```bash
python producer.py
```

---

### 3. Run Consumer

```bash
python consumer.py
```

---

### 4. Verify Data in Snowflake

```sql
SELECT * FROM raw_data.sales_stream_raw;
```

---

### 5. Open Power BI Dashboard

* Connect to Snowflake
* Load analytics tables

---

## 🔥 Key Features

* Real-time streaming pipeline
* Event-driven architecture
* Medallion data model (Bronze → Silver → Gold)
* Star schema for analytics
* Live dashboard integration

---

## 💡 Learnings

* Kafka streaming fundamentals
* Snowflake data warehousing
* Data modeling (Star Schema)
* Real-time pipeline design
* Dashboard creation

---

## 🎯 Resume Highlight

> Built a real-time data pipeline using Apache Kafka and Snowflake, implementing medallion architecture and visualizing insights using Power BI dashboards.

---

## 📌 Future Enhancements

* Integrate Snowpipe for ingestion
* Add anomaly detection
* Use Spark Streaming
* Deploy using Docker Compose fully

---



---
