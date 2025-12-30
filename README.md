# 📦 Big Data E-Commerce Analytics Pipeline

<div align="center">

<!-- ARCHITECTURE IMAGE PLACEHOLDER -->

📌 **Architecture Diagram**
*<img width="1247" height="710" alt="image" src="https://github.com/user-attachments/assets/ca8d50af-e1ba-4943-a039-b7dad7d2babe" />*

</div>

## 🛍️ Project Overview

This project implements a **complete big-data analytics pipeline** for an e-commerce platform.
It simulates real order activity, stores it in MongoDB, archives old data, and visualizes real-time KPIs through a Streamlit dashboard.

The goal of this project is to demonstrate end-to-end **data engineering workflows** including:

* Realistic synthetic data generation
* Batch & near-real-time ingestion
* Archival logic for large datasets
* Analytics and dashboards
* Containerized deployment using Docker

Everything in this repository is fully reproducible and can be extended into a complete production-scale pipeline.

---

## 🧰 Tools & Technologies Used

<div align="center">

| Tool                                                                                                 | Purpose                                  |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| ![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker\&logoColor=white)                   | Container orchestration for all services |
| ![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?logo=mongodb\&logoColor=white)                | NoSQL database for order storage         |
| ![Python](https://img.shields.io/badge/Python-3670A0?logo=python\&logoColor=ffdd54)                  | Data generator + ETL scripts             |
| ![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit\&logoColor=white)          | Real-time analytics dashboard            |
| ![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas\&logoColor=white)                   | Data manipulation & flattening           |
| ![Plotly](https://img.shields.io/badge/Plotly-3F4F75?logo=plotly\&logoColor=white)                   | Interactive charting                     |
| ![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark\&logoColor=white)       | Optional: Batch processing & archiving   |
| ![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow\&logoColor=white) | Workflow scheduling (optional)           |

</div>

---

# 📁 Repository Structure

```
big-data-ecommerce/
│
├── airflow/                # Airflow DAGs for scheduling (optional)
├── app/
│   ├── jars/
│   └── spark_jobs/         # Spark ETL and archiving jobs
│
├── data/                   # Generated CSV files
├── data_generator/         # Stream-based data generator (orders)
│
├── dashboard/              # Streamlit live analytics dashboard
│
├── scripts/                # Helper scripts: archiving, CSV import/export
│
├── docker-compose.yml      # Full infrastructure orchestration
└── README.md
```

---

# 🚀 Features Implemented

### 1️⃣ Synthetic Data Generator

A Python generator creates realistic:

* Users
* Products
* Warehouses
* Couriers
* Orders with multiple items
* Delivery status
* Payment methods

This simulates real-world traffic for analytics testing.

---

### 2️⃣ MongoDB Storage

Orders are stored in a nested JSON schema:

```json
{
  "order": { ... },
  "order_items": [...],
  "delivery": { ... }
}
```

The structure supports flexible querying and denormalized analytics.

---

### 3️⃣ Archiving Logic

A custom archiver:

* Moves older orders to HDFS (Spark-based option)
* Inserts aggregated summaries into `orders_archive_summary`
* Prevents main collection from bloating

---

### 4️⃣ Real-Time Streamlit Dashboard

Fully interactive dashboard showing:

✔ Total orders
✔ Total revenue
✔ Avg order value
✔ Delivered vs Pending
✔ Payment method breakdown
✔ Top selling products
✔ Delivery status timeline
✔ Orders by warehouse & courier

The dashboard **auto-refreshes every 5 seconds**.

---

# 🐳 Setup & Run (Full Reproducible Tutorial)

> Follow these steps EXACTLY to replicate your full project.

---

## ✅ 1. Clone the Repository

```bash
git clone https://github.com/mibma/big-data-ecommerce
cd big-data-ecommerce
```

---

## ✅ 2. Start All Docker Services

```bash
docker compose up -d
```

Services started:

* MongoDB
* Spark Master & Workers
* Airflow (optional)

---

## ✅ 3. Generate Synthetic Data

```bash
cd data_generator
python3 generate_data.py
```

This will generate `orders.json`, `orders_export.csv` etc.

---

## ✅ 4. Import Data into MongoDB

### Option A — Using `mongoimport`

```bash
docker cp data/orders_export.csv mongodb:/tmp/orders_export.csv

docker exec -it mongodb bash

mongoimport \
  --username admin \
  --password admin123 \
  --authenticationDatabase admin \
  --db ecommerce \
  --collection orders \
  --type csv \
  --file /tmp/orders_export.csv \
  --headerline \
  --columnsHaveTypes \
  --fields "order.json,order_items.json,delivery.json"
```

---

### Option B — Using Python Import Script (Recommended)

```bash
python3 scripts/import_csv.py
```

This automatically parses nested JSON and inserts them correctly.

---

## ✅ 5. Run the Archiving System (Optional)

```bash
python3 scripts/archive_orders.py
```

---

## ✅ 6. Launch Streamlit Dashboard

```bash
streamlit run dashboard/app.py
```

Open:
📌 [http://localhost:8501](http://localhost:8501)

---

# 🧠 How the System Works (Pipeline Flow)

```
Data Generator → MongoDB → Archive Job → HDFS/Summary → Streamlit Dashboard
```

### 🔹 4. Add Airflow DAGs

Schedule:

* Daily archive
* Hourly ingestion
* Dashboard refresh tasks

### 🔹 5. Implement BI dashboards (Metabase / Superset)

---

# 🤝 Contributing

Pull requests are welcome!
Add new charts, new metrics, or improve the pipeline with new features.

---

# 📜 License

This project is licensed under the **MIT License**.

---

# 📬 Contact

Author: **Ibrahim (mibma)**
GitHub: [https://github.com/mibma](https://github.com/mibma)

