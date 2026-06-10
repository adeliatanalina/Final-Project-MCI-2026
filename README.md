# DustiniaDelixia Groceria — Finance Analyst Data Pipeline

## 1. Project Overview

This project implements an end-to-end data analytics pipeline for the **Finance Analyst persona** in the DustiniaDelixia Groceria final project.

The Head of Finance wants to understand:

1. Who are the high-value customers?
2. What payment methods do they prefer?
3. Do installment payments increase order value?
4. Where do high-value customers come from geographically?
5. Where is DustiniaDelixia losing potential revenue because payment options are not optimized?

The pipeline uses:

- Apache Airflow for orchestration
- Apache Spark / PySpark for data processing
- ClickHouse as the analytical data warehouse
- Metabase for dashboard visualization
- Docker Compose to run all services

---

## 2. Dataset

Put the following CSV files inside the `data/` folder:

```text
data/
├── category_translation.csv
├── closed_deals.csv
├── customers.csv
├── geolocation.csv
├── mql.csv
├── order_items.csv
├── order_payments.csv
├── order_reviews.csv
├── orders.csv
├── products.csv
└── sellers.csv
```

For the Finance Analyst persona, the main files used are:

| File | Purpose |
|---|---|
| customers.csv | Customer identity and location |
| orders.csv | Order status and timestamps |
| order_payments.csv | Payment method, installment, and payment value |
| order_items.csv | Product-level order item price and freight value |
| geolocation.csv | Geographic data, available for further extension |

---

## 3. Business Questions

1. Which customers generate the highest spending?
2. What payment types are most used by high-value customers?
3. Are credit card installments associated with higher transaction values?
4. Which cities and states contribute the most payment value?
5. Which geographic segments may have untapped revenue potential?
6. Which payment methods should DustiniaDelixia optimize to increase revenue?

---

## 4. Pipeline Architecture

```text
CSV Dataset
    ↓
Airflow DAG
    ↓
Task 1: Ingest CSV files into Parquet data lake
    ↓
Task 2: Process finance mart using PySpark
    ↓
Task 3: Load analytics tables into ClickHouse
    ↓
Metabase Dashboard
```

---

## 5. Folder Structure

```text
dustinia-finance-pipeline/
├── dags/
│   ├── finance_pipeline.py
│   └── scripts/
│       ├── ingest_csv_to_parquet.py
│       └── process_finance_spark.py
├── data/
│   └── put CSV files here
├── data_lake/
│   ├── raw/
│   └── processed/
├── clickhouse/
│   └── init/
│       └── 01_create_database.sql
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 6. How to Run

### Step 1 — Put Dataset in `data/`

Copy all CSV files into the `data/` folder.

Required files:

```text
customers.csv
orders.csv
order_payments.csv
order_items.csv
geolocation.csv
```

### Step 2 — Build Docker Image

```bash
docker-compose build
```

### Step 3 — Initialize Airflow

```bash
docker-compose up airflow-init
```

### Step 4 — Start Services

```bash
docker-compose up -d
```

### Step 5 — Open Airflow

Open:

```text
http://localhost:8080
```

Login:

```text
Username: admin
Password: admin
```

Trigger this DAG:

```text
dustinia_finance_pipeline
```

### Step 6 — Validate ClickHouse

```bash
docker exec -it dustinia-clickhouse clickhouse-client --user admin --password rahasia
```

Then run:

```sql
USE analytics;
SHOW TABLES;

SELECT COUNT(*) FROM finance_order_payments_mart;
SELECT COUNT(*) FROM finance_customer_value_mart;
SELECT COUNT(*) FROM finance_payment_method_summary;
SELECT COUNT(*) FROM finance_geo_payment_summary;
```

### Step 7 — Open Metabase

Open:

```text
http://localhost:3000
```

Use this connection:

| Field | Value |
|---|---|
| Database type | ClickHouse |
| Host | clickhouse-server |
| Port | 8123 |
| Database name | analytics |
| Username | admin |
| Password | rahasia |

---

## 7. Output Tables

### finance_order_payments_mart

Detailed order-payment table.

### finance_customer_value_mart

Customer-level table for high-value customer segmentation.

### finance_payment_method_summary

Payment method and installment summary.

### finance_geo_payment_summary

City and state payment contribution summary.

---

## 8. Recommended Metabase Dashboard

Dashboard title:

```text
Finance Performance & High-Value Customer Dashboard
```

Recommended charts:

1. Total Payment Value
2. Average Payment Value
3. Number of High-Value Customers
4. Payment Value by Payment Type
5. Average Payment Value by Installment Group
6. Top 10 States by Payment Value
7. Top 10 Cities by High-Value Customers
8. High-Value vs Regular Customer Segment
9. Credit Card Installment Impact on Payment Value
10. Payment Method Preference by State

---

## 9. SQL Queries for Dashboard

### Revenue by Payment Type

```sql
SELECT
    payment_type,
    SUM(total_payment_value) AS total_payment_value
FROM finance_payment_method_summary
GROUP BY payment_type
ORDER BY total_payment_value DESC;
```

### Average Payment by Installment Group

```sql
SELECT
    installment_group,
    AVG(avg_payment_value) AS avg_payment_value
FROM finance_payment_method_summary
GROUP BY installment_group
ORDER BY avg_payment_value DESC;
```

### Top States by Payment Value

```sql
SELECT
    customer_state,
    SUM(total_payment_value) AS total_payment_value
FROM finance_geo_payment_summary
GROUP BY customer_state
ORDER BY total_payment_value DESC
LIMIT 10;
```

### High-Value Customers by City

```sql
SELECT
    customer_state,
    customer_city,
    high_value_customer_count,
    total_payment_value
FROM finance_geo_payment_summary
ORDER BY high_value_customer_count DESC
LIMIT 10;
```

### Customer Segment Contribution

```sql
SELECT
    customer_value_segment,
    COUNT(*) AS total_customers,
    SUM(total_payment_value) AS total_payment_value,
    AVG(avg_payment_value) AS avg_payment_value
FROM finance_customer_value_mart
GROUP BY customer_value_segment
ORDER BY total_payment_value DESC;
```

---

## 10. Business Recommendation Direction

Use the dashboard to support recommendations such as:

1. Optimize credit card installment offers.
2. Focus campaigns on high-value regions.
3. Improve payment method experience.
4. Create targeted campaigns for high-value customers.
5. Identify regions with high customer count but low payment value.

---

## 11. Services

| Service | URL | Username | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | admin | admin |
| Metabase | http://localhost:3000 | create during setup | - |
| ClickHouse HTTP | http://localhost:8123 | admin | rahasia |
| ClickHouse TCP | localhost:9000 | admin | rahasia |
