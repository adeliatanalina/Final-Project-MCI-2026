# Finance Analytics Dashboard
## DustiniaDelixia Groceria — Finance Analyst Persona

This project was created as part of the MCI 2026 Lab Final Project for the **Finance Analyst** persona. The main focus of this project is to analyze customer payment behavior, identify high-value customers, and find revenue improvement opportunities through a more suitable payment strategy.

The final dashboard was created using **Metabase**, with data that has been processed through a pipeline based on **Apache Airflow, Apache Spark, and ClickHouse**.

---

## 1. Background

DustiniaDelixia Groceria is an e-commerce marketplace that connects MSME sellers with customers from various regions. As the number of transactions increases, the company has a large amount of operational data, especially related to orders, customers, locations, and payments.

From the Finance perspective, one of the issues that needs to be analyzed is the difference in spending value among customers. Most customers make transactions within a normal range, but there is a certain customer segment with much higher transaction values. This segment is important because it can become a major revenue source for the company.

Through this project, transaction and payment data are processed into an analytical dashboard so that the Finance team can understand:

- who the high-value customers are,
- which payment method contributes the most to revenue,
- whether credit card installments are related to higher transaction values,
- which regions generate the highest payment value,
- and which areas have payment optimization opportunities.

---

## 2. Objective

The objective of this project is to build a finance analytics pipeline and dashboard that can support data-driven decision-making.

More specifically, this project aims to:

1. Process e-commerce transaction data into an analysis-ready data mart.
2. Analyze revenue contribution based on payment methods.
3. Identify customers with high transaction values.
4. Compare payment behavior between regular customers and high-value customers.
5. Observe revenue contribution based on region.
6. Provide payment strategy recommendations to increase transaction value.

---

## 3. Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Data Processing | Apache Spark / PySpark |
| Data Warehouse | ClickHouse |
| Dashboard | Metabase |
| Containerization | Docker & Docker Compose |
| Programming Language | Python |
| Data Format | CSV, Parquet |

---

## 4. Dataset

The dataset used is an e-commerce dataset provided for the DustiniaDelixia Groceria case study. The files used include:

| File | Description |
|---|---|
| `customers.csv` | Customer data and customer location |
| `orders.csv` | Order data and transaction status |
| `order_payments.csv` | Payment method, installment, and payment value data |
| `order_items.csv` | Item data for each order |
| `geolocation.csv` | Location data based on zip code |
| `products.csv` | Product data |
| `sellers.csv` | Seller data |
| `order_reviews.csv` | Customer review data |
| `category_translation.csv` | Product category translation data |
| `mql.csv` | Marketing qualified leads data |
| `closed_deals.csv` | Closed deals data |

For the Finance Analyst dashboard, the main tables used most frequently are `customers`, `orders`, `order_payments`, and `order_items`.

---

## 5. Project Architecture

The workflow of this project is as follows:

```text
CSV Dataset
    ↓
Apache Airflow DAG
    ↓
Ingest CSV to Parquet
    ↓
Apache Spark Processing
    ↓
Finance Data Mart
    ↓
ClickHouse Data Warehouse
    ↓
Metabase Dashboard
```

This pipeline was created so that the analysis is not performed directly from raw files. The data is first cleaned, joined, and summarized into several analytical tables so that the dashboard is faster and easier to use.

---

## 6. Folder Structure

```text
dustinia-finance-pipeline/
├── dags/
│   ├── finance_pipeline.py
│   └── scripts/
│       ├── ingest_csv_to_parquet.py
│       └── process_finance_spark.py
├── data/
│   └── CSV dataset files
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

## 7. Pipeline Explanation

### 7.1 CSV Ingestion

The first stage of the pipeline is reading CSV files from the `data/` folder, then saving them into Parquet format in the `data_lake/raw` folder.

The file that runs this process is:

```text
dags/scripts/ingest_csv_to_parquet.py
```

Parquet format is used because it is more efficient for data analysis processes compared to repeatedly reading CSV files directly.

### 7.2 Spark Processing

The second stage is data processing using PySpark.

The file used is:

```text
dags/scripts/process_finance_spark.py
```

At this stage, the processes carried out include:

- reading data from Parquet,
- cleaning the required columns,
- joining customer, order, payment, and item data,
- calculating total payment value,
- creating customer segmentation,
- grouping payment methods,
- creating regional revenue summaries,
- and storing the final results into ClickHouse.

### 7.3 Load to ClickHouse

After the data is processed, the results are inserted into the ClickHouse database under the schema:

```text
analytics
```

ClickHouse is used because it is suitable for aggregation queries and analytical dashboard needs.

### 7.4 Dashboard Visualization

The dashboard was created using Metabase. Metabase was chosen because it is easy to use for creating visualizations and suitable for demo purposes for non-technical audiences.

The final dashboard is named:

```text
Finance Analytics Dashboard
```

---

## 8. Data Mart Tables

### 8.1 `finance_order_payments_mart`

This table contains detailed payment data per order.

Important columns:

- `order_id`
- `customer_unique_id`
- `customer_city`
- `customer_state`
- `payment_type`
- `payment_installments`
- `installment_group`
- `payment_value`
- `total_order_value`
- `high_value_order_flag`

This table is used to analyze total payment, average payment, payment methods, installments, and monthly trends.

### 8.2 `finance_customer_value_mart`

This table contains a summary of customer value.

Important columns:

- `customer_unique_id`
- `customer_city`
- `customer_state`
- `total_payment_value`
- `total_orders`
- `avg_payment_value`
- `preferred_payment_type`
- `customer_value_segment`

This table is used to identify high-value customers and observe their payment preferences.

### 8.3 `finance_payment_method_summary`

This table contains performance summaries based on payment type and installment group.

Important columns:

- `payment_type`
- `installment_group`
- `total_orders`
- `total_payment_value`
- `avg_payment_value`
- `high_value_order_count`

### 8.4 `finance_geo_payment_summary`

This table contains payment performance summaries based on region.

Important columns:

- `customer_state`
- `customer_city`
- `total_customers`
- `total_orders`
- `total_payment_value`
- `avg_payment_value`
- `high_value_customer_count`

This table is used to identify regions with the highest revenue contribution and payment optimization opportunities.

---

## 9. Dashboard Components

### 9.1 Executive KPI Summary

This section displays the main performance summary:

| KPI | Purpose |
|---|---|
| Total Payment Value | Shows the total payment value |
| Average Payment Value | Shows the average payment per transaction |
| High-Value Customers | Shows the number of high-value customers |
| Credit Card Revenue Share | Shows the contribution of credit card payments to total revenue |

### 9.2 Monthly Payment Value Trend

This chart shows the payment value trend over time. The purpose is to observe whether payment value experienced an increase, decrease, or certain pattern in a specific period.

### 9.3 Total Payment Value by Payment Method

This chart compares revenue contribution based on payment method. From the dashboard, the **credit card** payment method appears to be the largest contributor to total payment value.

### 9.4 Average Payment Value by Payment Method

This chart is used to identify which payment method has the highest average transaction value. This analysis is important because certain payment methods may be related to higher-value transactions.

### 9.5 Credit Card Installment Impact on Payment Value

This chart analyzes the relationship between credit card installments and payment value. From the Finance perspective, this chart is one of the most important parts because installments can become a strategy to increase basket size.

### 9.6 Revenue Contribution by Customer Segment

This chart shows the revenue contribution from regular customers and high-value customers. This segmentation helps the company determine customer priorities for retention or targeted campaigns.

### 9.7 Preferred Payment Type of High-Value Customers

This chart shows the payment method most commonly used by high-value customers. This information can be used to determine a more suitable payment promotion strategy.

### 9.8 Top 10 States by Total Payment Value

This chart shows the regions with the largest payment value contribution. From the dashboard, states such as **SP, RJ, and MG** appear as regions with strong revenue contribution.

### 9.9 Strategic Payment Opportunity by Region

This table shows regions with payment opportunity potential based on the number of customers, total orders, total payment value, average payment value, and revenue per customer.

---

## 10. Dashboard Documentation

This section explains the final dashboard created in Metabase. You can add dashboard screenshots in the sections provided below.

### 10.1 Dashboard Preview

The main dashboard displays a finance performance summary from the perspective of payments, high-value customers, payment methods, installments, and regional contribution.

```

```

---

### 10.2 Executive KPI Section

The first part of the dashboard contains the main KPIs that provide a quick overview of payment performance.

The displayed KPIs are:

| KPI | Explanation |
|---|---|
| Total Payment Value | Total payment value processed from all transactions |
| Average Payment Value | Average payment value per transaction |
| High-Value Customers | Number of customers included in the high-value segment |
| Credit Card Revenue Share | Percentage of revenue contribution from credit card payments |

These KPIs are placed at the top so users can immediately understand the overall finance performance before looking at more detailed analysis.

```

```

---

### 10.3 Monthly Payment Value Trend

The **Monthly Payment Value Trend** chart is used to observe changes in payment value over time. This visualization helps Finance understand whether revenue is increasing, decreasing, or fluctuating in certain periods.

This chart also displays the number of orders, so users can compare whether the increase in payment value is aligned with the increase in transaction volume.

**Purpose:**

```text
To monitor payment value growth and transaction movement over time.
```

**Insight direction:**

If total payment value increases together with total orders, then revenue growth is likely driven by an increase in transaction volume. However, if payment value increases without a significant increase in orders, average order value may be the main factor.

> Add the Monthly Payment Value Trend chart screenshot here:

```

```

---

### 10.4 Payment Method Analysis

The payment method analysis section is used to understand the contribution of each payment method to total revenue.

Charts used:

1. **Total Payment Value by Payment Method**
2. **Average Payment Value by Payment Method**

From the dashboard, the **credit card** payment method appears to be the main contributor to total payment value. This shows that credit cards have an important role in the company’s revenue.

In addition to total payment value, the dashboard also compares **average payment value**. This metric is important because a payment method with high total revenue does not always have the highest average transaction value. By looking at both, the analysis becomes more balanced.

**Purpose:**

```text
To compare payment methods based on total revenue contribution and average transaction value.
```

**Insight direction:**

If credit cards have high total payment value and average payment value, the company can prioritize optimizing the credit card payment experience.

```md

```

---

### 10.5 Credit Card Installment Analysis

The **Credit Card Installment Impact on Payment Value** chart is used to see whether the use of credit card installments is related to higher transaction values.

This section is important for the Finance Analyst persona because installments can become one strategy to increase basket size. If installment transactions have a higher average payment value compared to non-installment transactions, the company can consider displaying installment options more clearly on the checkout page.

**Purpose:**

```text
To evaluate whether installment payment options are associated with higher payment values.
```

**Insight direction:**

If certain installment groups generate higher average payment value, the company can run special campaigns to encourage installment usage for high-value transactions.

```md

```

---

### 10.6 Customer Segment Analysis

The customer segment analysis section focuses on the comparison between **Regular Customers** and **High-Value Customers**.

Charts used:

1. **Revenue Contribution by Customer Segment**
2. **Preferred Payment Type of High-Value Customers**

The first chart shows how much revenue each customer segment contributes. The second chart shows the payment method most commonly used by high-value customers.

This analysis is important because high-value customers can become the main target for retention strategies and personalized payment campaigns.

**Purpose:**

```text
To understand the revenue contribution and payment preference of high-value customers.
```

**Insight direction:**

If high-value customers use credit cards more frequently, then promotional strategies for this segment should focus on credit card benefits, installments, or special cashback.

```md

```

---

### 10.7 Geographic Revenue Analysis

The geographic revenue analysis section is used to observe payment value contribution based on region.

Charts used:

1. **Top 10 States by Total Payment Value**
2. **Strategic Payment Opportunity by Region**

The **Top 10 States by Total Payment Value** chart shows the regions with the largest revenue contribution. Meanwhile, the **Strategic Payment Opportunity by Region** table helps identify regions with potential based on total customers, total orders, total payment value, average payment value, and revenue per customer.

**Purpose:**

```text
To identify high-performing regions and potential areas for payment optimization campaigns.
```

**Insight direction:**

Regions with high payment value can be prioritized for finance campaigns. Meanwhile, regions with high revenue per customer can be considered as targets for a premium customer strategy.

```md

```

---

### 10.8 Dashboard Design Rationale

This dashboard is arranged with a flow from general summary to more specific analysis.

The design order is:

1. **KPI Summary** — provides a quick overview of finance performance.
2. **Trend Analysis** — shows changes in payment value over time.
3. **Payment Method Analysis** — explains the most contributing payment methods.
4. **Installment Analysis** — tests the potential of installments as a strategy to increase basket size.
5. **Customer Segment Analysis** — identifies high-value customers and their payment preferences.
6. **Geographic Analysis** — identifies regions with high revenue and opportunities.

With this structure, the dashboard can be used by non-technical audiences because each section has a clear purpose and follows a logical decision-making flow.

---

### 10.9 Dashboard Summary

Overall, the dashboard shows that payment behavior has an important role in the revenue performance of DustiniaDelixia Groceria. Credit cards are the payment method with the largest contribution, and high-value customers play an important role in total revenue.

This dashboard can help the Finance team to:

- understand payment performance,
- determine customer segment priorities,
- evaluate installment potential,
- identify high-value regions,
- and create a more targeted payment optimization strategy.

---

---

## 11. Key Findings

Based on the final dashboard, several key findings are:

1. **Total payment value reached around 16.0M.**  
   This shows that the transaction data has a sufficiently large revenue scale for further analysis.

2. **Average payment value is around 154.1.**  
   This figure can be used as a baseline to compare regular customers and high-value customers.

3. **The number of high-value customers reached 19,253.**  
   This segment is important because it has a larger transaction contribution and can become a target for retention strategies.

4. **Credit card revenue share reached around 78.34%.**  
   This shows that credit cards are a very dominant payment method in revenue contribution.

5. **Credit card is the payment method with the highest total payment value.**  
   This means that optimization of credit cards and installments can have a direct impact on revenue.

6. **High-value customers also tend to prefer credit cards.**  
   This strengthens the argument that payment strategies for high-value customers should focus on credit cards and installments.

7. **Several regions have much larger revenue contribution compared to other regions.**  
   Regions with high payment value can be prioritized for finance campaigns or installment promotions.

---

## 12. Business Recommendations

### 12.1 Optimize Credit Card and Installment Payment

Because credit cards are the largest revenue contributor, the company can strengthen the credit card payment experience, especially on the checkout page.

Several strategies that can be applied:

- display installment options more clearly,
- provide special promotions for credit card transactions,
- create installment campaigns for high basket size,
- and collaborate with payment providers for periodic promotions.

### 12.2 Target High-Value Customers

High-value customers need to be managed as a special segment. The company can create strategies such as:

- personalized payment offers,
- cashback for certain transactions,
- early access promotions,
- loyalty programs,
- and product recommendations based on transaction history.

### 12.3 Focus on High-Revenue Regions

Regions with high total payment value can be the main targets for payment campaigns. This strategy is more effective than running the same promotion across all regions without segmentation.

### 12.4 Identify Under-Optimized Payment Regions

Regions with a relatively high number of customers but low average payment value can be analyzed further. These regions may have revenue potential but have not yet been encouraged by the appropriate payment method.

### 12.5 Monitor Payment Performance Regularly

This dashboard should not only be used once, but monitored regularly. With monthly monitoring, the Finance team can see whether the implemented payment strategy truly increases transaction value.

---

## 13. How to Run the Project

### 13.1 Build Docker Image

```bash
docker-compose build
```

### 13.2 Initialize Airflow

```bash
docker-compose up airflow-init
```

### 13.3 Start All Services

```bash
docker-compose up -d
```

### 13.4 Open Airflow

```text
http://localhost:8080
```

Login:

```text
Username: admin
Password: admin
```

Trigger DAG:

```text
dustinia_finance_pipeline
```

---

## 14. Validate ClickHouse Tables

Enter ClickHouse:

```bash
docker exec -it dustinia-clickhouse clickhouse-client --user admin --password rahasia
```

Run the following query:

```sql
USE analytics;
SHOW TABLES;

SELECT COUNT(*) FROM finance_order_payments_mart;
SELECT COUNT(*) FROM finance_customer_value_mart;
SELECT COUNT(*) FROM finance_payment_method_summary;
SELECT COUNT(*) FROM finance_geo_payment_summary;
```

If all tables have rows, the pipeline has run correctly.

---

## 15. Open Metabase Dashboard

Open Metabase:

```text
http://localhost:3000
```

Use the following connection:

| Field | Value |
|---|---|
| Database type | ClickHouse |
| Host | `clickhouse` or `clickhouse-server` |
| Port | `8123` |
| Database name | `analytics` |
| Username | `admin` |
| Password | `rahasia` |

---

## 16. Conclusion

This project successfully built an end-to-end finance analytics pipeline to analyze e-commerce transaction data. Raw CSV data is processed through Airflow and Spark, inserted into ClickHouse, then visualized in a Metabase dashboard.

The dashboard results show that payment methods, especially credit cards and installments, play an important role in revenue contribution. In addition, high-value customer segmentation and regional analysis can help the company design a more targeted payment strategy.

Overall, this dashboard can help the Finance team make more data-driven decisions, especially in optimizing payment methods, increasing basket size, and prioritizing customers and regions with high revenue potential.

---

## 17. Author

```text
Name        : [Your Name]
Role        : Finance Analyst
Project     : Final Project Lab MCI 2026
Institution : [Your Institution]
```
