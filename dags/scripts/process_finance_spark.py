from __future__ import annotations

import os
import shutil
from pathlib import Path

import pandas as pd
from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_DIR = Path(os.getenv("RAW_DIR", "/opt/airflow/data_lake/raw"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", "/opt/airflow/data_lake/processed"))

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "rahasia")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DustiniaDelixiaFinancePipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_parquet_table(spark: SparkSession, table_name: str):
    path = RAW_DIR / table_name / f"{table_name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    return spark.read.parquet(str(path))


def clean_string_column(column_name: str):
    return F.coalesce(F.col(column_name).cast("string"), F.lit(""))


def write_single_parquet(df, output_name: str) -> Path:
    output_path = PROCESSED_DIR / output_name

    if output_path.exists():
        shutil.rmtree(output_path)

    df.coalesce(1).write.mode("overwrite").parquet(str(output_path))
    return output_path


def find_single_part_file(parquet_dir: Path) -> Path:
    part_files = list(parquet_dir.glob("part-*.parquet"))
    if not part_files:
        raise FileNotFoundError(f"No part parquet file found inside {parquet_dir}")
    return part_files[0]


def load_parquet_to_clickhouse(parquet_path: Path, table_name: str, columns: list[str]) -> None:
    pdf = pd.read_parquet(parquet_path)
    pdf = pdf[columns]
    pdf = pdf.where(pd.notnull(pdf), None)

    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    client.execute(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table_name}")

    rows = [tuple(row) for row in pdf.itertuples(index=False, name=None)]

    if rows:
        client.execute(
            f"INSERT INTO {CLICKHOUSE_DATABASE}.{table_name} ({', '.join(columns)}) VALUES",
            rows,
        )

    print(f"Loaded {len(rows):,} rows into {CLICKHOUSE_DATABASE}.{table_name}")


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    print("Reading raw Parquet files...")
    customers = read_parquet_table(spark, "customers")
    orders = read_parquet_table(spark, "orders")
    payments = read_parquet_table(spark, "order_payments")
    items = read_parquet_table(spark, "order_items")

    customers_clean = (
        customers
        .select(
            clean_string_column("customer_id").alias("customer_id"),
            clean_string_column("customer_unique_id").alias("customer_unique_id"),
            clean_string_column("customer_city").alias("customer_city"),
            clean_string_column("customer_state").alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )

    orders_clean = (
        orders
        .select(
            clean_string_column("order_id").alias("order_id"),
            clean_string_column("customer_id").alias("customer_id"),
            clean_string_column("order_status").alias("order_status"),
            F.to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
        )
        .dropDuplicates(["order_id"])
    )

    payments_clean = (
        payments
        .select(
            clean_string_column("order_id").alias("order_id"),
            F.coalesce(F.col("payment_sequential").cast("int"), F.lit(0)).alias("payment_sequential"),
            clean_string_column("payment_type").alias("payment_type"),
            F.coalesce(F.col("payment_installments").cast("int"), F.lit(0)).alias("payment_installments"),
            F.coalesce(F.col("payment_value").cast("double"), F.lit(0.0)).alias("payment_value"),
        )
    )

    item_summary = (
        items
        .groupBy("order_id")
        .agg(
            F.sum(F.coalesce(F.col("price").cast("double"), F.lit(0.0))).alias("total_item_value"),
            F.sum(F.coalesce(F.col("freight_value").cast("double"), F.lit(0.0))).alias("total_freight_value"),
        )
    )

    base = (
        payments_clean
        .join(orders_clean, on="order_id", how="left")
        .join(customers_clean, on="customer_id", how="left")
        .join(item_summary, on="order_id", how="left")
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        .withColumn("order_purchase_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .withColumn("total_item_value", F.coalesce(F.col("total_item_value"), F.lit(0.0)))
        .withColumn("total_freight_value", F.coalesce(F.col("total_freight_value"), F.lit(0.0)))
        .withColumn("total_order_value", F.col("total_item_value") + F.col("total_freight_value"))
        .withColumn(
            "installment_group",
            F.when(F.col("payment_installments") <= 1, F.lit("No installment"))
            .when(F.col("payment_installments").between(2, 3), F.lit("2-3 installments"))
            .when(F.col("payment_installments").between(4, 6), F.lit("4-6 installments"))
            .otherwise(F.lit("7+ installments")),
        )
        .filter(F.col("order_id") != "")
    )

    high_value_threshold = (
        base
        .agg(F.expr("percentile_approx(payment_value, 0.80)").alias("p80"))
        .collect()[0]["p80"]
    )

    if high_value_threshold is None:
        high_value_threshold = 0.0

    print(f"High-value order threshold p80: {high_value_threshold}")

    finance_order_payments_mart = (
        base
        .withColumn(
            "high_value_order_flag",
            F.when(F.col("payment_value") >= F.lit(float(high_value_threshold)), F.lit(1)).otherwise(F.lit(0)),
        )
        .select(
            "order_id",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "order_status",
            F.coalesce(F.col("order_purchase_date"), F.lit("1970-01-01").cast("date")).alias("order_purchase_date"),
            F.coalesce(F.col("order_purchase_month"), F.lit("unknown")).alias("order_purchase_month"),
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "installment_group",
            "payment_value",
            F.col("total_item_value").alias("price"),
            F.col("total_freight_value").alias("freight_value"),
            "total_item_value",
            "total_freight_value",
            "total_order_value",
            "high_value_order_flag",
        )
    )

    customer_payment_type_rank = (
        finance_order_payments_mart
        .groupBy("customer_unique_id", "payment_type")
        .agg(F.sum("payment_value").alias("payment_type_value"))
    )

    rank_window = Window.partitionBy("customer_unique_id").orderBy(F.desc("payment_type_value"))

    preferred_payment = (
        customer_payment_type_rank
        .withColumn("rank", F.row_number().over(rank_window))
        .filter(F.col("rank") == 1)
        .select("customer_unique_id", F.col("payment_type").alias("preferred_payment_type"))
    )

    customer_base = (
        finance_order_payments_mart
        .groupBy("customer_unique_id", "customer_city", "customer_state")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("payment_value").alias("avg_payment_value"),
            F.max("payment_installments").alias("max_installments"),
        )
        .filter(F.col("customer_unique_id") != "")
    )

    customer_value_threshold = (
        customer_base
        .agg(F.expr("percentile_approx(total_payment_value, 0.80)").alias("p80"))
        .collect()[0]["p80"]
    )

    if customer_value_threshold is None:
        customer_value_threshold = 0.0

    print(f"High-value customer threshold p80: {customer_value_threshold}")

    finance_customer_value_mart = (
        customer_base
        .join(preferred_payment, on="customer_unique_id", how="left")
        .withColumn(
            "customer_value_segment",
            F.when(F.col("total_payment_value") >= F.lit(float(customer_value_threshold)), F.lit("High Value"))
            .otherwise(F.lit("Regular")),
        )
        .select(
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "total_payment_value",
            "total_orders",
            "avg_payment_value",
            F.coalesce(F.col("preferred_payment_type"), F.lit("unknown")).alias("preferred_payment_type"),
            "max_installments",
            "customer_value_segment",
        )
    )

    finance_payment_method_summary = (
        finance_order_payments_mart
        .groupBy("payment_type", "installment_group")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("payment_value").alias("total_payment_value"),
            F.avg("payment_value").alias("avg_payment_value"),
            F.sum("high_value_order_flag").alias("high_value_order_count"),
        )
        .select(
            "payment_type",
            "installment_group",
            "total_orders",
            "total_payment_value",
            "avg_payment_value",
            "high_value_order_count",
        )
    )

    finance_geo_payment_summary = (
        finance_order_payments_mart
        .join(
            finance_customer_value_mart.select("customer_unique_id", "customer_value_segment"),
            on="customer_unique_id",
            how="left",
        )
        .groupBy("customer_state", "customer_city")
        .agg(
            F.countDistinct("customer_unique_id").alias("total_customers"),
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("payment_value").alias("total_payment_value"),
            F.avg("payment_value").alias("avg_payment_value"),
            F.countDistinct(
                F.when(F.col("customer_value_segment") == "High Value", F.col("customer_unique_id"))
            ).alias("high_value_customer_count"),
        )
        .filter(F.col("customer_state") != "")
        .select(
            "customer_state",
            "customer_city",
            "total_customers",
            "total_orders",
            "total_payment_value",
            "avg_payment_value",
            "high_value_customer_count",
        )
    )

    marts = {
        "finance_order_payments_mart": finance_order_payments_mart,
        "finance_customer_value_mart": finance_customer_value_mart,
        "finance_payment_method_summary": finance_payment_method_summary,
        "finance_geo_payment_summary": finance_geo_payment_summary,
    }

    mart_columns = {
        "finance_order_payments_mart": [
            "order_id",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "order_status",
            "order_purchase_date",
            "order_purchase_month",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "installment_group",
            "payment_value",
            "price",
            "freight_value",
            "total_item_value",
            "total_freight_value",
            "total_order_value",
            "high_value_order_flag",
        ],
        "finance_customer_value_mart": [
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "total_payment_value",
            "total_orders",
            "avg_payment_value",
            "preferred_payment_type",
            "max_installments",
            "customer_value_segment",
        ],
        "finance_payment_method_summary": [
            "payment_type",
            "installment_group",
            "total_orders",
            "total_payment_value",
            "avg_payment_value",
            "high_value_order_count",
        ],
        "finance_geo_payment_summary": [
            "customer_state",
            "customer_city",
            "total_customers",
            "total_orders",
            "total_payment_value",
            "avg_payment_value",
            "high_value_customer_count",
        ],
    }

    for table_name, df in marts.items():
        output_dir = write_single_parquet(df, table_name)
        part_file = find_single_part_file(output_dir)
        load_parquet_to_clickhouse(part_file, table_name, mart_columns[table_name])

    spark.stop()
    print("Finance pipeline finished successfully.")


if __name__ == "__main__":
    main()
