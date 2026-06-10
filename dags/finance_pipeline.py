from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "finance-analyst",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dustinia_finance_pipeline",
    description="End-to-end Finance Analyst pipeline for DustiniaDelixia Groceria",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mci", "finance", "clickhouse", "spark"],
) as dag:

    ingest_csv_to_parquet = BashOperator(
        task_id="ingest_csv_to_parquet",
        bash_command="python /opt/airflow/dags/scripts/ingest_csv_to_parquet.py",
    )

    process_finance_spark = BashOperator(
        task_id="process_finance_spark",
        bash_command="python /opt/airflow/dags/scripts/process_finance_spark.py",
    )

    validate_clickhouse_tables = BashOperator(
        task_id="validate_clickhouse_tables",
        bash_command=r"""
python - <<'PY'
from clickhouse_driver import Client

client = Client(
    host="clickhouse-server",
    port=9000,
    user="admin",
    password="rahasia",
    database="analytics",
)

tables = [
    "finance_order_payments_mart",
    "finance_customer_value_mart",
    "finance_payment_method_summary",
    "finance_geo_payment_summary",
]

for table in tables:
    count = client.execute(f"SELECT COUNT(*) FROM analytics.{table}")[0][0]
    print(f"{table}: {count} rows")

print("Validation finished successfully.")
PY
""",
    )

    ingest_csv_to_parquet >> process_finance_spark >> validate_clickhouse_tables
