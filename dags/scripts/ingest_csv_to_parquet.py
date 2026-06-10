from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
RAW_DIR = Path(os.getenv("RAW_DIR", "/opt/airflow/data_lake/raw"))

REQUIRED_FILES = [
    "customers.csv",
    "orders.csv",
    "order_payments.csv",
    "order_items.csv",
    "geolocation.csv",
]

OPTIONAL_FILES = [
    "category_translation.csv",
    "closed_deals.csv",
    "mql.csv",
    "order_reviews.csv",
    "products.csv",
    "sellers.csv",
]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        str(col).strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]
    return df


def read_csv_safely(file_path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path)
    except UnicodeDecodeError:
        return pd.read_csv(file_path, encoding="latin1")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    missing_files = [name for name in REQUIRED_FILES if not (DATA_DIR / name).exists()]
    if missing_files:
        raise FileNotFoundError(
            "Missing required CSV files in /opt/airflow/data: "
            + ", ".join(missing_files)
        )

    for file_name in REQUIRED_FILES + OPTIONAL_FILES:
        csv_path = DATA_DIR / file_name

        if not csv_path.exists():
            print(f"Skipping optional file because it does not exist: {file_name}")
            continue

        table_name = file_name.replace(".csv", "")
        output_dir = RAW_DIR / table_name
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Reading {csv_path}")
        df = read_csv_safely(csv_path)
        df = normalize_columns(df)

        output_path = output_dir / f"{table_name}.parquet"
        df.to_parquet(output_path, index=False)

        print(f"Saved {len(df):,} rows to {output_path}")

    print("CSV ingestion to Parquet finished successfully.")


if __name__ == "__main__":
    main()
