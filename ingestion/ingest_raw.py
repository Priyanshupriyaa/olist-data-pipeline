"""
Olist Raw Ingestion
Loads all Olist CSV files into PostgreSQL raw schema (olist_raw database)
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection
RAW_DB_CONN = os.getenv(
    "RAW_DB_CONN",
    "postgresql://airflow:airflow@postgres:5432/olist_warehouse"
)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")

# Olist dataset files → table names
OLIST_FILES = {
    "olist_orders_dataset.csv": "raw_orders",
    "olist_order_items_dataset.csv": "raw_order_items",
    "olist_order_payments_dataset.csv": "raw_order_payments",
    "olist_order_reviews_dataset.csv": "raw_order_reviews",
    "olist_customers_dataset.csv": "raw_customers",
    "olist_sellers_dataset.csv": "raw_sellers",
    "olist_products_dataset.csv": "raw_products",
    "olist_geolocation_dataset.csv": "raw_geolocation",
    "product_category_name_translation.csv": "raw_product_category_translation",
}


def get_engine():
    return create_engine(RAW_DB_CONN)


def create_ingestion_log(engine):
    """Create audit log table if not exists"""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                rows_loaded INTEGER,
                ingested_at TIMESTAMP DEFAULT NOW(),
                status VARCHAR(20),
                error_msg TEXT
            )
        """))


def ingest_file(engine, filepath: str, table_name: str) -> dict:
    """Load a single CSV into PostgreSQL raw table"""
    result = {"table": table_name, "rows": 0, "status": "success", "error": None}
    try:
        logger.info(f"Loading {filepath} → {table_name}")
        df = pd.read_csv(filepath, low_memory=False)

        # Add metadata columns
        df["_ingested_at"] = datetime.utcnow()
        df["_source_file"] = os.path.basename(filepath)

        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
            chunksize=5000,
            method="multi"
        )
        result["rows"] = len(df)
        logger.info(f"✓ {table_name}: {len(df)} rows loaded")

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        logger.error(f"✗ {table_name}: {e}")

    return result


def log_result(engine, result: dict):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ingestion_log (table_name, rows_loaded, status, error_msg)
            VALUES (:table, :rows, :status, :error)
        """), result)


def run_ingestion():
    engine = get_engine()
    create_ingestion_log(engine)

    summary = []
    for filename, table_name in OLIST_FILES.items():
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath} — skipping")
            continue
        result = ingest_file(engine, filepath, table_name)
        log_result(engine, result)
        summary.append(result)

    # Print summary
    print("\n" + "="*50)
    print("INGESTION SUMMARY")
    print("="*50)
    for r in summary:
        status_icon = "✓" if r["status"] == "success" else "✗"
        print(f"{status_icon} {r['table']:<45} {r['rows']:>8} rows")
    print("="*50)

    failed = [r for r in summary if r["status"] == "failed"]
    if failed:
        raise Exception(f"{len(failed)} tables failed ingestion")


if __name__ == "__main__":
    run_ingestion()
