"""
Olist E-Commerce Data Pipeline DAG
Schedule: Daily at 6 AM UTC
Flow: Raw Ingestion → dbt Staging → dbt Marts → Data Quality Checks
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "priyanshu",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def run_ingestion():
    import sys
    sys.path.insert(0, "/opt/airflow/ingestion")
    from ingest_raw import run_ingestion as _run
    _run()


def run_data_quality_checks():
    """Basic DQ checks on warehouse marts"""
    import psycopg2
    import logging

    logger = logging.getLogger(__name__)
    conn = psycopg2.connect(
        host="postgres", port=5432,
        dbname="olist_warehouse",
        user="airflow", password="airflow"
    )
    cur = conn.cursor()

    checks = [
    (
        "fct_orders row count > 0",
        "SELECT COUNT(*) FROM staging_marts.fct_orders",
        lambda x: x > 0,
    ),
    (
        "No NULL order_ids in fct_orders",
        "SELECT COUNT(*) FROM staging_marts.fct_orders WHERE order_id IS NULL",
        lambda x: x == 0,
    ),
    (
        "dim_customers row count > 0",
        "SELECT COUNT(*) FROM staging_marts.dim_customers",
        lambda x: x > 0,
    ),
    (
        "Revenue > 0",
        "SELECT SUM(total_payment_value) FROM staging_marts.fct_orders",
        lambda x: x > 0,
    ),
]

    failed = []
    for check_name, query, validator in checks:
        cur.execute(query)
        result = cur.fetchone()[0]
        if validator(result):
            logger.info(f"✓ PASSED: {check_name} (result={result})")
        else:
            logger.error(f"✗ FAILED: {check_name} (result={result})")
            failed.append(check_name)

    cur.close()
    conn.close()

    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
    logger.info("All data quality checks passed!")


with DAG(
    dag_id="olist_data_pipeline",
    default_args=default_args,
    description="End-to-end Olist e-commerce data pipeline",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["olist", "data-engineering", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Step 1: Ingest raw CSVs into PostgreSQL
    ingest_raw = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
        doc_md="Loads all Olist CSV files into olist_raw database",
    )

    # Step 2: dbt staging models (clean + rename raw tables)
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}",
        doc_md="Runs dbt staging models: clean types, rename columns",
    )

    # Step 3: dbt mart models (star schema)
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}",
        doc_md="Runs dbt mart models: fact + dimension tables",
    )

    # Step 4: dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
        doc_md="Runs dbt schema + data tests",
    )

    # Step 5: Custom data quality checks
    dq_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
        doc_md="Custom DQ checks on mart tables",
    )

    # Step 6: Generate dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"cd {DBT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
        doc_md="Generates dbt documentation",
    )

    end = EmptyOperator(task_id="end")

    # DAG dependency chain
    start >> ingest_raw >> dbt_staging >> dbt_marts >> dbt_test >> dq_checks >> dbt_docs >> end
