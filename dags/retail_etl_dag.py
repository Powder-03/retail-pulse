from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_generate_data(**kwargs) -> None:
    from src.generators.generate_data import generate_all_datasets

    generate_all_datasets(include_incremental=False)


def _run_ingest_bronze(**kwargs) -> None:
    from src.ingestion.ingest import run_ingestion

    summary = run_ingestion()
    print(f"Ingestion Summary: {summary}")


def _run_transform_silver(**kwargs) -> None:
    from src.transform.silver_etl import run_silver_etl

    dq_report = run_silver_etl()
    print(f"Silver DQ Report: {dq_report}")


def _run_build_gold(**kwargs) -> None:
    from src.analytics.gold_build import run_gold_pipeline

    run_gold_pipeline()


default_args = {
    "owner": "retail_etl_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="smart_retail_medallion_etl",
    default_args=default_args,
    description="End-to-end Smart Retail Lakehouse ETL: Source -> Bronze -> Silver Parquet -> Gold DuckDB & PostgreSQL",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "medallion", "duckdb", "pyspark", "postgres"],
) as dag:
    generate_source_data = PythonOperator(
        task_id="generate_source_data",
        python_callable=_run_generate_data,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_run_ingest_bronze,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=_run_transform_silver,
    )

    build_gold_and_sync_pg = PythonOperator(
        task_id="build_gold_and_sync_pg",
        python_callable=_run_build_gold,
    )

    generate_source_data >> ingest_bronze >> transform_silver >> build_gold_and_sync_pg