from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Point to your project so the ingestion script is importable
sys.path.insert(0, "/opt/airflow/project")

from src.components.lichess_api_data_ingestion import DataIngestion

# ── default args ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "stevenhan",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── task functions ───────────────────────────────────────────────────────────

def run_ingestion():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
        "/opt/airflow/.dbt/chess-pipeline-key.json"
    )
    DataIngestion(
        bq_project="chess-497919",
        bq_dataset="lichess_raw",
        output_dir="/opt/airflow/project/data",
        keyfile_path="/opt/airflow/config/chess-pipeline-key.json",
    ).run_user_games(
        username="stevenhan",
        perf_type="rapid+classical",
        max_games=5000,
    )

# ── DAG ──────────────────────────────────────────────────────────────────────

# Tells Python where to find user-installed packages
_DBT_ENV = {
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "HOME": "/home/airflow",
    "PYTHONUSERBASE": "/home/airflow/.local",
    "PYTHONPATH": "/home/airflow/.local/lib/python3.13/site-packages",
}

with DAG(
    dag_id="chess_pipeline",
    default_args=default_args,
    description="Lichess ingestion → dbt staging → intermediate → marts",
    start_date=datetime(2026, 6, 1),
    schedule="0 6 * * *",   # daily at 6am
    catchup=False,
) as dag:

    ingest = PythonOperator(
        task_id="ingest_lichess_games",
        python_callable=run_ingestion,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            "cd /opt/airflow/project/chess_analytics && "
            "dbt run --select staging --profiles-dir /opt/airflow/.dbt --target docker"
        ),
        env=_DBT_ENV,
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=(
            "cd /opt/airflow/project/chess_analytics && "
            "dbt run --select intermediate --profiles-dir /opt/airflow/.dbt --target docker"
        ),
        env=_DBT_ENV,
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            "cd /opt/airflow/project/chess_analytics && "
            "dbt run --select marts --profiles-dir /opt/airflow/.dbt --target docker"
        ),
        env=_DBT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/project/chess_analytics && "
            "dbt test --profiles-dir /opt/airflow/.dbt --target docker"
        ),
        env=_DBT_ENV,
    )

    # ── pipeline order ────────────────────────────────────────────────────────
    ingest >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test