"""FactoryFlow daily batch pipeline.

Steps: detect or generate the day's raw data -> ingest into PostgreSQL ->
dbt run -> dbt test -> score anomalies -> publish.

Status note (kept honest on purpose): the scoring and publish steps are
placeholders until Phase 5 (quality baseline + IsolationForest) and Phase 6
(dashboard) land. They run and log clearly what they will do, but do not yet
compute or publish anything real. See docs/backlog.md.
"""

from __future__ import annotations

import hashlib
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

logger = logging.getLogger("factoryflow.airflow")

default_args = {
    "owner": "factoryflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}


def generate_or_detect_file(ds: str, **_context) -> str:
    """Use an already-arrived file if present, otherwise generate one.

    In a real factory this task would poll a drop folder / SFTP / object
    store. Here we simulate that: each run date gets its own deterministic
    synthetic batch (seeded from the date), written under data/raw/{ds}/,
    so re-running the same date always produces the same file.
    """
    from factoryflow.generator import generate_dataset, write_dataset_to_csv

    data_dir = PROJECT_ROOT / "data" / "raw" / ds
    marker = data_dir / "production_events.csv"

    if marker.exists():
        logger.info("File already present for %s, skipping generation", ds)
        return str(data_dir)

    seed = int(hashlib.sha256(ds.encode()).hexdigest(), 16) % (2**31)
    dataset = generate_dataset(
        seed=seed,
        days=1,
        n_machines=5,
        events_per_day_per_machine=20,
        id_prefix=f"{ds}-",
    )
    write_dataset_to_csv(dataset, data_dir)
    logger.info("Generated new batch for %s (seed=%s) at %s", ds, seed, data_dir)
    return str(data_dir)


def ingest_to_postgres(ds: str, **context) -> dict:
    from factoryflow.ingest import apply_schema, get_connection, ingest_dataset

    data_dir = Path(context["ti"].xcom_pull(task_ids="generate_or_detect_file"))
    conn = get_connection()
    try:
        apply_schema(conn)
        results = ingest_dataset(conn, data_dir, source_file=f"airflow/{ds}")
        logger.info("Ingestion results for %s: %s", ds, results)
        return results
    finally:
        conn.close()


def score_anomalies(**_context) -> None:
    logger.info(
        "score_anomalies: placeholder task. Statistical baseline + IsolationForest "
        "land in Phase 5 (see docs/backlog.md). No scoring performed yet."
    )


def publish_results(**_context) -> None:
    logger.info(
        "publish_results: placeholder task. The Streamlit dashboard (Phase 6) will "
        "read from the dbt marts built by this DAG. Nothing published yet."
    )


with DAG(
    dag_id="factoryflow_pipeline",
    description="Detect/generate data, ingest, transform with dbt, score, publish.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["factoryflow"],
) as dag:
    generate_or_detect = PythonOperator(
        task_id="generate_or_detect_file",
        python_callable=generate_or_detect_file,
        op_kwargs={"ds": "{{ ds }}"},
    )

    ingest = PythonOperator(
        task_id="ingest_to_postgres",
        python_callable=ingest_to_postgres,
        op_kwargs={"ds": "{{ ds }}"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "if [ -f {{ params.dbt_venv }}/bin/activate ]; then "
            "source {{ params.dbt_venv }}/bin/activate; fi && "
            "cd {{ params.dbt_dir }} && "
            "DBT_PROFILES_DIR={{ params.dbt_dir }} dbt run"
        ),
        params={"dbt_dir": str(PROJECT_ROOT / "dbt"), "dbt_venv": str(PROJECT_ROOT / ".venv-dbt")},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "if [ -f {{ params.dbt_venv }}/bin/activate ]; then "
            "source {{ params.dbt_venv }}/bin/activate; fi && "
            "cd {{ params.dbt_dir }} && "
            "DBT_PROFILES_DIR={{ params.dbt_dir }} dbt test"
        ),
        params={"dbt_dir": str(PROJECT_ROOT / "dbt"), "dbt_venv": str(PROJECT_ROOT / ".venv-dbt")},
    )

    score = PythonOperator(task_id="score_anomalies", python_callable=score_anomalies)
    publish = PythonOperator(task_id="publish_results", python_callable=publish_results)

    generate_or_detect >> ingest >> dbt_run >> dbt_test >> score >> publish
