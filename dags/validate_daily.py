"""
DAG: validate Data Lake vs Cloud SQL record counts (daily)

Compares COUNT(*) per table on both sides and logs results to
_validation_log. A task fails (and emails) when counts diverge.
Scheduled overnight to minimize overlap with active syncs; the retry
absorbs transient mid-sync deltas.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.validate import validate_table, cleanup_validation_log
from config import TABLES_5MIN, TABLES_15MIN, TABLES_60MIN

GCP_PROJECT = os.environ["GCP_PROJECT"]

ALL_TABLES = TABLES_5MIN + TABLES_15MIN + TABLES_60MIN

default_args = {
    "owner": "data-platform",
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="datalake_validate_daily",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # 10:00 UTC = 2-3am Pacific
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["infor", "datalake", "validation"],
) as dag:

    validations = [
        PythonOperator(
            task_id=f"validate_{table}",
            python_callable=validate_table,
            op_kwargs={"table": table, "gcp_project": GCP_PROJECT},
        )
        for table in ALL_TABLES
    ]

    # Prune old history after validations run, regardless of their outcome,
    # so a count mismatch doesn't block retention cleanup.
    cleanup = PythonOperator(
        task_id="cleanup_validation_log",
        python_callable=cleanup_validation_log,
        op_kwargs={"gcp_project": GCP_PROJECT},
        trigger_rule="all_done",
    )

    validations >> cleanup
