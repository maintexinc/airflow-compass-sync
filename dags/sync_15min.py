"""
DAG: Infor Data Lake -> Cloud SQL  (every 15 min)
Tables: inventory, oeeh, oeel, poeh, poel, wteh, wtel
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.sync import sync_table
from config import TABLES_15MIN

GCP_PROJECT = os.environ["GCP_PROJECT"]

default_args = {
    "owner": "data-platform",
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="datalake_sync_15min",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["infor", "datalake"],
) as dag:

    for table in TABLES_15MIN:
        PythonOperator(
            task_id=f"sync_{table}",
            python_callable=sync_table,
            op_kwargs={"table": table, "gcp_project": GCP_PROJECT},
        )
