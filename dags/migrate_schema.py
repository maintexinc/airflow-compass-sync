"""
DAG: apply pending SQL schema migrations to Cloud SQL.

Manually triggered from the Airflow UI on deploy (schedule_interval=None) — schema
changes are never applied on an automatic timer. Reuses the same Cloud SQL DSN
helper as the sync DAGs. The runner holds a Postgres advisory lock and applies
each migration in its own transaction, so it is safe to trigger even while a sync
DAG is running.

Deploy note: the SQL files live in setup/migrations/ in the repo; that directory
must be present on the VM at MIGRATIONS_DIR (default /opt/airflow/migrations).
See setup/migrations/README.md.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.migrate import apply_migrations
from lib.secrets import get_cloudsql_dsn

GCP_PROJECT = os.environ["GCP_PROJECT"]
MIGRATIONS_DIR = os.environ.get("MIGRATIONS_DIR", "/opt/airflow/migrations")

default_args = {
    "owner": "data-platform",
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,  # never auto-retry a partially-applied migration; fix forward
}


def _run(**_):
    dsn = get_cloudsql_dsn(GCP_PROJECT)
    applied = apply_migrations(dsn, MIGRATIONS_DIR)
    print(f"applied {len(applied)} migration(s): {applied or 'none — already up to date'}")


with DAG(
    dag_id="datalake_migrate_schema",
    default_args=default_args,
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["infor", "datalake", "schema"],
) as dag:
    PythonOperator(task_id="apply_migrations", python_callable=_run)
