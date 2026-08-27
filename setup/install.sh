#!/bin/bash
# Airflow setup on Debian/Ubuntu e2-small GCE VM
# Run as root after provisioning the VM.
set -euo pipefail

AIRFLOW_HOME=/opt/airflow
AIRFLOW_USER=airflow
PYTHON=python3

# 1. System deps
apt-get update -q
apt-get install -y -q "${PYTHON}" "${PYTHON}-pip" "${PYTHON}-venv" postgresql postgresql-client

# 1b. Create the Airflow metadata database in local Postgres
systemctl enable postgresql
systemctl start postgresql
sudo -u postgres createuser airflow || true
sudo -u postgres createdb -O airflow airflow || true
# Password is set after /etc/airflow.env is edited by the operator - see next steps below

# 2. Airflow user
useradd -m -s /bin/bash "${AIRFLOW_USER}" || true
mkdir -p "${AIRFLOW_HOME}/dags/lib"
chown -R "${AIRFLOW_USER}:${AIRFLOW_USER}" "${AIRFLOW_HOME}"

# 3. Virtualenv + packages
sudo -u "${AIRFLOW_USER}" "${PYTHON}" -m venv "${AIRFLOW_HOME}/venv"
sudo -u "${AIRFLOW_USER}" "${AIRFLOW_HOME}/venv/bin/pip" install --quiet \
    "apache-airflow[postgres]==2.9.*" \
    psycopg2-binary \
    requests \
    google-cloud-secret-manager \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# 4. Environment file - no secrets stored here; all credentials pulled from Secret Manager at runtime
cat > /etc/airflow.env <<'EOF'
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:AIRFLOW_DB_PASS@localhost/airflow
AIRFLOW__WEBSERVER__SECRET_KEY=CHANGE_ME
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Concurrency limits tuned for e2-medium (2 vCPU, 4 GB RAM)
# Workload is I/O-bound (REST fetch + DB upsert), so we oversubscribe the 2 vCPUs.
AIRFLOW__CORE__PARALLELISM=8
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
AIRFLOW__SCHEDULER__MAX_THREADS=2
AIRFLOW__WEBSERVER__WORKERS=4
AIRFLOW__WEBSERVER__WORKER_CLASS=gthread

# GCP project ID - not sensitive, just config
GCP_PROJECT=your-gcp-project-id

# SMTP
AIRFLOW__SMTP__SMTP_HOST=smtp.example.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_USER=no-reply@example.com
AIRFLOW__SMTP__SMTP_MAIL_FROM=no-reply@example.com
AIRFLOW__SMTP__SMTP_PASSWORD=CHANGE_ME
EOF
chmod 640 /etc/airflow.env

# 5. Systemd units
cat > /etc/systemd/system/airflow-scheduler.service <<'EOF'
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
EnvironmentFile=/etc/airflow.env
User=airflow
ExecStart=/opt/airflow/venv/bin/airflow scheduler
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/airflow-webserver.service <<'EOF'
[Unit]
Description=Airflow Webserver
After=network.target

[Service]
EnvironmentFile=/etc/airflow.env
User=airflow
ExecStart=/opt/airflow/venv/bin/airflow webserver --port 8080
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

echo ""
echo "Next steps:"
echo "  1. Edit /etc/airflow.env - set GCP_PROJECT, AIRFLOW_DB_PASS, SMTP_PASSWORD, SECRET_KEY"
echo "  2. Set the local Postgres password to match AIRFLOW_DB_PASS:"
echo "       sudo -u postgres psql -c \"ALTER USER airflow PASSWORD 'YOURPASS';\""
echo "  3. Ensure VM service account has roles/secretmanager.secretAccessor"
echo "  4. Copy dags/ directory to ${AIRFLOW_HOME}/dags/, and setup/migrations/ to ${AIRFLOW_HOME}/migrations/"
echo "  5. Run: sudo -u airflow env \$(cat /etc/airflow.env | grep -v '^#' | xargs) /opt/airflow/venv/bin/airflow db migrate"
echo "  6. Run: sudo -u airflow env \$(cat /etc/airflow.env | grep -v '^#' | xargs) /opt/airflow/venv/bin/airflow users create \\"
echo "            --username admin --role Admin --firstname Admin --lastname User --email admin@example.com"
echo "  7. Apply the database schema with the migration runner (DATABASE_URL = Cloud SQL DSN)."
echo "     Use the airflow venv python (it has psycopg2), run from the repo checkout:"
echo "       # FRESH database (creates infor schema, reporting views, state table, indexes):"
echo "       DATABASE_URL=<cloud-sql-dsn> /opt/airflow/venv/bin/python setup/migrate.py"
echo "       # database that ALREADY has the baseline schema (existing GCP / on-prem):"
echo "       DATABASE_URL=<cloud-sql-dsn> /opt/airflow/venv/bin/python setup/migrate.py --baseline 003_create_state_table.sql"
echo "       DATABASE_URL=<cloud-sql-dsn> /opt/airflow/venv/bin/python setup/migrate.py"
echo "     Thereafter, trigger the 'datalake_migrate_schema' DAG from the Airflow UI on deploy."
echo "     See setup/migrations/README.md for the full workflow."
echo "  8. systemctl enable --now airflow-scheduler airflow-webserver"
