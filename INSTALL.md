# Installation

This covers the Airflow/DAG application layer only. The GCP infrastructure it
runs on — project, VPC, Cloud SQL instance, GCE VM, service accounts, IAM, and
Secret Manager — is assumed to already exist and is out of scope here.

---

## Provision the VM

SSH into the GCE VM as root and run:

```bash
bash setup/install.sh
```

The script does the following automatically:

- Installs Python 3.11, PostgreSQL, and system dependencies
- Creates the `airflow` OS user and home directory at `/opt/airflow`
- Creates the local `airflow` Postgres user and database
- Creates a Python virtualenv at `/opt/airflow/venv` and installs the
  dependencies from `requirements.txt`:
  - `apache-airflow[postgres]==2.9.*`
  - `psycopg2-binary`
  - `requests`
  - `google-cloud-secret-manager`
- Writes `/etc/airflow.env` with placeholder values to be filled in
- Installs and registers the `airflow-scheduler` and `airflow-webserver`
  systemd services (but does not start them)

---

## Post-install steps

### 1. Edit `/etc/airflow.env`

Set your GCP project ID, Airflow DB password, SMTP password, and a random
secret key:

```bash
GCP_PROJECT=your-gcp-project-id
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:YOURPASS@localhost/airflow
AIRFLOW__WEBSERVER__SECRET_KEY=some-long-random-string
AIRFLOW__SMTP__SMTP_HOST=smtp.example.com
AIRFLOW__SMTP__SMTP_PASSWORD=your-smtp-password
```

Generate a secret key with: `python3 -c "import secrets; print(secrets.token_hex(32))"`

### 2. Set the Airflow Postgres user password

The install script created the `airflow` user and database, but you must set
the password to match the one in your `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`:

```bash
sudo -u postgres psql -c "ALTER USER airflow PASSWORD 'YOURPASS';"
sudo -u airflow env $(cat /etc/airflow.env | grep -v '^#' | xargs) \
  /opt/airflow/venv/bin/airflow db migrate
```

### 3. Create an Airflow admin user

```bash
sudo -u airflow env $(cat /etc/airflow.env | grep -v '^#' | xargs) \
  /opt/airflow/venv/bin/airflow users create \
  --username admin --role Admin \
  --firstname Admin --lastname User \
  --email admin@example.com
```

### 4. Deploy the DAGs

The `airflow` user owns `/opt/airflow/dags/`, so upload to your home
directory first and then copy into place:

```bash
cp -r dags/* /opt/airflow/dags/
chown -R airflow:airflow /opt/airflow/dags/
```

### 5. Set up the Cloud SQL schema

Run from any machine with `psql` access to Cloud SQL:

Order matters — the schema DDL opens with
`CREATE SCHEMA infor AUTHORIZATION infor_user`, so the roles must exist first:

```bash
# Create the infor_user / reporting_ro roles (no passwords set)
psql "$CLOUDSQL_DSN" < setup/create_roles.sql

# Create the infor schema and its tables
psql "$CLOUDSQL_DSN" < setup/create_infor_schema.sql

# Create the reporting views over them (optional -- see README)
psql "$CLOUDSQL_DSN" < setup/create_reporting_schema.sql

# Create the watermark tracking table
psql "$CLOUDSQL_DSN" < setup/create_state_table.sql
```

Then set a password for `infor_user` and store it wherever the DSN secret lives:

```bash
psql "$CLOUDSQL_DSN" -c "ALTER ROLE infor_user WITH PASSWORD 'choose-one';"
```

The `_validation_log` table used by the validation DAG is created
automatically on first run, so it does not need a setup script.

To wipe all data and force a full fresh load (truncates every `infor.*`
table and resets the watermarks so the next sync does a full initial load),
run `setup/truncate_for_reload.sql`, then trigger the sync DAGs followed by
`datalake_validate_daily`:

```bash
psql "$CLOUDSQL_DSN" < setup/truncate_for_reload.sql
```

### 6. Start Airflow

```bash
systemctl enable --now airflow-scheduler airflow-webserver
```

The Airflow UI is available at `http://VM-IP:8080`. Put an IAP tunnel or VPN
in front of it rather than exposing port 8080 publicly.

---

## Performance tuning

The concurrency settings in `/etc/airflow.env` are tuned for an **e2-medium
(2 vCPU, 4 GB RAM)**. The sync workload is I/O-bound (Compass REST fetch +
Cloud SQL upsert, page by page), so the 2 vCPUs are safely oversubscribed —
tasks spend most of their time waiting on network and DB, not on CPU.

```bash
AIRFLOW__CORE__PARALLELISM=4                 # global cap on concurrent task slots
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2    # e.g. 4 of the 62 hourly tables sync at once
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1     # never overlap runs of the same DAG
AIRFLOW__SCHEDULER__MAX_THREADS=2            # one DAG-parsing thread per vCPU
AIRFLOW__WEBSERVER__WORKERS=4                # gunicorn workers for UI responsiveness
AIRFLOW__WEBSERVER__WORKER_CLASS=gthread     # threaded workers (vs. one-request-at-a-time sync)
```

Keep `MAX_ACTIVE_TASKS_PER_DAG` ≤ `PARALLELISM` so one DAG can't starve the
others, and leave `MAX_ACTIVE_RUNS_PER_DAG=1` so a slow run never overlaps the
next trigger and double-syncs a table.

Two external ceilings to watch if you raise these further:

- **Cloud SQL `max_connections`** — each concurrent sync task holds at least one
  connection to the target DB. 4-8 in flight is comfortable, but that's the next
  bottleneck before the VM is.
- **Compass API rate limits** — `MAX_ACTIVE_TASKS_PER_DAG` in-flight query jobs.
  If Infor throttles you, dial it back to 2-3.

These are env-only changes. After editing `/etc/airflow.env` on the VM:

```bash
sudo systemctl restart airflow-scheduler airflow-webserver
```

Note: editing the repo's `setup/install.sh` only affects *future* provisions -
`/etc/airflow.env` is written once at install time, so update it on the VM
directly.

---

## Deploying DAG changes

The `airflow` user owns `/opt/airflow/dags/`, so the SSH user can't write
there directly. Upload to a staging directory, then `sudo cp` into place:

```bash
gcloud compute scp --recurse dags/ "$VM_NAME":~/dags-update \
  --project="$GCP_PROJECT" --zone="$VM_ZONE" --tunnel-through-iap

gcloud compute ssh "$VM_NAME" --project="$GCP_PROJECT" --zone="$VM_ZONE" \
  --tunnel-through-iap \
  --command="sudo cp -r ~/dags-update/* /opt/airflow/dags/ && \
             sudo chown -R airflow:airflow /opt/airflow/dags/ && \
             rm -rf ~/dags-update"
```

Airflow auto-detects DAG file changes within ~30 seconds.

To smoke-test a single task without scheduling:

```bash
sudo -u airflow env $(cat /etc/airflow.env | grep -v '^#' | xargs) \
  /opt/airflow/venv/bin/airflow tasks test datalake_sync_5min sync_icswu 2026-06-10
```
