# datalake-sync

Syncs tables from an Infor Data Lake (Compass REST API) into PostgreSQL for
reporting, on a schedule, with per-table watermarks and a daily reconciliation
check against the source.

Two things live here: the Airflow DAGs that do the syncing (`dags/`) and the
warehouse schema plus its migration runner (`setup/`).

---

## Architecture

```
Infor Data Lake (Compass REST API v2, OAuth 2.0)
  -> Self-hosted Apache Airflow  (single VM)
       - DAG: datalake_sync_5min        (every 5 min)
       - DAG: datalake_sync_15min       (every 15 min)
       - DAG: datalake_sync_60min       (hourly)
       - DAG: datalake_validate_daily   (daily)  - reconciliation
       - DAG: datalake_migrate_schema   (manual) - schema migrations
  -> PostgreSQL (Cloud SQL or any managed/self-hosted instance)
       - Schema: infor            (sync target, raw)
       - Schema: reporting        (views over infor, for consumers)
       - Table:  _sync_state       (watermark tracking)
       - Table:  _validation_log   (validation history)
```

Credentials are read from **GCP Secret Manager** at runtime — nothing sensitive
is stored on disk or in this repository. See
[Running without GCP Secret Manager](#running-without-gcp-secret-manager) if you
use a different secret store.

See [INSTALL.md](INSTALL.md) for provisioning and deployment.

---

## Table groups

Tables are grouped by sync frequency in [dags/config.py](dags/config.py). The
groups shipped here are an example set of Infor SX.e tables; replace them with
the tables your tenant actually serves.

| Group | Cadence | Intended for |
|---|---|---|
| `TABLES_5MIN` | every 5 minutes | small, fast-changing tables |
| `TABLES_15MIN` | every 15 minutes | open-order and transaction data |
| `TABLES_60MIN` | hourly | reference and master data |

Cadence is a cost decision as much as a freshness one: each table costs one
Compass query job per run, whether or not anything changed.

---

## How the sync works

Each table sync follows this pattern:

1. **Get current max** — query `SELECT max(infor.lastmodified()) FROM infor.includedeleted('tablename')` to find the latest timestamp in the Data Lake.
2. **Check watermark** — look up `_sync_state` for the last watermark for this table.
3. **First run (no watermark)** — fetch all rows up to current max (full initial load).
4. **Subsequent runs** — fetch only rows where `lastmodified >= last_watermark AND lastmodified <= current_max`.
5. **Resolve columns from the validated map** — each table's column list comes from [dags/column_map.json](dags/column_map.json). The sync never infers columns from row data: Compass omits JSON keys whose value is NULL, so the keys present in a response understate the schema. Inferring from them once nulled out whole columns for entire 10k-row pages. Source keys are matched case-insensitively, values are coerced to the target column type (booleans to `0`/`1`, floats to integers where the column is an integer type), and a key absent from a row is written as NULL — including on updates. Keys not in the map are logged as source schema drift.
6. **Upsert** — write rows using `INSERT ... ON CONFLICT (rowpointer) DO UPDATE`, page by page so memory stays bounded. All tables use `rowpointer` as the primary key. The SQL text is identical for every page and run of a table.
7. **Advance watermark** — update `_sync_state`, only after all pages succeed (a mid-load failure just re-upserts the same rows on retry).

The `infor.includedeleted()` Compass function includes soft-deleted rows, so
deletes in the source are reflected downstream.

### The soft-delete flag

`infor.*.deleted` is `bool NULL`, and a live record can carry NULL rather than
`false` — upstream loads into the Data Lake do not always populate it. Any
predicate that *compares* the column evaluates to NULL under three-valued logic
and silently drops those rows: `deleted = false`, `deleted <> true`, and
`NOT deleted` are all unsafe. Filter with `deleted IS NOT TRUE`.

SQL that must also run on Compass uses `"deleted" IS NULL OR NOT "deleted"`
instead — Compass is T-SQL and has no `IS NOT TRUE`.

The `reporting` views apply this filter so consumers do not have to; see below.

### The reporting schema

[setup/create_reporting_schema.sql](setup/create_reporting_schema.sql) defines a
view per synced table in a separate `reporting` schema. Each is a plain `SELECT`
of its table's columns with `WHERE deleted IS NOT TRUE`, so consumers get live
rows without having to repeat the soft-delete rule — or get it wrong.

Two of the 72 views are derived rather than 1:1 (`kpsk_latest` and
`rtmst_status` fold multiple rows into a current-state row). They are examples
of the pattern, not part of the sync; drop them if they do not fit your data.

The layer is optional; the sync writes to `infor` either way. It exists so that
read access can be granted on `reporting` alone (`reporting_ro`), leaving the
raw tables and the soft-delete semantics out of consumers' hands. The views
shipped here match the example table set; regenerate them for your own tables.

### The validated column map

The sync refuses to run a table that is not in `dags/column_map.json`. The map
records, per table, every column the sync is allowed to write and its target
type. It is what makes step 5 above safe.

**The map shipped here describes the example table set.** Regenerate or hand-edit
it for your own tenant: for each table, list the columns present in *both* your
Postgres schema and the Data Lake's `information_schema`, matched
case-insensitively. A table missing from the map fails loudly rather than
syncing partial data, which is the intended behaviour.

---

## Validation

`datalake_validate_daily` runs three complementary checks on both sides for
every table, all from a single aggregate query per side (so a table still costs
one Compass job). Because both sides use soft deletes and keep full record
history, the aggregates are directly comparable.

1. **Row counts** — total (include-deleted) *and* active (`deleted` is not true).
2. **Per-column non-null counts** — `COUNT(col)` per column. A matching row count can still hide rows upserted with NULLs (e.g. a source column silently dropped by the sync's name-match filter); comparing non-null counts surfaces exactly that.
3. **Per-column sums** — `SUM` over numeric columns, to catch wrong (non-null) values a null-count would miss. Integers are cast to `DECIMAL(38,0)` and numerics wider than precision 28 are skipped, to stay overflow-safe; sums compare within a tiny relative tolerance to absorb float serialization.

The pipeline/meta columns (`rowpointer`, `deleted`, and the `xxx_*`
extraction/modification timestamps) are excluded from the column-level checks.

- Each check is written to `_validation_log` (both total counts + delta, both active counts + delta, a `column_mismatches` JSONB array, `status`, timestamp) so history is queryable.
- A task fails (and emails) when a check diverges, naming the first several discrepancies. There is 1 retry with a 10-minute delay to absorb transient mid-sync deltas.
- **A table missing only deleted rows on the Postgres side does not fail.** It is recorded as `status = 'deleted_gap'` and logged, because deleted rows reach no consumer. This applies only when the active counts match exactly, Postgres is behind rather than ahead, and every column difference is accounted for by the missing rows — missing or extra *active* rows, rows Postgres has that the source does not, and any unexplained value drift still fail.
- A final `cleanup_validation_log` task prunes rows older than 90 days (`RETENTION_DAYS` in [dags/lib/validate.py](dags/lib/validate.py)).

`_validation_log` columns are added automatically (idempotent
`ADD COLUMN IF NOT EXISTS`) on the next run, so an existing deployment needs no
manual migration.

Query the latest status per table:

```sql
SELECT DISTINCT ON (table_name)
       table_name, status, dl_count, pg_count, delta,
       dl_active_count, pg_active_count, active_delta,
       column_mismatches, checked_at
  FROM _validation_log
 ORDER BY table_name, checked_at DESC;
```

`status` is one of `ok`, `deleted_gap` (informational), or `mismatch` (alerting).

### Interpreting a validation mismatch email

The failure email names each failing check with `dl=` (Data Lake), `pg=`
(Postgres), `delta=pg-dl`, and a one-line meaning. Common patterns:

- **`active_count`, pg higher (positive delta)** — deletes in the source didn't reach Postgres. Usually fixed by a reload.
- **`non_null_count`, pg far lower on sparse columns** — the column isn't being written by the sync; check that it is present in `dags/column_map.json`.
- **`non_null_count`, a partial gap on essentially-empty columns** — usually `''`-vs-`NULL` drift, not lost data. `COUNT(col)` counts empty strings as non-null. Reconciles on a clean reload; benign.
- **`sum` gap** — wrong numeric values, not just missing rows.

If a Compass **aggregate disagrees with the actual rows** for the same object,
first rule out empty strings. If it is genuinely inconsistent after a Data
Catalog type change, Compass's stored copy is stale: clear it with
`EXEC INFOR.CLEAR_TABLE('<table>', 'true')` in Compass, then re-query.

---

## Alerting

Failure emails go to the addresses in `default_args["email"]` in each DAG file
(placeholder: `alerts@example.com`). The sync DAGs retry twice before alerting,
so transient outages do not generate noise.

---

## Maintenance

### Force a full resync of one table

Delete the watermark row — the next DAG run does a full initial load:

```sql
DELETE FROM _sync_state WHERE table_name = 'oeeh';
```

To also clear existing rows first:

```sql
DELETE FROM _sync_state WHERE table_name = 'oeeh';
TRUNCATE infor.oeeh;
```

To wipe **all** tables without dropping the schema, use
[setup/truncate_for_reload.sql](setup/truncate_for_reload.sql), then run the
reload driver below.

### Reload sequentially (memory-safe)

[setup/reload.py](setup/reload.py) drives the sync one table at a time in a
single process, avoiding the Airflow parallelism that OOM-hangs a small VM on
wide tables. Use it for any full-history reload instead of triggering the sync
DAGs.

```bash
cd /opt/airflow

# clears each table's watermark, then full-loads every table in config, one at a time
GCP_PROJECT=your-gcp-project venv/bin/python setup/reload.py

GCP_PROJECT=your-gcp-project venv/bin/python setup/reload.py --tables oeeh,oeel   # subset
GCP_PROJECT=your-gcp-project venv/bin/python setup/reload.py --resume             # continue a died run
GCP_PROJECT=your-gcp-project venv/bin/python setup/reload.py --dry-run            # print the plan
```

It re-upserts every row (fixing wrong values in place) but does not delete rows;
for a clean slate that also drops rows no longer in the source, run
`setup/truncate_for_reload.sql` first. Finally run `datalake_validate_daily`.

### Rebuild the database from scratch

**Destructive** — drops all data and the migration tracker; the reload
repopulates from the Data Lake. Set `DATABASE_URL` first.

```bash
cd /opt/airflow
git pull

psql "$DATABASE_URL" -c "DROP SCHEMA IF EXISTS reporting CASCADE;
                         DROP SCHEMA IF EXISTS infor CASCADE;
                         DROP TABLE  IF EXISTS schema_migrations;
                         DROP TABLE  IF EXISTS _sync_state;
                         DROP TABLE  IF EXISTS _validation_log;"

psql "$DATABASE_URL" -f setup/create_roles.sql              # roles (no-op if they exist)
psql "$DATABASE_URL" -f setup/create_infor_schema.sql       # tables
psql "$DATABASE_URL" -f setup/create_reporting_schema.sql   # views + grants
psql "$DATABASE_URL" -f setup/create_state_table.sql        # watermark table
```

Then run [setup/reload.py](setup/reload.py) for the full-history load, and
finally `datalake_validate_daily`. Avoid triggering all three sync DAGs at once
for a from-scratch reload; the parallelism OOM-hangs a small VM (see the
concurrency notes in [INSTALL.md](INSTALL.md#performance-tuning)).

### Add a table

1. Add the table name to the appropriate group in `dags/config.py`.
2. Add its `CREATE TABLE` to `setup/create_infor_schema.sql` and run it.
3. Add its column list to `dags/column_map.json` — the sync refuses to run without it.
4. Add a view for it to `setup/create_reporting_schema.sql` if consumers need one.
5. Redeploy the DAGs. Airflow auto-detects the change within ~30 seconds.

### Remove a table

Remove it from `dags/config.py`, then optionally drop the table and delete its
`_sync_state` row.

### Change a table's sync frequency

Move it between the `TABLES_*` groups in `dags/config.py` and redeploy.

### Rotate Compass credentials

```bash
gcloud secrets versions add compass-ionapi-prd --data-file="new-credentials.ionapi"
```

Running DAGs pick up new credentials within 1 hour (the cache TTL in
[dags/lib/secrets.py](dags/lib/secrets.py)). No restart required.

### Monitor sync health

```sql
SELECT table_name, last_watermark, last_run, last_row_count,
       now() - last_run AS age
  FROM _sync_state
 ORDER BY age DESC;
```

Tables with `age` well past their interval indicate a problem — check the
Airflow UI for failed task runs.

### Airflow service management

```bash
systemctl status airflow-scheduler
systemctl status airflow-webserver
journalctl -u airflow-scheduler -f
```

### Limit Airflow log files

Airflow 2.9 never deletes task logs on its own, and frequent DAGs generate
hundreds of files a day, which can fill the VM disk. Bound retention by age:

```bash
sudo crontab -u airflow -e
# delete task/scheduler logs older than 7 days, then prune empty dirs:
0 3 * * * find /opt/airflow/logs -type f -mtime +7 -delete; find /opt/airflow/logs -type d -empty -delete
```

Use `-mtime +3` during a full-history reload if disk is tight. `airflow db clean`
separately prunes the Airflow metadata DB (rows, not files) — run it monthly.

---

## Environments

Each Compass environment has its own `.ionapi` credential file, stored as its
own secret. The `env` argument selects between them:

| Environment | Secret Manager secret |
|---|---|
| Production | `compass-ionapi-prd` |
| Training | `compass-ionapi-trn` |

To test against training, change `get_ionapi(GCP_PROJECT)` to
`get_ionapi(GCP_PROJECT, env="trn")` in the DAG files.

### Running without GCP Secret Manager

[dags/lib/secrets.py](dags/lib/secrets.py) is the only module that talks to
Secret Manager. It exposes two functions — `get_ionapi(project, env)` returning
the parsed `.ionapi` JSON, and `get_cloudsql_dsn(project, env)` returning a
Postgres DSN string. Replace their bodies to read from Vault, AWS Secrets
Manager, or environment variables, and nothing else needs to change.

`setup/migrate.py` also honours `DATABASE_URL`, which always takes precedence
over the secret store.

---

## Schema migrations

`setup/migrations/NNN_*.sql`, applied in filename order, once each, recorded in
a `schema_migrations` table. Run with `setup/migrate.py` or the
`datalake_migrate_schema` DAG. The directory ships empty — number your first
migration `001`. See [setup/migrations/README.md](setup/migrations/README.md).

The runner wraps **each migration in one transaction**, so migration files must
not contain `BEGIN`/`COMMIT`, and any statement that errors rolls the whole
migration back.

---

## File reference

```
dags/
  config.py            - Table groups by sync frequency
  column_map.json      - Per-table column allowlist and target types
  sync_5min.py         - DAG: 5-minute tables
  sync_15min.py        - DAG: 15-minute tables
  sync_60min.py        - DAG: hourly tables
  validate_daily.py    - DAG: daily reconciliation
  migrate_schema.py    - DAG: manual schema migrations
  lib/
    compass.py         - Compass REST client (OAuth, query submit/poll/fetch)
    sync.py            - Table sync: watermark, paging, upsert
    validate.py        - Data Lake vs Postgres reconciliation
    migrate.py         - Migration runner
    secrets.py         - Secret Manager access with a 1-hour cache

setup/
  install.sh                 - Provision Airflow on a fresh VM
  create_roles.sql           - Database roles the DDL expects (run first)
  create_infor_schema.sql    - Table DDL for the infor schema
  create_reporting_schema.sql- Views over infor, soft-delete filtered
  create_state_table.sql     - Watermark table
  truncate_for_reload.sql    - Wipe all data, keep the schema
  reload.py                  - Sequential full-history reload driver
  migrate.py                 - Migration runner CLI
  migrations/                - Numbered migration files (ships empty)

INSTALL.md   - Provisioning and deployment
```
