# Database schema migrations

A deliberately tiny, framework-free migration system for the Cloud SQL Postgres
database. It is just three things: **ordered `.sql` files + a table that records
which ones ran + a small Python loop that applies the rest.** No new tool to
learn, no Java, pure `psycopg2` (already a project dependency).

## Pieces

| Path | What it is |
|---|---|
| `setup/migrations/NNN_*.sql` | The migrations, applied in filename order, once each. |
| `dags/lib/migrate.py` | The ~110-line runner (`apply_migrations`, `baseline`, `status`). Importable by Airflow. |
| `setup/migrate.py` | CLI wrapper for running locally / from a deploy. |
| `dags/migrate_schema.py` | `datalake_migrate_schema` Airflow DAG — manual trigger, runs the same runner. |
| `schema_migrations` (table) | Created automatically in the target DB; one row per applied version. |

## How a database is described

- **Baseline** = the current full schema, kept canonical in `setup/create_*.sql`.
  A baseline migration is a thin pointer to one of those files via an
  `-- include:` directive, so the baseline is tracked in `schema_migrations`
  without copying a megabyte of DDL into this directory.
- **Changes** = every change after the baseline is a new numbered file
  containing plain, readable SQL.

This directory ships empty. Number your first migration `001`; a typical opening
chain looks like:

```
001_baseline_infor_schema.sql      -- include: ../create_infor_schema.sql
002_create_state_table.sql         -- include: ../create_state_table.sql
003_add_oeel_lastsale_index.sql    <plain SQL>
```

A change that re-applies a whole canonical file re-includes it rather than
freezing a copy: `CREATE OR REPLACE VIEW` is idempotent, so re-applying the
current canonical DDL *is* the change. A database that has not yet run the
earlier include gets the same result from it, and the later migration is a
no-op there. A change to a single object carries its own DDL instead, so the
migration says what it changes.

## Watching a long migration

The runner sends each migration's statements one at a time (still inside that
migration's single transaction) and prints what it is running:

```
-> 003_retype_numeric_columns.sql: 48 statement(s)
   [7/48] ALTER TABLE infor.arsc ALTER COLUMN "ardatccost" TYPE numeric(28,5), …
        ↳ 42.3s
```

Statements taking a second or more get their elapsed time. Files with more than
500 statements (the baseline DDL) print every 50th instead of every one.
`--quiet` turns the narration off.

That still cannot narrate from *inside* a single long statement — a table
rewrite on a large table can run for many minutes with no output. From a second
terminal:

```sh
cd /opt/airflow && venv/bin/python setup/migrate.py --activity
```

It finds the migrator session (it is the one holding the advisory lock, and it
tags itself `application_name=datalake-migrate`) and reports the running
statement, how long it has been going, and — the useful part — whether it is
**waiting on a lock** rather than working, plus which sessions are blocking it
and which it is blocking. Cancelling is safe: each migration is one transaction,
so it rolls back whole and is not recorded.

Note that a migration needing `ACCESS EXCLUSIVE` (anything that rewrites a
table, e.g. `005`) will queue behind the sync DAGs and block everything behind
it while it waits. Pause the syncs for those.

## Running it

On the VM nothing needs setting up — the DSN comes from the same Secret
Manager secret the DAGs use. Use the Airflow venv, which has `psycopg2` and the
Google client libraries:

```sh
cd /opt/airflow
venv/bin/python setup/migrate.py            # apply all pending migrations
venv/bin/python setup/migrate.py --status   # list applied / pending (never blocks)
venv/bin/python setup/migrate.py --activity # what a running migration is doing
venv/bin/python setup/migrate.py --env trn  # target the training database
```

The DSN is resolved as:

1. `DATABASE_URL` if set — always wins, so you can point at any database
   (e.g. `host=… port=5432 dbname=infor user=…`)
2. otherwise secret `cloudsql-dsn-<env>` in the project named by `GCP_PROJECT`,
   or by `GCP_PROJECT=` in `/etc/airflow.env` on the VM

Each command prints which of the two it used. The DSN contains a password and is
never printed.

`--status` and `--activity` are read-only and lock-free, so they stay usable
while a migration is running — which is when you most want them. Only the
writers (`apply`, `--baseline`) take the advisory lock, and if another migrator
holds it they now say so and name the pid instead of hanging silently.

From Airflow: trigger the **`datalake_migrate_schema`** DAG. It pulls the DSN from
Secret Manager (`get_cloudsql_dsn`) and reads the SQL files from `MIGRATIONS_DIR`
(default `/opt/airflow/migrations`), so the deploy must copy `setup/migrations/`
there.

### First-time adoption on an EXISTING database

A database that already has the baseline schema must record those migrations as
applied **without** re-running them (otherwise the runner would try to
`CREATE TABLE` objects that already exist). Do this once per existing database,
naming the last baseline migration:

```sh
cd /opt/airflow
venv/bin/python setup/migrate.py --baseline 002_create_state_table.sql   # record 001-002, no exec
venv/bin/python setup/migrate.py                                         # then apply 003+ for real
```

A brand-new/empty database needs no baseline step — just
`venv/bin/python setup/migrate.py` runs everything in order.

## Adding a change

1. Create the next-numbered file, e.g. `003_add_oeel_lastsale_index.sql`, with plain SQL.
2. `venv/bin/python setup/migrate.py --status` to confirm it shows as pending, then
   apply it.
3. Apply it to **every** environment. Comparing
   `SELECT version FROM schema_migrations` across them is how you catch drift.

Conventions:
- **Never edit a migration that has already been applied anywhere.** Fix forward
  with a new file. (The runner skips files already in `schema_migrations`.)
- Each file runs in its **own transaction**; a failure rolls back that file and
  stops. So keep each migration to one coherent change.
- For a change to a table/view **shape** (new column, retype, new/changed view),
  also update the canonical `setup/create_*.sql` so a fresh bootstrap and the
  dictionary catalog stay accurate. Index/constraint/comment-only changes need
  the migration alone.
- `CREATE INDEX CONCURRENTLY` cannot run inside a transaction; if you need it on a
  large hot table, run that statement by hand (or give it its own migration and
  note it), since the runner wraps each file in a transaction.
- Views: `CREATE OR REPLACE VIEW` is safe to re-run, but it cannot drop or rename
  a view's columns — for that, `DROP VIEW … CASCADE;` then `CREATE` in the migration.
- **`deleted` is nullable — test it, never compare it.** `infor.*.deleted` is
  `bool NULL` and a live record can carry NULL, so `deleted = false`,
  `deleted <> true`, and `NOT deleted` all evaluate to NULL and drop live rows.
  Use `deleted IS NOT TRUE`, or `"deleted" IS NULL OR NOT "deleted"` in SQL that
  must also run on Compass. See the README's "The soft-delete flag".

## Why not a framework?

Flyway/Liquibase are Java; Sqitch is Perl; Atlas is a Go binary with its own DSL;
dbt is Python but a whole framework to learn. For this database's size and a
Python/Airflow shop, the homegrown runner is the simplest readable thing that still
gives per-database version tracking. If richer needs appear later (reverts,
lineage, data tests), `yoyo-migrations` (plain-SQL, pip) or dbt are the natural
next steps.
