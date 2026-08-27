#!/usr/bin/env python3
"""
CLI for the SQL schema-migration runner (implementation in dags/lib/migrate.py).

On the VM the DSN is found automatically — same secret the DAGs use — so no
environment setup is needed:

    venv/bin/python setup/migrate.py                  # apply all pending migrations
    venv/bin/python setup/migrate.py --status         # applied / pending (never blocks)
    venv/bin/python setup/migrate.py --activity       # what a RUNNING migration is
                                                      # doing (from a second terminal)
    venv/bin/python setup/migrate.py --quiet          # apply without per-statement output
    venv/bin/python setup/migrate.py --env trn        # target the training database
    venv/bin/python setup/migrate.py --baseline 003_create_state_table.sql
        # record migrations up to that file as applied WITHOUT running them
        # (run once on a database that ALREADY has the baseline schema, e.g. the
        #  existing GCP and on-prem databases, so 001-003 aren't re-executed)

The DSN is resolved in this order:

    1. $DATABASE_URL, if set — always wins, so you can point at any database
    2. Secret Manager secret ``cloudsql-dsn-<env>`` in the project named by
       $GCP_PROJECT, or by GCP_PROJECT= in /etc/airflow.env on the VM

The DSN holds a password and is never printed; only where it came from is.

Run from a repo checkout (so dags/lib is importable and setup/migrations is found).
See setup/migrations/README.md for the full workflow.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS = os.path.join(HERE, "migrations")
sys.path.insert(0, os.path.join(HERE, "..", "dags"))  # make `lib.migrate` importable

from lib.migrate import activity, apply_migrations, baseline, status  # noqa: E402

AIRFLOW_ENV_FILE = "/etc/airflow.env"


def _project_from_env_file(path=AIRFLOW_ENV_FILE):
    """GCP_PROJECT out of the VM's systemd EnvironmentFile, or None.

    Plain KEY=value written by setup/install.sh; quotes tolerated in case it is
    edited by hand.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("GCP_PROJECT="):
                    return line.split("=", 1)[1].strip().strip("\"'") or None
    except OSError:
        pass
    return None


def resolve_dsn(env="prd"):
    """(dsn, human-readable source). Exits with guidance if it cannot be found."""
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn, "$DATABASE_URL"

    project = os.environ.get("GCP_PROJECT") or _project_from_env_file()
    if not project:
        sys.exit(
            "error: no database configured.\n"
            "  Set DATABASE_URL, or set GCP_PROJECT so the DSN can be read from\n"
            f"  Secret Manager (cloudsql-dsn-{env}). On the VM GCP_PROJECT comes\n"
            f"  from {AIRFLOW_ENV_FILE}, which was not readable."
        )
    try:
        from lib.secrets import get_cloudsql_dsn
        return (get_cloudsql_dsn(project, env),
                f"Secret Manager (cloudsql-dsn-{env}, project {project})")
    except ImportError:
        sys.exit(
            "error: google-cloud-secret-manager is not installed in this "
            "interpreter.\n  Use the Airflow venv (/opt/airflow/venv/bin/python) "
            "or set DATABASE_URL yourself."
        )
    except Exception as exc:                      # noqa: BLE001 - report and stop
        first = str(exc).strip().splitlines()[0]  # API errors are multi-line protobuf
        sys.exit(
            f"error: could not read cloudsql-dsn-{env} from project {project}:\n"
            f"  {first}\n"
            f"  Check the VM's service account has "
            f"roles/secretmanager.secretAccessor, or set DATABASE_URL yourself."
        )


def main(argv):
    env = "prd"
    if "--env" in argv:
        i = argv.index("--env")
        if i + 1 >= len(argv):
            sys.exit("usage: migrate.py --env <prd|trn> [other options]")
        env = argv[i + 1]
        if env not in ("prd", "trn"):
            sys.exit(f"error: unknown environment {env!r} (expected prd or trn)")
        argv = argv[:i] + argv[i + 2:]

    dsn, source = resolve_dsn(env)
    print(f"database: {source}", flush=True)

    if argv and argv[0] == "--status":
        applied, pending, running = status(dsn, MIGRATIONS)
        print("applied:", ", ".join(applied) or "(none)")
        print("pending:", ", ".join(pending) or "(none)")
        if running:
            print(f"NOTE: a migration is running now (pid {running}) — "
                  f"see: migrate.py --activity")
    elif argv and argv[0] == "--activity":
        return _activity(dsn)
    elif argv and argv[0] == "--baseline":
        if len(argv) < 2:
            sys.exit("usage: migrate.py --baseline <through_version.sql>")
        baseline(dsn, MIGRATIONS, argv[1])
    elif argv and argv[0] in ("-q", "--quiet"):
        apply_migrations(dsn, MIGRATIONS, progress=False)
    elif argv:
        sys.exit(f"unknown argument: {argv[0]}")
    else:
        apply_migrations(dsn, MIGRATIONS)


def _activity(dsn):
    """Print what a running migration is doing. Safe to run any time."""
    me, blocked = activity(dsn)
    if me is None:
        print("no migration is running (nobody holds the advisory lock)")
        return 0
    waiting = me["wait_event_type"] == "Lock"
    print(f"migrator pid {me['pid']}  state={me['state']}")
    print(f"  transaction open : {me['xact_elapsed']}")
    print(f"  current statement: {me['query_elapsed']}")
    if waiting:
        print(f"  WAITING on a lock ({me['wait_event']}) — blocked by pid(s) "
              f"{me['blocked_by']}; it is queued, not working")
    elif me["wait_event_type"]:
        print(f"  waiting: {me['wait_event_type']}/{me['wait_event']}")
    else:
        print("  running (not blocked)")
    if me.get("tables_locked"):
        print(f"  progress         : {me['tables_locked']} infor table(s) locked so "
              f"far (rewrites hold the lock to commit); furthest alphabetically: "
              f"{me['furthest_table']}")
    q = " ".join((me["query"] or "").split())
    if q.lstrip().startswith("--"):
        print("  NOTE: the whole migration file is one statement -- this is the "
              "pre-batching runner. Newer code sends statements one at a time "
              "and prints [k/N] progress.")
    print(f"  sql: {q[:300]}{'…' if len(q) > 300 else ''}")
    if blocked:
        print(f"\n  it is blocking {len(blocked)} session(s):")
        for b in blocked[:10]:
            print(f"    pid {b['pid']:>7}  {b['elapsed']}  {b['query']}")
    print("\ncancelling is safe: each migration is one transaction, so it rolls "
          "back whole and is not recorded.")
    print(f"  SELECT pg_cancel_backend({me['pid']});")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]) or 0)
