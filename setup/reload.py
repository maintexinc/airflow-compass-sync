#!/usr/bin/env python3
"""
Sequential full/partial reload driver for the Data Lake -> Cloud SQL sync.

Runs sync_table for each table ONE AT A TIME in a single process. This is the
memory-safe way to do a full-history reload: Airflow's parallel task execution
(PARALLELISM x 10k-row Compass pages on wide tables) is what OOM-hangs the
e2-medium VM, so for a big reload drive it from here rather than triggering the
sync DAGs.

Requires (same as the DAGs):
    GCP_PROJECT   env var -- secrets + Cloud SQL DSN are resolved from Secret Manager.

Run from a repo checkout so `lib.*` and `config` import:

    cd /opt/airflow && GCP_PROJECT=your-gcp-project venv/bin/python setup/reload.py

Options:
    --tables a,b,c    reload only these tables (default: every table in config)
    --resume          skip tables that already have a watermark -- use this to
                      continue a run that died partway
    --keep-watermark  do NOT clear watermarks (incremental catch-up, not a reload)
    --dry-run         print the plan and exit

Default behaviour clears each table's watermark just before syncing it, forcing a
full initial load. This re-upserts every row (fixing wrong values in place); it
does NOT delete rows. For a truly clean slate that also drops rows no longer in
the source, run setup/truncate_for_reload.sql first, then this script.

Resumable: a finished table has its watermark set, so re-running with --resume
skips it. Per-table failures are logged and the run continues; exit code is
nonzero if any table failed.
"""
import argparse
import logging
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "dags"))  # make lib.* / config importable

from lib.sync import sync_table, get_pg_conn, get_watermark  # noqa: E402
from lib.secrets import get_cloudsql_dsn  # noqa: E402
import config  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("reload")


def all_tables():
    """Every synced table in group order, de-duplicated (config already removes
    higher-frequency tables from the 60-min list, but guard anyway)."""
    ordered = (list(config.TABLES_5MIN)
               + list(config.TABLES_15MIN)
               + list(config.TABLES_60MIN))
    seen, out = set(), []
    for t in ordered:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def clear_watermark(dsn, table):
    conn = get_pg_conn(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM _sync_state WHERE table_name = %s", (table,))
        conn.commit()
    finally:
        conn.close()


def has_watermark(dsn, table):
    conn = get_pg_conn(dsn)
    try:
        return get_watermark(conn, table) is not None
    finally:
        conn.close()


def main(argv):
    ap = argparse.ArgumentParser(description="Sequential reload driver.")
    ap.add_argument("--tables", help="comma-separated subset (default: all)")
    ap.add_argument("--resume", action="store_true",
                    help="skip tables that already have a watermark")
    ap.add_argument("--keep-watermark", action="store_true",
                    help="do not clear watermarks (incremental, not a reload)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    project = os.environ.get("GCP_PROJECT")
    if not project:
        sys.exit("error: set GCP_PROJECT")

    tables = ([t.strip() for t in args.tables.split(",") if t.strip()]
              if args.tables else all_tables())
    dsn = get_cloudsql_dsn(project)

    log.info("reload plan: %d table(s), resume=%s keep_watermark=%s",
             len(tables), args.resume, args.keep_watermark)
    if args.dry_run:
        for t in tables:
            print(t)
        return 0

    ok, failed, skipped = [], [], []
    t0 = time.time()
    for i, table in enumerate(tables, 1):
        if args.resume and has_watermark(dsn, table):
            log.info("[%d/%d] %s: has watermark, skipping (--resume)",
                     i, len(tables), table)
            skipped.append(table)
            continue
        if not args.keep_watermark:
            clear_watermark(dsn, table)  # force a full initial load
        log.info("[%d/%d] %s: starting", i, len(tables), table)
        start = time.time()
        try:
            sync_table(table, project)
            log.info("[%d/%d] %s: done in %.0fs",
                     i, len(tables), table, time.time() - start)
            ok.append(table)
        except Exception:
            log.exception("[%d/%d] %s: FAILED", i, len(tables), table)
            failed.append(table)

    log.info("reload finished in %.0fs: %d ok, %d failed, %d skipped",
             time.time() - t0, len(ok), len(failed), len(skipped))
    if failed:
        log.error("failed tables: %s", ", ".join(failed))
        log.error("re-run with --resume to retry only the unfinished tables")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
