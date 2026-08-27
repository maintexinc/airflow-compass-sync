"""
Data validation: compare Data Lake vs Cloud SQL per table.

Three complementary checks, all driven off one aggregate query per side so a
table costs a single Compass job:

  1. Row counts  -- total (include-deleted) AND active (deleted is not true;
     a live record may carry NULL rather than false). A total match with an
     active mismatch means a delete didn't propagate.
  2. Per-column non-null counts. A count match can still hide rows that were
     upserted with NULLs (e.g. a source column silently dropped by the sync's
     name-match filter). Comparing COUNT(col) per column surfaces exactly that.
  3. Per-column SUM over numeric columns -- catches wrong (non-null) values
     that a null-count would miss.

Both sides use soft deletes and keep full history, so the aggregates are
directly comparable (Data Lake counted via infor.includedeleted(), matching
what the sync replicates). The pipeline/meta columns are excluded: rowpointer
and deleted are structural, and the xxx_* extraction/modification timestamps
differ between source and destination by design.

A table missing only DELETED rows on the Postgres side is recorded as
`deleted_gap` and does not fail the check: reporting views filter deleted rows
out, so their absence changes no reported answer. Missing or extra ACTIVE rows,
Postgres holding rows the Data Lake does not, and any column difference the row
gap cannot account for all still fail -- see _is_deleted_only_gap.
"""

import logging
from decimal import Decimal, InvalidOperation

from psycopg2.extras import Json, RealDictCursor

from .compass import CompassClient
from .secrets import get_ionapi, get_cloudsql_dsn
from .sync import SCHEMA, get_pg_conn

log = logging.getLogger(__name__)

VALIDATION_LOG_SQL = """
CREATE TABLE IF NOT EXISTS _validation_log (
    table_name        TEXT NOT NULL,
    dl_count          BIGINT NOT NULL,
    pg_count          BIGINT NOT NULL,
    delta             BIGINT NOT NULL,
    dl_active_count   BIGINT,
    pg_active_count   BIGINT,
    active_delta      BIGINT,
    column_mismatches JSONB,
    status            TEXT,
    checked_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# Bring pre-existing deployments (older 5-column table) up to the new shape.
# The log is app-managed, not part of the schema migrations, so evolve it here.
VALIDATION_LOG_ALTER_SQL = """
ALTER TABLE _validation_log ADD COLUMN IF NOT EXISTS dl_active_count   BIGINT;
ALTER TABLE _validation_log ADD COLUMN IF NOT EXISTS pg_active_count   BIGINT;
ALTER TABLE _validation_log ADD COLUMN IF NOT EXISTS active_delta      BIGINT;
ALTER TABLE _validation_log ADD COLUMN IF NOT EXISTS column_mismatches JSONB;
ALTER TABLE _validation_log ADD COLUMN IF NOT EXISTS status            TEXT;
"""

# How long to retain validation history before pruning.
RETENTION_DAYS = 90

# Pipeline/structural columns excluded from column-level checks.
META_COLUMNS = frozenset({
    "rowpointer",
    "deleted",
    "xxx_extraction_datetime",
    "xxx_last_modified_datetime",
    "transdttmz",
})

_INT_TYPES = frozenset({"smallint", "integer", "bigint"})
_DECIMAL_TYPES = frozenset({"numeric", "decimal"})

# Cap the numeric precision we SUM. Trino's SUM(decimal(p,s)) returns
# decimal(38,s) and throws on overflow, so leave headroom (38 minus ~10 digits
# of row-count growth) rather than risk a query error on a wide table.
_SUM_PRECISION_CAP = 28

# Relative tolerance for SUM comparison: absorbs any decimal->float rounding in
# JSON serialization (float64 carries ~1e-15 relative error) while still
# flagging real drift, which shifts a sum by whole units.
_SUM_REL_TOLERANCE = Decimal("1e-9")

# How many per-column mismatches to name in the raised error message.
_MAX_REPORTED = 8


def cleanup_validation_log(gcp_project: str, retention_days: int = RETENTION_DAYS):
    """Delete _validation_log rows older than the retention window."""
    pg = get_pg_conn(get_cloudsql_dsn(gcp_project))
    try:
        with pg.cursor() as cur:
            cur.execute(VALIDATION_LOG_SQL)
            cur.execute(
                "DELETE FROM _validation_log "
                "WHERE checked_at < now() - make_interval(days => %s)",
                (retention_days,),
            )
            deleted = cur.rowcount
        pg.commit()
        log.info("validation log cleanup: deleted %d rows older than %d days",
                 deleted, retention_days)
    finally:
        pg.close()


def _column_metadata(pg_conn, table: str):
    """Return [(name, data_type, numeric_precision)] for the table's columns."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, numeric_precision
              FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s
             ORDER BY ordinal_position
            """,
            (SCHEMA, table),
        )
        return cur.fetchall()


def _plan_checks(columns):
    """From column metadata, decide which columns get a null-count and a SUM.

    Returns (count_cols, sum_cols): the columns to COUNT(non-null), and the
    subset safe to SUM. Both lists exclude META_COLUMNS.
    """
    count_cols, sum_cols = [], []
    for name, data_type, precision in columns:
        if name in META_COLUMNS:
            continue
        count_cols.append(name)
        if data_type in _INT_TYPES:
            sum_cols.append(name)
        elif data_type in _DECIMAL_TYPES and precision and precision <= _SUM_PRECISION_CAP:
            sum_cols.append(name)
    return count_cols, sum_cols


def _sum_expr(name, data_type_by_col):
    """SUM expression, lossless and overflow-safe on both engines.

    Integers are cast to DECIMAL(38,0) (scale 0 -> no rounding) to avoid bigint
    overflow; bounded numerics are summed natively so their scale is preserved.
    Identical SQL runs on Compass (Trino) and Postgres.
    """
    if data_type_by_col[name] in _INT_TYPES:
        return f'SUM(CAST("{name}" AS DECIMAL(38,0)))'
    return f'SUM("{name}")'


def _build_agg_sql(from_clause, count_cols, sum_cols, data_type_by_col):
    """One SELECT of every aggregate. Same text for both engines."""
    parts = [
        'COUNT(*) AS total_cnt',
        # Active = deleted is not true; a live record may carry NULL rather
        # than false, and bare `NOT "deleted"` is NULL for those rows, which
        # undercounts. Postgres spells this `deleted IS NOT TRUE` (what the
        # reporting views use); Compass is T-SQL and has no such test, and
        # parses a bare `false`/`true` as a column reference, so the explicit
        # IS NULL / NOT form below is the one predicate valid on both engines.
        'COUNT(CASE WHEN "deleted" IS NULL OR NOT "deleted" THEN 1 END) AS active_cnt',
    ]
    parts += [f'COUNT("{c}") AS "nc__{c}"' for c in count_cols]
    parts += [f'{_sum_expr(c, data_type_by_col)} AS "sm__{c}"' for c in sum_cols]
    return f"SELECT {', '.join(parts)} FROM {from_clause}"


def _to_decimal(value):
    """Coerce a SUM result (int, float, str, Decimal, or None) to Decimal.

    None means an empty/all-null column; treat it as 0 so both sides compare
    consistently regardless of which engine returned NULL.
    """
    if value is None:
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _sums_match(a, b):
    da, db = _to_decimal(a), _to_decimal(b)
    if da == db:
        return True
    denom = max(abs(da), abs(db), Decimal(1))
    return abs(da - db) / denom <= _SUM_REL_TOLERANCE


# Plain-language meaning of each check, shown in the failure email so the
# alert is self-explanatory without opening the code.
_CHECK_HINTS = {
    "total_count": "total rows incl. deleted; a gap = rows missing on one side",
    "active_count": "non-deleted rows; pg higher usually = deletes not propagated to Postgres",
    "non_null_count": "rows with a non-null value (note: '' counts as non-null, "
                      "so blank-vs-NULL representation drift shows up here)",
    "sum": "numeric total; a gap = wrong values, not just missing rows",
}


def _delta_str(dl, pg):
    """pg-minus-dl as a signed string; handles ints (counts) and decimals (sums)."""
    try:
        return f"{int(pg) - int(dl):+d}"
    except (TypeError, ValueError):
        try:
            return f"{_to_decimal(pg) - _to_decimal(dl):+}"
        except (InvalidOperation, ValueError):
            return "?"


def _row_summary(dl_total, pg_total, dl_active, pg_active):
    """One plain-English sentence describing the row-count situation, or None
    if the row counts match on both checks."""
    total_d = pg_total - dl_total
    active_d = pg_active - dl_active
    deleted_d = total_d - active_d
    if total_d == 0 and active_d == 0:
        return None
    if total_d == 0:
        return (f"row counts match, but {abs(active_d)} row(s) differ in "
                f"deleted status (pg active {active_d:+d} vs dl)")
    # Normal case: both components shift the same direction as the total.
    if active_d * total_d >= 0 and deleted_d * total_d >= 0:
        side = "Postgres is missing" if total_d < 0 else "Postgres has"
        extra = "" if total_d < 0 else " extra"
        return (f"{side} {abs(total_d)}{extra} row(s) "
                f"({abs(active_d)} active + {abs(deleted_d)} deleted) "
                f"vs the Data Lake")
    return f"row counts differ: total {total_d:+d}, active {active_d:+d} (pg minus dl)"


def _classify_column_mismatches(mismatches, total_delta):
    """Split column-level mismatches into (explained, unexplained) by the
    row-count gap.

    A non_null_count gap is "explained" when it points the same direction as
    the row gap and is no larger -- exactly the signature of N rows missing on
    one side that carry non-null values. A sum gap is explained when that same
    column's non-null count gap is explained (the missing rows carried the
    summed values). A sum gap on a column whose non-null counts MATCH cannot
    come from missing rows: it means different values in rows present on both
    sides, and is always reported.
    """
    explained, unexplained = [], []
    explained_nc_cols = set()
    for m in mismatches:
        if m["column"] is None or m["check"] != "non_null_count":
            continue
        delta = int(m["pg"]) - int(m["dl"])
        if total_delta != 0 and delta * total_delta > 0 and abs(delta) <= abs(total_delta):
            explained.append(m)
            explained_nc_cols.add(m["column"])
        else:
            unexplained.append(m)
    for m in mismatches:
        if m["column"] is None or m["check"] != "sum":
            continue
        (explained if m["column"] in explained_nc_cols else unexplained).append(m)
    return explained, unexplained


def _is_deleted_only_gap(mismatches, dl_total, pg_total, dl_active, pg_active):
    """True when the only difference is soft-deleted rows the Data Lake has and
    Postgres does not.

    Deleted rows reach no consumer: every reporting view filters them out, so
    Postgres holding fewer of them changes no reported answer. Requires all of
      * Postgres short on total rows (never ahead -- extra rows a sync cannot
        have created still need investigating),
      * active counts equal, so nothing live is missing, and
      * every column difference explained by that row gap, so the missing rows
        account for the aggregates and no value has drifted.
    Any of those failing means the gap is not just deleted rows, and the check
    fails as usual.
    """
    if pg_total >= dl_total or pg_active != dl_active:
        return False
    _, unexplained = _classify_column_mismatches(mismatches, pg_total - dl_total)
    return not unexplained


def _format_mismatch_report(table, mismatches, dl_total, pg_total,
                            dl_active, pg_active):
    """Human-readable, multi-line report for the failure email/log.

    Leads with a plain-English row-count summary. Column differences that are
    fully consistent with the row gap (e.g. 7 missing rows shifting 76
    non-null counts by 7) are rolled up into one sentence instead of listed;
    only differences the row gap CANNOT explain -- genuine value drift -- get
    the per-column detail. Full detail is always in _validation_log.
    """
    total_d = pg_total - dl_total
    explained, unexplained = _classify_column_mismatches(mismatches, total_d)
    summary = _row_summary(dl_total, pg_total, dl_active, pg_active)

    lines = []
    if summary:
        lines.append(f"table={table}: {summary}")
        lines.append(f"  rows: dl={dl_total} pg={pg_total}; "
                     f"active: dl={dl_active} pg={pg_active}")
        if explained:
            n_nc = sum(1 for m in explained if m["check"] == "non_null_count")
            n_sm = len(explained) - n_nc
            total_cols = len(explained) + len(unexplained)
            what = (f"All {len(explained)}" if not unexplained
                    else f"{len(explained)} of {total_cols}")
            lines.append(
                f"  {what} column difference(s) "
                f"({n_nc} non-null counts, {n_sm} sums) are consistent with "
                f"those rows -- no evidence of wrong values.")
        if total_d < 0 and not unexplained:
            lines.append(
                "  Likely cause: rows changed in the Data Lake after the last "
                "sync watermark; the next sync run should clear this.")
        elif total_d > 0:
            lines.append(
                "  Postgres has rows the Data Lake does not return -- a sync "
                "cannot remove them; investigate.")
    else:
        lines.append(f"table={table}: {len(unexplained)} column mismatch(es) "
                     f"with matching row counts")

    if unexplained:
        lines.append(f"  {len(unexplained)} difference(s) NOT explained by the "
                     f"row counts  [dl=Data Lake, pg=Postgres, delta=pg-dl]:")
        by_check = {}
        for m in unexplained:
            by_check.setdefault(m["check"], []).append(m)
        for check, ms in by_check.items():
            lines.append(f"    {check} differs on {len(ms)} column(s):")
            lines.append(f"        -> {_CHECK_HINTS.get(check, '')}")
            for m in ms[:_MAX_REPORTED]:
                lines.append(
                    f"          {m['column']}: dl={m['dl']} pg={m['pg']} "
                    f"(delta={_delta_str(m['dl'], m['pg'])})")
            more = len(ms) - _MAX_REPORTED
            if more > 0:
                lines.append(f"          (+{more} more column(s))")

    return "\n".join(lines)


def validate_table(table: str, gcp_project: str):
    """Compare Data Lake and Postgres for one table; raise on any mismatch.

    Runs one aggregate query per side (total + active counts, per-column
    non-null counts, per-column sums), records the outcome to _validation_log
    either way, and raises ValueError listing the discrepancies if any.
    """
    client = CompassClient(get_ionapi(gcp_project))
    pg = get_pg_conn(get_cloudsql_dsn(gcp_project))

    try:
        columns = _column_metadata(pg, table)
        data_type_by_col = {name: dtype for name, dtype, _ in columns}
        count_cols, sum_cols = _plan_checks(columns)

        dl_sql = _build_agg_sql(
            f"infor.includedeleted('{table}')", count_cols, sum_cols, data_type_by_col)
        dl = client.query(dl_sql)[0]

        with pg.cursor(cursor_factory=RealDictCursor) as cur:
            pg_sql = _build_agg_sql(
                f'{SCHEMA}."{table}"', count_cols, sum_cols, data_type_by_col)
            cur.execute(pg_sql)
            pgr = cur.fetchone()

        dl_total, pg_total = int(dl["total_cnt"]), int(pgr["total_cnt"])
        dl_active, pg_active = int(dl["active_cnt"]), int(pgr["active_cnt"])

        mismatches = []
        if dl_total != pg_total:
            mismatches.append(
                {"column": None, "check": "total_count", "dl": dl_total, "pg": pg_total})
        if dl_active != pg_active:
            mismatches.append(
                {"column": None, "check": "active_count", "dl": dl_active, "pg": pg_active})

        # Use .get(): Compass omits result keys whose aggregate is NULL (a SUM
        # over an all-null column), so a direct index would KeyError. A missing
        # count means 0 rows; a missing sum means no numeric data (-> 0 via
        # _to_decimal(None)) -- both the correct comparison value.
        for c in count_cols:
            dl_nc, pg_nc = int(dl.get(f"nc__{c}") or 0), int(pgr.get(f"nc__{c}") or 0)
            if dl_nc != pg_nc:
                mismatches.append(
                    {"column": c, "check": "non_null_count", "dl": dl_nc, "pg": pg_nc})

        for c in sum_cols:
            dl_sm, pg_sm = dl.get(f"sm__{c}"), pgr.get(f"sm__{c}")
            if not _sums_match(dl_sm, pg_sm):
                mismatches.append(
                    {"column": c, "check": "sum",
                     "dl": str(_to_decimal(dl_sm)), "pg": str(_to_decimal(pg_sm))})

        if not mismatches:
            status = "ok"
        elif _is_deleted_only_gap(mismatches, dl_total, pg_total,
                                  dl_active, pg_active):
            status = "deleted_gap"
        else:
            status = "mismatch"

        with pg.cursor() as cur:
            cur.execute(VALIDATION_LOG_SQL)
            cur.execute(VALIDATION_LOG_ALTER_SQL)
            cur.execute(
                """
                INSERT INTO _validation_log (
                    table_name, dl_count, pg_count, delta,
                    dl_active_count, pg_active_count, active_delta,
                    column_mismatches, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (table, dl_total, pg_total, dl_total - pg_total,
                 dl_active, pg_active, dl_active - pg_active,
                 Json(mismatches), status),
            )
        pg.commit()

        if status == "mismatch":
            report = _format_mismatch_report(
                table, mismatches, dl_total, pg_total, dl_active, pg_active)
            log.error("validation FAILED:\n%s", report)
            raise ValueError(report)

        if status == "deleted_gap":
            log.info(
                "validation ok (deleted rows only, not an error):\n%s",
                _format_mismatch_report(
                    table, mismatches, dl_total, pg_total, dl_active, pg_active))
            return

        log.info(
            "table=%s ok: total=%d active=%d, %d columns / %d sums checked",
            table, dl_total, dl_active, len(count_cols), len(sum_cols))

    finally:
        pg.close()
