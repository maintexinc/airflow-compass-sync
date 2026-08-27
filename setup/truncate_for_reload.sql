-- Truncate all infor.* tables and reset watermarks for a full fresh load.
--
-- Usage:
--     psql "$CLOUDSQL_DSN" < setup/truncate_for_reload.sql
--
-- After running this, trigger the sync DAGs (datalake_sync_5min / _15min /
-- _60min). Each table has no stored watermark, so the sync performs a full
-- initial load rather than an incremental one. Then run datalake_validate_daily.
--
-- Safe by design:
--   * infor.* tables have no foreign keys between them, so a single multi-table
--     TRUNCATE needs neither CASCADE nor a specific order.
--   * The `reporting` schema is views only; they reflect the reload automatically
--     and TRUNCATE does not touch them.
--   * `_validation_log` and `schema_migrations` are intentionally left intact.
--
-- The watermark reset is the essential second step: the sync decides
-- initial-vs-incremental purely from `_sync_state` (see dags/lib/sync.py). Empty
-- tables + stale watermarks would leave the tables nearly empty after an
-- incremental run.

BEGIN;

-- 1. Truncate every base table in the infor schema.
DO $$
DECLARE
    stmt text;
BEGIN
    SELECT 'TRUNCATE TABLE '
           || string_agg(format('%I.%I', schemaname, tablename), ', ')
      INTO stmt
      FROM pg_tables
     WHERE schemaname = 'infor';
    IF stmt IS NOT NULL THEN
        EXECUTE stmt;
        RAISE NOTICE 'Truncated infor.* tables: %', stmt;
    ELSE
        RAISE NOTICE 'No tables found in the infor schema; nothing truncated';
    END IF;
END $$;

-- 2. Reset watermarks so the next sync does a full initial load.
TRUNCATE TABLE _sync_state;

COMMIT;
