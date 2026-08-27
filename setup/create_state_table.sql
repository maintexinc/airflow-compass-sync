CREATE TABLE IF NOT EXISTS _sync_state (
    table_name     TEXT PRIMARY KEY,
    last_watermark TIMESTAMPTZ,
    last_run       TIMESTAMPTZ,
    last_row_count INTEGER
);
