"""
Minimal SQL schema-migration runner — no framework, no new tool.

It applies the ``*.sql`` files in a migrations directory in filename order, once
each, recording applied versions in a ``schema_migrations`` table. That's the
whole idea: ordered SQL files + a table that remembers which ones ran + a loop.

A migration file may delegate to another SQL file with a single directive line::

    -- include: ../create_infor_schema.sql

so the large baseline scripts stay in their canonical place under ``setup/``
(where the dictionary tooling also reads them) instead of being copied in here.

Public functions:
    apply_migrations(dsn, migrations_dir, progress) -> list of versions applied
    baseline(dsn, migrations_dir, through_version)  -> mark <= through applied, NO exec
    status(dsn, migrations_dir)                      -> (applied, pending, running_pid)
    activity(dsn)                                    -> (migrator session, blocked sessions)

A Postgres *session* advisory lock serialises concurrent runners, and each
migration runs in its own transaction, so a failure rolls back that one file and
stops (fix forward with a new migration; never edit an already-applied one).
"""
import glob
import os
import re
import time

import psycopg2

_LOCK_KEY = 728199  # arbitrary constant; one migrator at a time
_INCLUDE = re.compile(r"^\s*--\s*include:\s*(.+?)\s*$", re.M)

# Tags the migrator's session so it can be found in pg_stat_activity.
APP_NAME = "datalake-migrate"

# Progress output. A migration is executed statement by statement so the runner
# can say what is running -- a single ALTER TABLE that rewrites a large table can
# take many minutes, and without this the run is silent until it commits.
_SLOW_SECONDS = 1.0     # report completion time for statements at least this slow
_EVERY = 50             # for large files, print only every Nth statement
_MAX_ECHO = 500         # files with more statements than this use _EVERY

_DOLLAR = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")


def _connect(dsn):
    """Connect, tagging application_name so the session is identifiable."""
    try:
        return psycopg2.connect(dsn, application_name=APP_NAME)
    except TypeError:                       # very old psycopg2
        return psycopg2.connect(dsn)


def split_statements(sql):
    """Split SQL into statements on top-level semicolons.

    Aware of single quotes (with '' escapes), quoted identifiers, dollar-quoted
    bodies ($$ ... $$ / $tag$ ... $tag$) and both comment styles -- a naive split
    on ";" corrupts the DO block that migration 005 uses to drop the views.
    Comment-only fragments are dropped.
    """
    out, buf, i, n = [], [], 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            buf.append(ch)
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        buf.append("''")
                        i += 2
                        continue
                    buf.append("'")
                    i += 1
                    break
                buf.append(sql[i])
                i += 1
            continue
        if ch == '"':
            buf.append(ch)
            i += 1
            while i < n:
                buf.append(sql[i])
                if sql[i] == '"':
                    i += 1
                    break
                i += 1
            continue
        if ch == "$":
            m = _DOLLAR.match(sql, i)
            if m:
                tag = m.group(0)
                end = sql.find(tag, i + len(tag))
                end = n if end == -1 else end + len(tag)
                buf.append(sql[i:end])
                i = end
                continue
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j
            continue
        if sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            j = n if j == -1 else j + 2
            buf.append(sql[i:j])
            i = j
            continue
        if ch == ";":
            out.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    out.append("".join(buf))

    kept = []
    for stmt in out:
        bare = re.sub(r"/\*.*?\*/", " ", stmt, flags=re.S)
        bare = re.sub(r"--[^\n]*", " ", bare)
        if bare.strip():
            kept.append(stmt.strip())
    return kept


def _snippet(stmt, width=96):
    one = " ".join(re.sub(r"--[^\n]*", " ", stmt).split())
    return one if len(one) <= width else one[: width - 1] + "…"


def _run_statements(cur, stmts, version, progress):
    """Execute a migration's statements, narrating as it goes."""
    total = len(stmts)
    echo_all = total <= _MAX_ECHO
    if progress:
        print(f"-> {version}: {total} statement(s)", flush=True)
    for k, stmt in enumerate(stmts, 1):
        show = progress and (echo_all or k % _EVERY == 0 or k == total)
        if show:
            print(f"   [{k}/{total}] {_snippet(stmt)}", flush=True)
        t0 = time.monotonic()
        cur.execute(stmt)
        dt = time.monotonic() - t0
        if progress and dt >= _SLOW_SECONDS:
            print(f"        ↳ {dt:.1f}s", flush=True)


def _migration_files(migrations_dir):
    return sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))


def _sql_for(path):
    """File contents, or — if it carries an ``-- include:`` directive — the
    contents of the referenced file (resolved relative to the migration)."""
    text = open(path, encoding="utf-8").read()
    m = _INCLUDE.search(text)
    if m:
        target = os.path.normpath(os.path.join(os.path.dirname(path), m.group(1)))
        return open(target, encoding="utf-8").read()
    return text


def _applied_versions(conn):
    """Applied versions, creating the bookkeeping table if needed.

    Deliberately takes NO lock: read-only callers (``status``) must never block
    behind a running migration.
    """
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "  version text PRIMARY KEY,"
            "  applied_at timestamptz NOT NULL DEFAULT now())"
        )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM schema_migrations")
        return {r[0] for r in cur.fetchall()}


def _lock_holder(conn):
    """pid holding the migration advisory lock, or None."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_locks WHERE locktype = 'advisory' "
            "AND classid = 0 AND objid = %s AND objsubid = 1 AND granted",
            (_LOCK_KEY,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _lock(conn):
    """Serialise migrators. Announces the wait instead of hanging silently."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (_LOCK_KEY,))
        if cur.fetchone()[0]:
            return
    holder = _lock_holder(conn)
    print(f"another migration is running (pid {holder}); waiting for it to "
          f"finish — check it with: migrate.py --activity", flush=True)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))


def _ensure(conn):
    """Take the advisory lock and return applied versions (writers only)."""
    _lock(conn)
    return _applied_versions(conn)


def apply_migrations(dsn, migrations_dir, progress=True):
    """Run every pending migration in order. Returns the versions applied.

    Statements are sent one at a time so progress can be reported, but they all
    still run inside the migration's single transaction -- atomicity is
    unchanged. Set ``progress=False`` for silence.
    """
    conn = _connect(dsn)
    try:
        done = _ensure(conn)
        applied = []
        for path in _migration_files(migrations_dir):
            version = os.path.basename(path)
            if version in done:
                continue
            stmts = split_statements(_sql_for(path))
            t0 = time.monotonic()
            with conn.cursor() as cur:  # one transaction per migration
                _run_statements(cur, stmts, version, progress)
                cur.execute(
                    "INSERT INTO schema_migrations(version) VALUES (%s)", (version,)
                )
            conn.commit()
            applied.append(version)
            print(f"applied {version} ({time.monotonic() - t0:.1f}s)", flush=True)
        if not applied:
            print("already up to date")
        return applied
    finally:
        conn.close()


def activity(dsn):
    """What the migrator session is doing, seen from a separate connection.

    The runner cannot report from inside a long statement -- it is busy in the
    server. This finds that session (it holds the advisory lock) and reports the
    running statement, how long it has been going, and whether it is waiting on
    a lock rather than working. Also lists sessions it is blocking, which is what
    you want before deciding to cancel.
    """
    conn = _connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pid FROM pg_locks WHERE locktype = 'advisory' "
                "AND classid = 0 AND objid = %s AND objsubid = 1 AND granted",
                (_LOCK_KEY,),
            )
            pids = [r[0] for r in cur.fetchall()]
            if not pids:
                return None, []
            cur.execute(
                "SELECT pid, state, wait_event_type, wait_event,"
                "       now() - xact_start, now() - query_start,"
                "       pg_blocking_pids(pid), query"
                "  FROM pg_stat_activity WHERE pid = ANY(%s)",
                (pids,),
            )
            cols = ("pid", "state", "wait_event_type", "wait_event",
                    "xact_elapsed", "query_elapsed", "blocked_by", "query")
            me = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute(
                "SELECT pid, state, now() - query_start, left(query, 80)"
                "  FROM pg_stat_activity"
                " WHERE %s && pg_blocking_pids(pid) AND pid <> ALL(%s)",
                (pids, pids),
            )
            blocked = [dict(zip(("pid", "state", "elapsed", "query"), r))
                       for r in cur.fetchall()]
            # Progress proxy: a rewrite takes AccessExclusiveLock on each table
            # and holds it to commit. The changes are invisible to us, but the
            # locks are not -- so the count of locked tables is how far it got.
            cur.execute(
                "SELECT count(*), max(c.relname) FROM pg_locks l"
                "  JOIN pg_class c ON c.oid = l.relation"
                "  JOIN pg_namespace n ON n.oid = c.relnamespace"
                " WHERE l.pid = ANY(%s) AND l.mode = 'AccessExclusiveLock'"
                "   AND n.nspname = 'infor' AND c.relkind = 'r'",
                (pids,),
            )
            n_tables, furthest = cur.fetchone()
        if me:
            me[0]["tables_locked"] = n_tables
            me[0]["furthest_table"] = furthest
        return (me[0] if me else None), blocked
    finally:
        conn.close()


def baseline(dsn, migrations_dir, through_version):
    """Record every migration up to and including ``through_version`` as applied
    WITHOUT executing it. Run once on a database that already has the baseline
    schema, so the runner won't try to re-create existing objects."""
    conn = _connect(dsn)
    try:
        done = _ensure(conn)
        marked = []
        for path in _migration_files(migrations_dir):
            version = os.path.basename(path)
            if version > through_version:
                break
            if version in done:
                continue
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO schema_migrations(version) VALUES (%s)", (version,)
                )
            conn.commit()
            marked.append(version)
            print(f"baselined {version} (recorded, not executed)")
        if not marked:
            print("nothing to baseline")
        return marked
    finally:
        conn.close()


def status(dsn, migrations_dir):
    """Return (applied, pending, running_pid).

    Read-only and lock-free -- it must stay usable while a migration is running,
    which is exactly when you want to ask.  ``running_pid`` is the pid of a
    migrator currently holding the advisory lock, or None.
    """
    conn = _connect(dsn)
    try:
        done = _applied_versions(conn)
        running = _lock_holder(conn)
    finally:
        conn.close()
    applied, pending = [], []
    for path in _migration_files(migrations_dir):
        version = os.path.basename(path)
        (applied if version in done else pending).append(version)
    return applied, pending, running
