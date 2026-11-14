"""SQLite persistence layer.

Durability contract
-------------------
Every ingested line is committed together with the *next* byte offset of its
source file in a single SQLite transaction. A crash can therefore only leave
the database in one of two states:

* the line and the advanced offset were both committed, or
* neither was committed.

On restart the collector resumes exactly at the last committed offset, which
guarantees no line is recorded twice and no line is skipped.

WAL + ``synchronous=FULL`` make committed data survive process/container
crashes and power loss while still allowing the API to read while the
ingestor is writing.

Lossless quarantine
-------------------
``malformed_lines.raw_line`` is the human-readable (possibly lossy) text.
``malformed_lines.raw_line_bytes`` holds the *exact* bytes of the source line
(without the terminating ``\\n``) whenever the collector could read them, so
an invalid UTF-8 byte sequence can always be recovered through Base64 even
though SQLite text columns cannot represent it. Neither column is ever
rewritten: old databases keep their text-only rows queryable.

Schema evolution
----------------
``init_db`` creates missing tables and then adds the lossless columns to an
existing ``malformed_lines`` table (``ALTER TABLE ... ADD COLUMN``); old rows
simply have ``NULL`` in the new columns. No existing column or row is
dropped/altered, so pre-upgrade text rows remain queryable.
"""

from __future__ import annotations

import base64
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS ingest_state (
    dev          INTEGER NOT NULL,
    inode        INTEGER NOT NULL,
    source_code  TEXT    NOT NULL,
    path         TEXT    NOT NULL,
    offset       INTEGER NOT NULL,
    last_seen    TEXT    NOT NULL,
    PRIMARY KEY (dev, inode)
);

CREATE TABLE IF NOT EXISTS log_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT,
    path        TEXT,
    status      INTEGER,
    method      TEXT,
    source_code TEXT    NOT NULL,
    dev         INTEGER NOT NULL,
    inode       INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    raw_line    TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Defence in depth: the same physical byte position of one source file can
-- be recorded once only. source_code (the file's archive name) is part of the
-- key so an inode number recycled after deletion can never collide with the
-- previous file's rows. Content is intentionally *not* part of this key, so
-- two lines with identical content are stored as separate rows.
CREATE UNIQUE INDEX IF NOT EXISTS ux_log_events_position
    ON log_events (dev, inode, source_code, byte_offset);
CREATE INDEX IF NOT EXISTS ix_log_events_path   ON log_events (path);
CREATE INDEX IF NOT EXISTS ix_log_events_status ON log_events (status);
CREATE INDEX IF NOT EXISTS ix_log_events_ts     ON log_events (ts);

CREATE TABLE IF NOT EXISTS malformed_lines (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_code    TEXT    NOT NULL,
    dev            INTEGER NOT NULL,
    inode          INTEGER NOT NULL,
    byte_offset    INTEGER NOT NULL,
    raw_line       TEXT    NOT NULL,
    error          TEXT    NOT NULL,
    error_kind     TEXT,
    raw_line_bytes BLOB,
    path           TEXT,
    ingested_at    TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""

# Kept separate from SCHEMA: these indexes reference columns that do not exist
# in a pre-upgrade database until ``_migrate_schema`` has added them.
INDEXES = """
CREATE INDEX IF NOT EXISTS ix_malformed_error_kind ON malformed_lines (error_kind);
CREATE INDEX IF NOT EXISTS ix_malformed_position
    ON malformed_lines (dev, inode, source_code, byte_offset);
"""

# Columns added after the original text-only quarantine table was shipped.
# They are nullable so every existing row remains valid and readable.
_MALFORMED_ADDITIONS = (
    ("error_kind", "TEXT"),
    ("raw_line_bytes", "BLOB"),
    ("path", "TEXT"),
)


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Additive, idempotent migration for databases created by older builds."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(malformed_lines)")}
    for name, decl in _MALFORMED_ADDITIONS:
        if name not in columns:
            # ADD COLUMN cannot add a NOT NULL column without a default, and
            # leaving it nullable is exactly what old rows need.
            conn.execute(f"ALTER TABLE malformed_lines ADD COLUMN {name} {decl}")


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False: the ingestor runs in a background thread while
    # FastAPI request handlers run in the threadpool. Writes are serialised by
    # CollectorLoop.write_lock; WAL permits those writes alongside readers.
    conn = sqlite3.connect(
        db_path, timeout=10, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    _migrate_schema(conn)
    conn.executescript(INDEXES)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit BEGIN/COMMIT; on error everything rolls back together.

    A quarantined line, the good lines that follow it in the same batch and
    the advanced resume offset are all written inside one call, so a write
    failure leaves *none* of them committed and the next poll re-reads the
    same bytes (no gap, no duplicate, no split batch).
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        # Covers failures inside the body *and* a failing COMMIT (e.g. disk
        # full). Either way nothing of this unit may stay committed.
        try:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def get_state(conn: sqlite3.Connection, dev: int, inode: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM ingest_state WHERE dev = ? AND inode = ?",
        (dev, inode),
    ).fetchone()


def all_states(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM ingest_state ORDER BY source_code"
    ).fetchall()


def query_events(
    conn: sqlite3.Connection,
    *,
    path: Optional[str] = None,
    status: Optional[int] = None,
    status_min: Optional[int] = None,
    status_max: Optional[int] = None,
    source_code: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if path is not None:
        where.append("path = ?")
        params.append(path)
    if status is not None:
        where.append("status = ?")
        params.append(status)
    if status_min is not None:
        where.append("status >= ?")
        params.append(status_min)
    if status_max is not None:
        where.append("status <= ?")
        params.append(status_max)
    if source_code is not None:
        where.append("source_code = ?")
        params.append(source_code)
    clause = (" WHERE " + " AND ".join(where)) if where else ""

    total = conn.execute(
        f"SELECT COUNT(*) AS c FROM log_events{clause}", params
    ).fetchone()["c"]

    rows = conn.execute(
        f"""SELECT id, ts, path, status, method, source_code,
                   dev, inode, byte_offset, raw_line, ingested_at
              FROM log_events{clause}
             ORDER BY id
             LIMIT ? OFFSET ?""",
        [*params, limit, offset],
    ).fetchall()

    next_offset = offset + limit if offset + limit < total else None
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "items": [dict(r) for r in rows],
    }


def counts(conn: sqlite3.Connection) -> dict[str, int]:
    row = conn.execute(
        """SELECT
                 (SELECT COUNT(*) FROM log_events)      AS events,
                 (SELECT COUNT(*) FROM malformed_lines) AS malformed,
                 (SELECT COUNT(*) FROM malformed_lines
                   WHERE error_kind = 'invalid_utf8')   AS encoding_errors,
                 (SELECT COUNT(*) FROM ingest_state)    AS tracked_files"""
    ).fetchone()
    return dict(row)


# --------------------------------------------------------------------------
# Quarantine
# --------------------------------------------------------------------------

def insert_malformed(
    conn: sqlite3.Connection,
    *,
    source_code: str,
    dev: int,
    inode: int,
    byte_offset: int,
    raw_text: str,
    raw_bytes: Optional[bytes],
    error: str,
    error_kind: str,
    path: Optional[str],
) -> None:
    """Insert one quarantined line with its exact bytes.

    ``raw_line`` keeps the readable text for backwards compatibility (old
    consumers/tests read it); ``raw_line_bytes`` is the lossless original and
    is what ``/malformed`` exposes as Base64.
    """
    conn.execute(
        """INSERT INTO malformed_lines
               (source_code, dev, inode, byte_offset, raw_line, raw_line_bytes,
                error, error_kind, path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (source_code, dev, inode, byte_offset, raw_text, raw_bytes,
         error, error_kind, path),
    )


def _malformed_row(row: sqlite3.Row) -> dict[str, Any]:
    """Serialise a quarantine row, including a lossless Base64 of the bytes.

    Rows written before the lossless columns existed have
    ``raw_line_bytes = NULL``; for those we fall back to the text column and
    mark the encoding explicitly so a caller cannot mistake the fallback for
    the original bytes.
    """
    item = dict(row)
    blob = item.get("raw_line_bytes")
    if blob is not None:
        raw = bytes(blob)
        item["raw_line_base64"] = base64.b64encode(raw).decode("ascii")
        item["raw_line_bytes_length"] = len(raw)
        item["raw_line_encoding"] = "base64"
        item["raw_line_lossless"] = True
    else:
        # Legacy row: only the textual rendering was stored. Its UTF-8 bytes
        # are the best available representation; flag that it is not the
        # original byte sequence.
        text = item.get("raw_line") or ""
        item["raw_line_base64"] = base64.b64encode(
            text.encode("utf-8", errors="surrogatepass")
        ).decode("ascii")
        item["raw_line_bytes_length"] = None
        item["raw_line_encoding"] = "utf8-of-text-fallback"
        item["raw_line_lossless"] = False
    item["raw_line_bytes"] = None  # never dump raw BLOB as JSON
    item["error_kind"] = item.get("error_kind") or "invalid_json"
    return item


def query_malformed(
    conn: sqlite3.Connection,
    *,
    source_code: Optional[str] = None,
    error_kind: Optional[str] = None,
    dev: Optional[int] = None,
    inode: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if source_code is not None:
        where.append("source_code = ?")
        params.append(source_code)
    if error_kind is not None:
        # Legacy rows have NULL error_kind but were JSON failures; keep them
        # reachable under the historical kind.
        if error_kind == "invalid_json":
            where.append("(error_kind = ? OR error_kind IS NULL)")
        else:
            where.append("error_kind = ?")
        params.append(error_kind)
    if dev is not None:
        where.append("dev = ?")
        params.append(dev)
    if inode is not None:
        where.append("inode = ?")
        params.append(inode)
    clause = (" WHERE " + " AND ".join(where)) if where else ""

    total = conn.execute(
        f"SELECT COUNT(*) AS c FROM malformed_lines{clause}", params
    ).fetchone()["c"]
    rows = conn.execute(
        f"SELECT * FROM malformed_lines{clause} ORDER BY id LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()
    kinds = {
        r["k"]: r["c"] for r in conn.execute(
            "SELECT COALESCE(error_kind, 'invalid_json') AS k, COUNT(*) AS c "
            "FROM malformed_lines GROUP BY 1 ORDER BY 1"
        ).fetchall()
    }
    next_offset = offset + limit if offset + limit < total else None
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "by_error_kind": kinds,
        "items": [_malformed_row(r) for r in rows],
    }


def get_malformed(conn: sqlite3.Connection, malformed_id: int) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM malformed_lines WHERE id = ?", (malformed_id,)
    ).fetchone()
    return None if row is None else _malformed_row(row)
