"""Continuous JSON-lines log collector.

Correctness model
=================

1. File identity is ``(st_dev, st_ino)`` — never the path/name. When nginx
   rotates, the old inode keeps being written briefly and the path points at
   a brand new inode; both are tracked independently, so an offset from the
   old file is never applied to the new one.
2. Only lines terminated by ``\\n`` are ingested. A dangling tail without a
   newline stays at the same byte offset and is re-read on the next poll, so
   a half-written line can never enter the warehouse.
3. Rows and the advanced offset are written in one SQLite transaction
   (see :mod:`app.storage`). Any exit — before, during or after commit —
   resumes from the last committed offset: no gaps, no duplicates.
4. Rotated archives are never deleted or truncated; they are simply kept
   in the watched directory and drained to EOF once.
5. Lines are split on raw bytes and decoded strictly. A line that is not
   valid UTF-8 is a *data* problem, not something to paper over: it never
   reaches ``/events``, it is quarantined as ``error_kind='invalid_utf8'``
   with its exact bytes stored as a BLOB. Nothing is rewritten with ``U+FFFD``
   at ingest time, so the original byte sequence can always be recovered
   (Base64) together with its physical identity ``(dev, inode)``,
   ``source_code`` and byte offset. JSON that is merely invalid/unexpected
   keeps the historical ``invalid_json`` quarantine path. Valid lines are
   parsed exactly as before.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from . import storage

log = logging.getLogger("collector.ingest")

# Read at most this many bytes per file per poll; partial tails are handled
# on the next poll, so large files drain over successive ticks.
READ_CHUNK = 1024 * 1024

# Quarantine kinds used in ``malformed_lines.error_kind``.
ERROR_INVALID_UTF8 = "invalid_utf8"
ERROR_INVALID_JSON = "invalid_json"


@dataclass(frozen=True)
class FileIdentity:
    dev: int
    inode: int
    size: int
    path: str
    source_code: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _source_code(path: str) -> str:
    """Stable, human-readable identifier of the source file."""
    return os.path.basename(path)


def normalize_dirs(directory: str | Sequence[str]) -> list[str]:
    """Accept a single directory or several watched roots.

    Keeping this tiny helper means callers (and tests) can pass either form
    without special-casing; production still watches one read-only mount.
    """
    if isinstance(directory, str):
        return [directory]
    return [d for d in directory if d]


def list_log_files(directory: str) -> list[FileIdentity]:
    """Snapshot the directory. A file that disappears mid-scan is skipped
    here and picked up (or detected as missing) on the next poll."""
    try:
        names = sorted(os.listdir(directory))
    except FileNotFoundError:
        log.warning("watched directory %s vanished", directory)
        return []

    files: list[FileIdentity] = []
    for name in names:
        full = os.path.join(directory, name)
        try:
            st = os.stat(full)
        except FileNotFoundError:
            continue
        if not os.path.isfile(full) or os.path.islink(full):
            continue
        files.append(
            FileIdentity(
                dev=st.st_dev,
                inode=st.st_ino,
                size=st.st_size,
                path=full,
                source_code=_source_code(full),
            )
        )
    return files


def parse_line(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    """Return (parsed, error). Never raises — bad lines are quarantined,
    they must not stop the pipeline."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid json: {exc.msg} at pos {exc.pos}"
    if not isinstance(obj, dict):
        return None, "top-level JSON value is not an object"
    return obj, None


@dataclass(frozen=True)
class RawLine:
    """One complete source line, still in its physical byte form."""

    offset: int            # absolute byte offset of the first byte
    data: bytes            # exact bytes from the file (CR of CRLF stripped)
    text: Optional[str]    # decoded text, None iff the bytes are invalid UTF-8
    decode_error: Optional[str] = None


def decode_line(data: bytes) -> tuple[Optional[str], Optional[str]]:
    """Strictly decode one line. Returns ``(text, error)`` — exactly one is
    set. No replacement characters are ever introduced here."""
    try:
        return data.decode("utf-8"), None
    except UnicodeDecodeError as exc:
        # e.g. "'utf-8' codec can't decode byte 0xff in position 10: invalid start byte"
        return None, str(exc)


@dataclass(frozen=True)
class PreparedLine:
    """Classification of one physical line before it is persisted."""

    raw_bytes: bytes
    raw_text: str                 # human-readable rendering (may be lossy)
    parsed: Optional[dict[str, Any]]
    error: Optional[str]
    error_kind: Optional[str]     # None for valid lines

    @property
    def valid(self) -> bool:
        return self.error_kind is None


def prepare_line(line: RawLine) -> PreparedLine:
    """Decide whether a line is an event or a quarantine candidate.

    * invalid UTF-8  -> ``invalid_utf8`` (lossless bytes retained)
    * valid UTF-8 but not a JSON object -> ``invalid_json``
    * otherwise parsed as usual
    """
    if line.text is None:
        # Lossy text is only the readable rendering; ``raw_bytes`` stays
        # pristine and is what the API returns as Base64.
        return PreparedLine(
            raw_bytes=line.data,
            raw_text=line.data.decode("utf-8", errors="replace"),
            parsed=None,
            error=f"invalid utf-8: {line.decode_error}",
            error_kind=ERROR_INVALID_UTF8,
        )

    parsed, err = parse_line(line.text)
    if err is not None:
        return PreparedLine(
            raw_bytes=line.data,
            raw_text=line.text,
            parsed=None,
            error=err,
            error_kind=ERROR_INVALID_JSON,
        )
    return PreparedLine(
        raw_bytes=line.data,
        raw_text=line.text,
        parsed=parsed,
        error=None,
        error_kind=None,
    )


def _normalise(parsed: dict[str, Any], raw: str) -> dict[str, Any]:
    ts = parsed.get("time") or parsed.get("@timestamp") or parsed.get("ts")
    if not isinstance(ts, str):
        ts = None

    # Prefer the path without query string for filtering, but keep the raw
    # line verbatim for full fidelity.
    request = parsed.get("request")
    path = parsed.get("path") or parsed.get("uri")
    if not isinstance(path, str) and isinstance(request, str):
        parts = request.split(" ")
        if len(parts) >= 2:
            path = parts[1]
    if isinstance(path, str):
        path = path.split("?", 1)[0]
    else:
        path = None

    status = parsed.get("status")
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        status = None

    method = parsed.get("method")
    if not isinstance(method, str) and isinstance(request, str):
        method = request.split(" ", 1)[0] or None
    if not isinstance(method, str):
        method = None

    return {"ts": ts, "path": path, "status": status, "method": method}


@dataclass
class _Drained:
    complete_lines: list[RawLine]
    next_offset: int


def drain_file(path: str, start_offset: int, current_size: int) -> _Drained:
    """Read complete, newline-terminated lines starting at ``start_offset``.

    Splitting happens on raw bytes so byte offsets stay exact and a multi-byte
    UTF-8 sequence straddling a read chunk is never torn apart. Decoding is
    strict: an invalid byte sequence is reported on the :class:`RawLine`
    (``text is None``) instead of being replaced, so the caller can quarantine
    the pristine bytes.

    The returned ``next_offset`` never passes the last newline, so an
    unterminated tail is revisited on the next poll.
    """
    out: list[RawLine] = []
    if start_offset > current_size:
        # Truncation (not expected for the archive-style rotation, but handled):
        # the file was replaced/recreated smaller — restart from zero.
        log.warning(
            "file %s shrank (offset %s > size %s); restarting from 0",
            path, start_offset, current_size,
        )
        start_offset = 0

    fd = os.open(path, os.O_RDONLY)
    try:
        os.lseek(fd, start_offset, os.SEEK_SET)
        offset = start_offset
        carry = b""
        while True:
            chunk = os.read(fd, READ_CHUNK)
            if not chunk:
                break
            # Split on raw bytes: a decoded-character count is not a byte
            # count, and a multi-byte UTF-8 sequence straddling a read chunk
            # must not be torn apart.
            *byte_parts, carry = (carry + chunk).split(b"\n")
            for piece in byte_parts:
                start = offset
                offset += len(piece) + 1  # +1 for the consumed '\n'
                if piece.endswith(b"\r"):
                    piece = piece[:-1]
                text, decode_error = decode_line(piece)
                out.append(RawLine(offset=start, data=piece, text=text,
                                   decode_error=decode_error))
        # A non-empty carry without newline: do not advance past it.
        if carry:
            # offset currently points right after the last consumed newline;
            # the tail starts exactly there and will be re-read next poll.
            log.debug("partial tail of %d bytes held back in %s",
                      len(carry), path)
    finally:
        os.close(fd)

    return _Drained(complete_lines=out, next_offset=offset)


def _write_state(
    conn: sqlite3.Connection,
    identity: FileIdentity,
    next_offset: int,
) -> None:
    conn.execute(
        """INSERT INTO ingest_state
               (dev, inode, source_code, path, offset, last_seen)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(dev, inode) DO UPDATE SET
               source_code = excluded.source_code,
               path        = excluded.path,
               offset      = excluded.offset,
               last_seen   = excluded.last_seen""",
        (identity.dev, identity.inode, identity.source_code,
         identity.path, next_offset, _utc_now()),
    )


def _persist_batch(
    conn: sqlite3.Connection,
    identity: FileIdentity,
    lines: list[RawLine],
    next_offset: int,
) -> tuple[int, int]:
    """Persist every complete line and the advanced offset atomically.

    The whole batch is one transaction: quarantined lines, the good lines
    that follow them and the resume offset commit together. If any INSERT
    fails, the transaction rolls back and the next poll re-reads the exact
    same byte range — the isolated line cannot be skipped while its
    successors are committed, nor can rows be duplicated.
    """
    ingested = 0
    malformed = 0
    with storage.transaction(conn):
        for line in lines:
            prepared = prepare_line(line)
            if not prepared.valid:
                # Never enters /events; exact bytes retained for Base64.
                storage.insert_malformed(
                    conn,
                    source_code=identity.source_code,
                    dev=identity.dev,
                    inode=identity.inode,
                    byte_offset=line.offset,
                    raw_text=prepared.raw_text,
                    raw_bytes=prepared.raw_bytes,
                    error=prepared.error or "unparseable line",
                    error_kind=prepared.error_kind or ERROR_INVALID_JSON,
                    path=identity.path,
                )
                malformed += 1
                if prepared.error_kind == ERROR_INVALID_UTF8:
                    log.warning(
                        "quarantined invalid-utf8 line at %s byte %s "
                        "(inode=%s, %s bytes)",
                        identity.source_code, line.offset, identity.inode,
                        len(prepared.raw_bytes),
                    )
                continue

            norm = _normalise(prepared.parsed or {}, prepared.raw_text)
            conn.execute(
                """INSERT INTO log_events
                       (ts, path, status, method, source_code,
                        dev, inode, byte_offset, raw_line)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (norm["ts"], norm["path"], norm["status"], norm["method"],
                 identity.source_code, identity.dev, identity.inode,
                 line.offset, prepared.raw_text),
            )
            ingested += 1

        # Offset advances in the same transaction as the rows above.
        _write_state(conn, identity, next_offset)
    return ingested, malformed


def _process_file(
    conn: sqlite3.Connection,
    identity: FileIdentity,
) -> dict[str, Any]:
    state = storage.get_state(conn, identity.dev, identity.inode)
    if state is None:
        start_offset = 0
        log.info("new file discovered: %s (dev=%s inode=%s)",
                 identity.source_code, identity.dev, identity.inode)
    else:
        start_offset = state["offset"]
        if state["path"] != identity.path:
            # Same inode at a new path = rotation by rename. The inode is the
            # identity: keep draining from the committed offset, and from now
            # on label new rows with the archive's source code. (Inode-number
            # recycling cannot occur here: archives are retained, never
            # deleted, and nginx workers hold the renamed files open.)
            log.info("file renamed (rotation): inode %s %s -> %s; "
                     "resuming at offset %s",
                     identity.inode, state["path"], identity.path,
                     start_offset)
        if start_offset == identity.size:
            return {"source_code": identity.source_code, "ingested": 0,
                    "malformed": 0, "offset": start_offset, "size": identity.size}

    drained = drain_file(identity.path, start_offset, identity.size)

    ingested = 0
    malformed = 0
    if drained.complete_lines or drained.next_offset != start_offset:
        ingested, malformed = _persist_batch(
            conn, identity, drained.complete_lines, drained.next_offset
        )

    return {
        "source_code": identity.source_code,
        "dev": identity.dev,
        "inode": identity.inode,
        "ingested": ingested,
        "malformed": malformed,
        "offset": drained.next_offset,
        "size": identity.size,
    }


def poll_once(conn: sqlite3.Connection, directory: str | Sequence[str]) -> dict[str, Any]:
    """One collection pass over every file currently in the watched roots."""
    started = time.monotonic()
    directories = normalize_dirs(directory)
    files: list[FileIdentity] = []
    for d in directories:
        files.extend(list_log_files(d))
    per_file: list[dict[str, Any]] = []
    total_ingested = 0
    total_malformed = 0

    for identity in files:
        try:
            result = _process_file(conn, identity)
        except FileNotFoundError:
            # Rotated away between scan and open: next poll resolves it.
            log.info("file %s disappeared during scan", identity.source_code)
            continue
        except OSError as exc:
            log.error("I/O error on %s: %s", identity.source_code, exc)
            continue
        except sqlite3.Error:
            # The batch (quarantine rows + good rows + offset) rolled back as
            # a unit. Keep the committed offset untouched and retry next poll.
            log.exception("persist failed for %s; offset not advanced",
                          identity.source_code)
            continue
        per_file.append(result)
        total_ingested += result["ingested"]
        total_malformed += result["malformed"]

    known = {(s["dev"], s["inode"]): s for s in storage.all_states(conn)}
    present = {(f.dev, f.inode) for f in files}
    for key, state in known.items():
        if key not in present:
            # Archived file removed from the shared dir: keep its committed
            # offset so a later re-appearance with the same inode cannot be
            # double-read; record that it is currently absent.
            log.info(
                "tracked file %s (inode=%s) no longer present; "
                "keeping committed offset %s",
                state["source_code"], key[1], state["offset"],
            )

    return {
        "polled_at": _utc_now(),
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
        "files_seen": len(files),
        "ingested": total_ingested,
        "malformed": total_malformed,
        "files": per_file,
    }


class CollectorLoop:
    def __init__(self, conn: sqlite3.Connection, directory: str | Sequence[str],
                 interval: float = 0.5):
        self.conn = conn
        self.directory = directory
        self.interval = interval
        self._stop = False
        # Serialises writers (background loop vs. POST /ingest/poll-now).
        self.write_lock = threading.Lock()
        self.last_result: Optional[dict[str, Any]] = None
        self.total_polls = 0
        self.total_ingested = 0

    def request_stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        log.info("collector started on %s (interval=%ss)",
                 normalize_dirs(self.directory), self.interval)
        while not self._stop:
            try:
                with self.write_lock:
                    result = poll_once(self.conn, self.directory)
                self.last_result = result
                self.total_polls += 1
                self.total_ingested += result["ingested"]
                if result["ingested"] or result["malformed"]:
                    log.info("poll #%s ingested %s line(s), quarantined %s",
                             self.total_polls, result["ingested"],
                             result["malformed"])
            except Exception:
                log.exception("poll failed; will retry")
            # Interruptible sleep so SIGTERM shuts down promptly.
            slept = 0.0
            while slept < self.interval and not self._stop:
                time.sleep(min(0.1, self.interval - slept))
                slept += 0.1
        log.info("collector stopped after %s polls, %s total lines",
                 self.total_polls, self.total_ingested)
