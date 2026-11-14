"""FastAPI service: continuous ingestion + query API.

Endpoints
---------
GET  /health                 liveness + counters
GET  /ingest/progress        per-file collection progress (offset/size)
POST /ingest/poll-now        force an immediate collection pass
GET  /events                 paginated events, filter by path / status /
                             status range / source code
GET  /events/{event_id}      single event with the raw JSON line
GET  /malformed              quarantined lines; invalid-UTF-8 rows carry the
                             original bytes as Base64 plus physical identity
GET  /malformed/{id}         single quarantined line
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from . import storage
from .ingest import CollectorLoop, poll_once

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("collector.api")

DB_PATH = os.getenv("DB_PATH", "/data/warehouse.db")
LOG_DIR = os.getenv("LOG_DIR", "/logs")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))


class Health(BaseModel):
    status: str
    log_dir: str
    db_path: str
    poll_interval_seconds: float
    collector_running: bool
    total_polls: int
    total_ingested: int
    counts: dict


class FileProgress(BaseModel):
    source_code: str
    dev: int
    inode: int
    path: str
    offset: int
    size: Optional[int] = None
    complete: bool
    last_seen: str


class ProgressResponse(BaseModel):
    collector_running: bool
    total_polls: int
    total_ingested: int
    last_poll: Optional[dict] = None
    counts: dict
    files: list[FileProgress]


class Event(BaseModel):
    id: int
    ts: Optional[str]
    path: Optional[str]
    status: Optional[int]
    method: Optional[str]
    source_code: str
    dev: int
    inode: int
    byte_offset: int
    raw_line: str
    ingested_at: str


class EventPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: Optional[int]
    items: list[Event]


class MalformedLine(BaseModel):
    id: int
    source_code: str
    dev: int
    inode: int
    byte_offset: int
    # Readable rendering (U+FFFD for undecodable bytes); kept for backwards
    # compatibility. Never use it to recover the original bytes.
    raw_line: str
    # Lossless original bytes, Base64 (standard alphabet, padded). For rows
    # written before the lossless column existed this is the UTF-8 encoding of
    # ``raw_line`` and ``raw_line_lossless`` is false.
    raw_line_base64: str
    raw_line_bytes_length: Optional[int]
    raw_line_encoding: str
    raw_line_lossless: bool
    error: str
    error_kind: str
    path: Optional[str] = None
    ingested_at: str


class MalformedPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: Optional[int]
    by_error_kind: dict[str, int]
    items: list[MalformedLine]


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = storage.connect(DB_PATH)
    storage.init_db(conn)

    loop = CollectorLoop(conn, LOG_DIR, interval=POLL_INTERVAL)
    thread = threading.Thread(target=loop.run_forever, name="collector",
                              daemon=True)
    thread.start()
    state.update(conn=conn, loop=loop, thread=thread)
    log.info("API ready; watching %s, db %s", LOG_DIR, DB_PATH)
    try:
        yield
    finally:
        log.info("shutdown requested, stopping collector loop...")
        loop.request_stop()
        thread.join(timeout=10)
        # A checkpoint persists WAL frames into the main db file on exit.
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            log.exception("checkpoint on shutdown failed")
        conn.close()
        log.info("shutdown complete")


app = FastAPI(
    title="Nginx 访问日志入仓 API",
    description=(
        "持续采集共享只读目录中的 JSON 行 Nginx 日志，按 (设备, inode) "
        "识别文件、行与续采偏移同事务提交，支持按路径与状态码分页查询故障请求。\n\n"
        "**编码错误行**（非法 UTF-8 字节）绝不会出现在 `/events`：它们以 "
        "`error_kind=invalid_utf8` 进入 `/malformed`，`raw_line_base64` 给出"
        "原始行字节的无损 Base64，并带 `dev`/`inode`/`source_code`/"
        "`byte_offset` 物理定位。有效行仍按原规则解析。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


def _conn():
    return state["conn"]


@app.get("/health", response_model=Health, tags=["meta"])
def health():
    loop: CollectorLoop = state["loop"]
    return Health(
        status="ok",
        log_dir=LOG_DIR,
        db_path=DB_PATH,
        poll_interval_seconds=POLL_INTERVAL,
        collector_running=threading.main_thread().is_alive() and not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        counts=storage.counts(_conn()),
    )


@app.get("/ingest/progress", response_model=ProgressResponse, tags=["ingest"])
def progress():
    conn = _conn()
    loop: CollectorLoop = state["loop"]
    rows = storage.all_states(conn)
    # Current sizes from the live directory, read-only.
    current: dict[tuple[int, int], int] = {}
    try:
        names = os.listdir(LOG_DIR)
    except FileNotFoundError:
        names = []
    for name in names:
        try:
            st = os.stat(os.path.join(LOG_DIR, name))
        except FileNotFoundError:
            continue
        current[(st.st_dev, st.st_ino)] = st.st_size

    files = []
    for row in rows:
        size = current.get((row["dev"], row["inode"]))
        files.append(FileProgress(
            source_code=row["source_code"],
            dev=row["dev"],
            inode=row["inode"],
            path=row["path"],
            offset=row["offset"],
            size=size,
            complete=(size is not None and row["offset"] >= size),
            last_seen=row["last_seen"],
        ))
    return ProgressResponse(
        collector_running=not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        last_poll=loop.last_result,
        counts=storage.counts(conn),
        files=files,
    )


@app.post("/ingest/poll-now", tags=["ingest"])
def poll_now():
    """Force an immediate scan/ingest pass (useful for tests/demos)."""
    loop: CollectorLoop = state["loop"]
    with loop.write_lock:
        return poll_once(_conn(), LOG_DIR)


@app.get("/events", response_model=EventPage, tags=["query"])
def list_events(
    path: Optional[str] = Query(None, description="精确路径，例如 /api/orders"),
    status: Optional[int] = Query(None, description="精确状态码，例如 500"),
    status_min: Optional[int] = Query(None, description="状态码下界，例如 400"),
    status_max: Optional[int] = Query(None, description="状态码上界，例如 599"),
    source_code: Optional[str] = Query(None, description="来源文件代号，例如 access.log.20261113T100000.log"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """按路径、状态码分页取数。查故障请求示例：
    ``/events?status_min=500&status_max=599&limit=50``。"""
    return storage.query_events(
        _conn(),
        path=path,
        status=status,
        status_min=status_min,
        status_max=status_max,
        source_code=source_code,
        limit=limit,
        offset=offset,
    )


@app.get("/events/{event_id}", response_model=Event, tags=["query"])
def get_event(event_id: int):
    row = _conn().execute(
        "SELECT * FROM log_events WHERE id = ?", (event_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return dict(row)


@app.get("/malformed", response_model=MalformedPage, tags=["query"])
def list_malformed(
    source_code: Optional[str] = Query(None, description="来源文件代号"),
    error_kind: Optional[str] = Query(
        None, description="错误类型：invalid_utf8 / invalid_json"),
    dev: Optional[int] = Query(None, description="文件设备号（物理身份）"),
    inode: Optional[int] = Query(None, description="文件 inode（物理身份）"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """坏行隔离区。编码错误行（``error_kind=invalid_utf8``）不进入
    ``/events``，这里用 ``raw_line_base64`` 无损返回原始行字节，并附带
    ``dev``/``inode``/``source_code``/``byte_offset`` 以便回到物理文件定位。

    ``raw_line`` 仅为可读渲染（无效字节显示为 �），恢复原字节请用
    ``raw_line_base64``。
    """
    return storage.query_malformed(
        _conn(),
        source_code=source_code,
        error_kind=error_kind,
        dev=dev,
        inode=inode,
        limit=limit,
        offset=offset,
    )


@app.get("/malformed/{malformed_id}", response_model=MalformedLine,
         tags=["query"])
def get_malformed(malformed_id: int):
    item = storage.get_malformed(_conn(), malformed_id)
    if item is None:
        raise HTTPException(status_code=404, detail="malformed line not found")
    return item
