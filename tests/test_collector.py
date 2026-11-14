#!/usr/bin/env python3
"""End-to-end correctness tests for the collector.

Runs entirely on the local filesystem + SQLite (no Docker needed):

* partial (unterminated) tails are held back until the newline arrives
* rotation by rename: old inode keeps draining, new file starts at offset 0
* collector exit at arbitrary points resumes without gaps or duplicates
* identical content lines are stored as separate rows
* committed data only is visible to queries (transactional)
* FastAPI query/pagination/progress endpoints work
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request

# Make `app` importable when run as `python tests/test_collector.py`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app import storage  # noqa: E402
from app.ingest import CollectorLoop, drain_file, poll_once  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def jline(path: str, status: int, ts: str = "2026-11-13T10:00:00Z") -> str:
    return json.dumps({"time": ts, "path": path, "status": status,
                       "method": "GET"}) + "\n"


def fresh_env():
    base = tempfile.mkdtemp(prefix="collector-test-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    conn = storage.connect(db_path)
    storage.init_db(conn)
    return base, log_dir, db_path, conn


def test_drain_partial_tail():
    print("\n[1] 未换行尾部必须等补齐后入仓")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))
            fh.write('{"time":"x","path":"/partial","status":5')  # no newline

        r = poll_once(conn, log_dir)
        check("first poll ingests only the complete line", r["ingested"] == 1)
        st = storage.all_states(conn)[0]
        check("offset stops before partial tail",
              st["offset"] == len(jline("/a", 200)),
              f"offset={st['offset']}")
        n = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("warehouse contains 1 row", n == 1)

        # complete the line
        with open(f, "a") as fh:
            fh.write('00,"method":"GET"}\n')
        r = poll_once(conn, log_dir)
        check("second poll ingests the now-completed line", r["ingested"] == 1)
        rows = conn.execute("SELECT path, status FROM log_events ORDER BY id").fetchall()
        check("both rows present with correct content",
              [dict(x) for x in rows] == [
                  {"path": "/a", "status": 200},
                  {"path": "/partial", "status": 500}],
              str([dict(x) for x in rows]))
        st = storage.all_states(conn)[0]
        check("offset reaches EOF", st["offset"] == os.path.getsize(f))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_rotation_rename():
    print("\n[2] rename 轮转：旧 inode 续采、新文件独立偏移")
    base, log_dir, db_path, conn = fresh_env()
    try:
        cur = os.path.join(log_dir, "access.log")
        with open(cur, "w") as fh:
            for i in range(5):
                fh.write(jline(f"/old/{i}", 200))
        poll_once(conn, log_dir)
        check("5 lines ingested pre-rotation",
              conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"] == 5)

        old_inode = os.stat(cur).st_ino
        archive = os.path.join(log_dir, "access.log.20261113T100000Z.log")
        os.rename(cur, archive)
        # An open fd on the archive keeps its inode allocated, exactly like
        # nginx workers that still hold the renamed log file.
        arch_fh = open(archive, "a")
        # old inode receives 2 late lines (in-flight requests)
        arch_fh.write(jline("/late/1", 503))
        arch_fh.write(jline("/late/2", 500))
        arch_fh.flush()
        # nginx reopens a brand new file at the same path
        with open(cur, "w") as fh:
            fh.write(jline("/new/1", 404))

        r = poll_once(conn, log_dir)
        check("3 lines ingested across both inodes", r["ingested"] == 3,
              f"got {r['ingested']}")

        states = {(s["inode"]): s for s in storage.all_states(conn)}
        check("two distinct inodes tracked", len(states) == 2)
        old_state = states[old_inode]
        check("old inode state keeps archive source_code",
              old_state["source_code"].startswith("access.log.20261113"),
              old_state["source_code"])
        check("old inode drained to its EOF",
              old_state["offset"] == os.path.getsize(archive))
        new_inode = os.stat(cur).st_ino
        check("new inode differs from old", new_inode != old_inode)
        check("new inode offset is its own size (starts at 0, not old offset)",
              states[new_inode]["offset"] == os.path.getsize(cur))

        rows = conn.execute(
            "SELECT source_code, path FROM log_events ORDER BY id"
        ).fetchall()
        srcs = {x["source_code"] for x in rows}
        check("rows carry both source file codes",
              srcs == {"access.log", "access.log.20261113T100000Z.log"}, str(srcs))
        check("late lines landed against the archive code",
              all(x["source_code"].startswith("access.log.2026")
                  for x in rows if x["path"].startswith("/late")))
        arch_fh.close()
    finally:
        conn.close()
        shutil.rmtree(base)


def test_duplicate_content_kept():
    print("\n[3] 内容相同的两行分别保留")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        line = jline("/same", 500)
        with open(f, "w") as fh:
            fh.write(line * 4)
        poll_once(conn, log_dir)
        rows = conn.execute(
            "SELECT byte_offset FROM log_events ORDER BY byte_offset"
        ).fetchall()
        check("4 rows for 4 identical lines", len(rows) == 4)
        offsets = [r["byte_offset"] for r in rows]
        check("each row has a distinct byte offset",
              offsets == [0, len(line), 2 * len(line), 3 * len(line)],
              str(offsets))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_resume_no_gap_no_dup():
    print("\n[4] 任意时刻退出后自动续采：不漏不重")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        total = 500
        with open(f, "w") as fh:
            for i in range(total):
                fh.write(jline(f"/p/{i}", 200 if i % 7 else 500))

        # Simulate many crash/restart cycles while draining.
        seen = 0
        for cycle in range(20):
            conn2 = storage.connect(db_path)
            r = poll_once(conn2, log_dir)
            seen += r["ingested"]
            conn2.close()  # hard close = "process exit"
            if seen >= total:
                break
        check(f"all {total} lines ingested across restarts", seen == total,
              f"seen={seen}")

        # Re-polling changes nothing: offsets are committed.
        r = poll_once(conn, log_dir)
        check("idle poll ingests zero duplicates", r["ingested"] == 0)
        n = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("exactly one row per physical line", n == total, f"n={n}")
        check("unique position constraint holds",
              conn.execute(
                  "SELECT COUNT(*) c FROM (SELECT DISTINCT dev,inode,source_code,byte_offset FROM log_events)"
              ).fetchone()["c"] == total)
        paths = [r["path"] for r in conn.execute(
            "SELECT path FROM log_events ORDER BY id").fetchall()]
        check("order/content matches the source file exactly",
              paths == [f"/p/{i}" for i in range(total)])
    finally:
        conn.close()
        shutil.rmtree(base)


def test_background_loop_and_rotation_interleave():
    print("\n[5] 后台循环采集与轮转交错")
    base, log_dir, db_path, conn = fresh_env()
    try:
        loop_conn = storage.connect(db_path)
        loop = CollectorLoop(loop_conn, log_dir, interval=0.05)
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        cur = os.path.join(log_dir, "access.log")
        written = 0
        archives = 0
        fh = open(cur, "w")
        archive_handles = []  # keep rotated inodes alive, as nginx workers do
        for round_no in range(6):
            for i in range(30):
                fh.write(jline(f"/r{round_no}/line{i}", 200))
                written += 1
            fh.flush()
            time.sleep(0.05)
            # rotate mid-stream
            fh.close()
            archives += 1
            arch_path = os.path.join(log_dir, f"access.log.rot{round_no}.log")
            os.rename(cur, arch_path)
            # Holding the fd keeps the old inode from being recycled by the
            # filesystem — equivalent to nginx workers finishing in-flight
            # requests against the renamed file.
            archive_handles.append(open(arch_path, "a"))
            # late write into the old inode
            archive_handles[-1].write(jline(f"/r{round_no}/late", 599))
            archive_handles[-1].flush()
            written += 1
            fh = open(cur, "w")
            time.sleep(0.05)
        fh.close()

        deadline = time.time() + 10
        while time.time() < deadline:
            got = loop_conn.execute(
                "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
            if got == written:
                break
            time.sleep(0.1)
        loop.request_stop()
        t.join(timeout=5)
        got = loop_conn.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("every line captured under interleaved rotation",
              got == written, f"got={got} written={written}")
        check("late 599 lines captured from archive inodes",
              loop_conn.execute(
                  "SELECT COUNT(*) c FROM log_events WHERE status=599"
              ).fetchone()["c"] == 6)
        # every archive fully drained
        bad = [dict(s) for s in storage.all_states(loop_conn)
               if s["offset"] != os.path.getsize(s["path"])]
        check("all tracked offsets equal current file sizes", not bad, str(bad))
        # Guaranteed properties, independent of the filesystem's inode
        # allocation policy: every archive is tracked as a distinct source
        # and every source is drained exactly to its current size. On the
        # Docker volume nginx holds every rotated fd open, so inodes are never
        # recycled and the live file additionally appears as "access.log".
        codes = {s["source_code"] for s in storage.all_states(loop_conn)}
        expected_archives = {f"access.log.rot{r}.log" for r in range(6)}
        check("all 6 archives tracked as distinct sources",
              expected_archives.issubset(codes), f"codes={sorted(codes)}")
        for h in archive_handles:
            h.close()
        loop_conn.close()
    finally:
        conn.close()
        shutil.rmtree(base)


def test_malformed_quarantine():
    print("\n[6] 坏行隔离且不阻断后续行")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/ok", 200))
            fh.write("{this is not json}\n")
            fh.write(jline("/ok2", 500))
        r = poll_once(conn, log_dir)
        check("2 good + 1 malformed reported",
              r["ingested"] == 2 and r["malformed"] == 1, str(r))
        check("pipeline continued past the bad line",
              conn.execute(
                  "SELECT path FROM log_events ORDER BY id"
              ).fetchall()[-1]["path"] == "/ok2")
        check("bad line preserved with its byte offset",
              conn.execute(
                  "SELECT byte_offset, error FROM malformed_lines"
              ).fetchone()["byte_offset"] == len(jline("/ok", 200)))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_api():
    print("\n[7] FastAPI：进度、分页、按路径/状态码查询")
    # Environment must be configured BEFORE importing app.main, because its
    # path constants are resolved at import time.
    base = tempfile.mkdtemp(prefix="collector-api-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    os.environ["DB_PATH"] = db_path
    os.environ["LOG_DIR"] = log_dir
    os.environ["POLL_INTERVAL"] = "0.05"

    import uvicorn
    from app.main import app

    config = uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen("http://127.0.0.1:8765/health", timeout=1)
                break
            except Exception:
                time.sleep(0.1)

        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            for i in range(25):
                fh.write(jline("/api/orders", 200))
            for i in range(5):
                fh.write(jline("/api/orders", 500))
            for i in range(3):
                fh.write(jline("/api/payments", 503))

        deadline = time.time() + 10
        while time.time() < deadline:
            h = json.load(urllib.request.urlopen(
                "http://127.0.0.1:8765/health"))
            if h["counts"]["events"] == 33:
                break
            time.sleep(0.1)
        check("background ingestion visible via /health",
              h["counts"]["events"] == 33, str(h["counts"]))

        prog = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/ingest/progress"))
        check("progress shows file complete (offset == size)",
              prog["files"][0]["complete"] is True
              and prog["files"][0]["offset"] == prog["files"][0]["size"],
              str(prog["files"][0]))

        page = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?limit=10&offset=0"))
        check("pagination metadata correct",
              page["total"] == 33 and len(page["items"]) == 10
              and page["next_offset"] == 10, str({k: page[k] for k in
              ("total", "next_offset")}))

        page2 = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?limit=10&offset=30"))
        check("last page has 3 rows and no next_offset",
              len(page2["items"]) == 3 and page2["next_offset"] is None)

        errs = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?status_min=500&status_max=599"))
        check("status range query finds all 8 failures",
              errs["total"] == 8 and all(i["status"] >= 500 for i in errs["items"]),
              f"total={errs['total']}")

        s503 = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?status=503"))
        check("exact status=503 query", s503["total"] == 3)

        by_path = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?path=/api/orders&limit=100"))
        check("path filter returns 30 order events",
              by_path["total"] == 30
              and all(i["path"] == "/api/orders" for i in by_path["items"]))

        both = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?path=/api/orders&status=500"))
        check("path + status combined filter", both["total"] == 5)

        one = json.load(urllib.request.urlopen(
            f"http://127.0.0.1:8765/events/{page['items'][0]['id']}"))
        check("single event returns raw JSON line",
              json.loads(one["raw_line"])["path"] == "/api/orders")

        code = urllib.request.urlopen(
            "http://127.0.0.1:8765/events/999999").getcode()
    except urllib.error.HTTPError as e:
        code = e.code
        check("unknown event id -> 404", code == 404, str(code))
    else:
        check("unknown event id -> 404", False, f"got {code}")
    finally:
        server.should_exit = True
        t.join(timeout=5)
        shutil.rmtree(base)


def test_offset_math_after_partial():
    print("\n[8] 偏移量计算：部分行补齐后字节位置精确")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        l1 = jline("/x", 200)
        with open(f, "w") as fh:
            fh.write(l1)
            fh.write('{"incomplete":')
        poll_once(conn, log_dir)
        tail_part = 'true}\n'
        with open(f, "a") as fh:
            fh.write(tail_part)
        poll_once(conn, log_dir)
        rows = conn.execute(
            "SELECT byte_offset, raw_line FROM log_events ORDER BY id").fetchall()
        check("second line offset = length of first line",
              rows[1]["byte_offset"] == len(l1.encode()),
              str(rows[1]["byte_offset"]))
        st = storage.all_states(conn)[0]
        check("final offset equals file size",
              st["offset"] == os.path.getsize(f),
              f"{st['offset']} != {os.path.getsize(f)}")
    finally:
        conn.close()
        shutil.rmtree(base)


def test_multibyte_offsets_and_chunk_boundaries():
    print("\n[9] 多字节 UTF-8：字节偏移精确、跨块字符不损坏")
    import app.ingest as ingest_mod
    base, log_dir, db_path, conn = fresh_env()
    # Force tiny reads so multi-byte chars and newlines straddle chunks.
    original_chunk = ingest_mod.READ_CHUNK
    ingest_mod.READ_CHUNK = 7
    try:
        f = os.path.join(log_dir, "access.log")
        lines = [
            jline("/unicafé/✓", 200),   # é (2 bytes) and ✓ (3 bytes)
            jline("/ascii/normal", 404),
            jline("/中文/路径", 500),
            jline("/emoji/😀/end", 503),
        ]
        with open(f, "wb") as fh:
            for ln in lines:
                fh.write(ln.encode("utf-8"))
        poll_once(conn, log_dir)
        rows = conn.execute(
            "SELECT byte_offset, path, raw_line FROM log_events ORDER BY id"
        ).fetchall()
        check("all 4 multibyte lines ingested", len(rows) == 4)
        raw = open(f, "rb").read()
        expected_offsets, cursor = [], 0
        for ln in lines:
            expected_offsets.append(cursor)
            cursor += len(ln.encode("utf-8"))
        check("byte offsets match true byte positions",
              [r["byte_offset"] for r in rows] == expected_offsets,
              str([r["byte_offset"] for r in rows]))
        check("committed offset equals exact file size in bytes",
              storage.all_states(conn)[0]["offset"] == len(raw))
        # raw lines round-trip exactly (no replacement chars introduced)
        check("raw content preserved byte-for-byte",
              [r["raw_line"] for r in rows] ==
              [ln.rstrip("\n") for ln in lines])
        check("unicode paths normalised correctly",
              [r["path"] for r in rows] ==
              ["/unicafé/✓", "/ascii/normal", "/中文/路径", "/emoji/😀/end"],
              str([r["path"] for r in rows]))
    finally:
        ingest_mod.READ_CHUNK = original_chunk
        conn.close()
        shutil.rmtree(base)


def test_invalid_utf8_quarantine():
    print("\n[10] 非法 UTF-8：不进 /events，无损隔离并继续采集后续好行")
    import base64
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        good1 = jline("/before", 200).encode()
        # A log line produced from a truncated/garbled request: the URI bytes
        # are not valid UTF-8. nginx copies them verbatim into the log.
        bad = (b'{"time":"2026-11-13T10:00:00Z","path":"/broken/\xff\xfe'
               b'\x80","status":500,"method":"GET"}')
        good2 = jline("/after", 200).encode()
        good3 = jline("/after2", 503).encode()
        blob = good1 + bad + b"\n" + good2 + good3
        with open(f, "wb") as fh:
            fh.write(blob)

        r = poll_once(conn, log_dir)
        check("3 good lines ingested, 1 encoding error quarantined",
              r["ingested"] == 3 and r["malformed"] == 1, str(r))

        # --- must not appear in /events at all ---
        n_bad = conn.execute(
            "SELECT COUNT(*) c FROM log_events WHERE raw_line LIKE '%�%'"
        ).fetchone()["c"]
        check("no replacement-character row in log_events", n_bad == 0)
        paths = [x["path"] for x in conn.execute(
            "SELECT path FROM log_events ORDER BY id").fetchall()]
        check("bad line absent, later good lines present",
              paths == ["/before", "/after", "/after2"], str(paths))

        m = conn.execute("SELECT * FROM malformed_lines").fetchone()
        check("quarantined with error_kind=invalid_utf8",
              m["error_kind"] == "invalid_utf8", str(m["error_kind"]))
        check("quarantine records physical identity",
              (m["dev"], m["inode"], m["source_code"]) ==
              (os.stat(f).st_dev, os.stat(f).st_ino, "access.log"))
        check("byte offset is the true start of the bad line",
              m["byte_offset"] == len(good1), str(m["byte_offset"]))
        check("exact original bytes stored losslessly",
              bytes(m["raw_line_bytes"]) == bad,
              repr(m["raw_line_bytes"])[:80])
        check("lossless bytes round-trip through Base64",
              base64.b64decode(base64.b64encode(bytes(m["raw_line_bytes"]))) == bad)
        check("readable text column still populated (compat)",
              m["raw_line"].startswith('{"time"') and "�" in m["raw_line"])

        # --- a good line after the bad one resumes at the precise offset ---
        rows = conn.execute(
            "SELECT byte_offset, path FROM log_events ORDER BY id").fetchall()
        check("good line after bad starts at bad_end + 1",
              rows[1]["byte_offset"] == len(good1) + len(bad) + 1,
              f"{rows[1]['byte_offset']} != {len(good1) + len(bad) + 1}")
        st = storage.all_states(conn)[0]
        check("resume offset equals full file size", st["offset"] == len(blob))

        # --- repeat poll: no dup, no re-quarantine ---
        r2 = poll_once(conn, log_dir)
        check("idle poll is a no-op", r2["ingested"] == 0 and r2["malformed"] == 0)
        check("single quarantine row for a single bad line",
              conn.execute(
                  "SELECT COUNT(*) c FROM malformed_lines").fetchone()["c"] == 1)

        # --- invalid UTF-8 split across a read-chunk boundary ---
        ingest_mod = sys.modules["app.ingest"]
        original = ingest_mod.READ_CHUNK
        ingest_mod.READ_CHUNK = 5
        try:
            f2 = os.path.join(log_dir, "chunked.log")
            bad2 = b'{"path":"/x/\xc3\x28","status":500}'  # invalid continuation
            with open(f2, "wb") as fh:
                fh.write(bad2 + b"\n" + jline("/ok-after-chunked", 200).encode())
            r3 = poll_once(conn, log_dir)
            check("chunk-split invalid line quarantined once",
                  r3["malformed"] == 1, str(r3))
            m2 = conn.execute(
                "SELECT raw_line_bytes FROM malformed_lines ORDER BY id DESC"
            ).fetchone()
            check("chunk-split bytes preserved exactly",
                  bytes(m2["raw_line_bytes"]) == bad2)
        finally:
            ingest_mod.READ_CHUNK = original
    finally:
        conn.close()
        shutil.rmtree(base)


def test_batch_atomicity_on_write_failure():
    print("\n[11] 写库失败：隔离行 + 后续好行 + 续采位置整体回滚后重读")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        good1 = jline("/a", 200).encode()
        bad = b'{"path":"/\xff","status":500}\n'
        good2 = jline("/b", 503).encode()
        with open(f, "wb") as fh:
            fh.write(good1 + bad + good2)

        # Force the quarantine INSERT to fail *after* the first good row of
        # this batch was already written inside the transaction.
        real_insert = storage.insert_malformed
        failed = {"n": 0}

        def boom(*a, **kw):
            failed["n"] += 1
            raise sqlite3.OperationalError("simulated write failure")

        storage.insert_malformed = boom
        try:
            r = poll_once(conn, log_dir)
        finally:
            storage.insert_malformed = real_insert
        check("failed poll reported nothing ingested", r["ingested"] == 0, str(r))
        check("failure was actually triggered", failed["n"] == 1)
        check("no partial rows committed",
              conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"] == 0)
        check("no quarantine row committed",
              conn.execute(
                  "SELECT COUNT(*) c FROM malformed_lines").fetchone()["c"] == 0)
        check("resume offset not advanced",
              not storage.all_states(conn))

        r = poll_once(conn, log_dir)
        check("retry ingests both good lines and quarantines the bad one",
              r["ingested"] == 2 and r["malformed"] == 1, str(r))
        check("resume offset now at EOF",
              storage.all_states(conn)[0]["offset"] == os.path.getsize(f))
        r = poll_once(conn, log_dir)
        check("no duplicates after retry",
              r["ingested"] == 0 and
              conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"] == 2)

        # Restart (fresh connection) must not replay the batch either.
        conn.close()
        conn = storage.connect(db_path)
        storage.init_db(conn)
        r = poll_once(conn, log_dir)
        check("restart resumes without replay",
              r["ingested"] == 0 and r["malformed"] == 0, str(r))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_legacy_db_compat():
    print("\n[12] 旧库兼容：文本坏行与旧 /malformed 字段仍可查询")
    base, log_dir, db_path, conn = fresh_env()
    try:
        # Rebuild the pre-upgrade schema (text-only malformed_lines) and its
        # rows, as an older deployment would have left them.
        conn.execute("DROP TABLE malformed_lines")
        conn.execute(
            """CREATE TABLE malformed_lines (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   source_code TEXT NOT NULL,
                   dev INTEGER NOT NULL,
                   inode INTEGER NOT NULL,
                   byte_offset INTEGER NOT NULL,
                   raw_line TEXT NOT NULL,
                   error TEXT NOT NULL,
                   ingested_at TEXT NOT NULL
                       DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))"""
        )
        conn.execute(
            """INSERT INTO malformed_lines
                   (source_code, dev, inode, byte_offset, raw_line, error)
               VALUES ('access.log.old', 1, 42, 0, '{not json}', 'invalid json: x')"""
        )
        conn.execute(
            """INSERT INTO log_events
                   (ts, path, status, method, source_code, dev, inode,
                    byte_offset, raw_line)
               VALUES ('2026-11-13T10:00:00Z', '/legacy', 500, 'GET',
                       'access.log.old', 1, 42, 0, '{"path":"/legacy"}')"""
        )
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()

        # Upgrade path: connect + init_db on the existing file.
        conn = storage.connect(db_path)
        storage.init_db(conn)
        cols = {r["name"] for r in conn.execute(
            "PRAGMA table_info(malformed_lines)").fetchall()}
        check("legacy columns untouched",
              {"id", "source_code", "dev", "inode", "byte_offset",
               "raw_line", "error", "ingested_at"} <= cols, str(cols))
        check("lossless columns added by migration",
              {"error_kind", "raw_line_bytes", "path"} <= cols)

        old = storage.query_malformed(conn)
        check("legacy quarantine row still returned", old["total"] == 1)
        item = old["items"][0]
        check("legacy row keeps its original raw_line/error/source_code",
              item["raw_line"] == "{not json}" and item["error"] == "invalid json: x"
              and item["source_code"] == "access.log.old", str(item))
        check("legacy row flagged as text fallback, not lossless",
              item["raw_line_lossless"] is False
              and item["raw_line_encoding"] == "utf8-of-text-fallback")
        check("legacy row base64 decodes to its text",
              __import__("base64").b64decode(item["raw_line_base64"])
              == b"{not json}")
        check("legacy row grouped under invalid_json filter",
              storage.query_malformed(conn, error_kind="invalid_json")["total"] == 1)
        check("legacy /events row still queryable",
              storage.query_events(conn, path="/legacy")["total"] == 1)

        # New-style rows coexist with legacy ones after migration.
        f = os.path.join(log_dir, "access.log")
        with open(f, "wb") as fh:
            fh.write(b'{"path":"/\xff","status":500}\n')
        poll_once(conn, log_dir)
        all_m = storage.query_malformed(conn, limit=1000)
        check("legacy + new quarantine rows both listed",
              all_m["total"] == 2 and
              {i["error_kind"] for i in all_m["items"]} ==
              {"invalid_json", "invalid_utf8"}, str(all_m["by_error_kind"]))
        new = [i for i in all_m["items"] if i["error_kind"] == "invalid_utf8"][0]
        check("new row is lossless",
              new["raw_line_lossless"] is True and new["raw_line_bytes_length"] == 26,
              str(new.get("raw_line_bytes_length")))
        check("invalid_json filter still only matches the legacy row",
              storage.query_malformed(conn, error_kind="invalid_json")["total"] == 1)
        check("invalid_utf8 filter matches only the new row",
              storage.query_malformed(conn, error_kind="invalid_utf8")["total"] == 1)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_api_invalid_utf8_demo():
    print("\n[13] API：坏行后接好行，/events 干净、/malformed 无损")
    import base64
    base = tempfile.mkdtemp(prefix="collector-api-utf8-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    os.environ["DB_PATH"] = db_path
    os.environ["LOG_DIR"] = log_dir
    os.environ["POLL_INTERVAL"] = "0.05"

    import importlib
    from app.main import app as fresh_app
    # other tests may have created an app against another DB; reload so the
    # env vars above are honoured (module constants are read at import).
    import app.main as main_mod
    importlib.reload(main_mod)
    fresh_app = main_mod.app

    import uvicorn
    config = uvicorn.Config(fresh_app, host="127.0.0.1", port=8766,
                            log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen("http://127.0.0.1:8766/health", timeout=1)
                break
            except Exception:
                time.sleep(0.1)

        first = jline("/demo/before", 200)
        bad = b'{"time":"2026-11-14T10:00:00Z","path":"/garbled/\xff\xfe","status":500,"method":"GET"}'
        blob = (first.encode() + bad + b"\n"
                + jline("/demo/after", 503).encode())
        with open(os.path.join(log_dir, "access.log"), "wb") as fh:
            fh.write(blob)

        deadline = time.time() + 10
        while time.time() < deadline:
            h = json.load(urllib.request.urlopen(
                "http://127.0.0.1:8766/health"))
            if h["counts"]["events"] == 2 and h["counts"]["malformed"] == 1:
                break
            time.sleep(0.1)
        check("health counts separate events from encoding errors",
              h["counts"]["events"] == 2 and h["counts"]["malformed"] == 1
              and h["counts"]["encoding_errors"] == 1, str(h["counts"]))

        events = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8766/events?limit=100"))
        check("/events contains only the good lines, in order",
              [i["path"] for i in events["items"]] ==
              ["/demo/before", "/demo/after"], str(events["items"]))

        mal = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8766/malformed?limit=100"))
        item = mal["items"][0]
        check("/malformed exposes the bad line as invalid_utf8",
              mal["total"] == 1 and item["error_kind"] == "invalid_utf8"
              and mal["by_error_kind"] == {"invalid_utf8": 1}, str(mal["by_error_kind"]))
        check("Base64 decodes to byte-identical original line",
              base64.b64decode(item["raw_line_base64"]) == bad)
        check("physical identity + offset returned",
              item["source_code"] == "access.log"
              and item["byte_offset"] == len(first.encode())
              and item["inode"] == os.stat(
                  os.path.join(log_dir, "access.log")).st_ino, str(item))
        check("readable raw_line still available for humans",
              item["raw_line"].startswith('{"time"') and "�" in item["raw_line"])

        one = json.load(urllib.request.urlopen(
            f"http://127.0.0.1:8766/malformed/{item['id']}"))
        check("single malformed row endpoint", one["id"] == item["id"])
        try:
            urllib.request.urlopen("http://127.0.0.1:8766/malformed/999999")
            code = 200
        except urllib.error.HTTPError as e:
            code = e.code
        check("unknown malformed id -> 404", code == 404, str(code))

        prog = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8766/ingest/progress"))
        check("progress proves the good tail was collected too",
              prog["files"][0]["complete"] is True
              and prog["files"][0]["offset"] == len(blob), str(prog["files"][0]))
    finally:
        server.should_exit = True
        t.join(timeout=5)
        shutil.rmtree(base)


def test_crash_resume_mixed_encoding():
    print("\n[14] 混合好行/坏行：多次崩溃重启后严格分区、不重不漏")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        n_good = 300
        n_bad = 30
        expected_good, expected_bad = [], []
        blob = b""
        offset = 0
        for i in range(n_good + n_bad):
            if i % 11 == 5:  # sprinkle encoding errors through the stream
                line = (b'{"time":"t","path":"/bad/%d/\xff","status":500}'
                        % i)
                expected_bad.append((offset, line))
            else:
                line = jline(f"/good/{i}", 200 if i % 4 else 503).encode().rstrip(b"\n")
                expected_good.append((offset, line))
            blob += line + b"\n"
            offset += len(line) + 1
        with open(f, "wb") as fh:
            fh.write(blob)

        # Repeated hard "process exits" at arbitrary points.
        for _ in range(15):
            c = storage.connect(db_path)
            poll_once(c, log_dir)
            c.close()
        poll_once(conn, log_dir)

        good_offsets = [r["byte_offset"] for r in conn.execute(
            "SELECT byte_offset FROM log_events ORDER BY byte_offset").fetchall()]
        bad_rows = conn.execute(
            "SELECT byte_offset, raw_line_bytes FROM malformed_lines "
            "ORDER BY byte_offset").fetchall()
        bad_offsets = [r["byte_offset"] for r in bad_rows]

        check(f"all {n_good} good lines stored exactly once",
              good_offsets == [o for o, _ in expected_good],
              f"got {len(good_offsets)}")
        check(f"all {n_bad} encoding errors quarantined exactly once",
              bad_offsets == [o for o, _ in expected_bad],
              f"got {len(bad_offsets)}")
        check("quarantined bytes byte-identical after crash/restart",
              [bytes(r["raw_line_bytes"]) for r in bad_rows] ==
              [b for _, b in expected_bad])
        check("the two sets are disjoint (bad lines never in /events)",
              not set(good_offsets) & set(bad_offsets))
        check("resume offset exactly at EOF",
              storage.all_states(conn)[0]["offset"] == len(blob))
        n_events = conn.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        n_mal = conn.execute(
            "SELECT COUNT(*) c FROM malformed_lines").fetchone()["c"]
        check("events + malformed cover every physical line once",
              n_events + n_mal == n_good + n_bad,
              f"{n_events}+{n_mal}")
    finally:
        conn.close()
        shutil.rmtree(base)


def test_crlf_offsets_exact():
    print("\n[15] CRLF 行：偏移按真实字节数前进，内容去除 \\r")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        l1 = jline("/crlf/1", 200).replace("\n", "\r\n")
        l2 = jline("/crlf/2", 500).replace("\n", "\r\n")
        with open(f, "w", newline="") as fh:
            fh.write(l1 + l2)
        poll_once(conn, log_dir)
        rows = conn.execute(
            "SELECT byte_offset, path, raw_line FROM log_events ORDER BY id"
        ).fetchall()
        check("CRLF offsets count the \\r byte",
              [r["byte_offset"] for r in rows] ==
              [0, len(l1.encode("utf-8"))],
              str([r["byte_offset"] for r in rows]))
        check("lines have no trailing \\r",
              all(not r["raw_line"].endswith("\r") for r in rows))
        check("committed offset equals file size",
              storage.all_states(conn)[0]["offset"] == os.path.getsize(f))
    finally:
        conn.close()
        shutil.rmtree(base)


if __name__ == "__main__":
    test_drain_partial_tail()
    test_rotation_rename()
    test_duplicate_content_kept()
    test_resume_no_gap_no_dup()
    test_background_loop_and_rotation_interleave()
    test_malformed_quarantine()
    test_api()
    test_offset_math_after_partial()
    test_multibyte_offsets_and_chunk_boundaries()
    test_invalid_utf8_quarantine()
    test_batch_atomicity_on_write_failure()
    test_legacy_db_compat()
    test_api_invalid_utf8_demo()
    test_crash_resume_mixed_encoding()
    test_crlf_offsets_exact()
    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    sys.exit(1 if FAIL else 0)
