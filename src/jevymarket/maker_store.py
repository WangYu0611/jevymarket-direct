"""Isolated maker journal; async writer moves SQLite commits off the reaction path."""
from __future__ import annotations

import asyncio
import gzip
import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from .maker_config import VERSION, MakerConfig
from .maker_engine import PaperEngine


def encode(data):
    return json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


@contextmanager
def single_process(path: Path):
    handle = open(str(path) + ".lock", "a+b")
    try:
        handle.seek(0)
        if not handle.read(1):
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            import msvcrt
        except ImportError:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        yield
    finally:
        # OS releases the lock even after a crash; never delete another lock.
        handle.close()


class MakerStore:
    def __init__(self, path: Path, config: MakerConfig):
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.failed = False
        self.stopping = False
        with sqlite3.connect(self.path) as conn:
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if tables and "maker_meta" not in tables:
                raise ValueError("拒绝写入非Maker数据库；旧v4数据不迁移不修改")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS maker_meta(id INTEGER PRIMARY KEY CHECK(id=1), data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS maker_orders(id TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS maker_events(id INTEGER PRIMARY KEY, ts REAL NOT NULL, kind TEXT NOT NULL, data TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS maker_events_kind ON maker_events(kind, id);
            """)
            protocol = {"version": VERSION, "config": asdict(config), "paper_only": True}
            current = conn.execute("SELECT data FROM maker_meta WHERE id=1").fetchone()
            if current and json.loads(current[0]) != protocol:
                raise ValueError("实验参数已变，请使用新的Maker数据库，不能混样")
            conn.execute("INSERT OR IGNORE INTO maker_meta VALUES(1,?)", (encode(protocol),))

    def emit(self, kind, data):
        if self.failed:
            raise RuntimeError("journal_writer_failed")
        try:
            self.queue.put_nowait((time.time(), kind, encode(data)))
        except asyncio.QueueFull as exc:
            self.failed = True
            raise RuntimeError("journal_queue_overflow_stop") from exc

    def load_orders(self):
        with readonly(self.path) as conn:
            return [json.loads(r[0]) for r in conn.execute("SELECT data FROM maker_orders ORDER BY rowid")]

    def _batch(self, batch):
        with sqlite3.connect(self.path, timeout=5) as conn:
            for ts, kind, data in batch:
                conn.execute("INSERT INTO maker_events(ts,kind,data) VALUES(?,?,?)", (ts, kind, data))
                if kind == "order":
                    conn.execute("INSERT INTO maker_orders VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
                                 (json.loads(data)["id"], data))

    async def writer(self):
        try:
            while not self.stopping or not self.queue.empty():
                batch = []
                try:
                    row = await asyncio.wait_for(self.queue.get(), 0.1)
                except TimeoutError:
                    continue
                batch.append(row)
                while len(batch) < 500 and not self.queue.empty():
                    batch.append(self.queue.get_nowait())
                await asyncio.to_thread(self._batch, batch)
                for _ in batch:
                    self.queue.task_done()
        except BaseException:
            self.failed = True
            raise


@contextmanager
def readonly(path: Path):
    if not path.is_file():
        raise ValueError("找不到指定的Maker数据库")
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        yield conn
    finally:
        conn.close()


def statistics(path: Path, out: Path | None = None):
    with readonly(path) as conn:
        row = conn.execute("SELECT data FROM maker_meta WHERE id=1").fetchone()
        if not row:
            raise ValueError("不是Maker实验数据库")
        meta = json.loads(row[0])
        engine = PaperEngine(MakerConfig(**meta["config"]))
        # Stats must not reconcile/alter running orders.
        from .maker_engine import PaperOrder
        engine.orders = [PaperOrder(**json.loads(r[0])) for r in conn.execute("SELECT data FROM maker_orders ORDER BY rowid")]
        summary = engine.report()
        summary["event_counts"] = dict(conn.execute("SELECT kind,count(*) FROM maker_events GROUP BY kind"))
        latencies = [json.loads(r[0])["elapsed_ms"] for r in conn.execute("SELECT data FROM maker_events WHERE kind='reaction'")]
        values = sorted(latencies)
        summary["local_reaction_ms"] = {key: values[min(len(values)-1, int((len(values)-1)*p))] if values else None
                                          for key, p in (("p50", .5), ("p95", .95), ("p99", .99))}
        summary["local_reaction_over_100ms"] = sum(v > 100 for v in values)
        if out:
            out.parent.mkdir(parents=True, exist_ok=True)
            opener = gzip.open if out.suffix == ".gz" else open
            # Stream events, no need to load a many-hour WS journal into memory.
            with opener(out, "xt", encoding="utf-8") as f:
                f.write('{"meta":' + encode(meta) + ',"summary":' + encode(summary) + ',"orders":')
                f.write(encode([asdict(o) for o in engine.orders]))
                f.write(',"events":[')
                first = True
                for ts, kind, data in conn.execute("SELECT ts,kind,data FROM maker_events ORDER BY id"):
                    if not first:
                        f.write(",")
                    first = False
                    f.write(encode({"received_ts": ts, "kind": kind, "data": json.loads(data)}))
                f.write("]}")
        return summary
