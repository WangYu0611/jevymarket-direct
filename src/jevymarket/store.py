"""SQLite log of every Jev decision and every order, for later calibration analysis."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT,
    question TEXT,
    state_json TEXT,
    p_yes REAL,
    answerable REAL,
    clarity INTEGER,
    yes_ask REAL,
    no_ask REAL,
    midpoint REAL,
    edge REAL,
    action TEXT,
    reason TEXT,
    jev_model TEXT,
    jev_cost REAL,
    research_cost REAL,
    raw_json TEXT
);
CREATE TABLE IF NOT EXISTS research (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    slug TEXT NOT NULL,
    model TEXT,
    brief_json TEXT NOT NULL,
    cost REAL
);
CREATE INDEX IF NOT EXISTS idx_research_slug ON research(slug, ts);
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT,
    token_id TEXT,
    outcome TEXT,
    side TEXT,
    price REAL,
    size REAL,
    usd REAL,
    order_id TEXT,
    status TEXT,
    dry_run INTEGER,
    response_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_decisions_slug ON decisions(slug);
CREATE INDEX IF NOT EXISTS idx_orders_condition ON orders(condition_id);
CREATE TABLE IF NOT EXISTS price_anchors (
    slug TEXT PRIMARY KEY,
    timeframe TEXT NOT NULL,
    window_start REAL NOT NULL,
    twap_window INTEGER NOT NULL,
    price REAL NOT NULL,
    observed_ts REAL NOT NULL,
    source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_price_anchors_start ON price_anchors(window_start);
"""


class Store:
    def __init__(self, path: str | Path = "jevymarket.db"):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(decisions)")}
        if "research_cost" not in cols:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN research_cost REAL")
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def log_decision(self, **row: Any) -> int:
        for k in ("state_json", "raw_json"):
            if k in row and not isinstance(row[k], (str, type(None))):
                row[k] = json.dumps(row[k], default=str)
        row.setdefault("ts", time.time())
        cols = ", ".join(row)
        qs = ", ".join("?" for _ in row)
        cur = self.conn.execute(f"INSERT INTO decisions ({cols}) VALUES ({qs})", tuple(row.values()))
        self.conn.commit()
        return int(cur.lastrowid)

    def log_order(self, **row: Any) -> int:
        if "response_json" in row and not isinstance(row["response_json"], (str, type(None))):
            row["response_json"] = json.dumps(row["response_json"], default=str)
        row.setdefault("ts", time.time())
        cols = ", ".join(row)
        qs = ", ".join("?" for _ in row)
        cur = self.conn.execute(f"INSERT INTO orders ({cols}) VALUES ({qs})", tuple(row.values()))
        self.conn.commit()
        return int(cur.lastrowid)

    # --- authoritative short-term reference anchors ---------------------------

    def put_price_anchor(
        self,
        *,
        slug: str,
        timeframe: str,
        window_start: float,
        twap_window: int,
        price: float,
        observed_ts: float,
        source: str,
    ) -> bool:
        """Persist the first trusted Chainlink anchor for a recurring market."""
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO price_anchors
            (slug, timeframe, window_start, twap_window, price, observed_ts, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (slug, timeframe, window_start, twap_window, price, observed_ts, source),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_price_anchor(self, slug: str) -> dict | None:
        row = self.conn.execute(
            """
            SELECT slug, timeframe, window_start, twap_window, price, observed_ts, source
            FROM price_anchors WHERE slug = ?
            """,
            (slug,),
        ).fetchone()
        return dict(row) if row is not None else None

    # --- research cache ------------------------------------------------------

    def get_brief(self, slug: str, max_age_s: float) -> dict | None:
        row = self.conn.execute(
            "SELECT brief_json, ts FROM research WHERE slug = ? AND ts >= ? ORDER BY ts DESC LIMIT 1",
            (slug, time.time() - max_age_s),
        ).fetchone()
        if row is None:
            return None
        d = json.loads(row["brief_json"])
        d["_cached_ts"] = row["ts"]
        return d

    def put_brief(self, slug: str, model: str, brief: dict, cost: float) -> int:
        cur = self.conn.execute(
            "INSERT INTO research (ts, slug, model, brief_json, cost) VALUES (?, ?, ?, ?, ?)",
            (time.time(), slug, model, json.dumps(brief, default=str), cost),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def has_order_for(self, condition_id: str, include_dry_run: bool = False) -> bool:
        q = "SELECT 1 FROM orders WHERE condition_id = ? AND status NOT IN ('failed','rejected')"
        if not include_dry_run:
            q += " AND dry_run = 0"
        return self.conn.execute(q + " LIMIT 1", (condition_id,)).fetchone() is not None

    def recent_decisions(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM decisions ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()

    def stats(self) -> dict[str, Any]:
        c = self.conn
        n_dec = c.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        n_trade = c.execute("SELECT COUNT(*) FROM decisions WHERE action = 'trade'").fetchone()[0]
        cost = c.execute("SELECT COALESCE(SUM(jev_cost),0) FROM decisions").fetchone()[0]
        n_orders = c.execute("SELECT COUNT(*) FROM orders WHERE dry_run = 0").fetchone()[0]
        n_briefs = c.execute("SELECT COUNT(*) FROM research").fetchone()[0]
        rcost = c.execute("SELECT COALESCE(SUM(cost),0) FROM research").fetchone()[0]
        usd = c.execute("SELECT COALESCE(SUM(usd),0) FROM orders WHERE dry_run = 0 AND status NOT IN ('failed','rejected')").fetchone()[0]
        buckets = c.execute(
            """
            SELECT CAST(p_yes*10 AS INT) AS b, COUNT(*) AS n, AVG(p_yes) AS avg_p, AVG(midpoint) AS avg_mkt
            FROM decisions WHERE p_yes IS NOT NULL AND midpoint IS NOT NULL
            GROUP BY b ORDER BY b
            """
        ).fetchall()
        return {
            "decisions": n_dec,
            "trade_signals": n_trade,
            "jev_cost_usd": cost,
            "briefs": n_briefs,
            "research_cost_usd": rcost,
            "live_orders": n_orders,
            "live_usd": usd,
            "buckets": [dict(r) for r in buckets],
        }
