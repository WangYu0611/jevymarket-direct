"""Isolated v3 observations, paired Jev results and paper orders; v2 is untouched."""

from __future__ import annotations

import json
import math
import time
from dataclasses import asdict
from pathlib import Path

from .signal import JevView, Trade
from .store import Store

SCHEMA = """
CREATE TABLE IF NOT EXISTS fast_experiments (
    version TEXT PRIMARY KEY, created_ts REAL NOT NULL, parameters_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fast_observations (
    id INTEGER PRIMARY KEY, version TEXT NOT NULL, session TEXT NOT NULL,
    slug TEXT NOT NULL, condition_id TEXT, ts REAL NOT NULL, seconds_left INTEGER,
    checkpoint INTEGER, quant_p REAL, market_p REAL, yes_ask REAL, no_ask REAL,
    status TEXT NOT NULL, reason TEXT, payload_json TEXT NOT NULL,
    jev_status TEXT NOT NULL DEFAULT 'not_requested', jev_p REAL,
    jev_answerable REAL, jev_clarity INTEGER, jev_requested_ts REAL,
    jev_received_ts REAL, jev_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_fast_obs ON fast_observations(version, slug, ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fast_cp
ON fast_observations(version, slug, checkpoint) WHERE checkpoint IS NOT NULL;
CREATE TABLE IF NOT EXISTS fast_orders (
    version TEXT NOT NULL, slug TEXT NOT NULL, observation_id INTEGER NOT NULL,
    ts REAL NOT NULL, outcome TEXT NOT NULL, price REAL NOT NULL,
    size REAL NOT NULL, usd REAL NOT NULL, trade_json TEXT NOT NULL,
    PRIMARY KEY(version, slug)
);
"""


def dump(data) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


class FastStore(Store):
    def __init__(self, path: str | Path = "jevymarket.db"):
        super().__init__(path)
        self.conn.executescript(SCHEMA)

    def ensure_experiment(self, version: str, parameters: dict) -> None:
        encoded = dump(parameters)
        self.conn.execute("INSERT OR IGNORE INTO fast_experiments VALUES (?, ?, ?)",
                          (version, time.time(), encoded))
        self.conn.commit()
        stored = self.conn.execute("SELECT parameters_json FROM fast_experiments WHERE version=?", (version,)).fetchone()
        if stored[0] != encoded:
            raise ValueError("该实验已有不同参数（包括轮询间隔/Jev模式）。请使用新的 --experiment 名称，避免混样。")

    def parameters(self, version: str) -> dict | None:
        row = self.conn.execute("SELECT parameters_json FROM fast_experiments WHERE version=?", (version,)).fetchone()
        return json.loads(row[0]) if row else None

    def record(self, *, version: str, session: str, slug: str, condition_id: str | None,
               ts: float, seconds_left: int | None, checkpoint: int | None,
               quant_p: float | None, market_p: float | None, yes_ask: float | None,
               no_ask: float | None, status: str, reason: str, payload: dict) -> tuple[int, int | None]:
        with self.conn:
            if checkpoint is not None and self.conn.execute(
                "SELECT 1 FROM fast_observations WHERE version=? AND slug=? AND checkpoint=?",
                (version, slug, checkpoint),
            ).fetchone():
                checkpoint = None
            cur = self.conn.execute(
                """INSERT INTO fast_observations
                (version,session,slug,condition_id,ts,seconds_left,checkpoint,quant_p,market_p,
                 yes_ask,no_ask,status,reason,payload_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (version, session, slug, condition_id, ts, seconds_left, checkpoint, quant_p,
                 market_p, yes_ask, no_ask, status, reason, dump(payload)),
            )
        return int(cur.lastrowid), checkpoint

    def mark_jev(self, observation_id: int, status: str, *, requested_ts: float | None = None) -> None:
        with self.conn:
            self.conn.execute("UPDATE fast_observations SET jev_status=?, jev_requested_ts=? WHERE id=?",
                              (status, requested_ts, observation_id))

    def complete_jev(self, observation_id: int, view: JevView, received_ts: float) -> None:
        if not (math.isfinite(view.p_yes) and 0 <= view.p_yes <= 1
                and math.isfinite(view.answerable) and 0 <= view.answerable <= 1):
            raise ValueError("Jev 返回非法概率")
        with self.conn:
            self.conn.execute(
                """UPDATE fast_observations SET jev_status='ok', jev_p=?, jev_answerable=?,
                jev_clarity=?, jev_received_ts=?, jev_json=? WHERE id=?""",
                (view.p_yes, view.answerable, view.clarity, received_ts, dump(view.raw), observation_id),
            )

    def paper_order(self, version: str, slug: str, observation_id: int, trade: Trade,
                    *, max_exposure: float) -> str:
        # SQLite uniqueness makes the first-order policy survive restarts. This
        # transaction has no await and does NOT submit anything to Polymarket.
        with self.conn:
            if self.conn.execute("SELECT 1 FROM fast_orders WHERE version=? AND slug=?", (version, slug)).fetchone():
                return "已有第一笔模拟订单；只记录后续观察"
            exposure = self.conn.execute(
                """SELECT COALESCE(SUM(o.usd),0) FROM fast_orders o
                LEFT JOIN market_results r ON r.slug=o.slug
                WHERE o.version=? AND r.slug IS NULL""", (version,),
            ).fetchone()[0]
            if exposure + trade.usd > max_exposure:
                return f"未结算模拟投入 ${exposure:.2f} + 本单超过敞口上限"
            self.conn.execute("INSERT INTO fast_orders VALUES (?,?,?,?,?,?,?,?,?)",
                              (version, slug, observation_id, time.time(), trade.outcome,
                               trade.price, trade.size, trade.usd, dump(asdict(trade))))
        return f"模拟下单 {trade.outcome} {trade.size:.2f}份 @ {trade.price:.3f}，${trade.usd:.2f}"

    def pending(self, version: str, *, limit: int = 20) -> list[str]:
        rows = self.conn.execute(
            """SELECT DISTINCT o.slug FROM fast_observations o LEFT JOIN market_results r ON r.slug=o.slug
            WHERE o.version=? AND r.slug IS NULL ORDER BY o.slug DESC""", (version,),
        ).fetchall()
        now = time.time()
        return [r[0] for r in rows if r[0].startswith("btc-updown-5m-")
                and int(r[0].rsplit("-", 1)[1]) + 315 <= now][:limit]

    def observations(self, version: str, *, resolved_only: bool = False, checkpoints_only: bool = False) -> list[dict]:
        query = """SELECT o.*, r.up_won FROM fast_observations o
                   LEFT JOIN market_results r ON r.slug=o.slug WHERE o.version=?"""
        if resolved_only:
            query += " AND r.slug IS NOT NULL"
        if checkpoints_only:
            query += " AND o.checkpoint IS NOT NULL"
        query += " ORDER BY o.ts, o.id"
        return [dict(r) for r in self.conn.execute(query, (version,)).fetchall()]

    def orders(self, version: str) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            """SELECT o.*, r.up_won FROM fast_orders o LEFT JOIN market_results r ON r.slug=o.slug
            WHERE o.version=? ORDER BY o.ts""", (version,),
        ).fetchall()]


def probability_metrics(rows: list[dict], field: str) -> dict:
    usable = [r for r in rows if r.get(field) is not None and r.get("up_won") is not None]
    n = len(usable)
    if not n:
        return {"n": 0, "markets": 0, "brier": None, "logloss": None, "accuracy": None}
    brier = loss = hits = 0.0
    for r in usable:
        p, y = float(r[field]), int(r["up_won"])
        brier += (p - y) ** 2
        q = min(1 - 1e-9, max(1e-9, p))
        loss -= y * math.log(q) + (1 - y) * math.log(1 - q)
        hits += (p >= 0.5) == bool(y)
    return {"n": n, "markets": len({r["slug"] for r in usable}),
            "brier": brier / n, "logloss": loss / n, "accuracy": hits / n}


def unit_replay(rows: list[dict], parameters: dict, *, field: str = "quant_p", jev_gate: bool = False) -> dict:
    """Fixed-$1 diagnostic, not an executable fill simulation.

    A/B/C pairing uses a caller-selected common cohort. With Jev it is explicitly
    counterfactual at request-time quotes, NOT a latency-aware tradable backtest.
    """
    seen: set[str] = set()
    profits: list[float] = []
    for row in rows:
        if row["slug"] in seen or row.get(field) is None or row.get("up_won") is None:
            continue
        if jev_gate and (row.get("jev_answerable") is None or row.get("jev_clarity") is None
                        or row["jev_answerable"] < parameters["min_answerable"]
                        or row["jev_clarity"] < parameters["min_clarity"]):
            continue
        stored_book = json.loads(row.get("payload_json") or "{}").get("book", {})
        invalid_book = False
        for side in ("yes", "no"):
            bid, ask = stored_book.get(f"{side}_bid"), stored_book.get(f"{side}_ask")
            if bid is not None and ask is not None and (ask < bid or ask - bid > parameters["max_spread"]):
                invalid_book = True
        if invalid_book:
            continue
        p = row[field]
        options = [(True, p, row["yes_ask"]), (False, 1 - p, row["no_ask"])]
        options = [o for o in options if o[2] is not None and 0 < o[2] < 1
                   and parameters["min_trade_price"] <= o[2] <= parameters["max_trade_price"]]
        if not options:
            continue
        up, prob, price = max(options, key=lambda o: o[1] - o[2])
        if prob - price < parameters["min_edge"]:
            continue
        seen.add(row["slug"])
        profits.append(1 / price - 1 if up == bool(row["up_won"]) else -1.0)
    return {"trades": len(profits), "wins": sum(p > 0 for p in profits), "pnl": sum(profits),
            "roi": sum(profits) / len(profits) if profits else None}
