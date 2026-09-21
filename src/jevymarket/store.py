"""SQLite log of every Jev decision and every order, for later calibration analysis."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
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
    jev_p_yes REAL,
    timeframe TEXT,
    strategy_version TEXT,
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
    slug TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    window_start REAL NOT NULL,
    twap_window INTEGER NOT NULL,
    price REAL NOT NULL,
    observed_ts REAL NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (slug, twap_window)
);
CREATE INDEX IF NOT EXISTS idx_price_anchors_start ON price_anchors(window_start);
CREATE TABLE IF NOT EXISTS price_samples (
    source TEXT NOT NULL,
    twap_window INTEGER NOT NULL DEFAULT 0,
    ts INTEGER NOT NULL,
    price REAL NOT NULL,
    PRIMARY KEY (source, twap_window, ts)
);
CREATE INDEX IF NOT EXISTS idx_price_samples_lookup
ON price_samples(source, twap_window, ts);
CREATE TABLE IF NOT EXISTS market_results (
    slug TEXT PRIMARY KEY,
    condition_id TEXT,
    timeframe TEXT,
    resolved_ts REAL NOT NULL,
    winner TEXT NOT NULL,
    up_won INTEGER NOT NULL,
    up_final_price REAL,
    down_final_price REAL,
    source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_results_timeframe
ON market_results(timeframe, resolved_ts);
CREATE TABLE IF NOT EXISTS evaluation_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    condition_id TEXT,
    timeframe TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    checkpoint_seconds INTEGER NOT NULL,
    ts REAL NOT NULL,
    seconds_left INTEGER NOT NULL,
    quant_p REAL,
    jev_p REAL,
    market_p REAL,
    yes_ask REAL,
    no_ask REAL,
    book_json TEXT,
    state_json TEXT,
    UNIQUE(slug, strategy_version, checkpoint_seconds)
);
CREATE INDEX IF NOT EXISTS idx_eval_samples_version
ON evaluation_samples(strategy_version, timeframe, checkpoint_seconds);
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
        if "jev_p_yes" not in cols:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN jev_p_yes REAL")
        if "timeframe" not in cols:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN timeframe TEXT")
        if "strategy_version" not in cols:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN strategy_version TEXT")

        anchor_info = self.conn.execute("PRAGMA table_info(price_anchors)").fetchall()
        pk_cols = [r["name"] for r in sorted(anchor_info, key=lambda row: row["pk"]) if r["pk"]]
        if anchor_info and pk_cols == ["slug"]:
            self.conn.executescript(
                """
                ALTER TABLE price_anchors RENAME TO price_anchors_old;
                CREATE TABLE price_anchors (
                    slug TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    window_start REAL NOT NULL,
                    twap_window INTEGER NOT NULL,
                    price REAL NOT NULL,
                    observed_ts REAL NOT NULL,
                    source TEXT NOT NULL,
                    PRIMARY KEY (slug, twap_window)
                );
                INSERT OR IGNORE INTO price_anchors
                SELECT slug, timeframe, window_start, twap_window, price, observed_ts, source
                FROM price_anchors_old;
                DROP TABLE price_anchors_old;
                CREATE INDEX IF NOT EXISTS idx_price_anchors_start
                ON price_anchors(window_start);
                """
            )
        self._backfill_decision_metadata()
        self.conn.commit()

    def _backfill_decision_metadata(self) -> None:
        rows = self.conn.execute(
            """
            SELECT id, state_json, raw_json, jev_p_yes, timeframe
            FROM decisions
            WHERE jev_p_yes IS NULL OR timeframe IS NULL
            """
        ).fetchall()
        for row in rows:
            state: dict[str, Any] = {}
            raw: dict[str, Any] = {}
            try:
                state = json.loads(row["state_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                pass
            try:
                raw = json.loads(row["raw_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                pass

            timeframe = row["timeframe"] or state.get("timeframe")
            jev_p = row["jev_p_yes"]
            if jev_p is None:
                answers = raw.get("answers") if isinstance(raw, dict) else None
                answer = answers.get("resolves_yes") if isinstance(answers, dict) else None
                if isinstance(answer, dict):
                    for key in ("noul", "probability", "p", "value"):
                        value = answer.get(key)
                        if isinstance(value, (int, float)) and not isinstance(value, bool):
                            jev_p = float(value)
                            break

            self.conn.execute(
                "UPDATE decisions SET jev_p_yes = ?, timeframe = ? WHERE id = ?",
                (jev_p, timeframe, row["id"]),
            )

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

    def get_price_anchor(self, slug: str, twap_window: int) -> dict | None:
        row = self.conn.execute(
            """
            SELECT slug, timeframe, window_start, twap_window, price, observed_ts, source
            FROM price_anchors WHERE slug = ? AND twap_window = ?
            """,
            (slug, twap_window),
        ).fetchone()
        return dict(row) if row is not None else None

    # --- realtime price history ------------------------------------------------

    def put_price_sample(
        self,
        *,
        source: str,
        price: float,
        observed_ts: float,
        twap_window: int = 0,
    ) -> None:
        """Store one sample per second; later updates in the same second replace earlier ones."""
        self.conn.execute(
            """
            INSERT INTO price_samples (source, twap_window, ts, price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source, twap_window, ts)
            DO UPDATE SET price = excluded.price
            """,
            (source, int(twap_window), int(observed_ts), float(price)),
        )
        self.conn.commit()

    def get_price_samples(
        self,
        *,
        source: str,
        since_ts: float,
        twap_window: int = 0,
        until_ts: float | None = None,
    ) -> list[dict]:
        q = (
            "SELECT ts, price FROM price_samples "
            "WHERE source = ? AND twap_window = ? AND ts >= ?"
        )
        params: list[Any] = [source, int(twap_window), int(since_ts)]
        if until_ts is not None:
            q += " AND ts <= ?"
            params.append(int(until_ts))
        q += " ORDER BY ts"
        return [dict(row) for row in self.conn.execute(q, tuple(params)).fetchall()]

    def prune_price_samples(self, before_ts: float) -> int:
        cur = self.conn.execute(
            "DELETE FROM price_samples WHERE ts < ?",
            (int(before_ts),),
        )
        self.conn.commit()
        return int(cur.rowcount)

    # --- settlement / evaluation -----------------------------------------------

    def put_market_result(
        self,
        *,
        slug: str,
        condition_id: str | None,
        timeframe: str | None,
        winner: str,
        up_won: bool,
        up_final_price: float | None,
        down_final_price: float | None,
        source: str,
        resolved_ts: float | None = None,
    ) -> bool:
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO market_results
            (slug, condition_id, timeframe, resolved_ts, winner, up_won,
             up_final_price, down_final_price, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slug,
                condition_id,
                timeframe,
                resolved_ts or time.time(),
                winner,
                int(up_won),
                up_final_price,
                down_final_price,
                source,
            ),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def get_market_result(self, slug: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM market_results WHERE slug = ?",
            (slug,),
        ).fetchone()
        return dict(row) if row is not None else None

    def pending_short_market_slugs(
        self,
        *,
        now_ts: float | None = None,
        grace_seconds: float = 30.0,
        limit: int = 200,
    ) -> list[str]:
        cutoff = (now_ts or time.time()) - grace_seconds
        rows = self.conn.execute(
            """
            SELECT slug, MAX(ts) AS last_ts, state_json
            FROM decisions
            WHERE slug NOT IN (SELECT slug FROM market_results)
            GROUP BY slug
            ORDER BY last_ts DESC
            LIMIT ?
            """,
            (limit * 3,),
        ).fetchall()
        pending: list[str] = []
        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if state.get("timeframe") not in {"5m", "15m", "1h"}:
                continue
            end = state.get("market_end_time")
            if not isinstance(end, str):
                continue
            try:
                end_ts = datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp()
            except ValueError:
                continue
            if end_ts <= cutoff:
                pending.append(str(row["slug"]))
                if len(pending) >= limit:
                    break
        return pending

    # --- fixed checkpoint evaluation samples ----------------------------------

    def log_evaluation_sample(
        self,
        *,
        slug: str,
        condition_id: str | None,
        timeframe: str,
        strategy_version: str,
        checkpoint_seconds: int,
        seconds_left: int,
        quant_p: float | None,
        jev_p: float | None,
        market_p: float | None,
        yes_ask: float | None,
        no_ask: float | None,
        book_json: dict | None,
        state_json: dict | None,
        ts: float | None = None,
    ) -> bool:
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO evaluation_samples
            (slug, condition_id, timeframe, strategy_version, checkpoint_seconds,
             ts, seconds_left, quant_p, jev_p, market_p, yes_ask, no_ask,
             book_json, state_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slug,
                condition_id,
                timeframe,
                strategy_version,
                int(checkpoint_seconds),
                ts or time.time(),
                int(seconds_left),
                quant_p,
                jev_p,
                market_p,
                yes_ask,
                no_ask,
                json.dumps(book_json, default=str) if book_json is not None else None,
                json.dumps(state_json, default=str) if state_json is not None else None,
            ),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def evaluation_sample_count(self, strategy_version: str | None = None) -> int:
        if strategy_version is None:
            return int(
                self.conn.execute("SELECT COUNT(*) FROM evaluation_samples").fetchone()[0]
            )
        return int(
            self.conn.execute(
                "SELECT COUNT(*) FROM evaluation_samples WHERE strategy_version = ?",
                (strategy_version,),
            ).fetchone()[0]
        )

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

    def stats(
        self,
        *,
        min_edge: float = 0.08,
        min_answerable: float = 0.70,
        min_clarity: int = 2,
        min_trade_price: float = 0.10,
        max_trade_price: float = 0.90,
    ) -> dict[str, Any]:
        c = self.conn
        n_dec = c.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        n_trade = c.execute(
            "SELECT COUNT(*) FROM decisions WHERE action LIKE 'trade%'"
        ).fetchone()[0]
        n_orders = c.execute(
            "SELECT COUNT(*) FROM orders WHERE dry_run = 0"
        ).fetchone()[0]
        n_dry_orders = c.execute(
            "SELECT COUNT(*) FROM orders WHERE dry_run = 1 AND status = 'dry_run'"
        ).fetchone()[0]
        n_results = c.execute("SELECT COUNT(*) FROM market_results").fetchone()[0]
        n_briefs = c.execute("SELECT COUNT(*) FROM research").fetchone()[0]
        versions = [
            dict(row)
            for row in c.execute(
                """
                SELECT COALESCE(strategy_version, 'legacy') AS strategy_version,
                       COUNT(*) AS n
                FROM decisions
                GROUP BY COALESCE(strategy_version, 'legacy')
                ORDER BY n DESC
                """
            ).fetchall()
        ]
        jev_cost = c.execute(
            "SELECT COALESCE(SUM(jev_cost),0) FROM decisions"
        ).fetchone()[0]
        research_cost = c.execute(
            "SELECT COALESCE(SUM(cost),0) FROM research"
        ).fetchone()[0]
        usd = c.execute(
            """
            SELECT COALESCE(SUM(usd),0) FROM orders
            WHERE dry_run = 0 AND status NOT IN ('failed','rejected')
            """
        ).fetchone()[0]

        buckets = c.execute(
            """
            SELECT CAST(p_yes*10 AS INT) AS b, COUNT(*) AS n,
                   AVG(p_yes) AS avg_p, AVG(midpoint) AS avg_mkt
            FROM decisions
            WHERE p_yes IS NOT NULL AND midpoint IS NOT NULL
            GROUP BY b ORDER BY b
            """
        ).fetchall()

        return {
            "decisions": n_dec,
            "trade_signals": n_trade,
            "briefs": n_briefs,
            "strategy_versions": versions,
            "evaluation_samples": self.evaluation_sample_count(),
            "jev_cost_usd": jev_cost,
            "research_cost_usd": research_cost,
            "live_orders": n_orders,
            "dry_orders": n_dry_orders,
            "resolved_markets": n_results,
            "live_usd": usd,
            "buckets": [dict(r) for r in buckets],
            "model_comparison": self.model_comparison(),
            "strategy_comparison": self.strategy_comparison(
                min_edge=min_edge,
                min_answerable=min_answerable,
                min_clarity=min_clarity,
                min_trade_price=min_trade_price,
                max_trade_price=max_trade_price,
            ),
            "dry_run_performance": self.dry_run_performance(),
        }

    @staticmethod
    def _brier(values: list[tuple[float, int]]) -> float | None:
        if not values:
            return None
        return sum((p - y) ** 2 for p, y in values) / len(values)

    @staticmethod
    def _log_loss(values: list[tuple[float, int]]) -> float | None:
        if not values:
            return None
        import math

        eps = 1e-6
        total = 0.0
        for p, y in values:
            p = min(1 - eps, max(eps, p))
            total -= y * math.log(p) + (1 - y) * math.log(1 - p)
        return total / len(values)

    @staticmethod
    def _direction_accuracy(values: list[tuple[float, int]]) -> float | None:
        if not values:
            return None
        return sum((p >= 0.5) == bool(y) for p, y in values) / len(values)

    def model_comparison(self) -> list[dict[str, Any]]:
        """Compare all probability models on the exact same quantitative-era snapshots."""
        rows = self.conn.execute(
            """
            SELECT d.id, d.state_json, d.jev_p_yes, d.midpoint,
                   d.timeframe, r.up_won
            FROM decisions d
            JOIN market_results r ON r.slug = d.slug
            ORDER BY d.ts
            """
        ).fetchall()

        scopes = ("全部", "5m", "15m", "1h")
        model_names = ("量化 Φ(Z)", "Jev", "Polymarket")
        by_scope: dict[str, dict[str, list[tuple[float, int]]]] = {
            scope: {name: [] for name in model_names}
            for scope in scopes
        }

        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            quant = state.get("quantitative_signal")
            p_quant = quant.get("p_up") if isinstance(quant, dict) else None
            if not isinstance(p_quant, (int, float)):
                # Same-sample comparison: old pre-quantitative snapshots are excluded.
                continue
            if not isinstance(row["jev_p_yes"], (int, float)):
                continue
            if not isinstance(row["midpoint"], (int, float)):
                continue

            y = int(row["up_won"])
            timeframe = row["timeframe"]
            target_scopes = ["全部"]
            if timeframe in {"5m", "15m", "1h"}:
                target_scopes.append(str(timeframe))

            values = {
                "量化 Φ(Z)": float(p_quant),
                "Jev": float(row["jev_p_yes"]),
                "Polymarket": float(row["midpoint"]),
            }
            for scope in target_scopes:
                for name, p in values.items():
                    by_scope[scope][name].append((p, y))

        out = []
        for scope in scopes:
            for name in model_names:
                values = by_scope[scope][name]
                out.append({
                    "timeframe": scope,
                    "model": name,
                    "n": len(values),
                    "brier": self._brier(values),
                    "log_loss": self._log_loss(values),
                    "accuracy": self._direction_accuracy(values),
                })
        return out

    def strategy_comparison(
        self,
        *,
        min_edge: float,
        min_answerable: float,
        min_clarity: int,
        min_trade_price: float,
        max_trade_price: float,
    ) -> list[dict[str, Any]]:
        """First qualifying $1 trade per market, split by horizon and overall."""
        rows = self.conn.execute(
            """
            SELECT d.slug, d.ts, d.state_json, d.jev_p_yes, d.answerable,
                   d.clarity, d.yes_ask, d.no_ask, d.timeframe, r.up_won
            FROM decisions d
            JOIN market_results r ON r.slug = d.slug
            ORDER BY d.ts
            """
        ).fetchall()

        configs = (
            ("A 纯Φ(Z)", "quant", False),
            ("B Φ(Z)+Jev门控", "quant", True),
            ("C Jev概率", "jev", True),
        )
        scopes = ("全部", "5m", "15m", "1h")
        state = {
            (scope, name): {
                "seen": set(),
                "trades": 0,
                "wins": 0,
                "pnl": 0.0,
            }
            for scope in scopes
            for name, _, _ in configs
        }

        def choose_trade(
            p_up: float,
            yes_ask: float | None,
            no_ask: float | None,
        ) -> tuple[str, float] | None:
            choices: list[tuple[str, float, float]] = []
            if yes_ask is not None and min_trade_price <= yes_ask <= max_trade_price:
                choices.append(("UP", yes_ask, p_up - yes_ask))
            if no_ask is not None and min_trade_price <= no_ask <= max_trade_price:
                choices.append(("DOWN", no_ask, (1.0 - p_up) - no_ask))
            if not choices:
                return None
            side, ask, edge = max(choices, key=lambda item: item[2])
            return (side, ask) if edge >= min_edge else None

        for row in rows:
            try:
                decision_state = json.loads(row["state_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            quant = decision_state.get("quantitative_signal")
            quant_p = quant.get("p_up") if isinstance(quant, dict) else None
            # Keep A/B/C on exactly the same quantitative-era opportunity set.
            if not isinstance(quant_p, (int, float)):
                continue

            jev_p = row["jev_p_yes"]
            yes_ask = float(row["yes_ask"]) if row["yes_ask"] is not None else None
            no_ask = float(row["no_ask"]) if row["no_ask"] is not None else None
            up_won = bool(row["up_won"])
            timeframe = str(row["timeframe"] or "")
            target_scopes = ["全部"]
            if timeframe in {"5m", "15m", "1h"}:
                target_scopes.append(timeframe)

            for scope in target_scopes:
                for name, probability_kind, use_gate in configs:
                    bucket = state[(scope, name)]
                    if row["slug"] in bucket["seen"]:
                        continue

                    if use_gate and (
                        row["answerable"] is None
                        or float(row["answerable"]) < min_answerable
                        or row["clarity"] is None
                        or int(row["clarity"]) < min_clarity
                    ):
                        continue

                    p = quant_p if probability_kind == "quant" else jev_p
                    if not isinstance(p, (int, float)):
                        continue

                    trade = choose_trade(float(p), yes_ask, no_ask)
                    if trade is None:
                        continue

                    side, ask = trade
                    bucket["seen"].add(row["slug"])
                    bucket["trades"] += 1
                    won = up_won if side == "UP" else not up_won
                    if won:
                        bucket["wins"] += 1
                        bucket["pnl"] += (1.0 / ask) - 1.0
                    else:
                        bucket["pnl"] -= 1.0

        out: list[dict[str, Any]] = []
        for scope in scopes:
            for name, _, _ in configs:
                bucket = state[(scope, name)]
                trades = int(bucket["trades"])
                wins = int(bucket["wins"])
                pnl = float(bucket["pnl"])
                out.append({
                    "timeframe": scope,
                    "strategy": name,
                    "trades": trades,
                    "wins": wins,
                    "hit_rate": wins / trades if trades else None,
                    "pnl_usd": pnl,
                    "roi": pnl / trades if trades else None,
                })
        return out

    def dry_run_performance(self) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT o.slug, o.outcome, o.price, o.size, o.usd,
                   r.winner, r.up_won
            FROM orders o
            JOIN market_results r ON r.slug = o.slug
            WHERE o.dry_run = 1 AND o.status = 'dry_run'
            ORDER BY o.ts
            """
        ).fetchall()

        pnl = 0.0
        wins = 0
        stake = 0.0
        seen_slugs: set[str] = set()
        counted = 0
        for row in rows:
            slug = str(row["slug"])
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            counted += 1
            won = str(row["outcome"]).upper() == str(row["winner"]).upper()
            usd = float(row["usd"] or 0)
            size = float(row["size"] or 0)
            stake += usd
            if won:
                wins += 1
                pnl += size - usd
            else:
                pnl -= usd

        return {
            "trades": counted,
            "wins": wins,
            "hit_rate": (wins / counted) if counted else None,
            "stake_usd": stake,
            "pnl_usd": pnl,
            "roi": (pnl / stake) if stake > 0 else None,
        }
