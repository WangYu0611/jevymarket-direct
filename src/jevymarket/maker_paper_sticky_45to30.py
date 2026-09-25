"""Sticky 45→30s maker paper runner for fill-rate and win-rate research."""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .maker import MakerRuntime
from .maker_config import MakerConfig
from .maker_diagnostics import diagnostic_report
from .maker_engine import choose_quote
from .maker_paper_gate import evaluate_statistics
from .maker_store import MakerStore, readonly, single_process, statistics
from .run_paths import run_output_path

REVISION = "v6-paper-sticky-45to30-r1"
ENTRY_SECONDS = 45.0
CANCEL_SECONDS = 30.0
QUOTE_TTL_SECONDS = 5.0


def trial_config() -> MakerConfig:
    return MakerConfig(
        entry_seconds=ENTRY_SECONDS,
        cancel_before_end_seconds=CANCEL_SECONDS,
        quote_ttl_seconds=QUOTE_TTL_SECONDS,
    )


class StickyMakerRuntime(MakerRuntime):
    """Preserve queue priority while an already-resting order remains safe."""

    def react(self, wall=None, mono=None):
        wall = time.time() if wall is None else wall
        mono = time.monotonic() if mono is None else mono
        active = self.engine.active()
        desired = None
        reason = "market_unavailable"
        est = None

        if self.market and self.cache:
            try:
                if not self.clock_ok or wall - self.clock_ts > 60:
                    raise ValueError("clock_unverified")
                if wall - self.metadata_ts > 45:
                    raise ValueError("market_unavailable")
                if not self.c.cancel_before_end_seconds < self.market.end - wall <= self.c.entry_seconds:
                    raise ValueError("outside_entry_window")
                est = self.reference.estimate(self.market.start, self.market.window, wall)
                budget = self.c.max_order_usd if active else self.engine.budget(self.market.slug, wall)
                desired, reason = choose_quote(self.market, self.cache, est, self.c, wall, mono, budget)
            except ValueError as exc:
                reason = str(exc)

        safe = desired is not None and not self.engine.halted and not self.observe_only
        if active and safe:
            book = self.cache.books.get(active.token) if self.cache else None
            safe = bool(
                active.slug == self.market.slug
                and active.token == desired.token
                and active.generation == self.cache.generation
                and book is not None
                and book.fresh(wall, mono, self.c.max_book_age_seconds)
                and active.price < book.ask
                and active.price <= desired.fair_p - self.c.min_edge + 1e-9
            )

        if active and not safe:
            self.engine.cancel("invalidated_quote", wall, mono)

        self.engine.advance(self.cache, wall, mono, safe)

        if self.engine.active() is None and desired and safe:
            desired, reason = choose_quote(
                self.market,
                self.cache,
                est,
                self.c,
                wall,
                mono,
                self.engine.budget(self.market.slug, wall),
            )
            if desired:
                self.engine.submit(self.market, self.cache, desired, wall, mono)

        if self.market and any(o.slug == self.market.slug and o.filled > 0 for o in self.engine.orders):
            reason = "already_filled"
        self.estimate, self.reason = est, reason


def compact_report(db: Path, out: Path) -> dict:
    diagnostic = diagnostic_report(db)
    stats = statistics(db)
    with readonly(db) as conn:
        orders = [json.loads(row[0]) for row in conn.execute(
            "SELECT data FROM maker_orders ORDER BY rowid"
        )]
        executions = [json.loads(row[0]) for row in conn.execute(
            "SELECT data FROM maker_events WHERE kind='execution' ORDER BY id"
        )]

    report = {
        "format": REVISION,
        "paper_only": True,
        "live_trading_enabled": False,
        "window": {
            "entry_seconds": ENTRY_SECONDS,
            "cancel_before_end_seconds": CANCEL_SECONDS,
            "quote_ttl_seconds": QUOTE_TTL_SECONDS,
            "sticky_queue_priority": True,
        },
        "statistics": stats,
        "performance_gate": evaluate_statistics(stats),
        "orders": orders,
        "executions": executions,
        "diagnostic_summary": diagnostic["summary"],
        "late_observation_examples": diagnostic["late_observation_examples"],
        "limitations": [
            "Public prints plus L2 queue-ahead are paper fill estimates, not authenticated own fills.",
            "Same-price cancellations ahead of our hypothetical order are not observable, so the queue model may undercount fills.",
            "Sticky behavior preserves a safe old price but still cancels on stale/invalid books, direction changes, edge loss or T-30.",
            "A >65% paper win rate is only a project gate, not a profitability guarantee.",
        ],
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="BTC5m Maker Sticky 45→30秒纸面研究；仅模拟，不连接真实账户"
    )
    parser.add_argument("--seconds", type=int, default=14400)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if not 1800 <= args.seconds <= 86400:
        parser.error("seconds必须在1800到86400之间")

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    db = run_output_path(args.db, f"jevymarket.paper-sticky-45to30_{stamp}.db")
    out = run_output_path(args.out, f"v6_paper_sticky_45to30_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("Sticky纸面实验输出已存在；拒绝覆盖或混入旧实验")

    config = trial_config()
    print(
        f"{REVISION} | 纸面post-only | {ENTRY_SECONDS:g}→{CANCEL_SECONDS:g}秒"
        f" | TTL={QUOTE_TTL_SECONDS:g}s | 安全旧单不追价 | 无真实下单",
        flush=True,
    )
    with single_process(db):
        store = MakerStore(db, config)
        runtime = StickyMakerRuntime(config, store)
        asyncio.run(runtime.run(seconds=args.seconds, observe_only=False))

    report = compact_report(db, out)
    summary = {
        "quotes": report["statistics"]["quotes"],
        "filled_orders_estimated": report["statistics"]["filled_orders_estimated"],
        "settled_filled_markets": report["statistics"]["settled_filled_markets"],
        "wins_estimated": report["statistics"]["wins_estimated"],
        "losses_estimated": report["statistics"]["losses_estimated"],
        "gross_pnl_estimated": report["statistics"]["gross_pnl_estimated"],
        "performance_gate": report["performance_gate"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}\n数据库 → {db.resolve()}", flush=True)


if __name__ == "__main__":
    main()
