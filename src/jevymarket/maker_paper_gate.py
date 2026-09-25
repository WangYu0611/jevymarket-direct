"""Paper-performance gate before any real-money canary may be discussed."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

MIN_SETTLED_FILLED_MARKETS = 50
MIN_WIN_RATE_EXCLUSIVE = 0.65


def evaluate_statistics(stats: dict) -> dict:
    settled = int(stats.get("settled_filled_markets") or 0)
    wins = int(stats.get("wins_estimated") or 0)
    losses = int(stats.get("losses_estimated") or 0)
    decided = wins + losses
    win_rate = wins / decided if decided else None
    checks = {
        "minimum_settled_markets": settled >= MIN_SETTLED_FILLED_MARKETS,
        "win_rate_over_65pct": win_rate is not None and win_rate > MIN_WIN_RATE_EXCLUSIVE,
        "positive_gross_pnl": float(stats.get("gross_pnl_estimated") or 0) > 0,
        "positive_without_top3_wins": float(stats.get("pnl_minus_top3_positive_contributions") or 0) > 0,
        "no_uncertain_orders": int(stats.get("uncertain_orders") or 0) == 0,
        "no_pending_filled_orders": int(stats.get("pending_filled_orders") or 0) == 0,
        "wins_losses_reconcile": decided == settled,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "settled_filled_markets": settled,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "minimum_settled_filled_markets": MIN_SETTLED_FILLED_MARKETS,
        "required_win_rate": f">{MIN_WIN_RATE_EXCLUSIVE:.0%}",
        "scope": "Paper-only evidence gate; passing does not guarantee live profitability.",
    }


def evaluate_report(report: dict) -> dict:
    stats = report.get("statistics")
    if not isinstance(stats, dict):
        raise ValueError("paper report missing statistics")
    if report.get("paper_only") is not True:
        raise ValueError("performance gate requires a paper-only report")
    return evaluate_statistics(stats)


def read_report(path: Path) -> dict:
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        report = json.load(handle)
    if not isinstance(report, dict):
        raise ValueError("paper report must be a JSON object")
    return report


def evaluate_path(path: Path) -> dict:
    return evaluate_report(read_report(path))
