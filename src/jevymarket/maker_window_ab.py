"""Same-feed maker timing A/B. Observe-only; never creates paper or live orders."""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import time
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .maker import MakerRuntime
from .maker_config import MakerConfig
from .maker_diagnostics import diagnostic_report
from .maker_engine import choose_quote
from .maker_protocol import data_health
from .maker_store import MakerStore, readonly, single_process

REVISION = "v6-window-ab-r1"
SAMPLE_SECONDS = 1.0
ARM_SPECS = {
    "A_late_30_to_10": {"entry_seconds": 30.0, "cancel_before_end_seconds": 10.0},
    "B_early_60_to_20": {"entry_seconds": 60.0, "cancel_before_end_seconds": 20.0},
}


def make_arms(base: MakerConfig) -> dict[str, MakerConfig]:
    return {name: replace(base, **spec) for name, spec in ARM_SPECS.items()}


def book_gate(health: dict) -> str:
    statuses = [row.get("status") for row in health.get("book_details", {}).values()]
    if not statuses:
        return "unavailable"
    if any(s in {"invalidated", "awaiting_snapshot"} for s in statuses):
        return "unavailable"
    if any(s in {"stale_source", "stale_receive", "future_source", "receive_clock_invalid"} for s in statuses):
        return "stale"
    if any(s in {"valid_bid_only", "valid_ask_only", "valid_empty"} for s in statuses):
        return "one_sided_or_empty"
    if all(s == "ready" for s in statuses):
        return "two_sided_ready"
    return "other"


def evaluate(runtime: MakerRuntime, config: MakerConfig, wall: float, mono: float, estimate):
    try:
        quote, reason = choose_quote(runtime.market, runtime.cache, estimate, config, wall, mono,
                                     config.max_order_usd)
    except ValueError as exc:
        quote, reason = None, str(exc)
    return {"reason": reason, "candidate": asdict(quote) if quote else None}


class WindowABRuntime(MakerRuntime):
    def __init__(self, config: MakerConfig, store: MakerStore):
        super().__init__(config, store)
        self.arms = make_arms(config)
        self.last_ab_sample_mono = None

    def react(self, wall=None, mono=None):
        wall = time.time() if wall is None else wall
        mono = time.monotonic() if mono is None else mono
        self.engine.advance(self.cache, wall, mono, False)
        self.estimate = None
        self.reason = "window_ab_observe_only"
        if self.market is None or self.cache is None:
            return
        if self.last_ab_sample_mono is not None and mono - self.last_ab_sample_mono < SAMPLE_SECONDS:
            return
        left = self.market.end - wall
        active = {name: c.cancel_before_end_seconds < left <= c.entry_seconds
                  for name, c in self.arms.items()}
        if not any(active.values()):
            return
        self.last_ab_sample_mono = mono

        common_reason = None
        estimate = None
        try:
            if not self.clock_ok or wall - self.clock_ts > 60:
                raise ValueError("clock_unverified")
            if wall - self.metadata_ts > 45:
                raise ValueError("market_unavailable")
            estimate = self.reference.estimate(self.market.start, self.market.window, wall)
        except ValueError as exc:
            common_reason = str(exc)

        health = data_health(self, wall, mono)
        rows = {}
        for name, config in self.arms.items():
            if not active[name]:
                rows[name] = {"active": False}
                continue
            if common_reason is not None:
                result = {"reason": common_reason, "candidate": None}
            else:
                result = evaluate(self, config, wall, mono, estimate)
            rows[name] = {"active": True, **result}

        self.store.emit("window_ab", {
            "revision": REVISION,
            "session_id": self.session_id,
            "ts": wall,
            "slug": self.market.slug,
            "seconds_left": left,
            "book_gate": book_gate(health),
            "book_statuses": {label: row.get("status")
                              for label, row in health.get("book_details", {}).items()},
            "estimate_p_up": estimate.p_up if estimate is not None else None,
            "arms": rows,
        })


def _complete_window(seconds: list[float], spec: dict) -> bool:
    if not seconds:
        return False
    return max(seconds) >= spec["entry_seconds"] - 2.0 and min(seconds) <= spec["cancel_before_end_seconds"] + 2.0


def aggregate(samples: list[dict]) -> dict:
    summary = {}
    per_market = {name: defaultdict(list) for name in ARM_SPECS}
    candidates = {name: set() for name in ARM_SPECS}
    for row in samples:
        for name, spec in ARM_SPECS.items():
            arm = row["arms"].get(name, {})
            if not arm.get("active"):
                continue
            per_market[name][row["slug"]].append(row["seconds_left"])
            if arm.get("candidate") is not None:
                candidates[name].add(row["slug"])

    complete = {}
    for name, spec in ARM_SPECS.items():
        active_rows = [r for r in samples if r["arms"].get(name, {}).get("active")]
        reasons = Counter(r["arms"][name].get("reason", "unknown") for r in active_rows)
        gates = Counter(r.get("book_gate", "unknown") for r in active_rows)
        candidate_rows = sum(r["arms"][name].get("candidate") is not None for r in active_rows)
        complete[name] = {slug for slug, values in per_market[name].items()
                          if _complete_window(values, spec)}
        summary[name] = {
            **spec,
            "active_samples": len(active_rows),
            "markets_sampled": len(per_market[name]),
            "complete_markets": len(complete[name]),
            "candidate_samples": candidate_rows,
            "candidate_sample_rate": candidate_rows / len(active_rows) if active_rows else None,
            "candidate_markets": len(candidates[name]),
            "reasons": dict(reasons),
            "book_gates": dict(gates),
        }

    paired = complete["A_late_30_to_10"] & complete["B_early_60_to_20"]
    matrix = Counter()
    for slug in paired:
        a = slug in candidates["A_late_30_to_10"]
        b = slug in candidates["B_early_60_to_20"]
        matrix["both" if a and b else "A_only" if a else "B_only" if b else "neither"] += 1
    bands = {
        "B_60_to_40": lambda left: 40 < left <= 60,
        "B_40_to_20": lambda left: 20 < left <= 40,
        "A_30_to_10": lambda left: 10 < left <= 30,
    }
    band_summary = {}
    for label, predicate in bands.items():
        arm_name = "A_late_30_to_10" if label.startswith("A_") else "B_early_60_to_20"
        rows = [r for r in samples if predicate(r["seconds_left"]) and r["arms"].get(arm_name, {}).get("active")]
        candidates_in_band = sum(r["arms"][arm_name].get("candidate") is not None for r in rows)
        band_summary[label] = {
            "samples": len(rows),
            "candidate_samples": candidates_in_band,
            "candidate_rate": candidates_in_band / len(rows) if rows else None,
            "reasons": dict(Counter(r["arms"][arm_name].get("reason", "unknown") for r in rows)),
            "book_gates": dict(Counter(r.get("book_gate", "unknown") for r in rows)),
        }

    overlap = [r for r in samples
               if r["arms"].get("A_late_30_to_10", {}).get("active")
               and r["arms"].get("B_early_60_to_20", {}).get("active")]
    mismatch = 0
    for r in overlap:
        a, b = r["arms"]["A_late_30_to_10"], r["arms"]["B_early_60_to_20"]
        if a.get("reason") != b.get("reason") or bool(a.get("candidate")) != bool(b.get("candidate")):
            mismatch += 1

    return {
        "arms": summary,
        "bands": band_summary,
        "paired_complete_markets": len(paired),
        "paired_candidate_matrix": dict(matrix),
        "overlap_samples": len(overlap),
        "overlap_decision_mismatches": mismatch,
        "scope": "Same-feed shadow eligibility only; no paper orders, fills, rebates, or profitability claim.",
    }


def report(path: Path, out: Path) -> dict:
    with readonly(path) as conn:
        samples = [json.loads(row[0]) for row in conn.execute(
            "SELECT data FROM maker_events WHERE kind='window_ab' ORDER BY id"
        )]
        order_count = conn.execute("SELECT count(*) FROM maker_orders").fetchone()[0]
    diagnostic = diagnostic_report(path)
    result = {
        "format": REVISION,
        "observe_only": True,
        "trading_enabled": False,
        "paper_orders_created": order_count,
        "arm_specs": ARM_SPECS,
        "summary": aggregate(samples),
        "diagnostic_summary": diagnostic["summary"],
        "candidate_examples": [r for r in samples
                               if any(a.get("candidate") is not None for a in r["arms"].values())][:24],
        "limitations": [
            "Eligibility is sampled from one shared public-data stream; it is not a fill simulation.",
            "A and B differ in timing windows only; all model, confidence, price, freshness and size rules are identical.",
            "B has a longer window than A, so compare candidate rate and complete-market incidence, not raw sample count alone.",
            "One run cannot establish profitability or live execution quality.",
        ],
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description="BTC5m Maker窗口A/B：同一行情流影子资格对照，不生成订单")
    parser.add_argument("--seconds", type=int, default=1800, choices=range(300, 3601))
    parser.add_argument("--db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    db = args.db or Path(f"jevymarket.window-ab_{stamp}.db")
    out = args.out or Path(f"v6_window_ab_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("A/B输出已存在；拒绝覆盖或混入旧实验")

    base = MakerConfig(entry_seconds=60.0, cancel_before_end_seconds=10.0)
    print(f"{REVISION} | A=30→10秒 | B=60→20秒 | 同行情流只观察 | 不生成模拟或真实订单", flush=True)
    with single_process(db):
        store = MakerStore(db, base)
        runtime = WindowABRuntime(base, store)
        asyncio.run(runtime.run(seconds=args.seconds, observe_only=True))
    result = report(db, out)
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}\n数据库 → {db.resolve()}", flush=True)


if __name__ == "__main__":
    main()
