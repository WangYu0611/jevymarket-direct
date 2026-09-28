"""V8.3 tiered real-order entry point.

This command keeps the authenticated/fail-closed V8.2 execution path while
installing the V8.3 candidate filter and T-110 fallback runtime.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import price_value_live_t120 as v82live
from .run_paths import run_output_path
from .v83_candidate_filter import (
    T110_MIN_EDGE,
    T110_MIN_JEV,
    T110_MIN_QUANT,
    T120_EXTREME_MIN_JEV,
    T120_EXTREME_MIN_QUANT,
    T120_HIGH_ASK_MIN,
    T120_HIGH_MIN_EDGE,
    T120_LOW_ASK_MAX,
    T120_LOW_MIN_EDGE,
    T120_STRONG_JEV,
)
from .v83_live_runtime import (
    MAX_ORDERS,
    MAX_SESSION_NOTIONAL_USD,
    PAPER_REVISION,
    REVISION,
    V83LiveStore,
    V83Runner,
    build_live_report,
    final_value_decision,
)

CONFIRM_ONE = "ONE_REAL_V83_TIERED"
CONFIRM_FIVE = "FIVE_REAL_V83_TIERED"
DEFAULT_SECONDS = 28_800


def strategy_manifest() -> dict:
    return {
        "revision": REVISION,
        "live_slots": [120, 110],
        "t120_strong_jev_min": T120_STRONG_JEV,
        "t120_extreme": {
            "jev_min": T120_EXTREME_MIN_JEV,
            "quant_min": T120_EXTREME_MIN_QUANT,
            "low_ask_max": T120_LOW_ASK_MAX,
            "low_edge_min": T120_LOW_MIN_EDGE,
            "high_ask_min": T120_HIGH_ASK_MIN,
            "high_edge_min": T120_HIGH_MIN_EDGE,
            "sizing": "minimum exchange shares",
        },
        "t110_fallback": {
            "jev_min": T110_MIN_JEV,
            "quant_min": T110_MIN_QUANT,
            "edge_min": T110_MIN_EDGE,
            "sizing": "minimum exchange shares",
        },
        "max_orders": MAX_ORDERS,
        "max_session_planned_notional_usd": MAX_SESSION_NOTIONAL_USD,
    }


@contextmanager
def patched_v83_runtime(*, allow_five_from_canary_gate: bool):
    names = (
        "LiveStore",
        "LiveT120Runner",
        "value_decision",
        "PAPER_REVISION",
        "REVISION",
        "build_live_report",
        "evaluate_live_gate",
        "experiment_parameters",
        "MAX_SESSION_ORDERS",
        "MAX_SESSION_NOTIONAL_USD",
    )
    saved = {name: getattr(v82live, name) for name in names}
    original_gate = saved["evaluate_live_gate"]
    original_parameters = saved["experiment_parameters"]

    def evaluate_gate(path: Path) -> dict:
        gate = original_gate(path)
        gate["v83_strategy_manifest"] = strategy_manifest()
        if allow_five_from_canary_gate:
            gate["v83_original_session_ready"] = gate.get("session_ready")
            gate["session_ready"] = bool(gate.get("canary_ready"))
            gate["v83_five_order_gate"] = (
                "Explicit five-order mode uses the existing canary gate plus "
                "stricter V8.3 candidate filters."
            )
        return gate

    def experiment_parameters(settings, interval_seconds, fee_rate):
        params = original_parameters(settings, interval_seconds, fee_rate)
        params["v83_tiered_filter"] = strategy_manifest()
        return params

    v82live.LiveStore = V83LiveStore
    v82live.LiveT120Runner = V83Runner
    v82live.value_decision = final_value_decision
    v82live.PAPER_REVISION = PAPER_REVISION
    v82live.REVISION = REVISION
    v82live.build_live_report = build_live_report
    v82live.evaluate_live_gate = evaluate_gate
    v82live.experiment_parameters = experiment_parameters
    v82live.MAX_SESSION_ORDERS = MAX_ORDERS
    v82live.MAX_SESSION_NOTIONAL_USD = MAX_SESSION_NOTIONAL_USD
    try:
        yield
    finally:
        for name, value in saved.items():
            setattr(v82live, name, value)


def resolve_paper_report(path: Path | None) -> Path:
    if path is not None:
        candidates = (path, Path("runs") / path.name)
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        raise FileNotFoundError(f"paper_report_not_found:{path}")

    matches: list[Path] = []
    for pattern in (
        "v82_t120_main_shadow*.json.gz",
        "v81_early_value_forward*.json.gz",
    ):
        matches.extend(Path("runs").glob(pattern))
    matches = [path for path in matches if path.is_file()]
    if not matches:
        raise FileNotFoundError("no_supported_paper_report_in_runs")
    return max(matches, key=lambda item: item.stat().st_mtime)


def resolve_paths(
    db_arg: Path | None,
    resume_db: Path | None,
    out_arg: Path | None,
    stamp: str,
) -> tuple[Path, Path, bool]:
    if db_arg is not None and resume_db is not None:
        raise ValueError("cannot_use_db_and_resume_db_together")
    if resume_db is not None:
        db = Path(resume_db)
        if not db.is_file():
            raise ValueError("resume_database_not_found")
        out = run_output_path(out_arg, f"v83_tiered_live_resume_{stamp}.json.gz")
        if out.exists():
            raise FileExistsError("report_already_exists")
        return db, out, True

    db = run_output_path(db_arg, f"jevymarket.v83-tiered-live_{stamp}.db")
    out = run_output_path(out_arg, f"v83_tiered_live_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("live_output_already_exists")
    return db, out, False


async def async_main(args) -> dict:
    with patched_v83_runtime(allow_five_from_canary_gate=args.mode == "session"):
        return await v82live.async_main(args)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "V8.3 分层策略：T-120强Jev/极端市场 + T-110严格回补；"
            "复用官方SDK的FAK真钱执行链"
        )
    )
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--check-only", action="store_true")
    modes.add_argument("--live-one", action="store_true")
    modes.add_argument("--live-five", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--paper-report", type=Path)
    parser.add_argument("--seconds", type=int, default=DEFAULT_SECONDS)
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument("--db", type=Path)
    paths.add_argument("--resume-db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    if not 300 <= args.seconds <= 86_400:
        parser.error("seconds必须在300到86400之间")

    try:
        args.paper_report = resolve_paper_report(args.paper_report)
    except FileNotFoundError as exc:
        parser.error(str(exc))

    if args.live_one:
        if args.confirm != CONFIRM_ONE:
            parser.error(f"--live-one必须显式添加 --confirm {CONFIRM_ONE}")
        args.mode = "one"
    elif args.live_five:
        if args.confirm != CONFIRM_FIVE:
            parser.error(f"--live-five必须显式添加 --confirm {CONFIRM_FIVE}")
        args.mode = "session"
    else:
        args.mode = "check"

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    try:
        args.db, out, resumed = resolve_paths(
            args.db, args.resume_db, args.out, stamp
        )
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))

    print(f"V8.3 gate report → {args.paper_report.resolve()}", flush=True)
    if args.check_only:
        result = asyncio.run(async_main(args))
    else:
        with v82live.single_instance(str(args.db)):
            result = asyncio.run(async_main(args))

    result["resumed_existing_database"] = resumed
    result["v83_strategy_manifest"] = strategy_manifest()
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(
            result,
            handle,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
    print(json.dumps({
        key: result.get(key)
        for key in (
            "format", "termination", "geoblock", "preflight", "paper_gate"
        )
    }, ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}", flush=True)


if __name__ == "__main__":
    main()
