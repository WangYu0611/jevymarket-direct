"""Evidence-led BTC5m paper execution trial. No live order transport."""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .maker import MakerRuntime
from .maker_config import MakerConfig
from .maker_diagnostics import diagnostic_report
from .maker_store import MakerStore, readonly, single_process, statistics

REVISION = "v6-paper-45to30-r1"
ENTRY_SECONDS = 45.0
CANCEL_SECONDS = 30.0


def trial_config() -> MakerConfig:
    return MakerConfig(entry_seconds=ENTRY_SECONDS, cancel_before_end_seconds=CANCEL_SECONDS)


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
        "window": {"entry_seconds": ENTRY_SECONDS, "cancel_before_end_seconds": CANCEL_SECONDS},
        "statistics": stats,
        "orders": orders,
        "executions": executions,
        "diagnostic_summary": diagnostic["summary"],
        "late_observation_examples": diagnostic["late_observation_examples"],
        "limitations": [
            "Public prints and queue-ahead are paper fill estimates, not authenticated own fills.",
            "This window was selected from one preceding 30-minute same-feed eligibility run; it is not proven optimal.",
            "No fees/rebates or live acknowledgement latency are validated by this paper trial.",
            "A zero-fill run is evidence about the tested conditions, not proof that the strategy cannot fill.",
        ],
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="BTC5m Maker 45→30秒证据驱动纸面执行试验；只模拟，不提供实盘入口"
    )
    parser.add_argument("--seconds", type=int, default=3600, choices=range(1800, 7201))
    parser.add_argument("--db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    db = args.db or Path(f"jevymarket.paper-45to30_{stamp}.db")
    out = args.out or Path(f"v6_paper_45to30_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("纸面试验输出已存在；拒绝覆盖或混入旧实验")

    config = trial_config()
    print(
        f"{REVISION} | 纸面post-only | {ENTRY_SECONDS:g}→{CANCEL_SECONDS:g}秒窗口"
        " | 保持92%/新鲜度/价格/仓位规则 | 无真实下单",
        flush=True,
    )
    with single_process(db):
        store = MakerStore(db, config)
        runtime = MakerRuntime(config, store)
        asyncio.run(runtime.run(seconds=args.seconds, observe_only=False))

    report = compact_report(db, out)
    print(json.dumps(report["statistics"], ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}\n数据库 → {db.resolve()}", flush=True)


if __name__ == "__main__":
    main()
