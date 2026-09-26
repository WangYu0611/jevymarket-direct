"""V8.1 early-window price-value experiment.

V8.1 is a fresh forward protocol informed by V8 diagnostics. It concentrates
execution research on T-120..T-90 while preserving V8 value/risk rules.

No authenticated Polymarket client and no real orders are used.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import time
import uuid
from collections import Counter
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

from polymarket import AsyncPublicClient
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .fast_cli import single_instance
from .fast_runner import current_slug, settle_some, settlement_worker
from .fast_strategy import fast_settings, local_snapshot, next_deadline, snapshot_payload
from .jev import JevClient
from .market_data import watch_chainlink_anchors
from .network import ReadUnavailable
from .price_value_forward import (
    ARMS,
    CRYPTO_TAKER_FEE_RATE,
    MIN_RESOLVED_TRADES,
    REQUIRED_WIN_RATE,
    V8JevWorker,
    V8Runner,
    V8Store,
    arm_metrics,
    jev_quality,
    print_value_panel,
    probability_direction,
    quant_direction,
    value_decision,
)
from .run_paths import run_output_path
from .signal import JevView, quantitative_up_probability

REVISION = "v8.1-early-window-value-r1"
REPORT_FORMAT = "v8.1-early-window-value-report-r1"
EARLY_START_SECONDS = 120
EARLY_END_SECONDS = 90
EARLY_SLOTS = (120, 110, 100)
INTERVAL_SECONDS = 10.0
DEFAULT_SECONDS = 86_400

EVENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS v81_value_events (
    id INTEGER PRIMARY KEY,
    version TEXT NOT NULL,
    arm TEXT NOT NULL,
    slug TEXT NOT NULL,
    slot INTEGER NOT NULL,
    ts REAL NOT NULL,
    accepted INTEGER NOT NULL,
    reason TEXT NOT NULL,
    UNIQUE(version, arm, slug, slot)
);
CREATE INDEX IF NOT EXISTS idx_v81_events
ON v81_value_events(version, arm, ts);
"""


def early_slot(seconds_left: int | None) -> int | None:
    """Map every second in (T-120, T-90] into one of three 10-second slots."""
    if seconds_left is None or not EARLY_END_SECONDS < seconds_left <= EARLY_START_SECONDS:
        return None
    if seconds_left > 110:
        return 120
    if seconds_left > 100:
        return 110
    return 100


class EarlyStore(V8Store):
    def __init__(self, path):
        super().__init__(path)
        self.conn.executescript(EVENT_SCHEMA)

    def has_value_trade(self, version: str, arm: str, slug: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM v8_value_trades WHERE version=? AND arm=? AND slug=?",
            (version, arm, slug),
        ).fetchone() is not None

    def record_value_event(
        self, *, version: str, arm: str, slug: str, slot: int,
        accepted: bool, reason: str, ts: float | None = None,
    ) -> None:
        if arm not in ARMS:
            raise ValueError("unknown_v81_arm")
        with self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO v81_value_events
                (version,arm,slug,slot,ts,accepted,reason)
                VALUES (?,?,?,?,?,?,?)""",
                (version, arm, slug, slot, time.time() if ts is None else ts,
                 int(accepted), reason),
            )

    def value_events(self, version: str) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            """SELECT * FROM v81_value_events
               WHERE version=? ORDER BY ts,id""",
            (version,),
        ).fetchall()]


def early_arm_metrics(rows: list[dict]) -> dict:
    metrics = arm_metrics(rows)
    metrics.pop("checkpoint_distribution", None)
    settled = [r for r in rows if r.get("up_won") is not None]
    slots = Counter(r["checkpoint"] for r in settled)
    metrics["entry_slot_distribution"] = {
        f"T-{slot}": slots[slot] for slot in EARLY_SLOTS
    }
    return metrics


def rejection_summary(events: list[dict]) -> dict:
    out = {}
    for arm in ARMS:
        rows = [r for r in events if r["arm"] == arm]
        rejected = Counter(r["reason"] for r in rows if not r["accepted"])
        out[arm] = {
            "evaluations": len(rows),
            "accepted_events": sum(bool(r["accepted"]) for r in rows),
            "top_rejections": dict(rejected.most_common(10)),
        }
    return out


def build_report(store: EarlyStore, version: str) -> dict:
    params = store.parameters(version)
    if params is None:
        raise ValueError("experiment_not_found")
    trades = {arm: store.value_trades(version, arm) for arm in ARMS}
    arms = {arm: early_arm_metrics(trades[arm]) for arm in ARMS}
    events = store.value_events(version)
    observations = store.observations(version)
    qualified = [arm for arm in ARMS if arms[arm]["gate"]["passed"]]
    return {
        "format": REPORT_FORMAT,
        "experiment_revision": REVISION,
        "paper_only": True,
        "live_trading_enabled": False,
        "primary_window": {
            "start_seconds": EARLY_START_SECONDS,
            "end_seconds_exclusive": EARLY_END_SECONDS,
            "slots": list(EARLY_SLOTS),
            "meaning": "Only T-120 through just before T-90 can create value trades.",
        },
        "protocol": params,
        "coverage": {
            "observations": len(observations),
            "early_slot_observations": sum(r.get("checkpoint") in EARLY_SLOTS for r in observations),
            "markets": len({r["slug"] for r in observations}),
            "jev_requested": sum(
                r.get("jev_status") not in {"not_requested", "not_eligible_quant"}
                for r in observations
            ),
            "jev_success": sum(r.get("jev_status") == "ok" for r in observations),
            "jev_status_counts": dict(Counter(r.get("jev_status") for r in observations)),
        },
        "arms": arms,
        "rejection_summary": rejection_summary(events),
        "qualified_arms": qualified,
        "price_value_gate_passed": bool(qualified),
        "trades": {
            arm: [
                {k: row.get(k) for k in (
                    "arm", "slug", "checkpoint", "signal_ts", "decision_ts",
                    "direction", "quant_p", "jev_p", "jev_answerable",
                    "jev_clarity", "market_mid", "bid", "ask", "spread",
                    "depth_5c_usd", "tick_size", "min_order_size",
                    "edge_probability", "edge", "size", "notional_usd",
                    "taker_fee_rate", "taker_fee_usd_est", "up_won",
                )}
                for row in trades[arm]
            ]
            for arm in ARMS
        },
        "limitations": [
            "V8.1 is a fresh protocol chosen after V8 diagnostics; V8 outcomes are not reused as V8.1 validation.",
            "Only T-120..T-90 is eligible for value trades; later windows are intentionally excluded.",
            "Best ask is public order-book evidence, not authenticated fill proof.",
            "B/C refresh the public book and Quant after Jev returns.",
            "The V8 fee, price, spread, edge, depth, concentration and stress rules remain unchanged.",
        ],
    }


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.1f}%"


def print_scoreboard(console: Console, report: dict, *, final: bool = False) -> None:
    table = Table(
        title="V8.1 T-120→T-90 累计成绩" + (" · FINAL" if final else ""),
        header_style="bold white",
    )
    for col in ("组别", "交易/结算", "胜/负", "胜率", "净PnL", "净ROI", "距50", "状态"):
        table.add_column(col)
    labels = {
        "A_quant_taker": "A Quant→ASK",
        "B_quant_jev_taker": "B Quant+Jev→响应后ASK",
        "C_jev_value": "C Jev自身8% edge",
    }
    for arm in ARMS:
        m = report["arms"][arm]
        status = "[bold green]PASS[/]" if m["gate"]["passed"] else "[yellow]收集中[/]"
        table.add_row(
            labels[arm],
            f"{m['trades']}/{m['settled']}",
            f"{m['wins']}/{m['losses']}",
            _pct(m["win_rate"]),
            f"USD {m['net_pnl_estimated']:+.2f}",
            _pct(m["net_roi_estimated"]),
            str(max(0, MIN_RESOLVED_TRADES - m["settled"])),
            status,
        )
    console.print(table)


def print_rejections(console: Console, report: dict) -> None:
    table = Table(title="V8.1 最近累计过滤原因", header_style="bold white")
    table.add_column("组别")
    table.add_column("评估")
    table.add_column("通过事件")
    table.add_column("主要拒绝原因")
    for arm in ARMS:
        r = report["rejection_summary"][arm]
        reasons = " | ".join(
            f"{name}={count}" for name, count in list(r["top_rejections"].items())[:4]
        ) or "—"
        table.add_row(arm, str(r["evaluations"]), str(r["accepted_events"]), reasons)
    console.print(table)
    statuses = report["coverage"].get("jev_status_counts", {})
    important = [
        f"{name}={statuses.get(name, 0)}"
        for name in ("ok", "busy", "error", "expired", "disabled_auth", "interrupted")
        if statuses.get(name, 0)
    ]
    console.print("[dim]Jev状态：" + (" | ".join(important) if important else "尚无请求") + "[/]")


class EarlyRunner(V8Runner):
    async def on_jev_result(
        self, observation_id: int, slug: str, slot: int,
        original_direction: str, view: JevView, received: float,
    ) -> None:
        if not jev_quality(view, self.s):
            self.store.record_value_event(
                version=self.version, arm="B_quant_jev_taker", slug=slug,
                slot=slot, accepted=False, reason="jev_quality_failed",
            )
            self.store.record_value_event(
                version=self.version, arm="C_jev_value", slug=slug,
                slot=slot, accepted=False, reason="jev_quality_failed",
            )
            return
        if probability_direction(view.p_yes) != original_direction:
            for arm in ("B_quant_jev_taker", "C_jev_value"):
                self.store.record_value_event(
                    version=self.version, arm=arm, slug=slug, slot=slot,
                    accepted=False, reason="jev_direction_disagrees",
                )
            return
        try:
            cand = await self.confirmation_read(slug)
            fresh = local_snapshot(cand, self.s, self.store)
            fresh_quant = quantitative_up_probability(fresh)
        except (ReadUnavailable, ValueError, TimeoutError):
            for arm in ("B_quant_jev_taker", "C_jev_value"):
                self.store.record_value_event(
                    version=self.version, arm=arm, slug=slug, slot=slot,
                    accepted=False, reason="response_refresh_unavailable",
                )
            return

        fresh_direction = quant_direction(fresh_quant)
        if fresh_quant is None or fresh_direction != original_direction:
            for arm in ("B_quant_jev_taker", "C_jev_value"):
                self.store.record_value_event(
                    version=self.version, arm=arm, slug=slug, slot=slot,
                    accepted=False, reason="quant_not_still_high_confidence_same_side",
                )
            return

        decision_b, reason_b = value_decision(
            fresh_quant, fresh_direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        inserted_b = False
        if decision_b is not None:
            inserted_b = self.store.record_value_trade(
                version=self.version, arm="B_quant_jev_taker", slug=slug,
                checkpoint_value=slot, observation_id=observation_id,
                signal_ts=fresh.captured_at.timestamp(), decision_ts=time.time(),
                quant_p=fresh_quant, jev=view, market_mid=cand.book.midpoint,
                decision=decision_b, fee_rate=self.fee_rate,
            )
        self.store.record_value_event(
            version=self.version, arm="B_quant_jev_taker", slug=slug,
            slot=slot, accepted=decision_b is not None,
            reason="value_ready" if decision_b is not None else reason_b,
        )
        print_value_panel(
            self.console, arm="B_quant_jev_taker", slug=slug, cp=slot,
            decision=decision_b, reason=reason_b, quant_p=fresh_quant,
            jev=view, inserted=inserted_b,
        )

        decision_c, reason_c = value_decision(
            view.p_yes, fresh_direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        if decision_b is None and decision_c is not None:
            decision_c = None
            reason_c = "quant_value_filter_failed"
        inserted_c = False
        if decision_c is not None:
            inserted_c = self.store.record_value_trade(
                version=self.version, arm="C_jev_value", slug=slug,
                checkpoint_value=slot, observation_id=observation_id,
                signal_ts=fresh.captured_at.timestamp(), decision_ts=time.time(),
                quant_p=fresh_quant, jev=view, market_mid=cand.book.midpoint,
                decision=decision_c, fee_rate=self.fee_rate,
            )
        self.store.record_value_event(
            version=self.version, arm="C_jev_value", slug=slug,
            slot=slot, accepted=decision_c is not None,
            reason="value_ready" if decision_c is not None else reason_c,
        )
        print_value_panel(
            self.console, arm="C_jev_value", slug=slug, cp=slot,
            decision=decision_c, reason=reason_c, quant_p=fresh_quant,
            jev=view, inserted=inserted_c,
        )

    async def tick(self, *, budget_seconds: float = 8) -> None:
        slug = current_slug()
        market_start = int(slug.rsplit("-", 1)[1])
        approximate_left = int(market_start + 300 - time.time())
        slot = early_slot(approximate_left)
        if slot is None:
            return

        started = time.monotonic()
        cand = snapshot = None
        quant_p = None
        status, reason = "unavailable", ""
        try:
            async with asyncio.timeout(budget_seconds):
                cand = await self.read(slug)
            snapshot = local_snapshot(cand, self.s, self.store)
            actual_slot = early_slot(snapshot.seconds_left)
            if actual_slot is None:
                return
            slot = actual_slot
            if not snapshot.trade_ready:
                reason = "prediction_inputs_not_ready"
            else:
                quant_p = quantitative_up_probability(snapshot)
                if quant_p is None:
                    reason = "quant_probability_unavailable"
                else:
                    status = "prediction_ready"
                    reason = "early_window_candidate"
        except (ReadUnavailable, TimeoutError) as exc:
            reason = str(exc) or "read_timeout"

        observed_ts = snapshot.captured_at.timestamp() if snapshot else time.time()
        payload = snapshot_payload(cand, snapshot) if snapshot is not None else {}
        payload.update(
            early_window=True,
            early_slot=slot,
            read_compute_seconds=time.monotonic() - started,
        )
        eligible_slot = slot if quant_p is not None and snapshot is not None and snapshot.trade_ready else None
        observation_id, recorded_slot = self.store.record(
            version=self.version, session=self.session, slug=slug,
            condition_id=cand.condition_id if cand else None, ts=observed_ts,
            seconds_left=snapshot.seconds_left if snapshot else approximate_left,
            checkpoint=eligible_slot, quant_p=quant_p,
            market_p=cand.book.midpoint if cand else None,
            yes_ask=cand.book.yes_ask if cand else None,
            no_ask=cand.book.no_ask if cand else None,
            status=status, reason=reason, payload=payload,
        )
        if recorded_slot is None:
            return
        if quant_p is None or cand is None or snapshot is None:
            return

        direction = quant_direction(quant_p)
        if direction is None:
            self.store.mark_jev(observation_id, "not_eligible_quant")
            self.store.record_value_event(
                version=self.version, arm="A_quant_taker", slug=slug,
                slot=slot, accepted=False, reason="quant_not_high_confidence",
            )
            self.console.print(Panel(
                f"{slug} · T-{snapshot.seconds_left}s · slot T-{slot}\n"
                f"Quant P(Up)={quant_p:.3f}\n"
                "[yellow]SKIP[/] · Quant未达到92%/8%",
                title="[bold] V8.1 EARLY WINDOW [/]",
                border_style="yellow", expand=False,
            ))
            return

        decision_a, reason_a = value_decision(
            quant_p, direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        inserted_a = False
        if decision_a is not None:
            inserted_a = self.store.record_value_trade(
                version=self.version, arm="A_quant_taker", slug=slug,
                checkpoint_value=slot, observation_id=observation_id,
                signal_ts=observed_ts, decision_ts=time.time(), quant_p=quant_p,
                jev=None, market_mid=cand.book.midpoint, decision=decision_a,
                fee_rate=self.fee_rate,
            )
        self.store.record_value_event(
            version=self.version, arm="A_quant_taker", slug=slug,
            slot=slot, accepted=decision_a is not None,
            reason="value_ready" if decision_a is not None else reason_a,
        )
        print_value_panel(
            self.console, arm="A_quant_taker", slug=slug, cp=slot,
            decision=decision_a, reason=reason_a, quant_p=quant_p,
            inserted=inserted_a,
        )

        if not (
            self.store.has_value_trade(self.version, "B_quant_jev_taker", slug)
            and self.store.has_value_trade(self.version, "C_jev_value", slug)
        ):
            self.shadow.offer(
                observation_id, cand, self.s, snapshot, slot, direction
            )
        else:
            self.store.mark_jev(observation_id, "not_requested")


def experiment_parameters(settings, interval: float, fee_rate: float) -> dict:
    return {
        "protocol_revision": REVISION,
        "timeframe": "5m",
        "primary_window_seconds": {
            "start": EARLY_START_SECONDS,
            "end_exclusive": EARLY_END_SECONDS,
        },
        "slots": list(EARLY_SLOTS),
        "interval_seconds": interval,
        "quant_confidence": .92,
        "min_edge": settings.min_edge,
        "min_trade_price": settings.min_trade_price,
        "max_trade_price": settings.max_trade_price,
        "max_spread": settings.max_spread,
        "max_usd_per_trade": settings.max_usd_per_trade,
        "jev_min_answerable": settings.min_answerable,
        "jev_min_clarity": settings.min_clarity,
        "taker_fee_rate_assumption": fee_rate,
        "minimum_resolved_trades": MIN_RESOLVED_TRADES,
        "required_win_rate": f">{REQUIRED_WIN_RATE:.0%}",
        "paper_only": True,
        "authenticated_client": False,
        "fresh_validation_after_v8": True,
    }


async def run_v81(
    settings, console: Console, *, version: str, interval: float,
    seconds: int, fee_rate: float,
) -> None:
    if not settings.dry_run or settings.allowed_timeframes != "5m":
        raise ValueError("V8.1 requires BTC 5m dry-run settings")
    if not settings.typesafe_api_key:
        raise ValueError("TYPESAFE_API_KEY is required for V8.1")
    next_deadline(0, 0, interval)

    store = EarlyStore(settings.db_path)
    tasks = []
    try:
        store.ensure_experiment(version, experiment_parameters(settings, interval, fee_rate))
        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            confirm_public = await stack.enter_async_context(AsyncPublicClient())
            settlement = await stack.enter_async_context(AsyncPublicClient())
            jev = await stack.enter_async_context(JevClient(
                settings.typesafe_api_key, model=settings.jev_model,
                base_url=settings.typesafe_base_url,
                timeout=settings.jev_timeout_seconds,
                max_retries=settings.jev_max_retries,
            ))
            shadow = V8JevWorker(jev, store, console)
            runner = EarlyRunner(
                settings, store, public, shadow, console,
                version=version, session=uuid.uuid4().hex,
                fee_rate=fee_rate, confirm_public=confirm_public,
            )
            shadow.on_result = runner.on_jev_result
            tasks = [
                asyncio.create_task(watch_chainlink_anchors(settings, store), name="chainlink"),
                asyncio.create_task(shadow.work(), name="jev-v81"),
                asyncio.create_task(
                    settlement_worker(settlement, settings, store, version),
                    name="settlement",
                ),
            ]
            console.print(Panel(
                "[bold]BTC 5m · EARLY PRICE VALUE[/]\n"
                "主交易窗口：[bold cyan]T-120 → T-90[/]\n"
                "评估槽位：T-120 / T-110 / T-100\n"
                "T-90之后不再产生这条策略的新交易\n"
                f"Quant=92% | edge={settings.min_edge:.0%} | ASK≤{settings.max_trade_price:.2f}\n"
                "[bold green]纯模拟 · Jev异步确认 · 无真实订单[/]",
                title=f"[bold cyan] {REVISION} [/]",
                border_style="cyan", expand=False,
            ))
            clock = asyncio.get_running_loop().time
            deadline = clock()
            stop_at = clock() + seconds
            next_board = clock()
            try:
                while clock() < stop_at:
                    for task in tasks:
                        if task.done():
                            task.result()
                            raise RuntimeError(f"task_stopped:{task.get_name()}")
                    await runner.tick(budget_seconds=min(8.0, interval * .8))
                    if clock() >= next_board:
                        report = build_report(store, version)
                        print_scoreboard(console, report)
                        print_rejections(console, report)
                        next_board = clock() + 60
                    deadline, skipped = next_deadline(deadline, clock(), interval)
                    if skipped:
                        console.print(
                            f"[yellow]跳过{skipped}个超时节拍；不追补历史窗口[/]"
                        )
                    await asyncio.sleep(max(0.0, min(deadline, stop_at) - clock()))
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                while not shadow.queue.empty():
                    observation_id, *_ = shadow.queue.get_nowait()
                    store.mark_jev(observation_id, "interrupted")
                    shadow.queue.task_done()
    finally:
        store.close()


async def settle_all(settings, db: Path, version: str) -> int:
    store = EarlyStore(db)
    try:
        async with AsyncPublicClient() as client:
            total = 0
            while True:
                n = await settle_some(client, settings, store, version, limit=500)
                total += n
                if n == 0:
                    return total
    finally:
        store.close()


def resolve_paths(
    db_arg: Path | None, resume_db: Path | None,
    out_arg: Path | None, stamp: str,
) -> tuple[Path, Path, bool]:
    if db_arg is not None and resume_db is not None:
        raise ValueError("cannot_use_db_and_resume_db_together")
    if resume_db is not None:
        db = Path(resume_db)
        if not db.is_file():
            raise ValueError("resume_database_not_found")
        out = run_output_path(
            out_arg, f"v81_early_value_forward_resume_{stamp}.json.gz"
        )
        if out.exists():
            raise FileExistsError("report_already_exists")
        return db, out, True
    db = run_output_path(db_arg, f"jevymarket.v81-early-value_{stamp}.db")
    out = run_output_path(out_arg, f"v81_early_value_forward_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("experiment_output_already_exists")
    return db, out, False


def write_report(report: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(
            report, handle, ensure_ascii=False,
            allow_nan=False, separators=(",", ":"),
        )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="V8.1 BTC5m T-120到T-90价格价值前向实验；无真实订单"
    )
    parser.add_argument("--seconds", type=int, default=DEFAULT_SECONDS)
    parser.add_argument("--loop", type=float, default=INTERVAL_SECONDS)
    parser.add_argument(
        "--taker-fee-rate", type=float, default=CRYPTO_TAKER_FEE_RATE
    )
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument("--db", type=Path)
    paths.add_argument("--resume-db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    if not 1800 <= args.seconds <= 86400:
        parser.error("seconds必须在1800到86400之间")
    if args.loop != INTERVAL_SECONDS:
        parser.error("V8.1固定10秒采样")
    if not 0 <= args.taker_fee_rate <= .2:
        parser.error("taker fee rate超出实验允许范围")

    stamp = datetime.now(
        timezone(timedelta(hours=8))
    ).strftime("%Y%m%d_%H%M%S_%f")
    try:
        db, out, resumed = resolve_paths(
            args.db, args.resume_db, args.out, stamp
        )
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))

    settings = fast_settings().model_copy(
        update={"db_path": str(db), "strategy_version": REVISION}
    )
    console = Console()
    try:
        with single_instance(str(db)):
            asyncio.run(run_v81(
                settings, console, version=REVISION,
                interval=args.loop, seconds=args.seconds,
                fee_rate=args.taker_fee_rate,
            ))
    except KeyboardInterrupt:
        console.print(
            "[yellow]已停止；保留V8.1前向记录并导出累计结果。[/]"
        )

    settled = asyncio.run(settle_all(settings, db, REVISION))
    store = EarlyStore(db)
    try:
        report = build_report(store, REVISION)
    finally:
        store.close()
    report["settlements_added_at_export"] = settled
    report["resumed_existing_database"] = resumed
    write_report(report, out)
    print_scoreboard(console, report, final=True)
    print_rejections(console, report)
    console.print(Panel(
        f"报告：[bold]{out.resolve()}[/]\n数据库：[bold]{db.resolve()}[/]",
        title="[bold green] V8.1 导出完成 [/]",
        border_style="green", expand=False,
    ))


if __name__ == "__main__":
    main()
