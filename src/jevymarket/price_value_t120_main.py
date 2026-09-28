"""V8.2 T-120 primary strategy with T-100/C shadow research.

Primary strategy:
    T-120 + Quant high confidence + Jev quality/direction confirmation
    + response-time refreshed Quant + latest best ask value rules.

Shadow research:
    - T-100 A/B/C continues to be recorded.
    - C (Jev-own-edge) at T-120 continues to be recorded.
    - T-120 A is retained as a Quant-only control.

Only the primary T-120 B arm can pass the experiment gate or reach any future
live hook. Shadow results never unlock the primary strategy.
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
from .price_value_early_forward import EarlyRunner, EarlyStore
from .price_value_forward import (
    ARMS,
    CRYPTO_TAKER_FEE_RATE,
    MIN_RESOLVED_TRADES,
    REQUIRED_WIN_RATE,
    V8JevWorker,
    arm_metrics,
    print_value_panel,
    quant_direction,
    value_decision,
)
from .run_paths import run_output_path
from .signal import quantitative_up_probability

REVISION = "v8.2-t120-main-shadow-r2"
REPORT_FORMAT = "v8.2-t120-main-shadow-report-r2"
PRIMARY_SLOT = 120
PRIMARY_PREFETCH_SECONDS = 130
SHADOW_SLOT = 100
SLOTS = (PRIMARY_SLOT, SHADOW_SLOT)
INTERVAL_SECONDS = 10.0
DEFAULT_SECONDS = 86_400
PRIMARY_ARM = "B_quant_jev_taker"

V82_SLOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS v82_slot_trades (
    id INTEGER PRIMARY KEY,
    version TEXT NOT NULL,
    arm TEXT NOT NULL,
    slug TEXT NOT NULL,
    slot INTEGER NOT NULL,
    observation_id INTEGER NOT NULL,
    signal_ts REAL NOT NULL,
    decision_ts REAL NOT NULL,
    direction TEXT NOT NULL,
    quant_p REAL NOT NULL,
    jev_p REAL,
    jev_answerable REAL,
    jev_clarity INTEGER,
    market_mid REAL,
    bid REAL NOT NULL,
    ask REAL NOT NULL,
    spread REAL NOT NULL,
    depth_5c_usd REAL NOT NULL,
    tick_size REAL NOT NULL,
    min_order_size REAL NOT NULL,
    edge_probability REAL NOT NULL,
    edge REAL NOT NULL,
    size REAL NOT NULL,
    notional_usd REAL NOT NULL,
    taker_fee_rate REAL NOT NULL,
    taker_fee_usd_est REAL NOT NULL,
    UNIQUE(version, arm, slug, slot)
);
CREATE INDEX IF NOT EXISTS idx_v82_slot_trades
ON v82_slot_trades(version, arm, slot, decision_ts);
"""


class V82Store(EarlyStore):
    """Keep independent T-120/T-100 research rows even within the same market."""

    def __init__(self, path):
        super().__init__(path)
        self.conn.executescript(V82_SLOT_SCHEMA)

    def record_value_trade(
        self,
        *,
        version: str,
        arm: str,
        slug: str,
        checkpoint_value: int,
        observation_id: int,
        signal_ts: float,
        decision_ts: float,
        quant_p: float,
        jev,
        market_mid: float | None,
        decision,
        fee_rate: float,
    ) -> bool:
        values = (
            version, arm, slug, checkpoint_value, observation_id,
            signal_ts, decision_ts, decision.direction, quant_p,
            jev.p_yes if jev else None,
            jev.answerable if jev else None,
            jev.clarity if jev else None,
            market_mid, decision.bid, decision.ask, decision.spread,
            decision.depth_5c_usd, decision.tick_size,
            decision.min_order_size, decision.probability, decision.edge,
            decision.size, decision.notional_usd, fee_rate,
            decision.taker_fee_usd_est,
        )
        with self.conn:
            slot_cur = self.conn.execute(
                """INSERT OR IGNORE INTO v82_slot_trades
                (version,arm,slug,slot,observation_id,signal_ts,decision_ts,
                 direction,quant_p,jev_p,jev_answerable,jev_clarity,market_mid,
                 bid,ask,spread,depth_5c_usd,tick_size,min_order_size,
                 edge_probability,edge,size,notional_usd,taker_fee_rate,
                 taker_fee_usd_est)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
        # Preserve the legacy first-trade-per-arm table for compatibility, but
        # V8.2 reporting uses v82_slot_trades as the authoritative research set.
        super().record_value_trade(
            version=version,
            arm=arm,
            slug=slug,
            checkpoint_value=checkpoint_value,
            observation_id=observation_id,
            signal_ts=signal_ts,
            decision_ts=decision_ts,
            quant_p=quant_p,
            jev=jev,
            market_mid=market_mid,
            decision=decision,
            fee_rate=fee_rate,
        )
        return slot_cur.rowcount == 1

    def slot_trades(
        self, version: str, arm: str | None = None, slot: int | None = None
    ) -> list[dict]:
        query = """SELECT t.*, r.up_won FROM v82_slot_trades t
                   LEFT JOIN market_results r ON r.slug=t.slug
                   WHERE t.version=?"""
        args: list[object] = [version]
        if arm is not None:
            query += " AND t.arm=?"
            args.append(arm)
        if slot is not None:
            query += " AND t.slot=?"
            args.append(slot)
        query += " ORDER BY t.decision_ts,t.id"
        out = []
        for row in self.conn.execute(query, args).fetchall():
            item = dict(row)
            item["checkpoint"] = item["slot"]
            out.append(item)
        return out


def strategy_slot(seconds_left: int | None) -> int | None:
    """Record only T-120 and T-100; T-110 is intentionally retired."""
    if seconds_left is None:
        return None
    if 110 < seconds_left <= 120:
        return PRIMARY_SLOT
    if 90 < seconds_left <= 100:
        return SHADOW_SLOT
    return None


def read_candidate_slot(seconds_left: int | None) -> int | None:
    """Open the T-120 read one cadence early so first-market metadata cannot eat the slot.

    The early 121..130s read is warm-up only unless the network read itself
    finishes inside the real 111..120s primary window. T-110 remains retired.
    """
    if seconds_left is None:
        return None
    if 120 < seconds_left <= PRIMARY_PREFETCH_SECONDS:
        return PRIMARY_SLOT
    return strategy_slot(seconds_left)


def safe_trade(row: dict) -> dict:
    return {k: row.get(k) for k in (
        "arm", "slug", "slot", "signal_ts", "decision_ts", "direction",
        "quant_p", "jev_p", "jev_answerable", "jev_clarity", "market_mid",
        "bid", "ask", "spread", "depth_5c_usd", "tick_size", "min_order_size",
        "edge_probability", "edge", "size", "notional_usd",
        "taker_fee_rate", "taker_fee_usd_est", "up_won",
    )}


def slot_metrics(rows: list[dict], slot: int) -> dict:
    return arm_metrics([row for row in rows if row.get("slot") == slot])


def build_report(store: V82Store, version: str) -> dict:
    params = store.parameters(version)
    if params is None:
        raise ValueError("experiment_not_found")

    trades = {arm: store.slot_trades(version, arm) for arm in ARMS}
    primary_rows = [
        row for row in trades[PRIMARY_ARM]
        if row.get("slot") == PRIMARY_SLOT
    ]
    primary = arm_metrics(primary_rows)

    shadow = {
        "t120_quant_control": slot_metrics(trades["A_quant_taker"], PRIMARY_SLOT),
        "t120_jev_own_edge_c": slot_metrics(trades["C_jev_value"], PRIMARY_SLOT),
        "t100_quant_a": slot_metrics(trades["A_quant_taker"], SHADOW_SLOT),
        "t100_quant_jev_b": slot_metrics(trades["B_quant_jev_taker"], SHADOW_SLOT),
        "t100_jev_own_edge_c": slot_metrics(trades["C_jev_value"], SHADOW_SLOT),
    }

    events = store.value_events(version)
    observations = store.observations(version)
    return {
        "format": REPORT_FORMAT,
        "experiment_revision": REVISION,
        "paper_only": True,
        "live_trading_enabled": False,
        "primary_strategy": {
            "slot": PRIMARY_SLOT,
            "prefetch_start_seconds": PRIMARY_PREFETCH_SECONDS,
            "arm": PRIMARY_ARM,
            "description": "T-120 Quant + Jev confirmation + response-time refreshed Quant/latest ASK",
            "metrics": primary,
            "passed": primary["gate"]["passed"],
        },
        "shadow_research": {
            "affects_primary_gate": False,
            "t110_retired": True,
            "metrics": shadow,
        },
        "protocol": params,
        "coverage": {
            "observations": len(observations),
            "slot_observations": sum(r.get("checkpoint") in SLOTS for r in observations),
            "t120_observations": sum(r.get("checkpoint") == PRIMARY_SLOT for r in observations),
            "t100_observations": sum(r.get("checkpoint") == SHADOW_SLOT for r in observations),
            "markets": len({r["slug"] for r in observations}),
            "jev_requested": sum(
                r.get("jev_status") not in {"not_requested", "not_eligible_quant"}
                for r in observations
            ),
            "jev_success": sum(r.get("jev_status") == "ok" for r in observations),
            "jev_status_counts": dict(Counter(r.get("jev_status") for r in observations)),
        },
        "rejection_summary": {
            arm: {
                "evaluations": len([e for e in events if e["arm"] == arm]),
                "accepted_events": sum(
                    bool(e["accepted"]) for e in events if e["arm"] == arm
                ),
                "top_rejections": dict(Counter(
                    e["reason"]
                    for e in events
                    if e["arm"] == arm and not e["accepted"]
                ).most_common(10)),
            }
            for arm in ARMS
        },
        "price_value_gate_passed": primary["gate"]["passed"],
        "qualified_primary": PRIMARY_ARM if primary["gate"]["passed"] else None,
        "trades": {
            arm: [safe_trade(row) for row in trades[arm]]
            for arm in ARMS
        },
        "limitations": [
            "V8.2 is a fresh forward protocol chosen after V8.1; V8.1 outcomes are not reused as V8.2 validation.",
            "R2 opens a warm-up read at T-130..T-120 so metadata latency cannot silently consume the T-120 slot.",
            "Only T-120 B is the primary strategy and only it can pass the experiment gate.",
            "T-100 and C are retained as shadow research and never affect the primary gate.",
            "T-110 is intentionally retired from new V8.2 Jev requests.",
            "Best ask is public order-book evidence, not authenticated fill proof.",
        ],
    }


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.1f}%"


def print_primary(console: Console, report: dict, *, final: bool = False) -> None:
    m = report["primary_strategy"]["metrics"]
    table = Table(
        title="V8.2 主策略 · T-120 Quant + Jev + 最新ASK" + (" · FINAL" if final else ""),
        header_style="bold white",
    )
    for col in ("交易/结算", "胜/负", "胜率", "净PnL", "净ROI", "去Top3", "+1tick", "距50", "状态"):
        table.add_column(col)
    status = "[bold green]PASS[/]" if m["gate"]["passed"] else "[yellow]收集中[/]"
    table.add_row(
        f"{m['trades']}/{m['settled']}",
        f"{m['wins']}/{m['losses']}",
        _pct(m["win_rate"]),
        f"USD {m['net_pnl_estimated']:+.2f}",
        _pct(m["net_roi_estimated"]),
        f"USD {m['net_pnl_minus_top3_positive_contributions']:+.2f}",
        f"USD {m['stress_plus_1tick']['net_pnl_estimated']:+.2f}",
        str(max(0, MIN_RESOLVED_TRADES - m["settled"])),
        status,
    )
    console.print(table)


def print_shadow(console: Console, report: dict) -> None:
    metrics = report["shadow_research"]["metrics"]
    rows = (
        ("T-120 A Quant控制", "t120_quant_control"),
        ("T-120 C Jev自身edge", "t120_jev_own_edge_c"),
        ("T-100 A Quant", "t100_quant_a"),
        ("T-100 B Quant+Jev", "t100_quant_jev_b"),
        ("T-100 C Jev自身edge", "t100_jev_own_edge_c"),
    )
    table = Table(
        title="V8.2 影子研究 · 不影响主策略PASS",
        header_style="bold white",
    )
    for col in ("研究项", "交易/结算", "胜/负", "胜率", "净PnL", "净ROI"):
        table.add_column(col)
    for label, key in rows:
        m = metrics[key]
        table.add_row(
            label,
            f"{m['trades']}/{m['settled']}",
            f"{m['wins']}/{m['losses']}",
            _pct(m["win_rate"]),
            f"USD {m['net_pnl_estimated']:+.2f}",
            _pct(m["net_roi_estimated"]),
        )
    console.print(table)


def print_rejections(console: Console, report: dict) -> None:
    table = Table(title="V8.2 累计过滤原因", header_style="bold white")
    table.add_column("组别")
    table.add_column("评估")
    table.add_column("通过事件")
    table.add_column("主要拒绝原因")
    for arm in ARMS:
        r = report["rejection_summary"][arm]
        reasons = " | ".join(
            f"{name}={count}"
            for name, count in list(r["top_rejections"].items())[:4]
        ) or "—"
        table.add_row(
            arm, str(r["evaluations"]), str(r["accepted_events"]), reasons
        )
    console.print(table)
    statuses = report["coverage"].get("jev_status_counts", {})
    important = [
        f"{name}={statuses.get(name, 0)}"
        for name in (
            "ok", "busy", "error", "expired", "disabled_auth", "interrupted"
        )
        if statuses.get(name, 0)
    ]
    console.print(
        "[dim]Jev状态："
        + (" | ".join(important) if important else "尚无请求")
        + "[/]"
    )


class V82Runner(EarlyRunner):
    async def tick(self, *, budget_seconds: float = 8) -> None:
        slug = current_slug()
        market_start = int(slug.rsplit("-", 1)[1])
        approximate_left = int(market_start + 300 - time.time())
        candidate_slot = read_candidate_slot(approximate_left)
        if candidate_slot is None:
            return
        slot = candidate_slot

        started = time.monotonic()
        cand = snapshot = None
        quant_p = None
        status, reason = "unavailable", ""
        try:
            async with asyncio.timeout(budget_seconds):
                cand = await self.read(slug)
            snapshot = local_snapshot(cand, self.s, self.store)
            actual_slot = strategy_slot(snapshot.seconds_left)
            if actual_slot is None:
                # The 121..130s read intentionally warms market metadata/book.
                # If it finishes before T-120, wait for the next 10s cadence.
                if candidate_slot == PRIMARY_SLOT and snapshot.seconds_left > PRIMARY_SLOT:
                    return
                # Do not silently lose a primary slot if the first read crossed
                # below T-110; persist a diagnostic row without a checkpoint.
                status = "timing_miss"
                reason = "primary_window_crossed_during_read"
            else:
                slot = actual_slot
                if slot == PRIMARY_SLOT:
                    # Metadata was fetched no more than ~10s before the primary
                    # read. Touch its freshness so T-100 research 20s later
                    # doesn't pay another metadata round-trip at the slot edge.
                    self.metadata_read_at = time.monotonic()
            if actual_slot is not None and not snapshot.trade_ready:
                reason = "prediction_inputs_not_ready"
            elif actual_slot is not None:
                quant_p = quantitative_up_probability(snapshot)
                if quant_p is None:
                    reason = "quant_probability_unavailable"
                else:
                    status = "prediction_ready"
                    reason = "v82_t120_primary_or_t100_shadow"
        except (ReadUnavailable, TimeoutError) as exc:
            reason = str(exc) or "read_timeout"

        observed_ts = snapshot.captured_at.timestamp() if snapshot else time.time()
        payload = snapshot_payload(cand, snapshot) if snapshot is not None else {}
        payload.update(
            v82=True,
            role="primary" if slot == PRIMARY_SLOT else "shadow",
            strategy_slot=slot,
            read_compute_seconds=time.monotonic() - started,
        )
        eligible_slot = (
            slot
            if quant_p is not None and snapshot is not None and snapshot.trade_ready
            else None
        )
        observation_id, recorded_slot = self.store.record(
            version=self.version,
            session=self.session,
            slug=slug,
            condition_id=cand.condition_id if cand else None,
            ts=observed_ts,
            seconds_left=snapshot.seconds_left if snapshot else approximate_left,
            checkpoint=eligible_slot,
            quant_p=quant_p,
            market_p=cand.book.midpoint if cand else None,
            yes_ask=cand.book.yes_ask if cand else None,
            no_ask=cand.book.no_ask if cand else None,
            status=status,
            reason=reason,
            payload=payload,
        )
        if recorded_slot is None or quant_p is None or cand is None or snapshot is None:
            return

        direction = quant_direction(quant_p)
        if direction is None:
            self.store.mark_jev(observation_id, "not_eligible_quant")
            self.store.record_value_event(
                version=self.version,
                arm="A_quant_taker",
                slug=slug,
                slot=slot,
                accepted=False,
                reason="quant_not_high_confidence",
            )
            role = "PRIMARY" if slot == PRIMARY_SLOT else "SHADOW"
            self.console.print(Panel(
                f"{slug} · T-{snapshot.seconds_left}s · slot T-{slot}\n"
                f"Quant P(Up)={quant_p:.3f}\n"
                "[yellow]SKIP[/] · Quant未达到92%/8%",
                title=f"[bold] V8.2 {role} [/]",
                border_style="yellow",
                expand=False,
            ))
            return

        decision_a, reason_a = value_decision(
            quant_p, direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        inserted_a = False
        if decision_a is not None:
            inserted_a = self.store.record_value_trade(
                version=self.version,
                arm="A_quant_taker",
                slug=slug,
                checkpoint_value=slot,
                observation_id=observation_id,
                signal_ts=observed_ts,
                decision_ts=time.time(),
                quant_p=quant_p,
                jev=None,
                market_mid=cand.book.midpoint,
                decision=decision_a,
                fee_rate=self.fee_rate,
            )
        self.store.record_value_event(
            version=self.version,
            arm="A_quant_taker",
            slug=slug,
            slot=slot,
            accepted=decision_a is not None,
            reason="value_ready" if decision_a is not None else reason_a,
        )
        print_value_panel(
            self.console,
            arm="A_quant_taker",
            slug=slug,
            cp=slot,
            decision=decision_a,
            reason=reason_a,
            quant_p=quant_p,
            inserted=inserted_a,
        )

        # Jev remains required for primary T-120 B and for T-100/C shadow research.
        self.shadow.offer(
            observation_id, cand, self.s, snapshot, slot, direction
        )


def experiment_parameters(settings, interval: float, fee_rate: float) -> dict:
    return {
        "protocol_revision": REVISION,
        "timeframe": "5m",
        "primary_strategy": {
            "slot": PRIMARY_SLOT,
            "arm": PRIMARY_ARM,
            "rule": "Quant>=92% + Jev quality/direction + refreshed Quant + latest ASK value rules",
        },
        "shadow_research": {
            "slots": [PRIMARY_SLOT, SHADOW_SLOT],
            "retain_t100": True,
            "retain_c": True,
            "t110_retired": True,
            "affects_primary_gate": False,
        },
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
        "minimum_primary_resolved_trades": MIN_RESOLVED_TRADES,
        "required_primary_win_rate": f">{REQUIRED_WIN_RATE:.0%}",
        "paper_only": True,
        "authenticated_client": False,
        "fresh_validation_after_v81": True,
    }


async def run_v82(
    settings, console: Console, *, version: str, interval: float,
    seconds: int, fee_rate: float,
) -> None:
    if not settings.dry_run or settings.allowed_timeframes != "5m":
        raise ValueError("V8.2 requires BTC 5m dry-run settings")
    if not settings.typesafe_api_key:
        raise ValueError("TYPESAFE_API_KEY is required for V8.2")
    next_deadline(0, 0, interval)

    store = V82Store(settings.db_path)
    tasks = []
    try:
        store.ensure_experiment(
            version, experiment_parameters(settings, interval, fee_rate)
        )
        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            confirm_public = await stack.enter_async_context(AsyncPublicClient())
            settlement = await stack.enter_async_context(AsyncPublicClient())
            jev = await stack.enter_async_context(JevClient(
                settings.typesafe_api_key,
                model=settings.jev_model,
                base_url=settings.typesafe_base_url,
                timeout=settings.jev_timeout_seconds,
                max_retries=settings.jev_max_retries,
            ))
            shadow = V8JevWorker(jev, store, console)
            runner = V82Runner(
                settings,
                store,
                public,
                shadow,
                console,
                version=version,
                session=uuid.uuid4().hex,
                fee_rate=fee_rate,
                confirm_public=confirm_public,
            )
            shadow.on_result = runner.on_jev_result
            tasks = [
                asyncio.create_task(
                    watch_chainlink_anchors(settings, store), name="chainlink"
                ),
                asyncio.create_task(shadow.work(), name="jev-v82"),
                asyncio.create_task(
                    settlement_worker(settlement, settings, store, version),
                    name="settlement",
                ),
            ]
            console.print(Panel(
                "[bold]BTC 5m · V8.2[/]\n"
                "主策略：[bold cyan]T-120 Quant + Jev + 最新ASK[/]\n"
                "影子研究：T-100 A/B/C + T-120 A/C\n"
                "T-110：停止新采集\n"
                f"Quant=92% | edge={settings.min_edge:.0%} | ASK≤{settings.max_trade_price:.2f}\n"
                "[bold green]纯模拟 · 主策略与影子研究分离[/]",
                title=f"[bold cyan] {REVISION} [/]",
                border_style="cyan",
                expand=False,
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
                    await runner.tick(
                        budget_seconds=min(8.0, interval * .8)
                    )
                    if clock() >= next_board:
                        report = build_report(store, version)
                        print_primary(console, report)
                        print_shadow(console, report)
                        print_rejections(console, report)
                        next_board = clock() + 60
                    deadline, skipped = next_deadline(
                        deadline, clock(), interval
                    )
                    if skipped:
                        console.print(
                            f"[yellow]跳过{skipped}个超时节拍；不追补历史槽位[/]"
                        )
                    await asyncio.sleep(
                        max(0.0, min(deadline, stop_at) - clock())
                    )
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
    store = V82Store(db)
    try:
        async with AsyncPublicClient() as client:
            total = 0
            while True:
                n = await settle_some(
                    client, settings, store, version, limit=500
                )
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
            out_arg, f"v82_t120_main_shadow_resume_{stamp}.json.gz"
        )
        if out.exists():
            raise FileExistsError("report_already_exists")
        return db, out, True
    db = run_output_path(
        db_arg, f"jevymarket.v82-t120-main-shadow_{stamp}.db"
    )
    out = run_output_path(
        out_arg, f"v82_t120_main_shadow_{stamp}.json.gz"
    )
    if db.exists() or out.exists():
        raise FileExistsError("experiment_output_already_exists")
    return db, out, False


def write_report(report: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(
            report,
            handle,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="V8.2 T-120 Quant+Jev+最新ASK主策略；保留T-100/C影子研究"
    )
    parser.add_argument("--seconds", type=int, default=DEFAULT_SECONDS)
    parser.add_argument("--loop", type=float, default=INTERVAL_SECONDS)
    parser.add_argument(
        "--taker-fee-rate",
        type=float,
        default=CRYPTO_TAKER_FEE_RATE,
    )
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument("--db", type=Path)
    paths.add_argument("--resume-db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    if not 1800 <= args.seconds <= 86_400:
        parser.error("seconds必须在1800到86400之间")
    if args.loop != INTERVAL_SECONDS:
        parser.error("V8.2固定10秒采样")
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
            asyncio.run(run_v82(
                settings,
                console,
                version=REVISION,
                interval=args.loop,
                seconds=args.seconds,
                fee_rate=args.taker_fee_rate,
            ))
    except KeyboardInterrupt:
        console.print(
            "[yellow]已停止；保留V8.2前向记录并导出累计结果。[/]"
        )

    settled = asyncio.run(settle_all(settings, db, REVISION))
    store = V82Store(db)
    try:
        report = build_report(store, REVISION)
    finally:
        store.close()
    report["settlements_added_at_export"] = settled
    report["resumed_existing_database"] = resumed
    write_report(report, out)
    print_primary(console, report, final=True)
    print_shadow(console, report)
    print_rejections(console, report)
    console.print(Panel(
        f"报告：[bold]{out.resolve()}[/]\n数据库：[bold]{db.resolve()}[/]",
        title="[bold green] V8.2 导出完成 [/]",
        border_style="green",
        expand=False,
    ))


if __name__ == "__main__":
    main()
