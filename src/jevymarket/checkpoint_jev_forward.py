"""BTC5m forward checkpoint prediction experiment: Quant vs Jev confirmation.

No paper orders and no authenticated Polymarket client are created. The experiment
records forward predictions at fixed checkpoints and settles them against official
market outcomes.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import math
import statistics
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
from .fast_runner import FastRunner, JevShadow, settle_some, settlement_worker
from .fast_store import FastStore, probability_metrics
from .fast_strategy import fast_settings, local_snapshot, next_deadline, snapshot_payload
from .jev import JevClient
from .market_data import watch_chainlink_anchors
from .run_paths import run_output_path
from .signal import quantitative_up_probability

REVISION = "v7-checkpoint-jev-forward-r1"
REPORT_FORMAT = "v7-checkpoint-jev-forward-report-r2"
CHECKPOINTS = (120, 90, 60, 45)
CHECKPOINT_LATENESS_SECONDS = 10
INTERVAL_SECONDS = 10.0
QUANT_CONFIDENCE = 0.92
MIN_RESOLVED_SIGNALS = 50
REQUIRED_WIN_RATE = 0.65
ARMS = ("A_quant", "B_quant_jev", "C_quant_jev_market")



def side_probability(p: float | None, direction: str | None) -> float | None:
    if p is None or direction not in {"UP", "DOWN"} or not math.isfinite(p) or not 0 <= p <= 1:
        return None
    return p if direction == "UP" else 1 - p


def directional_edge(row: dict, model_field: str = "quant_p") -> float | None:
    direction = row.get("signal_direction")
    model = side_probability(row.get(model_field), direction)
    market = side_probability(row.get("market_p"), direction)
    if model is None or market is None:
        return None
    return model - market


def edge_metrics(signals: list[dict], model_field: str = "quant_p") -> dict:
    values = [directional_edge(r, model_field) for r in signals]
    values = [v for v in values if v is not None]
    return {
        "n": len(values),
        "positive": sum(v > 0 for v in values),
        "positive_fraction": sum(v > 0 for v in values) / len(values) if values else None,
        "mean": sum(values) / len(values) if values else None,
        "median": statistics.median(values) if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def _pct(p: float | None) -> str:
    return "—" if p is None else f"{100 * p:.1f}%"


def _direction_style(direction: str | None) -> str:
    if direction == "UP":
        return "[bold green]UP ↑[/]"
    if direction == "DOWN":
        return "[bold red]DOWN ↓[/]"
    return "[dim]无信号[/]"


def print_checkpoint_panel(console: Console, row: dict) -> None:
    qdir = quant_direction(row.get("quant_p"))
    market_dir = probability_direction(row.get("market_p"))
    qside = side_probability(row.get("quant_p"), qdir)
    mside = side_probability(row.get("market_p"), qdir)
    edge = qside - mside if qside is not None and mside is not None else None

    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("市场", row["slug"])
    table.add_row("Quant", f"{_direction_style(qdir)}  {_pct(qside)}")
    table.add_row("Polymarket", f"{_direction_style(market_dir)}  P(预测方向)={_pct(mside)}")
    table.add_row("Quant - Market", "—" if edge is None else f"[bold cyan]{edge * 100:+.2f} pp[/]")
    if qdir is None:
        table.add_row("A Quant", "[yellow]不出信号[/]")
        table.add_row("Jev", "[dim]跳过：Quant未达到92%[/]")
        table.add_row("B / C", "[dim]不出信号[/]")
    else:
        table.add_row("A Quant", f"[bold green]SIGNAL {qdir}[/]")
        table.add_row("Jev", "[bold yellow]异步确认中…[/]")
        table.add_row("B / C", "[yellow]等待Jev[/]")

    console.print(Panel(
        table,
        title=f"[bold cyan] BTC 5m CHECKPOINT  T-{row['checkpoint']}s [/]",
        border_style="cyan",
        expand=False,
    ))


def print_jev_panel(console: Console, row: dict, parameters: dict) -> None:
    qdir = quant_direction(row.get("quant_p"))
    jdir = probability_direction(row.get("jev_p"))
    mdir = probability_direction(row.get("market_p"))
    quality = jev_quality(row, parameters)
    bdir = arm_direction(row, "B_quant_jev", parameters)
    cdir = arm_direction(row, "C_quant_jev_market", parameters)
    latency = None
    if row.get("jev_requested_ts") is not None and row.get("jev_received_ts") is not None:
        latency = row["jev_received_ts"] - row["jev_requested_ts"]

    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("市场", row["slug"])
    table.add_row("Quant", f"{_direction_style(qdir)}  {_pct(side_probability(row.get('quant_p'), qdir))}")
    table.add_row("Jev", f"{_direction_style(jdir)}  {_pct(side_probability(row.get('jev_p'), jdir))}")
    table.add_row(
        "Jev质量",
        f"answerable={_pct(row.get('jev_answerable'))} | clarity={row.get('jev_clarity')} | "
        f"{'[green]PASS[/]' if quality else '[red]FAIL[/]'}",
    )
    table.add_row("Polymarket", f"{_direction_style(mdir)}  {_pct(side_probability(row.get('market_p'), mdir))}")
    table.add_row("Jev延迟", "—" if latency is None else f"{latency:.2f}s")
    table.add_row("A Quant", f"[green]✓ {qdir}[/]" if qdir else "[dim]—[/]")
    table.add_row("B + Jev", f"[green]✓ {bdir}[/]" if bdir else "[red]✗ 过滤[/]")
    table.add_row("C + Jev + Market", f"[green]✓ {cdir}[/]" if cdir else "[red]✗ 过滤[/]")

    console.print(Panel(
        table,
        title="[bold magenta] JEV CONFIRMATION [/]",
        border_style="magenta",
        expand=False,
    ))


def print_scoreboard(console: Console, report: dict, *, final: bool = False) -> None:
    table = Table(
        title="前向预测累计成绩（独立已结算市场）" + (" · FINAL" if final else ""),
        show_header=True,
        header_style="bold white",
    )
    for col in ("组别", "已结算", "胜 / 负", "胜率", "距50", "95%下限", "状态"):
        table.add_column(col)
    labels = {
        "A_quant": "A  Quant",
        "B_quant_jev": "B  Quant + Jev",
        "C_quant_jev_market": "C  Quant + Jev + Market",
    }
    for arm in ARMS:
        m = report["arms"][arm]
        n = m["resolved_signal_markets"]
        need = max(0, MIN_RESOLVED_SIGNALS - n)
        status = "[bold green]PASS[/]" if m["gate"]["passed"] else (
            "[yellow]样本不足[/]" if need else "[red]胜率未过[/]"
        )
        table.add_row(
            labels[arm],
            str(n),
            f"{m['wins']} / {m['losses']}",
            _pct(m["win_rate"]),
            str(need),
            _pct(m["wilson_95"]["lower"]),
            status,
        )
    console.print(table)
    cov = report["coverage"]
    console.print(
        f"[dim]checkpoint市场={cov['checkpoint_markets']} | 已结算checkpoint市场={cov['resolved_checkpoint_markets']} | "
        f"Jev成功={cov['jev_success']}/{cov['jev_requested']} | 不生成订单[/]"
    )


async def jev_display_worker(store: FastStore, version: str, console: Console) -> None:
    parameters = store.parameters(version) or {}
    seen = {r["id"] for r in store.observations(version, checkpoints_only=True)}
    terminal = {"ok", "error", "expired", "disabled_auth", "disabled", "busy", "interrupted", "not_eligible_quant"}
    while True:
        for row in store.observations(version, checkpoints_only=True):
            if row["id"] in seen or row.get("jev_status") not in terminal:
                continue
            seen.add(row["id"])
            if row.get("jev_status") == "ok":
                print_jev_panel(console, row, parameters)
            elif row.get("jev_status") not in {"not_eligible_quant"}:
                console.print(Panel(
                    f"市场 {row['slug']} · T-{row['checkpoint']}s\n"
                    f"Jev状态：[bold red]{row.get('jev_status')}[/]\n"
                    "该checkpoint不会伪造Jev结果，Quant记录仍保留。",
                    title="[bold red] JEV 未完成 [/]",
                    border_style="red",
                    expand=False,
                ))
        await asyncio.sleep(.5)


async def dashboard_worker(store: FastStore, version: str, console: Console) -> None:
    last = None
    while True:
        report = build_report(store, version)
        key = tuple(
            (report["arms"][arm]["resolved_signal_markets"],
             report["arms"][arm]["wins"],
             report["arms"][arm]["losses"])
            for arm in ARMS
        )
        if key != last:
            print_scoreboard(console, report)
            last = key
        await asyncio.sleep(30)


def checkpoint(seconds_left: int | None) -> int | None:
    if seconds_left is None:
        return None
    return next(
        (cp for cp in CHECKPOINTS if cp - CHECKPOINT_LATENESS_SECONDS < seconds_left <= cp),
        None,
    )


def quant_direction(p: float | None) -> str | None:
    if p is None or not math.isfinite(p):
        return None
    tolerance = 1e-12
    if p >= QUANT_CONFIDENCE - tolerance:
        return "UP"
    if p <= 1 - QUANT_CONFIDENCE + tolerance:
        return "DOWN"
    return None


def probability_direction(p: float | None) -> str | None:
    if p is None or not math.isfinite(p) or not 0 <= p <= 1 or abs(p - .5) < 1e-12:
        return None
    return "UP" if p > .5 else "DOWN"


def jev_quality(row: dict, parameters: dict) -> bool:
    return bool(
        row.get("jev_status") == "ok"
        and row.get("jev_p") is not None
        and row.get("jev_answerable") is not None
        and row.get("jev_clarity") is not None
        and row["jev_answerable"] >= parameters["min_answerable"]
        and row["jev_clarity"] >= parameters["min_clarity"]
    )


def arm_direction(row: dict, arm: str, parameters: dict) -> str | None:
    qdir = quant_direction(row.get("quant_p"))
    if qdir is None:
        return None
    if arm == "A_quant":
        return qdir
    if not jev_quality(row, parameters):
        return None
    if probability_direction(row.get("jev_p")) != qdir:
        return None
    if arm == "B_quant_jev":
        return qdir
    if arm == "C_quant_jev_market":
        return qdir if probability_direction(row.get("market_p")) == qdir else None
    raise ValueError("unknown_arm")


def first_signals(rows: list[dict], arm: str, parameters: dict) -> list[dict]:
    selected = []
    seen = set()
    for row in sorted(rows, key=lambda r: (r["ts"], r["id"])):
        if row["slug"] in seen or row.get("checkpoint") not in CHECKPOINTS:
            continue
        direction = arm_direction(row, arm, parameters)
        if direction is None:
            continue
        seen.add(row["slug"])
        copy = dict(row)
        copy["signal_direction"] = direction
        selected.append(copy)
    return selected


def wilson_95(wins: int, n: int) -> tuple[float | None, float | None]:
    if n <= 0:
        return None, None
    z = 1.959963984540054
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def arm_metrics(signals: list[dict], resolved_market_count: int) -> dict:
    resolved = [r for r in signals if r.get("up_won") is not None]
    wins = sum((r["signal_direction"] == "UP") == bool(r["up_won"]) for r in resolved)
    n = len(resolved)
    rate = wins / n if n else None
    lo, hi = wilson_95(wins, n)
    checkpoints = Counter(r["checkpoint"] for r in resolved)
    return {
        "signal_markets": len(signals),
        "resolved_signal_markets": n,
        "pending_signal_markets": len(signals) - n,
        "wins": wins,
        "losses": n - wins,
        "win_rate": rate,
        "wilson_95": {"lower": lo, "upper": hi},
        "coverage_of_resolved_checkpoint_markets": n / resolved_market_count if resolved_market_count else None,
        "checkpoint_distribution": {f"T-{cp}": checkpoints[cp] for cp in CHECKPOINTS},
        "gate": {
            "minimum_50_resolved_signals": n >= MIN_RESOLVED_SIGNALS,
            "win_rate_strictly_over_65pct": rate is not None and rate > REQUIRED_WIN_RATE,
            "passed": n >= MIN_RESOLVED_SIGNALS and rate is not None and rate > REQUIRED_WIN_RATE,
        },
    }


def safe_signal(row: dict) -> dict:
    requested = row.get("jev_requested_ts")
    received = row.get("jev_received_ts")
    direction = row["signal_direction"]
    quant_side = side_probability(row.get("quant_p"), direction)
    jev_side = side_probability(row.get("jev_p"), direction)
    market_side = side_probability(row.get("market_p"), direction)
    return {
        "slug": row["slug"],
        "checkpoint": row["checkpoint"],
        "seconds_left": row.get("seconds_left"),
        "observed_ts": row["ts"],
        "direction": row["signal_direction"],
        "quant_p": row.get("quant_p"),
        "jev_p": row.get("jev_p"),
        "jev_answerable": row.get("jev_answerable"),
        "jev_clarity": row.get("jev_clarity"),
        "market_p": row.get("market_p"),
        "quant_side_probability": quant_side,
        "jev_side_probability": jev_side,
        "market_side_probability": market_side,
        "quant_minus_market": quant_side - market_side if quant_side is not None and market_side is not None else None,
        "jev_minus_market": jev_side - market_side if jev_side is not None and market_side is not None else None,
        "jev_latency_seconds": received - requested if requested is not None and received is not None else None,
        "up_won": row.get("up_won"),
    }


def build_report(store: FastStore, version: str) -> dict:
    parameters = store.parameters(version)
    if parameters is None:
        raise ValueError("experiment_not_found")
    rows = store.observations(version)
    cps = [r for r in rows if r.get("checkpoint") in CHECKPOINTS]
    resolved_checkpoint_markets = {
        r["slug"] for r in cps if r.get("quant_p") is not None and r.get("up_won") is not None
    }
    arms = {}
    selections = {}
    for arm in ARMS:
        signals = first_signals(cps, arm, parameters)
        selections[arm] = signals
        arms[arm] = arm_metrics(signals, len(resolved_checkpoint_markets))

    paired_rows = [
        r for r in cps
        if r.get("up_won") is not None
        and r.get("quant_p") is not None
        and r.get("market_p") is not None
        and r.get("jev_status") == "ok"
        and r.get("jev_p") is not None
    ]
    latencies = [
        r["jev_received_ts"] - r["jev_requested_ts"]
        for r in cps
        if r.get("jev_requested_ts") is not None and r.get("jev_received_ts") is not None
        and r["jev_received_ts"] >= r["jev_requested_ts"]
    ]
    latencies.sort()
    p95_index = min(len(latencies) - 1, math.ceil(.95 * len(latencies)) - 1) if latencies else None

    qualified = [arm for arm in ARMS if arms[arm]["gate"]["passed"]]
    return {
        "format": REPORT_FORMAT,
        "experiment_revision": REVISION,
        "paper_only": True,
        "orders_created": 0,
        "purpose": "Forward prediction validation before returning to execution research.",
        "protocol": {
            "checkpoints": list(CHECKPOINTS),
            "checkpoint_lateness_seconds": CHECKPOINT_LATENESS_SECONDS,
            "interval_seconds": parameters["interval_seconds"],
            "quant_confidence": QUANT_CONFIDENCE,
            "jev_min_answerable": parameters["min_answerable"],
            "jev_min_clarity": parameters["min_clarity"],
            "minimum_resolved_signal_markets": MIN_RESOLVED_SIGNALS,
            "required_win_rate": f">{REQUIRED_WIN_RATE:.0%}",
            "arm_definitions": {
                "A_quant": "First checkpoint where Quant predicts UP>=0.92 or DOWN>=0.92.",
                "B_quant_jev": "A plus successful Jev quality gate and same directional side as Quant.",
                "C_quant_jev_market": "B plus Polymarket midpoint directional agreement.",
            },
        },
        "coverage": {
            "observations": len(rows),
            "checkpoint_observations": len(cps),
            "checkpoint_markets": len({r["slug"] for r in cps}),
            "resolved_checkpoint_markets": len(resolved_checkpoint_markets),
            "quant_high_confidence_checkpoints": sum(quant_direction(r.get("quant_p")) is not None for r in cps),
            "jev_requested": sum(r.get("jev_status") not in {"not_requested", "not_eligible_quant"} for r in cps),
            "jev_success": sum(r.get("jev_status") == "ok" for r in cps),
        },
        "jev_latency_seconds": {
            "n": len(latencies),
            "median": (latencies[len(latencies)//2] if latencies else None),
            "p95": (latencies[p95_index] if p95_index is not None else None),
            "max": (latencies[-1] if latencies else None),
        },
        "checkpoint_probability_diagnostics": {
            "note": "Checkpoint rows are correlated within a market; these are diagnostics, not independent win-rate samples.",
            "quant": probability_metrics(paired_rows, "quant_p"),
            "jev": probability_metrics(paired_rows, "jev_p"),
            "market": probability_metrics(paired_rows, "market_p"),
        },
        "arms": arms,
        "edge_diagnostics": {
            arm: {
                "quant_minus_market": edge_metrics(selections[arm], "quant_p"),
                "jev_minus_market": edge_metrics(selections[arm], "jev_p"),
            }
            for arm in ARMS
        },
        "qualified_arms": qualified,
        "prediction_gate_passed": bool(qualified),
        "signals": {arm: [safe_signal(r) for r in selections[arm]] for arm in ARMS},
        "limitations": [
            "Each arm's headline win rate uses at most one forward signal per independent 5-minute market.",
            "Jev is requested only for high-confidence Quant checkpoints, so this experiment tests Jev as a confirmation filter.",
            "No order placement, queue model, fee, spread capture, slippage or execution PnL is part of the headline prediction gate.",
            "A >65% observed win rate on >=50 resolved signals is a project threshold, not a guarantee of future profitability.",
            "B/C can raise win rate by filtering markets while reducing coverage; both metrics must be read together.",
        ],
    }


class ForwardRunner(FastRunner):
    async def tick(self, *, budget_seconds: float = 8) -> None:
        started = time.monotonic()
        gap = None if self.previous_started is None else started - self.previous_started
        self.previous_started = started
        from .fast_runner import current_slug
        from .network import ReadUnavailable

        slug = current_slug()
        cand = snapshot = None
        quant_p = None
        status, reason = "unavailable", ""
        try:
            async with asyncio.timeout(budget_seconds):
                cand = await self.read(slug)
            snapshot = local_snapshot(cand, self.s, self.store)
            if not snapshot.trade_ready:
                reason = "prediction_inputs_not_ready"
            else:
                quant_p = quantitative_up_probability(snapshot)
                if quant_p is None:
                    reason = "quant_probability_unavailable"
                else:
                    status = "prediction_ready"
                    reason = "checkpoint_prediction_candidate"
        except (ReadUnavailable, TimeoutError) as exc:
            reason = str(exc) or "read_timeout"

        observed_ts = snapshot.captured_at.timestamp() if snapshot else time.time()
        payload = snapshot_payload(cand, snapshot) if snapshot is not None else {}
        payload.update(tick_gap_seconds=gap, read_compute_seconds=time.monotonic() - started)
        cp = checkpoint(snapshot.seconds_left) if snapshot and quant_p is not None else None
        observation_id, recorded_cp = self.store.record(
            version=self.version,
            session=self.session,
            slug=slug,
            condition_id=cand.condition_id if cand else None,
            ts=observed_ts,
            seconds_left=snapshot.seconds_left if snapshot else None,
            checkpoint=cp,
            quant_p=quant_p,
            market_p=cand.book.midpoint if cand else None,
            yes_ask=cand.book.yes_ask if cand else None,
            no_ask=cand.book.no_ask if cand else None,
            status=status,
            reason=reason,
            payload=payload,
        )
        if recorded_cp is None:
            return

        qdir = quant_direction(quant_p)
        display_row = {
            "slug": slug,
            "checkpoint": recorded_cp,
            "quant_p": quant_p,
            "market_p": cand.book.midpoint,
        }
        print_checkpoint_panel(self.console, display_row)
        if qdir is None:
            self.store.mark_jev(observation_id, "not_eligible_quant")
        else:
            self.shadow.offer(observation_id, cand, self.s, snapshot)


def experiment_parameters(s, interval: float) -> dict:
    return {
        "protocol_revision": REVISION,
        "interval_seconds": float(interval),
        "timeframe": "5m",
        "checkpoints": list(CHECKPOINTS),
        "checkpoint_lateness_seconds": CHECKPOINT_LATENESS_SECONDS,
        "quant_confidence": QUANT_CONFIDENCE,
        "jev_mode": "high_confidence_checkpoint_confirmation",
        "jev_model": s.jev_model,
        "jev_timeout_seconds": s.jev_timeout_seconds,
        "jev_max_retries": s.jev_max_retries,
        "min_answerable": s.min_answerable,
        "min_clarity": s.min_clarity,
        "short_term_min_history_seconds": s.short_term_min_history_seconds,
        "short_term_max_sample_age_seconds": s.short_term_max_sample_age_seconds,
        "max_spread": s.max_spread,
        "paper_only": True,
        "orders_enabled": False,
    }


async def run_forward(s, console: Console, *, version: str, interval: float, seconds: int) -> None:
    if not s.dry_run or s.allowed_timeframes != "5m":
        raise ValueError("forward experiment requires BTC 5m dry-run settings")
    if not s.typesafe_api_key:
        raise ValueError("TYPESAFE_API_KEY is required for the Jev forward experiment")
    next_deadline(0, 0, interval)

    store = FastStore(s.db_path)
    tasks = []
    try:
        store.ensure_experiment(version, experiment_parameters(s, interval))
        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            settlement = await stack.enter_async_context(AsyncPublicClient())
            jev = await stack.enter_async_context(JevClient(
                s.typesafe_api_key,
                model=s.jev_model,
                base_url=s.typesafe_base_url,
                timeout=s.jev_timeout_seconds,
                max_retries=s.jev_max_retries,
            ))
            shadow = JevShadow(jev, store, console)
            runner = ForwardRunner(
                s, store, public, shadow, console,
                version=version, session=uuid.uuid4().hex,
            )
            tasks = [
                asyncio.create_task(watch_chainlink_anchors(s, store), name="chainlink"),
                asyncio.create_task(shadow.work(), name="jev-shadow"),
                asyncio.create_task(settlement_worker(settlement, s, store, version), name="settlement"),
                asyncio.create_task(jev_display_worker(store, version, console), name="jev-display"),
                asyncio.create_task(dashboard_worker(store, version, console), name="dashboard"),
            ]
            console.print(Panel(
                "[bold]BTC 5m 前向预测验证[/]\n"
                f"Checkpoint: {CHECKPOINTS}\n"
                "Quant高置信阈值: 92%\n"
                "Jev: 仅高置信checkpoint异步确认\n"
                "[bold green]无模拟订单 · 无真实账户 · 只验证预测[/]",
                title=f"[bold cyan] {REVISION} [/]",
                border_style="cyan",
                expand=False,
            ))
            clock = asyncio.get_running_loop().time
            deadline = clock()
            stop_at = clock() + seconds
            try:
                while clock() < stop_at:
                    for task in tasks:
                        if task.done():
                            task.result()
                            raise RuntimeError(f"task_stopped:{task.get_name()}")
                    await runner.tick(budget_seconds=min(8.0, interval * .8))
                    deadline, skipped = next_deadline(deadline, clock(), interval)
                    if skipped:
                        console.print(f"跳过{skipped}个超时节拍；不追补旧checkpoint", markup=False)
                    await asyncio.sleep(max(0.0, min(deadline, stop_at) - clock()))
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                while not shadow.queue.empty():
                    observation_id, _, _ = shadow.queue.get_nowait()
                    store.mark_jev(observation_id, "interrupted")
                    shadow.queue.task_done()
    finally:
        store.close()


async def settle_all(s, db: Path, version: str) -> int:
    store = FastStore(db)
    try:
        async with AsyncPublicClient() as client:
            total = 0
            while True:
                n = await settle_some(client, s, store, version, limit=500)
                total += n
                if n == 0:
                    return total
    finally:
        store.close()


def write_report(report: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))



def resolve_run_paths(db_arg: Path | None, resume_db: Path | None, out_arg: Path | None,
                      stamp: str) -> tuple[Path, Path, bool]:
    if db_arg is not None and resume_db is not None:
        raise ValueError("cannot_use_db_and_resume_db_together")
    if resume_db is not None:
        db = Path(resume_db)
        if not db.is_file():
            raise ValueError("resume_database_not_found")
        out = run_output_path(out_arg, f"v7_checkpoint_jev_forward_resume_{stamp}.json.gz")
        if out.exists():
            raise FileExistsError("report_already_exists")
        return db, out, True

    db = run_output_path(db_arg, f"jevymarket.forward-jev_{stamp}.db")
    out = run_output_path(out_arg, f"v7_checkpoint_jev_forward_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("experiment_output_already_exists")
    return db, out, False


def main(argv=None):
    parser = argparse.ArgumentParser(description="BTC5m checkpoint + Jev 前向预测实验；无订单")
    parser.add_argument("--seconds", type=int, default=21600)
    parser.add_argument("--loop", type=float, default=INTERVAL_SECONDS)
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument("--db", type=Path, help="新实验数据库路径；默认runs/")
    paths.add_argument("--resume-db", type=Path, help="续跑已有v7数据库并累计统计")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if not 1800 <= args.seconds <= 86400:
        parser.error("seconds必须在1800到86400之间")
    if args.loop != INTERVAL_SECONDS:
        parser.error("本实验固定10秒节拍，避免同时改变采样频率")

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    try:
        db, out, resumed = resolve_run_paths(args.db, args.resume_db, args.out, stamp)
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))

    s = fast_settings().model_copy(update={"db_path": str(db), "strategy_version": REVISION})
    console = Console()
    try:
        with single_instance(str(db)):
            asyncio.run(run_forward(s, console, version=REVISION, interval=args.loop, seconds=args.seconds))
    except KeyboardInterrupt:
        console.print("已停止；前向checkpoint记录已保留，开始导出当前结果。", markup=False)

    settled = asyncio.run(settle_all(s, db, REVISION))
    store = FastStore(db)
    try:
        report = build_report(store, REVISION)
    finally:
        store.close()
    report["settlements_added_at_export"] = settled
    report["resumed_existing_database"] = resumed
    write_report(report, out)
    print_scoreboard(console, report, final=True)
    console.print(Panel(
        f"报告：[bold]{out.resolve()}[/]\n"
        f"数据库：[bold]{db.resolve()}[/]\n"
        f"模式：{'续跑累计' if resumed else '新实验'}",
        title="[bold green] 已导出 [/]",
        border_style="green",
        expand=False,
    ))


if __name__ == "__main__":
    main()
