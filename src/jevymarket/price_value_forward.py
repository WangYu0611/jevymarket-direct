"""V8 BTC5m forward price-value experiment.

Prediction quality is already a separate gate. V8 asks whether a high-confidence
signal remains economically attractive at an observable taker price.

No authenticated Polymarket client and no real orders are used.
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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from polymarket import AsyncPublicClient
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .fast_cli import single_instance
from .fast_runner import FastRunner, settle_some, settlement_worker
from .fast_store import FastStore
from .fast_strategy import fast_settings, local_snapshot, next_deadline, snapshot_payload
from .jev import JevClient, JevError
from .market_data import watch_chainlink_anchors
from .markets import build_state
from .network import ReadUnavailable
from .run_paths import run_output_path
from .signal import Book, JevView, ask_jev, quantitative_up_probability

REVISION = "v8-price-value-forward-r1"
REPORT_FORMAT = "v8-price-value-forward-report-r1"
CHECKPOINTS = (120, 90, 60, 45)
CHECKPOINT_LATENESS_SECONDS = 10
INTERVAL_SECONDS = 10.0
QUANT_CONFIDENCE = 0.92
MIN_RESOLVED_TRADES = 50
REQUIRED_WIN_RATE = 0.65
CRYPTO_TAKER_FEE_RATE = 0.07
DEFAULT_SECONDS = 43_200
ARMS = ("A_quant_taker", "B_quant_jev_taker", "C_jev_value")

V8_SCHEMA = """
CREATE TABLE IF NOT EXISTS v8_value_trades (
    id INTEGER PRIMARY KEY,
    version TEXT NOT NULL,
    arm TEXT NOT NULL,
    slug TEXT NOT NULL,
    checkpoint INTEGER NOT NULL,
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
    UNIQUE(version, arm, slug)
);
CREATE INDEX IF NOT EXISTS idx_v8_trades
ON v8_value_trades(version, arm, decision_ts);
"""


@dataclass(frozen=True)
class ValueDecision:
    direction: str
    probability: float
    bid: float
    ask: float
    spread: float
    depth_5c_usd: float
    tick_size: float
    min_order_size: float
    edge: float
    size: float
    notional_usd: float
    taker_fee_usd_est: float


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


def side_probability(p_up: float, direction: str) -> float:
    return p_up if direction == "UP" else 1 - p_up


def side_book(book: Book, direction: str) -> tuple[float | None, float | None, float | None]:
    if direction == "UP":
        return book.yes_bid, book.yes_ask, book.yes_ask_depth_5c_usd
    if direction == "DOWN":
        return book.no_bid, book.no_ask, book.no_ask_depth_5c_usd
    raise ValueError("invalid_direction")


def taker_fee_usd(shares: float, price: float, rate: float = CRYPTO_TAKER_FEE_RATE) -> float:
    if not all(math.isfinite(x) for x in (shares, price, rate)) or shares < 0 or not 0 < price < 1 or rate < 0:
        raise ValueError("invalid_fee_inputs")
    raw = Decimal(str(shares)) * Decimal(str(rate)) * Decimal(str(price)) * (Decimal(1) - Decimal(str(price)))
    return float(raw.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP))


def value_decision(
    p_up: float,
    direction: str,
    book: Book,
    settings,
    *,
    fee_rate: float = CRYPTO_TAKER_FEE_RATE,
) -> tuple[ValueDecision | None, str]:
    if direction not in {"UP", "DOWN"}:
        return None, "no_direction"
    if not math.isfinite(p_up) or not 0 <= p_up <= 1:
        return None, "invalid_probability"

    bid, ask, depth = side_book(book, direction)
    if bid is None or ask is None or depth is None:
        return None, "incomplete_side_book"
    if not all(math.isfinite(x) for x in (bid, ask, depth, book.tick_size, book.min_order_size)):
        return None, "invalid_book_numeric"
    if not 0 < bid < ask < 1:
        return None, "crossed_or_invalid_book"
    spread = ask - bid
    if spread > settings.max_spread + 1e-12:
        return None, "spread_too_wide"
    if not settings.min_trade_price <= ask <= settings.max_trade_price:
        return None, "ask_outside_price_band"

    probability = side_probability(p_up, direction)
    edge = probability - ask
    if edge < settings.min_edge - 1e-12:
        return None, "edge_below_threshold"

    size = math.floor(settings.max_usd_per_trade / ask * 100) / 100
    if size < book.min_order_size:
        return None, "minimum_size_exceeds_budget"
    notional = round(size * ask, 6)
    if notional <= 0 or notional > settings.max_usd_per_trade + 1e-9:
        return None, "notional_outside_budget"
    if depth + 1e-9 < notional:
        return None, "insufficient_5c_ask_depth"

    fee = taker_fee_usd(size, ask, fee_rate)
    return ValueDecision(
        direction=direction,
        probability=probability,
        bid=bid,
        ask=ask,
        spread=spread,
        depth_5c_usd=depth,
        tick_size=book.tick_size,
        min_order_size=book.min_order_size,
        edge=edge,
        size=size,
        notional_usd=notional,
        taker_fee_usd_est=fee,
    ), "value_ready"


def jev_quality(view: JevView, settings) -> bool:
    return view.answerable >= settings.min_answerable and view.clarity >= settings.min_clarity


class V8Store(FastStore):
    def __init__(self, path):
        super().__init__(path)
        self.conn.executescript(V8_SCHEMA)

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
        jev: JevView | None,
        market_mid: float | None,
        decision: ValueDecision,
        fee_rate: float,
    ) -> bool:
        if arm not in ARMS:
            raise ValueError("unknown_v8_arm")
        values = (
            version, arm, slug, checkpoint_value, observation_id, signal_ts, decision_ts,
            decision.direction, quant_p, jev.p_yes if jev else None,
            jev.answerable if jev else None, jev.clarity if jev else None, market_mid,
            decision.bid, decision.ask, decision.spread, decision.depth_5c_usd,
            decision.tick_size, decision.min_order_size, decision.probability, decision.edge,
            decision.size, decision.notional_usd, fee_rate, decision.taker_fee_usd_est,
        )
        with self.conn:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO v8_value_trades
                (version,arm,slug,checkpoint,observation_id,signal_ts,decision_ts,direction,
                 quant_p,jev_p,jev_answerable,jev_clarity,market_mid,bid,ask,spread,
                 depth_5c_usd,tick_size,min_order_size,edge_probability,edge,size,
                 notional_usd,taker_fee_rate,taker_fee_usd_est)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
        return cur.rowcount == 1

    def value_trades(self, version: str, arm: str | None = None) -> list[dict]:
        query = """SELECT t.*, r.up_won FROM v8_value_trades t
                   LEFT JOIN market_results r ON r.slug=t.slug
                   WHERE t.version=?"""
        args: list[object] = [version]
        if arm is not None:
            query += " AND t.arm=?"
            args.append(arm)
        query += " ORDER BY t.decision_ts,t.id"
        return [dict(r) for r in self.conn.execute(query, args).fetchall()]


def trade_pnl(row: dict, *, price_override: float | None = None) -> tuple[float, float, float]:
    price = row["ask"] if price_override is None else price_override
    if not 0 < price < 1:
        raise ValueError("invalid_stress_price")
    size = float(row["size"])
    fee = taker_fee_usd(size, price, float(row["taker_fee_rate"]))
    won = (row["direction"] == "UP") == bool(row["up_won"])
    gross = size * ((1.0 if won else 0.0) - price)
    return gross, fee, gross - fee


def stress_summary(rows: list[dict], mode: str) -> dict:
    settled = [r for r in rows if r.get("up_won") is not None]
    pnls = []
    for row in settled:
        if mode == "actual":
            price = row["ask"]
        elif mode == "plus_1tick":
            price = min(.999999, row["ask"] + row["tick_size"])
        elif mode == "plus_1cent":
            price = min(.999999, row["ask"] + .01)
        else:
            raise ValueError("unknown_stress_mode")
        _, _, net = trade_pnl(row, price_override=price)
        pnls.append(net)
    return {
        "mode": mode,
        "settled": len(settled),
        "net_pnl_estimated": sum(pnls),
        "positive": sum(x > 0 for x in pnls),
    }


def arm_metrics(rows: list[dict]) -> dict:
    settled = [r for r in rows if r.get("up_won") is not None]
    wins = sum((r["direction"] == "UP") == bool(r["up_won"]) for r in settled)
    contributions = []
    gross = fees = stake = 0.0
    for row in settled:
        g, f, n = trade_pnl(row)
        gross += g
        fees += f
        stake += row["notional_usd"]
        contributions.append(n)
    net = sum(contributions)
    positives = sorted((x for x in contributions if x > 0), reverse=True)
    n = len(settled)
    win_rate = wins / n if n else None
    edges = [r["edge"] for r in rows]
    asks = [r["ask"] for r in rows]
    checkpoints = Counter(r["checkpoint"] for r in settled)
    stress_tick = stress_summary(rows, "plus_1tick")
    stress_cent = stress_summary(rows, "plus_1cent")
    gate_checks = {
        "minimum_50_settled_trades": n >= MIN_RESOLVED_TRADES,
        "win_rate_over_65pct": win_rate is not None and win_rate > REQUIRED_WIN_RATE,
        "positive_net_pnl_estimated": net > 0,
        "positive_without_top3_wins": net - sum(positives[:3]) > 0,
        "positive_after_1tick_stress": stress_tick["net_pnl_estimated"] > 0,
    }
    return {
        "trades": len(rows),
        "settled": n,
        "pending": len(rows) - n,
        "wins": wins,
        "losses": n - wins,
        "win_rate": win_rate,
        "stake_usd": stake,
        "gross_pnl_before_fee": gross,
        "estimated_taker_fee_usd": fees,
        "net_pnl_estimated": net,
        "net_roi_estimated": net / stake if stake else None,
        "net_pnl_minus_top3_positive_contributions": net - sum(positives[:3]),
        "ask_mean": sum(asks) / len(asks) if asks else None,
        "ask_median": statistics.median(asks) if asks else None,
        "edge_mean": sum(edges) / len(edges) if edges else None,
        "edge_median": statistics.median(edges) if edges else None,
        "checkpoint_distribution": {f"T-{cp}": checkpoints[cp] for cp in CHECKPOINTS},
        "stress_plus_1tick": stress_tick,
        "stress_plus_1cent": stress_cent,
        "gate": {"checks": gate_checks, "passed": all(gate_checks.values())},
    }


def safe_trade(row: dict) -> dict:
    return {k: row.get(k) for k in (
        "arm", "slug", "checkpoint", "signal_ts", "decision_ts", "direction",
        "quant_p", "jev_p", "jev_answerable", "jev_clarity", "market_mid",
        "bid", "ask", "spread", "depth_5c_usd", "tick_size", "min_order_size",
        "edge_probability", "edge", "size", "notional_usd",
        "taker_fee_rate", "taker_fee_usd_est", "up_won",
    )}


def build_report(store: V8Store, version: str) -> dict:
    parameters = store.parameters(version)
    if parameters is None:
        raise ValueError("experiment_not_found")
    rows = store.observations(version)
    trades = {arm: store.value_trades(version, arm) for arm in ARMS}
    arms = {arm: arm_metrics(trades[arm]) for arm in ARMS}
    qualified = [arm for arm in ARMS if arms[arm]["gate"]["passed"]]
    return {
        "format": REPORT_FORMAT,
        "experiment_revision": REVISION,
        "paper_only": True,
        "live_trading_enabled": False,
        "execution_model": "Immediate taker-like crossing at observed best ask; no authenticated fill claim.",
        "protocol": parameters,
        "coverage": {
            "observations": len(rows),
            "checkpoint_observations": sum(r.get("checkpoint") in CHECKPOINTS for r in rows),
            "checkpoint_markets": len({r["slug"] for r in rows if r.get("checkpoint") in CHECKPOINTS}),
            "jev_requested": sum(r.get("jev_status") not in {"not_requested", "not_eligible_quant"} for r in rows),
            "jev_success": sum(r.get("jev_status") == "ok" for r in rows),
        },
        "arms": arms,
        "qualified_arms": qualified,
        "price_value_gate_passed": bool(qualified),
        "trades": {arm: [safe_trade(r) for r in trades[arm]] for arm in ARMS},
        "limitations": [
            "Best ask is public order-book evidence, not proof our hypothetical order would fill at exactly that price.",
            "5-cent ask-side depth must cover the simulated notional, but exact top-level size and multi-level VWAP are not reconstructed here.",
            "Crypto taker fee uses a fixed 0.07 protocol-rate assumption and the published C*rate*p*(1-p) formula; taker rebates are excluded.",
            "One-tick and one-cent worse-price stress are diagnostics, not observed fills.",
            "All thresholds were fixed before this new V8 forward sample; V7 outcomes are not reused as V8 validation data.",
        ],
    }


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.1f}%"


def print_scoreboard(console: Console, report: dict, *, final: bool = False) -> None:
    table = Table(
        title="V8 价格价值累计成绩" + (" · FINAL" if final else ""),
        header_style="bold white",
    )
    for col in ("组别", "交易/结算", "胜/负", "胜率", "净PnL", "净ROI", "距50", "门槛"):
        table.add_column(col)
    labels = {
        "A_quant_taker": "A Quant→ASK",
        "B_quant_jev_taker": "B Quant+Jev→响应后ASK",
        "C_jev_value": "C Jev自身也有8% edge",
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


def print_value_panel(console: Console, *, arm: str, slug: str, cp: int, decision: ValueDecision | None,
                      reason: str, quant_p: float, jev: JevView | None = None, inserted: bool = False) -> None:
    title = {
        "A_quant_taker": "A · QUANT VALUE",
        "B_quant_jev_taker": "B · JEV CONFIRMED VALUE",
        "C_jev_value": "C · JEV OWN VALUE",
    }[arm]
    if decision is None:
        body = (
            f"{slug} · T-{cp}s\n"
            f"Quant P(Up)={quant_p:.3f}\n"
            f"结果：[yellow]SKIP[/] · {reason}"
        )
        console.print(Panel(body, title=title, border_style="yellow", expand=False))
        return
    jev_text = "" if jev is None else (
        f"\nJev P(Up)={jev.p_yes:.3f} | answerable={jev.answerable:.2f} | clarity={jev.clarity}"
    )
    body = (
        f"{slug} · T-{cp}s\n"
        f"方向：[bold {'green' if decision.direction == 'UP' else 'red'}]{decision.direction}[/]\n"
        f"模型概率={decision.probability:.3f} | ASK={decision.ask:.3f} | "
        f"EDGE=[bold cyan]{decision.edge * 100:+.2f} pp[/]\n"
        f"spread={decision.spread:.3f} | 5c深度=USD {decision.depth_5c_usd:.2f}\n"
        f"模拟 USD {decision.notional_usd:.2f} | fee≈USD {decision.taker_fee_usd_est:.5f}"
        f"{jev_text}\n"
        f"状态：{'[bold green]TRADE RECORDED[/]' if inserted else '[dim]该市场此组已有第一笔[/]'}"
    )
    console.print(Panel(body, title=title, border_style="green" if inserted else "cyan", expand=False))


class V8JevWorker:
    def __init__(self, client: JevClient, store: V8Store, console: Console):
        self.client, self.store, self.console = client, store, console
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self.on_result = None
        self.disabled = False

    def offer(self, observation_id: int, cand, settings, snapshot, cp: int, direction: str) -> None:
        if self.disabled:
            self.store.mark_jev(observation_id, "disabled")
            return
        state = build_state(cand, settings, brief=None)
        state.pop("market_implied_probability_primary", None)
        state["short_term_market_data"] = snapshot.to_state()
        state["microstructure"] = cand.book.microstructure_state()
        end = int(cand.slug.rsplit("-", 1)[1]) + 300
        try:
            self.queue.put_nowait((observation_id, cand.slug, state, end, cp, direction))
        except asyncio.QueueFull:
            self.store.mark_jev(observation_id, "busy")
        else:
            self.store.mark_jev(observation_id, "queued")

    async def work(self) -> None:
        while True:
            observation_id, slug, state, end, cp, direction = await self.queue.get()
            requested = time.time()
            try:
                budget = min(20.0, end - requested - .5)
                if budget <= 0:
                    self.store.mark_jev(observation_id, "expired")
                    continue
                self.store.mark_jev(observation_id, "running", requested_ts=requested)
                try:
                    async with asyncio.timeout(budget):
                        view = await ask_jev(self.client, state)
                    received = time.time()
                    if received >= end:
                        self.store.mark_jev(observation_id, "expired", requested_ts=requested)
                        continue
                    self.store.complete_jev(observation_id, view, received)
                    if self.on_result is not None:
                        await self.on_result(observation_id, slug, cp, direction, view, received)
                except (JevError, TimeoutError, ValidationError, ValueError, KeyError) as exc:
                    if isinstance(exc, JevError) and exc.status in (401, 402, 403):
                        self.disabled = True
                        status = "disabled_auth"
                    else:
                        status = "error"
                    self.store.mark_jev(observation_id, status, requested_ts=requested)
                    self.console.print(Panel(
                        f"{slug} · T-{cp}s\nJev状态：[bold red]{status}[/] · {type(exc).__name__}",
                        title="[bold red] JEV ERROR [/]",
                        border_style="red",
                        expand=False,
                    ))
            finally:
                self.queue.task_done()


class V8Runner(FastRunner):
    def __init__(self, *args, fee_rate: float, **kwargs):
        super().__init__(*args, **kwargs)
        self.fee_rate = fee_rate

    async def on_jev_result(self, observation_id: int, slug: str, cp: int, original_direction: str,
                            view: JevView, received: float) -> None:
        if not jev_quality(view, self.s):
            self.console.print(Panel(
                f"{slug} · T-{cp}s\nJev质量不足：answerable={view.answerable:.2f}, clarity={view.clarity}",
                title="[yellow] JEV FILTERED [/]",
                border_style="yellow",
                expand=False,
            ))
            return
        if probability_direction(view.p_yes) != original_direction:
            self.console.print(Panel(
                f"{slug} · T-{cp}s\nQuant={original_direction}，Jev={probability_direction(view.p_yes)}",
                title="[yellow] JEV DIRECTION DISAGREES [/]",
                border_style="yellow",
                expand=False,
            ))
            return
        try:
            cand = await self.read(slug)
            fresh = local_snapshot(cand, self.s, self.store)
            fresh_quant = quantitative_up_probability(fresh)
        except (ReadUnavailable, ValueError, TimeoutError):
            return
        fresh_direction = quant_direction(fresh_quant)
        if fresh_quant is None or fresh_direction != original_direction:
            return

        decision_b, reason_b = value_decision(
            fresh_quant, fresh_direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        inserted_b = False
        if decision_b is not None:
            inserted_b = self.store.record_value_trade(
                version=self.version, arm="B_quant_jev_taker", slug=slug,
                checkpoint_value=cp, observation_id=observation_id,
                signal_ts=fresh.captured_at.timestamp(), decision_ts=received,
                quant_p=fresh_quant, jev=view, market_mid=cand.book.midpoint,
                decision=decision_b, fee_rate=self.fee_rate,
            )
        print_value_panel(
            self.console, arm="B_quant_jev_taker", slug=slug, cp=cp,
            decision=decision_b, reason=reason_b, quant_p=fresh_quant, jev=view, inserted=inserted_b,
        )

        decision_c, reason_c = value_decision(
            view.p_yes, fresh_direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        inserted_c = False
        if decision_b is not None and decision_c is not None:
            inserted_c = self.store.record_value_trade(
                version=self.version, arm="C_jev_value", slug=slug,
                checkpoint_value=cp, observation_id=observation_id,
                signal_ts=fresh.captured_at.timestamp(), decision_ts=received,
                quant_p=fresh_quant, jev=view, market_mid=cand.book.midpoint,
                decision=decision_c, fee_rate=self.fee_rate,
            )
        elif decision_b is None and decision_c is not None:
            reason_c = "quant_value_filter_failed"
            decision_c = None
        print_value_panel(
            self.console, arm="C_jev_value", slug=slug, cp=cp,
            decision=decision_c, reason=reason_c, quant_p=fresh_quant, jev=view, inserted=inserted_c,
        )

    async def tick(self, *, budget_seconds: float = 8) -> None:
        from .fast_runner import current_slug

        started = time.monotonic()
        gap = None if self.previous_started is None else started - self.previous_started
        self.previous_started = started
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
            version=self.version, session=self.session, slug=slug,
            condition_id=cand.condition_id if cand else None, ts=observed_ts,
            seconds_left=snapshot.seconds_left if snapshot else None,
            checkpoint=cp, quant_p=quant_p, market_p=cand.book.midpoint if cand else None,
            yes_ask=cand.book.yes_ask if cand else None, no_ask=cand.book.no_ask if cand else None,
            status=status, reason=reason, payload=payload,
        )
        if recorded_cp is None:
            return

        direction = quant_direction(quant_p)
        if direction is None:
            self.store.mark_jev(observation_id, "not_eligible_quant")
            self.console.print(Panel(
                f"{slug} · T-{recorded_cp}s\nQuant P(Up)={quant_p:.3f}\n"
                "未达到92%/8%高置信阈值，不进入价格价值判断。",
                title="[dim] V8 CHECKPOINT · NO SIGNAL [/]",
                border_style="white",
                expand=False,
            ))
            return

        decision_a, reason_a = value_decision(
            quant_p, direction, cand.book, self.s, fee_rate=self.fee_rate
        )
        inserted_a = False
        if decision_a is not None:
            inserted_a = self.store.record_value_trade(
                version=self.version, arm="A_quant_taker", slug=slug,
                checkpoint_value=recorded_cp, observation_id=observation_id,
                signal_ts=observed_ts, decision_ts=time.time(), quant_p=quant_p,
                jev=None, market_mid=cand.book.midpoint, decision=decision_a,
                fee_rate=self.fee_rate,
            )
        print_value_panel(
            self.console, arm="A_quant_taker", slug=slug, cp=recorded_cp,
            decision=decision_a, reason=reason_a, quant_p=quant_p, inserted=inserted_a,
        )
        self.shadow.offer(observation_id, cand, self.s, snapshot, recorded_cp, direction)


def experiment_parameters(settings, interval: float, fee_rate: float) -> dict:
    return {
        "protocol_revision": REVISION,
        "timeframe": "5m",
        "checkpoints": list(CHECKPOINTS),
        "checkpoint_lateness_seconds": CHECKPOINT_LATENESS_SECONDS,
        "interval_seconds": interval,
        "quant_confidence": QUANT_CONFIDENCE,
        "min_edge": settings.min_edge,
        "min_trade_price": settings.min_trade_price,
        "max_trade_price": settings.max_trade_price,
        "max_spread": settings.max_spread,
        "max_usd_per_trade": settings.max_usd_per_trade,
        "jev_min_answerable": settings.min_answerable,
        "jev_min_clarity": settings.min_clarity,
        "taker_fee_rate_assumption": fee_rate,
        "taker_fee_formula": "shares * rate * price * (1-price), rounded to 5 decimals",
        "taker_rebate_included": False,
        "minimum_resolved_trades": MIN_RESOLVED_TRADES,
        "required_win_rate": f">{REQUIRED_WIN_RATE:.0%}",
        "paper_only": True,
        "authenticated_client": False,
        "arms": {
            "A_quant_taker": "Quant high-confidence + fixed value rules at checkpoint best ask.",
            "B_quant_jev_taker": "Jev confirms; refresh book and Quant after Jev, then same value rules at response-time best ask.",
            "C_jev_value": "B must qualify and Jev's own directional probability must also beat response-time ask by the same fixed edge.",
        },
    }


async def run_v8(settings, console: Console, *, version: str, interval: float,
                 seconds: int, fee_rate: float) -> None:
    if not settings.dry_run or settings.allowed_timeframes != "5m":
        raise ValueError("V8 requires BTC 5m dry-run settings")
    if not settings.typesafe_api_key:
        raise ValueError("TYPESAFE_API_KEY is required for V8")
    next_deadline(0, 0, interval)

    store = V8Store(settings.db_path)
    tasks = []
    try:
        store.ensure_experiment(version, experiment_parameters(settings, interval, fee_rate))
        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            settlement = await stack.enter_async_context(AsyncPublicClient())
            jev = await stack.enter_async_context(JevClient(
                settings.typesafe_api_key, model=settings.jev_model,
                base_url=settings.typesafe_base_url, timeout=settings.jev_timeout_seconds,
                max_retries=settings.jev_max_retries,
            ))
            shadow = V8JevWorker(jev, store, console)
            runner = V8Runner(
                settings, store, public, shadow, console,
                version=version, session=uuid.uuid4().hex, fee_rate=fee_rate,
            )
            shadow.on_result = runner.on_jev_result
            tasks = [
                asyncio.create_task(watch_chainlink_anchors(settings, store), name="chainlink"),
                asyncio.create_task(shadow.work(), name="jev-v8"),
                asyncio.create_task(settlement_worker(settlement, settings, store, version), name="settlement"),
            ]
            console.print(Panel(
                "[bold]BTC 5m · PRICE VALUE FORWARD[/]\n"
                "A = Quant当下真实ASK\n"
                "B = Jev返回后重新读盘口/Quant再用真实ASK\n"
                "C = B + Jev自己也有8% edge\n"
                f"Quant阈值=92% | 固定edge={settings.min_edge:.0%} | 价格≤{settings.max_trade_price:.2f}\n"
                f"单笔≤USD {settings.max_usd_per_trade:.2f} | crypto taker fee rate={fee_rate:.3f}\n"
                "[bold green]纯模拟 · 无私钥 · 无真实订单[/]",
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
                    await runner.tick(budget_seconds=min(8.0, interval * .8))
                    if clock() >= next_board:
                        print_scoreboard(console, build_report(store, version))
                        next_board = clock() + 60
                    deadline, skipped = next_deadline(deadline, clock(), interval)
                    if skipped:
                        console.print(f"[yellow]跳过{skipped}个超时节拍；不追补旧checkpoint[/]")
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
    store = V8Store(db)
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


def write_report(report: dict, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def resolve_paths(db_arg: Path | None, resume_db: Path | None, out_arg: Path | None,
                  stamp: str) -> tuple[Path, Path, bool]:
    if db_arg is not None and resume_db is not None:
        raise ValueError("cannot_use_db_and_resume_db_together")
    if resume_db is not None:
        db = Path(resume_db)
        if not db.is_file():
            raise ValueError("resume_database_not_found")
        out = run_output_path(out_arg, f"v8_price_value_forward_resume_{stamp}.json.gz")
        if out.exists():
            raise FileExistsError("report_already_exists")
        return db, out, True
    db = run_output_path(db_arg, f"jevymarket.v8-price-value_{stamp}.db")
    out = run_output_path(out_arg, f"v8_price_value_forward_{stamp}.json.gz")
    if db.exists() or out.exists():
        raise FileExistsError("experiment_output_already_exists")
    return db, out, False


def main(argv=None):
    parser = argparse.ArgumentParser(description="V8 BTC5m价格价值前向实验；真实ASK/费用模拟，无订单")
    parser.add_argument("--seconds", type=int, default=DEFAULT_SECONDS)
    parser.add_argument("--loop", type=float, default=INTERVAL_SECONDS)
    parser.add_argument("--taker-fee-rate", type=float, default=CRYPTO_TAKER_FEE_RATE)
    paths = parser.add_mutually_exclusive_group()
    paths.add_argument("--db", type=Path)
    paths.add_argument("--resume-db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    if not 1800 <= args.seconds <= 86400:
        parser.error("seconds必须在1800到86400之间")
    if args.loop != INTERVAL_SECONDS:
        parser.error("V8固定10秒采样，避免同时改变协议")
    if not 0 <= args.taker_fee_rate <= .2:
        parser.error("taker fee rate超出实验允许范围")

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    try:
        db, out, resumed = resolve_paths(args.db, args.resume_db, args.out, stamp)
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))

    settings = fast_settings().model_copy(update={"db_path": str(db), "strategy_version": REVISION})
    console = Console()
    try:
        with single_instance(str(db)):
            asyncio.run(run_v8(
                settings, console, version=REVISION, interval=args.loop,
                seconds=args.seconds, fee_rate=args.taker_fee_rate,
            ))
    except KeyboardInterrupt:
        console.print("[yellow]已停止；保留全部V8前向记录并导出当前累计结果。[/]")

    settled = asyncio.run(settle_all(settings, db, REVISION))
    store = V8Store(db)
    try:
        report = build_report(store, REVISION)
    finally:
        store.close()
    report["settlements_added_at_export"] = settled
    report["resumed_existing_database"] = resumed
    write_report(report, out)
    print_scoreboard(console, report, final=True)
    console.print(Panel(
        f"报告：[bold]{out.resolve()}[/]\n数据库：[bold]{db.resolve()}[/]",
        title="[bold green] V8 导出完成 [/]",
        border_style="green",
        expand=False,
    ))


if __name__ == "__main__":
    main()
