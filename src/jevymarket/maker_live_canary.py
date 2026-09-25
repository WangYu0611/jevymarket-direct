"""Single-order BTC5m live canary with strict maker-only risk caps.

Default mode is account preflight only. Real money requires both --live-one and
the exact confirmation phrase. No secret values are written to reports.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from polymarket import AsyncSecureClient
from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder
from polymarket.streams import UserSpec

from .config import load_settings
from .maker import MakerRuntime
from .maker_config import MakerConfig
from .maker_diagnostics import diagnostic_report
from .maker_engine import Quote, choose_quote
from .maker_store import MakerStore, single_process
from .run_paths import run_output_path

REVISION = "v6-live-one-order-canary-r1"
CONFIRM_PHRASE = "ONE_REAL_POST_ONLY_ORDER"
ENTRY_SECONDS = 45.0
CANCEL_SECONDS = 30.0
MAX_REST_SECONDS = 5.0
POLL_SECONDS = 0.10
MAX_ORDER_USD = 5.0
TERMINAL_STATUSES = frozenset({"CANCELED", "MATCHED", "UNMATCHED"})


@dataclass(frozen=True)
class CandidateSnapshot:
    slug: str
    condition: str
    token: str
    outcome: str
    price: float
    size: float
    fair_p: float
    generation: int
    observed_wall: float
    market_end: float

    @property
    def stake_usd(self) -> float:
        return self.price * self.size


def canary_config() -> MakerConfig:
    return MakerConfig(entry_seconds=ENTRY_SECONDS, cancel_before_end_seconds=CANCEL_SECONDS)


def _valid_private_key(value: str) -> bool:
    raw = (value or "").strip().removeprefix("0x")
    return len(raw) == 64 and all(c in "0123456789abcdefABCDEF" for c in raw)


def safe_error(exc: BaseException) -> dict:
    classes, seen = [], set()
    while exc is not None and id(exc) not in seen and len(classes) < 8:
        seen.add(id(exc))
        classes.append(type(exc).__name__)
        exc = exc.__cause__ or exc.__context__
    return {"classes": classes or ["UnknownError"], "chain_truncated": exc is not None}


def safe_response(resp) -> dict:
    if isinstance(resp, AcceptedOrder):
        return {
            "kind": "accepted",
            "order_id": str(resp.order_id),
            "status": str(resp.status or ""),
            "trade_ids": [str(x) for x in (resp.trade_ids or ())][:16],
        }
    if isinstance(resp, RejectedOrder):
        return {"kind": "rejected", "code": str(resp.code or "")[:128]}
    return {"kind": type(resp).__name__}


def _number(value) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def safe_user_event(event, order_id: str) -> dict | None:
    """Whitelist only fields needed to reconcile our one canary order."""
    kind = getattr(event, "type", None)
    payload = getattr(event, "payload", None)
    if kind == "order" and str(getattr(payload, "id", "")) == order_id:
        return {
            "type": "order",
            "id": order_id,
            "status": str(getattr(payload, "status", "") or ""),
            "order_event_type": str(getattr(payload, "order_event_type", "") or ""),
            "size_matched": _number(getattr(payload, "size_matched", None)),
            "price": _number(getattr(payload, "price", None)),
            "timestamp": str(getattr(payload, "timestamp", "") or "")[:64],
        }
    if kind != "trade":
        return None
    matched = str(getattr(payload, "taker_order_id", "")) == order_id
    maker_orders = getattr(payload, "maker_orders", None) or ()
    maker_match = next((x for x in maker_orders if str(getattr(x, "order_id", "")) == order_id), None)
    if not matched and maker_match is None:
        return None
    return {
        "type": "trade",
        "id": str(getattr(payload, "id", "") or "")[:128],
        "order_id": order_id,
        "price": _number(getattr(maker_match or payload, "price", None)),
        "size": _number(getattr(maker_match, "matched_amount", None)
                       if maker_match is not None else getattr(payload, "size", None)),
        "status": str(getattr(payload, "status", "") or ""),
        "trader_side": str(getattr(payload, "trader_side", "") or ""),
        "timestamp": str(getattr(payload, "timestamp", "") or "")[:64],
    }


class LiveSignalRuntime(MakerRuntime):
    """Public-data runtime that emits quote candidates but never paper orders."""

    def __init__(self, config: MakerConfig, store: MakerStore):
        super().__init__(config, store)
        self.candidate_event = asyncio.Event()
        self.latest_candidate: CandidateSnapshot | None = None

    def decision(self, wall: float, mono: float) -> tuple[Quote | None, str]:
        if self.market is None or self.cache is None:
            return None, "market_unavailable"
        try:
            if not self.clock_ok or wall - self.clock_ts > 60:
                raise ValueError("clock_unverified")
            if wall - self.metadata_ts > 45:
                raise ValueError("market_unavailable")
            if not self.c.cancel_before_end_seconds < self.market.end - wall <= self.c.entry_seconds:
                raise ValueError("outside_entry_window")
            est = self.reference.estimate(self.market.start, self.market.window, wall)
            quote, reason = choose_quote(self.market, self.cache, est, self.c, wall, mono, self.c.max_order_usd)
            self.estimate = est
            return quote, reason
        except ValueError as exc:
            return None, str(exc)

    def react(self, wall=None, mono=None):
        wall = time.time() if wall is None else wall
        mono = time.monotonic() if mono is None else mono
        quote, reason = self.decision(wall, mono)
        self.reason = reason
        self.engine.advance(self.cache, wall, mono, False)
        if quote is None or self.market is None or self.cache is None:
            self.latest_candidate = None
            return
        self.latest_candidate = CandidateSnapshot(
            slug=self.market.slug,
            condition=self.market.condition,
            token=quote.token,
            outcome=quote.outcome,
            price=quote.price,
            size=quote.size,
            fair_p=quote.fair_p,
            generation=self.cache.generation,
            observed_wall=wall,
            market_end=self.market.end,
        )
        self.candidate_event.set()

    def active_order_safe(self, candidate: CandidateSnapshot, wall: float, mono: float) -> tuple[bool, str]:
        if self.market is None or self.cache is None:
            return False, "market_unavailable"
        if self.market.slug != candidate.slug or self.market.condition != candidate.condition:
            return False, "market_changed"
        if self.cache.generation != candidate.generation:
            return False, "book_generation_changed"
        quote, reason = self.decision(wall, mono)
        if quote is None:
            return False, reason
        if quote.token != candidate.token:
            return False, "direction_changed"
        book = self.cache.books.get(candidate.token)
        if book is None or not book.fresh(wall, mono, self.c.max_book_age_seconds):
            return False, "book_not_ready_or_stale"
        if candidate.price >= book.ask:
            return False, "would_cross"
        if candidate.price > quote.fair_p - self.c.min_edge + 1e-9:
            return False, "edge_invalidated"
        # Sticky maker: a safer old price may rest even when the newly desired
        # price changes. This preserves queue priority instead of chasing every tick.
        return True, "safe_to_rest"


async def account_preflight(client: AsyncSecureClient) -> dict:
    result = {"wallet_type": str(client.wallet_type), "balance_usd": None,
              "open_orders_present": None, "trading_approved": None}
    balance = await client.get_balance_allowance(asset_type="COLLATERAL")
    result["balance_usd"] = _number(balance.balance)
    if result["balance_usd"] is not None:
        result["balance_usd"] /= 1e6
    page = await client.list_open_orders().first_page()
    result["open_orders_present"] = bool(page.items)
    approvals = await client.get_trading_approvals_state(wallet=client.wallet)
    result["trading_approved"] = bool(approvals.is_fully_approved)
    return result


async def read_user_stream(client, condition: str, order_id_box: dict, events: list,
                           stop: asyncio.Event, ready: asyncio.Event):
    try:
        async with await client.subscribe(UserSpec(markets=[condition])) as stream:
            ready.set()
            async for event in stream:
                if stop.is_set():
                    return
                order_id = order_id_box.get("id")
                if not order_id:
                    continue
                row = safe_user_event(event, order_id)
                if row is not None:
                    events.append(row)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # no exception text / credentials in report
        events.append({"type": "stream_error", **safe_error(exc)})


async def fetch_order_state(client, order_id: str) -> dict:
    order = await client.get_order(order_id=order_id)
    return {
        "id": order_id,
        "status": str(order.status or ""),
        "price": _number(order.price),
        "original_size": _number(order.original_size),
        "size_matched": _number(order.size_matched),
    }


async def cancel_known_order(client, order_id: str, report: dict) -> bool:
    attempts = report.setdefault("cancel_attempts", [])
    for _ in range(3):
        started = time.perf_counter_ns()
        try:
            await client.cancel_order(order_id=order_id)
            attempts.append({"ok": True, "elapsed_ms": (time.perf_counter_ns() - started) / 1e6})
        except Exception as exc:
            attempts.append({"ok": False, "elapsed_ms": (time.perf_counter_ns() - started) / 1e6,
                             "error": safe_error(exc)})
        await asyncio.sleep(.15)
        try:
            state = await fetch_order_state(client, order_id)
            report.setdefault("order_states", []).append(state)
            if state["status"].upper() in TERMINAL_STATUSES:
                return True
            if (state["size_matched"] or 0) >= (state["original_size"] or math.inf):
                return True
        except Exception as exc:
            report.setdefault("state_errors", []).append(safe_error(exc))
    return False


async def execute_one(client: AsyncSecureClient, runtime: LiveSignalRuntime,
                      candidate: CandidateSnapshot, report: dict):
    if candidate.stake_usd > MAX_ORDER_USD + 1e-9:
        report["termination"] = "candidate_exceeds_canary_cap"
        return
    now, mono = time.time(), time.monotonic()
    safe, reason = runtime.active_order_safe(candidate, now, mono)
    if not safe:
        report["termination"] = "candidate_invalid_before_submit"
        report["invalid_reason"] = reason
        return

    order_id_box, user_events, stop_stream, stream_ready = {}, [], asyncio.Event(), asyncio.Event()
    stream_task = asyncio.create_task(
        read_user_stream(client, candidate.condition, order_id_box, user_events, stop_stream, stream_ready),
        name="live-canary-user-stream",
    )
    try:
        await asyncio.wait_for(stream_ready.wait(), 2.0)
    except TimeoutError:
        report["termination"] = "user_stream_not_ready"
        stop_stream.set()
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)
        return
    if stream_task.done():
        report["termination"] = "user_stream_stopped_before_submit"
        await asyncio.gather(stream_task, return_exceptions=True)
        return
    started_ns = time.perf_counter_ns()
    try:
        response = await client.place_limit_order(
            token_id=candidate.token,
            side="BUY",
            price=str(candidate.price),
            size=str(candidate.size),
            post_only=True,
        )
    except Exception as exc:
        report["placement"] = {"ok": False, "elapsed_ms": (time.perf_counter_ns() - started_ns) / 1e6,
                               "error": safe_error(exc)}
        report["termination"] = "placement_exception"
        stop_stream.set()
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)
        return

    report["placement"] = {"ok": isinstance(response, AcceptedOrder),
                           "elapsed_ms": (time.perf_counter_ns() - started_ns) / 1e6,
                           "response": safe_response(response)}
    if not isinstance(response, AcceptedOrder):
        report["termination"] = "placement_rejected"
        stop_stream.set()
        stream_task.cancel()
        await asyncio.gather(stream_task, return_exceptions=True)
        return

    order_id = str(response.order_id)
    order_id_box["id"] = order_id
    report["order_id"] = order_id
    report["candidate"] = asdict(candidate)
    placed_mono = time.monotonic()
    cancel_reason = None

    while True:
        wall, mono = time.time(), time.monotonic()
        try:
            state = await fetch_order_state(client, order_id)
            report.setdefault("order_states", []).append(state)
            matched = state["size_matched"] or 0
            if matched > 0:
                cancel_reason = "real_fill_detected_cancel_remainder"
                break
            if state["status"].upper() in TERMINAL_STATUSES:
                report["termination"] = "exchange_terminal_without_local_cancel"
                break
        except Exception as exc:
            report.setdefault("state_errors", []).append(safe_error(exc))

        safe, reason = runtime.active_order_safe(candidate, wall, mono)
        if not safe:
            cancel_reason = "public_signal_invalid:" + reason
            break
        if mono - placed_mono >= MAX_REST_SECONDS:
            cancel_reason = "canary_rest_limit"
            break
        if wall >= candidate.market_end - CANCEL_SECONDS:
            cancel_reason = "hard_market_cutoff"
            break
        await asyncio.sleep(POLL_SECONDS)

    if cancel_reason is not None:
        report["cancel_reason"] = cancel_reason
        verified = await cancel_known_order(client, order_id, report)
        report["cancel_verified"] = verified
        report["termination"] = "cancel_verified" if verified else "cancel_unverified"
        if not verified:
            # Fail closed: keep trying until market cutoff/end rather than silently
            # exiting with a potentially live order.
            while time.time() < candidate.market_end:
                if await cancel_known_order(client, order_id, report):
                    report["cancel_verified"] = True
                    report["termination"] = "cancel_verified_after_retry"
                    break
                await asyncio.sleep(.5)

    stop_stream.set()
    stream_task.cancel()
    await asyncio.gather(stream_task, return_exceptions=True)
    report["user_events"] = user_events


async def run_live(seconds: int, db: Path, report: dict, client: AsyncSecureClient):
    config = canary_config()
    with single_process(db):
        store = MakerStore(db, config)
        runtime = LiveSignalRuntime(config, store)
        public_task = asyncio.create_task(runtime.run(seconds=seconds, observe_only=True), name="live-canary-public")
        try:
            deadline = time.monotonic() + seconds
            candidate = None
            while time.monotonic() < deadline and not public_task.done():
                try:
                    await asyncio.wait_for(runtime.candidate_event.wait(), min(.5, deadline - time.monotonic()))
                except TimeoutError:
                    continue
                runtime.candidate_event.clear()
                if runtime.latest_candidate is not None:
                    candidate = runtime.latest_candidate
                    break
            if candidate is None:
                report["termination"] = "no_candidate"
            else:
                report["candidate_seen"] = asdict(candidate)
                try:
                    positions = await client.list_positions(
                        user=str(client.wallet), market=[candidate.condition], status="OPEN"
                    ).first_page()
                except Exception as exc:
                    report["termination"] = "market_position_check_failed"
                    report["position_check_error"] = safe_error(exc)
                else:
                    if positions.items:
                        report["termination"] = "refuse_existing_market_position"
                    else:
                        await execute_one(client, runtime, candidate, report)
        finally:
            runtime.stopped = True
            runtime.signal()
            results = await asyncio.gather(public_task, return_exceptions=True)
            if results and isinstance(results[0], BaseException):
                report["public_runtime_error"] = safe_error(results[0])
    report["diagnostic_summary"] = diagnostic_report(db)["summary"]


async def async_main(args) -> dict:
    settings = load_settings()
    report = {
        "format": REVISION,
        "paper_only": False,
        "live_order_cap": 1,
        "max_order_usd": MAX_ORDER_USD,
        "post_only_required": True,
        "max_rest_seconds": MAX_REST_SECONDS,
        "started_wall": time.time(),
    }
    if not _valid_private_key(settings.polymarket_private_key):
        report["termination"] = "private_key_missing_or_invalid"
        return report

    try:
        client = await AsyncSecureClient.create(
            private_key=settings.polymarket_private_key,
            wallet=settings.polymarket_wallet or None,
        )
    except Exception as exc:
        report["termination"] = "client_create_failed"
        report["client_error"] = safe_error(exc)
        return report

    try:
        try:
            preflight = await account_preflight(client)
        except Exception as exc:
            report["termination"] = "preflight_failed"
            report["preflight_error"] = safe_error(exc)
            return report
        report["preflight"] = preflight

        if args.check_only:
            report["termination"] = "check_only_complete"
            return report
        if preflight["open_orders_present"]:
            report["termination"] = "refuse_existing_open_orders"
            return report
        if not preflight["trading_approved"]:
            report["termination"] = "refuse_missing_trading_approvals"
            return report
        if preflight["balance_usd"] is None or preflight["balance_usd"] < MAX_ORDER_USD:
            report["termination"] = "refuse_insufficient_balance"
            return report

        db = run_output_path(args.db, args.db_name)
        if db.exists():
            report["termination"] = "refuse_existing_database"
            return report
        try:
            await run_live(args.seconds, db, report, client)
        except Exception as exc:
            report["termination"] = "live_run_exception"
            report["live_error"] = safe_error(exc)
        report["db_filename"] = db.name
        return report
    finally:
        await client.close()
        report["ended_wall"] = time.time()


def main(argv=None):
    parser = argparse.ArgumentParser(description="BTC5m单订单实测canary；默认只检查账户，不自动交易")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check-only", action="store_true", help="仅认证/余额/审批/未结订单检查，不下单")
    group.add_argument("--live-one", action="store_true", help="最多提交1个真实post-only订单")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--seconds", type=int, default=900, choices=range(300, 1801))
    parser.add_argument("--db", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    if args.live_one and args.confirm != CONFIRM_PHRASE:
        parser.error(f"真实下单必须显式添加 --confirm {CONFIRM_PHRASE}")

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    args.db_name = f"jevymarket.live-canary_{stamp}.db"
    out = run_output_path(args.out, f"v6_live_canary_{stamp}.json.gz")
    if out.exists():
        parser.error("输出已存在，拒绝覆盖")

    report = asyncio.run(async_main(args))
    with gzip.open(out, "xt", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    print(json.dumps({k: report.get(k) for k in ("format", "termination", "preflight", "placement",
                                                 "cancel_reason", "cancel_verified")},
                     ensure_ascii=False, indent=2))
    print(f"已导出 → {out.resolve()}", flush=True)


if __name__ == "__main__":
    main()
