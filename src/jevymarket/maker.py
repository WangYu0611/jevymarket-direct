"""BTC 5m late maker research runner. Public reads only; live trading is absent.

Run: python -m jevymarket.maker run --dry-run --loop 2
Stats: python -m jevymarket.maker stats --out maker.json.gz
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from .maker_book import BookGap, trade_message
from .maker_config import VERSION, MakerConfig
from .maker_diagnostics import ReactionWindow, diagnostic_report, safe_book_message
from .maker_engine import Market, PaperEngine, choose_quote
from .maker_ingress import SnapshotBookCache as BookCache
from .maker_model import ReferenceCache, finite, source_time, twap_window
from .maker_protocol import (
    IO_REVISION,
    clock_retry_seconds,
    clock_sample,
    data_health,
    decode_stream_frame,
    error_reason,
    health_text,
)
from .maker_store import MakerStore, encode, single_process, statistics

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS = "wss://ws-live-data.polymarket.com"
TOPICS = {"raw": "crypto_prices_chainlink", "twap30": "crypto_prices_twap_thirty", "twap60": "crypto_prices_twap_sixty"}
DEFAULT_DB = Path("jevymarket.maker-v6.db")
REASONS = {
    "outside_entry_window": "观察中，尚未进入最后10秒或已经进入撤单保护区",
    "missing_anchor_or_reference": "等待新窗口的精确目标价/参考行情",
    "history_missing": "原始行情历史不足，继续预热",
    "history_gap": "原始行情有缺口，暂停挂单",
    "stale_reference": "参考行情过期，暂停挂单",
    "raw_behind_twap": "等待raw与TWAP源时间对齐",
    "book_not_ready_or_stale": "等待新鲜完整WebSocket盘口",
    "confidence_or_agreement_failed": "确定性/模型盘口一致性不足，不挂单",
    "minimum_size_exceeds_budget": "最小份数超过金额或单市场利润上限，不放大仓位",
    "quote_ready": "满足Maker报价条件（不等于已成交）",
    "market_unavailable": "等待市场信息",
    "clock_unverified": "服务器时钟未核对或偏差过大，暂停挂单",
    "already_filled": "本市场已有估计成交，不加仓不反手",
    "observe_only_qualified": "候选满足旧策略条件，但仅观察模式禁止生成模拟挂单",
}


def array(value):
    value = json.loads(value) if isinstance(value, str) else value
    if not isinstance(value, list):
        raise ValueError("invalid_array")
    return value


def parse_market(data: dict, slug: str) -> Market:
    if data.get("slug") != slug or not slug.startswith("btc-updown-5m-"):
        raise ValueError("wrong_market")
    start = int(slug.rsplit("-", 1)[1])
    if start % 300 or data.get("active") is not True or data.get("closed") is not False or data.get("acceptingOrders") is not True:
        raise ValueError("market_not_accepting")
    expiry = datetime.fromisoformat(data["endDate"].replace("Z", "+00:00"))
    if expiry.tzinfo is None or abs(expiry.timestamp() - start - 300) > 1:
        raise ValueError("market_window_mismatch")
    labels, tokens = array(data["outcomes"]), array(data["clobTokenIds"])
    if len(labels) != 2 or len(tokens) != 2 or {str(x).upper() for x in labels} != {"UP", "DOWN"}:
        raise ValueError("ambiguous_outcome_mapping")
    mapped = {str(k).upper(): str(v) for k, v in zip(labels, tokens, strict=True)}
    if mapped["UP"] == mapped["DOWN"] or any(not v.isdigit() for v in mapped.values()):
        raise ValueError("invalid_token_ids")
    condition = str(data.get("conditionId", ""))
    if (not condition.startswith("0x") or len(condition) != 66
            or any(ch not in "0123456789abcdefABCDEF" for ch in condition[2:])):
        raise ValueError("invalid_condition_id")
    w = twap_window(str(data.get("description", "")), str(data.get("resolutionSource", "")))
    return Market(slug, condition, start, w, mapped["UP"], mapped["DOWN"])


def official_winner(data: dict, slug: str, condition: str, now: float) -> str | None:
    if data.get("slug") != slug or data.get("conditionId") != condition or data.get("closed") is not True:
        return None
    if now < int(slug.rsplit("-", 1)[1]) + 300:
        return None
    labels, prices = array(data["outcomes"]), array(data["outcomePrices"])
    if len(labels) != 2 or len(prices) != 2 or {str(x).upper() for x in labels} != {"UP", "DOWN"}:
        return None
    mapped = {str(k).upper(): Decimal(str(v)) for k, v in zip(labels, prices, strict=True)}
    if mapped == {"UP": Decimal(1), "DOWN": Decimal(0)}:
        return "UP"
    if mapped == {"UP": Decimal(0), "DOWN": Decimal(1)}:
        return "DOWN"
    return None


def reference_message(msg: dict, topic: str) -> tuple[float, float] | None:
    if msg.get("topic") != topic or msg.get("type") != "update":
        return None
    payload = msg.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("invalid_reference_payload")
    if payload.get("symbol") != "btc/usd":
        return None
    ts = source_time(payload.get("timestamp"))  # Never use envelope/receive time.
    if topic != TOPICS["raw"]:
        expected = 30 if topic == TOPICS["twap30"] else 60
        if payload.get("window_s") != expected:
            raise ValueError("twap_message_window_mismatch")
    exact = payload.get("full_accuracy_value")
    price = finite(Decimal(str(exact)) / Decimal(10 ** 18) if exact is not None else payload.get("value"))
    return ts, price


def safe_book_event(msg: dict) -> dict:
    return safe_book_message(msg)


async def public_json(http, url: str, **params):
    # Read-only endpoint allowlist. No orders/cancellation/signing transport exists.
    if not (url.startswith(GAMMA + "/markets/slug/") or url in {CLOB + "/book", CLOB + "/time"}):
        raise ValueError("non_public_read_endpoint")
    options = {"headers": {"Cache-Control": "no-cache", "Pragma": "no-cache"}} if url == CLOB + "/time" else {}
    result = await http.get(url, params=params, timeout=4, **options)
    result.raise_for_status()
    return result.json()


async def heartbeat(ws, seconds):
    while True:
        await ws.send("PING")
        await asyncio.sleep(seconds)


class MakerRuntime:
    def __init__(self, config: MakerConfig, store: MakerStore):
        self.c, self.store = config, store
        self.engine = PaperEngine(config, store.emit)
        self.engine.restore(store.load_orders(), time.time())
        self.reference = ReferenceCache(config)
        self.market = None
        self.cache = None
        self.book_task = None
        self.wake = asyncio.Event()
        self.wake_at = None
        self.metadata_ts = 0.0
        self.clock_ts = 0.0
        self.clock_ok = False
        self.clock_info = {}
        self.stream_controls = Counter()
        self.stopped = False
        self.errors = Counter()
        self.estimate = None
        self.reason = "market_unavailable"
        self.session_id = uuid.uuid4().hex
        self.reaction_window = ReactionWindow()
        self.observe_only = False

    def signal(self):
        if self.wake_at is None:
            self.wake_at = time.monotonic()
        self.wake.set()

    def error(self, stage, exc):
        code = type(exc).__name__
        detail = error_reason(exc)
        key = (stage, code, detail)
        self.errors[key] += 1
        if self.errors[key] == 1 or self.errors[key] % 30 == 0:
            print(f"[{stage}] 公共数据暂不可用：{code}/{detail}；不使用过期价格，不记录虚构成交", flush=True)
        self.store.emit("source_error", {"stage": stage, "code": code, "reason": detail, "io_revision": IO_REVISION})
        self.signal()

    async def stream_messages(self, ws, raw, name):
        messages, control, reply = decode_stream_frame(raw)
        if control:
            self.stream_controls[f"{name}:{control}"] += 1
        if reply is not None:
            await asyncio.wait_for(ws.send(reply), 2)
        return messages

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
                reason = str(exc)  # Only our local enumerated validation errors.
        safe = desired is not None and not self.engine.halted and not self.observe_only
        if active and safe:
            safe = (active.slug == self.market.slug and active.token == desired.token
                    and active.price <= desired.fair_p - self.c.min_edge + 1e-9
                    and active.generation == self.cache.generation)
        if active and not safe:
            self.engine.cancel("invalidated_quote", wall, mono)
        self.engine.advance(self.cache, wall, mono, safe)
        active = self.engine.active()
        if active and active.state == "active" and desired and safe:
            if abs(active.price - desired.price) > 1e-9 and mono - self.engine.last_submit_mono >= self.c.min_requote_seconds:
                self.engine.cancel("fair_price_requote", wall, mono)
        if self.engine.active() is None and desired and safe:
            # Recompute the budget after acknowledgements; never cancel-and-post
            # concurrently or release an unacknowledged cancellation reservation.
            desired, reason = choose_quote(self.market, self.cache, est, self.c, wall, mono,
                                           self.engine.budget(self.market.slug, wall))
            if desired:
                self.engine.submit(self.market, self.cache, desired, wall, mono)
        if self.market and any(o.slug == self.market.slug and o.filled > 0 for o in self.engine.orders):
            reason = "already_filled"
        if self.observe_only and desired is not None:
            reason = "observe_only_qualified"
        self.estimate, self.reason = est, reason

    async def references(self, name):
        from websockets.asyncio.client import connect
        topic = TOPICS[name]
        sub = {"action": "subscribe", "subscriptions": [{"topic": topic,
               "type": "*" if name == "raw" else "update", "filters": '{"symbol":"btc/usd"}'}]}
        while True:
            try:
                async with connect(RTDS_WS, open_timeout=10, close_timeout=2, ping_interval=None,
                                   max_size=2 ** 20, max_queue=16) as ws:
                    await ws.send(encode(sub))
                    ping = asyncio.create_task(heartbeat(ws, 5))
                    last_valid = time.monotonic()
                    try:
                        while True:
                            if ping.done():
                                ping.result()
                            raw = await asyncio.wait_for(ws.recv(), max(.01, 20 - (time.monotonic() - last_valid)))
                            for msg in await self.stream_messages(ws, raw, name):
                                quote = reference_message(msg, topic)
                                if quote and self.reference.add(name, *quote, time.time()):
                                    last_valid = time.monotonic()
                                    self.store.emit("reference", {"name": name, "source_ts": quote[0], "price": quote[1]})
                                    self.signal()
                            if time.monotonic() - last_valid > 20:
                                raise TimeoutError("reference_silent")
                    finally:
                        ping.cancel()
                        await asyncio.gather(ping, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.reference.samples[name].clear()
                self.engine.cancel("reference_disconnected", time.time(), time.monotonic())
                self.error(name, exc)
                await asyncio.sleep(2)

    def apply_book_message(self, m, cache, msg, wall, mono):
        try:
            if msg.get("event_type") == "market_resolved" and msg.get("market") == m.condition:
                cache.invalidate()
                raise BookGap("market_closed_verify_official_result")
            cache.apply(msg, wall, mono)
            discarded = getattr(cache, "last_discard", None)
            if discarded is not None:
                self.store.emit("book_discard", {"io_revision": IO_REVISION, "session_id": self.session_id,
                    "slug": m.slug, "received_ts": wall, "received_mono": mono,
                    **discarded, "event": safe_book_event(msg)})
                return  # Not an applied book_event, fresh quote, or fill.
            trade = trade_message(msg, m.condition, wall)
        except (BookGap, ValueError, KeyError, TypeError) as exc:
            self.store.emit("book_reject", {"io_revision": IO_REVISION, "session_id": self.session_id,
                "slug": m.slug, "received_ts": wall, "received_mono": mono,
                "reason": error_reason(exc), "event": safe_book_event(msg),
                "cache_failure": cache.last_failure if msg.get("event_type") in
                {"book", "price_change", "tick_size_change"} else None})
            raise
        if trade:
            self.engine.on_trade(trade, wall)
        if msg.get("event_type") in {"book", "price_change", "tick_size_change", "last_trade_price"}:
            self.store.emit("book_event", safe_book_event(msg))
        self.signal()

    def protect_book_failure(self, m, cache):
        """Local risk action BEFORE heartbeat cleanup or websocket close awaits."""
        cache.invalidate()
        self.engine.cancel("book_disconnected", time.time(), time.monotonic())
        active = self.engine.active()
        if active and active.slug == m.slug and active.active_ts is not None and not active.uncertain:
            active.uncertain, self.engine.halted = True, True
            self.engine.changed(active, "book_disconnect_unknown_fills")
        self.signal()

    async def books(self, m: Market, cache: BookCache):
        from websockets.asyncio.client import connect
        while time.time() < m.end:
            protected = False
            try:
                cache.invalidate()
                async with connect(MARKET_WS, open_timeout=10, close_timeout=2, ping_interval=None,
                                   max_size=2 ** 20, max_queue=16) as ws:
                    ping = None
                    try:
                        await ws.send(encode({"assets_ids": list(cache.books), "type": "market", "custom_feature_enabled": True}))
                        ping = asyncio.create_task(heartbeat(ws, 10))
                        while time.time() < m.end:
                            if ping.done():
                                ping.result()
                            raw = await asyncio.wait_for(ws.recv(), 20)
                            for msg in await self.stream_messages(ws, raw, "orderbook"):
                                wall, mono = time.time(), time.monotonic()
                                self.apply_book_message(m, cache, msg, wall, mono)
                    except Exception:
                        # Do not leave this to the outer handler: __aexit__ can
                        # await the close handshake before that handler runs.
                        self.protect_book_failure(m, cache)
                        protected = True
                        raise
                    finally:
                        cache.invalidate()
                        self.engine.cancel("book_disconnect_or_roll", time.time(), time.monotonic())
                        self.signal()
                        if ping:
                            ping.cancel()
                            await asyncio.gather(ping, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not protected:
                    self.protect_book_failure(m, cache)
                self.error("orderbook", exc)
                await asyncio.sleep(1)
            finally:
                cache.invalidate()
                self.engine.cancel("book_disconnect_or_roll", time.time(), time.monotonic())
                self.signal()

    async def discover(self, http):
        while True:
            slug = f"btc-updown-5m-{int(time.time()) // 300 * 300}"
            try:
                data = await public_json(http, GAMMA + "/markets/slug/" + slug)
                m = parse_market(data, slug)
                if int(time.time()) // 300 * 300 != m.start:
                    continue
                if self.market is None or self.market != m or self.book_task is None or self.book_task.done():
                    self.engine.cancel("market_change", time.time(), time.monotonic())
                    if self.book_task:
                        self.book_task.cancel()
                        await asyncio.gather(self.book_task, return_exceptions=True)
                    specs = {}
                    for token in (m.up_token, m.down_token):
                        book = await public_json(http, CLOB + "/book", token_id=token)
                        if str(book.get("asset_id")) != token or book.get("market") != m.condition:
                            raise ValueError("book_metadata_mismatch")
                        tick, minimum = finite(book["tick_size"]), finite(book["min_order_size"])
                        if not 0 < tick < 1 or minimum <= 0:
                            raise ValueError("invalid_order_constraints")
                        specs[token] = (tick, minimum)
                    self.market, self.cache = m, BookCache(m.condition, specs)
                    self.engine.seen_trades.clear()
                    self.store.emit("market", asdict(m))
                    self.book_task = asyncio.create_task(self.books(m, self.cache), name="maker-book")
                self.metadata_ts = time.time()
                self.signal()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.metadata_ts = 0
                self.error("metadata", exc)
            await asyncio.sleep(2 if self.market is None or time.time() >= self.market.end else 15)

    async def clock_once(self, http):
        before, started = time.time(), time.monotonic()
        try:
            stamp = source_time(await public_json(http, CLOB + "/time"))
            after, rtt = time.time(), time.monotonic() - started
            sample = clock_sample(stamp, before, after, rtt)
            previously_ok = self.clock_ok
            self.clock_info = sample
            self.clock_ok, self.clock_ts = sample["acceptable"], after
            self.store.emit("clock", dict(sample, io_revision=IO_REVISION))
            if not self.clock_ok or not previously_ok:
                print(f"[server_clock] {'校验通过' if self.clock_ok else '暂停报价'}：{sample['reason']}"
                      f" | RTT={rtt:.3f}s | 偏差区间=[{sample['offset_lower_seconds']:+.3f},"
                      f"{sample['offset_upper_seconds']:+.3f}]s（秒级精度，非NTP）", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.clock_ok = False
            self.clock_info = {"acceptable": False, "reason": "clock_request_failed"}
            self.error("server_clock", exc)
        self.signal()
        return self.clock_ok

    async def clock(self, http):
        failures = 0
        while True:
            failures = 0 if await self.clock_once(http) else failures + 1
            # Bad samples still immediately block quoting. Retry sooner, do not
            # widen the RTT/offset guard or reuse a failed/stale clock sample.
            await asyncio.sleep(clock_retry_seconds(failures))

    async def settle_once(self, http):
        pending = {}
        for o in self.engine.orders:
            if o.winner is None and time.time() >= o.end and (o.filled > 0 or o.uncertain):
                pending[o.slug] = o.condition
        for slug, condition in list(pending.items())[:20]:
            try:
                data = await public_json(http, GAMMA + "/markets/slug/" + slug)
                winner = official_winner(data, slug, condition, time.time())
                if winner:
                    self.engine.settle(slug, winner, time.time())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.error("settlement", exc)

    async def settlements(self, http):
        while True:
            await self.settle_once(http)
            await asyncio.sleep(30)

    async def run(self, seconds=None, settlement_only=False, observe_only=False):
        import httpx
        self.observe_only = bool(observe_only)
        tasks = []
        writer = asyncio.create_task(self.store.writer(), name="maker-writer")
        try:
            async with httpx.AsyncClient(timeout=4) as http:
                if settlement_only:
                    await self.settle_once(http)
                    return
                tasks = [asyncio.create_task(self.references(k), name=k) for k in TOPICS]
                tasks += [asyncio.create_task(self.discover(http), name="maker-discover"),
                          asyncio.create_task(self.clock(http), name="maker-clock"),
                          asyncio.create_task(self.settlements(http), name="maker-settle")]
                deadline, started = time.monotonic(), time.monotonic()
                last_mono, last_wall = started, time.time()
                self.store.emit("runtime", {"io_revision": IO_REVISION, "strategy_version": VERSION, "session_id": self.session_id,
                                            "observe_only": self.observe_only})
                print(f"{VERSION} | {IO_REVISION} | 仅模拟post-only | WS实时输入 | 定期评估/记录={self.c.interval_seconds:g}s | 最后{self.c.entry_seconds:g}s至T-{self.c.cancel_before_end_seconds:g}s入场 | 无真实下单", flush=True)
                if self.observe_only:
                    print("仅观察模式：不生成新的模拟挂单；旧账本和风险仍保留，不是收益实验", flush=True)
                while not self.stopped and (seconds is None or time.monotonic() - started < seconds):
                    for task in [*tasks, writer]:
                        if task.done():
                            task.result()
                            raise RuntimeError("worker_stopped")
                    try:
                        await asyncio.wait_for(self.wake.wait(), self.c.watchdog_seconds)
                    except TimeoutError:
                        pass
                    self.wake.clear()
                    wall, mono = time.time(), time.monotonic()
                    if abs((wall - last_wall) - (mono - last_mono)) > .5:
                        self.clock_ok = False
                        self.engine.cancel("wall_clock_jump", wall, mono)
                    last_wall, last_mono = wall, mono
                    triggered = self.wake_at
                    self.wake_at = None
                    # A scheduler stall is a risk event, not a fast reaction.
                    lag = 0 if triggered is None else mono - triggered
                    if lag > self.c.reaction_budget_seconds:
                        self.engine.cancel("reaction_budget_missed", wall, mono)
                        self.store.emit("reaction", {"elapsed_ms": lag * 1000, "over_budget": True})
                    else:
                        self.react(wall, mono)
                        elapsed = time.monotonic() - (triggered if triggered is not None else mono)
                        if triggered is not None and self.engine.active():
                            self.store.emit("reaction", {"elapsed_ms": elapsed * 1000,
                                                         "over_budget": elapsed > self.c.reaction_budget_seconds})
                        if elapsed > self.c.reaction_budget_seconds:
                            self.engine.cancel("compute_budget_missed", time.time(), time.monotonic())
                    measured_ms = (time.monotonic() - (triggered if triggered is not None else mono)) * 1000
                    self.reaction_window.add(measured_ms, triggered is not None)
                    if mono >= deadline:
                        self.store.emit("reaction_window", dict(self.reaction_window.take(),
                                                               io_revision=IO_REVISION, session_id=self.session_id))
                        health = data_health(self, wall, mono)
                        self.store.emit("observation", {"ts": wall, "slug": self.market.slug if self.market else None,
                            "io_revision": IO_REVISION, "session_id": self.session_id, "observe_only": self.observe_only,
                            "data_health": health, "stream_controls": dict(self.stream_controls),
                            "reason": self.reason, "estimate": asdict(self.estimate) if self.estimate else None,
                            "book": {t: {"bid": b.bid, "ask": b.ask, "source_ts": b.ts, "tick": b.tick, "minimum": b.minimum}
                                     for t, b in self.cache.books.items()} if self.cache else None,
                            "risk_reserved_usd": self.engine.exposure(), "halted": self.engine.halted})
                        view = self.engine.report()
                        ts = datetime.fromtimestamp(wall, timezone(timedelta(hours=8))).strftime("%m-%d %H:%M:%S")
                        print(f"[{ts}] {REASONS.get(self.reason, self.reason)} | 报价{view['quotes']} 估计成交{view['filled_orders_estimated']} | 毛PnL估计 ${view['gross_pnl_estimated']:+.2f} | 风险占用 ${view['open_risk_reserved_usd']:.2f} | 账本风险暂停={self.engine.halted}", flush=True)
                        print("  " + health_text(health), flush=True)
                        deadline += (int(max(0, mono - deadline) / self.c.interval_seconds) + 1) * self.c.interval_seconds
        finally:
            if self.store.failed:
                self.engine.emit = lambda kind, data: None
            self.engine.cancel("shutdown", time.time(), time.monotonic())
            # Keep readers alive through simulated cancel acknowledgement.
            await asyncio.sleep(self.c.paper_cancel_latency_seconds + .02)
            self.engine.advance(self.cache, time.time(), time.monotonic(), False)
            for task in [*tasks, self.book_task]:
                if task:
                    task.cancel()
            await asyncio.gather(*(t for t in [*tasks, self.book_task] if t), return_exceptions=True)
            if not self.store.failed:
                self.store.emit("reaction_window", dict(self.reaction_window.take(),
                                                       io_revision=IO_REVISION, session_id=self.session_id))
            self.store.stopping = True
            await asyncio.wait_for(writer, 10)


def main():
    parser = argparse.ArgumentParser(description="BTC 5m Maker前向研究：只模拟，不提供实盘入口")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--dry-run", action="store_true", required=True)
    run.add_argument("--loop", type=float, default=2.0)
    run.add_argument("--seconds", type=float)
    run.add_argument("--observe-only", action="store_true", help="只观察和诊断，不生成新的模拟挂单")
    for p in (run, commands.add_parser("settle")):
        p.add_argument("--db", type=Path, default=DEFAULT_DB)
        p.add_argument("--config", type=Path)
    stats = commands.add_parser("stats")
    stats.add_argument("--db", type=Path, default=DEFAULT_DB)
    stats.add_argument("--out", type=Path)
    diagnose = commands.add_parser("diagnose", help="只读导出最近一次运行的小型盘口诊断，不复制全部行情")
    diagnose.add_argument("--db", type=Path, default=DEFAULT_DB)
    diagnose.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.command == "diagnose":
        report = diagnostic_report(args.db, args.out)
        print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
        if args.out:
            print(f"已导出小型盘口诊断 → {args.out}")
        return
    if args.command == "stats":
        print(json.dumps(statistics(args.db, args.out), ensure_ascii=False, indent=2))
        return
    config = MakerConfig(**json.loads(args.config.read_text(encoding="utf-8"))) if args.config else MakerConfig()
    if args.command == "run":
        config = replace(config, interval_seconds=args.loop)
        if args.seconds is not None and (not 0 < args.seconds < 1e9):
            parser.error("seconds必须为有限正数")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with single_process(args.db):
        store = MakerStore(args.db, config)
        runtime = MakerRuntime(config, store)
        try:
            asyncio.run(runtime.run(getattr(args, "seconds", None), args.command == "settle",
                                    getattr(args, "observe_only", False)))
        except KeyboardInterrupt:
            print("已停止；保留所有研究记录。使用maker settle回填官方结算。")


if __name__ == "__main__":
    main()
