"""Single-flight 5m paper loop, continuous RTDS, independent Jev/settlement tasks."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import AsyncExitStack
from datetime import UTC, datetime

from polymarket import AsyncPublicClient
from pydantic import ValidationError
from rich.console import Console

from .config import Settings
from .fast_store import FastStore
from .fast_strategy import (
    checkpoint,
    experiment_parameters,
    local_snapshot,
    next_deadline,
    quantitative_trade,
    snapshot_payload,
)
from .jev import JevClient, JevError
from .market_data import watch_chainlink_anchors
from .markets import Candidate, build_state, fetch_book, market_is_current, passes_static_filters
from .network import ReadUnavailable, read_with_retry
from .settlement import resolved_market_from_market
from .signal import Trade, ask_jev, quantitative_up_probability

log = logging.getLogger(__name__)


def current_slug(now: float | None = None) -> str:
    return f"btc-updown-5m-{int(time.time() if now is None else now) // 300 * 300}"


class JevShadow:
    """One bounded worker. A reply only updates its immutable source observation."""

    def __init__(self, client: JevClient | None, store: FastStore, console: Console):
        self.client, self.store, self.console = client, store, console
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        self.disabled = client is None

    def offer(self, observation_id: int, cand: Candidate, s: Settings, snapshot) -> None:
        if self.disabled:
            self.store.mark_jev(observation_id, "disabled")
            return
        state = build_state(cand, s, brief=None)
        state.pop("market_implied_probability_primary", None)
        state["short_term_market_data"] = snapshot.to_state()
        state["microstructure"] = cand.book.microstructure_state()
        end = int(cand.slug.rsplit("-", 1)[1]) + 300
        try:
            self.queue.put_nowait((observation_id, state, end))
        except asyncio.QueueFull:
            self.store.mark_jev(observation_id, "busy")
        else:
            self.store.mark_jev(observation_id, "queued")

    async def work(self) -> None:
        while True:
            observation_id, state, end = await self.queue.get()
            requested = time.time()
            try:
                if self.disabled or self.client is None:
                    self.store.mark_jev(observation_id, "disabled")
                    continue
                budget = min(20.0, end - requested - 0.5)
                if budget <= 0:
                    self.store.mark_jev(observation_id, "expired")
                    continue
                self.store.mark_jev(observation_id, "running", requested_ts=requested)
                try:
                    async with asyncio.timeout(budget):
                        try:
                            view = await ask_jev(self.client, state)
                        except RuntimeError as exc:
                            if str(exc).startswith("Unexpected Jev answer shape:"):
                                raise ValueError("Jev回答结构不完整") from exc
                            raise
                    received = time.time()
                    if received >= end:
                        self.store.mark_jev(observation_id, "expired", requested_ts=requested)
                        continue
                    self.store.complete_jev(observation_id, view, received)
                    self.console.print(
                        f"  Jev影子：样本#{observation_id} P(Up)={view.p_yes:.3f}；"
                        f"耗时 {received - requested:.1f}s；不影响模拟下单", markup=False,
                    )
                except (JevError, TimeoutError, ValidationError, ValueError, KeyError) as exc:
                    if isinstance(exc, JevError) and exc.status in (401, 402, 403):
                        self.disabled = True
                        status = "disabled_auth"
                    else:
                        status = "error"
                    self.store.mark_jev(observation_id, status, requested_ts=requested)
                    self.console.print(
                        f"  Jev影子样本#{observation_id}：{type(exc).__name__}，"
                        f"{'授权/额度异常，停止影子调用' if self.disabled else '本次缺失'}；量化采集继续",
                        markup=False,
                    )
            except asyncio.CancelledError:
                self.store.mark_jev(observation_id, "interrupted", requested_ts=requested)
                raise
            finally:
                self.queue.task_done()


async def settle_some(client: AsyncPublicClient, s: Settings, store: FastStore,
                      version: str, *, limit: int = 8) -> int:
    count = 0
    for slug in store.pending(version, limit=limit):
        try:
            market = await read_with_retry(lambda name=slug: client.get_market(slug=name),
                                           label="5m官方结算读取", attempts=2, timeout_seconds=4)
        except ReadUnavailable:
            continue
        result = resolved_market_from_market(market, s)
        if result is None or result.slug != slug or result.timeframe != "5m":
            continue
        # Do not call a merely closed 0.99/0.01 book a final result.
        final = (result.up_final_price, result.down_final_price)
        if not ((abs(final[0] - 1) <= 1e-9 and abs(final[1]) <= 1e-9)
                or (abs(final[1] - 1) <= 1e-9 and abs(final[0]) <= 1e-9)):
            continue
        if store.put_market_result(
            slug=result.slug, condition_id=result.condition_id, timeframe="5m", winner=result.winner,
            up_won=result.up_won, up_final_price=result.up_final_price,
            down_final_price=result.down_final_price, source=result.source,
        ):
            count += 1
            log.info("5m官方结算：%s → %s", slug, result.winner)
    return count


async def settlement_worker(client, s, store, version) -> None:
    while True:
        await settle_some(client, s, store, version)
        await asyncio.sleep(30)


class FastRunner:
    def __init__(self, s: Settings, store: FastStore, public: AsyncPublicClient,
                 shadow: JevShadow, console: Console, *, version: str, session: str):
        self.s, self.store, self.public, self.shadow, self.console = s, store, public, shadow, console
        self.version, self.session = version, session
        self.market = None
        self.metadata_read_at = 0.0
        self.previous_started: float | None = None

    async def read(self, slug: str) -> Candidate:
        if self.market is None or self.market.slug != slug or time.monotonic() - self.metadata_read_at > 30:
            self.market = await read_with_retry(lambda: self.public.get_market(slug=slug),
                                                label="5m市场元数据", timeout_seconds=4)
            self.metadata_read_at = time.monotonic()
        if self.market.slug != slug:
            raise ReadUnavailable("接口返回了不同市场，拒绝使用")
        reason = passes_static_filters(self.market, self.s)
        if reason:
            raise ReadUnavailable(f"市场暂不可分析：{reason}")
        book = await read_with_retry(lambda: fetch_book(self.public, self.market),
                                     label="5m最新盘口", timeout_seconds=6)
        if not market_is_current(self.market, self.s):
            raise ReadUnavailable("读取期间市场已结束，不补发旧信号")
        return Candidate(market=self.market, book=book)

    async def tick(self, *, budget_seconds: float = 8) -> None:
        started = time.monotonic()
        gap = None if self.previous_started is None else started - self.previous_started
        self.previous_started = started
        slug = current_slug()
        cand = snapshot = result = None
        quant_p = None
        status, reason = "unavailable", ""
        try:
            # Only bounded READS are inside this timeout. Never wrap a write or
            # a whole live-trading round in a retry/timeout.
            async with asyncio.timeout(budget_seconds):
                cand = await self.read(slug)
            snapshot = local_snapshot(cand, self.s, self.store)
            if not snapshot.trade_ready:
                missing = []
                if snapshot.target_price is None:
                    missing.append("缺窗口目标价")
                if snapshot.current_price is None:
                    missing.append("TWAP缺失/过期")
                if snapshot.path_features is None or not snapshot.path_features.feature_ready:
                    missing.append("raw路径预热/缺口/过期")
                if not snapshot.seconds_left:
                    missing.append("窗口已结束")
                reason = "；".join(missing)
            else:
                quant_p = quantitative_up_probability(snapshot)
                if quant_p is None:
                    reason = "无法计算有效 Z/概率"
                else:
                    result = quantitative_trade(quant_p, cand.book, self.s)
                    status = "trade_signal" if isinstance(result, Trade) else "ready"
                    reason = result.rationale if isinstance(result, Trade) else result.reason
        except (ReadUnavailable, TimeoutError) as exc:
            reason = str(exc) or "本轮读取超过时间预算；不使用旧数据"
        observed_ts = snapshot.captured_at.timestamp() if snapshot else time.time()
        payload = snapshot_payload(cand, snapshot) if snapshot is not None else {}
        payload.update(tick_gap_seconds=gap, read_compute_seconds=time.monotonic() - started)
        observation_id, cp = self.store.record(
            version=self.version, session=self.session, slug=slug,
            condition_id=cand.condition_id if cand else None, ts=observed_ts,
            seconds_left=snapshot.seconds_left if snapshot else None,
            checkpoint=checkpoint(snapshot.seconds_left) if snapshot and quant_p is not None else None,
            quant_p=quant_p, market_p=cand.book.midpoint if cand else None,
            yes_ask=cand.book.yes_ask if cand else None, no_ask=cand.book.no_ask if cand else None,
            status=status, reason=reason, payload=payload,
        )
        timestamp = datetime.now().astimezone().strftime("%H:%M:%S")
        gap_text = "首次" if gap is None else f"{gap:.1f}s"
        if quant_p is None:
            self.console.print(f"[{timestamp}] BTC 5m | 间隔 {gap_text} | 不完整：{reason}", markup=False)
            return
        assert cand is not None and snapshot is not None
        self.console.print(
            f"[{timestamp}] BTC 5m {slug} | 间隔 {gap_text} | 剩余 {snapshot.seconds_left}s | "
            f"目标 ${snapshot.target_price:.2f} 当前 ${snapshot.current_price:.2f} | "
            f"Z={snapshot.path_features.distance_z:+.2f} P(Up)={quant_p:.3f} | "
            f"Up/Down ask={cand.book.yes_ask}/{cand.book.no_ask}", markup=False,
        )
        if cp is not None:
            self.console.print(f"  checkpoint T-{cp}s 已记录，实际剩余 {snapshot.seconds_left}s", markup=False)
            self.shadow.offer(observation_id, cand, self.s, snapshot)
        if isinstance(result, Trade):
            reason = self.store.paper_order(self.version, slug, observation_id, result,
                                            max_exposure=self.s.max_open_exposure_usd)
        self.console.print(f"  {reason}", markup=False)


async def run_fast(s: Settings, console: Console, *, interval: float, version: str,
                   jev_enabled: bool = True, once: bool = False) -> None:
    if not s.dry_run or s.allowed_timeframes != "5m":
        raise ValueError("此实验只支持 BTC 5m dry-run，不支持真实下单")
    s = s.model_copy(update={"strategy_version": version})
    # Validate without opening network connections.
    next_deadline(0, 0, interval)
    actual_jev = jev_enabled and bool(s.typesafe_api_key)
    store = FastStore(s.db_path)
    tasks: list[asyncio.Task] = []
    try:
        store.ensure_experiment(version, experiment_parameters(s, interval, actual_jev))
        async with AsyncExitStack() as stack:
            public = await stack.enter_async_context(AsyncPublicClient())
            settlement_client = await stack.enter_async_context(AsyncPublicClient())
            jev = None
            if actual_jev:
                jev = await stack.enter_async_context(JevClient(
                    s.typesafe_api_key, model=s.jev_model, base_url=s.typesafe_base_url,
                    timeout=s.jev_timeout_seconds, max_retries=s.jev_max_retries,
                ))
            shadow = JevShadow(jev, store, console)
            runner = FastRunner(s, store, public, shadow, console, version=version, session=uuid.uuid4().hex)
            tasks = [asyncio.create_task(watch_chainlink_anchors(s, store), name="chainlink"),
                     asyncio.create_task(shadow.work(), name="jev-shadow"),
                     asyncio.create_task(settlement_worker(settlement_client, s, store, version), name="settlement")]
            console.print(
                f"实验 {version} | 仅 BTC 5m | 目标节拍 {interval:g}s | "
                f"Jev={'固定checkpoint异步对照' if actual_jev else '未启用'} | 仅模拟，不读取私钥下单",
                markup=False,
            )
            clock = asyncio.get_running_loop().time
            deadline = clock()
            try:
                while True:
                    for task in tasks:
                        if task.done():
                            task.result()  # Do not hide a dead listener/worker.
                            raise RuntimeError(f"任务意外结束：{task.get_name()}")
                    await runner.tick(budget_seconds=min(8.0, interval * 0.8))
                    if once:
                        break
                    deadline, skipped = next_deadline(deadline, clock(), interval)
                    if skipped:
                        console.print(f"本轮超时：跳过 {skipped} 个节拍，不重叠、不追补旧信号", markup=False)
                    await asyncio.sleep(max(0.0, deadline - clock()))
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
