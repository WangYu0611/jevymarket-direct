"""Order execution with hard caps. The only module that can spend money."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.models.clob.order_response import AcceptedOrder, RejectedOrder

from .config import Settings
from .markets import Candidate
from .signal import Trade
from .store import Store

log = logging.getLogger(__name__)


@dataclass
class Exposure:
    positions_usd: float = 0.0
    open_orders_usd: float = 0.0
    condition_ids: set[str] = field(default_factory=set)

    @property
    def total(self) -> float:
        return self.positions_usd + self.open_orders_usd


@dataclass
class Placed:
    ok: bool
    order_id: str | None
    status: str
    message: str | None = None
    raw: dict | None = None


def _has_key(s: Settings) -> bool:
    k = (s.polymarket_private_key or "").strip()
    return len(k.removeprefix("0x")) == 64


class Executor:
    """Wraps either an authenticated client (can trade) or, for --dry-run without a key,
    a public client that only reads positions for the configured wallet address."""

    def __init__(self, client: AsyncSecureClient | AsyncPublicClient, wallet: str | None,
                 settings: Settings, store: Store, dry_run: bool):
        self.client = client
        self.wallet = wallet
        self.s = settings
        self.store = store
        self.dry_run = dry_run
        self.trades_this_run = 0
        self._exposure: Exposure | None = None

    @property
    def authenticated(self) -> bool:
        return isinstance(self.client, AsyncSecureClient)

    @property
    def wallet_type(self) -> str:
        return self.client.wallet_type if self.authenticated else "address-only"

    @classmethod
    async def create(cls, settings: Settings, store: Store, dry_run: bool) -> Executor:
        if _has_key(settings):
            client = await AsyncSecureClient.create(
                private_key=settings.polymarket_private_key,
                wallet=settings.polymarket_wallet or None,
            )
            log.info("钱包 %s（%s），模拟模式=%s", client.wallet, client.wallet_type, dry_run)
            return cls(client, str(client.wallet), settings, store, dry_run)
        if not dry_run:
            raise ValueError("真实交易需要 32 字节十六进制 POLYMARKET_PRIVATE_KEY；0x…40 位的是地址，不是私钥")
        wallet = settings.polymarket_wallet or None
        log.info("未配置私钥：以只读地址模式进行模拟交易，钱包=%s", wallet or "<未配置钱包>")
        return cls(AsyncPublicClient(), wallet, settings, store, dry_run)

    async def close(self) -> None:
        await self.client.close()

    # --- state -------------------------------------------------------------

    async def exposure(self, refresh: bool = False) -> Exposure:
        if self._exposure is not None and not refresh:
            return self._exposure
        ex = Exposure()
        if self.wallet:
            async for p in self.client.list_positions(user=self.wallet, status="OPEN").iter_items():
                ex.positions_usd += float(p.current_value or 0)
                if p.condition_id:
                    ex.condition_ids.add(str(p.condition_id))
        if self.authenticated:
            async for o in self.client.list_open_orders().iter_items():
                remaining = float(o.original_size or 0) - float(o.size_matched or 0)
                if (o.side or "").upper() == "BUY":
                    ex.open_orders_usd += remaining * float(o.price or 0)
                if o.condition_id:
                    ex.condition_ids.add(str(o.condition_id))
        self._exposure = ex
        return ex

    async def collateral_balance_usd(self) -> float | None:
        if not self.authenticated:
            return None
        try:
            ba = await self.client.get_balance_allowance(asset_type="COLLATERAL")
            # balance is in USDC base units (6 dp) as a string/Decimal
            return float(ba.balance) / 1e6
        except Exception as e:  # noqa: BLE001
            log.warning("查询余额失败：%s", e)
            return None

    # --- guards ------------------------------------------------------------

    async def check(self, c: Candidate, t: Trade) -> str | None:
        """Return a refusal reason or None if the trade may go ahead."""
        if self.trades_this_run >= self.s.max_trades_per_run:
            return f"本轮最大交易数 {self.s.max_trades_per_run} 已达到"
        if t.usd > self.s.max_usd_per_trade * 1.5:
            return f"${t.usd:.2f} 超过单笔交易上限"
        if self.store.has_order_for(c.condition_id):
            return "数据库记录显示该市场已经下过单"
        ex = await self.exposure()
        if c.condition_id in ex.condition_ids:
            return "链上已有该市场敞口"
        if ex.total + t.usd > self.s.max_open_exposure_usd:
            return f"当前敞口 ${ex.total:.2f} + 本单 ${t.usd:.2f} > 上限 ${self.s.max_open_exposure_usd:.2f}"
        return None

    # --- action ------------------------------------------------------------

    async def place(self, c: Candidate, t: Trade) -> Placed:
        refusal = await self.check(c, t)
        if refusal:
            log.info("拒绝下单 %s：%s", c.slug, refusal)
            return Placed(ok=False, order_id=None, status="refused", message=refusal)

        if self.dry_run:
            self.trades_this_run += 1
            self._bump_exposure(c, t)
            self.store.log_order(
                slug=c.slug, condition_id=c.condition_id, token_id=t.token_id, outcome=t.outcome,
                side=t.side, price=t.price, size=t.size, usd=t.usd, order_id=None,
                status="dry_run", dry_run=1, response_json=None,
            )
            log.info("模拟下单：买入 %s %s 份 @ %.3f（$%.2f），市场=%s", t.outcome, t.size, t.price, t.usd, c.slug)
            return Placed(ok=True, order_id=None, status="dry_run")

        assert isinstance(self.client, AsyncSecureClient)
        resp = await self.client.place_limit_order(
            token_id=t.token_id, side="BUY", price=str(t.price), size=str(t.size)
        )
        if isinstance(resp, AcceptedOrder):
            self.trades_this_run += 1
            self._bump_exposure(c, t)
            self.store.log_order(
                slug=c.slug, condition_id=c.condition_id, token_id=t.token_id, outcome=t.outcome,
                side=t.side, price=t.price, size=t.size, usd=t.usd, order_id=str(resp.order_id),
                status=str(resp.status or "live"), dry_run=0, response_json=resp.model_dump(),
            )
            log.info("已提交 %s：买入 %s %.2f 份 @ %.3f（$%.2f），订单=%s，状态=%s", c.slug, t.outcome, t.size, t.price, t.usd, resp.order_id, resp.status)
            return Placed(ok=True, order_id=str(resp.order_id), status=str(resp.status or "live"), raw=resp.model_dump())
        assert isinstance(resp, RejectedOrder)
        self.store.log_order(
            slug=c.slug, condition_id=c.condition_id, token_id=t.token_id, outcome=t.outcome,
            side=t.side, price=t.price, size=t.size, usd=t.usd, order_id=None,
            status="rejected", dry_run=0, response_json=resp.model_dump(),
        )
        log.warning("交易所拒绝 %s：%s %s", c.slug, resp.code, resp.message)
        return Placed(ok=False, order_id=None, status="rejected", message=f"{resp.code}: {resp.message}", raw=resp.model_dump())

    def _bump_exposure(self, c: Candidate, t: Trade) -> None:
        if self._exposure is not None:
            self._exposure.open_orders_usd += t.usd
            self._exposure.condition_ids.add(c.condition_id)

    async def setup_approvals(self, attempts: int = 6) -> str:
        """Approve exchange contracts. The public Polygon RPC intermittently answers
        'Unknown block' mid-batch; already-approved items are skipped, so just retry."""
        assert isinstance(self.client, AsyncSecureClient)
        from polymarket import RequestRejectedError

        last: Exception | None = None
        for i in range(1, attempts + 1):
            try:
                handle = await self.client.setup_trading_approvals()
                await handle.wait()
                state = await self.client.get_trading_approvals_state(wallet=self.wallet)
                if state.is_fully_approved:
                    return "fully approved"
                last = RuntimeError(f"still missing {len(state.missing.erc20)} erc20 / {len(state.missing.erc1155)} erc1155")
            except RequestRejectedError as e:
                last = e
            log.warning("approvals attempt %d/%d: %s", i, attempts, last)
            await asyncio.sleep(2 * i)
        raise RuntimeError(f"approvals incomplete after {attempts} attempts: {last}")
