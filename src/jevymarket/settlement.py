"""Backfill resolved short-term market outcomes from Polymarket's official Gamma API."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from polymarket import AsyncPublicClient
from polymarket.models.gamma.market import Market

from .config import Settings
from .markets import market_timeframe
from .store import Store

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedMarket:
    slug: str
    condition_id: str | None
    timeframe: str | None
    winner: str
    up_won: bool
    up_final_price: float
    down_final_price: float
    source: str = "Polymarket Gamma via official SDK"


def resolved_market_from_market(market: Market, settings: Settings) -> ResolvedMarket | None:
    """Return a trustworthy binary result only after Gamma shows a closed 1/0 market."""
    if not market.slug or not market.state.closed:
        return None

    yes = market.outcomes.yes
    no = market.outcomes.no
    if yes.price is None or no.price is None:
        return None

    yes_price = float(yes.price)
    no_price = float(no.price)

    # Resolved binary markets converge to 1/0. A small tolerance avoids Decimal/API
    # representation quirks while refusing ambiguous merely-closed markets.
    if yes_price >= 0.99 and no_price <= 0.01:
        winner = str(yes.label)
    elif no_price >= 0.99 and yes_price <= 0.01:
        winner = str(no.label)
    else:
        return None

    yes_label = str(yes.label or "").strip().upper()
    no_label = str(no.label or "").strip().upper()
    winner_upper = winner.strip().upper()

    if yes_label == "UP":
        up_won = winner_upper == yes_label
        up_price, down_price = yes_price, no_price
    elif no_label == "UP":
        up_won = winner_upper == no_label
        up_price, down_price = no_price, yes_price
    else:
        return None

    return ResolvedMarket(
        slug=str(market.slug),
        condition_id=str(market.condition_id) if market.condition_id else None,
        timeframe=market_timeframe(market, settings),
        winner=winner_upper,
        up_won=up_won,
        up_final_price=up_price,
        down_final_price=down_price,
    )


async def settle_pending_markets(
    client: AsyncPublicClient,
    settings: Settings,
    store: Store,
    *,
    limit: int = 100,
    grace_seconds: float = 30.0,
) -> int:
    """Fetch finished markets not yet in the local result table and persist resolutions."""
    slugs = store.pending_short_market_slugs(
        now_ts=datetime.now(UTC).timestamp(),
        grace_seconds=grace_seconds,
        limit=limit,
    )
    inserted = 0

    for slug in slugs:
        try:
            market = await client.get_market(slug=slug)
        except Exception as exc:  # noqa: BLE001
            log.debug("结算回填暂时无法读取 %s：%s", slug, exc)
            continue

        result = resolved_market_from_market(market, settings)
        if result is None:
            continue

        if store.put_market_result(
            slug=result.slug,
            condition_id=result.condition_id,
            timeframe=result.timeframe,
            winner=result.winner,
            up_won=result.up_won,
            up_final_price=result.up_final_price,
            down_final_price=result.down_final_price,
            source=result.source,
        ):
            inserted += 1
            log.info(
                "已回填结算：%s → %s（Up=%s）",
                result.slug,
                result.winner,
                result.up_won,
            )

    return inserted
