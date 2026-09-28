"""One-order Polymarket live connectivity smoke test.

This is deliberately separate from strategy execution. It submits one tiny,
post-only BUY on the current BTC 5m market at a low non-crossing price and
immediately cancels it. The purpose is to verify the real authentication,
signing, CLOB submission, order-id, query, and cancel path with tightly bounded
fill risk.

It never bypasses Polymarket geoblocking and refuses to run if account preflight
is not clean.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, Decimal

from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.models.clob.order_response import AcceptedOrder

from .config import load_settings
from .executor import builder_api_key_from_settings
from .fast_runner import current_slug
from .maker_live_canary import (
    _valid_private_key,
    account_preflight,
    cancel_known_order,
    safe_error,
    safe_response,
)
from .markets import fetch_book
from .polymarket_contract import live_sdk_contract
from .price_value_live_t120 import geoblock_check

CONFIRM = "ONE_MINIMUM_LIVE_ORDER"
MAX_SMOKE_NOTIONAL_USD = Decimal("0.25")
PREFERRED_PRICE = Decimal("0.01")


@dataclass(frozen=True)
class SmokeOrder:
    direction: str
    token_id: str
    price: Decimal
    size: Decimal
    notional_usd: Decimal


def _aligned_low_price(tick: Decimal) -> Decimal:
    if tick <= 0 or tick >= 1:
        raise ValueError("invalid_tick_size")
    target = max(tick, PREFERRED_PRICE)
    steps = (target / tick).to_integral_value(rounding=ROUND_CEILING)
    return steps * tick


def build_smoke_order(book) -> SmokeOrder:
    """Build a post-only order with tiny bounded notional.

    We choose the side whose ask is furthest above the low probe price, reducing
    the chance that the order could become marketable before cancellation.
    post_only=True is still the final exchange-side protection.
    """
    tick = Decimal(str(book.tick_size))
    minimum = Decimal(str(book.min_order_size))
    if minimum <= 0:
        raise ValueError("invalid_min_order_size")
    price = _aligned_low_price(tick)

    choices: list[tuple[Decimal, str, str]] = []
    if book.yes_ask is not None:
        ask = Decimal(str(book.yes_ask))
        if ask > price:
            choices.append((ask, "UP", str(book.yes_token_id)))
    if book.no_ask is not None:
        ask = Decimal(str(book.no_ask))
        if ask > price:
            choices.append((ask, "DOWN", str(book.no_token_id)))
    if not choices:
        raise ValueError("no_non_crossing_smoke_side")

    _, direction, token_id = max(choices, key=lambda row: row[0])
    notional = price * minimum
    if notional > MAX_SMOKE_NOTIONAL_USD:
        raise ValueError("smoke_notional_exceeds_cap")
    return SmokeOrder(
        direction=direction,
        token_id=token_id,
        price=price,
        size=minimum,
        notional_usd=notional,
    )


async def run_smoke() -> dict:
    sdk = live_sdk_contract()
    if not sdk["ok"]:
        return {"termination": "live_sdk_contract_failed", "sdk_contract": sdk}

    try:
        geo = await geoblock_check()
    except Exception as exc:
        return {
            "termination": "geoblock_unverified",
            "sdk_contract": sdk,
            "error": safe_error(exc),
        }
    if geo["blocked"]:
        return {
            "termination": "geoblocked",
            "sdk_contract": sdk,
            "geoblock": geo,
        }

    settings = load_settings()
    if not _valid_private_key(settings.polymarket_private_key):
        return {
            "termination": "private_key_missing_or_invalid",
            "sdk_contract": sdk,
            "geoblock": geo,
        }

    try:
        secure = await AsyncSecureClient.create(
            private_key=settings.polymarket_private_key,
            wallet=settings.polymarket_wallet or None,
            api_key=builder_api_key_from_settings(settings),
        )
    except Exception as exc:
        return {
            "termination": "client_create_failed",
            "sdk_contract": sdk,
            "geoblock": geo,
            "error": safe_error(exc),
        }

    report: dict = {
        "termination": "started",
        "sdk_contract": sdk,
        "geoblock": geo,
        "max_smoke_notional_usd": float(MAX_SMOKE_NOTIONAL_USD),
    }
    try:
        try:
            preflight = await account_preflight(secure)
        except Exception as exc:
            report.update(termination="preflight_failed", error=safe_error(exc))
            return report
        report["preflight"] = preflight

        if preflight["open_orders_present"]:
            report["termination"] = "existing_open_orders"
            return report
        if not preflight["trading_approved"]:
            report["termination"] = "trading_not_approved"
            return report

        slug = current_slug()
        try:
            async with AsyncPublicClient() as public:
                market = await public.get_market(slug=slug)
                book = await fetch_book(public, market)
            order = build_smoke_order(book)
        except Exception as exc:
            report.update(termination="market_or_order_build_failed", error=safe_error(exc))
            return report

        report["market"] = {
            "slug": slug,
            "condition_id": str(market.condition_id),
            "tick_size": float(book.tick_size),
            "min_order_size": float(book.min_order_size),
            "yes_ask": book.yes_ask,
            "no_ask": book.no_ask,
        }
        report["smoke_order"] = {
            **asdict(order),
            "price": str(order.price),
            "size": str(order.size),
            "notional_usd": str(order.notional_usd),
        }

        balance = preflight.get("balance_usd")
        if balance is None or Decimal(str(balance)) + Decimal("0.0000001") < order.notional_usd:
            report["termination"] = "insufficient_balance"
            return report

        try:
            positions = await secure.list_positions(
                condition_id=str(market.condition_id),
                status="OPEN",
            ).first_page()
        except Exception as exc:
            report.update(termination="position_query_failed", error=safe_error(exc))
            return report
        if positions.items:
            report["termination"] = "existing_market_position"
            return report

        # Re-check geoblock immediately before the irreversible action.
        try:
            geo_final = await geoblock_check()
        except Exception as exc:
            report.update(termination="geoblock_unverified_before_submit", error=safe_error(exc))
            return report
        if geo_final["blocked"]:
            report["termination"] = "geoblocked_before_submit"
            report["geoblock"] = geo_final
            return report

        try:
            response = await secure.place_limit_order(
                token_id=order.token_id,
                side="BUY",
                price=str(order.price),
                size=str(order.size),
                post_only=True,
            )
        except Exception as exc:
            report.update(termination="placement_exception", error=safe_error(exc))
            return report

        report["placement"] = {
            "accepted": isinstance(response, AcceptedOrder),
            "response": safe_response(response),
        }
        if not isinstance(response, AcceptedOrder):
            report["termination"] = "placement_rejected"
            return report

        order_id = str(response.order_id)
        report["order_id"] = order_id

        # Immediate cancel is part of the smoke test, not an optional cleanup.
        verified = await cancel_known_order(secure, order_id, report)
        report["cancel_verified"] = verified
        report["termination"] = "live_smoke_passed" if verified else "cancel_unverified"
        return report
    finally:
        await secure.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="提交一笔极小 post-only 真钱订单并立即撤单，用于验证 Polymarket 实盘链路"
    )
    parser.add_argument("--confirm", default="")
    args = parser.parse_args(argv)
    if args.confirm != CONFIRM:
        parser.error(f"必须显式添加 --confirm {CONFIRM}")
    result = asyncio.run(run_smoke())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get("termination") != "live_smoke_passed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
