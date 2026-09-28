"""Read-only / sign-only Polymarket integration probe.

This module NEVER posts an order. It verifies the exact SDK contract, account
preflight, current BTC 5m market, order-book access, and market-order signing.

The 0.10 USD probe intentionally tests construction only; it is not evidence that
the exchange would accept or fill such a small order.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal

from polymarket import AsyncPublicClient, AsyncSecureClient

from .config import load_settings
from .executor import builder_api_key_from_settings
from .fast_runner import current_slug
from .maker_live_canary import _valid_private_key, account_preflight, safe_error
from .markets import fetch_book
from .polymarket_contract import live_sdk_contract
from .price_value_live_t120 import geoblock_check


def _side(book) -> tuple[str, str, float]:
    choices = []
    if book.yes_ask is not None:
        choices.append(("UP", book.yes_token_id, float(book.yes_ask)))
    if book.no_ask is not None:
        choices.append(("DOWN", book.no_token_id, float(book.no_ask)))
    if not choices:
        raise ValueError("no_ask_available")
    return min(choices, key=lambda row: row[2])


async def _sign(
    secure: AsyncSecureClient, *, asset_id: str, ask: float, amount: Decimal
) -> dict:
    try:
        signed = await secure.create_market_order(
            asset_id=asset_id,
            side="BUY",
            amount=str(amount),
            max_spend=str(amount),
            max_price=str(ask),
            order_type="FAK",
        )
    except Exception as exc:
        return {"ok": False, "error": safe_error(exc)}
    return {
        "ok": True,
        "order_type": str(signed.order_type),
        "side": str(signed.side),
        "maker_amount": int(signed.maker_amount),
        "taker_amount": int(signed.taker_amount),
    }


async def run_probe(amount: Decimal) -> dict:
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
        return {"termination": "geoblocked", "sdk_contract": sdk, "geoblock": geo}

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

    try:
        try:
            preflight = await account_preflight(secure)
        except Exception as exc:
            return {
                "termination": "preflight_failed",
                "sdk_contract": sdk,
                "geoblock": geo,
                "error": safe_error(exc),
            }

        slug = current_slug()
        async with AsyncPublicClient() as public:
            market = await public.get_market(slug=slug)
            book = await fetch_book(public, market)
        direction, asset_id, ask = _side(book)

        ten_cent = await _sign(
            secure, asset_id=asset_id, ask=ask, amount=amount
        )

        # Official SDK examples use the market's minimum_order_size as the BUY
        # amount for a sign-only market-order example. This is still NEVER posted.
        minimum = Decimal(str(market.trading.minimum_order_size or "5"))
        valid_amount = max(minimum, Decimal("5"))
        valid = await _sign(
            secure, asset_id=asset_id, ask=ask, amount=valid_amount
        )

        return {
            "termination": "sign_probe_complete",
            "sdk_contract": sdk,
            "geoblock": geo,
            "preflight": preflight,
            "market": {
                "slug": slug,
                "direction": direction,
                "ask": ask,
                "minimum_order_size": float(minimum),
            },
            "ten_cent_probe": {
                "requested_usd": float(amount),
                "posted": False,
                "exchange_acceptance_tested": False,
                **ten_cent,
            },
            "valid_sign_probe": {
                "requested_usd": float(valid_amount),
                "posted": False,
                "exchange_acceptance_tested": False,
                **valid,
            },
        }
    finally:
        await secure.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Polymarket SDK/认证/盘口/签名探针；绝不提交订单"
    )
    parser.add_argument("--amount", type=Decimal, default=Decimal("0.10"))
    args = parser.parse_args(argv)
    if args.amount <= 0:
        parser.error("amount必须>0")
    result = asyncio.run(run_probe(args.amount))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
