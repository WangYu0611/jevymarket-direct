"""Fail-closed contract checks for the exact Polymarket SDK used by live trading.

The live path deliberately supports one audited SDK version at a time. Polymarket's
0.x releases may contain breaking changes, and 0.11.0 includes two protected BUY
fixes that are directly relevant to our FAK + max_price canary.
"""
from __future__ import annotations

import inspect
from importlib.metadata import PackageNotFoundError, version

from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.streams import UserSpec

AUDITED_POLYMARKET_CLIENT_VERSION = "0.11.0"


def _params(callable_obj) -> set[str]:
    return set(inspect.signature(callable_obj).parameters)


def _require_params(label: str, callable_obj, required: set[str], failures: list[str]) -> None:
    actual = _params(callable_obj)
    missing = sorted(required - actual)
    if missing:
        failures.append(f"{label}:missing={','.join(missing)}")


def live_sdk_contract() -> dict:
    """Return a secret-free compatibility report for every SDK call in the live path."""
    try:
        installed = version("polymarket-client")
    except PackageNotFoundError:
        installed = "missing"

    failures: list[str] = []
    if installed != AUDITED_POLYMARKET_CLIENT_VERSION:
        failures.append(
            f"polymarket-client:expected={AUDITED_POLYMARKET_CLIENT_VERSION},installed={installed}"
        )

    # Auth/client lifecycle.
    _require_params(
        "AsyncSecureClient.create",
        AsyncSecureClient.create,
        {"private_key", "wallet", "api_key"},
        failures,
    )

    # Account guards.
    _require_params(
        "get_balance_allowance",
        AsyncSecureClient.get_balance_allowance,
        {"asset_type"},
        failures,
    )
    _require_params(
        "list_positions",
        AsyncSecureClient.list_positions,
        {"user", "condition_id", "status"},
        failures,
    )
    _require_params(
        "list_open_orders",
        AsyncSecureClient.list_open_orders,
        {"market"},
        failures,
    )
    _require_params(
        "get_trading_approvals_state",
        AsyncSecureClient.get_trading_approvals_state,
        {"wallet"},
        failures,
    )

    # Authenticated user stream and order lifecycle.
    _require_params("UserSpec", UserSpec, {"markets"}, failures)
    _require_params("secure.subscribe", AsyncSecureClient.subscribe, {"specs"}, failures)
    _require_params(
        "place_market_order",
        AsyncSecureClient.place_market_order,
        {"asset_id", "side", "amount", "max_spend", "max_price", "order_type"},
        failures,
    )
    _require_params(
        "wait_for_order_fill_settlement",
        AsyncSecureClient.wait_for_order_fill_settlement,
        {"order", "timeout_s"},
        failures,
    )
    _require_params("get_order", AsyncSecureClient.get_order, {"order_id"}, failures)
    _require_params(
        "list_account_trades",
        AsyncSecureClient.list_account_trades,
        {"market"},
        failures,
    )

    # Public market refresh used before and after Jev.
    _require_params("get_market", AsyncPublicClient.get_market, {"slug"}, failures)
    _require_params(
        "get_order_books",
        AsyncPublicClient.get_order_books,
        {"asset_ids"},
        failures,
    )

    return {
        "expected_version": AUDITED_POLYMARKET_CLIENT_VERSION,
        "installed_version": installed,
        "ok": not failures,
        "failures": failures,
        "audited_calls": [
            "AsyncSecureClient.create",
            "get_balance_allowance",
            "list_positions(condition_id=...)",
            "list_open_orders(market=...)",
            "get_trading_approvals_state",
            "UserSpec(markets=[condition_id])",
            "AsyncSecureClient.subscribe",
            "place_market_order(asset_id=..., BUY, amount, max_spend, max_price, FAK)",
            "wait_for_order_fill_settlement",
            "get_order(order_id=...)",
            "list_account_trades(market=...)",
            "AsyncPublicClient.get_market(slug=...)",
            "AsyncPublicClient.get_order_books(asset_ids=[...])",
        ],
    }


def assert_live_sdk_contract() -> dict:
    report = live_sdk_contract()
    if not report["ok"]:
        raise RuntimeError("live_sdk_contract_failed:" + "|".join(report["failures"]))
    return report
