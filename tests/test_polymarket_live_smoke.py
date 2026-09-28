from types import SimpleNamespace

import pytest

from jevymarket.polymarket_live_smoke import (
    MAX_SMOKE_NOTIONAL_USD,
    build_smoke_order,
)


def book(**overrides):
    values = dict(
        tick_size=0.01,
        min_order_size=5,
        yes_ask=0.52,
        no_ask=0.49,
        yes_token_id="yes",
        no_token_id="no",
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_smoke_order_uses_minimum_size_low_price_and_safer_side():
    order = build_smoke_order(book())
    assert order.direction == "UP"
    assert order.token_id == "yes"
    assert str(order.price) == "0.01"
    assert str(order.size) == "5"
    assert order.notional_usd <= MAX_SMOKE_NOTIONAL_USD


def test_smoke_order_refuses_to_cross():
    with pytest.raises(ValueError, match="no_non_crossing_smoke_side"):
        build_smoke_order(book(yes_ask=0.01, no_ask=0.01))


def test_smoke_order_hard_caps_possible_fill_notional():
    with pytest.raises(ValueError, match="smoke_notional_exceeds_cap"):
        build_smoke_order(book(min_order_size=100))
