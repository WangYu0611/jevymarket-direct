from types import SimpleNamespace

import pytest

from jevymarket.maker_book import BookCache
from jevymarket.maker_engine import Market, choose_quote
from jevymarket.maker_model import Estimate
from jevymarket.maker_paper_gate import evaluate_statistics
from jevymarket.maker_paper_sticky_45to30 import StickyMakerRuntime, improve_quote_one_tick, trial_config

START = 1_800_000_000
WALL = START + 260.0  # T-40
CONDITION = "0x" + "a" * 64
MARKET = Market(f"btc-updown-5m-{START}", CONDITION, START, 60, "11", "22")


def snapshot(token, bid, ask, wall):
    return {
        "event_type": "book",
        "market": CONDITION,
        "asset_id": token,
        "timestamp": str(int(wall * 1000)),
        "bids": [{"price": str(bid), "size": "2000"}],
        "asks": [{"price": str(ask), "size": "2000"}],
    }


def runtime_fixture():
    config = trial_config()
    cache = BookCache(CONDITION, {"11": (.01, 5), "22": (.01, 5)})
    cache.apply(snapshot("11", .94, .95, WALL), WALL, 100)
    cache.apply(snapshot("22", .05, .06, WALL), WALL, 100)
    estimate = Estimate(.98, WALL, WALL - .1, WALL - .1, 100, 110, 109, 2, 0)
    emitted = []
    store = SimpleNamespace(emit=lambda kind, data: emitted.append((kind, data)), load_orders=lambda: [])
    runtime = StickyMakerRuntime(config, store)
    runtime.market = MARKET
    runtime.cache = cache
    runtime.clock_ok = True
    runtime.clock_ts = WALL
    runtime.metadata_ts = WALL
    runtime.reference = SimpleNamespace(estimate=lambda *args: estimate)
    return runtime, cache, emitted



def test_one_tick_improvement_can_take_queue_front_without_crossing():
    config = trial_config()
    cache = BookCache(CONDITION, {"11": (.01, 5), "22": (.01, 5)})
    cache.apply(snapshot("11", .91, .95, WALL), WALL, 100)
    cache.apply(snapshot("22", .05, .09, WALL), WALL, 100)
    estimate = Estimate(.98, WALL, WALL - .1, WALL - .1, 100, 110, 109, 2, 0)

    base, reason = choose_quote(MARKET, cache, estimate, config, WALL, 100, 5)
    assert reason == "quote_ready"
    assert base is not None and base.price == .91

    improved = improve_quote_one_tick(base, cache, config, 5)
    assert improved is not None
    assert improved.price == .92
    assert improved.price < cache.books["11"].ask
    assert improved.fair_p - improved.price >= config.min_edge - 1e-9
    assert cache.books["11"].ahead(improved.price) == 0
    assert improved.price * improved.size <= 5 + 1e-9


def test_one_tick_improvement_never_crosses_one_tick_spread():
    config = trial_config()
    cache = BookCache(CONDITION, {"11": (.01, 5), "22": (.01, 5)})
    cache.apply(snapshot("11", .94, .95, WALL), WALL, 100)
    cache.apply(snapshot("22", .05, .06, WALL), WALL, 100)
    estimate = Estimate(.98, WALL, WALL - .1, WALL - .1, 100, 110, 109, 2, 0)

    base, reason = choose_quote(MARKET, cache, estimate, config, WALL, 100, 5)
    assert reason == "quote_ready"
    improved = improve_quote_one_tick(base, cache, config, 5)
    assert improved is not None
    assert improved.price == base.price
    assert improved.price < cache.books["11"].ask


def test_sticky_order_keeps_queue_when_new_desired_price_moves_up():
    runtime, cache, _ = runtime_fixture()
    runtime.react(WALL, 100)
    assert len(runtime.engine.orders) == 1
    runtime.react(WALL + .11, 100.11)
    order = runtime.engine.orders[0]
    assert order.state == "active"
    assert order.price == .93

    cache.apply(snapshot("11", .95, .96, WALL + .2), WALL + .2, 100.2)
    cache.apply(snapshot("22", .04, .05, WALL + .2), WALL + .2, 100.2)
    runtime.react(WALL + .2, 100.2)

    assert len(runtime.engine.orders) == 1
    assert order.state == "active"
    assert order.cancel_reason is None


def test_sticky_order_still_cancels_when_signal_becomes_unsafe():
    runtime, _, _ = runtime_fixture()
    runtime.react(WALL, 100)
    runtime.react(WALL + .11, 100.11)
    order = runtime.engine.orders[0]
    estimate = Estimate(.10, WALL + .2, WALL + .1, WALL + .1, 100, 110, 109, 2, 0)
    runtime.reference = SimpleNamespace(estimate=lambda *args: estimate)

    runtime.react(WALL + .2, 100.2)

    assert order.state == "cancel_pending"
    assert order.cancel_reason == "invalidated_quote"


def test_sticky_ttl_is_five_seconds_and_still_enforced():
    runtime, cache, _ = runtime_fixture()
    runtime.react(WALL, 100)
    runtime.react(WALL + .11, 100.11)
    order = runtime.engine.orders[0]

    now = WALL + 5.3
    cache.apply(snapshot("11", .94, .95, now), now, 105.3)
    cache.apply(snapshot("22", .05, .06, now), now, 105.3)
    runtime.clock_ts = now
    runtime.metadata_ts = now
    runtime.reference = SimpleNamespace(
        estimate=lambda *args: Estimate(.98, now, now - .1, now - .1, 100, 110, 109, 2, 0)
    )
    runtime.react(now, 105.3)

    assert runtime.c.quote_ttl_seconds == 5
    assert order.state == "cancel_pending"
    assert order.cancel_reason == "quote_ttl"


def passing_stats():
    return {
        "settled_filled_markets": 50,
        "wins_estimated": 33,
        "losses_estimated": 17,
        "gross_pnl_estimated": 4.0,
        "pnl_minus_top3_positive_contributions": 1.0,
        "uncertain_orders": 0,
        "pending_filled_orders": 0,
    }


def test_performance_gate_requires_more_than_65_percent_and_50_markets():
    result = evaluate_statistics(passing_stats())
    assert result["passed"]
    assert result["win_rate"] == pytest.approx(.66)

    too_few = passing_stats() | {"settled_filled_markets": 49, "wins_estimated": 33, "losses_estimated": 16}
    assert not evaluate_statistics(too_few)["passed"]

    exactly_65 = passing_stats() | {"settled_filled_markets": 60, "wins_estimated": 39, "losses_estimated": 21}
    assert evaluate_statistics(exactly_65)["win_rate"] == pytest.approx(.65)
    assert not evaluate_statistics(exactly_65)["passed"]


@pytest.mark.parametrize(("field", "value"), [
    ("gross_pnl_estimated", 0),
    ("pnl_minus_top3_positive_contributions", 0),
    ("uncertain_orders", 1),
    ("pending_filled_orders", 1),
])
def test_performance_gate_keeps_quality_guards(field, value):
    stats = passing_stats()
    stats[field] = value
    assert not evaluate_statistics(stats)["passed"]


def test_performance_gate_reconciles_wins_and_losses_to_markets():
    stats = passing_stats() | {"wins_estimated": 34, "losses_estimated": 17}
    result = evaluate_statistics(stats)
    assert not result["checks"]["wins_losses_reconcile"]
    assert not result["passed"]
