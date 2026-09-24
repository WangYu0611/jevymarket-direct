import asyncio
import sqlite3
from dataclasses import asdict, replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from jevymarket.maker import (
    TOPICS,
    MakerRuntime,
    official_winner,
    parse_market,
    public_json,
    reference_message,
)
from jevymarket.maker_book import BookCache, BookGap, trade_message
from jevymarket.maker_config import MakerConfig
from jevymarket.maker_engine import Market, PaperEngine, choose_quote
from jevymarket.maker_model import Estimate, ReferenceCache, floor_step, integral, twap_window
from jevymarket.maker_store import MakerStore, statistics

START = 1_800_000_000
WALL = START + 291.0
CONDITION = "0x" + "a" * 64
MARKET = Market(f"btc-updown-5m-{START}", CONDITION, START, 60, "11", "22")


def message(token="11", bid=.94, ask=.95, ts=WALL, qty=10):
    return {"event_type": "book", "market": CONDITION, "asset_id": token,
            "timestamp": str(int(ts * 1000)), "bids": [{"price": str(bid), "size": str(qty)}],
            "asks": [{"price": str(ask), "size": "20"}]}


def fixture():
    c = MakerConfig()
    cache = BookCache(CONDITION, {"11": (.01, 5), "22": (.01, 5)})
    cache.apply(message(), WALL, 100)
    cache.apply(message("22", .05, .06), WALL, 100)
    est = Estimate(.98, WALL, WALL-.1, WALL-.1, 100, 110, 109, 2, 0)
    q, reason = choose_quote(MARKET, cache, est, c, WALL, 100, 5)
    assert reason == "quote_ready"
    return c, cache, est, q


def trade(size=12, ts=WALL+.2, price=.93, key="a", side="SELL"):
    return dict(key=key, token="11", side=side, ts=ts, price=price, size=size)


@pytest.mark.parametrize("kwargs", [{"interval_seconds": 0}, {"interval_seconds": float("nan")},
                                    {"entry_seconds": 2}, {"max_order_usd": 6},
                                    {"watchdog_seconds": .2}, {"max_quotes_per_market": 1.5}])
def test_bad_config(kwargs):
    with pytest.raises(ValueError):
        MakerConfig(**kwargs)


def test_defaults_and_no_live_switch():
    c = MakerConfig()
    assert c.interval_seconds == 2 and c.entry_seconds == 10
    assert "live" not in asdict(c)
    assert c.reaction_budget_seconds == .1


def test_quote_not_crossing_and_small_payoff():
    c, b, e, q = fixture()
    assert q.price == .93 and q.price < b.books[q.token].ask
    assert q.size * q.price <= 5 and q.size * (1-q.price) <= .5
    assert q.fair_p == pytest.approx(.94)


@pytest.mark.parametrize("left", [0, 1, 2, 10.01, 30, 300])
def test_only_late_window(left):
    c, b, e, _ = fixture()
    assert choose_quote(MARKET, b, e, c, START+300-left, 100, 5)[0] is None


def test_model_does_not_override_market_direction():
    c, b, e, _ = fixture()
    assert choose_quote(MARKET, b, replace(e, p_up=.1), c, WALL, 100, 5)[0] is None


def test_min_size_never_rounds_up_above_budget():
    c, b, e, _ = fixture()
    assert choose_quote(MARKET, b, e, c, WALL, 100, 2)[1] == "minimum_size_exceeds_budget"


def test_stale_book_model_and_reference_rejected():
    c, b, e, _ = fixture()
    assert choose_quote(MARKET, b, e, c, WALL+1.1, 101.1, 5)[0] is None
    assert choose_quote(MARKET, b, replace(e, ts=WALL-3), c, WALL, 100, 5)[0] is None
    assert choose_quote(MARKET, b, replace(e, raw_ts=WALL-6), c, WALL, 100, 5)[0] is None


def test_snapshot_required_and_disconnect_clears_both_books():
    c, b, _, _ = fixture()
    delta = {"event_type": "price_change", "market": CONDITION, "timestamp": WALL*1000,
             "price_changes": [{"asset_id": "11", "side": "BUY", "price": ".93", "size": "20"}]}
    b.invalidate()
    with pytest.raises(BookGap):
        b.apply(delta, WALL, 100)
    assert not any(x.ready for x in b.books.values())


def test_delta_zero_deletes_and_inconsistent_bbo_fails_closed():
    c, b, _, _ = fixture()
    delta = {"event_type": "price_change", "market": CONDITION, "timestamp": WALL*1000,
             "price_changes": [{"asset_id": "11", "side": "BUY", "price": ".94", "size": "0", "best_bid": "0"}]}
    b.apply(delta, WALL, 100)
    assert b.books["11"].bid is None
    delta["price_changes"][0].update(size="10", best_bid=".5")
    with pytest.raises(BookGap):
        b.apply(delta, WALL, 100)


def test_wrong_condition_ignored_and_tick_resets_generation():
    c, b, _, _ = fixture()
    b.apply(dict(message(), market="other"), WALL, 100)
    assert b.books["11"].ready
    old = b.generation
    with pytest.raises(BookGap):
        b.apply({"event_type": "tick_size_change", "market": CONDITION, "asset_id": "11",
                 "timestamp": WALL*1000, "new_tick_size": ".001"}, WALL, 100)
    assert b.generation > old and b.books["11"].tick == .001


@pytest.mark.parametrize("modify", [{"timestamp": (WALL+1)*1000}, {"timestamp": (WALL-10)*1000},
                                    {"bids": [{"price": "NaN", "size": "5"}]}])
def test_bad_book_clock_or_nan(modify):
    _, b, _, _ = fixture()
    with pytest.raises(BookGap):
        b.apply(dict(message(), **modify), WALL, 100)
    assert not any(x.ready for x in b.books.values())


def test_fill_only_after_arrival_and_queue_consumption():
    c, b, _, q = fixture()
    engine = PaperEngine(c)
    assert engine.submit(MARKET, b, q, WALL, 100)
    engine.on_trade(trade(), WALL+.02)
    assert engine.orders[0].filled == 0
    engine.advance(b, WALL+.11, 100.11, True)
    engine.on_trade(trade(key="b", size=8), WALL+.2)
    assert engine.orders[0].filled == 0 and engine.orders[0].queue_ahead == 2
    engine.on_trade(trade(key="c", size=4, ts=WALL+.3), WALL+.3)
    assert engine.orders[0].filled == 2
    engine.on_trade(trade(key="c", size=4, ts=WALL+.3), WALL+.3)
    assert engine.orders[0].filled == 2  # duplicate print


def test_touch_and_buy_aggressor_never_fill():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    b.books["11"].asks = {.93: 100}
    e.on_trade(trade(side="BUY"), WALL+.2)
    assert e.orders[0].filled == 0


def test_cancel_inflight_reserves_risk_and_allows_fill():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    reserved = e.exposure()
    e.cancel("test", WALL+.12, 100.12)
    assert not e.submit(MARKET, b, q, WALL+.2, 100.2)
    assert e.exposure() == reserved
    e.on_trade(trade(size=12, ts=WALL+.15), WALL+.15)
    assert e.orders[0].filled == 2
    e.advance(b, WALL+.23, 100.23, True)
    assert e.active() is None and e.exposure() == pytest.approx(2*q.price)
    assert not e.submit(MARKET, b, q, WALL+.4, 100.4)  # do not add after partial fill


def test_submit_can_arrive_during_pending_cancel():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.cancel("signal_reversed", WALL+.05, 100.05)
    e.advance(b, WALL+.11, 100.11, False)
    assert e.orders[0].active_ts is not None and e.orders[0].state == "cancel_pending"
    e.on_trade(trade(ts=WALL+.12), WALL+.12)
    assert e.orders[0].filled == 2
    e.advance(b, WALL+.16, 100.16, False)
    assert e.orders[0].state == "cancelled"


def test_crossing_at_arrival_rejected_no_taker_fallback():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    b.books["11"].bids = {.92: 10}
    b.books["11"].asks = {.93: 10}
    e.advance(b, WALL+.11, 100.11, True)
    assert e.orders[0].state == "rejected" and e.orders[0].filled == 0


def test_unobservable_arrival_and_late_trade_halt():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    b.invalidate()
    e.advance(b, WALL+.11, 100.11, False)
    assert e.halted and e.orders[0].uncertain


def test_late_cancel_race_not_silently_unfilled():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.cancel("test", WALL+.12, 100.12)
    e.advance(b, WALL+.23, 100.23, True)
    e.on_trade(trade(ts=WALL+.15), WALL+.3)
    assert e.orders[0].uncertain and e.halted and e.exposure() > 0


def test_ttl_end_cutoff_and_idempotent_settlement():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.on_trade(trade(size=50), WALL+.2)
    o = e.orders[0]
    e.settle(MARKET.slug, "UP", MARKET.end+1)
    first = o.pnl
    e.settle(MARKET.slug, "UP", MARKET.end+2)
    assert o.pnl == first and first <= .5
    with pytest.raises(ValueError):
        e.settle(MARKET.slug, "DOWN", MARKET.end+3)
    assert e.halted


def test_restart_preserves_fills_and_marks_unknown():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.on_trade(trade(), WALL+.2)
    restored = PaperEngine(c)
    restored.restore([asdict(o) for o in e.orders], WALL+.5)
    assert restored.orders[0].filled == 2 and restored.halted
    assert restored.orders[0].state == "interrupted"


def test_daily_loss_limit_includes_pending_and_unknown_risk():
    c, b, _, q = fixture()
    e = PaperEngine(replace(c, daily_loss_limit_usd=4))
    assert not e.submit(MARKET, b, q, WALL, 100)
    assert e.budget(MARKET.slug, WALL) == 4


def test_reference_causality_and_exact_anchor():
    c = MakerConfig()
    r = ReferenceCache(c)
    for t in range(START-200, START+293):
        price = 100.0 if t < START+100 else 110.0
        assert r.add("raw", t, price, t+.01)
    assert r.add("twap60", START, 100, START+.01)
    assert (START, 60) in r.anchors
    assert r.add("twap60", START+291, 110, START+291.01)
    e = r.estimate(START, 60, START+292.1)
    assert .92 < e.p_up <= .995 and e.final_mean == pytest.approx(110)
    assert not r.add("raw", START+292, 999, START+293)  # duplicate
    assert not r.add("raw", START+299, 999, START+293)  # future
    assert not r.add("raw", START+293, 999, START+299)  # stale


def test_no_default_target_or_unknown_resolution():
    assert twap_window("60-second TWAP", "Chainlink") == 60
    assert twap_window("TWAP-30s", "Chainlink") == 30
    for desc in ("BTC goes up", "30-second and 60-second TWAP"):
        with pytest.raises(ValueError):
            twap_window(desc, "Chainlink")
    with pytest.raises(ValueError):
        ReferenceCache(MakerConfig()).estimate(START, 60, WALL)


def test_integral_and_gap_guard():
    rows = [(0, 10), (1, 20), (2, 30)]
    assert integral(rows, .5, 2, 1) == 25
    with pytest.raises(ValueError):
        integral(rows, 0, 5, 1)
    assert floor_step(.9325, .0025) == .9325
    assert floor_step(.9399, .01) == .93


def metadata():
    return {"slug": MARKET.slug, "active": True, "closed": False, "acceptingOrders": True,
            "endDate": datetime.fromtimestamp(MARKET.end, UTC).isoformat(),
            "outcomes": '["Down","Up"]', "clobTokenIds": '["22","11"]',
            "conditionId": CONDITION, "description": "60-second TWAP", "resolutionSource": "Chainlink"}


def test_metadata_mapping_and_official_only():
    d = metadata()
    assert parse_market(d, MARKET.slug) == MARKET
    d.update(closed=True, outcomePrices='["0.01","0.99"]')
    assert official_winner(d, MARKET.slug, CONDITION, MARKET.end+1) is None
    d['outcomePrices'] = '["0","1"]'
    assert official_winner(d, MARKET.slug, CONDITION, MARKET.end+1) == "UP"
    assert official_winner(d, MARKET.slug, "wrong", MARKET.end+1) is None
    assert official_winner(d, MARKET.slug, CONDITION, MARKET.end-1) is None


def test_reference_contract_payload_clock_not_envelope():
    msg = {"topic": TOPICS["twap30"], "type": "update", "timestamp": WALL*1000,
           "payload": {"symbol": "btc/usd", "timestamp": (WALL-1)*1000,
                       "value": 10, "full_accuracy_value": "10010000000000000000", "window_s": 30}}
    ts, price = reference_message(msg, TOPICS["twap30"])
    assert ts == WALL-1 and price == 10.01
    del msg["payload"]["timestamp"]
    with pytest.raises(ValueError):
        reference_message(msg, TOPICS["twap30"])


def test_trade_contract_and_age():
    msg = dict(trade(), event_type="last_trade_price", asset_id="11", market=CONDITION,
               timestamp=(WALL+.2)*1000)
    assert trade_message(msg, CONDITION, WALL+.3)["side"] == "SELL"
    assert trade_message(msg, CONDITION, WALL+2) is None


def test_store_rejects_old_db_before_mutation(tmp_path):
    path = tmp_path / "old.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE fast_orders(id INTEGER)")
    before = path.read_bytes()
    with pytest.raises(ValueError):
        MakerStore(path, MakerConfig())
    assert path.read_bytes() == before


def test_journal_readonly_stats_and_compressed_export(tmp_path):
    path = tmp_path / "maker.db"
    c, b, _, q = fixture()
    store = MakerStore(path, c)
    engine = PaperEngine(c, store.emit)
    engine.submit(MARKET, b, q, WALL, 100)
    async def flush():
        store.stopping = True
        await store.writer()
    asyncio.run(flush())
    report = statistics(path, tmp_path/"out.json.gz")
    assert report["quotes"] == 1 and report["filled_orders_estimated"] == 0
    assert report["top1_share_net_profit"] is None and report["confirmed_rebates_usd"] == 0
    with pytest.raises(FileExistsError):
        statistics(path, tmp_path/"out.json.gz")
    assert len(store.load_orders()) == 1
    with pytest.raises(ValueError):
        MakerStore(path, replace(c, interval_seconds=3))


def test_controller_cancels_without_waiting_for_two_second_tick():
    c, b, est, q = fixture()
    emitted = []
    fake_store = SimpleNamespace(emit=lambda kind, data: emitted.append((kind, data)), load_orders=lambda: [])
    rt = MakerRuntime(c, fake_store)
    rt.market, rt.cache, rt.clock_ok, rt.clock_ts, rt.metadata_ts = MARKET, b, True, WALL, WALL
    rt.reference = SimpleNamespace(estimate=lambda *a: est)
    rt.react(WALL, 100)
    assert rt.engine.active() is not None
    rt.react(WALL+.11, 100.11)
    rt.reference = SimpleNamespace(estimate=lambda *a: replace(est, p_up=.1))
    rt.react(WALL+.12, 100.12)
    assert rt.engine.active().state == "cancel_pending"
    assert any(k == "execution" and v["event"] == "cancel_intent" for k, v in emitted)


def test_public_transport_cannot_submit_live_order():
    async def run():
        with pytest.raises(ValueError):
            await public_json(None, "https://clob.polymarket.com/order")
    asyncio.run(run())


def test_broker_rejects_mismatched_outcome_and_fractional_quantum():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    assert not e.submit(MARKET, b, replace(q, outcome="DOWN"), WALL, 100)
    assert not e.submit(MARKET, b, replace(q, size=5.001), WALL, 100)
    assert not e.submit(MARKET, b, replace(q, fair_p=2), WALL, 100)
    assert e.submit(MARKET, b, q, WALL, 100)
    assert e.orders[0].post_only and e.orders[0].order_type == "GTC"


def test_quote_ttl_and_end_cutoff_really_cancel():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    assert e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.advance(b, WALL+2.2, 102.2, True)
    assert e.orders[0].state == "cancel_pending" and e.orders[0].cancel_reason == "quote_ttl"
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.advance(b, MARKET.end-2, 107, True)
    assert e.orders[0].state == "cancel_pending" and e.orders[0].cancel_reason == "risk_or_end_cutoff"


def test_cancel_ack_required_before_requote_releases_reservation():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.cancel("reprice", WALL+.2, 100.2)
    assert not e.submit(MARKET, b, q, WALL+.25, 100.25)
    e.advance(b, WALL+.31, 100.31, True)
    assert e.submit(MARKET, b, q, WALL+.31, 100.31)
    assert len(e.orders) == 2


def test_unknown_settlement_uses_worst_case_loss_for_future_budget():
    c, b, _, q = fixture()
    e = PaperEngine(c)
    e.submit(MARKET, b, q, WALL, 100)
    e.advance(b, WALL+.11, 100.11, True)
    e.cancel("lost_connection", WALL+.2, 100.2)
    e.orders[0].uncertain = True
    e.advance(b, WALL+.31, 100.31, False)
    e.settle(MARKET.slug, "DOWN", MARKET.end+1)
    assert e.risk_pnl(e.orders[0]) == pytest.approx(-q.price*q.size)
    assert e.report()["uncertain_settled_pnl_lower"] < 0
    assert not e.halted  # official result resolves possible directional liability


def test_invalid_condition_hex_refused():
    d = metadata()
    d["conditionId"] = "0x" + "z"*64
    with pytest.raises(ValueError):
        parse_market(d, MARKET.slug)
