import json
from types import SimpleNamespace

import pytest

from jevymarket import entry_ab
from jevymarket.config import Settings
from jevymarket.fast_strategy import experiment_parameters
from jevymarket.signal import Trade

START = 1790040000
SLUG = f"btc-updown-5m-{START}"


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=START - 10.0)
    monkeypatch.setattr(entry_ab.time, "time", lambda: clock.now)
    store = entry_ab.EntryStore(tmp_path / "test.db")
    params = dict(experiment_parameters(Settings(_env_file=None), 10, False), **entry_ab.PROTOCOL)
    store.ensure_experiment(entry_ab.VERSION, params)
    clock.now = START + 20
    yield store, clock
    store.close()


def observation(store, clock, *, cp=None, slug=SLUG, version=entry_ab.VERSION, valid=True):
    row, _ = store.record(
        version=version, session="test", slug=slug, condition_id="c", ts=clock.now,
        seconds_left=int(entry_ab.market_start(slug) + 300 - clock.now), checkpoint=cp,
        quant_p=.8 if valid else None, market_p=.4, yes_ask=.4, no_ask=.6,
        status="trade_signal" if valid else "unavailable", reason="test", payload={},
    )
    return row


def trade(outcome="UP", price=.4):
    return Trade(outcome, "public-token", "BUY", price, 5, round(price * 5, 4), .8, .4, "test")


def settle(store, slug=SLUG, up=True):
    store.put_market_result(slug=slug, condition_id="c", timeframe="5m", winner="UP" if up else "DOWN",
                            up_won=up, up_final_price=float(up), down_final_price=float(not up), source="test")


def test_continuous_does_not_block_checkpoint_and_restart_keeps_orders(prepared):
    st, clock = prepared
    first = observation(st, clock)
    st.paper_order(entry_ab.VERSION, SLUG, first, trade(), max_exposure=50)
    assert [o["arm"] for o in st.paired_orders(entry_ab.VERSION)] == ["continuous"]
    clock.now = START + 60
    second = observation(st, clock, cp=240)
    st.paper_order(entry_ab.VERSION, SLUG, second, trade("DOWN", .7), max_exposure=50)
    by_arm = {o["arm"]: o for o in st.paired_orders(entry_ab.VERSION)}
    assert by_arm["continuous"]["observation_id"] == first
    assert by_arm["checkpoint"]["observation_id"] == second
    assert by_arm["checkpoint"]["outcome"] == "DOWN"
    reopened = entry_ab.EntryStore(st.path)
    try:
        reopened.paper_order(entry_ab.VERSION, SLUG, second, trade("DOWN", .7), max_exposure=50)
        assert len(reopened.paired_orders(entry_ab.VERSION)) == 2
        assert reopened.conn.execute("SELECT COUNT(*) FROM fast_orders").fetchone()[0] == 0
        assert reopened.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
    finally:
        reopened.close()


def test_same_first_checkpoint_uses_same_observation_and_quote(prepared):
    st, clock = prepared
    clock.now = START + 60
    row = observation(st, clock, cp=240)
    st.paper_order(entry_ab.VERSION, SLUG, row, trade(), max_exposure=50)
    orders = st.paired_orders(entry_ab.VERSION)
    assert len(orders) == 2
    assert {o["observation_id"] for o in orders} == {row}
    assert {o["price"] for o in orders} == {.4}
    assert {o["usd"] for o in orders} == {2}


def test_accounts_have_independent_equal_exposure_limits(prepared):
    st, clock = prepared
    row = observation(st, clock)
    st.paper_order(entry_ab.VERSION, SLUG, row, trade(), max_exposure=2)
    new_slug = f"btc-updown-5m-{START + 300}"
    clock.now = START + 360
    row = observation(st, clock, slug=new_slug, cp=240)
    msg = st.paper_order(entry_ab.VERSION, new_slug, row, trade(), max_exposure=2)
    assert "本组敞口上限" in msg
    orders = st.paired_orders(entry_ab.VERSION)
    assert {(o["arm"], o["slug"]) for o in orders} == {("continuous", SLUG), ("checkpoint", new_slug)}
    settle(st)
    clock.now += 10
    row = observation(st, clock, slug=new_slug)
    st.paper_order(entry_ab.VERSION, new_slug, row, trade(), max_exposure=2)
    assert len(st.paired_orders(entry_ab.VERSION)) == 3


def test_warmup_and_stale_inputs_cannot_create_orders(prepared):
    st, clock = prepared
    old_slug = f"btc-updown-5m-{START - 300}"
    clock.now = START - 5
    row = observation(st, clock, slug=old_slug)
    assert "共同预热" in st.paper_order(entry_ab.VERSION, old_slug, row, trade(), max_exposure=50)
    clock.now = START + 20
    row = observation(st, clock)
    clock.now += 6
    assert "过期" in st.paper_order(entry_ab.VERSION, SLUG, row, trade(), max_exposure=50)
    assert st.paired_orders(entry_ab.VERSION) == []


def test_wrong_observation_or_legacy_manifest_is_rejected(prepared):
    st, clock = prepared
    row = observation(st, clock, valid=False)
    with pytest.raises(ValueError):
        st.paper_order(entry_ab.VERSION, SLUG, row, trade(), max_exposure=50)
    row = observation(st, clock, version="v3-old")
    with pytest.raises(ValueError):
        st.paper_order("v3-old", SLUG, row, trade(), max_exposure=50)
    with pytest.raises(ValueError):
        st.ensure_experiment(entry_ab.VERSION, {"changed": True})


def test_report_is_paired_with_no_trade_zero_but_pending_excluded(prepared):
    st, clock = prepared
    row = observation(st, clock)
    st.paper_order(entry_ab.VERSION, SLUG, row, trade(), max_exposure=50)
    settle(st, up=False)
    new_slug = f"btc-updown-5m-{START + 300}"
    clock.now = START + 360
    row = observation(st, clock, slug=new_slug, cp=240)
    st.paper_order(entry_ab.VERSION, new_slug, row, trade(), max_exposure=50)
    data = entry_ab.report(st, entry_ab.VERSION)
    assert data["resolved_common_markets"] == 1
    assert data["unit_paired_difference"] == 1  # CP did not enter; baseline lost $1.
    assert data["arms"]["continuous"]["settled"] == 1
    assert data["arms"]["continuous"]["pending"] == 1
    assert data["arms"]["checkpoint"]["settled"] == 0
    assert data["arms"]["checkpoint"]["pending"] == 1
    assert data["arms"]["checkpoint"]["gross_roi"] is None


def test_opposite_entries_and_unit_normalization(prepared):
    st, clock = prepared
    row = observation(st, clock)
    st.paper_order(entry_ab.VERSION, SLUG, row, trade(), max_exposure=50)
    clock.now = START + 60
    row = observation(st, clock, cp=240)
    st.paper_order(entry_ab.VERSION, SLUG, row, trade("DOWN", .5), max_exposure=50)
    settle(st, up=False)
    data = entry_ab.report(st, entry_ab.VERSION)
    assert data["opposite_direction_markets"] == 1
    assert data["unit_paired_difference"] == 2
    assert data["arms"]["continuous"]["gross_pnl"] == -2
    assert data["arms"]["checkpoint"]["gross_pnl"] == 2.5


def test_export_whitelist_and_no_overwrite(tmp_path):
    row = {"id": 1, "slug": SLUG, "api_key": "SECRET", "reason": "SECRET",
           "jev_json": '{"api_key":"SECRET"}', "payload_json": json.dumps({
               "wallet": "SECRET", "snapshot": {"api_key": "SECRET", "target_price": 100,
               "path_features": {"distance_z": 1, "token": "SECRET"}},
               "book": {"yes_ask": .4, "wallet": "SECRET"}})}
    safe = entry_ab.safe_observation(row)
    assert "SECRET" not in json.dumps(safe)
    assert safe["snapshot"]["target_price"] == 100
    path = str(tmp_path / "data.json")
    entry_ab.write_report(path, safe)
    with pytest.raises(FileExistsError):
        entry_ab.write_report(path, {})


def test_run_requires_dry_run_before_configuration():
    with pytest.raises(SystemExit) as caught:
        entry_ab.main(["run"])
    assert caught.value.code != 0


@pytest.mark.parametrize("version,interval,dry", [("v3-old", 10, True), ("v4-test", 5, True), ("v4-test", 10, False)])
async def test_invalid_run_profile_rejected_before_database_or_network(version, interval, dry):
    s = Settings(_env_file=None, dry_run=dry, allowed_timeframes="5m")
    with pytest.raises(ValueError):
        await entry_ab.run_pair(s, None, version=version, interval=interval)
