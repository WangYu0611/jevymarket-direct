import asyncio
import io
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from rich.console import Console
from rich.text import Text
from typer.testing import CliRunner

from jevymarket import fast_cli, fast_runner
from jevymarket.config import Settings
from jevymarket.fast_store import FastStore, probability_metrics, unit_replay
from jevymarket.fast_strategy import (
    VERSION,
    checkpoint,
    experiment_parameters,
    fast_settings,
    local_snapshot,
    next_deadline,
    quantitative_trade,
)
from jevymarket.jev import JevError
from jevymarket.network import ReadUnavailable
from jevymarket.signal import Book, JevView, Skip, Trade, evaluate


def config():
    return Settings(_env_file=None, allowed_timeframes="5m", dry_run=True)


def book():
    return Book("up", "down", .59, .60, .39, .40, .01, 5, "UP", "DOWN")


def view():
    return JevView(.7, .9, 3, 3.0, .8, "test", 0, {})


def record(st, slug="btc-updown-5m-1790010000", **kwargs):
    row = dict(version=VERSION, session="test", slug=slug, condition_id="c", ts=100,
               seconds_left=120, checkpoint=120, quant_p=.8, market_p=.595,
               yes_ask=.6, no_ask=.4, status="trade_signal", reason="test", payload={})
    row.update(kwargs)
    return st.record(**row)


def test_profile_overrides_old_environment(monkeypatch):
    monkeypatch.setenv("ALLOWED_TIMEFRAMES", "5m,15m,1h")
    monkeypatch.setenv("STRATEGY_VERSION", "v2-microstructure-checkpoints")
    monkeypatch.setenv("DRY_RUN", "false")
    s = fast_settings()
    assert s.allowed_timeframes == "5m"
    assert s.strategy_version == VERSION
    assert s.dry_run and not s.research_enabled


@pytest.mark.parametrize(("finish", "deadline", "missed"), [(2, 10, 0), (9, 10, 0), (10, 10, 0), (12, 20, 1), (31, 40, 3)])
def test_fixed_rate_does_not_add_work_time(finish, deadline, missed):
    assert next_deadline(0, finish, 10) == (deadline, missed)


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_invalid_interval(interval):
    with pytest.raises(ValueError):
        next_deadline(0, 0, interval)


@pytest.mark.parametrize(("left", "expected"), [(241, None), (240, 240), (231, 240), (230, None), (180, 180), (29, 30), (20, None), (0, None), (None, None)])
def test_checkpoint_is_not_early_or_backfilled(left, expected):
    assert checkpoint(left) == expected


def test_quant_uses_no_jev_but_keeps_old_sizing():
    s = config()
    old = evaluate(view(), book(), s, probability_yes=.8, probability_source="quant")
    new = quantitative_trade(.8, book(), s)
    assert isinstance(old, Trade) and isinstance(new, Trade)
    assert (new.outcome, new.price, new.size, new.usd) == (old.outcome, old.price, old.size, old.usd)


def test_quant_refuses_invalid_or_crossed_book():
    assert isinstance(quantitative_trade(float("nan"), book(), config()), Skip)
    crossed = Book("up", "down", .8, .6, .39, .4, .01, 5, "UP", "DOWN")
    assert isinstance(quantitative_trade(.8, crossed, config()), Skip)


def test_experiment_manifest_prevents_mixing(tmp_path):
    st = FastStore(tmp_path / "data.db")
    try:
        params = experiment_parameters(config(), 10, True)
        st.ensure_experiment(VERSION, params)
        st.ensure_experiment(VERSION, params)
        assert "api_key" not in str(params)
        with pytest.raises(ValueError):
            st.ensure_experiment(VERSION, dict(params, interval_seconds=30))
    finally:
        st.close()


def test_paper_order_unique_after_restart_and_exposure_persisted(tmp_path):
    path = tmp_path / "data.db"
    st = FastStore(path)
    trade = quantitative_trade(.8, book(), config())
    assert isinstance(trade, Trade)
    first, cp = record(st)
    assert cp == 120
    _, cp = record(st, ts=110)
    assert cp is None
    assert "模拟下单" in st.paper_order(VERSION, "m1", first, trade, max_exposure=5)
    st.close()
    st = FastStore(path)
    try:
        assert "已有" in st.paper_order(VERSION, "m1", first, trade, max_exposure=5)
        assert "敞口上限" in st.paper_order(VERSION, "m2", first, trade, max_exposure=5)
        st.put_market_result(slug="m1", condition_id="c", timeframe="5m", winner="UP", up_won=True,
                             up_final_price=1, down_final_price=0, source="test")
        assert "模拟下单" in st.paper_order(VERSION, "m2", first, trade, max_exposure=5)
        assert st.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
    finally:
        st.close()


def test_legacy_data_and_parameters_are_not_deleted(tmp_path):
    st = FastStore(tmp_path / "data.db")
    try:
        st.log_decision(slug="old", p_yes=.5, action="skip", strategy_version="v2", state_json={}, raw_json={})
        record(st)
        assert st.conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 1
        assert st.observations("v2") == []
    finally:
        st.close()


def test_local_snapshot_uses_recent_twap_only(tmp_path):
    st = FastStore(tmp_path / "data.db")
    now_ts = 1790010120.0
    slug = "btc-updown-5m-1790010000"
    cand = SimpleNamespace(slug=slug, market=SimpleNamespace(slug=slug, description="TWAP 60s", resolution=None))
    try:
        st.put_price_anchor(slug=slug, timeframe="5m", window_start=1790010000, twap_window=60,
                            price=100, observed_ts=1790010000, source="test")
        for i in range(331):
            st.put_price_sample(source="chainlink_spot", observed_ts=now_ts - 330 + i, price=100 + i * .001)
        st.put_price_sample(source="chainlink_twap", twap_window=60, observed_ts=now_ts - 1, price=100.2)
        st.put_price_sample(source="chainlink_twap", twap_window=60, observed_ts=now_ts + 30, price=999)
        snap = local_snapshot(cand, config(), st, now=datetime.fromtimestamp(now_ts, UTC))
        assert snap.current_price == 100.2 and snap.trade_ready
        stale = local_snapshot(cand, config(), st, now=datetime.fromtimestamp(now_ts + 10, UTC))
        assert stale.current_price is None and not stale.trade_ready
        assert stale.target_price == 100
    finally:
        st.close()


def test_missing_jev_is_not_zero_and_gate_has_real_effect_when_low():
    params = experiment_parameters(config(), 10, True)
    row = {"slug": "m", "quant_p": .8, "jev_p": None, "market_p": .6, "up_won": 1,
           "yes_ask": .6, "no_ask": .4, "jev_answerable": .9, "jev_clarity": 3}
    assert probability_metrics([row], "jev_p")["n"] == 0
    assert probability_metrics([row], "quant_p")["n"] == 1
    assert unit_replay([row], params) == unit_replay([row], params, jev_gate=True)
    row["jev_answerable"] = .1
    assert unit_replay([row], params, jev_gate=True)["trades"] == 0
    assert unit_replay([row], params)["trades"] == 1


async def test_slow_jev_does_not_block_ticks_or_replace_new_probability(tmp_path, monkeypatch):
    now_ts = int(time.time()) // 300 * 300 + 180
    monkeypatch.setattr(fast_runner.time, "time", lambda: now_ts)
    slug = f"btc-updown-5m-{now_ts - 180}"
    st = FastStore(tmp_path / "data.db")
    console = Console(file=io.StringIO())
    cand = SimpleNamespace(slug=slug, condition_id="c", book=book())
    snap = SimpleNamespace(trade_ready=True, target_price=100, current_price=101, seconds_left=120,
                           path_features=SimpleNamespace(distance_z=1.0),
                           captured_at=datetime.fromtimestamp(now_ts, UTC), to_state=lambda: {})
    started, release = asyncio.Event(), asyncio.Event()

    async def slow(*_args):
        started.set()
        await release.wait()
        return view()

    monkeypatch.setattr(fast_runner, "build_state", lambda *a, **k: {})
    monkeypatch.setattr(fast_runner, "ask_jev", slow)
    monkeypatch.setattr(fast_runner, "local_snapshot", lambda *a: snap)
    monkeypatch.setattr(fast_runner, "snapshot_payload", lambda *a: {})
    probabilities = iter([.8, .2])
    monkeypatch.setattr(fast_runner, "quantitative_up_probability", lambda *a: next(probabilities))
    shadow = fast_runner.JevShadow(object(), st, console)
    runner = fast_runner.FastRunner(config(), st, object(), shadow, console, version=VERSION, session="test")
    monkeypatch.setattr(runner, "read", AsyncMock(return_value=cand))
    worker = asyncio.create_task(shadow.work())
    try:
        await asyncio.wait_for(runner.tick(), timeout=1)
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(runner.tick(), timeout=1)
        rows = st.observations(VERSION)
        assert len(rows) == 2
        assert rows[0]["jev_status"] == "running"
        assert rows[1]["quant_p"] == .2 and rows[1]["jev_p"] is None
        release.set()
        await asyncio.wait_for(shadow.queue.join(), timeout=1)
        rows = st.observations(VERSION)
        assert rows[0]["jev_p"] == .7 and rows[0]["quant_p"] == .8
        assert rows[1]["jev_p"] is None
        assert len(st.orders(VERSION)) == 1
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        st.close()


async def test_jev_auth_failure_disables_shadow_not_quant(tmp_path, monkeypatch):
    st = FastStore(tmp_path / "data.db")
    first, _ = record(st)
    shadow = fast_runner.JevShadow(object(), st, Console(file=io.StringIO()))
    monkeypatch.setattr(fast_runner, "ask_jev", AsyncMock(side_effect=JevError(401, "bad key")))
    shadow.queue.put_nowait((first, {}, time.time() + 120))
    worker = asyncio.create_task(shadow.work())
    try:
        await asyncio.wait_for(shadow.queue.join(), timeout=1)
        assert shadow.disabled
        assert st.observations(VERSION)[0]["jev_status"] == "disabled_auth"
        assert st.observations(VERSION)[0]["quant_p"] == .8
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        st.close()


async def test_read_failure_records_missing_not_fake_data(tmp_path, monkeypatch):
    st = FastStore(tmp_path / "data.db")
    console = Console(file=io.StringIO())
    shadow = fast_runner.JevShadow(None, st, console)
    runner = fast_runner.FastRunner(config(), st, object(), shadow, console, version=VERSION, session="test")
    monkeypatch.setattr(runner, "read", AsyncMock(side_effect=ReadUnavailable("SSL read failed")))
    try:
        await runner.tick()
        row = st.observations(VERSION)[0]
        assert row["quant_p"] is None and row["checkpoint"] is None
        assert row["status"] == "unavailable"
        assert st.orders(VERSION) == []
    finally:
        st.close()


def test_cli_refuses_live_before_any_api_call(monkeypatch):
    run = AsyncMock()
    monkeypatch.setattr(fast_cli, "run_fast", run)
    result = CliRunner().invoke(fast_cli.app, ["run"])
    # Rich may insert styling between the hyphens and the option name.
    plain = Text.from_ansi(result.output).plain
    assert result.exit_code == 2 and "--dry-run" in plain
    run.assert_not_awaited()


def test_stats_empty_v3_preserves_v2(tmp_path, monkeypatch):
    s = config()
    s.db_path = str(tmp_path / "data.db")
    monkeypatch.setattr(fast_cli, "settings", lambda: s)
    result = CliRunner().invoke(fast_cli.app, ["stats", "--no-settle"])
    assert result.exit_code == 0
    assert "v2" in result.output


def test_single_instance_lock_is_released(tmp_path):
    path = str(tmp_path / "data.db")
    with fast_cli.single_instance(path):
        with pytest.raises(ValueError):
            with fast_cli.single_instance(path):
                pass
    with fast_cli.single_instance(path):
        pass


def test_stats_populated_experiment_without_network(tmp_path, monkeypatch):
    s = config()
    s.db_path = str(tmp_path / "data.db")
    monkeypatch.setattr(fast_cli, "settings", lambda: s)
    st = FastStore(s.db_path)
    slug = "btc-updown-5m-1790010000"
    try:
        st.ensure_experiment(VERSION, experiment_parameters(s, 10, True))
        observation_id, _ = record(st, slug)
        st.complete_jev(observation_id, view(), received_ts=105)
        trade = quantitative_trade(.8, book(), s)
        assert isinstance(trade, Trade)
        st.paper_order(VERSION, slug, observation_id, trade, max_exposure=50)
        st.put_market_result(slug=slug, condition_id="c", timeframe="5m", winner="UP", up_won=True,
                             up_final_price=1, down_final_price=0, source="test")
    finally:
        st.close()
    result = CliRunner().invoke(fast_cli.app, ["stats", "--no-settle"])
    assert result.exit_code == 0, result.exception
    assert VERSION in result.output and "Kelly" in result.output


def test_dry_run_cli_dispatches_ten_second_profile(tmp_path, monkeypatch):
    s = config()
    s.db_path = str(tmp_path / "data.db")
    monkeypatch.setattr(fast_cli, "settings", lambda: s)
    run = AsyncMock()
    monkeypatch.setattr(fast_cli, "run_fast", run)
    result = CliRunner().invoke(fast_cli.app, ["run", "--dry-run", "--loop", "10", "--once"])
    assert result.exit_code == 0, result.exception
    run.assert_awaited_once()
    assert run.await_args.kwargs["interval"] == 10
    assert run.await_args.kwargs["version"] == VERSION
    assert run.await_args.kwargs["once"]
