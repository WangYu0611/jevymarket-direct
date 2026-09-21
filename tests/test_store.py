import time

from jevymarket.store import Store


def test_brief_cache_ttl(tmp_path):
    st = Store(tmp_path / "t.db")
    assert st.get_brief("slug", 3600) is None
    st.put_brief("slug", "m", {"summary": "s", "sources": ["u"]}, 0.01)
    hit = st.get_brief("slug", 3600)
    assert hit and hit["summary"] == "s" and "_cached_ts" in hit
    # Expired
    st.conn.execute("UPDATE research SET ts = ?", (time.time() - 7200,))
    st.conn.commit()
    assert st.get_brief("slug", 3600) is None
    s = st.stats()
    assert s["briefs"] == 1 and s["research_cost_usd"] == 0.01


def test_decision_and_order_logging(tmp_path):
    st = Store(tmp_path / "t.db")
    st.log_decision(slug="a", condition_id="c1", p_yes=0.6, midpoint=0.5, action="trade",
                    jev_cost=0.0001, research_cost=0.01, state_json={"q": 1}, raw_json={"x": 1})
    st.log_order(slug="a", condition_id="c1", token_id="t", outcome="YES", side="BUY",
                 price=0.5, size=10, usd=5.0, order_id="o1", status="live", dry_run=0)
    assert st.has_order_for("c1")
    assert not st.has_order_for("c2")
    st.log_order(slug="b", condition_id="c2", token_id="t", outcome="YES", side="BUY",
                 price=0.5, size=10, usd=5.0, order_id=None, status="dry_run", dry_run=1)
    assert not st.has_order_for("c2")
    assert st.has_order_for("c2", include_dry_run=True)
    s = st.stats()
    assert s["decisions"] == 1 and s["live_orders"] == 1 and s["live_usd"] == 5.0



def test_resolved_model_and_strategy_metrics(tmp_path):
    st = Store(tmp_path / "t.db")
    slug = "btc-updown-5m-1789980000"

    st.log_decision(
        slug=slug,
        condition_id="c",
        p_yes=0.80,
        jev_p_yes=0.60,
        timeframe="5m",
        midpoint=0.70,
        yes_ask=0.70,
        no_ask=0.31,
        answerable=0.90,
        clarity=3,
        action="trade",
        state_json={
            "timeframe": "5m",
            "market_end_time": "2026-09-21T08:45:00+00:00",
            "quantitative_signal": {"p_up": 0.80},
        },
        raw_json={"answers": {"resolves_yes": {"noul": 0.60}}},
    )
    st.put_market_result(
        slug=slug,
        condition_id="c",
        timeframe="5m",
        winner="UP",
        up_won=True,
        up_final_price=1.0,
        down_final_price=0.0,
        source="test",
    )

    # Old versions could create repeated dry-run orders for the same market.
    st.log_order(
        slug=slug,
        condition_id="c",
        token_id="up",
        outcome="UP",
        side="BUY",
        price=0.70,
        size=1 / 0.70,
        usd=1.0,
        order_id=None,
        status="dry_run",
        dry_run=1,
        strategy_version="v2",
    )
    st.log_order(
        slug=slug,
        condition_id="c",
        token_id="up",
        outcome="UP",
        side="BUY",
        price=0.80,
        size=1 / 0.80,
        usd=1.0,
        order_id=None,
        status="dry_run",
        dry_run=1,
    )

    stats = st.stats()
    by_model = {
        row["model"]: row
        for row in stats["model_comparison"]
        if row["timeframe"] == "全部"
    }
    assert abs(by_model["量化 Φ(Z)"]["brier"] - 0.04) < 1e-9
    assert abs(by_model["Jev"]["brier"] - 0.16) < 1e-9
    assert abs(by_model["Polymarket"]["brier"] - 0.09) < 1e-9

    by_strategy = {
        row["strategy"]: row
        for row in stats["strategy_comparison"]
        if row["timeframe"] == "全部"
    }
    assert by_strategy["A 纯Φ(Z)"]["trades"] == 1
    assert by_strategy["A 纯Φ(Z)"]["wins"] == 1
    assert by_strategy["B Φ(Z)+Jev门控"]["wins"] == 1
    assert by_strategy["C Jev概率"]["trades"] == 1
    assert by_strategy["C Jev概率"]["wins"] == 0

    perf = stats["dry_run_performance"]
    assert perf["trades"] == 1
    assert perf["wins"] == 1
    assert perf["pnl_usd"] > 0
    st.close()



def test_evaluation_checkpoint_is_unique_per_version(tmp_path):
    st = Store(tmp_path / "t.db")
    kwargs = dict(
        slug="btc-updown-5m-1",
        condition_id="c",
        timeframe="5m",
        strategy_version="v2",
        checkpoint_seconds=120,
        seconds_left=115,
        quant_p=0.6,
        jev_p=0.55,
        jev_answerable=0.9,
        jev_clarity=3,
        market_p=0.58,
        yes_ask=0.59,
        no_ask=0.42,
        book_json={"directional_imbalance_5c": 0.2},
        state_json={"timeframe": "5m"},
    )
    assert st.log_evaluation_sample(**kwargs)
    assert not st.log_evaluation_sample(**kwargs)
    assert st.evaluation_sample_count("v2") == 1

    kwargs["strategy_version"] = "v3"
    assert st.log_evaluation_sample(**kwargs)
    assert st.evaluation_sample_count() == 2
    st.close()



def test_clear_experiment_data_preserves_authoritative_and_live_data(tmp_path):
    st = Store(tmp_path / "t.db")
    st.log_decision(
        slug="old",
        condition_id="c-old",
        p_yes=0.5,
        action="skip",
        strategy_version="legacy",
        state_json={},
        raw_json={},
    )
    st.log_evaluation_sample(
        slug="old",
        condition_id="c-old",
        timeframe="5m",
        strategy_version="v2",
        checkpoint_seconds=120,
        seconds_left=118,
        quant_p=0.5,
        jev_p=0.5,
        jev_answerable=0.9,
        jev_clarity=3,
        market_p=0.5,
        yes_ask=0.51,
        no_ask=0.50,
        book_json={},
        state_json={},
    )
    st.log_order(
        slug="old",
        condition_id="c-old",
        token_id="up",
        outcome="UP",
        side="BUY",
        price=0.5,
        size=2,
        usd=1,
        order_id=None,
        status="dry_run",
        dry_run=1,
        strategy_version="v2",
    )
    st.log_order(
        slug="live",
        condition_id="c-live",
        token_id="up",
        outcome="UP",
        side="BUY",
        price=0.5,
        size=2,
        usd=1,
        order_id="live-1",
        status="live",
        dry_run=0,
        strategy_version="v2",
    )
    st.put_market_result(
        slug="old",
        condition_id="c-old",
        timeframe="5m",
        winner="UP",
        up_won=True,
        up_final_price=1.0,
        down_final_price=0.0,
        source="official",
    )
    st.put_price_anchor(
        slug="btc-updown-5m-1",
        timeframe="5m",
        window_start=1.0,
        twap_window=60,
        price=100.0,
        observed_ts=1.0,
        source="test",
    )
    st.put_price_sample(
        source="chainlink_spot",
        price=100.0,
        observed_ts=time.time(),
    )

    counts = st.clear_experiment_data()
    assert counts == {
        "decisions": 1,
        "evaluation_samples": 1,
        "dry_run_orders": 1,
    }
    assert st.conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
    assert st.evaluation_sample_count() == 0
    assert st.conn.execute(
        "SELECT COUNT(*) FROM orders WHERE dry_run = 1"
    ).fetchone()[0] == 0
    assert st.conn.execute(
        "SELECT COUNT(*) FROM orders WHERE dry_run = 0"
    ).fetchone()[0] == 1
    assert st.get_market_result("old") is not None
    assert st.get_price_anchor("btc-updown-5m-1", 60) is not None
    assert st.get_price_samples(
        source="chainlink_spot",
        since_ts=time.time() - 60,
    )
    st.close()


def test_experiment_stats_are_version_isolated(tmp_path):
    st = Store(tmp_path / "t.db")
    st.put_market_result(
        slug="m-v2",
        condition_id="c-v2",
        timeframe="5m",
        winner="UP",
        up_won=True,
        up_final_price=1.0,
        down_final_price=0.0,
        source="official",
    )
    st.put_market_result(
        slug="m-old",
        condition_id="c-old",
        timeframe="5m",
        winner="DOWN",
        up_won=False,
        up_final_price=0.0,
        down_final_price=1.0,
        source="official",
    )

    for version, slug, quant_p, jev_p, market_p in (
        ("v2", "m-v2", 0.8, 0.7, 0.75),
        ("legacy", "m-old", 0.9, 0.9, 0.9),
    ):
        st.log_decision(
            slug=slug,
            condition_id=f"c-{version}",
            p_yes=quant_p,
            jev_p_yes=jev_p,
            timeframe="5m",
            strategy_version=version,
            answerable=0.9,
            clarity=3,
            yes_ask=0.6,
            no_ask=0.41,
            midpoint=market_p,
            action="skip",
            state_json={"quantitative_signal": {"p_up": quant_p}},
            raw_json={},
        )
        st.log_evaluation_sample(
            slug=slug,
            condition_id=f"c-{version}",
            timeframe="5m",
            strategy_version=version,
            checkpoint_seconds=120,
            seconds_left=120,
            quant_p=quant_p,
            jev_p=jev_p,
            jev_answerable=0.9,
            jev_clarity=3,
            market_p=market_p,
            yes_ask=0.6,
            no_ask=0.41,
            book_json={},
            state_json={},
        )

    stats = st.experiment_stats("v2")
    assert stats["strategy_version"] == "v2"
    assert stats["evaluation_samples"] == 1
    assert stats["resolved_markets"] == 1

    overall = {
        row["model"]: row
        for row in stats["model_comparison"]
        if row["timeframe"] == "全部"
    }
    assert overall["量化 Φ(Z)"]["n"] == 1
    assert abs(overall["量化 Φ(Z)"]["brier"] - 0.04) < 1e-9
    st.close()
