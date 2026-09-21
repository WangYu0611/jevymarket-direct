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
    by_model = {row["model"]: row for row in stats["model_comparison"]}
    assert abs(by_model["量化 Φ(Z)"]["brier"] - 0.04) < 1e-9
    assert abs(by_model["Jev"]["brier"] - 0.16) < 1e-9
    assert abs(by_model["Polymarket"]["brier"] - 0.09) < 1e-9

    by_strategy = {
        row["strategy"]: row for row in stats["strategy_comparison"]
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
