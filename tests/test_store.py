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
