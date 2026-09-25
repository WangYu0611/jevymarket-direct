import asyncio
import json
from types import SimpleNamespace

import pytest

from jevymarket import maker_live_canary as canary


def test_private_key_validation():
    assert canary._valid_private_key("0x" + "a" * 64)
    assert canary._valid_private_key("B" * 64)
    assert not canary._valid_private_key("0x1234")
    assert not canary._valid_private_key("z" * 64)


def test_live_mode_requires_exact_confirmation_before_any_async_work(monkeypatch):
    called = False

    def fail_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("async_main must not run")

    monkeypatch.setattr(canary.asyncio, "run", fail_run)
    with pytest.raises(SystemExit):
        canary.main(["--live-one"])
    assert not called



def test_live_mode_requires_passing_paper_report_before_async_work(tmp_path, monkeypatch):
    report_path = tmp_path / "paper.json"
    report_path.write_text(json.dumps({
        "paper_only": True,
        "statistics": {
            "settled_filled_markets": 10,
            "wins_estimated": 8,
            "losses_estimated": 2,
            "gross_pnl_estimated": 5,
            "pnl_minus_top3_positive_contributions": 1,
            "uncertain_orders": 0,
            "pending_filled_orders": 0,
        },
    }), encoding="utf-8")
    called = False

    def fail_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("async_main must not run before paper gate passes")

    monkeypatch.setattr(canary.asyncio, "run", fail_run)
    with pytest.raises(SystemExit):
        canary.main([
            "--live-one",
            "--confirm", canary.CONFIRM_PHRASE,
            "--paper-report", str(report_path),
        ])
    assert not called


def test_safe_user_event_filters_unrelated_and_removes_owner_fields():
    payload = SimpleNamespace(
        id="ORDER1", status="LIVE", order_event_type="PLACEMENT",
        size_matched="0", price="0.95", timestamp="2026-01-01T00:00:00Z",
        owner="SECRET_API_KEY", maker_address="0xsecret",
    )
    row = canary.safe_user_event(SimpleNamespace(type="order", payload=payload), "ORDER1")
    assert row["id"] == "ORDER1" and row["status"] == "LIVE"
    assert "owner" not in row and "maker_address" not in row
    assert canary.safe_user_event(SimpleNamespace(type="order", payload=payload), "OTHER") is None


def test_trade_event_matches_maker_order():
    maker = SimpleNamespace(order_id="ORDER1", matched_amount="2.5", price="0.95")
    payload = SimpleNamespace(
        id="TRADE1", taker_order_id="OTHER", maker_orders=(maker,),
        status="TRADE_STATUS_MATCHED", trader_side="MAKER",
        timestamp="2026-01-01T00:00:00Z", size="9", price="0.90",
        owner="SECRET",
    )
    row = canary.safe_user_event(SimpleNamespace(type="trade", payload=payload), "ORDER1")
    assert row["type"] == "trade"
    assert row["size"] == 2.5 and row["price"] == .95
    assert "owner" not in row


def test_invalid_candidate_never_calls_private_client():
    candidate = canary.CandidateSnapshot(
        slug="s", condition="c", token="1", outcome="UP",
        price=.95, size=5, fair_p=.97, generation=1,
        observed_wall=1, market_end=100,
    )
    class Runtime:
        def active_order_safe(self, *args):
            return False, "unsafe"

    class Client:
        async def place_limit_order(self, **kwargs):
            raise AssertionError("must not place")

    report = {}
    asyncio.run(canary.execute_one(Client(), Runtime(), candidate, report))
    assert report["termination"] == "candidate_invalid_before_submit"


class FakeStream:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(3600)
        raise StopAsyncIteration


class FakeAccepted:
    def __init__(self):
        self.order_id = "ORDER1"
        self.status = "live"
        self.trade_ids = ()
        self.transactions_hashes = ()


class FakeClient:
    def __init__(self):
        self.place_kwargs = None
        self.cancel_calls = 0
        self.cancelled = False

    async def subscribe(self, spec):
        return FakeStream()

    async def place_limit_order(self, **kwargs):
        self.place_kwargs = kwargs
        return FakeAccepted()

    async def get_order(self, order_id):
        return SimpleNamespace(
            status="CANCELED" if self.cancelled else "LIVE",
            price="0.95", original_size="5", size_matched="0",
        )

    async def cancel_order(self, order_id):
        self.cancel_calls += 1
        self.cancelled = True
        return SimpleNamespace()


def test_execute_one_is_post_only_and_verifies_cancel(monkeypatch):
    monkeypatch.setattr(canary, "AcceptedOrder", FakeAccepted)

    candidate = canary.CandidateSnapshot(
        slug="s", condition="c", token="1", outcome="UP",
        price=.95, size=5, fair_p=.97, generation=1,
        observed_wall=1, market_end=10_000,
    )

    class Runtime:
        calls = 0
        def active_order_safe(self, *args):
            self.calls += 1
            return (True, "safe") if self.calls == 1 else (False, "signal_invalid")

    client = FakeClient()
    report = {}
    asyncio.run(canary.execute_one(client, Runtime(), candidate, report))

    assert client.place_kwargs["post_only"] is True
    assert client.place_kwargs["side"] == "BUY"
    assert client.cancel_calls >= 1
    assert report["cancel_verified"] is True
    assert report["termination"] == "cancel_verified"


def test_stake_cap_is_hard():
    candidate = canary.CandidateSnapshot(
        slug="s", condition="c", token="1", outcome="UP",
        price=.99, size=6, fair_p=.999, generation=1,
        observed_wall=1, market_end=100,
    )
    assert candidate.stake_usd > canary.MAX_ORDER_USD
