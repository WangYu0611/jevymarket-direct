"""Deterministic clock-domain and integrated error-diagnostic regressions."""
import asyncio
import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest

from jevymarket.maker_config import MakerConfig
from jevymarket.maker_diagnostics import ReactionWindow, diagnostic_report, merge_reactions
from jevymarket.maker_precision import (
    MEASUREMENT_REVISION,
    TransportFailures,
    clock_receipt,
    run_reaction,
)


class Clock:
    def __init__(self, stamps):
        self.stamps = iter(stamps)

    def perf_counter_ns(self):
        return next(self.stamps)

    def monotonic(self):
        return 65536.0  # Deliberately unrelated origin; no progress at sub-tick durations.

    def time(self):
        return 1_800_000_000.0

    def get_clock_info(self, name):
        return SimpleNamespace(resolution=.015625 if name == "monotonic" else 1e-7)


def runtime_stub(trigger=0, active=False):
    cancelled, emitted, evaluated = [], [], []
    rt = SimpleNamespace(c=MakerConfig(), wake_at_ns=trigger, reaction_window=ReactionWindow())
    rt.engine = SimpleNamespace(cancel=lambda *a: cancelled.append(a), active=lambda: active)
    rt.store = SimpleNamespace(emit=lambda *a: emitted.append(a))
    rt.react = lambda *a: evaluated.append(a)
    return rt, cancelled, emitted, evaluated


def test_subtick_duration_uses_perf_and_preserves_lifecycle_origin():
    rt, cancelled, emitted, evaluated = runtime_stub()
    run_reaction(rt, 1_800_000_000, 65536, clock=Clock([250_000, 500_000, 510_000]))
    assert evaluated == [(1_800_000_000, 65536)] and not cancelled and not emitted
    report = merge_reactions([rt.reaction_window.take()])
    assert report["event"]["max_ms"] == pytest.approx(.51)
    assert report["event"]["p50_upper_ms"] == .6
    assert report["watchdog"]["samples"] == 0 and rt.wake_at_ns is None


@pytest.mark.parametrize("trigger", [None, 0])
def test_watchdog_and_zero_signal_origin_are_distinct(trigger):
    rt, _, _, _ = runtime_stub(trigger)
    run_reaction(rt, 1000, 65536, clock=Clock([0, 100_000, 100_000]))
    r = merge_reactions([rt.reaction_window.take()])
    group = "watchdog" if trigger is None else "event"
    assert r[group]["samples"] == 1 and r[group]["max_ms"] == .1


@pytest.mark.parametrize("lag_ns,blocked", [(99_999_999, False), (100_000_000, False), (100_000_001, True)])
def test_precise_existing_budget_boundary(lag_ns, blocked):
    rt, cancelled, emitted, evaluated = runtime_stub()
    run_reaction(rt, 1000, 65536, clock=Clock([lag_ns] * 3))
    assert bool(cancelled) is blocked and bool(evaluated) is not blocked
    assert merge_reactions([rt.reaction_window.take()])["event"]["over_100ms"] == int(blocked)
    if blocked:
        assert cancelled == [("reaction_budget_missed", 1000, 65536)]
        assert emitted[0][1]["over_budget"]


@pytest.mark.parametrize("trigger,active", [(None, False), (0, False), (0, True)])
def test_compute_overrun_cancels_with_monotonic_not_perf_origin(trigger, active):
    rt, cancelled, _, _ = runtime_stub(trigger, active)
    run_reaction(rt, 1000, 65536, clock=Clock([0, 100_000_001, 100_000_002]))
    assert cancelled == [("compute_budget_missed", 1_800_000_000, 65536)]
    report = merge_reactions([rt.reaction_window.take()])
    assert report["watchdog" if trigger is None else "event"]["over_100ms"] == 1


def test_active_order_legacy_reaction_event_uses_new_measurement():
    rt, _, emitted, _ = runtime_stub(active=True)
    run_reaction(rt, 1000, 65536, clock=Clock([10_000, 40_000, 50_000]))
    assert emitted == [("reaction", {"elapsed_ms": .04, "over_budget": False})]


def test_backward_counter_fails_closed_not_negative_or_zero_latency():
    rt, cancelled, _, evaluated = runtime_stub(trigger=200)
    with pytest.raises(RuntimeError, match="measurement_clock_invalid"):
        run_reaction(rt, 1000, 65536, clock=Clock([199]))
    assert cancelled == [("measurement_clock_invalid", 1000, 65536)]
    assert not evaluated and rt.reaction_window.take()["groups"] == {}


def test_receipt_separates_resolution_histogram_and_lifecycle():
    r = clock_receipt(Clock([]))
    assert r["resolution_seconds"] == 1e-7 and r["lifecycle_resolution_seconds"] == .015625
    assert r["clock"] == "perf_counter_ns" and r["histogram_quantum_ms"] == .1
    assert r["revision"] == MEASUREMENT_REVISION


def test_summary_rewhitelists_no_text_and_counts_missing_old_causes():
    tally = TransportFailures()
    tally.add({"stage": "metadata"})
    tally.add({"stage": "orderbook", "transport_detail": {"classes": ["SECRET"]}})
    tally.add({"stage": "server_clock", "transport_detail": {
        "classes": ["ConnectError", "gaierror", "SECRET", {}],
        "errno_codes": [11001, True, "SECRET", 2**40], "winerror_codes": [10054],
        "message": "SECRET", "proxy": "SECRET"}})
    r = tally.report()
    assert r["events"] == 2 and r["without_recorded_detail"] == 1
    assert r["groups"][0]["errno_codes"] == [11001] and "SECRET" not in json.dumps(r)
    assert r["groups"][0]["classes"] == ["ConnectError", "gaierror", "other_exception", "other_exception"]


def test_summary_group_bound_and_known_group_after_limit():
    tally = TransportFailures()
    for n in range(20):
        tally.add({"stage": "metadata", "transport_detail": {"classes": ["OSError"], "errno_codes": [n]}})
    tally.add({"stage": "metadata", "transport_detail": {"classes": ["OSError"], "errno_codes": [0]}})
    r = tally.report()
    assert len(r["groups"]) == 16 and r["omitted_group_events"] == 4
    assert r["events"] == 21 and r["groups"][0]["events"] == 2


def test_summary_truncated_chain_is_explicit():
    tally = TransportFailures()
    tally.add({"stage": "metadata", "transport_detail": {"classes": ["ConnectError"] * 20}})
    row = tally.report()["groups"][0]
    assert len(row["classes"]) == 12 and row["chain_truncated"]


@pytest.mark.parametrize("measurement", [None, clock_receipt(Clock([]))])
def test_diagnose_latest_run_preserves_clock_provenance_and_readonly(tmp_path, measurement):
    path = tmp_path / "diag.db"
    with sqlite3.connect(path) as db:
        db.executescript("""CREATE TABLE maker_meta(id INTEGER PRIMARY KEY, data TEXT);
            CREATE TABLE maker_orders(id TEXT PRIMARY KEY, data TEXT);
            CREATE TABLE maker_events(id INTEGER PRIMARY KEY, ts REAL, kind TEXT, data TEXT);""")
        db.execute("INSERT INTO maker_meta VALUES (1,'{}')")
        runtime = {} if measurement is None else {"measurement": measurement}
        rows = [("runtime", {}), ("source_error", {"stage": "metadata"}),
                ("runtime", runtime), ("source_error", {"stage": "server_clock", "transport_detail": {
                    "classes": ["ConnectError", "gaierror"], "errno_codes": [11001]}})]
        for n, (kind, data) in enumerate(rows, 1):
            db.execute("INSERT INTO maker_events VALUES (?,?,?,?)", (n, n, kind, json.dumps(data)))
    before = path.read_bytes()
    out = tmp_path / "diag.json.gz"
    r = diagnostic_report(path, out)["summary"]
    assert path.read_bytes() == before
    assert r["http_failures_latest_run"]["events"] == 1
    assert r["http_failures_latest_run"]["without_recorded_detail"] == 0
    assert r["measurement_latest_run"] == (measurement or {"clock": "legacy_or_unrecorded", "resolution_seconds": None})
    assert r["local_reaction_all_evaluations"]["event"]["p50_upper_ms"] is None
    with pytest.raises(FileExistsError):
        diagnostic_report(path, out)


def test_runtime_signal_preserves_first_perf_timestamp(monkeypatch):
    from jevymarket import maker
    store = SimpleNamespace(emit=lambda *a: None, load_orders=lambda: [])
    rt = maker.MakerRuntime(MakerConfig(), store)
    clock = Clock([0, 900_000])
    monkeypatch.setattr(maker, "time", clock)  # module binding, NOT stdlib global mutation
    rt.signal()
    rt.signal()
    assert rt.wake_at_ns == 0 and rt.wake.is_set()
    rt.wake_at_ns = None
    rt.signal()
    assert rt.wake_at_ns == 900_000


def test_runtime_errors_keep_safe_causes_in_main_journal(capsys):
    from jevymarket import maker
    emitted = []
    store = SimpleNamespace(emit=lambda *a: emitted.append(a), load_orders=lambda: [])
    rt = maker.MakerRuntime(MakerConfig(), store)
    exc = httpx.ConnectError("SECRET_URL_BODY")
    exc.__cause__ = OSError(11001, "SECRET_OS_MESSAGE")
    rt.error("metadata", exc)
    assert emitted[0][0] == "source_error"
    data = emitted[0][1]
    assert data["session_id"] == rt.session_id
    assert data["transport_detail"]["classes"] == ["ConnectError", "OSError"]
    assert data["transport_detail"]["errno_codes"] == [11001]
    assert "SECRET" not in json.dumps(emitted) + capsys.readouterr().out
    rt.error("orderbook", ValueError("SECRET"))
    assert "transport_detail" not in emitted[-1][1] and rt.engine.orders == []


@pytest.mark.asyncio
async def test_runtime_clock_rtt_uses_perf_without_changing_clock_guard(monkeypatch):
    from jevymarket import maker
    emitted = []
    rt = maker.MakerRuntime(MakerConfig(), SimpleNamespace(emit=lambda *a: emitted.append(a), load_orders=lambda: []))
    clock = Clock([0, 400_000, 500_000])
    monkeypatch.setattr(maker, "time", clock)

    async def public(*args):
        return 1_800_000_000

    monkeypatch.setattr(maker, "public_json", public)
    assert await rt.clock_once(None)
    sample = next(d for kind, d in emitted if kind == "clock")
    assert sample["rtt_seconds"] == .0004 and sample["reason"] == "ok"


@pytest.mark.asyncio
async def test_runtime_offline_run_includes_precision_and_zero_order_diagnostics(tmp_path, monkeypatch):
    from jevymarket import maker
    from jevymarket.maker_store import MakerStore
    config = MakerConfig()
    path = tmp_path / "run.db"
    rt = maker.MakerRuntime(config, MakerStore(path, config))

    async def idle(*args):
        await asyncio.Event().wait()

    rt.references = rt.discover = rt.clock = rt.settlements = idle
    original = maker.run_reaction

    def one_iteration(runtime, *args, **kwargs):
        original(runtime, *args, **kwargs)
        runtime.stopped = True

    monkeypatch.setattr(maker, "run_reaction", one_iteration)
    await asyncio.wait_for(rt.run(observe_only=True), 5)
    s = diagnostic_report(path)["summary"]
    assert s["runtime"]["observe_only"]
    assert s["measurement_latest_run"]["revision"] == MEASUREMENT_REVISION
    assert s["measurement_latest_run"]["clock"] == "perf_counter_ns"
    assert s["local_reaction_all_evaluations"]["watchdog"]["samples"] == 1
    assert s["ledger_orders_all_runs"] == 0 and s["http_failures_latest_run"]["events"] == 0
