import gzip
import json
import sqlite3
from dataclasses import asdict

from jevymarket.maker_config import MakerConfig
from jevymarket.maker_paper_45to30 import (
    CANCEL_SECONDS,
    ENTRY_SECONDS,
    REVISION,
    compact_report,
    trial_config,
)
from jevymarket.maker_store import MakerStore


def test_trial_changes_timing_only():
    base = MakerConfig()
    trial = trial_config()
    assert trial.entry_seconds == ENTRY_SECONDS == 45
    assert trial.cancel_before_end_seconds == CANCEL_SECONDS == 30
    unchanged = asdict(base)
    for key in ("entry_seconds", "cancel_before_end_seconds"):
        unchanged.pop(key)
    observed = asdict(trial)
    for key in ("entry_seconds", "cancel_before_end_seconds"):
        observed.pop(key)
    assert observed == unchanged


def test_trial_window_has_large_cancel_margin():
    c = trial_config()
    assert c.cancel_before_end_seconds < c.entry_seconds
    assert c.paper_cancel_latency_seconds < c.cancel_before_end_seconds
    assert c.max_book_age_seconds == 1
    assert c.min_confidence == .92


def test_compact_report_is_readonly_and_contains_no_live_claim(tmp_path):
    db = tmp_path / "paper.db"
    store = MakerStore(db, trial_config())
    store.stopping = True

    import asyncio
    asyncio.run(store.writer())

    before = db.read_bytes()
    out = tmp_path / "report.json.gz"
    report = compact_report(db, out)
    after = db.read_bytes()

    assert before == after
    assert report["format"] == REVISION
    assert report["paper_only"] is True
    assert report["live_trading_enabled"] is False
    assert report["orders"] == []
    with gzip.open(out, "rt", encoding="utf-8") as handle:
        disk = json.load(handle)
    assert disk["window"] == {"entry_seconds": 45.0, "cancel_before_end_seconds": 30.0}


def test_existing_output_refuses_overwrite(tmp_path):
    db = tmp_path / "paper.db"
    store = MakerStore(db, trial_config())
    store.stopping = True
    import asyncio
    asyncio.run(store.writer())
    out = tmp_path / "report.json.gz"
    out.write_bytes(b"keep")
    try:
        compact_report(db, out)
    except FileExistsError:
        pass
    else:
        raise AssertionError("existing output must not be overwritten")
    assert out.read_bytes() == b"keep"
