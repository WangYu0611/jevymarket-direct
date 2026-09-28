from __future__ import annotations

import ast
from importlib.metadata import version
from pathlib import Path

from jevymarket.polymarket_contract import (
    AUDITED_POLYMARKET_CLIENT_VERSION,
    live_sdk_contract,
)


def _calls_named(tree: ast.AST, attr: str):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == attr:
            yield node


def test_exact_audited_polymarket_sdk_is_installed():
    assert version("polymarket-client") == AUDITED_POLYMARKET_CLIENT_VERSION
    report = live_sdk_contract()
    assert report["ok"], report["failures"]


def test_no_list_positions_call_uses_removed_market_keyword():
    offenders = []
    for path in Path("src/jevymarket").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for call in _calls_named(tree, "list_positions"):
            if any(keyword.arg == "market" for keyword in call.keywords):
                offenders.append(str(path))
    assert offenders == []


def test_order_book_reads_use_current_asset_ids_keyword():
    path = Path("src/jevymarket/markets.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    calls = list(_calls_named(tree, "get_order_books"))
    assert calls
    for call in calls:
        names = {keyword.arg for keyword in call.keywords}
        assert "asset_ids" in names
        assert "token_ids" not in names


def test_sign_probe_cannot_post_or_place_orders():
    path = Path("src/jevymarket/polymarket_probe.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden = {"place_market_order", "place_limit_order", "post_order", "post_orders"}
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in forbidden:
                calls.append(node.func.attr)
    assert calls == []
