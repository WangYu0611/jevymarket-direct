import asyncio
import sqlite3
import ssl
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
import typer
from polymarket.errors import RateLimitError, RequestRejectedError
from polymarket.errors import TransportError as SDKTransportError

from jevymarket import cli, executor, network
from jevymarket.jev import JevError
from jevymarket.network import ReadUnavailable, read_with_retry


@pytest.fixture(autouse=True)
def fast_retries(monkeypatch):
    real_sleep = asyncio.sleep
    network._cooldowns.clear()

    async def sleep(_seconds):
        await real_sleep(0)

    monkeypatch.setattr(network.asyncio, "sleep", sleep)
    yield
    network._cooldowns.clear()


async def test_ssl_read_retries_then_returns_new_result():
    operation = AsyncMock(side_effect=[ssl.SSLError("record layer failure"), "fresh"])
    assert await read_with_retry(operation, label="book") == "fresh"
    assert operation.await_count == 2


@pytest.mark.parametrize("error", [
    ssl.SSLError("record layer failure"),
    httpx.ReadTimeout("timeout"),
    httpx.ConnectError("connection reset"),
    httpx.RemoteProtocolError("Server disconnected"),
    SDKTransportError("SDK transport failure"),
    RequestRejectedError("upstream unavailable", status=503),
])
async def test_read_exhaustion_is_bounded(error):
    operation = AsyncMock(side_effect=error)
    with pytest.raises(ReadUnavailable) as caught:
        await read_with_retry(operation, label="book")
    assert operation.await_count == 3
    assert caught.value.__cause__ is error


async def test_read_deadline_cancels_stalled_operation():
    cancelled = asyncio.Event()

    async def stalled():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    with pytest.raises(ReadUnavailable):
        await read_with_retry(stalled, label="book", timeout_seconds=0.01)
    assert cancelled.is_set()


@pytest.mark.parametrize("error", [
    asyncio.CancelledError(),
    ValueError("invalid config"),
    RuntimeError("programming error"),
    sqlite3.OperationalError("database is locked"),
    ssl.SSLCertVerificationError("certificate verify failed"),
    httpx.LocalProtocolError("invalid request"),
    RequestRejectedError("unauthorized", status=401),
    RequestRejectedError("forbidden", status=403),
    RequestRejectedError("bad request", status=400),
])
async def test_fatal_errors_and_cancellation_are_not_retried(error):
    operation = AsyncMock(side_effect=error)
    with pytest.raises(type(error)):
        await read_with_retry(operation, label="book")
    assert operation.await_count == 1


async def test_wrapped_certificate_failure_is_not_retried():
    error = SDKTransportError("wrapped")
    error.__cause__ = ssl.SSLCertVerificationError("certificate verify failed")
    operation = AsyncMock(side_effect=error)
    with pytest.raises(SDKTransportError):
        await read_with_retry(operation, label="book")
    assert operation.await_count == 1


async def test_rate_limit_cooldown_survives_next_call():
    operation = AsyncMock(side_effect=RateLimitError("rate limited", retry_after=60))
    for _ in range(2):
        with pytest.raises(ReadUnavailable):
            await read_with_retry(operation, label="book")
    assert operation.await_count == 1


@pytest.fixture
def bot(monkeypatch):
    settings = SimpleNamespace(dry_run=True, db_path="unused.db", max_trades_per_run=10)
    store = Mock()
    store.has_order_for.return_value = False
    ex = Mock()
    ex.exposure = AsyncMock(return_value=SimpleNamespace(total=0.0, condition_ids=set()))
    ex.close = AsyncMock()
    ex.place = AsyncMock(return_value=SimpleNamespace(ok=True, status="dry_run", order_id=None))
    ex.trades_this_run = 0
    jev = SimpleNamespace(calls=0, total_input_tokens=0, total_output_tokens=0)
    pub_context = AsyncMock()
    jev_context = AsyncMock()
    jev_context.__aenter__.return_value = jev

    monkeypatch.setattr("polymarket.AsyncPublicClient", lambda: pub_context)
    monkeypatch.setattr(executor.Executor, "create", AsyncMock(return_value=ex))
    monkeypatch.setattr(cli, "_settings", lambda **kwargs: settings)
    monkeypatch.setattr(cli, "Store", lambda _path: store)
    monkeypatch.setattr(cli, "_jev", lambda _s: jev_context)
    monkeypatch.setattr(cli, "console", Mock())
    monkeypatch.setattr(cli, "settle_pending_markets", AsyncMock(return_value=0))
    monkeypatch.setattr(cli, "market_timeframe", lambda m, _s: m.timeframe)
    monkeypatch.setattr(cli, "_print_snapshot", Mock())
    monkeypatch.setattr(cli, "_print_decision", Mock())
    monkeypatch.setattr(cli, "_snapshot_not_ready_reason", lambda _s: "expired")
    monkeypatch.setattr(cli, "_record_checkpoint_sample", Mock())
    monkeypatch.setattr(cli, "_decision_row", lambda c, *a, **kw: {"slug": c.slug})
    monkeypatch.setattr(cli, "quantitative_up_probability", lambda _s: 0.6)
    monkeypatch.setattr(cli, "market_data_and_ask", AsyncMock(return_value=({}, object())))
    monkeypatch.setattr(cli, "evaluate", Mock(return_value=SimpleNamespace(reason="skip")))

    candidates = [
        cli.Candidate(
            market=SimpleNamespace(slug=tf, condition_id=tf, timeframe=tf, question=tf),
            book=object(),
        )
        for tf in ("5m", "15m")
    ]
    snapshot = SimpleNamespace(trade_ready=True, to_state=lambda: {"fresh": True})
    monkeypatch.setattr(cli, "scan", AsyncMock(return_value=candidates))
    monkeypatch.setattr(cli, "fetch_book", AsyncMock(return_value=object()))
    monkeypatch.setattr(cli, "fetch_short_term_snapshots", AsyncMock(
        return_value={c.slug: snapshot for c in candidates}
    ))

    watcher = SimpleNamespace(started=False, stopped=False)

    async def watch(*_args):
        watcher.started = True
        try:
            await asyncio.Event().wait()
        finally:
            watcher.stopped = True

    monkeypatch.setattr(cli, "watch_chainlink_anchors", watch)
    return SimpleNamespace(
        settings=settings, store=store, ex=ex, candidates=candidates,
        snapshot=snapshot, watcher=watcher,
    )


def run_bot(loop=None):
    cli.run(dry_run=True, max_trades=None, limit=20, loop=loop, no_research=False)


async def test_snapshot_retry_also_refetches_order_book(bot, monkeypatch):
    old_book, new_book = object(), object()
    cli.fetch_book.side_effect = [old_book, new_book]
    cli.fetch_short_term_snapshots.side_effect = [
        ssl.SSLError("snapshot failed"), {"5m": bot.snapshot},
    ]
    fresh, snapshot = await cli._refresh_candidate(None, bot.candidates[0], bot.settings, bot.store)
    assert fresh.book is new_book
    assert snapshot is bot.snapshot
    assert cli.fetch_book.await_count == 2


def test_failed_refresh_skips_market_without_stale_trade_or_checkpoint(bot):
    async def book(_pub, market):
        if market.slug == "5m":
            raise ssl.SSLError("record layer failure")
        return object()

    cli.fetch_book.side_effect = book
    run_bot()
    assert cli.fetch_book.await_count == 4  # 3 failed 5m reads, then healthy 15m
    assert cli.evaluate.call_count == 1
    assert cli._record_checkpoint_sample.call_count == 1
    assert cli._record_checkpoint_sample.call_args.args[1].slug == "15m"
    assert bot.store.log_decision.call_args.kwargs == {"slug": "15m"}
    bot.ex.place.assert_not_awaited()
    bot.ex.close.assert_awaited_once()
    bot.store.close.assert_called_once()


def test_loop_continues_after_failed_read_stage_and_keeps_watcher(bot, monkeypatch):
    cli.scan.side_effect = [ssl.SSLError("bad") for _ in range(3)] + [bot.candidates]
    sleep_impl = asyncio.sleep
    rounds = 0

    async def sleep(seconds):
        nonlocal rounds
        if seconds == 30:
            assert bot.watcher.started and not bot.watcher.stopped
            rounds += 1
            if rounds == 2:
                raise asyncio.CancelledError()
        await sleep_impl(0)

    monkeypatch.setattr(cli.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        run_bot(loop=30)
    assert cli.scan.await_count == 4
    assert cli.evaluate.call_count == 2
    assert bot.watcher.stopped
    bot.ex.close.assert_awaited_once()
    bot.store.close.assert_called_once()


def test_single_pass_read_failure_exits_nonzero(bot):
    cli.scan.side_effect = ssl.SSLError("bad")
    with pytest.raises(typer.Exit) as caught:
        run_bot()
    assert caught.value.exit_code == 1
    bot.ex.place.assert_not_awaited()


def test_uncertain_live_order_is_never_retried(bot, monkeypatch):
    class TestTrade:
        pass

    bot.settings.dry_run = False
    monkeypatch.setattr(cli, "Trade", TestTrade)
    cli.evaluate.return_value = TestTrade()
    bot.ex.place.side_effect = ssl.SSLError("response lost after submission")
    with pytest.raises(ssl.SSLError):
        run_bot(loop=30)
    bot.ex.place.assert_awaited_once()
    cli.scan.assert_awaited_once()
    bot.store.log_decision.assert_not_called()


@pytest.mark.parametrize("status", [401, 402])
def test_jev_auth_or_credit_error_remains_fatal(bot, status):
    cli.market_data_and_ask.side_effect = JevError(status, "fatal")
    with pytest.raises(typer.Exit):
        run_bot(loop=30)
    cli.market_data_and_ask.assert_awaited_once()
    cli.fetch_book.assert_not_awaited()


def test_expired_after_refresh_is_not_used_for_trading(bot):
    cli.fetch_short_term_snapshots.side_effect = [
        {c.slug: bot.snapshot for c in bot.candidates},
        {"5m": SimpleNamespace(trade_ready=False)},
        {"15m": SimpleNamespace(trade_ready=False)},
    ]
    run_bot()
    cli.evaluate.assert_not_called()
    cli._record_checkpoint_sample.assert_not_called()
    bot.ex.place.assert_not_awaited()


def test_database_failure_is_not_hidden_by_network_guards(bot):
    bot.store.log_decision.side_effect = sqlite3.OperationalError("disk full")
    with pytest.raises(sqlite3.OperationalError):
        run_bot(loop=30)
    cli.scan.assert_awaited_once()
