"""jevymarket command-line interface."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import Settings, load_settings
from .evaluation import checkpoint_for
from .jev import JevClient, JevError, choice, noul, score
from .market_data import ShortTermSnapshot, fetch_short_term_snapshots, watch_chainlink_anchors
from .markets import TIMEFRAME_LABELS, Candidate, fetch_book, load_candidate, market_timeframe, scan
from .research import Brief, Researcher, ResearchError
from .settlement import settle_pending_markets
from .signal import Trade, evaluate, get_brief, market_data_and_ask, quantitative_up_probability
from .store import Store

app = typer.Typer(help="Polymarket BTC 短周期交易机器人：Chainlink/Binance 实时数据 + TypeSafe Jev。", no_args_is_help=True)
console = Console()
log = logging.getLogger("jevymarket")


def _settings(dry_run: bool | None = None) -> Settings:
    s = load_settings()
    if dry_run is not None:
        s.dry_run = dry_run
    logging.basicConfig(level=getattr(logging, s.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return s


def _run(coro):
    from polymarket import PolymarketError

    try:
        return asyncio.run(coro)
    except (JevError, ResearchError, PolymarketError, ValueError) as e:
        console.print(f"[red]{type(e).__name__}: {e}[/]")
        raise typer.Exit(1) from None


def _researcher(s: Settings, enabled: bool) -> Researcher | None:
    if not (enabled and s.research_enabled):
        return None
    return Researcher(s.deepseek_api_key, model=s.research_model, base_url=s.deepseek_base_url,
                      json_base_url=s.deepseek_json_base_url, max_searches=s.research_max_searches,
                      max_calls=s.max_research_per_run, exclude_domains=s.research_exclude_domains)


def _jev(s: Settings) -> JevClient:
    return JevClient(
        s.typesafe_api_key,
        model=s.jev_model,
        base_url=s.typesafe_base_url,
        timeout=s.jev_timeout_seconds,
        max_retries=s.jev_max_retries,
    )


# ---------------------------------------------------------------------------


@app.callback()
def _root(version: bool = typer.Option(False, "--version", is_eager=True)):
    if version:
        console.print(f"jevymarket {__version__}")
        raise typer.Exit()


@app.command("jev-test")
def jev_test(raw: bool = typer.Option(False, help="Print the raw JSON response only.")):
    """One hard-coded Jev call through TypeSafe to verify the key and response schema."""
    s = _settings()
    state = {
        "question": "Will the sun rise in the east tomorrow?",
        "description": "Resolves YES if the sun rises in the east on the day after 'today'.",
        "today": "2026-09-20",
        "days_until_resolution": 1,
    }
    questions = {
        "resolves_yes": noul("This market will resolve YES."),
        "domain": choice("What domain is this question about?", {
            "astronomy": "Celestial mechanics, planets, stars",
            "politics": "Elections, governments, policy",
            "sports": "Games, matches, athletes",
        }),
        "clarity": score("How clear are the resolution criteria?", [
            "Completely ambiguous", "Somewhat ambiguous", "Mostly clear", "Precise and objective",
        ]),
    }

    async def go():
        async with JevClient(s.typesafe_api_key, model=s.jev_model, base_url=s.typesafe_base_url) as jev:
            data = await jev.decide_raw(state, questions)
            if raw:
                print(json.dumps(data, indent=2))
                return
            console.print_json(json.dumps(data))
            d = await jev.decide(state, questions)
            console.print(f"\n[bold]parsed:[/] p_yes={d.answers['resolves_yes'].noul} "
                          f"domain={d.answers['domain'].choice} (conf {d.answers['domain'].confidence}) "
                          f"clarity={d.answers['clarity'].score_mean} | model={d.model} cost=${d.usage.cost:.6f}")

    _run(go())


@app.command("scan")
def scan_cmd(limit: int = typer.Option(15, "--limit", "-n"), pages: int = typer.Option(5)):
    """扫描符合条件的 BTC 5分钟 / 15分钟 / 1小时涨跌市场。"""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        store = Store(s.db_path)
        try:
            async with AsyncPublicClient() as c:
                candidates = await scan(c, s, limit=limit, pages=pages)
                snapshots = await fetch_short_term_snapshots(c, candidates, s, store=store)
                return candidates, snapshots
        finally:
            store.close()

    cands, snapshots = _run(go())
    t = Table(title=f"候选市场：{len(cands)} 个（仅 BTC 5分钟 / 15分钟 / 1小时）")
    columns = (
        "市场 slug", "周期", "目标价", "当前价", "差值", "剩余",
        "买入上涨", "卖出上涨", "买入下跌", "卖出下跌",
    )
    for col in columns:
        t.add_column(col, justify="left" if col in ("市场 slug", "周期") else "right")
    for cand in cands:
        tf = market_timeframe(cand.market, s) or "?"
        snap = snapshots.get(cand.slug)
        t.add_row(
            cand.slug[:72],
            TIMEFRAME_LABELS.get(tf, tf),
            _fmt_usd(snap.target_price if snap else None),
            _fmt_usd(snap.current_price if snap else None),
            _fmt_delta(snap.delta_usd if snap else None),
            _fmt_seconds(snap.seconds_left if snap else None),
            _fmt_cents(cand.book.yes_ask),
            _fmt_cents(cand.book.yes_bid),
            _fmt_cents(cand.book.no_ask),
            _fmt_cents(cand.book.no_bid),
        )
    console.print(t)
    for cand in cands:
        snap = snapshots.get(cand.slug)
        if snap and (snap.target_source or snap.current_source):
            pf = snap.path_features
            path_text = "无"
            if pf is not None:
                path_text = (
                    f"{'完整' if pf.feature_ready else '预热'} "
                    f"{pf.history_span_seconds}s；1m={_fmt_pct(pf.return_60s_pct)}；"
                    f"3m={_fmt_pct(pf.return_180s_pct)}；Z={_fmt_num(pf.distance_z)}"
                )
            console.print(
                f"[dim]{cand.slug}：目标价源={snap.target_source or '未知'}；"
                f"当前价源={snap.current_source or '未知'}；路径={path_text}[/]"
            )



@app.command()
def research(ref: str = typer.Argument(..., help="Market slug or polymarket.com URL"),
             fresh: bool = typer.Option(False, help="忽略缓存并重新研究。")):
    """只运行研究步骤并打印证据摘要。"""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        store = Store(s.db_path)
        r = Researcher(s.deepseek_api_key, model=s.research_model, base_url=s.deepseek_base_url,
                       json_base_url=s.deepseek_json_base_url, max_searches=s.research_max_searches,
                       exclude_domains=s.research_exclude_domains)
        async with AsyncPublicClient() as c, r:
            cand = await load_candidate(c, s, ref)
            console.print(f"[cyan]正在研究：{cand.slug}[/]")
            brief, cached = await get_brief(cand, s, store, r, fresh=fresh)
            if brief is None:
                console.print("[red]没有生成有效研究摘要[/]")
                raise typer.Exit(1) from None
            console.print(f"[bold]{cand.question}[/]")
            _print_brief(brief, cached, full=True)

    _run(go())


@app.command()
def decide(ref: str = typer.Argument(..., help="市场 slug 或 polymarket.com URL"),
           show_state: bool = typer.Option(False, help="打印发送给 Jev 的状态。"),
           no_research: bool = typer.Option(False, "--no-research", help="兼容参数；短周期模式默认不使用 DeepSeek。"),
           fresh: bool = typer.Option(False, help="兼容参数；短周期模式默认不使用 DeepSeek。")):
    """使用实时参考价格 + Jev 判断单个短周期市场；不会真实下单。"""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        _ = (no_research, fresh)
        store = Store(s.db_path)
        try:
            async with AsyncPublicClient() as c, _jev(s) as jev:
                cand = await load_candidate(c, s, ref)
                snapshots = await fetch_short_term_snapshots(c, [cand], s, store=store)
                snapshot = snapshots.get(cand.slug)
                _print_snapshot(snapshot)
                if snapshot is None or not snapshot.trade_ready:
                    console.print(f"[yellow]跳过：{_snapshot_not_ready_reason(snapshot)}[/]")
                    return
                state, view = await market_data_and_ask(cand, s, jev, snapshot)
                if show_state:
                    console.print_json(json.dumps(state, ensure_ascii=False, default=str))
                fresh_book = await fetch_book(c, cand.market)
                fresh_cand = Candidate(market=cand.market, book=fresh_book)
                fresh_snapshots = await fetch_short_term_snapshots(
                    c, [fresh_cand], s, store=store
                )
                fresh_snapshot = fresh_snapshots.get(fresh_cand.slug)
                if fresh_snapshot is None or not fresh_snapshot.trade_ready:
                    console.print(
                        f"[yellow]Jev 返回后行情已变化，跳过："
                        f"{_snapshot_not_ready_reason(fresh_snapshot)}[/]"
                    )
                    return

                quant_p = quantitative_up_probability(fresh_snapshot)
                if quant_p is None:
                    console.print("[yellow]跳过：无法从最新价格路径计算量化上涨概率。[/]")
                    return
                state["quantitative_signal"] = {
                    "p_up": quant_p,
                    "method": "normal_cdf(distance_z)",
                    "execution_snapshot": fresh_snapshot.to_state(),
                }
                _record_checkpoint_sample(
                    store, fresh_cand, s, fresh_snapshot, state, view, quant_p
                )
                result = evaluate(
                    view,
                    fresh_cand.book,
                    s,
                    probability_yes=quant_p,
                    probability_source="量化Φ(Z)",
                )
                _print_decision(
                    fresh_cand,
                    view,
                    result,
                    snapshot=fresh_snapshot,
                    quant_p_yes=quant_p,
                )
                store.log_decision(
                    **_decision_row(
                        fresh_cand, state, view, result, brief=None, executed=False,
                        signal_p_yes=quant_p,
                    )
                )
        finally:
            store.close()

    _run(go())


@app.command()
def run(
    dry_run: bool = typer.Option(False, "--dry-run", help="只评估和记录，不真实下单。"),
    max_trades: int | None = typer.Option(None, help="覆盖每轮最大交易数。"),
    limit: int = typer.Option(20, "--limit", "-n", help="本轮最多分析的候选市场数量。"),
    loop: int | None = typer.Option(None, help="每 N 秒重复一轮。"),
    no_research: bool = typer.Option(False, "--no-research", help="兼容参数；短周期自动交易默认不使用 DeepSeek。"),
):
    """扫描 → 实时参考价格 → Jev 判断 → 风控 → 模拟/真实下单。"""
    s = _settings(dry_run=dry_run or None)
    if max_trades is not None:
        s.max_trades_per_run = max_trades
    from polymarket import AsyncPublicClient

    from .executor import Executor

    async def one_pass(store: Store, ex: Executor, pub: AsyncPublicClient, jev: JevClient):
        settled = await settle_pending_markets(pub, s, store, limit=40)
        if settled:
            console.print(f"[dim]已自动回填 {settled} 个已结算短周期市场[/]")
        cands = await scan(pub, s, limit=limit)
        snapshots = await fetch_short_term_snapshots(pub, cands, s, store=store)
        console.rule(
            f"候选 {len(cands)} 个 | 当前敞口 "
            f"${(await ex.exposure(refresh=True)).total:.2f} | 模拟模式={s.dry_run}"
        )
        for idx, cand in enumerate(cands, start=1):
            tf = market_timeframe(cand.market, s) or "?"
            console.print(
                f"\n[cyan]正在分析 {idx}/{len(cands)}：BTC "
                f"{TIMEFRAME_LABELS.get(tf, tf)}[/]  [dim]{cand.slug}[/]"
            )
            if (
                cand.condition_id in (await ex.exposure()).condition_ids
                or store.has_order_for(
                    cand.condition_id,
                    include_dry_run=s.dry_run,
                )
            ):
                console.print(f"[dim]{cand.slug}：已有持仓或挂单，跳过[/]")
                continue

            snapshot = snapshots.get(cand.slug)
            _print_snapshot(snapshot)
            if snapshot is None or not snapshot.trade_ready:
                console.print(
                    f"  [yellow]跳过：{_snapshot_not_ready_reason(snapshot)}[/]"
                )
                continue

            try:
                state, view = await market_data_and_ask(cand, s, jev, snapshot)
            except JevError as e:
                console.print(f"[red]{cand.slug}: {e}[/]")
                if e.status in (401, 402):
                    raise
                continue

            fresh_book = await fetch_book(pub, cand.market)
            fresh_cand = Candidate(market=cand.market, book=fresh_book)
            fresh_snapshots = await fetch_short_term_snapshots(
                pub, [fresh_cand], s, store=store
            )
            fresh_snapshot = fresh_snapshots.get(fresh_cand.slug)
            if fresh_snapshot is None or not fresh_snapshot.trade_ready:
                console.print(
                    f"  [yellow]Jev 返回后行情已变化，跳过："
                    f"{_snapshot_not_ready_reason(fresh_snapshot)}[/]"
                )
                continue

            quant_p = quantitative_up_probability(fresh_snapshot)
            if quant_p is None:
                console.print("  [yellow]跳过：无法从最新价格路径计算量化上涨概率。[/]")
                continue

            state["quantitative_signal"] = {
                "p_up": quant_p,
                "method": "normal_cdf(distance_z)",
                "execution_snapshot": fresh_snapshot.to_state(),
            }
            _record_checkpoint_sample(
                store, fresh_cand, s, fresh_snapshot, state, view, quant_p
            )
            result = evaluate(
                view,
                fresh_cand.book,
                s,
                probability_yes=quant_p,
                probability_source="量化Φ(Z)",
            )
            _print_decision(
                fresh_cand,
                view,
                result,
                snapshot=fresh_snapshot,
                quant_p_yes=quant_p,
            )
            executed = False
            if isinstance(result, Trade):
                placed = await ex.place(fresh_cand, result)
                executed = placed.ok
                if not placed.ok:
                    console.print(f"  [yellow]{_status_cn(placed.status)}：{placed.message}[/]")
                else:
                    console.print(
                        f"  [green]{_status_cn(placed.status)}[/] "
                        f"订单ID={placed.order_id}"
                    )

            store.log_decision(
                **_decision_row(
                    fresh_cand, state, view, result, brief=None, executed=executed,
                    signal_p_yes=quant_p,
                )
            )
            if ex.trades_this_run >= s.max_trades_per_run:
                console.print("[bold]本轮交易数量已达到上限[/]")
                break

        console.print(
            f"[dim]Jev：{jev.calls} 次调用，输入 {jev.total_input_tokens} / "
            f"输出 {jev.total_output_tokens} tokens | DeepSeek：短周期模式未调用[/]"
        )

    async def go():
        _ = no_research
        store = Store(s.db_path)
        ex = await Executor.create(s, store, dry_run=s.dry_run)
        watcher = asyncio.create_task(watch_chainlink_anchors(s, store))
        try:
            async with AsyncPublicClient() as pub, _jev(s) as jev:
                while True:
                    ex.trades_this_run = 0
                    await one_pass(store, ex, pub, jev)
                    if loop is None:
                        break
                    await asyncio.sleep(loop)
        finally:
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher
            await ex.close()
            store.close()

    _run(go())


@app.command("watch-prices")
def watch_prices():
    """持续记录 Chainlink 原始价格路径，并捕获 5分钟/15分钟窗口起始 TWAP。"""
    s = _settings()

    async def go():
        store = Store(s.db_path)
        console.print(
            "[cyan]开始监听 Chainlink BTC/USD 原始价 + TWAP 30s/60s。"
            "程序会持续保存短期价格路径，并在新窗口起点写入权威目标价。[/]"
        )
        try:
            await watch_chainlink_anchors(s, store)
        finally:
            store.close()

    try:
        _run(go())
    except KeyboardInterrupt:
        console.print("\n[dim]已停止价格监听。[/]")


@app.command()
def setup():
    """One-time on-chain approvals so the exchange can move your USDC / conditional tokens."""
    s = _settings()
    from .executor import Executor

    async def go():
        ex = await Executor.create(s, Store(s.db_path), dry_run=False)
        try:
            console.print(f"钱包 {ex.wallet}（{ex.wallet_type}）")
            console.print(await ex.setup_approvals())
        finally:
            await ex.close()

    _run(go())


@app.command()
def positions():
    """Show wallet balance, open positions and open orders."""
    s = _settings()
    from .executor import Executor

    async def go():
        ex = await Executor.create(s, Store(s.db_path), dry_run=True)
        try:
            bal = await ex.collateral_balance_usd()
            console.print(f"钱包 {ex.wallet}（{ex.wallet_type}） pUSD：{'无私钥，无法查询' if bal is None else f'${bal:,.2f}'}")
            t = Table(title="当前持仓")
            for col in ("市场", "方向", "数量", "均价", "现价", "价值 $", "盈亏 $"):
                t.add_column(col)
            async for p in ex.client.list_positions(user=ex.wallet, status="OPEN").iter_items():
                t.add_row(str(p.slug)[:60], str(p.outcome), f"{float(p.current_size or 0):.2f}", f"{float(p.avg_price or 0):.3f}",
                          f"{float(p.current_price or 0):.3f}", f"{float(p.current_value or 0):.2f}", f"{float(p.total_pnl or 0):+.2f}")
            console.print(t)
            t2 = Table(title="当前挂单")
            for col in ("订单ID", "买卖", "方向", "价格", "数量", "已成交", "状态"):
                t2.add_column(col)
            async for o in (ex.client.list_open_orders().iter_items() if ex.authenticated else _empty()):
                t2.add_row(str(o.id)[:12], str(o.side), str(o.outcome), str(o.price), str(o.original_size), str(o.size_matched), str(o.status))
            console.print(t2)
            exx = await ex.exposure(refresh=True)
            console.print(f"敞口：持仓 ${exx.positions_usd:.2f} + 挂单 ${exx.open_orders_usd:.2f} = ${exx.total:.2f}（上限 ${s.max_open_exposure_usd:.2f}）")
        finally:
            await ex.close()

    _run(go())


@app.command()
def stats():
    """自动回填结算结果，并显示概率质量与模拟交易表现。"""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        store = Store(s.db_path)
        try:
            async with AsyncPublicClient() as client:
                settled = await settle_pending_markets(
                    client, s, store, limit=500, grace_seconds=15.0
                )
            return settled, store.stats(
                min_edge=s.min_edge,
                min_answerable=s.min_answerable,
                min_clarity=s.min_clarity,
                min_trade_price=s.min_trade_price,
                max_trade_price=s.max_trade_price,
            )
        finally:
            store.close()

    settled, st = _run(go())
    if settled:
        console.print(f"[green]本次新增回填 {settled} 个已结算市场[/]")

    console.print(
        f"决策数={st['decisions']} 交易信号={st['trade_signals']} "
        f"已结算市场={st['resolved_markets']} 模拟订单={st['dry_orders']} "
        f"真实订单={st['live_orders']} 真实金额=${st['live_usd']:.2f}"
    )

    t = Table(title="概率模型对比（只统计已有官方结算结果的决策快照）")
    for col in ("周期", "模型", "样本", "Brier↓", "LogLoss↓", "方向命中率"):
        t.add_column(col, justify="right" if col not in ("周期", "模型") else "left")
    for row in st["model_comparison"]:
        t.add_row(
            row["timeframe"],
            row["model"],
            str(row["n"]),
            _fmt_metric(row["brier"], 4),
            _fmt_metric(row["log_loss"], 4),
            _fmt_percent(row["accuracy"]),
        )
    console.print(t)

    perf = st["dry_run_performance"]
    console.print(
        "[bold]模拟交易表现（按实际 dry-run 订单，未计手续费/滑点）：[/] "
        f"已结算 {perf['trades']} 笔，赢 {perf['wins']} 笔，"
        f"命中率 {_fmt_percent(perf['hit_rate'])}，"
        f"投入 ${perf['stake_usd']:.2f}，"
        f"毛PnL {perf['pnl_usd']:+.2f}，ROI {_fmt_percent(perf['roi'])}"
    )

    ts = Table(title="策略对照（每个市场首次满足条件时固定投入 $1；未计手续费/滑点）")
    for col in ("周期", "策略", "交易数", "赢", "命中率", "毛PnL", "ROI"):
        ts.add_column(col, justify="right" if col not in ("周期", "策略") else "left")
    for row in st["strategy_comparison"]:
        ts.add_row(
            row["timeframe"],
            row["strategy"],
            str(row["trades"]),
            str(row["wins"]),
            _fmt_percent(row["hit_rate"]),
            f"${row['pnl_usd']:+.2f}",
            _fmt_percent(row["roi"]),
        )
    console.print(ts)

    t2 = Table(title="交易概率分桶 vs 当时市场中间价（含历史旧决策，仅作参考）")
    for col in ("概率区间", "样本数", "交易概率均值", "市场均值"):
        t2.add_column(col, justify="right")
    for b in st["buckets"]:
        upper = min(1.0, (b["b"] + 1) / 10)
        t2.add_row(
            f"{b['b']/10:.1f}-{upper:.1f}",
            str(b["n"]),
            f"{b['avg_p']:.2f}",
            f"{b['avg_mkt']:.2f}",
        )
    console.print(t2)


@app.command("settle")
def settle_cmd():
    """只执行一次官方结算结果回填，不运行交易。"""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        store = Store(s.db_path)
        try:
            async with AsyncPublicClient() as client:
                return await settle_pending_markets(
                    client, s, store, limit=500, grace_seconds=15.0
                )
        finally:
            store.close()

    n = _run(go())
    console.print(f"已新增回填 {n} 个市场结果")


# ---------------------------------------------------------------------------


async def _empty():
    return
    yield


def _fmt(x: float | None) -> str:
    return "-" if x is None else f"{x:.3f}"


def _fmt_cents(x: float | None) -> str:
    if x is None:
        return "—"
    cents = x * 100
    return f"{cents:.1f}¢" if abs(cents - round(cents)) > 1e-9 else f"{cents:.0f}¢"


def _fmt_usd(x: float | None) -> str:
    return "—" if x is None else f"${x:,.2f}"


def _fmt_delta(x: float | None) -> str:
    if x is None:
        return "—"
    sign = "+" if x >= 0 else "-"
    return f"{sign}${abs(x):,.2f}"


def _fmt_seconds(seconds: int | None) -> str:
    if seconds is None:
        return "—"
    minutes, secs = divmod(max(0, seconds), 60)
    if minutes:
        return f"{minutes}分{secs:02d}秒"
    return f"{secs}秒"


def _fmt_pct(value: float | None) -> str:
    return "—" if value is None else f"{value:+.3f}%"


def _fmt_num(value: float | None, digits: int = 2) -> str:
    return "—" if value is None else f"{value:+.{digits}f}"


def _fmt_metric(value: float | None, digits: int = 4) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def _fmt_percent(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _snapshot_not_ready_reason(snapshot: ShortTermSnapshot | None) -> str:
    if snapshot is None:
        return "没有获取到参考数据"
    missing = []
    if snapshot.target_price is None:
        missing.append("目标价")
    if snapshot.current_price is None:
        missing.append("当前价")
    if snapshot.seconds_left is None or snapshot.seconds_left <= 0:
        missing.append("有效剩余时间")
    if missing:
        return "权威参考数据缺失：" + "、".join(missing)
    pf = snapshot.path_features
    if pf is None:
        return "价格路径尚未建立"
    if not pf.feature_ready:
        age = "未知" if pf.latest_sample_age_seconds is None else f"{pf.latest_sample_age_seconds:.1f}s"
        return (
            f"价格路径仍在预热：历史 {pf.history_span_seconds}s，"
            f"最新样本年龄 {age}；等待更多实时样本"
        )
    return "参考数据尚未达到交易条件"


def _print_brief(b: Brief, cached: bool, full: bool = False) -> None:
    tag = "缓存" if cached else "官方 API"
    console.print(f"  [cyan]研究证据[/] 截至 {b.as_of or '?'}（{b.model or '研究模型'}，{tag}）：{b.summary}")
    if not full:
        return
    for f in b.key_facts:
        console.print(f"    • {f}")
    if b.latest_development:
        console.print(f"    最新进展：{b.latest_development}")
    if b.for_yes:
        console.print("    [green]支持主方向：[/] " + " | ".join(b.for_yes))
    if b.against_yes:
        console.print("    [red]反对主方向：[/] " + " | ".join(b.against_yes))
    for u in b.sources[:8]:
        console.print(f"    [dim]{u}[/]")


def _outcome_cn(label: str) -> str:
    return {"UP": "上涨", "DOWN": "下跌", "YES": "是", "NO": "否"}.get(label.upper(), label)


def _status_cn(status: str) -> str:
    return {
        "dry_run": "模拟下单",
        "refused": "已拒绝",
        "rejected": "交易所拒绝",
        "live": "已提交",
    }.get(status, status)


def _record_checkpoint_sample(
    store: Store,
    c: Candidate,
    s: Settings,
    snapshot: ShortTermSnapshot,
    state: dict,
    view,
    quant_p: float,
) -> None:
    tf = market_timeframe(c.market, s)
    if tf is None:
        return
    checkpoint = checkpoint_for(
        tf,
        snapshot.seconds_left,
        tolerance_seconds=s.evaluation_checkpoint_tolerance_seconds,
    )
    if checkpoint is None or snapshot.seconds_left is None:
        return
    inserted = store.log_evaluation_sample(
        slug=c.slug,
        condition_id=c.condition_id,
        timeframe=tf,
        strategy_version=s.strategy_version,
        checkpoint_seconds=checkpoint,
        seconds_left=snapshot.seconds_left,
        quant_p=quant_p,
        jev_p=view.p_yes,
        market_p=c.book.midpoint,
        yes_ask=c.book.yes_ask,
        no_ask=c.book.no_ask,
        book_json=c.book.microstructure_state(),
        state_json=state,
    )
    if inserted:
        console.print(
            f"  [dim]实验样本：{s.strategy_version} / {tf} / T-{checkpoint}s 已记录[/]"
        )


def _print_snapshot(snapshot: ShortTermSnapshot | None) -> None:
    if snapshot is None:
        console.print("  [yellow]参考数据：未获取[/]")
        return
    readiness = "[green]完整[/]" if snapshot.trade_ready else "[yellow]不完整[/]"
    console.print(
        f"  参考数据：目标价 {_fmt_usd(snapshot.target_price)}；"
        f"当前价 {_fmt_usd(snapshot.current_price)}；"
        f"差值 {_fmt_delta(snapshot.delta_usd)}（{_fmt_pct(snapshot.delta_pct)}）；"
        f"剩余 {_fmt_seconds(snapshot.seconds_left)}；状态 {readiness}"
    )
    pf = snapshot.path_features
    if pf is not None:
        console.print(
            "  价格路径："
            f"30秒 {_fmt_pct(pf.return_30s_pct)}；"
            f"1分钟 {_fmt_pct(pf.return_60s_pct)}；"
            f"3分钟 {_fmt_pct(pf.return_180s_pct)}；"
            f"5分钟 {_fmt_pct(pf.return_300s_pct)}"
        )
        console.print(
            "  波动/趋势："
            f"1分钟RV {_fmt_pct(pf.realized_vol_60s_pct)}；"
            f"3分钟RV {_fmt_pct(pf.realized_vol_180s_pct)}；"
            f"1分钟趋势 {_fmt_pct(pf.trend_60s_pct_per_min)}/分钟；"
            f"上涨tick {_fmt_num(None if pf.up_tick_ratio_60s is None else pf.up_tick_ratio_60s * 100, 1)}%；"
            f"剩余波动 {_fmt_pct(pf.remaining_vol_pct)}；Z {_fmt_num(pf.distance_z)}"
        )
        console.print(
            f"  [dim]路径源={pf.history_source}；样本={pf.sample_count}；"
            f"跨度={pf.history_span_seconds}s；"
            f"最新样本年龄={('—' if pf.latest_sample_age_seconds is None else f'{pf.latest_sample_age_seconds:.1f}s')}[/]"
        )
    console.print(
        f"  [dim]目标价源={snapshot.target_source or '缺失'}；"
        f"当前价源={snapshot.current_source or '缺失'}[/]"
    )


def _print_decision(
    c: Candidate,
    v,
    result,
    brief: Brief | None = None,
    cached: bool = False,
    snapshot: ShortTermSnapshot | None = None,
    quant_p_yes: float | None = None,
) -> None:
    tf = market_timeframe(c.market, _settings()) or "?"
    primary = _outcome_cn(c.book.yes_label)
    secondary = _outcome_cn(c.book.no_label)
    head = (
        f"[bold]{c.slug}[/]\n"
        f"  市场：BTC {TIMEFRAME_LABELS.get(tf, tf)}涨跌\n"
        f"  原始问题：{c.question}\n"
        f"  盘口：买入{primary} {_fmt_cents(c.book.yes_ask)}；卖出{primary} {_fmt_cents(c.book.yes_bid)}；"
        f"买入{secondary} {_fmt_cents(c.book.no_ask)}；卖出{secondary} {_fmt_cents(c.book.no_bid)}\n"
        f"  量化：P({primary})={('—' if quant_p_yes is None else f'{quant_p_yes:.2f}')} "
        f"（Φ(Z)）\n"
        f"  Jev参考：P({primary})={v.p_yes:.2f}  信息充分度={v.answerable:.2f}  "
        f"规则清晰度={v.clarity_mean}（置信度 {v.clarity_confidence}）"
    )
    console.print(head)
    if snapshot is not None:
        _print_snapshot(snapshot)
    if brief is not None:
        _print_brief(brief, cached)
    if isinstance(result, Trade):
        console.print(
            f"  [green]交易信号[/] 买入 {_outcome_cn(result.outcome)} "
            f"{result.size} 份 @ {result.price}（${result.usd:.2f}） 优势 {result.edge:+.2f}"
        )
    else:
        console.print(f"  [dim]跳过：{result.reason}[/]")


def _decision_row(
    c: Candidate,
    state: dict,
    v,
    result,
    brief: Brief | None,
    executed: bool,
    signal_p_yes: float | None = None,
) -> dict:
    is_trade = isinstance(result, Trade)
    return dict(
        slug=c.slug, condition_id=c.condition_id, question=c.question, state_json=state,
        p_yes=(v.p_yes if signal_p_yes is None else signal_p_yes),
        jev_p_yes=v.p_yes,
        timeframe=market_timeframe(c.market, _settings()),
        strategy_version=_settings().strategy_version,
        answerable=v.answerable, clarity=v.clarity,
        yes_ask=c.book.yes_ask, no_ask=c.book.no_ask, midpoint=c.book.midpoint,
        edge=result.edge if is_trade else None,
        action=("trade" if is_trade else "skip") + ("" if not is_trade or executed else "_unexecuted"),
        reason=result.rationale if is_trade else result.reason,
        jev_model=v.model, jev_cost=v.cost, research_cost=(brief.cost if brief else None), raw_json=v.raw,
    )


if __name__ == "__main__":
    app()
