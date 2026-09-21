"""jevymarket command-line interface."""

from __future__ import annotations

import asyncio
import json
import logging

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .config import Settings, load_settings
from .jev import JevClient, JevError, choice, noul, score
from .markets import TIMEFRAME_LABELS, Candidate, load_candidate, market_timeframe, scan
from .research import Brief, Researcher, ResearchError
from .signal import Trade, evaluate, get_brief, research_and_ask
from .store import Store

app = typer.Typer(help="Polymarket BTC 短周期交易机器人：TypeSafe Jev + DeepSeek 官方 API。", no_args_is_help=True)
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
    return JevClient(s.typesafe_api_key, model=s.jev_model, base_url=s.typesafe_base_url)


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
        async with AsyncPublicClient() as c:
            return await scan(c, s, limit=limit, pages=pages)

    cands = _run(go())
    t = Table(title=f"候选市场：{len(cands)} 个（仅 BTC 5分钟 / 15分钟 / 1小时）")
    for col in ("市场 slug", "周期", "上涨买价", "上涨卖价", "下跌卖价", "流动性 $", "成交量 $"):
        t.add_column(col, justify="right" if col not in ("市场 slug", "周期") else "left")
    for cand in cands:
        m = cand.market
        tf = market_timeframe(m, s) or "?"
        t.add_row(cand.slug[:72], TIMEFRAME_LABELS.get(tf, tf), _fmt(cand.book.yes_bid),
                  _fmt(cand.book.yes_ask), _fmt(cand.book.no_ask),
                  f"{float(m.metrics.liquidity_num or 0):,.0f}", f"{float(m.metrics.volume_num or 0):,.0f}")
    console.print(t)



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
           no_research: bool = typer.Option(False, "--no-research", help="跳过 DeepSeek 研究步骤。"),
           fresh: bool = typer.Option(False, help="忽略研究缓存。")):
    """研究单个市场并让 Jev 判断；不会真实下单。"""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        store = Store(s.db_path)
        r = _researcher(s, not no_research)
        async with AsyncPublicClient() as c, _jev(s) as jev:
            try:
                cand = await load_candidate(c, s, ref)
                state, brief, cached, view = await research_and_ask(cand, s, store, r, jev, fresh=fresh)
            finally:
                if r:
                    await r.aclose()
            if show_state:
                console.print_json(json.dumps(state))
            result = evaluate(view, cand.book, s)
            _print_decision(cand, view, result, brief, cached)
            store.log_decision(**_decision_row(cand, state, view, result, brief, executed=False))

    _run(go())


@app.command()
def run(
    dry_run: bool = typer.Option(False, "--dry-run", help="只评估和记录，不真实下单。"),
    max_trades: int | None = typer.Option(None, help="覆盖每轮最大交易数。"),
    limit: int = typer.Option(20, "--limit", "-n", help="本轮最多分析的候选市场数量。"),
    loop: int | None = typer.Option(None, help="每 N 秒重复一轮。"),
    no_research: bool = typer.Option(False, "--no-research", help="跳过 DeepSeek 研究步骤。"),
):
    """扫描 → 研究 → Jev 判断 → 风控 → 模拟/真实下单。"""
    s = _settings(dry_run=dry_run or None)
    if max_trades is not None:
        s.max_trades_per_run = max_trades
    from polymarket import AsyncPublicClient

    from .executor import Executor

    async def one_pass(store: Store, ex: Executor, pub: AsyncPublicClient, jev: JevClient,
                       r: Researcher | None):
        cands = await scan(pub, s, limit=limit)
        console.rule(f"候选 {len(cands)} 个 | 当前敞口 ${(await ex.exposure(refresh=True)).total:.2f} | 模拟模式={s.dry_run}")
        for idx, cand in enumerate(cands, start=1):
            tf = market_timeframe(cand.market, s) or "?"
            console.print(f"\n[cyan]正在分析 {idx}/{len(cands)}：BTC {TIMEFRAME_LABELS.get(tf, tf)}[/]  [dim]{cand.slug}[/]")
            if cand.condition_id in (await ex.exposure()).condition_ids or store.has_order_for(cand.condition_id):
                console.print(f"[dim]{cand.slug}：已有持仓或挂单，跳过[/]")
                continue
            try:
                state, brief, cached, view = await research_and_ask(cand, s, store, r, jev)
            except JevError as e:
                console.print(f"[red]{cand.slug}: {e}[/]")
                if e.status in (401, 402):
                    raise
                continue
            result = evaluate(view, cand.book, s)
            _print_decision(cand, view, result, brief, cached)
            executed = False
            if isinstance(result, Trade):
                placed = await ex.place(cand, result)
                executed = placed.ok
                if not placed.ok:
                    console.print(f"  [yellow]{_status_cn(placed.status)}：{placed.message}[/]")
                else:
                    console.print(f"  [green]{_status_cn(placed.status)}[/] 订单ID={placed.order_id}")
            store.log_decision(**_decision_row(cand, state, view, result, brief, executed=executed))
            if ex.trades_this_run >= s.max_trades_per_run:
                console.print("[bold]本轮交易数量已达到上限[/]")
                break
        spend = f"Jev：{jev.calls} 次调用，输入 {jev.total_input_tokens} / 输出 {jev.total_output_tokens} tokens"
        if r:
            spend += f" | DeepSeek：{r.calls} 份研究，输入 {r.total_input_tokens} / 输出 {r.total_output_tokens} tokens"
        console.print(f"[dim]{spend}[/]")

    async def go():
        store = Store(s.db_path)
        ex = await Executor.create(s, store, dry_run=s.dry_run)
        r = _researcher(s, not no_research)
        try:
            async with AsyncPublicClient() as pub, _jev(s) as jev:
                while True:
                    ex.trades_this_run = 0
                    if r:
                        r.calls = 0
                    await one_pass(store, ex, pub, jev, r)
                    if loop is None:
                        break
                    await asyncio.sleep(loop)
        finally:
            if r:
                await r.aclose()
            await ex.close()
            store.close()

    _run(go())


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
    """Decision/order counts and a rough Jev-vs-market calibration table."""
    s = _settings()
    st = Store(s.db_path).stats()
    console.print(f"决策数={st['decisions']} 交易信号={st['trade_signals']} 研究摘要={st['briefs']} "
                  f"真实订单={st['live_orders']} 真实金额=${st['live_usd']:.2f}")
    t = Table(title="Jev 主方向概率分桶 vs 市场中间价")
    for col in ("概率区间", "样本数", "Jev均值", "市场均值"):
        t.add_column(col, justify="right")
    for b in st["buckets"]:
        t.add_row(f"{b['b']/10:.1f}-{(b['b']+1)/10:.1f}", str(b["n"]), f"{b['avg_p']:.2f}", f"{b['avg_mkt']:.2f}")
    console.print(t)


# ---------------------------------------------------------------------------


async def _empty():
    return
    yield


def _fmt(x: float | None) -> str:
    return "-" if x is None else f"{x:.3f}"


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


def _print_decision(c: Candidate, v, result, brief: Brief | None = None, cached: bool = False) -> None:
    tf = market_timeframe(c.market, _settings()) or "?"
    primary = _outcome_cn(c.book.yes_label)
    secondary = _outcome_cn(c.book.no_label)
    head = (
        f"[bold]{c.slug}[/]\n"
        f"  市场：BTC {TIMEFRAME_LABELS.get(tf, tf)}涨跌\n"
        f"  原始问题：{c.question}\n"
        f"  盘口：{primary} 买价/卖价 {_fmt(c.book.yes_bid)}/{_fmt(c.book.yes_ask)}；"
        f"{secondary} 卖价 {_fmt(c.book.no_ask)}\n"
        f"  Jev：P({primary})={v.p_yes:.2f}  信息充分度={v.answerable:.2f}  "
        f"规则清晰度={v.clarity_mean}（置信度 {v.clarity_confidence}）"
    )
    console.print(head)
    if brief is not None:
        _print_brief(brief, cached)
    if isinstance(result, Trade):
        console.print(
            f"  [green]交易信号[/] 买入 {_outcome_cn(result.outcome)} "
            f"{result.size} 份 @ {result.price}（${result.usd:.2f}） 优势 {result.edge:+.2f}"
        )
    else:
        console.print(f"  [dim]跳过：{result.reason}[/]")


def _decision_row(c: Candidate, state: dict, v, result, brief: Brief | None, executed: bool) -> dict:
    is_trade = isinstance(result, Trade)
    return dict(
        slug=c.slug, condition_id=c.condition_id, question=c.question, state_json=state,
        p_yes=v.p_yes, answerable=v.answerable, clarity=v.clarity,
        yes_ask=c.book.yes_ask, no_ask=c.book.no_ask, midpoint=c.book.midpoint,
        edge=result.edge if is_trade else None,
        action=("trade" if is_trade else "skip") + ("" if not is_trade or executed else "_unexecuted"),
        reason=result.rationale if is_trade else result.reason,
        jev_model=v.model, jev_cost=v.cost, research_cost=(brief.cost if brief else None), raw_json=v.raw,
    )


if __name__ == "__main__":
    app()
