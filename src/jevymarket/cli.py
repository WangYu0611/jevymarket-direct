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
from .markets import Candidate, load_candidate, scan
from .research import Brief, Researcher, ResearchError
from .signal import Trade, evaluate, get_brief, research_and_ask
from .store import Store

app = typer.Typer(help="Polymarket trading bot driven by Jev (TypeSafe AI) via OpenRouter.", no_args_is_help=True)
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
    return Researcher(s.openrouter_api_key, model=s.research_model, base_url=s.openrouter_base_url,
                      max_results=s.research_max_results, max_calls=s.max_research_per_run,
                      exclude_domains=s.research_exclude_domains)


def _jev(s: Settings) -> JevClient:
    return JevClient(s.openrouter_api_key, model=s.jev_model, base_url=s.openrouter_base_url)


# ---------------------------------------------------------------------------


@app.callback()
def _root(version: bool = typer.Option(False, "--version", is_eager=True)):
    if version:
        console.print(f"jevymarket {__version__}")
        raise typer.Exit()


@app.command("jev-test")
def jev_test(raw: bool = typer.Option(False, help="Print the raw JSON response only.")):
    """One hard-coded Jev call through OpenRouter to verify the key and response schema."""
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
        async with JevClient(s.openrouter_api_key, model=s.jev_model, base_url=s.openrouter_base_url) as jev:
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
    """List candidate markets that pass the static filters, with live best bid/ask."""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        async with AsyncPublicClient() as c:
            return await scan(c, s, limit=limit, pages=pages)

    cands = _run(go())
    t = Table(title=f"{len(cands)} candidates")
    for col in ("slug", "yes bid", "yes ask", "no ask", "days", "liq $", "vol $"):
        t.add_column(col, justify="right" if col != "slug" else "left")
    for c in cands:
        m = c.market
        t.add_row(c.slug[:70], _fmt(c.book.yes_bid), _fmt(c.book.yes_ask), _fmt(c.book.no_ask),
                  str(c.days_to_resolution), f"{float(m.metrics.liquidity_num or 0):,.0f}",
                  f"{float(m.metrics.volume_num or 0):,.0f}")
    console.print(t)



@app.command()
def research(ref: str = typer.Argument(..., help="Market slug or polymarket.com URL"),
             fresh: bool = typer.Option(False, help="Ignore the cache and research again.")):
    """Run only the researcher for one market and print the evidence brief."""
    s = _settings()
    from polymarket import AsyncPublicClient

    async def go():
        store = Store(s.db_path)
        r = Researcher(s.openrouter_api_key, model=s.research_model, base_url=s.openrouter_base_url,
                       max_results=s.research_max_results, exclude_domains=s.research_exclude_domains)
        async with AsyncPublicClient() as c, r:
            cand = await load_candidate(c, s, ref)
            brief, cached = await get_brief(cand, s, store, r, fresh=fresh)
            if brief is None:
                console.print("[red]no brief produced[/]")
                raise typer.Exit(1) from None
            console.print(f"[bold]{cand.question}[/]")
            _print_brief(brief, cached, full=True)

    _run(go())


@app.command()
def decide(ref: str = typer.Argument(..., help="Market slug or polymarket.com URL"),
           show_state: bool = typer.Option(False, help="Print the state sent to Jev."),
           no_research: bool = typer.Option(False, "--no-research", help="Skip the researcher step."),
           fresh: bool = typer.Option(False, help="Ignore the research cache.")):
    """Research + ask Jev about one market and show the proposed trade. Never places orders."""
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
    dry_run: bool = typer.Option(False, "--dry-run", help="Evaluate and log, but place no orders."),
    max_trades: int | None = typer.Option(None, help="Override MAX_TRADES_PER_RUN."),
    limit: int = typer.Option(20, "--limit", "-n", help="Max candidate markets to evaluate."),
    loop: int | None = typer.Option(None, help="Repeat every N seconds."),
    no_research: bool = typer.Option(False, "--no-research", help="Skip the researcher step."),
):
    """Scan → research → ask Jev → trade edges above threshold, under hard caps."""
    s = _settings(dry_run=dry_run or None)
    if max_trades is not None:
        s.max_trades_per_run = max_trades
    from polymarket import AsyncPublicClient

    from .executor import Executor

    async def one_pass(store: Store, ex: Executor, pub: AsyncPublicClient, jev: JevClient,
                       r: Researcher | None):
        cands = await scan(pub, s, limit=limit)
        console.rule(f"{len(cands)} candidates | exposure ${ (await ex.exposure(refresh=True)).total:.2f} | dry_run={s.dry_run}")
        for cand in cands:
            if cand.condition_id in (await ex.exposure()).condition_ids or store.has_order_for(cand.condition_id):
                console.print(f"[dim]{cand.slug}: already exposed, skip[/]")
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
                    console.print(f"  [yellow]{placed.status}: {placed.message}[/]")
                else:
                    console.print(f"  [green]{placed.status}[/] order_id={placed.order_id}")
            store.log_decision(**_decision_row(cand, state, view, result, brief, executed=executed))
            if ex.trades_this_run >= s.max_trades_per_run:
                console.print("[bold]trade cap for this run reached[/]")
                break
        spend = f"Jev: {jev.calls} calls, {jev.total_input_tokens} tokens, ${jev.total_cost:.5f}"
        if r:
            spend += f" | researcher: {r.calls} briefs, ${r.total_cost:.4f}"
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
            console.print(f"wallet {ex.wallet} ({ex.wallet_type})")
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
            console.print(f"wallet {ex.wallet} ({ex.wallet_type})  pUSD: {'n/a (no key)' if bal is None else f'${bal:,.2f}'}")
            t = Table(title="open positions")
            for col in ("slug", "outcome", "size", "avg", "cur", "value $", "pnl $"):
                t.add_column(col)
            async for p in ex.client.list_positions(user=ex.wallet, status="OPEN").iter_items():
                t.add_row(str(p.slug)[:60], str(p.outcome), f"{float(p.current_size or 0):.2f}", f"{float(p.avg_price or 0):.3f}",
                          f"{float(p.current_price or 0):.3f}", f"{float(p.current_value or 0):.2f}", f"{float(p.total_pnl or 0):+.2f}")
            console.print(t)
            t2 = Table(title="open orders")
            for col in ("id", "side", "outcome", "price", "size", "matched", "status"):
                t2.add_column(col)
            async for o in (ex.client.list_open_orders().iter_items() if ex.authenticated else _empty()):
                t2.add_row(str(o.id)[:12], str(o.side), str(o.outcome), str(o.price), str(o.original_size), str(o.size_matched), str(o.status))
            console.print(t2)
            exx = await ex.exposure(refresh=True)
            console.print(f"exposure: positions ${exx.positions_usd:.2f} + open orders ${exx.open_orders_usd:.2f} = ${exx.total:.2f} (cap ${s.max_open_exposure_usd:.2f})")
        finally:
            await ex.close()

    _run(go())


@app.command()
def stats():
    """Decision/order counts, Jev spend, and a rough Jev-vs-market calibration table."""
    s = _settings()
    st = Store(s.db_path).stats()
    console.print(f"decisions={st['decisions']} trade_signals={st['trade_signals']} "
                  f"jev_cost=${st['jev_cost_usd']:.5f} briefs={st['briefs']} research_cost=${st['research_cost_usd']:.4f} "
                  f"live_orders={st['live_orders']} live_usd=${st['live_usd']:.2f}")
    t = Table(title="Jev P(yes) buckets vs market midpoint")
    for col in ("bucket", "n", "avg jev", "avg market"):
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
    tag = "cached" if cached else f"${b.cost:.4f}"
    console.print(f"  [cyan]evidence[/] as of {b.as_of or '?'} ({b.model or 'researcher'}, {tag}): {b.summary}")
    if not full:
        return
    for f in b.key_facts:
        console.print(f"    • {f}")
    if b.latest_development:
        console.print(f"    latest: {b.latest_development}")
    if b.for_yes:
        console.print("    [green]for YES:[/] " + " | ".join(b.for_yes))
    if b.against_yes:
        console.print("    [red]against YES:[/] " + " | ".join(b.against_yes))
    for u in b.sources[:8]:
        console.print(f"    [dim]{u}[/]")


def _print_decision(c: Candidate, v, result, brief: Brief | None = None, cached: bool = False) -> None:
    head = (f"[bold]{c.slug}[/]\n  {c.question}\n"
            f"  market yes bid/ask {_fmt(c.book.yes_bid)}/{_fmt(c.book.yes_ask)}  no ask {_fmt(c.book.no_ask)}  "
            f"days={c.days_to_resolution}\n"
            f"  Jev: p_yes={v.p_yes:.2f} answerable={v.answerable:.2f} clarity={v.clarity_mean} "
            f"(conf {v.clarity_confidence}) cost=${v.cost:.6f}")
    console.print(head)
    if brief is not None:
        _print_brief(brief, cached)
    if isinstance(result, Trade):
        console.print(f"  [green]TRADE[/] BUY {result.outcome} {result.size} @ {result.price} (${result.usd:.2f}) edge {result.edge:+.2f}")
    else:
        console.print(f"  [dim]skip: {result.reason}[/]")


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
