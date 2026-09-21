# jevymarket-direct

A Polymarket trading bot whose pricing oracle is **Jev / TypeSafe System One**. This fork is now
focused on **Bitcoin Up/Down short-term markets only: 5 minutes, 15 minutes, and 1 hour**. It is
derived from [markusbug/jevymarket](https://github.com/markusbug/jevymarket), but removes OpenRouter
from the model path.

- **Jev** is called directly through TypeSafe's official `POST /v1/systemone` API.
- **Research** is done in two DeepSeek-official stages: Anthropic-compatible Messages + native
  `web_search_20250305` for current facts, then OpenAI-compatible Chat Completions JSON mode to
  normalize the verified memo into the strict `Brief` schema.
- Market scanning, signal gates, Kelly sizing, exposure caps, execution, caching, and SQLite logging
  retain the upstream design.

> Experimental software can place real-money orders. Start with `--dry-run`, use a dedicated
> low-balance wallet, and review `SECURITY.md` before any live use.

## Decision pipeline

~~~
scan -> DeepSeek web search -> DeepSeek JSON normalize -> compact evidence -> Jev -> evaluate -> execute -> SQLite
~~~

DeepSeek gathers dated, sourced facts and is instructed not to estimate a probability or use
prediction-market odds. Jev receives the compact evidence and produces the typed probability,
answerability, and clarity outputs used by the existing trading gates.

This fork adds two evidence safeguards:

1. A research call is rejected unless DeepSeek actually returns a native
   `web_search_tool_result`.
2. Prediction-market and odds domains are blocked in the native search request. If an excluded URL
   still appears in structured results or the final JSON source list, the entire brief is rejected.

## Install

~~~
git clone git@github.com:WangYu0611/jevymarket-direct.git
cd jevymarket-direct
uv sync
cp .env.example .env
~~~

Fill in `.env`:

~~~dotenv
TYPESAFE_API_KEY=...
DEEPSEEK_API_KEY=...
POLYMARKET_PRIVATE_KEY=0x...
# POLYMARKET_WALLET=0x...
~~~

No `OPENROUTER_API_KEY` is required.

## Commands

~~~
uv run jevymarket jev-test
uv run jevymarket scan -n 15
uv run jevymarket research <slug|url> --fresh
uv run jevymarket decide <slug|url> --show-state
uv run jevymarket run --dry-run
uv run jevymarket run --max-trades 1
uv run jevymarket run --loop 900
uv run jevymarket setup
uv run jevymarket positions
uv run jevymarket stats
~~~

The upstream `run` command is live by default. Use `--dry-run` until both provider calls and the
strategy output have been checked with your own credentials.

## Trading scope

The scanner only accepts BTC directional recurring markets matching the 5m, 15m, or 1h patterns.
ETH, SOL, 4h, daily target, and monthly target markets are ignored before model calls.

~~~dotenv
ALLOWED_ASSETS=BTC
ALLOWED_TIMEFRAMES=5m,15m,1h
SHORT_TERM_MIN_LIQUIDITY_USD=0
SHORT_TERM_MIN_VOLUME_USD=0
~~~

The 5m and 15m markets resolve from Chainlink BTC/USD TWAP; the hourly market resolves from the
Binance BTC/USDT 1H candle. The short-term strategy therefore still needs a dedicated real-time
market-data layer before serious live trading.

## Provider configuration

| variable | default | purpose |
|---|---|---|
| `TYPESAFE_API_KEY` | — | TypeSafe/Jev credential |
| `TYPESAFE_BASE_URL` | `https://api.typesafe.ai/v1` | TypeSafe official API base |
| `JEV_MODEL` | `jev-latest` | Jev model/version |
| `DEEPSEEK_API_KEY` | — | DeepSeek official credential |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com/anthropic/v1` | DeepSeek Messages/web-search API base |
| `DEEPSEEK_JSON_BASE_URL` | `https://api.deepseek.com` | DeepSeek Chat Completions JSON-mode base |
| `RESEARCH_MODEL` | `deepseek-v4-pro` | researcher model |
| `RESEARCH_MAX_SEARCHES` | `5` | native web-search uses per research call |
| `RESEARCH_TTL_HOURS` | `6` | cached brief lifetime |
| `MAX_RESEARCH_PER_RUN` | `20` | hard cap on research calls per pass |
| `RESEARCH_MAX_CHARS` | `2500` | evidence block limit passed to Jev |

The original trading/risk settings such as `MIN_EDGE`, `MIN_ANSWERABLE`, `MIN_CLARITY`, the
trade-price band, exposure caps, and Kelly fraction are unchanged.

## What changed from upstream

~~~
src/jevymarket/config.py    OpenRouter key -> TypeSafe + DeepSeek keys
src/jevymarket/jev.py       OpenRouter Decisions -> TypeSafe /v1/systemone
src/jevymarket/research.py  OpenRouter/Exa -> DeepSeek native server web search
src/jevymarket/cli.py       direct provider client construction
.env.example                direct-provider variables
tests/                      direct-provider request/response tests
~~~

`markets.py`, `signal.py`, `executor.py`, and `store.py` are intentionally kept on the
upstream strategy design so the provider migration does not silently alter signal or risk logic.

## Verification order

~~~
uv run pytest
uv run jevymarket jev-test
uv run jevymarket research <a-current-market> --fresh
uv run jevymarket decide <same-market> --show-state
uv run jevymarket run --dry-run
~~~

For research output, verify the facts are current, URLs are reputable, and no prediction-market or
odds site appears in the evidence.

## Attribution and license

Derived from [markusbug/jevymarket](https://github.com/markusbug/jevymarket). The upstream project
is MIT licensed; this repository retains its `LICENSE` and attribution.
