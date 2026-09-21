# Contributing

Thanks for taking a look. This is a small, opinionated bot; PRs that keep it small are the
most welcome.

## Setup

```bash
git clone https://github.com/markusbug/jevymarket && cd jevymarket
uv sync --all-groups
cp .env.example .env   # only needed for live commands; tests are fully mocked
```

## Before opening a PR

```bash
uv run ruff check .
uv run pytest
```

- Keep the Jev state small. Every field in `markets.build_state` costs accuracy; justify additions.
- Anything that can spend money lives in `executor.py` and must go through `Executor.check`.
- Pure logic (edge, sizing, gates) belongs in `signal.py` with a test.
- Never commit `.env`, `*.db`, keys, or wallet addresses.

## Ideas that would fit

- Exit logic / position management (currently buy-and-hold to resolution).
- A calibration report: Jev's `p_yes` vs actual resolutions from the SQLite log.
- Alternative researchers (native-search models, news APIs) behind the same `Brief` interface.
