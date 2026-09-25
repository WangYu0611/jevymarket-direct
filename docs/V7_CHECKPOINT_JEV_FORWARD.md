# v7 checkpoint + Jev forward prediction experiment

This experiment pauses Maker execution research and measures prediction quality first.

It never creates paper orders, never signs anything, and never opens an authenticated
Polymarket client.

## Protocol

BTC 5-minute markets only. Fixed checkpoints:

- T-120
- T-90
- T-60
- T-45

The process samples every 10 seconds. A checkpoint is recorded once per market, using
the first valid observation no more than 10 seconds late.

Quant remains the primary trigger. Jev is requested only when Quant is already highly
directional:

- UP when Quant P(Up) >= 0.92
- DOWN when Quant P(Up) <= 0.08
- otherwise Jev is not called at that checkpoint

Jev runs asynchronously from the stored request-time snapshot. It does not block the
market-data loop and it does not receive Polymarket's absolute market probability.

## Three forward arms

Each arm uses at most one signal per independent 5-minute market: the first checkpoint
that satisfies that arm.

- **A Quant** — high-confidence Quant only.
- **B Quant + Jev** — A plus Jev success, existing answerability/clarity quality gate,
  and Jev directional agreement.
- **C Quant + Jev + Market** — B plus Polymarket midpoint directional agreement.

The three arms are filters over forward observations. B/C do not retrospectively choose
a better checkpoint after resolution.

## Prediction gate

For each arm separately:

- at least 50 resolved independent signal markets;
- observed win rate strictly greater than 65%.

The report also includes a 95% Wilson interval, coverage, checkpoint distribution,
Jev request/success counts and Jev latency. Passing the project gate is not a guarantee
of future profitability.

Headline win rate is deliberately separate from execution. Fees, fills, queue position,
slippage and Maker spread capture are not part of this experiment.

## Run

First validation horizon: 6 hours. At most 72 complete 5-minute markets can occur in
six hours, so shorter than 4h10m could not possibly satisfy a 50-market gate even with
100% signal coverage.

```powershell
Set-Location -LiteralPath "C:\Users\wy331\Documents\jevymarket-direct"
git pull --ff-only

uv run --frozen python -m jevymarket.checkpoint_jev_forward --seconds 21600
```

A valid `TYPESAFE_API_KEY` is required because this experiment actually calls Jev.
It does not use `POLYMARKET_PRIVATE_KEY`.

Outputs default to:

```text
runs/jevymarket.forward-jev_*.db
runs/v7_checkpoint_jev_forward_*.json.gz
```

Upload only the gzip report for analysis. Do not upload `.env`.


## Clear terminal dashboard

The terminal now uses highlighted panels instead of relying on plain log lines.

At each checkpoint it shows:

- Quant direction and directional probability;
- Polymarket direction and probability of the Quant-predicted side;
- Quant minus Market probability edge in percentage points;
- whether Arm A has a signal;
- whether Jev is pending or skipped.

When Jev returns, a second highlighted panel shows:

- Jev direction/probability;
- answerability and clarity;
- Jev latency;
- A / B / C pass/filter status.

A cumulative scoreboard is printed whenever settled results change:

```text
前向预测累计成绩（独立已结算市场）
A Quant                 48   48/0   100.0%   距50=2
B Quant + Jev           47   47/0   100.0%   距50=3
C Quant + Jev + Market  47   47/0   100.0%   距50=3
```

The report also records directional Quant-vs-Market and Jev-vs-Market edge diagnostics.
No edge threshold is introduced in this dataset; the values are collected for the
next preregistered price-value experiment.

## Resume an existing forward database

Do not throw away a nearly-complete independent-market sample just to improve the UI.
An existing v7 database can be continued:

```powershell
uv run --frozen python -m jevymarket.checkpoint_jev_forward \
    --resume-db "runs/jevymarket.forward-jev_20260925_182857_021098.db" \
    --seconds 3600
```

The stored experiment manifest is checked before appending. Existing market/checkpoint
rows are not duplicated because the database enforces one recorded checkpoint per
market/checkpoint. A new cumulative report is written under `runs/`; the source
database is retained.
