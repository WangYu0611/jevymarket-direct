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
