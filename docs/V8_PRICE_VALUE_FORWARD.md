# V8 Price-Value Forward

V8 starts only after the v7 prediction gate has been passed. It does not reuse
v7 outcomes as validation data.

The question is no longer "can we predict BTC 5m direction?" but:

> When a high-confidence prediction appears, is the observable taker price still
> good enough to produce positive forward returns after fees?

## Fixed forward protocol

BTC 5-minute markets only.

Checkpoints:

- T-120
- T-90
- T-60
- T-45

Sampling remains every 10 seconds.

No thresholds in this file are fitted on the v7 result. V8 reuses pre-existing
project settings:

- Quant high-confidence: P(Up) >= 0.92 or <= 0.08
- minimum model-vs-ASK edge: 0.08
- allowed ASK band: 0.10 through 0.90
- maximum spread: 0.06
- maximum simulated notional per trade: USD 5
- Jev answerability / clarity gates from the existing settings

The predicted side must also have enough recorded 5-cent ask-side depth to cover
the simulated notional.

## Arms

### A — Quant -> checkpoint ASK

At the checkpoint, if Quant is high-confidence and all fixed value constraints
pass, V8 records one taker-like simulated BUY using the predicted side's current
best ask.

At most one A trade is recorded per market.

### B — Quant + Jev -> response-time ASK

Jev is requested asynchronously from the checkpoint snapshot.

When Jev returns, V8 does **not** reuse the old request-time price. It refreshes:

- the current public order book;
- the current Chainlink/TWAP snapshot;
- the current Quant probability.

B records a trade only if:

- Jev passes answerability/clarity;
- Jev direction agrees with the original Quant direction;
- refreshed Quant is still >=92% on the same side;
- the same fixed price/spread/edge/depth rules pass at the refreshed best ask.

This is intended to measure a more realistic Jev-confirmed decision price.

### C — Jev Own Value

C requires B to qualify, and additionally requires Jev's own directional
probability to exceed the response-time ask by the same fixed 8% edge.

## Taker fee assumption

The current V8 default uses a crypto taker fee rate parameter of:

```text
0.07
```

and estimates the fee in USD-equivalent terms as:

```text
shares * fee_rate * price * (1 - price)
```

rounded to five decimals.

This is an experiment assumption captured in the stored manifest. Taker rebates
are not included.

If Polymarket changes the rate in the future, start a new experiment manifest
rather than silently mixing fee regimes.

## Profitability gate

Each arm passes only if all are true:

- at least 50 settled forward simulated trades;
- win rate strictly >65%;
- estimated net PnL after taker fees >0;
- net PnL remains >0 after removing the three largest positive trade contributions;
- net PnL remains >0 if every recorded entry price is worsened by one tick.

The report also includes a +1 cent price-stress diagnostic.

Passing this gate is not a guarantee of live profitability. Best ask is public
order-book evidence, not authenticated fill proof.

## Run

A fresh V8 sample is required. Do **not** resume a v7 database.

Default horizon is 12 hours:

```powershell
Set-Location -LiteralPath "C:\Users\wy331\Documents\jevymarket-direct"
git pull --ff-only

uv run --frozen python -m jevymarket.price_value_forward --seconds 43200
```

Generated files:

```text
runs/jevymarket.v8-price-value_*.db
runs/v8_price_value_forward_*.json.gz
```

If interrupted, resume the V8 database only:

```powershell
uv run --frozen python -m jevymarket.price_value_forward \
    --resume-db "runs/jevymarket.v8-price-value_<timestamp>.db" \
    --seconds 21600
```

The terminal dashboard shows each accepted/skipped value decision and an
arm-by-arm cumulative table for settled trades, net PnL, net ROI and distance to
the 50-trade gate.

Upload only the gzip report for analysis. Do not upload `.env`.
