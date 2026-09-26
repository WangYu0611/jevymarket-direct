# V8.1 Early Window Price-Value Forward

V8.1 is a fresh forward experiment motivated by the V8 observation that the
useful price-value opportunities were concentrated well before the final minute.

It deliberately does **not** continue the V8 database. The timing change was
chosen after looking at V8 results, so V8.1 needs new future markets.

## Primary trading window

Only:

```text
T-120 seconds > seconds_left > T-90 seconds
```

can create value trades.

The 30-second early window is split into three non-overlapping 10-second
evaluation slots:

```text
T-120 slot: 111..120 seconds left
T-110 slot: 101..110 seconds left
T-100 slot:  91..100 seconds left
```

T-90 and later are excluded from this strategy. T-60 / T-45 are no longer
trade checkpoints.

The first qualifying value trade per arm per market is still the only one that
counts.

## Value rules are unchanged from V8

- Quant high-confidence: >=92% for the selected side
- minimum probability edge versus the predicted side's **best ask**: 8%
- allowed ask range: 0.10 through 0.90
- maximum spread: 0.06
- maximum simulated notional: USD 5
- predicted-side 5-cent ask depth must cover the simulated notional
- same taker fee model as V8
- one simulated trade per arm per market

The profitability gate is also unchanged:

- >=50 settled forward simulated trades
- win rate >65%
- net PnL after estimated taker fees >0
- net PnL after removing the three largest winning contributions >0
- net PnL after worsening every entry by one tick >0

## Arms

A — Quant -> current ask inside the T-120..T-90 window.

B — Quant + Jev. After Jev returns, the strategy independently refreshes the
public order book and Quant state. It uses the response-time ask, not the older
request-time price.

C — B plus Jev's own probability must also exceed the refreshed ask by the same
8% minimum edge.

## Terminal visibility

The terminal continues to show accepted/skipped value panels, but V8.1 adds a
persistent cumulative rejection table. It shows whether opportunities are being
lost because of:

- Quant not high-confidence
- ask outside the 0.10..0.90 band
- edge below 8%
- spread too wide
- insufficient 5-cent ask depth
- Jev quality or directional disagreement
- response-time Quant no longer high-confidence
- Jev busy / error / expired / interrupted

This is diagnostic only; these counts do not change the strategy.

## Run a fresh V8.1 sample

Recommended first run: 24 hours because the strict value filter produces far
fewer trades than raw prediction signals.

```powershell
Set-Location -LiteralPath "C:\Users\wy331\Documents\jevymarket-direct"
git pull --ff-only

uv run --frozen python -m jevymarket.price_value_early_forward --seconds 86400
```

Outputs:

```text
runs/jevymarket.v81-early-value_*.db
runs/v81_early_value_forward_*.json.gz
```

If Windows restarts or the process is interrupted, resume only the V8.1 DB:

```powershell
uv run --frozen python -m jevymarket.price_value_early_forward \
    --resume-db "runs\jevymarket.v81-early-value_<timestamp>.db" \
    --seconds 86400
```

Do not use a V7 or V8 database with `--resume-db`; the stored experiment
manifest will reject a mismatched protocol.
