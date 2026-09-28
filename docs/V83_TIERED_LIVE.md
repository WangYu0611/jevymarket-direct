# V8.3 Tiered Live Strategy

V8.3 responds to the first real V8.2 losing order without changing the proven
Polymarket authentication, FAK submission, settlement, or fail-closed guards.
It changes candidate selection and sizing only.

## Why this revision exists

The first real loss had:

```text
selected-side Quant ≈ 93.45%
selected-side Jev = 60%
entry ask cap = 0.56
actual fill ≈ 0.55
```

Execution was normal. The weakness was that V8.2 treated any Jev directional
agreement above 50% as confirmation. In the available T-120 historical sample,
all four losing rows also had selected-side Jev below 68%.

A strict Jev-only filter improved the historical hit rate but reduced the
candidate count too much. V8.3 therefore uses a price-regime tier plus a T-110
fallback. On the available historical report, this selected 37 rows versus 34
for the former T-120-only live rule; all 37 selected rows happened to win. The
three chronological thirds were 13/13, 12/12, and 12/12. These thresholds were
chosen after observing the outcomes, so those figures are optimistic and are
not a forecast or guarantee.

## Live tiers

### T-120 strong tier

- existing Quant high-confidence gate;
- existing Jev answerability and clarity gates;
- selected-side Jev probability at least 68%;
- existing latest-ASK, spread, edge, depth, and USD 5 rules;
- existing full strategy sizing, still capped at USD 5.

### T-120 price-regime tier

This retains controlled volume when Jev agrees but is not strong enough for the
full tier. It requires all of:

- selected-side Jev at least 55%;
- selected-side Quant at least 92%;
- either ASK <= 0.52 or ASK >= 0.77;
- the existing minimum 8% edge, spread, depth, and price rules;
- minimum exchange shares only, never the full USD 5 budget.

The former live loss at Jev 60% and ASK 0.56 falls inside the rejected mid-price
zone.

### T-110 controlled fallback

T-110 is restored to improve opportunity count. It requires the existing Quant
and edge gates plus either:

- selected-side Jev at least 64%; or
- selected-side Jev at least 55% while selected-side ASK is at least 0.80.

Every T-110 order uses minimum exchange shares. T-100 remains shadow-only
because the existing sample was materially weaker.

## Transient-book recovery

For `incomplete_side_book` and `ask_outside_price_band`, V8.3 performs three
short public-book refreshes before rejecting the candidate. This aims to recover
briefly incomplete snapshots without relaxing any probability, spread, depth,
or price rule.

## Risk limits

- FAK BUY path and final refreshed ASK recheck are unchanged.
- Final refresh must still pass the same V8.3 tier; a preliminary candidate
  cannot bypass the final gate.
- Full tier: at most USD 5.
- Price-regime and T-110 tiers: minimum exchange shares.
- Five-order mode: at most five real submission attempts and USD 25 cumulative
  planned notional.
- Existing geoblock, balance, approval, position, open-order, user-stream, and
  uncertain-submit halt guards remain active.

## Commands

The command automatically selects the newest supported gate report in `runs/`
when `--paper-report` is omitted.

Read/auth/gate check only:

```powershell
uv run python -m jevymarket.price_value_live_v83 --check-only
```

One real V8.3 attempt:

```powershell
uv run python -m jevymarket.price_value_live_v83 `
  --live-one `
  --confirm ONE_REAL_V83_TIERED `
  --seconds 21600
```

Explicit maximum-five-attempt session:

```powershell
uv run python -m jevymarket.price_value_live_v83 `
  --live-five `
  --confirm FIVE_REAL_V83_TIERED `
  --seconds 28800
```

Outputs:

```text
runs/jevymarket.v83-tiered-live_*.db
runs/v83_tiered_live_*.json.gz
```

The `candidate_meta` and per-order `v83_meta` fields show preliminary tier,
final tier, selected-side Quant, and selected-side Jev so each result can be
audited separately.
