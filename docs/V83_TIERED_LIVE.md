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

This is retrospective evidence from a small sample, not proof of future
performance. V8.3 therefore separates high-confidence orders from controlled
minimum-share fallbacks and records the tier on every candidate.

## Live tiers

### T-120 strong tier

- existing Quant high-confidence gate;
- existing Jev answerability and clarity gates;
- selected-side Jev probability at least 68%;
- existing latest-ASK, spread, edge, depth, and USD 5 rules;
- existing full strategy sizing, still capped at USD 5.

### T-120 market-extreme tier

This retains some volume when Jev agrees but is not strong enough for the full
tier. It requires all of:

- selected-side Jev at least 60%;
- selected-side Quant at least 94%;
- either ASK <= 0.50 with edge >= 0.20, or ASK >= 0.80 with edge >= 0.10;
- existing spread/depth/price rules;
- minimum exchange shares only, never the full USD 5 budget.

### T-110 strict fallback

T-110 is restored as a controlled fallback to improve opportunity count:

- selected-side Jev at least 64%;
- selected-side Quant at least 92%;
- edge at least 10%;
- existing spread/depth/price rules;
- minimum exchange shares only.

T-100 remains shadow-only because the existing sample was materially weaker.

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
- Fallback tiers: minimum exchange shares.
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
