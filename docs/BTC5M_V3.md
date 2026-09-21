# v3: BTC 5m, 10-second paper experiment

The default `jevymarket` entry point now runs **only BTC 5m**. Existing 15m/1h
code and v2 data remain available under `jevymarket legacy`; they are not scanned,
queried on Binance, or settled by the new runner. No .env or API keys are changed.
The new runner requires `--dry-run` and never constructs an authenticated client.

```powershell
uv run jevymarket run --dry-run --loop 10
# Ctrl+C, then:
uv run jevymarket stats
```

Default experiment: `v3-btc5m-10s-quant-shadow`. Settings from an old `.env` cannot
re-enable 15m/1h or label new observations as v2. A stored parameter manifest
prevents reusing an experiment name with a different cadence, threshold, sizing,
or Jev mode. Use `--experiment <new-name>` for a deliberate parameter change, and
pass the same name to `stats`. A per-database OS lock prevents two fast runners.

## What changes, and what does not

* Raw/TWAP30/TWAP60 continue on persistent, independent RTDS listeners. Both TWAP
  resolutions are retained because market rules select 30s or 60s. The strategy
  reads their latest timestamped SQLite samples; no new TWAP connection per tick.
* Polling is fixed-rate, **start to start**, default 10 seconds. Metadata is cached
  for up to 30 seconds within the same market; the order book is refreshed each
  tick. A read/compute tick has an 8-second network budget at the default cadence.
  A slow tick skips missed slots; no overlap, catch-up bursts, or stale order replay.
  OS scheduling, console output and network failures still mean this is not a
  hard real-time guarantee. Actual inter-tick gaps are recorded and reported.
* The primary is still Phi(Z), not an average with Jev. Existing edge/price-band
  and Kelly parameters are retained. Crossing/wide/invalid books are rejected.
  Target, TWAP and raw history must be present and fresh before a signal is used.
* Jev is **shadow-only**: it is asked at the first valid observation in each
  checkpoint window, never awaited by the main loop and never substitutes a past
  probability into a current signal. One worker and one pending job limit API
  concurrency. Calls have a maximum 20-second total budget and expire before
  market end. Failed/busy/disabled/interrupted calls remain explicitly missing.
* Checkpoints are T-240/180/120/60/30, recorded once per experiment/market at or
  after that target, less than 10 seconds late. Actual seconds remaining are saved.
  Missing checkpoints are not invented. More observations do not create more
  independent market outcomes.
* Only the first paper order per market/experiment is recorded; later ticks and
  checkpoints continue. Open paper notional is computed from persisted unsettled
  orders, so a restart does not reset it to zero. Official results alone release it.
* Settlement has its own client/task; only current-experiment 5m slugs are read.
  It requires closed markets and final 1/0 prices (1e-9 numerical tolerance), not
  just a 0.99/0.01 market. No trading/approval request is automatically retried.

## Reading stats

v3 uses `fast_experiments`, `fast_observations`, `fast_orders`, plus existing
price/anchor/official-result tables. It does not delete or relabel v2 records.

`stats` reports both all valid quant/market checkpoints and the smaller common
cohort on which Jev returned successfully. A missing Jev result is NOT zero.
It also reports actual gaps, independent market counts, checkpoint coverage,
realized calibration, and:

1. Actual paper Kelly orders.
2. The identical orders at identical entry quotes, normalized to $1 per order.
3. First qualifying signal from all 10-second observations, fixed $1.
4. First qualifying signal from fixed checkpoints, fixed $1.

Jev A/B/C figures are **request-time counterfactual diagnostics**, not executable
backtests. They use the same matched observations but do not simulate how the
book moved while Jev responded. Do not compare them to actual execution PnL as
though latency and entry opportunities were equal. Old B gates only answerability
and clarity; it does not use Jev's directional probability. Rejection counts are
printed to make zero-impact gates visible rather than tightening them arbitrarily.

All returns are **gross**: fees, slippage, queue position and actual fills are not
modelled. Fixed-$1 diagnostics also ignore minimum shares and exposure caps. A
short profitable sample is not a claim of alpha or permission to enable live mode.

Old v2 statistics (using the old `.env` strategy version):

```powershell
uv run jevymarket legacy stats
```

Do not run `reset-experiment` to start v3. Restarting during a market can leave its
anchor missing; skip it until a new valid boundary is captured. A log line showing
"raw路径预热/缺口/过期" does not imply that all 330 seconds were missing: a recent
coverage gap can also fail the quality gate.
