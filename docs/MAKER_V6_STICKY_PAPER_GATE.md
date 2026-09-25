# Sticky 45→30 paper validation

The previous 45→30 paper run created seven simulated maker orders and estimated zero fills.
Those orders rested only about 0.6–2.1 seconds while the public L2 queue estimate ahead
was roughly 1,500–2,800 shares. The next paper experiment therefore tests queue-priority
retention rather than widening the entry window again.

Changes relative to the preceding 45→30 paper runner:

- Same 45→30 second entry/cancel window.
- Same 0.92 confidence threshold, model, price/spread rules, 1s book freshness, size/risk caps and BBO recovery.
- Quote TTL increases from 2s to 5s.
- A safe resting order does not cancel just because the newly desired maker price moves.
- New quotes may improve the current best bid by one tick when the spread, post-only constraint and minimum edge all remain valid. This targets queue position rather than relaxing confidence.
- It still cancels on stale/invalid book data, book generation changes, direction change,
  edge loss, crossing risk, TTL, disconnect, or the T-30 cutoff.
- Paper fills remain estimates from public prints and L2 queue-ahead. Same-price cancellations
  ahead of our hypothetical order are not observable, so this simulator may undercount fills.

For the first validation, use a 2-hour run and all outputs go to `runs/`:

```powershell
uv run --frozen python -m jevymarket.maker_paper_sticky_45to30 --seconds 7200
```

## Project gate before any future real-money discussion

A paper report passes only if all of these are true:

- at least 50 settled filled markets;
- observed paper win rate is strictly greater than 65%;
- gross paper PnL is positive;
- PnL remains positive after removing the three largest positive contributions;
- zero uncertain orders;
- zero pending filled orders;
- wins + losses reconcile to settled filled markets.

This gate reduces small-sample and concentration risk. Passing it does not guarantee live profitability.
