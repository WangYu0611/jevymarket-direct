# V8.2 — T-120 Primary, T-100/C Shadow Research

V8.2 is the post-V8.1 mainline.

The strategy decision has been narrowed to one primary rule:

```text
T-120
  + Quant high confidence
  + Jev quality pass
  + Jev direction agrees with Quant
  + refresh Quant after Jev
  + use the latest predicted-side best ASK
  + existing V8 value/spread/depth rules
```

Only this rule can pass the V8.2 profitability gate or reach a future live
placement hook.

## Primary strategy

Primary arm:

```text
T-120 · B_quant_jev_taker
```

The fixed value rules are unchanged:

- selected-side Quant confidence >= 92%;
- model minus latest ASK edge >= 8%;
- ASK in 0.10..0.90;
- spread <= 0.06;
- simulated order <= USD 5;
- predicted-side 5-cent ask depth must cover the simulated notional;
- same taker-fee model as V8/V8.1.

The PASS gate is unchanged:

- >=50 settled primary trades;
- win rate strictly >65%;
- estimated net PnL >0;
- net PnL remains >0 after removing the three largest positive contributions;
- net PnL remains >0 after worsening every entry by one tick.

## Shadow research retained

The following data continue to be collected but **cannot** affect the primary
PASS result:

```text
T-120 A  Quant-only control
T-120 C  Jev-own-probability edge research

T-100 A  Quant-only
T-100 B  Quant + Jev + latest ASK
T-100 C  Jev-own-probability edge research
```

T-110 is retired from V8.2. It no longer creates a checkpoint or Jev request.

This keeps the two research questions the user wants to preserve:

1. Does T-100 continue to underperform or recover in future data?
2. Does the C/Jev-own-edge idea remain weak or improve with a larger sample?

Neither answer can change the primary strategy automatically.

## Independent slot storage

V8.1 stored at most one trade per arm per market. That was correct for the old
"first qualifying checkpoint" design, but it would suppress T-100 research when
the same arm had already qualified at T-120.

V8.2 therefore stores research rows using:

```text
version + arm + market + slot
```

as the uniqueness key.

A single market can therefore retain T-120 B and T-100 B independently, as
well as C rows for both slots.

## Terminal layout

The first table is the only primary score:

```text
V8.2 主策略 · T-120 Quant + Jev + 最新ASK
```

It shows settled trades, win rate, PnL, ROI, PnL without the top three winners,
+1 tick stress, distance to 50 and PASS state.

A second table is explicitly labeled:

```text
V8.2 影子研究 · 不影响主策略PASS
```

and shows T-100 plus C research separately.

## Fresh forward sample

V8.2 is a new protocol chosen after observing V8.1, so use a fresh V8.2 DB.
Do not resume a V8.1 database into V8.2.

Recommended run:

```powershell
Set-Location -LiteralPath "C:\Users\wy331\Documents\jevymarket-direct"
git pull --ff-only

uv run --frozen python -m jevymarket.price_value_t120_main --seconds 86400
```

Outputs:

```text
runs/jevymarket.v82-t120-main-shadow_*.db
runs/v82_t120_main_shadow_*.json.gz
```

If interrupted, resume only the V8.2 DB:

```powershell
uv run --frozen python -m jevymarket.price_value_t120_main \
    --resume-db "runs\jevymarket.v82-t120-main-shadow_<timestamp>.db" \
    --seconds 86400
```

The existing geographic restrictions in the live module remain fail-closed and
are separate from this paper/shadow experiment.
