# Daily Ops Supervisor — 2026-04-22

- Market open: **True** (NSE calendar)
- Strategies tested: **50**
- Strategy report stale: **False**
- Baseline return %: **0.0**
- Best return %: **0.0**
- Improvement return %: **0.0**
- Promote candidate: **False**
- Auto-promote applied: **False**
- Auto-promote reason: **market_open**

## Daily equity iteration plan
- [high] **expand_buy_sensitivity**: Daily equity lab did not beat baseline. Next sweep should loosen entry gates, especially ADX and trend-confirmation thresholds, instead of adding more universe breadth.
- [medium] **add_equity_near_miss_diagnostics**: Paper equity decision is HOLD. Add near-miss diagnostics so the daily loop can see which gates are preventing trades and tune them deliberately.

## Weekly universe CAGR check
- Status: **market_open**
- Requested symbols: **0**
- Tested symbols: **0**
- Coverage %: **None**
- CAGR %: **None**
- Total return %: **None**

## Paper trader check
- Executed today: **True**
- Self-healed: **False**
- Decision: **HOLD**
- Reason: **already_executed**
