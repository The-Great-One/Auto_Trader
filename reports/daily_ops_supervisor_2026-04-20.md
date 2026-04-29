# Daily Ops Supervisor — 2026-04-20

- Market open: **True** (NSE calendar)
- Strategies tested: **50**
- Strategy report stale: **False**
- Baseline return %: **-0.88**
- Best return %: **0.7**
- Improvement return %: **1.58**
- Promote candidate: **True**
- Auto-promote applied: **False**
- Auto-promote reason: **market_open**

## Daily equity iteration plan
- [medium] **exploit_best_equity_cluster**: Daily equity lab found a better candidate (buy_max_extension_atr_2.8). Center the next sweep around that cluster and test adjacent values before wider searches.
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
