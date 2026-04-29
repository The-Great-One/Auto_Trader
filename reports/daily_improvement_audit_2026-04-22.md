# Daily Improvement Audit, 2026-04-22

- Market open: **True**
- Issues found: **1**
- Improvement areas: **3**

## Issues
- [medium] **scorecard**: No orders or trades recorded for the trade date.

## Improvement areas
- [high] **Improve entry sensitivity review**: No-trade day detected. Revisit BUY gate strictness and inspect near-miss candidates in equity and options paper outputs.
- [medium] **Explain options near-misses better**: Current top options candidate is HOLD with score 6.0. Add gate-by-gate miss diagnostics so near-buy setups are easier to tune.
- [medium] **Run deeper options sweeps**: Latest options lab tested 87 variants. Consider a deeper weekend sweep for better parameter separation.

## Daily iteration plans

### Equity
- [high] **expand_buy_sensitivity**: Daily equity lab did not beat baseline. Next sweep should loosen entry gates, especially ADX and trend-confirmation thresholds, instead of adding more universe breadth.
- [medium] **add_equity_near_miss_diagnostics**: Paper equity decision is HOLD. Add near-miss diagnostics so the daily loop can see which gates are preventing trades and tune them deliberately.

### Options
- [medium] **broaden_options_search**: Daily options lab did not improve on baseline. Broaden the search across underlying trend filters, score thresholds, and exit logic.
- [medium] **deeper_options_weekend_sweep**: Latest options lab tested 0 variants. Keep daily runs lean, but use deeper weekend sweeps for better separation.
- [medium] **add_options_near_miss_diagnostics**: Top options paper candidate is still HOLD with score 6.0. Add gate-by-gate miss diagnostics so daily iteration can tune the last blockers.
