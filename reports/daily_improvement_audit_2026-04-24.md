# Daily Improvement Audit, 2026-04-24

- Market open: **True**
- Issues found: **0**
- Improvement areas: **1**

## Issues
- None

## Improvement areas
- [medium] **Explain options near-misses better**: Current top options candidate is HOLD with score 2.5. Add gate-by-gate miss diagnostics so near-buy setups are easier to tune.

## Daily iteration plans

### Equity
- None

### Options
- [medium] **broaden_options_search**: Daily options lab did not improve on baseline. Broaden the search across underlying trend filters, score thresholds, and exit logic.
- [medium] **deeper_options_weekend_sweep**: Latest options lab tested 0 variants. Keep daily runs lean, but use deeper weekend sweeps for better separation.
- [medium] **add_options_near_miss_diagnostics**: Top options paper candidate is still HOLD with score 2.5. Add gate-by-gate miss diagnostics so daily iteration can tune the last blockers.
