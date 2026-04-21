# Daily Improvement Audit, 2026-04-18

- Market open: **False**
- Issues found: **2**
- Improvement areas: **1**

## Issues
- [high] **options_paper**: Options paper shadow failed today: market_closed
- [high] **options_lab**: Scheduled options lab failed today: market_closed

## Improvement areas
- [medium] **Run deeper options sweeps**: Latest options lab tested 84 variants. Consider a deeper weekend sweep for better parameter separation.

## Daily iteration plans

### Equity
- None

### Options
- [high] **repair_options_inputs**: Options fetch failed, so daily options iteration is blocked. Restore contract data freshness before tuning strategy parameters.
