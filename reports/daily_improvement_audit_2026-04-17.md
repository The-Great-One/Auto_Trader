# Daily Improvement Audit, 2026-04-17

- Market open: **True**
- Issues found: **3**
- Improvement areas: **5**

## Issues
- [medium] **scorecard**: No orders or trades recorded for the trade date.
- [high] **equity_lab**: Equity strategy lab failed today: Traceback (most recent call last):
  File "/home/ubuntu/Auto_Trader/scripts/weekly_strategy_lab.py", line 686, in <module>
    main()
  File "/home/ubuntu/Auto_Trader/scripts/weekly_strategy_lab.py", line 631, in main
    data_map, data_context = load_data(tradebook_context, fundamental_context)
                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/Auto_Trader/scripts/weekly_strategy_lab.py", line 168, in load_data
    raise RuntimeError("Could not load any lab symbols with usable history")
RuntimeError: Could not load any lab symbols with usable history

- [high] **paper_shadow**: Daily ops paper check did not complete cleanly: failed_rc_1

## Improvement areas
- [high] **Improve entry sensitivity review**: No-trade day detected. Revisit BUY gate strictness and inspect near-miss candidates in equity and options paper outputs.
- [high] **Repair equity lab data loading**: Investigate missing/empty history for the requested lab basket and rebuild bad caches before the daily run.
- [medium] **Add explicit paper freshness checks**: Validate both paper_shadow_latest.json and paper_shadow_options_latest.json timestamps in daily ops, not just existence.
- [medium] **Explain options near-misses better**: Current top options candidate is HOLD with score 5.5. Add gate-by-gate miss diagnostics so near-buy setups are easier to tune.
- [medium] **Run deeper options sweeps**: Latest options lab tested 84 variants. Consider a deeper weekend sweep for better parameter separation.
