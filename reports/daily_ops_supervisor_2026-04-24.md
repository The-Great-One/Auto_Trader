# Daily Ops Supervisor — 2026-04-24

- Market open: **True** (NSE calendar)
- Strategies tested: **0**
- Strategy report stale: **True**
- Baseline return %: **None**
- Best return %: **None**
- Improvement return %: **None**
- Promote candidate: **False**
- Auto-promote applied: **False**
- Auto-promote reason: **market_open**

## Daily equity iteration plan
- [high] **repair_equity_iteration_inputs**: Equity lab did not complete cleanly. Repair history loading / freshness first so daily iteration is based on fresh data rather than stale reports.
- [medium] **add_equity_near_miss_diagnostics**: Paper equity decision is HOLD. Add near-miss diagnostics so the daily loop can see which gates are preventing trades and tune them deliberately.

## Weekly universe CAGR check
- Status: **market_open**
- Requested symbols: **0**
- Tested symbols: **0**
- Coverage %: **None**
- CAGR %: **None**
- Total return %: **None**

## Paper trader check
- Executed today: **False**
- Self-healed: **False**
- Decision: **HOLD**
- Reason: **failed_rc_0**

### Error
```
         "breakout_ok": false
        },
        "metric_snapshot": {
          "close": 166.7,
          "prev_close": 308.85,
          "ema5": 220.908,
          "ema10": 306.4289,
          "rsi": 25.8152,
          "macd_hist": null,
          "atr_pct": 0.7127,
          "volume": 1177605.0,
          "volume_sma20": 130172.25,
          "oi": 218790.0,
          "oi_sma5": 89934.0,
          "oi_pct_change": 42.5667,
          "underlying_close": 23897.9492,
          "underlying_rsi": 49.1905,
          "underlying_adx": 19.9707,
          "underlying_macd_hist": 110.2468
        },
        "threshold_snapshot": {
          "buy_score_min": 6.0,
          "option_rsi_min": 56.0,
          "volume_confirm_mult": 1.1,
          "oi_sma_mult": 1.02,
          "oi_change_min_pct": 1.0,
          "atr_pct_min": 0.03,
          "atr_pct_max": 1.5,
          "underlying_rsi_bull_min": 55.0,
          "underlying_rsi_bear_max": 45.0,
          "underlying_adx_min": 18.0
        },
        "last_close": 166.7,
        "volume": 1177605.0,
        "oi": 218790.0,
        "underlying_close": 23897.94921875,
        "expiry": "2026-05-05",
        "strike": 23950.0
      }
    ]
  }
}

```
