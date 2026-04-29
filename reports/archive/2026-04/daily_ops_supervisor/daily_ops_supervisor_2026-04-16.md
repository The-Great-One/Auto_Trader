# Daily Ops Supervisor — 2026-04-16

- Market open: **True** (NSE calendar)
- Strategies tested: **138**
- Baseline return %: **0.4**
- Best return %: **1.92**
- Improvement return %: **1.52**
- Promote candidate: **True**
- Auto-promote applied: **False**
- Auto-promote reason: **market_open**

## Paper trader check
- Executed today: **False**
- Self-healed: **False**
- Decision: **HOLD**
- Reason: **failed_rc_1**

### Error
```
Traceback (most recent call last):
  File "/home/ubuntu/Auto_Trader/scripts/paper_shadow.py", line 181, in <module>
    main()
  File "/home/ubuntu/Auto_Trader/scripts/paper_shadow.py", line 175, in main
    equity_payload = run_equity_shadow()
                     ^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/Auto_Trader/scripts/paper_shadow.py", line 73, in run_equity_shadow
    df = load_hist(symbol)
         ^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/Auto_Trader/scripts/paper_shadow.py", line 37, in load_hist
    out = at_utils.Indicators(df)
          ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/ubuntu/Auto_Trader/Auto_Trader/utils.py", line 373, in Indicators
    compute_supertrend(df, ATR, multiplier=2.0)
  File "/home/ubuntu/Auto_Trader/Auto_Trader/utils.py", line 166, in compute_supertrend
    up_shift[0] = dn_shift[0] = np.nan
    ~~~~~~~~^^^
IndexError: index 0 is out of bounds for axis 0 with size 0

```
