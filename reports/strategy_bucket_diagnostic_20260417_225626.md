# Strategy bucket diagnostic, 20260417_225626

- Universe symbols: **874**
- Loaded symbols: **601**
- Cap buckets: **{'SMALL_CAP': 589, 'MID_CAP': 211, 'LARGE_CAP': 66, 'ETF': 8}**
- Nifty 50 symbols in universe: **23**

## Buckets
### Nifty 50
- Requested/tested: **23 / 22**
- Backtest CAGR: **0.11%**
- Max drawdown: **-9.08%**
- Trades: **64**
- Symbols with trades: **77.27%**
- Round trips per symbol-year: **0.291**

### Large cap
- Requested/tested: **66 / 62**
- Backtest CAGR: **0.25%**
- Max drawdown: **-10.35%**
- Trades: **160**
- Symbols with trades: **64.52%**
- Round trips per symbol-year: **0.258**

### Mid cap
- Requested/tested: **211 / 174**
- Backtest CAGR: **0.25%**
- Max drawdown: **-25.45%**
- Trades: **572**
- Symbols with trades: **72.99%**
- Round trips per symbol-year: **0.329**

### Small cap
- Requested/tested: **589 / 360**
- Backtest CAGR: **-0.01%**
- Max drawdown: **-23.87%**
- Trades: **994**
- Symbols with trades: **67.78%**
- Round trips per symbol-year: **0.276**

## Diagnosis
- Even the cleaner large-cap / Nifty buckets are below the 20% CAGR bar, so universe cleanup alone will not fix it.
- Trade density is extremely low, which suggests the current buy logic is too restrictive for the chosen bars and universe.

## Recommendations
- Retune entry/exit conditions on the best bucket instead of only changing the universe.
- Loosen buy gates and/or hold winners longer, then rerun walk-forward and Monte Carlo on the best bucket.
