# Strategy bucket diagnostic, 20260417_230824

- Universe symbols: **589**
- Loaded symbols: **360**
- Cap buckets: **{'SMALL_CAP': 589}**
- Nifty 50 symbols in universe: **0**

## Buckets
### Nifty 50
- Requested/tested: **0 / 0**
- Backtest CAGR: **None%**
- Max drawdown: **None%**
- Trades: **None**
- Symbols with trades: **None%**
- Round trips per symbol-year: **None**

### Large cap
- Requested/tested: **0 / 0**
- Backtest CAGR: **None%**
- Max drawdown: **None%**
- Trades: **None**
- Symbols with trades: **None%**
- Round trips per symbol-year: **None**

### Mid cap
- Requested/tested: **0 / 0**
- Backtest CAGR: **None%**
- Max drawdown: **None%**
- Trades: **None**
- Symbols with trades: **None%**
- Round trips per symbol-year: **None**

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
