# Strategy bucket diagnostic, 20260417_231720

- Universe symbols: **23**
- Loaded symbols: **22**
- Cap buckets: **{'LARGE_CAP': 23}**
- Nifty 50 symbols in universe: **23**

## Buckets
### Nifty 50
- Requested/tested: **23 / 22**
- Backtest CAGR: **-0.05%**
- Max drawdown: **-19.12%**
- Trades: **90**
- Symbols with trades: **86.36%**
- Round trips per symbol-year: **0.41**

### Large cap
- Requested/tested: **23 / 22**
- Backtest CAGR: **-0.05%**
- Max drawdown: **-19.12%**
- Trades: **90**
- Symbols with trades: **86.36%**
- Round trips per symbol-year: **0.41**

### Mid cap
- Requested/tested: **0 / 0**
- Backtest CAGR: **None%**
- Max drawdown: **None%**
- Trades: **None**
- Symbols with trades: **None%**
- Round trips per symbol-year: **None**

### Small cap
- Requested/tested: **0 / 0**
- Backtest CAGR: **None%**
- Max drawdown: **None%**
- Trades: **None**
- Symbols with trades: **None%**
- Round trips per symbol-year: **None**

## Diagnosis
- Even the cleaner large-cap / Nifty buckets are below the 20% CAGR bar, so universe cleanup alone will not fix it.
- Trade density is extremely low, which suggests the current buy logic is too restrictive for the chosen bars and universe.

## Recommendations
- Retune entry/exit conditions on the best bucket instead of only changing the universe.
- Loosen buy gates and/or hold winners longer, then rerun walk-forward and Monte Carlo on the best bucket.
