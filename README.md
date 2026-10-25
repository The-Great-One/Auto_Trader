<div align="center">

<img src="icon.png" alt="Auto_Trader logo" width="120" height="120" />

# Auto_Trader

**Live paper-only RSI momentum rotation pipeline for NSE equities.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/The-Great-One/Auto_Trader)](https://github.com/The-Great-One/Auto_Trader/issues)
[![GitHub pull requests](https://img.shields.io/github/issues-pr/The-Great-One/Auto_Trader)](https://github.com/The-Great-One/Auto_Trader/pulls)
[![GitHub stars](https://img.shields.io/github/stars/The-Great-One/Auto_Trader?style=social)](https://github.com/The-Great-One/Auto_Trader/stargazers)

[Report Bug](https://github.com/The-Great-One/Auto_Trader/issues/new?template=bug_report.md) · [Request Feature](https://github.com/The-Great-One/Auto_Trader/issues/new?template=feature_request.md)

</div>

---

## Table of Contents

- [About](#about)
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)
- [Acknowledgments](#acknowledgments)

---

## About

`Auto_Trader` is the live **paper-only RSI momentum rotation pipeline** for NSE
equities. It consumes yfinance-fed historical and intraday prices, generates
champion-strategy signals, maintains a simulated ledger, and reports status
through Hermes.

No real orders are placed. Zerodha Kite is retired and is not a supported price,
login, or execution path; the Kite engine service is masked on the server.

Nightly strategy research lives in the separate `Trader_Labs` repository.

## Features

- ✅ Paper-only RSI momentum rotation (no live orders)
- ✅ yfinance-fed prices (Tickertape REST fallback)
- ✅ Fail-closed signal publication
- ✅ Whole-share target deltas, idempotent signal IDs
- ✅ Atomic state commit before rebuildable output
- ✅ Telegram status delivery via Hermes cron
- 🚧 Live order execution (not supported)

## Getting Started

### Prerequisites

- Python 3.10+
- Access to NSE data (yfinance)

### Installation

```bash
git clone https://github.com/The-Great-One/Auto_Trader.git
cd Auto_Trader
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Live data and execution flow

1. A Mac-side Hermes cron fetches NSE quotes with yfinance (Tickertape REST as
   fallback) and writes `reports/live_prices.json` on the server.
2. `scripts/rsi_momentum_paper_shadow.py` refreshes the paper signal at 10:15 on
   weekdays. It fails closed on stale/thin/malformed data.
3. `scripts/rsi_momentum_paper_ledger.py` marks positions to market every five
   minutes from 09:00–15:59, rebalancing when it sees a newer signal.
4. A Hermes cron reads the latest ledger output and delivers Telegram status.
5. Historical feathers are rebuilt on the Mac with yfinance and synced to the
   server.

### Correct paper execution contract

The shadow publishes a versioned D-close intent with target weights (including
inverse-volatility weights), target cash, a stable signal ID, and nullable
modeled D+1 opens. The ledger fills whole-share target deltas only from one
complete, timezone-aware, strictly fresh `paper_quote_snapshot_v1`. Fees use
actual traded notional, residual cash and target-weight deviations are retained,
and consumed signal IDs are idempotent.

### Repository contents

- `scripts/rsi_momentum_paper_shadow.py` — champion signal generator.
- `scripts/rsi_momentum_paper_ledger.py` — simulated portfolio, MTM, rebalance.
- `scripts/rsi_momentum_report.py`, `scripts/rsi_224466_rotation_lab.py` — signal/backtest support.
- `scripts/nightly_cleanup.py`, `scripts/prune_report_clutter.py` — cleanup.
- `tests/` — safety tests for shadow and ledger.

Generated reports, logs, market data, databases, and secrets are ignored by Git.

### Running tests

```bash
pytest -q
```

Do not manually run the ledger or shadow against production state without an
explicit operational reason and the shared ledger lock.

### Deployment

```bash
cd /home/ubuntu/Auto_Trader
git pull --ff-only origin main
```

See `PROJECT_MAP.md` for schedules and operational ownership.

## Roadmap

- [x] v0.1 – Paper shadow + ledger
- [ ] v0.2 – Strategy promotion workflow
- [ ] v1.0 – Validated champion promotion

See the open issues for the full list of proposed features and known bugs.

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and
follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.

## Contact

Sahil Goel – [@The-Great-One](https://github.com/The-Great-One) –
sahilgoel187@gmail.com

Project link: [https://github.com/The-Great-One/Auto_Trader](https://github.com/The-Great-One/Auto_Trader)

## Acknowledgments

- [Img Shields](https://shields.io)
- [Contributor Covenant](https://www.contributor-covenant.org)
