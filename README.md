# 🚀 AutoTrader Bot

⚠️ **Disclaimer**: This is not a college project. This is a fully productionized (almost) trading bot that can (hopefully) make you lots of money! 🚀

Welcome to **AutoTrader Bot**, a Python-based automated stock trading bot designed for real-time trading with customizable buy/sell strategies. This bot is optimized for Indian stocks and integrates with Zerodha Kite for seamless execution. It's built to be efficient, customizable, and easy to deploy.

## ✨ Features

- 🔄 **Custom Trading Strategies**: Leverage MACD, RSI, EMA, and Volume-based strategies to make buy/sell decisions.
- 📈 **Real-Time Data**: Fetches live price and volume data using Zerodha’s KiteTicker.
- 🤖 **Automated Orders**: Executes buy, sell, and hold signals based on predefined conditions.
- 🚀 **Performance Optimization**: Efficient order placement adhering to rate limits, with caching and multiprocessing capabilities.
- 💰 **Funds Management**: Allocates ₹20,000 per stock for trading or a custom amount set by you.

## 📚 Table of Contents

1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [Strategies](#strategies)
5. [Performance Optimization](#performance-optimization)
6. [Deployment](#deployment)
7. [Running the Bot as a Service with Systemd](#running-the-bot-as-a-service-with-systemd)
8. [Using Systemd Timers to Refresh Session Token](#using-systemd-timers-to-refresh-session-token)
9. [Future Enhancements](#future-enhancements)
10. [Contributing](#contributing)
11. [License](#license)

## 🛠️ Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/The-Great-One/Auto_Trader.git
    cd autotrader-bot
    ```

2. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Create a file called `my_secrets.py` in the `Auto_Trader` directory with your API credentials:
    ```python
    API_KEY = 'your_api_key'
    API_SECRET = 'your_api_secret'
    TOTP_KEY = 'your_totp_key'
    USER_NAME = 'your_username'
    PASS = 'your_password'
    TG_TOKEN = 'your_telegram_token'
    CHANNEL = '@your_channel'
    GITHUB_PAT = 'your_github_personal_access_token'
    ```

### ⚠️ Important Note:
   - You can get your **TOTP_KEY** by scanning the QR code shown during the 2FA setup on the [Zerodha](https://kite.zerodha.com) website.
   
   Steps:
   1. Log in to [Zerodha](https://kite.zerodha.com).
   2. Set up 2-factor authentication.
   3. Use an app like Google Authenticator or Authy to scan the QR code.
   4. Retrieve the TOTP key from the authenticator app and add it to `my_secrets.py`.

## ⚙️ Configuration

- **Rate Limits**: The bot complies with Zerodha's API rate limits (10 requests/second, 200 orders/minute).
- **Funds Management**: By default, the bot allocates ₹20,000 per stock from a total fund pool for trading.
- **Order Handling**: Sell orders are prioritized to free up funds for new buy orders.

### Example of a Rule:

Create a file called `RULE_SET_*.py` and define a function called `buy_or_sell(df, row, holdings)`.

- `df`: This is the entire DataFrame with calculated indicators.
- `row`: This contains the latest raw values from the ticker, providing real-time data.
- `holdings`: A list of symbols currently held by the bot.

If you want to add more indicators, you can modify them here or in the `utils.py` file, inside the indicators function.

#### Sample Implementation:
```python
def buy_or_sell(df, row, holdings):
    # Example logic based on RSI and MACD
    if row['rsi'] > 60 and row['macd_hist'] > 5 and row['symbol'] not in holdings:
        # Place Buy Order
        return 'BUY'
    elif row['rsi'] < 40 and row['symbol'] in holdings:
        # Place Sell Order
        return 'SELL'
    else:
        return 'HOLD'
```

## 🚀 Usage

To start the bot, run:

```bash
python wednesday.py
```

This will launch the bot, connect it to the Zerodha Kite API, fetch live stock data, and execute trades based on the defined strategy.

## 📊 Strategies

The following indicators are used in default strategies:

- **MACD**: Signal line and histogram-based decisions.
- **RSI**: Buy when RSI crosses above 60, sell below 40.
- **EMA**: Crossover-based strategies using EMA 10 and EMA 20.
- **Volume**: Volume spikes for trend confirmation.

You can modify these strategies by editing their respective `RULE_SET` files.

## ⚡ Performance Optimization

- **Caching**: Data is fetched once per day and stored in memory for fast access.
- **Multiprocessing**: Tasks are parallelized to avoid WebSocket blocking and ensure smooth data processing.
- **Non-blocking WebSocket**: The bot uses asynchronous I/O to keep WebSocket connections stable while processing data.

## ☁️ Deployment

You can deploy this bot on **Vultr** or any cloud platform to keep it running continuously. 

## 🛠️ Running the Bot as a Service with Systemd

To make the bot run continuously in the background as a service using `systemd`, follow these steps:

1. Create a new service file:

    ```bash
    sudo nano /etc/systemd/system/autotrader.service
    ```

2. Add the following content to the service file:

    ```ini
    [Unit]
    Description=AutoTrader Bot Service
    After=network.target

    [Service]
    User=your_username
    WorkingDirectory=/path/to/AutoTrader
    ExecStart=/usr/bin/python3 /path/to/AutoTrader/wednesday.py
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    ```

3. Reload `systemd` to recognize the new service:

    ```bash
    sudo systemctl daemon-reload
    ```

4. Start the service:

    ```bash
    sudo systemctl start autotrader.service
    ```

5. Enable the service to start on boot:

    ```bash
    sudo systemctl enable autotrader.service
    ```

6. Check the status of the service:

    ```bash
    sudo systemctl status autotrader.service
    ```

Now, the bot will automatically run as a service in the background and restart on failure.

## ⏰ Using Systemd Timers to Refresh Session Token

I wasn't able to refresh the session token in Python, so I'm using `systemd` to handle it. The bot will restart daily at 8:30 AM to refresh the session token.

1. Create a timer file:

    ```bash
    sudo nano /etc/systemd/system/autotrader.timer
    ```

2. Add the following content to the timer file:

    ```ini
    [Unit]
    Description=Run AutoTrader Bot at 8:30 AM daily to refresh the session token

    [Timer]
    OnCalendar=*-*-* 08:30:00
    Persistent=true

    [Install]
    WantedBy=timers.target
    ```

3. Enable and start the timer:

    ```bash
    sudo systemctl enable autotrader.timer
    sudo systemctl start autotrader.timer
    ```

4. Check the status of the timer to ensure it’s working:

    ```bash
    sudo systemctl status autotrader.timer
    ```

The bot will now refresh the session token daily at 8:30 AM.

## 🧠 Portfolio Intelligence (Equity + ETF + MF + News-aware Rebalance)

New scripts:

- `scripts/daily_portfolio_report.py`
  - Pulls Kite holdings + `mf_holdings()`
  - Builds allocation snapshot (Equity / ETF / MF)
  - Computes news risk score from Reuters RSS headlines
  - Generates target allocation drift and INR rebalance advice
  - Writes:
    - `reports/portfolio_intel_YYYY-MM-DD.json`
    - `reports/portfolio_intel_YYYY-MM-DD.md`

- `Auto_Trader/mf_execution.py`
  - Guarded mutual-fund execution helper
  - Validates MF symbols against Kite MF instruments
  - Enforces dry-run by default
  - Adds per-order and per-run amount caps
  - Can optionally require allowlisted MF symbols
  - Requires `AT_MF_ENABLE_LIVE=1` for live order placement

- `scripts/mf_order_manager.py`
  - Safe CLI for MF search, holdings, orders, SIPs, and guarded execution
  - Supports:
    - searching Kite MF instruments
    - viewing MF holdings / MF orders / MF SIPs
    - executing a JSON order plan (dry-run by default)
    - generating a MF rebalance plan from `portfolio_intel`
    - executing a JSON SIP plan (dry-run by default)
    - creating, modifying, and cancelling MF SIPs with live-guard rails

- `scripts/send_discord_health_alert.py`
  - Reads latest scorecard + portfolio intel report
  - Sends daily health card to Discord via webhook (`DISCORD_WEBHOOK_URL`)

- `scripts/weekly_strategy_supervisor.py`
  - Backtests multiple strategies (`RULE_SET_2`, `RULE_SET_7`) on recent NIFTYETF history
  - If current strategy is not profitable and an alternate is better, rotates rule-set and restarts `auto_trade.service`
  - Writes `reports/weekly_strategy_supervisor.json`

### MF execution examples

```bash
# Search mutual funds
python scripts/mf_order_manager.py search "parag parikh"

# Generate MF rebalance plan from latest portfolio report
python scripts/mf_order_manager.py rebalance-plan \
  --buy-symbol "INF879O01027" \
  --buy-symbol "INF179KC1DA9"

# Create a monthly SIP, dry-run by default
python scripts/mf_order_manager.py sip-create INF879O01027 \
  --amount 5000 --instalments 24 --frequency monthly --instalment-day 5

# Live execution requires BOTH --execute and env flag
AT_MF_ENABLE_LIVE=1 python scripts/mf_order_manager.py plan mf_plan.json --execute
AT_MF_ENABLE_LIVE=1 python scripts/mf_order_manager.py sip-create INF879O01027 \
  --amount 5000 --instalments 24 --frequency monthly --instalment-day 5 --execute
```

Optional guardrails:
- `AT_MF_MAX_ORDER_AMOUNT`
- `AT_MF_MAX_TOTAL_ORDER_AMOUNT`
- `AT_MF_MIN_ORDER_AMOUNT`
- `AT_MF_ENABLE_LIVE`
- `AT_MF_REQUIRE_ALLOWLIST`
- `AT_MF_ALLOWED_SYMBOLS`
- `AT_MF_ALLOWLIST_PATH`

Suggested cron (example):

```bash
# 16:20 IST generate intelligence report
20 16 * * 1-5 /home/ubuntu/Auto_Trader/venv/bin/python /home/ubuntu/Auto_Trader/scripts/daily_portfolio_report.py >> /home/ubuntu/Auto_Trader/reports/portfolio_intel_cron.log 2>&1

# 16:25 IST send Discord health alert
25 16 * * 1-5 DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/...' /home/ubuntu/Auto_Trader/venv/bin/python /home/ubuntu/Auto_Trader/scripts/send_discord_health_alert.py >> /home/ubuntu/Auto_Trader/reports/discord_alert_cron.log 2>&1
```

## 🛠️ Future Enhancements

- **Analytics Dashboard**: A real-time performance monitoring dashboard with profit/loss trends.

## 🤝 Contributing

We welcome contributions! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests.

## 📜 License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

---

**Happy Trading!** 🚀📈
