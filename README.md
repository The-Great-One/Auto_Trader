# 🚀 AutoTrader Bot

⚠️ **Disclaimer**: This is not a college project. This is a fully productionized (almost) trading bot that can (hopefully) make you lots of money! 🚀

Welcome to **AutoTrader Bot**, a Python-based automated stock trading bot designed for real-time trading with customizable buy/sell strategies. This bot is optimized for Indian stocks and integrates with Zerodha Kite for seamless execution. It's built to be efficient, customizable, and easy to deploy.

## ✨ Features

- 🔄 **Custom Trading Strategies**: Leverage MACD, RSI, EMA, and Volume-based strategies to make buy/sell decisions.
- 📈 **Real-Time Data**: Fetches live price and volume data using Zerodha’s KiteTicker.
- 🤖 **Automated Orders**: Executes buy, sell, and hold signals based on predefined conditions.
- 🚀 **Performance Optimization**: Efficient order placement adhering to rate limits, with caching and multiprocessing capabilities.
- 💰 **Funds Management**: Allocates ₹20,000 per stock for trading.
- 🛡️ **Error Handling**: Logging module coming soon.

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

## 🛠️ Future Enhancements

- **Additional Indicators**: Add support for Bollinger Bands, Stochastic Oscillator, etc.
- **More Exchanges**: Support for BSE, international exchanges.
- **Advanced Risk Management**: Stop-loss, trailing stops, and better fund management.
- **Analytics Dashboard**: A real-time performance monitoring dashboard with profit/loss trends.

## 🤝 Contributing

We welcome contributions! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests.

## 📜 License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

---

**Happy Trading!** 🚀📈
