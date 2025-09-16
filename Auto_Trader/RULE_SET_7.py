from Auto_Trader import logging

logger = logging.getLogger("Auto_Trade_Logger")

def buy_or_sell(df, row, holdings):

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    trend_strong = latest["Close"] > latest["EMA20"]
    macd_ok = latest["MACD"] > latest["MACD_Signal"] and latest["MACD_Hist"] > 0
    rsi_ok = 60 < latest["RSI"] <= 68 and latest["RSI"] >= prev["RSI"]
    vol_ok = latest["Volume"] > 1.2 * latest["SMA_20_Volume"]
    cmf_ok = (latest["CMF"] >= 0.05) and (latest["CMF"] > prev["CMF"])
    adx_ok = latest["ADX"] > 20

    # ---------------- BUY ----------------
    if all((trend_strong, macd_ok, rsi_ok, vol_ok, cmf_ok, adx_ok)):
        return "BUY"

    return "HOLD"
