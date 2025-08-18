def buy_or_sell(df, row, holdings):

    # Work on the requested index (defaults to last row if row == -1)
    latest = df.iloc[-1]
    prev   = df.iloc[-2]

    # 2) Multi‑EMA trend stack
    trend_strong = (
        latest["Close"] > latest["EMA20"]
    )

    # 3) “Bullet‑proof” MACD momentum
    macd_ok = (
        latest["MACD"] > latest["MACD_Signal"] and
        latest["MACD_Hist"] > 0
    )

    # 4) RSI 60‑68 and rising **or** fresh cross above 60
    rsi_ok = 60 < latest["RSI"] <= 68 and latest["RSI"] >= prev["RSI"]

    # 5) Volume surge ≥ 1.2× SMA‑20
    vol_ok = latest["Volume"] > 1.2 * latest["SMA_20_Volume"]

    # 6) NEW – CMF positive and improving
    cmf_ok = (latest["CMF"] > 0) and (latest["CMF"] > prev["CMF"])

    # 7) ADX filter ≥ 20  
    adx_ok = latest["ADX"] > 20

    # ---------------- BUY ----------------
    if all((trend_strong, macd_ok, rsi_ok, vol_ok, cmf_ok, adx_ok)):
        return "BUY"

    # ---------------- SELL ----------------
    sell_signal = (
        (latest["EMA9"] < latest["EMA21"] * 0.99 < latest["EMA50"] * 0.99) and
        (latest["RSI"] < 45) and
        (latest["MACD_Hist"] < 0) and
        (latest["Volume"] > 1.5 * latest["SMA_20_Volume"])
    )

    if sell_signal:
        return "SELL"

    return "HOLD"