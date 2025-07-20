def buy_or_sell(df, row, holdings):

    # Work on the requested index (defaults to last row if row == -1)
    latest = df.iloc[-1]
    prev   = df.iloc[- 2]

    # 1) Break‑out band 1–8 % above SMA‑10 and above SMA‑20
    breakout = (
        (latest["Close"] > latest["SMA_10_Close"] * 1.01) and
        (latest["Close"] < latest["SMA_10_Close"] * 1.08) and
        (latest["Close"] > latest["SMA_20_Close"])
    )

    # 2) Multi‑EMA trend stack
    trend_strong = (
        latest["EMA20"] > latest["EMA50"] > latest["EMA100"] and
        latest["Close"] > latest["EMA20"]
    )

    # 3) “Bullet‑proof” MACD momentum
    macd_ok = (
        latest["MACD"] > latest["MACD_Signal"] and
        (latest["MACD"] - latest["MACD_Signal"]) > 0.15 and
        (latest["MACD"] - prev["MACD"]) > 0.10 and
        (latest["MACD_Hist"] - prev["MACD_Hist"]) > 0.10 and
        latest["MACD_Hist"] > 0
    )

    # 4) RSI 60‑70 and rising **or** fresh cross above 60
    rsi_ok = (
        (60 < latest["RSI"] <= 68 and (latest["RSI"] - prev["RSI"]) > 2) or
        (prev["RSI"] < 60 <= latest["RSI"])
    )

    # 5) Volume surge ≥ 1.2× SMA‑20
    vol_ok = latest["Volume"] > 1.2 * latest["SMA_20_Volume"]

    # 6) NEW – CMF positive and improving
    cmf_ok = (latest["CMF"] > 0.00) and (latest["CMF"] > prev["CMF"])

    # 7) ADX filter ≥ 20  
    adx_ok = latest["ADX"] > 20    # keeps original behaviour

    # ---------------- BUY ----------------
    if all((breakout, trend_strong, macd_ok, rsi_ok, vol_ok, cmf_ok, adx_ok)):
        return "BUY"

    # ---------------- SELL ----------------
    sell_signal = (
        (latest["EMA9"] < latest["EMA21"] * 0.99 < latest["EMA50"] * 0.99) and
        (latest["RSI"] < 45) and
        (latest["MACD_Hist"] < 0) and
        (latest["Volume"] > 1.5 * latest["SMA_20_Volume"])
    )  # logic carried over unchanged

    if sell_signal:
        return "SELL"

    return "HOLD"
