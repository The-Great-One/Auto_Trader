import time
from multiprocessing import Pool, cpu_count
import os
import pandas as pd
import sys
from Auto_Trader.KITE_TRIGGER_ORDER import handle_decisions
from Auto_Trader.utils import process_stock_and_decide, load_instruments_data
import logging
import traceback
import queue  # Import Python's queue module for handling empty exceptions
import json
from datetime import datetime

logger = logging.getLogger("Auto_Trade_Logger")
TRADING_MODE = os.getenv("AT_TRADING_MODE", "DAILY").strip().upper()
BAR_MINUTES = max(1, int(os.getenv("AT_BAR_MINUTES", "5")))
PAPER_SHADOW_MODE = os.getenv("AT_PAPER_SHADOW_MODE", "0").strip() in {"1", "true", "TRUE", "yes", "YES"}
PAPER_ALERT_MIN_SECONDS = max(0, int(os.getenv("AT_PAPER_ALERT_MIN_SECONDS", "1800")))
_PAPER_ALERT_COOLDOWN = max(30, int(os.getenv("AT_PAPER_ALERT_COOLDOWN", "300")))  # Min seconds between paper alerts (5 min default)
_ALERTED_BUY_SYMBOLS = set()   # Symbols that have been BUY-alerted; cleared only when they SELL
_ALERTED_SELL_SYMBOLS = set()   # Symbols that have been SELL-alerted; cleared only when they BUY

# Live price feed for external paper ledger consumers (e.g., RSI momentum tracker).
# Only writes prices for symbols tracked by the paper ledger to avoid I/O spam.
_LIVE_PRICE_PATH = "reports/live_prices.json"
_LIVE_PRICE_INTERVAL = int(os.getenv("AT_LIVE_PRICE_INTERVAL", "5"))  # seconds
_LAST_LIVE_PRICE_DUMP = 0.0


def _publish_live_prices(data: list, instruments_dict: dict) -> None:
    """Write symbol→last_price for symbols tracked by the paper ledger."""
    global _LAST_LIVE_PRICE_DUMP

    now = datetime.now().timestamp()
    if now - _LAST_LIVE_PRICE_DUMP < _LIVE_PRICE_INTERVAL:
        return
    _LAST_LIVE_PRICE_DUMP = now

    # Only track symbols the paper ledger actually holds
    import json as _json

    try:
        tracked = _json.load(open(f"reports/paper_ledger_rsi_momentum_state.json"))
        wanted = set(tracked.get("positions", {}).keys())
    except Exception:
        wanted = set()

    if not wanted:
        return

    prices: dict[str, float] = {}
    for stock in data:
        symbol = stock.get("Symbol", stock.get("tradingsymbol", ""))
        if symbol in wanted:
            px = float(stock.get("last_price", 0.0) or 0.0)
            if px > 0:
                prices[symbol] = px
    if not prices:
        return

    try:
        # Maintain a rolling cache. Tick batches may not contain every wanted
        # symbol, so overwriting with only the current batch causes partial MTM.
        existing = {}
        try:
            with open(_LIVE_PRICE_PATH) as f:
                existing = _json.load(f)
        except Exception:
            existing = {}

        now_str = datetime.now().isoformat(timespec="seconds")
        merged_prices = existing.get("prices", {}) if isinstance(existing.get("prices"), dict) else {}
        price_times = existing.get("price_times", {}) if isinstance(existing.get("price_times"), dict) else {}

        for symbol, px in prices.items():
            merged_prices[symbol] = px
            price_times[symbol] = now_str

        # Keep only symbols currently wanted by the paper ledger.
        merged_prices = {s: p for s, p in merged_prices.items() if s in wanted}
        price_times = {s: t for s, t in price_times.items() if s in wanted}

        with open(_LIVE_PRICE_PATH, "w") as f:
            _json.dump({
                "time": now_str,
                "prices": merged_prices,
                "price_times": price_times,
            }, f)
    except Exception:
        pass


def _load_paper_live_state() -> dict:
    try:
        with open("reports/paper_shadow_live_state.json") as f:
            state = json.load(f)
        return state if isinstance(state, dict) else {}
    except Exception:
        return {}


def _save_paper_live_state(state: dict) -> None:
    os.makedirs("reports", exist_ok=True)
    with open("reports/paper_shadow_live_state.json", "w") as f:
        json.dump(state, f, indent=2)


def _publish_paper_decisions(message_queue, decisions):
    global _ALERTED_BUY_SYMBOLS, _ALERTED_SELL_SYMBOLS

    buys = [d for d in decisions if d.get("Decision") == "BUY"]
    sells = [d for d in decisions if d.get("Decision") == "SELL"]
    now = datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S")

    buy_symbols = sorted({d.get("Symbol") for d in buys[:20] if d.get("Symbol")})
    sell_symbols = sorted({d.get("Symbol") for d in sells[:20] if d.get("Symbol")})

    state = _load_paper_live_state()
    buy_datetimes = state.get("buy_datetimes") or {}
    if not isinstance(buy_datetimes, dict):
        buy_datetimes = {}

    # The state file is the source of truth for paper lifecycle alerts. Module
    # globals reset on service restart/import reload, so never depend on them
    # alone for dedupe. A BUY alerts only when the symbol was not already open;
    # a SELL alerts only when the symbol was open before this cycle.
    prior_open_symbols = set(buy_datetimes)
    alerted_buy_symbols = set(state.get("alerted_buy_symbols") or prior_open_symbols)
    alerted_sell_symbols = set(state.get("alerted_sell_symbols") or [])
    new_buys = [s for s in buy_symbols if s not in prior_open_symbols and s not in alerted_buy_symbols]
    new_sells = [s for s in sell_symbols if s in prior_open_symbols and s not in alerted_sell_symbols]

    # Persist the first BUY timestamp per symbol until that symbol SELLs.
    for symbol in buy_symbols:
        buy_datetimes.setdefault(symbol, ts)
    for symbol in sell_symbols:
        buy_datetimes.pop(symbol, None)

    alerted_buy_symbols.update(buy_datetimes)
    alerted_buy_symbols.update(new_buys)
    alerted_buy_symbols -= set(sell_symbols)
    alerted_sell_symbols.update(new_sells)
    alerted_sell_symbols -= set(buy_symbols)

    state.update({
        "updated_at": ts,
        "buy_datetimes": buy_datetimes,
        "open_buy_symbols": sorted(buy_datetimes),
        "alerted_buy_symbols": sorted(alerted_buy_symbols),
        "alerted_sell_symbols": sorted(alerted_sell_symbols),
    })
    _save_paper_live_state(state)

    decision_details = []
    for decision in decisions[:25]:
        symbol = decision.get("Symbol")
        decision_details.append(
            {
                "symbol": symbol,
                "decision": decision.get("Decision"),
                "close": decision.get("Close"),
                "asset_class": decision.get("AssetClass"),
                "contributing_rules": decision.get("ContributingRules"),
                "sentiment_overlay": decision.get("SentimentOverlay"),
                "sentiment_overlays": decision.get("SentimentOverlays"),
                "buy_datetime": buy_datetimes.get(symbol),
            }
        )

    payload = {
        "time": ts,
        "mode": "paper-shadow",
        "buy_count": len(buys),
        "sell_count": len(sells),
        "buys": buy_symbols,
        "sells": sell_symbols,
        "buy_datetimes": buy_datetimes,
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "decision_details": decision_details,
    }

    os.makedirs("reports", exist_ok=True)
    with open("reports/paper_shadow_live_latest.json", "w") as f:
        json.dump(payload, f, indent=2)

    if not buys and not sells:
        return

    # Keep module globals warm for same-process diagnostics, but persistent
    # state above is what prevents re-alerts across restarts.
    _ALERTED_BUY_SYMBOLS = alerted_buy_symbols
    _ALERTED_SELL_SYMBOLS = alerted_sell_symbols

    if not new_buys and not new_sells:
        logger.debug(f"[PAPER] No new alerts (buys={buy_symbols}, sells={sell_symbols}, alerted_buy={_ALERTED_BUY_SYMBOLS}, alerted_sell={_ALERTED_SELL_SYMBOLS})")
        return

    buy_details = []
    for d in decisions[:25]:
        symbol = d.get("Symbol")
        if symbol in new_buys:
            buy_details.append(f"{symbol}@{d.get('Close', '?')} at {buy_datetimes.get(symbol, ts)}")
    sell_details = []
    for d in decisions[:25]:
        symbol = d.get("Symbol")
        if symbol in new_sells:
            sell_details.append(f"{symbol}@{d.get('Close', '?')} at {ts}")
    detail_line = ""
    if buy_details:
        detail_line += " BUY: " + ", ".join(buy_details)
    if sell_details:
        detail_line += " SELL: " + ", ".join(sell_details)
    msg = f"[PAPER] {ts} | BUY:{len(new_buys)} SELL:{len(new_sells)}{detail_line}"
    logger.info(msg)
    message_queue.put(msg)


def _resolve_bar_timestamp(stock_data):
    ts = (
        stock_data.get("exchange_timestamp")
        or stock_data.get("last_trade_time")
        or stock_data.get("timestamp")
    )
    parsed = pd.to_datetime(ts, errors="coerce")
    if pd.isna(parsed):
        parsed = pd.Timestamp.now(tz="Asia/Kolkata")

    if TRADING_MODE == "INTRADAY":
        return parsed.floor(f"{BAR_MINUTES}min").tz_localize(None)
    return parsed.normalize().tz_localize(None)


def _update_intraday_bar(stock_data, bar_ts, bar_state, last_cum_volume):
    token = stock_data.get("instrument_token")
    price = float(stock_data.get("last_price", 0.0) or 0.0)
    if token is None or price <= 0:
        return

    cum_volume = float(stock_data.get("volume_traded", 0.0) or 0.0)
    prev_cum = last_cum_volume.get(token, cum_volume)
    delta_volume = max(0.0, cum_volume - prev_cum)
    last_cum_volume[token] = cum_volume

    prev_bar = bar_state.get(token)
    if prev_bar is None or prev_bar["ts"] != bar_ts:
        curr_bar = {
            "ts": bar_ts,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": delta_volume,
        }
    else:
        curr_bar = prev_bar
        curr_bar["high"] = max(curr_bar["high"], price)
        curr_bar["low"] = min(curr_bar["low"], price)
        curr_bar["close"] = price
        curr_bar["volume"] += delta_volume
    bar_state[token] = curr_bar

    ohlc = stock_data.get("ohlc") or {}
    ohlc["open"] = curr_bar["open"]
    ohlc["high"] = curr_bar["high"]
    ohlc["low"] = curr_bar["low"]
    ohlc["close"] = curr_bar["close"]
    stock_data["ohlc"] = ohlc
    stock_data["volume_traded"] = curr_bar["volume"]


def Apply_Rules(q, message_queue):
    """RSI Momentum monitor — replaces RULE_SET engine.

    Receives live Kite ticks, publishes prices for the RSI Momentum paper ledger,
    and periodically sends portfolio status updates to Telegram.

    Architecture preserved: ticker -> queue -> Apply_Rules -> message_queue -> Telegram
    Only the algo changed: RULE_SET buy/sell decisions replaced with RSI status reporting.
    """
    instruments_dict = load_instruments_data()
    _last_status_s = 0.0
    STATUS_INTERVAL_S = 300  # Publish RSI Momentum status every 5 minutes

    while True:
        try:
            data = q.get()
            if data is None:
                logger.warning("Received shutdown signal. Exiting Apply_Rules.")
                break

            # Keep only the most recent tick snapshot
            while True:
                try:
                    newer = q.get_nowait()
                    if newer is None:
                        logger.warning("Shutdown signal while draining queue.")
                        return
                    data = newer
                except queue.Empty:
                    break

            # Enrich tick data with instrument metadata
            for stock_data in data:
                instrument_token = stock_data.get("instrument_token")
                instrument_data = instruments_dict.get(instrument_token, {})
                stock_data.update(instrument_data)

            # Publish live prices for RSI Momentum paper ledger MTM
            _publish_live_prices(data, instruments_dict)

            # Periodic RSI Momentum status to Telegram
            now = time.time()
            if now - _last_status_s >= STATUS_INTERVAL_S:
                _last_status_s = now
                _send_rsi_momentum_status(message_queue)

        except queue.Empty:
            continue
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                logger.info("Manual interrupt. Exiting.")
                break
            logger.error(f"Apply_Rules error: {e}\n{traceback.format_exc()}")
            time.sleep(5)


def _send_rsi_momentum_status(message_queue):
    """Read RSI Momentum paper ledger state + live prices and push a crisp
    P&L summary to the Telegram message queue."""
    from pathlib import Path

    try:
        state_path = Path("reports/paper_ledger_rsi_momentum_state.json")
        if not state_path.exists():
            return

        state = json.loads(state_path.read_text())
        positions = state.get("positions", {})
        if not positions:
            return

        # Read live prices for current MTM
        live_prices = {}
        live_path = Path("reports/live_prices.json")
        if live_path.exists():
            try:
                live = json.loads(live_path.read_text())
                live_prices = live.get("prices", {})
            except Exception:
                pass

        # Read Hist_Data fallback prices
        import pandas as pd
        hist_dir = Path("intermediary_files/Hist_Data")
        prices_fallback = {}
        if hist_dir.is_dir():
            try:
                for f in sorted(hist_dir.glob("*.csv")):
                    df = pd.read_csv(f, index_col=0, parse_dates=True)
                    if "close" in df.columns:
                        last = df["close"].ffill().iloc[-1]
                        sym = f.stem
                        prices_fallback[sym] = float(last)
            except Exception:
                pass

        now_str = datetime.now().strftime("%H:%M")
        rows = []
        total_pnl = 0.0
        live_count = 0

        cost_basis = state.get("cost_basis", {})
        capital = sum(float(positions[sym]) * float(cost_basis.get(sym, 0))
                      for sym in positions if cost_basis.get(sym, 0))

        for sym in sorted(positions):
            qty = float(positions[sym])
            avg = float(cost_basis.get(sym, 0))
            if not qty or not avg:
                continue

            px = float(live_prices.get(sym, 0) or 0)
            if px > 0:
                live_count += 1
            else:
                px = float(prices_fallback.get(sym, 0) or 0)

            if px > 0:
                pnl = (px - avg) * qty
                total_pnl += pnl
                pnl_pct = (px - avg) / avg * 100
                invested = qty * avg
                rows.append((sym, pnl, pnl_pct, px, invested))
            else:
                rows.append((sym, 0.0, 0.0, 0.0, 0.0))

        # Sort by % return (best first)
        rows.sort(key=lambda r: r[2], reverse=True)

        pnl_pct_total = (total_pnl / capital * 100) if capital else 0
        sign = "+" if total_pnl >= 0 else ""
        emoji_total = "🟢" if total_pnl > 0 else ("🔴" if total_pnl < 0 else "🟡")

        # Format position lines with emoji indicators
        pos_lines = []
        for i, (sym, pnl, pnl_pct_sym, px, invested) in enumerate(rows, 1):
            if px > 0:
                if pnl_pct_sym > 2:
                    emoji = "🟢"  # green
                elif pnl_pct_sym < -2:
                    emoji = "🔴"  # red
                else:
                    emoji = "🟡"  # yellow
                pnl_sign = "+" if pnl >= 0 else ""
                pos_lines.append(
                    f" {i:2d}. {emoji} {sym}  {pnl_sign}₹{pnl:,.0f} ({pnl_pct_sym:+.1f}%)  ₹{px:,.0f}"
                )
            else:
                pos_lines.append(f" {i:2d}. ⚪ {sym}  ? (no price)")

        price_src = f"{live_count}/{len(positions)} live" if live_count else "EOD prices"
        cap_str = f"₹{capital:,.0f}" if capital > 0 else "?"

        msg = (
            f"📊 RSI Momentum — {now_str}\n"
            f"💰 P&L: {sign}₹{total_pnl:,.0f} ({pnl_pct_total:+.1f}%)"
            f"  |  {price_src}  |  {cap_str} cap\n\n"
            + "\n".join(pos_lines)
        )
        message_queue.put(msg)

    except Exception as e:
        logger.error(f"RSI status failed: {e}")
