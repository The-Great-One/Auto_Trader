#!/usr/bin/env python3
"""RSI + Momentum Paper Ledger — daily portfolio simulation.

Reads the latest paper shadow picks and simulates an equal-weight
portfolio. Rebalances on month-end signal dates, marks to market daily.
Tracks full P&L history, drawdown, and risk metrics.

State file: reports/paper_ledger_rsi_momentum_state.json
Output: reports/paper_ledger_rsi_momentum_latest.json
"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
import socket
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OUT_DIR = ROOT / "reports"
HIST_DIR = Path(os.getenv("RSI_LEDGER_HIST_DIR", str(ROOT / "intermediary_files" / "Hist_Data")))
OUT_DIR.mkdir(exist_ok=True)

# Config
# Match the paper deployment request and live-trader constraints:
# - ₹2L starting book by default
# - whole-share fills with residual cash, no fractional equity shares
INITIAL_CAPITAL = float(os.getenv("RSI_LEDGER_CAPITAL", "200000"))
COST_BPS = float(os.getenv("RSI_LEDGER_COST_BPS", "10"))
PAPER_SHADOW_FILE = OUT_DIR / "paper_shadow_rsi_momentum_latest.json"
STATE_FILE = OUT_DIR / "paper_ledger_rsi_momentum_state.json"
OUTPUT_FILE = OUT_DIR / "paper_ledger_rsi_momentum_latest.json"
LIVE_PRICE_MAX_AGE_SEC = float(os.getenv("RSI_LEDGER_LIVE_MAX_AGE_SEC", "600"))
TELEGRAM_ALERTS = os.getenv("RSI_LEDGER_TELEGRAM_ALERTS", "1").strip().lower() not in {"0", "false", "no", "off"}
ST_EXIT_MULT = float(os.getenv("RSI_LEDGER_ST_EXIT_MULT", "0"))  # 0=disabled, 2.0=recommended
MIN_REBALANCE_PICKS = int(os.getenv("RSI_LEDGER_MIN_PICKS", "8"))
MIN_PRICE_ROWS = int(os.getenv("RSI_LEDGER_MIN_ROWS", "350"))


class RebalanceDataError(RuntimeError):
    """Raised when a rebalance cannot be executed completely and atomically."""


# ── Data loading ──────────────────────────────────────────────

def load_prices(hist_dir: Path, min_rows: int = 350) -> pd.DataFrame:
    """Load OHLCV close prices from feather files."""
    if not hist_dir.is_dir():
        return pd.DataFrame()
    loaded = {}
    for fpath in sorted(hist_dir.glob("*.feather")):
        symbol = fpath.stem
        try:
            df = pd.read_feather(fpath)
        except Exception:
            continue
        if any(kw in symbol for kw in ["FUT", "OPT", "-I", "-II"]):
            continue
        date_col = next((c for c in ["date", "Date", "datetime"] if c in df.columns), None)
        close_col = next((c for c in ["close", "Close", "CLOSE"] if c in df.columns), None)
        if date_col is None or close_col is None:
            continue
        df[date_col] = pd.to_datetime(df[date_col]).dt.tz_localize(None)
        s = df.set_index(date_col)[close_col].dropna().sort_index()
        if len(s) >= min_rows:
            loaded[symbol] = s
    prices_df = pd.DataFrame(loaded).sort_index()
    # Bridge small data gaps (weekends, holidays) so symbols with
    # 1-3 missing days don't silently drop from rebalance executions.
    return prices_df.ffill(limit=3)


def load_ohlcv(hist_dir: Path, symbols: set) -> dict:
    """Load OHLCV data for SuperTrend. Returns {sym: DataFrame(Close,High,Low)}."""
    ohlcv = {}
    for fpath in sorted(hist_dir.glob("*.feather")):
        sym = fpath.stem
        if sym not in symbols or any(kw in sym for kw in ["FUT", "OPT", "-I", "-II"]):
            continue
        try:
            df = pd.read_feather(fpath)
        except Exception:
            continue
        dc = next((c for c in ["date", "Date", "datetime"] if c in df.columns), None)
        cc = next((c for c in ["close", "Close", "CLOSE"] if c in df.columns), None)
        hc = next((c for c in ["high", "High"] if c in df.columns), None)
        lc = next((c for c in ["low", "Low"] if c in df.columns), None)
        if not all([dc, cc, hc, lc]):
            continue
        df[dc] = pd.to_datetime(df[dc]).dt.tz_localize(None)
        df = df.set_index(dc).sort_index()
        df = df.rename(columns={cc: "Close", hc: "High", lc: "Low"})
        ohlcv[sym] = df[["Close", "High", "Low"]]
    return ohlcv


def _check_st_exits(state, ohlcv_data, prices_dict, today):
    """Check held positions against SuperTrend and sell if below.
    Returns list of exit trades for logging/alerts."""
    import traceback as _tb

    if ST_EXIT_MULT <= 0:
        return []  # disabled
    
    if not ohlcv_data:
        print("[ST-EXIT] No OHLCV data available — skipping ST check")
        return []
    
    exits = []
    for sym, shares in list(state.positions.items()):
        try:
            if sym not in ohlcv_data:
                continue
            
            odf = ohlcv_data[sym]
            # Need at least 20 bars for ATR+ST
            if len(odf) < 20:
                print(f"[ST-EXIT] {sym}: insufficient data ({len(odf)} bars, need 20) — skipping")
                continue
            
            h = odf["High"].values
            l = odf["Low"].values
            c = odf["Close"].values
            
            # ATR(14)
            tr = np.maximum(h - l, np.maximum(
                np.abs(h - np.roll(c, 1)), np.abs(l - np.roll(c, 1))))
            atr = np.full(len(tr), np.nan)
            atr[13] = np.nanmean(tr[:14])
            for i in range(14, len(tr)):
                atr[i] = (atr[i - 1] * 13 + tr[i]) / 14
            
            # SuperTrend
            hl2 = (h + l) * 0.5
            up = hl2 + ST_EXIT_MULT * atr
            dn = hl2 - ST_EXIT_MULT * atr
            
            up_shift = np.roll(up, 1); up_shift[0] = np.nan
            dn_shift = np.roll(dn, 1); dn_shift[0] = np.nan
            
            raw = np.where(c > up_shift, dn, np.where(c < dn_shift, up, np.nan))
            st_line = pd.Series(raw).ffill().values
            above_st = c[-1] > st_line[-1] if not np.isnan(st_line[-1]) else True
            
            if not above_st:
                # Position is below SuperTrend -> exit
                px = prices_dict.get(sym, c[-1])
                if px <= 0:
                    px = c[-1]
                cost_rate = COST_BPS / 10000.0
                gross = shares * px
                cost = gross * cost_rate
                net = gross - cost
                
                state.cash += net
                state.trade_log.append({
                    "date": today,
                    "action": "SELL_ST",
                    "symbol": sym,
                    "shares": round(shares, 2),
                    "price": round(px, 2),
                    "gross": round(gross, 2),
                    "cost": round(cost, 2),
                    "net": round(net, 2),
                    "reason": f"below Supertrend({ST_EXIT_MULT}xATR)",
                })
                exits.append({"symbol": sym, "shares": shares, "price": px, "net": net})
                # Track realized P&L before deleting cost basis
                if sym in state.cost_basis:
                    entry_cost = shares * state.cost_basis[sym]
                    state.realized_pnl += net - entry_cost
                del state.positions[sym]
                if sym in state.cost_basis:
                    del state.cost_basis[sym]
                print(f"[ST-EXIT] SELL_ST {sym}: {shares} shares @ {px:.2f} (net ₹{net:,.0f})")
        except Exception as e:
            print(f"[ST-EXIT] ERROR processing {sym}: {e}\n{_tb.format_exc()}")
            continue
    
    if exits:
        print(f"[ST-EXIT] Total: {len(exits)} exits, net ₹{sum(e['net'] for e in exits):,.0f}")
    
    return exits


def _format_st_exit_alert(exits, today):
    """Format a compact ST exit Telegram alert."""
    if not exits:
        return ""
    total_net = sum(e["net"] for e in exits)
    lines = [f"[PAPER][RSI-MOM] ST Exit ({ST_EXIT_MULT}xATR)", f"Date: {today}"]
    for e in exits:
        lines.append(f'SELL {e["symbol"]} {e["shares"]} @ Rs.{e["price"]:,.2f} (Rs.{e["net"]:,.0f})')
    lines.append(f'Total exited: Rs.{total_net:,.0f}')
    return "\n".join(lines)


# ── State management ──────────────────────────────────────────

@dataclass
class PortfolioState:
    """Persistent state for the paper ledger."""
    cash: float = INITIAL_CAPITAL
    positions: dict[str, float] = field(default_factory=dict)  # symbol → shares
    cost_basis: dict[str, float] = field(default_factory=dict)  # symbol → avg buy price
    total_invested: float = 0.0
    last_rebalance_date: str = ""
    daily_values: list[dict] = field(default_factory=list)  # [{date, value, return}]
    trade_log: list[dict] = field(default_factory=list)
    realized_pnl: float = 0.0
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "PortfolioState":
        return cls(
            cash=d.get("cash", INITIAL_CAPITAL),
            positions=d.get("positions", {}),
            cost_basis=d.get("cost_basis", {}),
            total_invested=d.get("total_invested", 0.0),
            last_rebalance_date=d.get("last_rebalance_date", ""),
            daily_values=d.get("daily_values", []),
            trade_log=d.get("trade_log", []),
            realized_pnl=d.get("realized_pnl", 0.0),
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
        )


def load_state() -> PortfolioState:
    if STATE_FILE.exists():
        try:
            return PortfolioState.from_dict(json.loads(STATE_FILE.read_text()))
        except Exception:
            pass
    state = PortfolioState(created_at=datetime.now().isoformat())
    return state


def save_state(state: PortfolioState) -> None:
    state.updated_at = datetime.now().isoformat()
    STATE_FILE.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")


def _format_money(value: float) -> str:
    return f"₹{value:,.0f}"


def _format_trade_line(trade: dict) -> str:
    symbol = trade.get("symbol", "?")
    shares = trade.get("shares", 0)
    price = trade.get("price", 0)
    gross = trade.get("gross", trade.get("net", 0))
    return f"{symbol} {shares} @ ₹{price} ({_format_money(float(gross or 0))})"


def _format_paper_rebalance_alert(
    trades: list[dict],
    signal_date: str,
    valuation_date: str,
    current_value: float,
    position_count: int,
) -> str:
    """Create a concise paper BUY/SELL Telegram alert for a rebalance."""
    sells = [t for t in trades if t.get("action") == "SELL"]
    buys = [t for t in trades if t.get("action") == "BUY"]
    host = socket.gethostname()
    total_val = current_value
    lines = [
        f"🔄 RSI Momentum Rebalance — {signal_date}",
        f"💰 ₹{total_val:,.0f}  |  {position_count} positions  |  2W-FRI",
    ]
    if sells:
        lines.append(f"\n🔴 SELL {len(sells)}:")
        for t in sells:
            symbol = t.get("symbol", "?")
            shares = t.get("shares", 0)
            price = t.get("price", 0)
            gross = float(t.get("gross", 0) or 0)
            lines.append(f"  {symbol}  {shares} sh  @ ₹{price:,.2f}  (₹{gross:,.0f})")
    if buys:
        lines.append(f"\n🟢 BUY {len(buys)}:")
        for t in buys:
            symbol = t.get("symbol", "?")
            shares = t.get("shares", 0)
            price = t.get("price", 0)
            gross = float(t.get("gross", 0) or 0)
            pct = (gross / total_val * 100) if total_val else 0
            lines.append(f"  {symbol}  {shares} sh  @ ₹{price:,.2f}  (₹{gross:,.0f}, {pct:.1f}%)")
    lines.append("\n📝 Paper only — no live orders")
    return "\n".join(lines)


def send_paper_telegram_alert(message: str) -> bool:
    """Send paper-ledger alert using the same Telegram token/channel as live trader."""
    if not TELEGRAM_ALERTS:
        return False
    try:
        from Auto_Trader.my_secrets import CHANNEL, TG_TOKEN
        from telegram import Bot
    except Exception as exc:
        print(f"WARN: Telegram alert unavailable: {exc}")
        return False

    if not TG_TOKEN or not CHANNEL:
        print("WARN: Telegram alert skipped: TG_TOKEN/CHANNEL not configured")
        return False

    chat_id = os.getenv("AT_TEST_TRADER_CHANNEL", "").strip() or CHANNEL
    try:
        bot = Bot(token=TG_TOKEN)
        asyncio.run(bot.send_message(chat_id=chat_id, text=message))
        return True
    except Exception as exc:
        print(f"WARN: Telegram alert failed: {exc}")
        return False


# ── Core simulation ──────────────────────────────────────────

def get_latest_signal() -> Optional[dict]:
    """Read the latest paper shadow signal."""
    if not PAPER_SHADOW_FILE.exists():
        return None
    try:
        data = json.loads(PAPER_SHADOW_FILE.read_text())
        return data.get("latest_signal")
    except Exception:
        return None


def portfolio_value(state: PortfolioState, prices: dict[str, float]) -> float:
    """Calculate current portfolio value (cash + positions MTM)."""
    position_value = 0.0
    for symbol, shares in state.positions.items():
        if symbol in prices and prices[symbol] > 0:
            position_value += shares * prices[symbol]
    return state.cash + position_value


def _has_valid_price(prices: pd.Series, symbol: str) -> bool:
    if symbol not in prices:
        return False
    try:
        value = float(prices[symbol])
    except (TypeError, ValueError):
        return False
    return math.isfinite(value) and value > 0


def validate_rebalance_inputs(
    state: PortfolioState,
    picks: list[str],
    prices_series: pd.Series,
    min_picks: int = MIN_REBALANCE_PICKS,
) -> None:
    """Validate a complete rotation before any portfolio state is mutated."""
    unique_picks = list(dict.fromkeys(picks))
    if len(unique_picks) != len(picks):
        raise RebalanceDataError("rebalance signal contains duplicate picks")
    if len(unique_picks) != min_picks:
        raise RebalanceDataError(
            f"rebalance signal has {len(unique_picks)} picks; exactly {min_picks} required"
        )

    missing_held = sorted(
        symbol for symbol in state.positions if not _has_valid_price(prices_series, symbol)
    )
    missing_picks = sorted(
        symbol for symbol in unique_picks if not _has_valid_price(prices_series, symbol)
    )
    problems = []
    if missing_held:
        problems.append(f"held positions missing valid sell prices: {', '.join(missing_held)}")
    if missing_picks:
        problems.append(f"targets missing valid buy prices: {', '.join(missing_picks)}")
    if problems:
        raise RebalanceDataError("; ".join(problems))


def _commit_rebalance_state(
    original: PortfolioState,
    staged: PortfolioState,
) -> PortfolioState:
    for field_name in PortfolioState.__dataclass_fields__:
        setattr(original, field_name, copy.deepcopy(getattr(staged, field_name)))
    return original


def execute_rebalance(
    state: PortfolioState,
    picks: list[str],
    prices_series: pd.Series,
    date: str,
    cost_bps: float = COST_BPS,
) -> PortfolioState:
    """Sell everything, buy new picks equal-weight."""
    # Validate the whole rotation before touching cash, positions, or logs.
    validate_rebalance_inputs(state, picks, prices_series)
    original_state = state
    state = copy.deepcopy(state)
    cost_rate = cost_bps / 10000.0

    # 1. Sell existing positions
    sold_value = 0.0
    for symbol, shares in list(state.positions.items()):
        if symbol in prices_series and prices_series[symbol] > 0:
            px = float(prices_series[symbol])
            gross = shares * px
            cost = gross * cost_rate
            net = gross - cost
            state.cash += net
            sold_value += net
            state.trade_log.append({
                "date": date,
                "action": "SELL",
                "symbol": symbol,
                "shares": round(shares, 2),
                "price": round(px, 2),
                "gross": round(gross, 2),
                "cost": round(cost, 2),
                "net": round(net, 2),
            })
    # Track realized P&L before clearing
    for symbol, shares in list(state.positions.items()):
        if symbol in state.cost_basis:
            entry_cost = shares * state.cost_basis[symbol]
            # Find the SELL trade for this symbol to get net proceeds
            for t in reversed(state.trade_log):
                if t.get("action") == "SELL" and t.get("symbol") == symbol:
                    state.realized_pnl += t["net"] - entry_cost
                    break
    state.positions.clear()
    state.cost_basis.clear()

    # 2. Buy new picks equal-weight using whole shares.
    # This mirrors the live equity environment more closely than fractional
    # shares: each target bucket gets a budget, buys floor(budget / all-in px),
    # and leaves uninvested residual cash in the ledger.
    unavailable = []
    available = []
    for s in picks:
        if s not in prices_series:
            unavailable.append((s, "symbol not in price data"))
        elif pd.isna(prices_series[s]) or prices_series[s] <= 0:
            unavailable.append((s, "price NaN or <= 0"))
        else:
            available.append(s)
    for sym, reason in unavailable:
        state.trade_log.append({
            "date": date,
            "action": "SKIP",
            "symbol": sym,
            "reason": reason,
            "price": 0,
        })
    if not available:
        state.last_rebalance_date = date
        return _commit_rebalance_state(original_state, state)

    per_symbol_capital = state.cash / len(available)
    skipped = []
    for symbol in available:
        px = float(prices_series[symbol])
        all_in_px = px * (1 + cost_rate)
        shares = math.floor(per_symbol_capital / all_in_px)
        if shares <= 0:
            skipped.append(symbol)
            continue
        gross = shares * px
        cost = gross * cost_rate
        debit = gross + cost
        if debit > state.cash:
            shares = math.floor(state.cash / all_in_px)
            if shares <= 0:
                skipped.append(symbol)
                continue
            gross = shares * px
            cost = gross * cost_rate
            debit = gross + cost
        state.positions[symbol] = float(shares)
        state.cost_basis[symbol] = px
        state.cash -= debit
        state.trade_log.append({
            "date": date,
            "action": "BUY",
            "symbol": symbol,
            "shares": int(shares),
            "price": round(px, 2),
            "gross": round(gross, 2),
            "cost": round(cost, 2),
            "net": round(debit, 2),
        })

    for symbol in skipped:
        state.trade_log.append({
            "date": date,
            "action": "SKIP",
            "symbol": symbol,
            "reason": "insufficient per-symbol capital for one whole share",
            "price": round(float(prices_series[symbol]), 2),
        })

    state.last_rebalance_date = date
    return _commit_rebalance_state(original_state, state)


def compute_metrics(daily_values: list[dict]) -> dict:
    """Compute performance metrics from daily value history."""
    if len(daily_values) < 20:
        return {"error": "insufficient history"}

    df = pd.DataFrame(daily_values)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    initial = df["value"].iloc[0]
    final = df["value"].iloc[-1]
    total_return = (final / initial) - 1

    # Daily returns
    df["returns"] = df["value"].pct_change().fillna(0)
    years = len(df) / 252
    cagr = (final / initial) ** (1 / years) - 1 if years > 0 else 0

    # Drawdown
    peak = df["value"].cummax()
    drawdown = df["value"] / peak - 1
    max_dd = float(drawdown.min())

    # Vol + Sharpe
    daily_r = df["returns"].iloc[1:]  # exclude first day
    vol = float(daily_r.std() * math.sqrt(252)) if len(daily_r) > 1 else 0.0
    sharpe = float((daily_r.mean() * 252) / vol) if vol > 0 else 0.0

    # Monthly returns
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M")
    monthly = df.groupby("month")["returns"].apply(lambda x: (1 + x).prod() - 1)
    positive_months = int((monthly > 0).sum())

    return {
        "days_tracked": len(df),
        "years": round(years, 2),
        "initial_capital": round(initial, 2),
        "current_value": round(final, 2),
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "vol_pct": round(vol * 100, 1),
        "sharpe": round(sharpe, 3),
        "positive_months": positive_months,
        "total_months": int(len(monthly)),
        "monthly_returns": {str(k): round(float(v) * 100, 2) for k, v in monthly.tail(12).items()},
    }


def should_rebalance(state: PortfolioState, signal: dict, today: str) -> bool:
    """Check if today is the first trading day after a new signal."""
    signal_date = signal.get("date", "")
    if not signal_date:
        return False
    # Rebalance if signal is newer than last rebalance
    if not state.last_rebalance_date:
        return True
    return signal_date > state.last_rebalance_date


def log_daily(state: PortfolioState, value: float, date: str) -> None:
    """Record/refresh portfolio value for a trading day.

    The ledger can run intraday/hourly. For metrics, keep one observation per
    trading date by replacing the same-date entry instead of appending multiple
    fake "daily" returns in one day.
    """
    rounded_value = round(value, 2)
    entry = {
        "date": date,
        "value": rounded_value,
        "positions": len(state.positions),
        "cash": round(state.cash, 2),
    }

    if state.daily_values and state.daily_values[-1].get("date") == date:
        # Preserve the return versus the prior trading day while refreshing MTM.
        prev_value = state.daily_values[-2]["value"] if len(state.daily_values) > 1 else rounded_value
        entry["return_pct"] = round(((rounded_value / prev_value) - 1) * 100, 4) if prev_value > 0 else 0.0
        state.daily_values[-1] = entry
    else:
        prev_value = state.daily_values[-1]["value"] if state.daily_values else rounded_value
        entry["return_pct"] = round(((rounded_value / prev_value) - 1) * 100, 4) if prev_value > 0 else 0.0
        state.daily_values.append(entry)

    # Keep last 2 years
    if len(state.daily_values) > 504:
        state.daily_values = state.daily_values[-504:]


# ── Main ────────────────────────────────────────────────────

def main() -> int:
    prices_df = load_prices(HIST_DIR, min_rows=MIN_PRICE_ROWS)
    if prices_df.empty:
        print("ERROR: no price data")
        return 1

    signal = get_latest_signal()
    if signal is None:
        print("WARN: no paper shadow signal found — skipping")
        return 0

    signal_date = signal.get("date", "")
    picks = signal.get("picks", [])

    if not picks:
        print("WARN: no picks in signal")
        return 0

    # Today = latest available date in price data (end-of-day)
    # In cron: this is today's EOD data
    today = str(prices_df.index[-1].date())
    today_prices = prices_df.iloc[-1]  # latest row prices

    # Signal date prices — for executing buys/sells at correct entry prices
    signal_dt = pd.Timestamp(signal_date)
    if signal_dt in prices_df.index:
        signal_prices = prices_df.loc[signal_dt]
    else:
        # Find the nearest trading day at or after signal date
        idx = prices_df.index.searchsorted(signal_dt)
        if idx < len(prices_df):
            signal_prices = prices_df.iloc[idx]
        else:
            signal_prices = prices_df.iloc[-1]

    # Load state
    state = load_state()

    # Check if rebalance needed
    new_trades: list[dict] = []
    trade_log_len_before = len(state.trade_log)
    try:
        if should_rebalance(state, signal, signal_date):
            print(f"REBALANCE: signal {signal_date} is newer than last rebalance {state.last_rebalance_date}")
            state = execute_rebalance(state, picks, signal_prices, signal_date)
            new_trades = state.trade_log[trade_log_len_before:]
        elif not state.positions:
            # First run — initialize with current signal
            print(f"INIT: first run, buying {len(picks)} picks from signal {signal_date}")
            state = execute_rebalance(state, picks, signal_prices, signal_date)
            new_trades = state.trade_log[trade_log_len_before:]
    except RebalanceDataError as exc:
        print(f"ERROR: rebalance aborted without state changes: {exc}")
        return 2

    # MTM current positions — prefer live prices from rt_compute, fall back to Hist_Data
    LIVE_PRICE_FILE = ROOT / "reports" / "live_prices.json"

    prices_dict: dict[str, float] = {}
    price_sources: dict[str, str] = {}
    live_time = ""
    live_age_sec: float | None = None
    fresh_live_count = 0

    if LIVE_PRICE_FILE.exists():
        try:
            live = json.loads(LIVE_PRICE_FILE.read_text())
            live_time = live.get("time", "")
            live_prices = live.get("prices", {})
            price_times = live.get("price_times", {}) if isinstance(live.get("price_times"), dict) else {}
            # Use fresh per-symbol ticks. Some paper positions do not tick every
            # few seconds, so default freshness is 10 minutes, configurable via
            # RSI_LEDGER_LIVE_MAX_AGE_SEC.
            live_dt = datetime.fromisoformat(live_time)
            now_ist = datetime.now()
            live_age_sec = (now_ist - live_dt).total_seconds()
            # After market close (15:30 IST), the last same-day live tick IS the
            # closing price. Bypass freshness only for today's ticks — never for
            # week-old live_prices.json data.
            market_closed = (now_ist.hour, now_ist.minute) >= (15, 30)
            allow_close_tick = market_closed and live_dt.date() == now_ist.date()
            max_age = float('inf') if allow_close_tick else LIVE_PRICE_MAX_AGE_SEC

            if live_prices:
                for sym in state.positions:
                    px = float(live_prices.get(sym, 0.0) or 0.0)
                    if px <= 0:
                        continue
                    sym_time = price_times.get(sym) or live_time
                    sym_dt = datetime.fromisoformat(sym_time)
                    sym_age = (datetime.now() - sym_dt).total_seconds()
                    if sym_age < max_age:
                        prices_dict[sym] = px
                        price_sources[sym] = f"live:{sym_time}"
                        fresh_live_count += 1
        except Exception:
            pass

    # Fall back to Hist_Data for any positions not covered by live feed.
    hist_fallback_count = 0
    for sym in state.positions:
        if sym in prices_dict:
            continue
        if sym in prices_df.columns:
            col = prices_df[sym].ffill()
            last_valid = col.last_valid_index()
            if last_valid is not None and col.loc[last_valid] > 0:
                prices_dict[sym] = float(col.loc[last_valid])
                price_sources[sym] = f"hist_data:{last_valid.date()}"
                hist_fallback_count += 1

    missing_price_symbols = sorted(set(state.positions) - set(prices_dict))
    if fresh_live_count and hist_fallback_count:
        price_source = f"mixed_live_hist ({fresh_live_count} live, {hist_fallback_count} hist; live {live_time})"
    elif fresh_live_count:
        price_source = f"live ({live_time})"
    else:
        price_source = "hist_data"

    valuation_date = today
    if fresh_live_count and live_time:
        try:
            valuation_date = str(datetime.fromisoformat(live_time).date())
        except Exception:
            valuation_date = today

    current_value = portfolio_value(state, prices_dict)

    # Log/refresh portfolio value for this valuation date
    log_daily(state, current_value, valuation_date)

    # ── SuperTrend exit check (daily, between rebalances) ──
    st_exits = []
    if ST_EXIT_MULT > 0 and len(state.positions) > 0:
        st_ohlcv = load_ohlcv(HIST_DIR, set(state.positions.keys()))
        st_exits = _check_st_exits(state, st_ohlcv, prices_dict, today)
        if st_exits:
            # Recompute portfolio value after exits
            current_value = portfolio_value(state, prices_dict)
            log_daily(state, current_value, valuation_date)

    # Compute metrics
    metrics = compute_metrics(state.daily_values)

    # Save state before sending Telegram so a notification retry cannot duplicate
    # the same paper rebalance on the next 5-minute cron tick.
    save_state(state)

    telegram_alert_sent = False
    if st_exits:
        telegram_alert_sent = send_paper_telegram_alert(
            _format_st_exit_alert(st_exits, today)
        )
    if new_trades:
        telegram_alert_sent = send_paper_telegram_alert(
            _format_paper_rebalance_alert(
                new_trades,
                signal_date=signal_date,
                valuation_date=valuation_date,
                current_value=current_value,
                position_count=len(state.positions),
            )
        )

    # Build output
    positions_detail = {}
    for sym, shares in state.positions.items():
        if sym in prices_dict:
            px = prices_dict[sym]
            mv = shares * px
            cost = state.cost_basis.get(sym, 0)
            positions_detail[sym] = {
                "shares": round(shares, 2),
                "avg_price": round(cost, 2),
                "current_price": round(px, 2),
                "price_source": price_sources.get(sym, "unknown"),
                "market_value": round(mv, 2),
                "pnl_pct": round((px / cost - 1) * 100, 2) if cost > 0 else 0.0,
            }

    output = {
        "generated_at": datetime.now().isoformat(),
        "valuation_date": valuation_date,
        "strategy": "rsi_momentum_rotation_paper_ledger",
        "signal": {
            "date": signal_date,
            "picks": picks,
        },
        "portfolio": {
            "initial_capital": INITIAL_CAPITAL,
            "cash": round(state.cash, 2),
            "position_value": round(current_value - state.cash, 2),
            "total_value": round(current_value, 2),
            "deployment_pct": round(((current_value - state.cash) / current_value) * 100, 2) if current_value > 0 else 0.0,
            "positions_count": len(state.positions),
            "price_source": price_source,
            "live_price_time": live_time,
            "live_price_age_sec": round(live_age_sec, 1) if live_age_sec is not None else None,
            "live_positions_priced": fresh_live_count,
            "hist_positions_priced": hist_fallback_count,
            "missing_price_symbols": missing_price_symbols,
            "positions": positions_detail,
            "last_rebalance": state.last_rebalance_date,
            "created_at": state.created_at,
        },
        "metrics": metrics,
        "telegram_alert": {
            "enabled": TELEGRAM_ALERTS,
            "rebalance_trade_count": len(new_trades),
            "sent": telegram_alert_sent,
        },
        "latest_trades": state.trade_log[-20:],
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")

    # Print summary
    print(f"\n=== RSI Momentum Paper Ledger ===")
    print(f"Date: {valuation_date} | Signal: {signal_date} | Picks: {len(picks)} | Price source: {price_source}")
    print(f"Portfolio:  ₹{current_value:,.2f}  (Cash: ₹{state.cash:,.2f}, Positions: {len(state.positions)})")
    if "total_return_pct" in metrics:
        print(f"Return:     {metrics['total_return_pct']:+.2f}%  CAGR: {metrics.get('cagr_pct', 0):+.2f}%")
        print(f"MaxDD:      {metrics['max_drawdown_pct']:+.2f}%  Sharpe: {metrics.get('sharpe', 0):.3f}")
    print(f"Saved: {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
