from __future__ import annotations

import json
import math
import os
import re
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html, dash_table, no_update
from dash.exceptions import PreventUpdate

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
INTERMEDIARY_DIR = ROOT / "intermediary_files"
TWITTER_DIR = INTERMEDIARY_DIR / "twitter_sentiment"
LAB_STATUS_PATH = INTERMEDIARY_DIR / "lab_status" / "weekly_strategy_lab_status.json"
LIVE_TELEGRAM_LEDGER_PATH = REPORTS_DIR / "live_telegram_options_paper_latest.json"
LIVE_TELEGRAM_LEDGER_HISTORY = REPORTS_DIR / "live_telegram_options_paper_equity_history.jsonl"
TELEGRAM_TRADE_AUDIT_PATH = REPORTS_DIR / "telegram_trade_audit_latest.json"
ECO_CALENDAR_PATH = REPORTS_DIR / "economic_calendar_sector_latest.json"
GLOBAL_MACRO_PATH = REPORTS_DIR / "global_macro_latest.json"
NEWS_BEHAVIOR_PATH = REPORTS_DIR / "news_topic_symbol_behavior_latest.json"
PORTFOLIO_TRACKER_PATH = REPORTS_DIR / "portfolio_tracker_latest.json"
EARNINGS_PIPELINE_PATH = REPORTS_DIR / "earnings_call_pipeline_latest.json"
WATCH_UPDATES_PATH = Path.home() / ".openclaw" / "telegram-user" / "watch_channel_updates.jsonl"
WATCH_RECEIPTS_PATH = Path.home() / ".openclaw" / "telegram-user" / "watch_receipts.jsonl"
SERVER_KEY = Path(os.getenv("AT_SERVER_KEY", os.path.expanduser("~/.openclaw/credentials/oracle_ssh_key")))
SERVER_HOST = os.getenv("AT_SERVER_HOST", os.getenv("AT_ORACLE", ""))
SERVER_REPO = os.getenv("AT_SERVER_REPO", "/home/ubuntu/Auto_Trader")
COMBINED_LAB_STATUS_FILES = [
    "sizing_exit_sweep_latest.json",
    "volatility_sizing_lab_latest.json",
    "regime_filter_lab_latest.json",
    "focused_cluster_lab_latest.json",
    "meta_label_lab_latest.json",
]
SERVER_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
GLOBAL_MACRO_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
ECO_CALENDAR_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
TELEGRAM_AUDIT_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
EVENT_PIPELINES_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
PORTFOLIO_TRACKER_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
DASH_DATA_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
EQUITY_SPOT_CACHE: dict[str, Any] = {}
LAST_COMPACT_TS: float = 0.0
COMPACT_INTERVAL_SECONDS = 300  # compact JSONL every 5 min
SSH_TTL_SECONDS = 45
GLOBAL_MACRO_TTL_SECONDS = 600
ECO_CALENDAR_TTL_SECONDS = 600
EVENT_PIPELINES_TTL_SECONDS = 900
EQUITY_SPOT_TTL_SECONDS = 900
IST = timezone(timedelta(hours=5, minutes=30))

def parse_timestamp_any(value: Any) -> pd.Timestamp | None:
    if value in (None, "", "-"):
        return None
    try:
        if isinstance(value, pd.Timestamp):
            dt = value
        elif isinstance(value, (int, float)):
            num = float(value)
            abs_num = abs(num)
            if abs_num >= 1e18:
                dt = pd.to_datetime(num, unit="ns", utc=True)
            elif abs_num >= 1e15:
                dt = pd.to_datetime(num, unit="us", utc=True)
            elif abs_num >= 1e12:
                dt = pd.to_datetime(num, unit="ms", utc=True)
            else:
                dt = pd.to_datetime(num, unit="s", utc=True)
        elif isinstance(value, str) and value.strip().isdigit():
            return parse_timestamp_any(int(value.strip()))
        else:
            dt = pd.Timestamp(value)
            if pd.isna(dt):
                return None
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
        if pd.isna(dt):
            return None
        if dt.tzinfo is None:
            dt = dt.tz_localize("UTC")
        return dt
    except Exception:
        return None


def to_ist(dt_str: Any) -> str:
    """Convert UTC/ISO/epoch timestamp to IST display string."""
    dt = parse_timestamp_any(dt_str)
    if dt is None:
        return "-" if dt_str in (None, "", "-") else str(dt_str)
    try:
        return dt.tz_convert(IST).strftime("%b %d, %H:%M")
    except Exception:
        return str(dt_str)


def to_ist_verbose(dt_str: Any) -> str:
    """Convert UTC/ISO/epoch timestamp to an IST string with timezone label."""
    dt = parse_timestamp_any(dt_str)
    if dt is None:
        return "-" if dt_str in (None, "", "-") else str(dt_str)
    try:
        return dt.tz_convert(IST).strftime("%b %d, %H:%M:%S IST")
    except Exception:
        return str(dt_str)

PAGE_STYLE = {
    "background": "#0a0e17",
    "color": "#e0e0e0",
    "minHeight": "100vh",
    "padding": "12px",
    "fontFamily": "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace",
    "fontSize": "13px",
}
CARD_STYLE = {
    "background": "#111827",
    "border": "1px solid #1e2a3a",
    "borderRadius": "4px",
    "padding": "12px",
}
BLOOMBERG_ORANGE = "#f5a623"
BLOOMBERG_GREEN = "#00ff88"
BLOOMBERG_RED = "#ff4444"
BLOOMBERG_BLUE = "#4fc3f7"
BLOOMBERG_YELLOW = "#ffd600"
BLOOMBERG_GRAY = "#8899aa"
TABLE_STYLE = {"overflowX": "auto"}
TABLE_CELL_STYLE = {
    "backgroundColor": "#0a0e17",
    "color": "#e0e0e0",
    "border": "1px solid #1e2a3a",
    "textAlign": "left",
    "padding": "6px 8px",
    "whiteSpace": "normal",
    "height": "auto",
    "fontSize": "12px",
    "fontFamily": "'JetBrains Mono', monospace",
}
TABLE_HEADER_STYLE = {"backgroundColor": "#111827", "fontWeight": "bold", "color": BLOOMBERG_ORANGE, "border": "1px solid #1e2a3a", "padding": "6px 8px", "fontSize": "11px", "letterSpacing": "0.5px"}


def load_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def latest_report(pattern: str) -> tuple[Path | None, dict[str, Any] | None]:
    paths = sorted(REPORTS_DIR.glob(pattern))
    if not paths:
        return None, None
    path = paths[-1]
    data = load_json(path)
    return path, data if isinstance(data, dict) else None


def recent_strategy_reports(limit: int = 20) -> list[tuple[Path, dict[str, Any]]]:
    rows: list[tuple[Path, dict[str, Any]]] = []
    for p in sorted(REPORTS_DIR.glob("strategy_lab_*.json"))[-limit:]:
        data = load_json(p)
        if isinstance(data, dict):
            rows.append((p, data))
    return rows


def recent_telegram_options_reports(limit: int = 20) -> list[tuple[Path, dict[str, Any]]]:
    rows: list[tuple[Path, dict[str, Any]]] = []
    for p in sorted(REPORTS_DIR.glob("telegram_options_paper*.json"))[-limit:]:
        data = load_json(p)
        if isinstance(data, dict):
            rows.append((p, data))
    return rows


MAX_JSONL_LINES = 5000
MAX_JSONL_BYTES = 5 * 1024 * 1024  # 5 MB – compact if larger


def load_jsonl(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except Exception:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    if limit is not None and len(rows) > limit:
        rows = rows[-limit:]
    return rows


def compact_jsonl(path: Path, max_lines: int = MAX_JSONL_LINES, max_bytes: int = MAX_JSONL_BYTES) -> bool:
    """Compact a JSONL file if it exceeds size or line limits.

    Keeps only the most recent *max_lines* entries. Returns True if compaction
    was performed, False if the file was already within limits.
    """
    if not path.exists():
        return False
    try:
        size = path.stat().st_size
        if size <= max_bytes and size <= 1_000_000:
            # Quick check: small file, count lines cheaply
            line_count = sum(1 for _ in open(path))
            if line_count <= max_lines:
                return False
        lines = path.read_text().splitlines()
        if len(lines) <= max_lines:
            return False
        kept = lines[-max_lines:]
        path.write_text("\n".join(kept) + "\n")
        print(f"[compact] {path.name}: {len(lines)} → {len(kept)} lines ({size} → {path.stat().st_size} bytes)")
        return True
    except Exception as exc:
        print(f"[compact] {path.name}: error – {exc}")
        return False


COMPACT_JSONL_PATHS = [
    WATCH_UPDATES_PATH,
    WATCH_RECEIPTS_PATH,
    LIVE_TELEGRAM_LEDGER_HISTORY,
]


def compact_all_jsonl() -> int:
    """Run compaction on all known JSONL files. Returns count of compacted files."""
    count = 0
    for p in COMPACT_JSONL_PATHS:
        if compact_jsonl(p):
            count += 1
    # Also check server-side JSONL archives via glob
    for p in REPORTS_DIR.glob("*_history.jsonl"):
        if compact_jsonl(p, max_lines=2000, max_bytes=MAX_JSONL_BYTES):
            count += 1
    return count


def to_df(items: Any) -> pd.DataFrame:
    if items is None:
        return pd.DataFrame()
    if isinstance(items, pd.DataFrame):
        return items
    if isinstance(items, dict):
        return pd.DataFrame([items])
    if isinstance(items, list):
        return pd.DataFrame(items)
    return pd.DataFrame()


def friendly(v: Any) -> str:
    if v is None or v == "":
        return "-"
    if isinstance(v, float):
        if math.isnan(v):
            return "-"
        if abs(v) >= 1000:
            return f"{v:,.2f}"
        return f"{v:.2f}"
    return str(v)


def fmt_pnl(val: float | None, prefix: str = "") -> str:
    """Format PnL with sign and color hint."""
    if val is None:
        return "-"
    sign = "+" if val > 0 else ""
    return f"{prefix}{sign}{val:,.2f}"


def fmt_pct(val: float | None) -> str:
    """Format percentage with sign."""
    if val is None:
        return "-"
    return f"{val:+.2f}%" if val != 0 else "0.00%"


def safe_num(v: Any) -> float | None:
    try:
        if v is None:
            return None
        out = float(v)
        return None if math.isnan(out) else out
    except Exception:
        return None


def normalize_price_history(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [c[0] if isinstance(c, tuple) else c for c in out.columns]
    if "Date" not in out.columns:
        out = out.reset_index()
    if "Date" not in out.columns or "Close" not in out.columns:
        return None
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out["Close"] = pd.to_numeric(out["Close"], errors="coerce")
    out = out.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)
    return out if not out.empty else None


def get_equity_spot_snapshot(symbol: str | None) -> dict[str, Any]:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return {}
    now_ts = time.time()
    cached = EQUITY_SPOT_CACHE.get(symbol)
    if cached and now_ts - float(cached.get("ts") or 0.0) <= EQUITY_SPOT_TTL_SECONDS:
        return cached.get("data") or {}

    data: dict[str, Any] = {}
    hist_path = INTERMEDIARY_DIR / "Hist_Data" / f"{symbol}.feather"
    try:
        if hist_path.exists():
            hist = normalize_price_history(pd.read_feather(hist_path))
            if hist is not None and not hist.empty:
                last = hist.iloc[-1]
                data = {
                    "price": round(float(last["Close"]), 2),
                    "date": pd.Timestamp(last["Date"]).isoformat(),
                    "source": "local_hist",
                }
    except Exception:
        data = {}

    if not data:
        try:
            import yfinance as yf

            candidates = [symbol] if "." in symbol else [f"{symbol}.NS", f"{symbol}.BO"]
            for cand in candidates:
                df = yf.download(cand, period="10d", interval="1d", auto_adjust=False, progress=False)
                hist = normalize_price_history(df)
                if hist is None or hist.empty:
                    continue
                last = hist.iloc[-1]
                data = {
                    "price": round(float(last["Close"]), 2),
                    "date": pd.Timestamp(last["Date"]).isoformat(),
                    "source": cand,
                }
                break
        except Exception:
            data = {}

    EQUITY_SPOT_CACHE[symbol] = {"ts": now_ts, "data": data}
    return data


def metric_card(title: str, value: Any, subtitle: str | None = None) -> html.Div:
    val_str = friendly(value)
    # Color-code the value based on content
    val_style = {"fontSize": "20px", "fontWeight": "700", "marginTop": "4px", "fontFamily": "'JetBrains Mono', monospace"}
    if isinstance(value, (int, float)):
        if value > 0:
            val_style["color"] = BLOOMBERG_GREEN
        elif value < 0:
            val_style["color"] = BLOOMBERG_RED
        else:
            val_style["color"] = BLOOMBERG_ORANGE
    else:
        val_str_color = str(value).lower().strip()
        numeric_hint = val_str_color.replace(",", "").replace("₹", "").replace("%", "")
        if numeric_hint.startswith("+"):
            val_style["color"] = BLOOMBERG_GREEN
        elif numeric_hint.startswith("-"):
            val_style["color"] = BLOOMBERG_RED
        elif val_str_color in ("active", "running", "ok", "true", "yes"):
            val_style["color"] = BLOOMBERG_GREEN
        elif val_str_color in ("stopped", "failed", "error", "false", "no"):
            val_style["color"] = BLOOMBERG_RED
        elif "sell" in val_str_color or "down" in val_str_color:
            val_style["color"] = BLOOMBERG_RED
        elif "buy" in val_str_color or "up" in val_str_color:
            val_style["color"] = BLOOMBERG_GREEN
        else:
            val_style["color"] = BLOOMBERG_ORANGE
    return html.Div(
        [
            html.Div(title.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "1px", "fontWeight": "600"}),
            html.Div(val_str, style=val_style),
            html.Div(subtitle or "", style={"fontSize": "11px", "color": "#5a6a7a", "marginTop": "2px"}),
        ],
        style={**CARD_STYLE, "flex": "1", "minWidth": "150px"},
    )


def market_move_card(title: str, last: Any, change_pct: float | None, subtitle: str | None = None) -> html.Div:
    color = BLOOMBERG_ORANGE
    change_text = "-"
    if change_pct is not None:
        change_text = f"{change_pct:+.2f}%"
        color = BLOOMBERG_GREEN if change_pct > 0 else BLOOMBERG_RED if change_pct < 0 else BLOOMBERG_ORANGE
    last_text = friendly(last)
    return html.Div(
        [
            html.Div(title.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "1px", "fontWeight": "600"}),
            html.Div(change_text, style={"fontSize": "20px", "fontWeight": "700", "marginTop": "4px", "fontFamily": "'JetBrains Mono', monospace", "color": color}),
            html.Div(f"last {last_text}" + (f" | {subtitle}" if subtitle else ""), style={"fontSize": "11px", "color": "#5a6a7a", "marginTop": "2px"}),
        ],
        style={**CARD_STYLE, "flex": "1", "minWidth": "150px"},
    )


def section(title: str, children: list[Any], subtitle: str | None = None) -> html.Div:
    header = [html.Div(title.upper(), style={"fontSize": "12px", "fontWeight": "700", "color": BLOOMBERG_ORANGE, "letterSpacing": "1px", "borderBottom": f"1px solid {BLOOMBERG_ORANGE}", "paddingBottom": "4px", "marginBottom": "8px"})]
    if subtitle:
        header.append(html.Div(subtitle, style={"color": "#5a6a7a", "marginBottom": "8px", "fontSize": "11px"}))
    return html.Div(header + children, style={"marginTop": "14px"})


def empty_message(text: str) -> html.Div:
    return html.Div(text, style={**CARD_STYLE, "color": "#94a3b8"})


def json_block(obj: Any, title: str) -> html.Details:
    return html.Details(
        [
            html.Summary(title, style={"cursor": "pointer", "fontWeight": "600"}),
            html.Pre(json.dumps(obj, indent=2, default=str), style={"whiteSpace": "pre-wrap", "marginTop": "10px"}),
        ],
        style={**CARD_STYLE, "marginTop": "12px"},
    )


def _maybe_format_timeish_series(name: str, series: pd.Series, verbose: bool = False) -> pd.Series:
    lower = str(name).lower()
    is_timeish = any(token in lower for token in ["date", "time", "timestamp", "published", "generated", "updated", "_at"])
    if pd.api.types.is_datetime64_any_dtype(series):
        try:
            localized = series.dt.tz_convert(IST)
        except Exception:
            try:
                localized = series.dt.tz_localize("UTC").dt.tz_convert(IST)
            except Exception:
                localized = series
        return localized.dt.strftime("%b %d, %H:%M:%S IST" if verbose else "%b %d, %H:%M")
    if not is_timeish:
        return series
    parsed = series.apply(parse_timestamp_any)
    if parsed.notna().sum() == 0:
        return series
    return parsed.apply(lambda x: x.tz_convert(IST).strftime("%b %d, %H:%M:%S IST" if verbose else "%b %d, %H:%M") if x is not None and pd.notna(x) else "-")


def table_from_df(df: pd.DataFrame, table_id: str, page_size: int = 10) -> dash_table.DataTable | html.Div:
    if df.empty:
        return empty_message("No data yet.")
    show = df.copy()
    for col in show.columns:
        show[col] = _maybe_format_timeish_series(col, show[col], verbose=False)
    # Sanitize NaN/Inf — these cause Dash "Invalid value" errors
    show = show.fillna("-").replace([float("inf"), float("-inf")], "-")
    return dash_table.DataTable(
        id=table_id,
        data=show.to_dict("records"),
        columns=[{"name": c, "id": c} for c in show.columns],
        page_size=page_size,
        sort_action="native",
        filter_action="native",
        style_table=TABLE_STYLE,
        style_cell=TABLE_CELL_STYLE,
        style_header=TABLE_HEADER_STYLE,
    )


def load_combined_lab_table(limit: int = 20) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path, data in recent_strategy_reports(limit=limit):
        rec = data.get("recommendation") or {}
        best = rec.get("best") or {}
        baseline = rec.get("baseline") or {}
        rows.append(
            {
                "report": path.name,
                "source": "strategy_lab",
                "lab_type": rec.get("lab_type") or "strategy_lab",
                "generated_at": rec.get("generated_at"),
                "best_name": best.get("name"),
                "best_return_pct": best.get("total_return_pct"),
                "best_score": best.get("selection_score"),
                "best_drawdown_pct": best.get("max_drawdown_pct"),
                "baseline_return_pct": baseline.get("total_return_pct"),
                "tested_variants": rec.get("tested_variants"),
            }
        )
    for file_name in COMBINED_LAB_STATUS_FILES:
        payload = load_json(REPORTS_DIR / file_name)
        if not isinstance(payload, dict):
            continue
        rows.append(
            {
                "report": file_name,
                "source": file_name.removesuffix("_latest.json"),
                "lab_type": payload.get("message") or file_name.removesuffix("_latest.json"),
                "generated_at": payload.get("generated_at"),
                "best_name": payload.get("best_variant"),
                "best_return_pct": payload.get("best_return_pct"),
                "best_score": payload.get("best_score"),
                "best_drawdown_pct": payload.get("best_drawdown_pct"),
                "baseline_return_pct": None,
                "tested_variants": payload.get("variants_done") or payload.get("variants_total"),
            }
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    return df.dropna(subset=["generated_at"]).sort_values("generated_at")


def load_telegram_options_table(limit: int = 20) -> pd.DataFrame:
    rows = []
    for path, data in recent_telegram_options_reports(limit=limit):
        rows.append(
            {
                "report": path.name,
                "generated_at": data.get("generated_at"),
                "channel": data.get("channel"),
                "starting_capital": data.get("starting_capital"),
                "signals_found": data.get("signals_found"),
                "trades_simulated": data.get("trades_simulated"),
                "final_equity": data.get("final_equity"),
                "total_return_pct": data.get("total_return_pct"),
                "win_rate_pct": data.get("win_rate_pct"),
                "avg_trade_return_pct": data.get("avg_trade_return_pct"),
                "best_trade_pct": data.get("best_trade_pct"),
                "worst_trade_pct": data.get("worst_trade_pct"),
            }
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    return df.sort_values("generated_at")


def load_live_telegram_ledger() -> dict[str, Any]:
    data = load_json(LIVE_TELEGRAM_LEDGER_PATH)
    return data if isinstance(data, dict) else {}


def load_live_telegram_equity_history() -> pd.DataFrame:
    rows = load_jsonl(LIVE_TELEGRAM_LEDGER_HISTORY, limit=2000)
    if not rows:
        return pd.DataFrame(columns=["timestamp", "equity", "cash"])
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce")
    for col in ["equity", "cash"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])


def load_telegram_channel_updates(limit: int = 120) -> pd.DataFrame:
    rows = load_jsonl(WATCH_UPDATES_PATH, limit=MAX_JSONL_LINES)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for col in ["captured_at", "date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "text_excerpt" not in df.columns and "text" in df.columns:
        df["text_excerpt"] = df["text"].fillna("").astype(str).str.slice(0, 180)
    wanted = [c for c in ["captured_at", "chat", "message_id", "date", "source", "has_media", "media_kind", "media_path", "text_excerpt"] if c in df.columns]
    if wanted:
        df = df[wanted]
    sort_col = "captured_at" if "captured_at" in df.columns else "date"
    return df.sort_values(sort_col, ascending=False).head(limit)


def load_telegram_receipts(limit: int = 120) -> pd.DataFrame:
    rows = load_jsonl(WATCH_RECEIPTS_PATH, limit=MAX_JSONL_LINES)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for col in ["captured_at", "date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.sort_values("captured_at", ascending=False).head(limit) if "captured_at" in df.columns else df.head(limit)


def load_telegram_latest_by_chat(limit: int = 20) -> pd.DataFrame:
    updates = load_telegram_channel_updates(limit=500)
    if updates.empty or "chat" not in updates.columns:
        return pd.DataFrame()
    sort_col = "date" if "date" in updates.columns else "captured_at"
    latest = updates.sort_values(sort_col, ascending=False).drop_duplicates(subset=["chat"], keep="first")
    cols = [c for c in ["chat", "date", "captured_at", "source", "has_media", "media_kind", "text_excerpt"] if c in latest.columns]
    return latest[cols].sort_values(sort_col, ascending=False).head(limit) if cols else latest.head(limit)


def load_sentiment_rows() -> pd.DataFrame:
    payload = load_json(REPORTS_DIR / "news_sentiment_latest.json")
    active = payload.get("active") if isinstance(payload, dict) else []
    if not active:
        return pd.DataFrame()
    rows = []
    for item in active:
        rows.append(
            {
                "symbol": item.get("symbol"),
                "status": item.get("status"),
                "item_count": item.get("item_count"),
                "weighted_sentiment": item.get("weighted_sentiment"),
                "dominant_types": ", ".join(item.get("dominant_types") or []),
                "block_buy": (item.get("trade_bias") or {}).get("block_buy"),
                "force_sell": (item.get("trade_bias") or {}).get("force_sell"),
                "sample_headlines": " | ".join((item.get("sample_headlines") or [])[:3]),
            }
        )
    return pd.DataFrame(rows)


def load_market_topics_rows() -> pd.DataFrame:
    payload = load_json(REPORTS_DIR / "market_topics_latest.json")
    topics = payload.get("topics") if isinstance(payload, dict) else []
    if not topics:
        return pd.DataFrame()
    rows = []
    for item in topics:
        rows.append(
            {
                "topic": item.get("topic"),
                "label": item.get("label"),
                "status": item.get("status"),
                "item_count": item.get("item_count"),
                "weighted_sentiment": item.get("weighted_sentiment"),
                "dominant_types": ", ".join(item.get("dominant_types") or []),
                "sample_headlines": " | ".join((item.get("sample_headlines") or [])[:3]),
            }
        )
    return pd.DataFrame(rows)


def load_telegram_trade_audit() -> dict[str, Any]:
    now = time.time()
    cached = TELEGRAM_AUDIT_CACHE.get("data")
    if cached is not None and now - float(TELEGRAM_AUDIT_CACHE.get("ts", 0.0)) < 20:
        return cached if isinstance(cached, dict) else {}

    audit_mtime = TELEGRAM_TRADE_AUDIT_PATH.stat().st_mtime if TELEGRAM_TRADE_AUDIT_PATH.exists() else 0.0
    watch_mtime = WATCH_UPDATES_PATH.stat().st_mtime if WATCH_UPDATES_PATH.exists() else 0.0
    if watch_mtime > audit_mtime + 2:
        try:
            subprocess.run(
                [str(ROOT / "venv" / "bin" / "python"), str(ROOT / "scripts" / "generate_telegram_trade_audit.py")],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=240,
            )
        except Exception:
            pass

    payload = load_json(TELEGRAM_TRADE_AUDIT_PATH)
    payload = payload if isinstance(payload, dict) else {}
    TELEGRAM_AUDIT_CACHE["ts"] = now
    TELEGRAM_AUDIT_CACHE["data"] = payload
    return payload


def load_global_macro() -> dict[str, Any]:
    now = time.time()
    cached = GLOBAL_MACRO_CACHE.get("data")
    if cached is not None and now - float(GLOBAL_MACRO_CACHE.get("ts", 0.0)) < GLOBAL_MACRO_TTL_SECONDS:
        return cached

    payload = load_json(GLOBAL_MACRO_PATH)
    file_fresh = False
    if GLOBAL_MACRO_PATH.exists():
        try:
            file_fresh = now - GLOBAL_MACRO_PATH.stat().st_mtime < GLOBAL_MACRO_TTL_SECONDS
        except Exception:
            file_fresh = False

    if not payload or not file_fresh:
        try:
            subprocess.run(
                [str(ROOT / "venv" / "bin" / "python"), str(ROOT / "scripts" / "fetch_global_macro.py")],
                capture_output=True,
                text=True,
                timeout=150,
            )
            payload = load_json(GLOBAL_MACRO_PATH)
        except Exception:
            payload = payload or {}

    out = payload if isinstance(payload, dict) else {}
    GLOBAL_MACRO_CACHE.update({"ts": now, "data": out})
    return out


def load_eco_calendar() -> dict[str, Any]:
    now = time.time()
    cached = ECO_CALENDAR_CACHE.get("data")
    if cached is not None and now - float(ECO_CALENDAR_CACHE.get("ts", 0.0)) < ECO_CALENDAR_TTL_SECONDS:
        return cached

    payload = load_json(ECO_CALENDAR_PATH)
    file_fresh = False
    if ECO_CALENDAR_PATH.exists():
        try:
            file_fresh = now - ECO_CALENDAR_PATH.stat().st_mtime < ECO_CALENDAR_TTL_SECONDS
        except Exception:
            file_fresh = False

    if not payload or not file_fresh:
        try:
            subprocess.run(
                [str(ROOT / "venv" / "bin" / "python"), str(ROOT / "scripts" / "fetch_economic_calendar_sectors.py")],
                capture_output=True,
                text=True,
                timeout=300,
            )
            payload = load_json(ECO_CALENDAR_PATH)
        except Exception:
            payload = payload or {}

    out = payload if isinstance(payload, dict) else {}
    ECO_CALENDAR_CACHE.update({"ts": now, "data": out})
    return out


def load_event_pipelines() -> dict[str, Any]:
    now = time.time()
    cached = EVENT_PIPELINES_CACHE.get("data")
    if cached is not None and now - float(EVENT_PIPELINES_CACHE.get("ts", 0.0)) < 30:
        return cached if isinstance(cached, dict) else {}

    news_payload = load_json(NEWS_BEHAVIOR_PATH) or {}
    earnings_payload = load_json(EARNINGS_PIPELINE_PATH) or {}

    output_mtime = max(
        NEWS_BEHAVIOR_PATH.stat().st_mtime if NEWS_BEHAVIOR_PATH.exists() else 0.0,
        EARNINGS_PIPELINE_PATH.stat().st_mtime if EARNINGS_PIPELINE_PATH.exists() else 0.0,
    )
    input_mtime = max(
        (REPORTS_DIR / "market_topics_latest.json").stat().st_mtime if (REPORTS_DIR / "market_topics_latest.json").exists() else 0.0,
        ECO_CALENDAR_PATH.stat().st_mtime if ECO_CALENDAR_PATH.exists() else 0.0,
        (REPORTS_DIR / "news_sentiment_latest.json").stat().st_mtime if (REPORTS_DIR / "news_sentiment_latest.json").exists() else 0.0,
    )
    if not news_payload or not earnings_payload or output_mtime + EVENT_PIPELINES_TTL_SECONDS < now or input_mtime > output_mtime + 2:
        try:
            subprocess.run(
                [str(ROOT / "venv" / "bin" / "python"), str(ROOT / "scripts" / "generate_market_event_pipelines.py")],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=300,
            )
            news_payload = load_json(NEWS_BEHAVIOR_PATH) or news_payload or {}
            earnings_payload = load_json(EARNINGS_PIPELINE_PATH) or earnings_payload or {}
        except Exception:
            pass

    out = {
        "news_behavior": news_payload if isinstance(news_payload, dict) else {},
        "earnings_pipeline": earnings_payload if isinstance(earnings_payload, dict) else {},
    }
    EVENT_PIPELINES_CACHE.update({"ts": now, "data": out})
    return out


def load_portfolio_tracker() -> dict[str, Any]:
    now = time.time()
    cached = PORTFOLIO_TRACKER_CACHE.get("data")
    if cached is not None and now - float(PORTFOLIO_TRACKER_CACHE.get("ts", 0.0)) < 60:
        return cached if isinstance(cached, dict) else {}
    payload = load_json(PORTFOLIO_TRACKER_PATH) or {}
    if not payload:
        try:
            subprocess.run(
                [str(ROOT / "venv" / "bin" / "python"), str(ROOT / "scripts" / "generate_portfolio_tracker.py")],
                cwd=str(ROOT), capture_output=True, text=True, timeout=60,
            )
            payload = load_json(PORTFOLIO_TRACKER_PATH) or {}
        except Exception:
            pass
    PORTFOLIO_TRACKER_CACHE.update({"ts": now, "data": payload})
    return payload if isinstance(payload, dict) else {}


def recent_report_files(limit: int = 80) -> pd.DataFrame:
    rows = []
    for path in REPORTS_DIR.iterdir():
        if not path.is_file():
            continue
        stat = path.stat()
        rows.append(
            {
                "file": path.name,
                "suffix": path.suffix,
                "modified_at": datetime.fromtimestamp(stat.st_mtime),
                "size_kb": round(stat.st_size / 1024, 1),
            }
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.sort_values("modified_at", ascending=False).head(limit)


def fetch_server_snapshot(force: bool = False) -> dict[str, Any]:
    now = time.time()
    if not force and SERVER_CACHE.get("data") is not None and now - SERVER_CACHE.get("ts", 0.0) < SSH_TTL_SECONDS:
        return SERVER_CACHE["data"]
    if not SERVER_KEY.exists():
        data = {"ok": False, "error": f"Missing SSH key: {SERVER_KEY}"}
        SERVER_CACHE.update({"ts": now, "data": data})
        return data
    remote = f"""
set -e
cd {SERVER_REPO}
echo '[host]'
hostname
echo '[time]'
date -Is
echo '[service]'
systemctl is-active auto_trade.service || true
echo '[substate]'
systemctl show auto_trade.service --property=SubState --value || true
echo '[pid]'
systemctl show auto_trade.service --property=ExecMainPID --value || true
echo '[restarts]'
systemctl show auto_trade.service --property=NRestarts --value || true
echo '[active_since]'
systemctl show auto_trade.service --property=ActiveEnterTimestamp --value || true
echo '[recent_reports]'
ls -1t reports | head -20 || true
echo '[journal]'
journalctl -u auto_trade.service -n 15 --no-pager || true
"""
    try:
        proc = subprocess.run(
            [
                "ssh",
                "-i",
                str(SERVER_KEY),
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ConnectTimeout=10",
                SERVER_HOST,
                remote,
            ],
            capture_output=True,
            text=True,
            timeout=25,
        )
        stdout = proc.stdout or ""
        sections: dict[str, list[str]] = {}
        current: str | None = None
        for line in stdout.splitlines():
            if line.startswith("[") and line.endswith("]"):
                current = line.strip("[]")
                sections[current] = []
            elif current:
                sections[current].append(line)
        data = {
            "ok": proc.returncode == 0,
            "stdout": stdout,
            "stderr": proc.stderr,
            "host": "\n".join(sections.get("host", [])).strip(),
            "time": "\n".join(sections.get("time", [])).strip(),
            "service": "\n".join(sections.get("service", [])).strip(),
            "substate": "\n".join(sections.get("substate", [])).strip(),
            "pid": "\n".join(sections.get("pid", [])).strip(),
            "restarts": "\n".join(sections.get("restarts", [])).strip(),
            "active_since": "\n".join(sections.get("active_since", [])).strip(),
            "recent_reports": sections.get("recent_reports", []),
            "journal": sections.get("journal", []),
        }
    except Exception as exc:
        data = {"ok": False, "error": str(exc)}
    SERVER_CACHE.update({"ts": now, "data": data})
    return data


def collect_data() -> dict[str, Any]:
    # Periodic JSONL compaction
    global LAST_COMPACT_TS
    now = time.time()
    if now - LAST_COMPACT_TS > COMPACT_INTERVAL_SECONDS:
        try:
            compact_all_jsonl()
        except Exception:
            pass
        LAST_COMPACT_TS = now

    scorecard_path, scorecard = latest_report("daily_scorecard_*.json")
    ops_path, daily_ops = latest_report("daily_ops_supervisor_*.json")
    portfolio_path, portfolio = latest_report("portfolio_intel_*.json")
    options_supervisor_path, options_supervisor = latest_report("options_research_supervisor_*.json")
    improvement_path, improvement = latest_report("daily_improvement_audit_*.json")
    five_year_path, five_year = latest_report("five_year_validation_*.json")
    exposure_sweep_path, exposure_sweep = latest_report("five_year_exposure_sweep_*.json")
    promoted_validation_path, promoted_validation = latest_report("five_year_validate_*.json")
    rulesets_path, telegram_rulesets = latest_report("telegram_channel_rulesets_*.json")
    paper = load_json(REPORTS_DIR / "paper_shadow_latest.json") or {}
    oracle_paper = load_json(REPORTS_DIR / "oracle_paper_shadow_latest.json") or {}
    live_paper = load_json(REPORTS_DIR / "paper_shadow_live_latest.json") or {}
    options_paper = load_json(REPORTS_DIR / "paper_shadow_options_latest.json") or {}
    news_payload = load_json(REPORTS_DIR / "news_sentiment_latest.json") or {}
    topics_payload = load_json(REPORTS_DIR / "market_topics_latest.json") or {}
    event_pipelines = load_event_pipelines()
    portfolio_tracker = load_portfolio_tracker()
    hourly_lab = load_json(REPORTS_DIR / "hourly_lab_status_latest.json") or {}
    lab_status = load_json(LAB_STATUS_PATH) or {}
    combined_labs = load_combined_lab_table(limit=30)
    telegram_ledger = load_live_telegram_ledger()
    telegram_history = load_live_telegram_equity_history()
    telegram_backtests = load_telegram_options_table(limit=30)
    telegram_updates = load_telegram_channel_updates(limit=120)
    telegram_latest_by_chat = load_telegram_latest_by_chat(limit=20)
    telegram_receipts = load_telegram_receipts(limit=120)
    server = fetch_server_snapshot()
    reports_df = recent_report_files(limit=120)
    return {
        "generated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "scorecard_path": scorecard_path.name if scorecard_path else None,
        "scorecard": scorecard or {},
        "daily_ops_path": ops_path.name if ops_path else None,
        "daily_ops": daily_ops or {},
        "portfolio_path": portfolio_path.name if portfolio_path else None,
        "portfolio": portfolio or {},
        "options_supervisor_path": options_supervisor_path.name if options_supervisor_path else None,
        "options_supervisor": options_supervisor or {},
        "improvement_path": improvement_path.name if improvement_path else None,
        "improvement": improvement or {},
        "five_year_path": five_year_path.name if five_year_path else None,
        "five_year": five_year or {},
        "exposure_sweep_path": exposure_sweep_path.name if exposure_sweep_path else None,
        "exposure_sweep": exposure_sweep or {},
        "rulesets_path": rulesets_path.name if rulesets_path else None,
        "promoted_validation_path": promoted_validation_path.name if promoted_validation_path else None,
        "promoted_validation": promoted_validation or {},
        "telegram_rulesets": telegram_rulesets or {},
        "paper": paper,
        "oracle_paper": oracle_paper,
        "live_paper": live_paper,
        "options_paper": options_paper,
        "news_payload": news_payload,
        "topics_payload": topics_payload,
        "news_behavior": event_pipelines.get("news_behavior") or {},
        "earnings_pipeline": event_pipelines.get("earnings_pipeline") or {},
        "portfolio_tracker": portfolio_tracker,
        "hourly_lab": hourly_lab,
        "lab_status": lab_status,
        "combined_labs": combined_labs,
        "telegram_ledger": telegram_ledger,
        "telegram_history": telegram_history,
        "telegram_backtests": telegram_backtests,
        "telegram_updates": telegram_updates,
        "telegram_latest_by_chat": telegram_latest_by_chat,
        "telegram_receipts": telegram_receipts,
        "telegram_trade_audit": load_telegram_trade_audit(),
        "server": server,
        "sentiment_df": load_sentiment_rows(),
        "topics_df": load_market_topics_rows(),
        "reports_df": reports_df,
    }


def build_hero(data: dict[str, Any]) -> list[Any]:
    combined = data["combined_labs"]
    latest_lab = combined.iloc[-1].to_dict() if not combined.empty else {}
    server = data["server"]
    exposure_best = ((data.get("exposure_sweep") or {}).get("best") or {})
    validated_cagr = exposure_best.get("cagr_pct") or (data["five_year"].get("vol_sizing") or {}).get("cagr_pct", "-")
    validated_name = exposure_best.get("name") or data.get("exposure_sweep_path") or data.get("five_year_path") or "5y validation"
    # Hunt status for hero
    hunt_path = ROOT / "reports" / "thirty_cagr_hunt_latest.json"
    hunt_data = None
    if hunt_path.exists():
        try:
            hunt_data = json.loads(hunt_path.read_text())
        except Exception:
            pass
    hunt_cagr = (hunt_data or {}).get("best_cagr_pct")
    hunt_status_short = ((hunt_data or {}).get("status") or "idle").upper()[:6]
    return [
        metric_card("auto_trade.service", server.get("service", "unknown"), server.get("substate", "")),
        metric_card("Paper decision", data.get("oracle_paper", {}).get("decision", data["paper"].get("decision", data["live_paper"].get("mode", "-"))), data.get("oracle_paper", {}).get("symbol", data["paper"].get("symbol", "paper shadow"))),
        metric_card("Telegram paper equity", data["telegram_ledger"].get("equity", "-"), f"cash {friendly(data['telegram_ledger'].get('cash'))}"),
        metric_card("Live portfolio value", data["portfolio"].get("total_value", "-"), data.get("portfolio_path") or "portfolio_intel"),
        metric_card("Latest lab return %", latest_lab.get("best_return_pct", "-"), latest_lab.get("best_name", "-")),
        metric_card("Best validated 5y CAGR %", validated_cagr, validated_name),
        metric_card("30% hunt CAGR %", hunt_cagr if hunt_cagr is not None else "-", hunt_status_short),
    ]


def build_overview_tab(data: dict[str, Any]) -> list[Any]:
    children: list[Any] = []
    combined = data["combined_labs"]
    telegram_history = data["telegram_history"]
    scorecard = data["scorecard"]
    daily_ops = data["daily_ops"]
    hourly = data["hourly_lab"]
    five_year = data["five_year"]

    top_cards = html.Div(
        [
            metric_card("Scorecard verdict", scorecard.get("verdict", "-"), data.get("scorecard_path") or "daily scorecard"),
            metric_card("Orders", scorecard.get("orders", "-"), "latest scorecard"),
            metric_card("Trades", scorecard.get("trades", "-"), "latest scorecard"),
            metric_card("Ops strategy test", (daily_ops.get("strategy_test") or {}).get("ok", "-"), data.get("daily_ops_path") or "daily ops"),
            metric_card("Lab status", (hourly.get("status") or {}).get("status", "-"), (hourly.get("status") or {}).get("message", "")),
            metric_card("5y return %", (five_year.get("vol_sizing") or {}).get("return_pct", "-"), "vol sizing validation"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Mission control", [top_cards], "The fastest read on service health, current paper state, lab status, and 5 year validation."))

    if not combined.empty:
        ordered = combined.sort_values("generated_at")
        line_cols = [c for c in ["best_return_pct", "baseline_return_pct"] if c in ordered.columns]
        fig = px.line(ordered, x="generated_at", y=line_cols, markers=True, title="Completed lab returns")
        fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
        scatter = px.scatter(
            ordered,
            x="best_drawdown_pct",
            y="best_return_pct",
            color="source",
            size=(ordered.get("tested_variants", pd.Series([1] * len(ordered))).fillna(1) + 0.1),
            hover_name="best_name",
            title="Lab winners, return vs drawdown",
        )
        scatter.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
        children.append(section("Research trajectory", [dcc.Graph(figure=fig), dcc.Graph(figure=scatter)]))
    else:
        children.append(section("Research trajectory", [empty_message("No completed lab reports yet.")]))

    if not telegram_history.empty:
        fig = px.line(telegram_history, x="timestamp", y=[c for c in ["equity", "cash"] if c in telegram_history.columns], markers=True, title="Telegram paper equity")
        fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
        children.append(section("Paper capital curve", [dcc.Graph(figure=fig)]))

    # Clean summary cards instead of raw JSON
    ops_summary_items = []
    for label, val in [
        ('Trade date', daily_ops.get('trade_date')),
        ('Market open', daily_ops.get('market_open')),
        ('Strategy OK', (daily_ops.get('strategy_test') or {}).get('ok')),
        ('Paper executed', (daily_ops.get('paper_trader') or {}).get('paper_executed')),
        ('Paper decision', (daily_ops.get('paper_trader') or {}).get('decision')),
    ]:
        if val is not None:
            ops_summary_items.append(html.Div([html.Span(label.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY}), html.Span(f" {val}", style={"fontSize": "13px", "fontWeight": "700"})], style={"display": "inline-block", "marginRight": "16px"}))
    if ops_summary_items:
        children.append(section("Daily ops summary", [html.Div(ops_summary_items, style=CARD_STYLE)]))

    hourly_summary_items = []
    hourly_data = hourly or {}
    hourly_status = (hourly_data.get('status') or {}).get('status', '-')
    hourly_msg = (hourly_data.get('status') or {}).get('message', '')
    hourly_summary_items.append(html.Div([
        html.Span(f"STATUS: {hourly_status}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_GREEN if hourly_status == 'running' else BLOOMBERG_ORANGE}),
        html.Span(f"  {hourly_msg}", style={"fontSize": "11px", "color": "#5a6a7a"}),
    ]))
    children.append(section("Lab status", hourly_summary_items))
    return children


def build_runtime_tab(data: dict[str, Any]) -> list[Any]:
    server = data["server"]
    daily_ops = data["daily_ops"]
    scorecard = data["scorecard"]
    options_supervisor = data["options_supervisor"]
    children: list[Any] = []

    service_cards = html.Div(
        [
            metric_card("Service state", server.get("service", "unknown"), server.get("substate", "")),
            metric_card("Server host", server.get("host", "-"), SERVER_HOST),
            metric_card("Service PID", server.get("pid", "-"), f"restarts {server.get('restarts', '-') }"),
            metric_card("Active since", server.get("active_since", "-"), server.get("time", "")),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("auto_trade runtime", [service_cards], f"Remote host {SERVER_HOST}, repo {SERVER_REPO}"))

    children.append(section("Recent server reports", [table_from_df(to_df([{"report": x} for x in server.get("recent_reports", [])]), "server-reports", page_size=10)]))
    children.append(section("Latest scorecard", [table_from_df(to_df([scorecard]), "runtime-scorecard", page_size=5)]))
    # Daily ops and options supervisor as clean summary cards
    ops_data = daily_ops or {}
    ops_items = []
    for label, val in [('Trade date', ops_data.get('trade_date')), ('Market open', ops_data.get('market_open')), ('Strategy OK', (ops_data.get('strategy_test') or {}).get('ok')), ('Paper decision', (ops_data.get('paper_trader') or {}).get('decision'))]:
        if val is not None:
            ops_items.append(html.Div([html.Span(label.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY}), html.Span(f" {val}", style={"fontSize": "13px", "fontWeight": "700"})], style={"display": "inline-block", "marginRight": "16px"}))
    if ops_items:
        children.append(section("Daily ops", [html.Div(ops_items, style=CARD_STYLE)]))

    if server.get("journal"):
        children.append(section("Recent service logs", [html.Pre("\n".join(server["journal"][-8:]), style={**CARD_STYLE, "whiteSpace": "pre-wrap", "overflowX": "auto", "fontSize": "11px", "fontFamily": "'JetBrains Mono', monospace", "color": "#8899aa"})]))
    return children


def build_portfolio_tab(data: dict[str, Any]) -> list[Any]:
    portfolio = data["portfolio"]
    live_ledger = data["telegram_ledger"]
    children: list[Any] = []
    portfolio_cards = html.Div(
        [
            metric_card("Total value", portfolio.get("total_value", "-"), data.get("portfolio_path") or "portfolio_intel"),
            metric_card("Total cost", portfolio.get("total_cost", "-"), "live holdings"),
            metric_card("PnL", portfolio.get("total_pnl", "-"), f"{friendly(portfolio.get('total_pnl_pct'))}%"),
            metric_card("Risk score", portfolio.get("risk_score", "-"), "portfolio intel"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Live portfolio", [portfolio_cards]))

    alloc = portfolio.get("current_allocation") or {}
    if alloc:
        alloc_df = pd.DataFrame({"bucket": list(alloc.keys()), "weight": list(alloc.values())})
        fig = px.pie(alloc_df, names="bucket", values="weight", title="Current allocation mix")
        fig.update_layout(template="plotly_dark", paper_bgcolor="#030712")
        children.append(section("Allocation mix", [dcc.Graph(figure=fig)]))

    rebalance = portfolio.get("rebalance_advice_inr") or {}
    if rebalance:
        rebalance_df = pd.DataFrame({"asset_class": list(rebalance.keys()), "rebalance_inr": list(rebalance.values())})
        children.append(section("Rebalance advice", [table_from_df(rebalance_df, "rebalance-table", page_size=6)]))

    headlines = portfolio.get("risk_headlines") or []
    if headlines:
        children.append(section("Portfolio risk headlines", [html.Ul([html.Li(x) for x in headlines[:8]], style=CARD_STYLE)]))

    live_cards = html.Div(
        [
            metric_card("Paper cash", live_ledger.get("cash", "-"), "Telegram paper"),
            metric_card("Paper equity", live_ledger.get("equity", "-"), "Telegram paper"),
            metric_card("Unrealized PnL", live_ledger.get("unrealized_pnl", "-"), "Telegram paper"),
            metric_card("Positions tracked", live_ledger.get("positions_tracked", "-"), "Telegram paper"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Paper portfolio", [live_cards], "Your live Telegram paper portfolio sits here so you can compare it against the real one quickly."))

    # ── PORTFOLIO TRACKER: MF + recommendations ──
    tracker = data.get("portfolio_tracker") or {}
    mf_holdings = tracker.get("mf_holdings") or []
    eq_holdings = tracker.get("equity_holdings") or []
    psum = tracker.get("portfolio_summary") or {}
    if tracker:
        tracker_cards = html.Div(
            [
                metric_card("MF value", friendly(psum.get("mf_value")), f"{psum.get('n_mf_holdings', 0)} funds"),
                metric_card("MF weight", friendly(psum.get("mf_weight_pct")), "% of portfolio"),
                metric_card("Equity value", friendly(psum.get("equity_value")), f"{psum.get('n_equity_holdings', 0)} holdings"),
                metric_card("Equity weight", friendly(psum.get("equity_weight_pct")), "% of portfolio"),
            ],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        )
        children.append(section("Portfolio tracker (equity + MF)", [tracker_cards], "Live holdings from Kite with buy/sell/hold recommendations."))

    # Category allocation table
    cat_breakdown = tracker.get("category_breakdown") or {}
    if cat_breakdown:
        cat_rows = []
        for cat, vals in sorted(cat_breakdown.items(), key=lambda x: -x[1]["value"]):
            pct = vals["value"] / (psum.get("total_value") or 1) * 100
            gain = ((vals["value"] - vals["cost"]) / vals["cost"] * 100) if vals["cost"] > 0 else 0
            cat_rows.append({"category": cat, "value_inr": round(vals["value"]), "weight_pct": round(pct, 1), "gain_pct": round(gain, 1)})
        children.append(section("Category allocation", [table_from_df(pd.DataFrame(cat_rows), "category-alloc-table", page_size=15)]))

    # MF recommendations table
    if mf_holdings:
        mf_rows = []
        for m in sorted(mf_holdings, key=lambda x: -x.get("weight_pct", 0)):
            fund_short = (m.get("fund") or m.get("tradingsymbol", "?"))[:50]
            mf_rows.append({
                "fund": fund_short,
                "recommend": m.get("recommendation", "-"),
                "category": m.get("category", "?"),
                "risk": m.get("risk_level", "?"),
                "value_inr": m.get("current_value", 0),
                "weight_pct": m.get("weight_pct", 0),
                "gain_pct": m.get("gain_pct", 0),
                "rationale": (m.get("rationale") or "")[:80],
            })
        children.append(section("MF holdings + recommendations", [table_from_df(pd.DataFrame(mf_rows), "mf-recommendations-table", page_size=15)], "Buy/sell/hold based on P\u0026L, risk, category outlook, and portfolio concentration."))

    # Equity recommendations table
    if eq_holdings:
        eq_rows = []
        for h in eq_holdings:
            eq_rows.append({
                "symbol": h.get("tradingsymbol", "?"),
                "recommend": h.get("recommendation", "-"),
                "value_inr": h.get("current_value", 0),
                "weight_pct": h.get("weight_pct", 0),
                "gain_pct": h.get("gain_pct", 0),
                "rationale": (h.get("rationale") or "")[:80],
            })
        children.append(section("Equity recommendations", [table_from_df(pd.DataFrame(eq_rows), "eq-recommendations-table", page_size=10)]))

    return children


def build_paper_tab(data: dict[str, Any]) -> list[Any]:
    paper = data["paper"]
    oracle_paper = data.get("oracle_paper") or {}
    live_paper = data["live_paper"]
    options_paper = data["options_paper"]
    ledger = data["telegram_ledger"]
    promoted_validation = data.get("promoted_validation") or {}
    children: list[Any] = []

    active_paper = {**(paper or {}), **(oracle_paper or {})}
    shadow_portfolio = active_paper.get("shadow_portfolio") or {}
    cards = html.Div(
        [
            metric_card("Oracle paper decision", active_paper.get("decision", "-"), active_paper.get("symbol", "paper shadow")),
            metric_card("Oracle paper updated", to_ist(active_paper.get("generated_at")), "latest shadow snapshot"),
            metric_card("Shadow equity", friendly(shadow_portfolio.get("equity")), f"return {friendly(shadow_portfolio.get('total_return_pct'))}%"),
            metric_card("Shadow qty", shadow_portfolio.get("quantity", "-"), f"cash {friendly(shadow_portfolio.get('cash'))}"),
            metric_card("Live paper mode", live_paper.get("mode", "-"), live_paper.get("time", "")),
            metric_card("Options candidates", len(options_paper.get("buy_candidates") or []), "paper shadow options"),
            metric_card("Options near misses", len(options_paper.get("near_miss_candidates") or []), "paper shadow options"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Paper trading state", [cards]))

    open_df = to_df(ledger.get("open_positions") or [])
    closed_df = to_df(ledger.get("closed_positions") or [])
    near_miss_df = to_df((options_paper.get("near_miss_candidates") or [])[:20])

    paper_items = []
    if active_paper:
        for label, key, fmt in [
            ('Decision', 'decision', str),
            ('Symbol', 'symbol', str),
            ('Mode', 'mode', str),
            ('Position qty', 'position_qty', lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)),
            ('Broker qty', 'broker_position_qty', lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x)),
        ]:
            val = active_paper.get(key)
            if val is not None:
                paper_items.append(html.Div([html.Span(label.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px"}), html.Span(f" {fmt(val)}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE})], style={"display": "inline-block", "marginRight": "16px"}))
        paper_items.append(html.Div([html.Span("UPDATED".upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px"}), html.Span(f" {to_ist_verbose(active_paper.get('generated_at'))}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE})], style={"display": "inline-block", "marginRight": "16px"}))
    if live_paper:
        for label, key in [('Live mode', 'mode'), ('Time', 'time')]:
            val = live_paper.get(key)
            if val:
                paper_items.append(html.Div([html.Span(label.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px"}), html.Span(f" {val}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE})], style={"display": "inline-block", "marginRight": "16px"}))
    if paper_items:
        children.append(section("Paper snapshot", [html.Div(paper_items, style={**CARD_STYLE, "display": "flex", "flexWrap": "wrap", "gap": "8px"})], "Oracle paper shadow is separate from the Telegram paper ledger. Service restarts update rules immediately, but the shadow snapshot only changes when paper_shadow.py runs."))

    if shadow_portfolio:
        shadow_cards = html.Div(
            [
                metric_card("Starting capital", friendly(shadow_portfolio.get("starting_capital")), "shadow book"),
                metric_card("Current equity", friendly(shadow_portfolio.get("equity")), f"MTM {friendly(shadow_portfolio.get('market_value'))}"),
                metric_card("Total PnL", friendly(shadow_portfolio.get("total_pnl")), f"{friendly(shadow_portfolio.get('total_return_pct'))}%"),
                metric_card("Realized", friendly(shadow_portfolio.get("realized_pnl")), "booked"),
                metric_card("Unrealized", friendly(shadow_portfolio.get("unrealized_pnl")), "open MTM"),
                metric_card("Last action", shadow_portfolio.get("last_action", "-"), to_ist(shadow_portfolio.get("last_action_at"))),
            ],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        )
        children.append(section("Oracle shadow portfolio, ₹1L book", [shadow_cards], "This is the simulated live paper-shadow book, independent of broker holdings, so you can see what the promoted rules would have done with ₹1,00,000."))

    validation = promoted_validation.get("validation") or {}
    variant = promoted_validation.get("variant") or {}
    if validation:
        validation_cards = html.Div(
            [
                metric_card("Promoted config", variant.get("name", "-"), data.get("promoted_validation_path") or "5y validate"),
                metric_card("1L starting capital", friendly(validation.get("starting_capital")), "5Y live-parity validation"),
                metric_card("1L final equity", friendly(validation.get("final_equity")), f"return {friendly(validation.get('return_pct'))}%"),
                metric_card("CAGR %", validation.get("cagr_pct", "-"), f"DD {friendly(validation.get('drawdown_pct'))}%"),
                metric_card("Trades", validation.get("trades", "-"), f"win {friendly(validation.get('win_rate_pct'))}%"),
            ],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        )
        config_items = []
        for block_name, block in [("buy", variant.get("buy") or {}), ("sell", variant.get("sell") or {}), ("env", variant.get("env") or {})]:
            if not block:
                continue
            config_items.append(html.Div([
                html.Div(block_name.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "1px", "marginBottom": "6px"}),
                html.Pre(json.dumps(block, indent=2), style={"margin": 0, "whiteSpace": "pre-wrap", "fontSize": "11px", "color": "#d1d5db", "fontFamily": "'JetBrains Mono', monospace"}),
            ], style={**CARD_STYLE, "flex": "1", "minWidth": "220px"}))
        children.append(section("Promoted rule validation, ₹1L capital", [validation_cards] + config_items, "This is a fresh 5Y live-parity validation of the currently promoted Oracle paper config with starting capital fixed at ₹1,00,000."))

    children.append(section("Open Telegram paper positions", [table_from_df(open_df, "paper-open-table", page_size=10)]))
    children.append(section("Closed Telegram paper positions", [table_from_df(closed_df, "paper-closed-table", page_size=10)]))
    children.append(section("Options paper near misses", [table_from_df(near_miss_df, "paper-nearmiss-table", page_size=10)]))
    return children


def compute_market_prediction(data: dict[str, Any]) -> dict[str, Any]:
    """Synthesize news sentiment + macro into a next-day market direction prediction."""
    news_payload = data.get("news_payload") or {}
    topics_payload = data.get("topics_payload") or {}
    macro = load_global_macro()

    # 1. Symbol sentiment
    active = news_payload.get("active") or []
    bull_count = sum(1 for a in active if (a.get("weighted_sentiment") or 0) > 0.1)
    bear_count = sum(1 for a in active if (a.get("weighted_sentiment") or 0) < -0.1)
    neutral_count = len(active) - bull_count - bear_count
    avg_sent = 0.0
    if active:
        avg_sent = sum(a.get("weighted_sentiment", 0) or 0 for a in active) / len(active)

    # 2. Topic sentiment
    topics = topics_payload.get("topics") or []
    topic_sent = 0.0
    if topics:
        topic_sent = sum(t.get("weighted_sentiment", 0) or 0 for t in topics) / len(topics)
    risk_topics = sum(1 for t in topics if (t.get("weighted_sentiment") or 0) < -0.1)
    bull_topics = sum(1 for t in topics if (t.get("weighted_sentiment") or 0) > 0.1)

    # 3. Macro drivers & events
    drivers = (macro.get("drivers") or []) if macro else []
    events = (macro.get("events") or []) if macro else []
    risk_drivers = sum(1 for d in drivers if d.get("impact") == "risk_off")
    risk_events = sum(1 for e in events if (e.get("severity") or 0) >= 35 and (e.get("weighted_sentiment") or 0) < -0.1)
    total_driver_strength = sum(d.get("strength", 0) or 0 for d in drivers)
    risk_strength = sum(d.get("strength", 0) or 0 for d in drivers if d.get("impact") == "risk_off")

    # 4. Region breadth
    regions = (macro.get("region_summary") or []) if macro else []
    asia_change = 0.0
    for r in regions:
        if r.get("region") == "Asia":
            asia_change = r.get("avg_change_pct", 0) or 0

    # 5. Score computation (scale: -100 to +100)
    # Symbol sentiment component (weight: 25)
    sym_score = avg_sent * 25 / 0.3  # normalize so 0.3 sentiment → full score
    # Topic sentiment component (weight: 20)
    top_score = topic_sent * 20 / 0.3
    # Macro risk component (weight: 30) — more risk drivers = more negative
    if total_driver_strength > 0:
        risk_ratio = risk_strength / total_driver_strength
    else:
        risk_ratio = 0.5
    macro_score = -(risk_ratio - 0.3) * 30 / 0.5  # 0.3 ratio is neutral
    # Region breadth component (weight: 15)
    region_score = asia_change * 15 / 2.0  # 2% move = full score
    # Bull vs bear count component (weight: 10)
    if bull_count + bear_count > 0:
        breadth_score = (bull_count - bear_count) / (bull_count + bear_count) * 10
    else:
        breadth_score = 0.0

    raw_score = sym_score + top_score + macro_score + region_score + breadth_score
    score = max(-100, min(100, raw_score))

    # Direction label
    if score > 15:
        direction = "BULLISH"
        color = BLOOMBERG_GREEN
    elif score < -15:
        direction = "BEARISH"
        color = BLOOMBERG_RED
    else:
        direction = "NEUTRAL"
        color = BLOOMBERG_ORANGE

    # Confidence (0-5): more agreement = higher confidence
    signals = [sym_score > 0, top_score > 0, macro_score > 0, region_score > 0, breadth_score > 0]
    agree = sum(1 for s in signals if s) if score > 0 else sum(1 for s in signals if not s)
    confidence = min(5, max(1, agree))

    # Key drivers text
    key_drivers = []
    for d in sorted(drivers, key=lambda x: x.get("strength", 0) or 0, reverse=True)[:3]:
        key_drivers.append(f"{d.get('label', '?')} ({d.get('impact', '?')}, strength {d.get('strength', '?')})")
    for e in sorted(events, key=lambda x: x.get("severity", 0) or 0, reverse=True)[:2]:
        if e.get("headline"):
            key_drivers.append(f"{e.get('label', '?')}: {e['headline'][:80]}")

    # Build component-level explanation
    components = [
        {"name": "Symbol sentiment", "weight": "25%", "score": round(sym_score, 1), "raw": f"avg={avg_sent:+.3f}, bull={bull_count}, bear={bear_count}, neutral={neutral_count}",
         "why": f"{bull_count} bullish symbols vs {bear_count} bearish. Avg sentiment {avg_sent:+.3f}. " + ("Mildly positive but not enough to offset macro headwinds." if avg_sent > 0 and score < 0 else "Contributing to bullish bias." if avg_sent > 0 else "Contributing to bearish bias.")},
        {"name": "Topic sentiment", "weight": "20%", "score": round(top_score, 1), "raw": f"avg={topic_sent:+.3f}, bull_topics={bull_topics}, risk_topics={risk_topics}",
         "why": f"{bull_topics} bullish topics vs {risk_topics} risk-flagged topics. Avg topic sentiment {topic_sent:+.3f}. " + ("Topics lean positive." if topic_sent > 0.05 else "Topics lean negative." if topic_sent < -0.05 else "Topics are neutral.")},
        {"name": "Macro risk drivers", "weight": "30%", "score": round(macro_score, 1), "raw": f"risk_off={risk_drivers}/{len(drivers)}, risk_strength={risk_strength:.1f}/{total_driver_strength:.1f}",
         "why": f"{risk_drivers}/{len(drivers)} drivers are risk-off (strength {risk_strength:.1f}/{total_driver_strength:.1f}). " + ("Heavy risk-off bias in macro drivers." if risk_ratio > 0.6 else "Moderate risk-off tilt." if risk_ratio > 0.4 else "Macro drivers balanced.")},
        {"name": "Region breadth (Asia)", "weight": "15%", "score": round(region_score, 1), "raw": f"asia_change={asia_change:+.2f}%",
         "why": f"Asia session moved {asia_change:+.2f}% average. " + ("Positive Asia lead." if asia_change > 0.3 else "Negative Asia lead — markets sold off." if asia_change < -0.3 else "Flat Asia session.")},
        {"name": "Bull vs bear count", "weight": "10%", "score": round(breadth_score, 1), "raw": f"bull={bull_count}, bear={bear_count}",
         "why": f"{bull_count} bullish symbols vs {bear_count} bearish. " + ("Broad positive breadth." if bull_count > bear_count * 2 else "Narrow breadth." if bull_count <= bear_count else "Moderate breadth.")},
    ]

    # Build natural-language explanation
    explanation_parts = []
    if score > 15:
        explanation_parts.append("Overall: Bullish bias driven by")
    elif score < -15:
        explanation_parts.append("Overall: Bearish bias driven by")
    else:
        explanation_parts.append("Overall: Neutral — mixed signals:")
    top_components = sorted(components, key=lambda c: abs(c["score"]), reverse=True)
    for c in top_components[:3]:
        direction_word = "positive" if c["score"] > 0 else "negative"
        explanation_parts.append(f"{c['name']} ({direction_word} contribution {c['score']:+.1f}): {c['why']}")
    if risk_events > 0:
        explanation_parts.append(f"{risk_events} high-severity geopolitical event(s) adding risk premium.")
    explanation = " ".join(explanation_parts)

    return {
        "direction": direction,
        "score": round(score, 1),
        "confidence": confidence,
        "color": color,
        "bull_count": bull_count,
        "bear_count": bear_count,
        "neutral_count": neutral_count,
        "avg_symbol_sentiment": round(avg_sent, 3),
        "avg_topic_sentiment": round(topic_sent, 3),
        "risk_drivers": risk_drivers,
        "risk_events": risk_events,
        "total_drivers": len(drivers),
        "asia_change_pct": round(asia_change, 2),
        "key_drivers": key_drivers[:5],
        "components": components,
        "explanation": explanation,
    }


def build_news_tab(data: dict[str, Any]) -> list[Any]:
    paper = data["paper"]
    sentiment_df = data["sentiment_df"]
    topics_df = data["topics_df"]
    news_payload = data["news_payload"]
    topics_payload = data["topics_payload"]
    news_behavior = data.get("news_behavior") or {}
    children: list[Any] = []

    symbol_news = paper.get("symbol_news_sentiment") or {}
    cards = html.Div(
        [
            metric_card("Tracked sentiment symbols", len(sentiment_df), to_ist_verbose(data.get("news_payload", {}).get("generated_at")) if isinstance(data.get("news_payload"), dict) else "news feed"),
            metric_card("Market topics", len(topics_df), to_ist_verbose(data.get("topics_payload", {}).get("generated_at")) if isinstance(data.get("topics_payload"), dict) else "topic feeds"),
            metric_card("Current paper symbol sentiment", symbol_news.get("weighted_sentiment", "-"), paper.get("symbol", "NIFTYETF")),
            metric_card("Paper symbol headlines", symbol_news.get("item_count", "-"), ", ".join(symbol_news.get("dominant_types") or [])),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    news_timestamp = html.Div(
        [
            html.Div(f"News feed updated: {to_ist_verbose(news_payload.get('generated_at') if isinstance(news_payload, dict) else None)}", style={"fontSize": "11px", "color": "#9ca3af"}),
            html.Div(f"Topic feed updated: {to_ist_verbose(topics_payload.get('generated_at') if isinstance(topics_payload, dict) else None)}", style={"fontSize": "11px", "color": "#9ca3af"}),
        ],
        style={**CARD_STYLE, "padding": "10px 12px"},
    )
    children.append(section("News and sentiment", [cards, news_timestamp], "This covers both symbol-level sentiment and macro topic feeds used by the paper overlay."))

    behavior_pairs = news_behavior.get("top_topic_symbol_pairs") or []
    if behavior_pairs:
        behavior_cards = html.Div(
            [
                metric_card("Topic rows matched", news_behavior.get("matched_events", 0), f"lookback {news_behavior.get('lookback_days', '-') }d"),
                metric_card("Tracked topic-symbol pairs", len(behavior_pairs), "news first, symbols second"),
                metric_card("Best avg alignment 3d %", behavior_pairs[0].get("avg_alignment_3d_pct", "-"), behavior_pairs[0].get("symbol", "")),
            ],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        )
        behavior_df = pd.DataFrame(behavior_pairs[:12])
        children.append(section("Topic news → symbol behavior", [behavior_cards, table_from_df(behavior_df, "topic-symbol-behavior-table", page_size=12)], "This flips the lens: start from topic/news events, map mentioned symbols, then score how those symbols behaved after the news."))

    recent_behavior = news_behavior.get("recent_events") or []
    if recent_behavior:
        recent_df = pd.DataFrame(recent_behavior[:12])
        children.append(section("Recent topic-driven symbol reactions", [table_from_df(recent_df, "recent-topic-reactions-table", page_size=12)]))

    # ── MARKET PREDICTION ──
    pred = compute_market_prediction(data)
    pred_direction = pred["direction"]
    pred_color = pred["color"]
    pred_score = pred["score"]
    pred_conf = pred["confidence"]
    stars = "★" * pred_conf + "☆" * (5 - pred_conf)
    pred_cards = html.Div(
        [
            html.Div(
                [
                    html.Div("TOMORROW'S MARKET", style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "1px"}),
                    html.Div(pred_direction, style={"fontSize": "28px", "fontWeight": "800", "color": pred_color, "letterSpacing": "2px"}),
                    html.Div(f"Score {pred_score:+.0f}/100  {stars}", style={"fontSize": "12px", "color": "#9ca3af"}),
                ],
                style={**CARD_STYLE, "textAlign": "center", "minWidth": "200px", "padding": "16px"},
            ),
            html.Div(
                [
                    html.Div(f"Bullish symbols: {pred['bull_count']}", style={"fontSize": "12px", "color": BLOOMBERG_GREEN}),
                    html.Div(f"Bearish symbols: {pred['bear_count']}", style={"fontSize": "12px", "color": BLOOMBERG_RED}),
                    html.Div(f"Neutral symbols: {pred['neutral_count']}", style={"fontSize": "12px", "color": "#9ca3af"}),
                    html.Div(f"Avg symbol sentiment: {pred['avg_symbol_sentiment']:+.3f}", style={"fontSize": "12px", "color": "#9ca3af"}),
                    html.Div(f"Avg topic sentiment: {pred['avg_topic_sentiment']:+.3f}", style={"fontSize": "12px", "color": "#9ca3af"}),
                    html.Div(f"Risk drivers: {pred['risk_drivers']}/{pred['total_drivers']}", style={"fontSize": "12px", "color": BLOOMBERG_RED if pred['risk_drivers'] > pred['total_drivers'] // 2 else "#9ca3af"}),
                    html.Div(f"Asia session: {pred['asia_change_pct']:+.2f}%", style={"fontSize": "12px", "color": BLOOMBERG_GREEN if pred['asia_change_pct'] > 0 else BLOOMBERG_RED if pred['asia_change_pct'] < 0 else "#9ca3af"}),
                ],
                style={**CARD_STYLE, "padding": "12px 16px", "minWidth": "180px"},
            ),
        ]
        + [
            html.Div(
                [html.Div(kd, style={"fontSize": "11px", "color": "#9ca3af", "marginBottom": "4px"}) for kd in pred.get("key_drivers", [])],
                style={**CARD_STYLE, "padding": "12px 16px", "flex": "1", "minWidth": "250px"},
            )
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "alignItems": "stretch"},
    )
    # ── WHY EXPLANATION (tap to expand) ──
    why_rows = []
    for comp in pred.get("components", []):
        arrow = "▲" if comp["score"] > 0 else "▼" if comp["score"] < 0 else "─"
        arrow_color = BLOOMBERG_GREEN if comp["score"] > 0 else BLOOMBERG_RED if comp["score"] < 0 else "#9ca3af"
        why_rows.append(html.Div([
            html.Span(f"{arrow} ", style={"fontSize": "13px", "color": arrow_color, "fontWeight": "700"}),
            html.Span(f"{comp['name']} ({comp['weight']})", style={"fontSize": "12px", "fontWeight": "700", "color": "#d1d5db"}),
            html.Span(f"  {comp['score']:+.1f}", style={"fontSize": "12px", "color": arrow_color, "fontWeight": "600"}),
            html.Div(f"{comp['raw']}", style={"fontSize": "10px", "color": "#6b7280", "marginLeft": "18px"}),
            html.Div(f"{comp['why']}", style={"fontSize": "11px", "color": "#9ca3af", "marginLeft": "18px", "marginBottom": "6px"}),
        ]))

    explanation = pred.get("explanation", "")
    why_panel = html.Details([
        html.Summary("▸ WHY THIS SCORE? — tap to expand", style={"cursor": "pointer", "fontWeight": "700", "fontSize": "13px", "color": BLOOMBERG_ORANGE, "padding": "8px 12px", "border": f"1px solid {BLOOMBERG_ORANGE}", "borderRadius": "4px", "backgroundColor": "#111827", "marginBottom": "4px"}),
        html.Div(why_rows + [
            html.Div("─" * 40, style={"fontSize": "10px", "color": "#3a4a5a", "margin": "8px 0"}),
            html.Div(explanation, style={"fontSize": "12px", "color": "#d1d5db", "lineHeight": "1.6", "padding": "8px 12px"}),
        ], style={"padding": "8px 12px", "backgroundColor": "#0d1117", "borderRadius": "0 0 4px 4px", "border": f"1px solid #1e2a3a"}),
    ], style={"marginBottom": "12px"})

    prediction_timestamp = html.Div(
        f"Prediction snapshot time: {to_ist_verbose(news_payload.get('generated_at') if isinstance(news_payload, dict) else None)}",
        style={"fontSize": "10px", "color": "#5a6a7a", "marginTop": "4px"},
    )
    children.append(section("Market prediction — next session", [pred_cards, why_panel, prediction_timestamp], "Based on symbol sentiment, topic sentiment, macro drivers, region breadth, and bull/bear counts. Score range: -100 (max bearish) to +100 (max bullish). Tap WHY to see the breakdown. NOT financial advice."))

    children.append(section("Active symbol sentiment", [table_from_df(sentiment_df, "sentiment-table", page_size=10)]))
    children.append(section("Market topics", [table_from_df(topics_df, "topics-table", page_size=10)]))

    if symbol_news:
        children.append(section("Current paper symbol news", [html.Ul([html.Li(x) for x in (symbol_news.get("sample_headlines") or [])], style=CARD_STYLE)]))

    # Summarize active sentiments as cards instead of raw JSON
    active_items = (news_payload.get("active") or [])[:5] if isinstance(news_payload, dict) else []
    if active_items:
        sentiment_rows = []
        for item in active_items:
            sym = item.get("symbol", "?")
            sent = item.get("weighted_sentiment", 0) or 0
            sent_color = BLOOMBERG_GREEN if sent > 0.1 else BLOOMBERG_RED if sent < -0.1 else BLOOMBERG_ORANGE
            block_buy = (item.get("trade_bias") or {}).get("block_buy", False)
            force_sell = (item.get("trade_bias") or {}).get("force_sell", False)
            bias_str = "BLOCK BUY" if block_buy else ("FORCE SELL" if force_sell else "neutral")
            sentiment_rows.append(
                html.Div([
                    html.Span(sym, style={"fontWeight": "700", "fontSize": "13px"}),
                    html.Span(f"  {sent:+.2f}", style={"fontSize": "13px", "color": sent_color, "fontWeight": "700"}),
                    html.Span(f"  [{bias_str}]", style={"fontSize": "11px", "color": BLOOMBERG_GRAY}),
                ], style={**CARD_STYLE, "marginBottom": "4px", "padding": "8px 12px"})
            )
        children.append(section("Active sentiment signals", sentiment_rows))

    # Summarize topics as cards
    topic_items = (topics_payload.get("topics") or [])[:5] if isinstance(topics_payload, dict) else []
    if topic_items:
        topic_rows = []
        for item in topic_items:
            topic = item.get("topic", "?")
            label = item.get("label", "")
            sent = item.get("weighted_sentiment", 0) or 0
            sent_color = BLOOMBERG_GREEN if sent > 0.1 else BLOOMBERG_RED if sent < -0.1 else BLOOMBERG_ORANGE
            topic_rows.append(
                html.Div([
                    html.Span(label or topic, style={"fontWeight": "700", "fontSize": "13px"}),
                    html.Span(f"  {sent:+.2f}", style={"fontSize": "13px", "color": sent_color, "fontWeight": "700"}),
                ], style={**CARD_STYLE, "marginBottom": "4px", "padding": "8px 12px"})
            )
        children.append(section("Market topics", topic_rows))
    return children


def build_telegram_tab(data: dict[str, Any]) -> list[Any]:
    ledger = data["telegram_ledger"]
    history = data["telegram_history"]
    updates = data.get("telegram_updates", pd.DataFrame())
    latest_by_chat = data.get("telegram_latest_by_chat", pd.DataFrame())
    backtests = data.get("telegram_backtests", pd.DataFrame())
    audit = data.get("telegram_trade_audit") or {}
    children: list[Any] = []

    def audit_stat(summary: dict[str, Any], key: str, field: str) -> Any:
        node = (summary or {}).get(key) or {}
        return node.get(field)

    audit_channels = audit.get("channels") or {}

    def pick_audit_channel(*names: str) -> dict[str, Any]:
        for name in names:
            payload = audit_channels.get(name) or audit.get(name)
            if payload:
                return payload
        return {}

    # ── TELEGRAM EQUITY ──
    shortterm = pick_audit_channel("shortterm01")
    sunil_cash = pick_audit_channel("financewithsunil", "finance_with_sunil")
    darkhorse = pick_audit_channel("darkhorseofstockmarket")
    milind = pick_audit_channel("milind4profits")
    equity_channels = [
        ("Shortterm01", shortterm),
        ("FinanceWithSunil", sunil_cash),
        ("Dark Horse", darkhorse),
        ("Milind4Profits", milind),
    ]
    available_equity_channels = [(label, payload) for label, payload in equity_channels if payload]

    def avg_present(values: list[Any]) -> float | None:
        clean = [float(v) for v in values if isinstance(v, (int, float)) and not math.isnan(float(v))]
        if not clean:
            return None
        return round(sum(clean) / len(clean), 2)

    def infer_equity_targets(text: str) -> str:
        low = str(text or "")
        m = re.search(r'UPSIDE\s+RESISTANCE\s*[-–]?\s*([0-9+\-/. ]+)', low, re.IGNORECASE)
        if m:
            vals = re.findall(r'\d+(?:\.\d+)?', m.group(1))[:4]
            return ", ".join(vals) if vals else "-"
        m = re.search(r'Target\s*[-:]\s*([0-9+\-/. ]+)', low, re.IGNORECASE)
        if m:
            vals = re.findall(r'\d+(?:\.\d+)?', m.group(1))[:4]
            return ", ".join(vals) if vals else "-"
        return "-"

    def infer_equity_stop(text: str) -> str:
        low = str(text or "")
        m = re.search(r'SL\s*[-:]\s*(\d+(?:\.\d+)?)', low, re.IGNORECASE)
        return m.group(1) if m else "-"

    equity_recent_rows = []
    for label, payload in available_equity_channels:
        for result in payload.get("sample_results") or []:
            equity_recent_rows.append({
                "channel": label,
                "symbol": result.get("symbol") or result.get("raw_symbol") or "?",
                "signal_type": result.get("signal_type") or "equity",
                "date": result.get("date"),
                "entry_ref": result.get("entry_ref"),
                "ret_5d_pct": result.get("ret_5d_pct"),
                "ret_10d_pct": result.get("ret_10d_pct"),
                "ret_20d_pct": result.get("ret_20d_pct"),
                "max_20d_pct": result.get("max_20d_pct"),
                "target_1_hit_20d": result.get("target_1_hit_20d"),
                "target_2_hit_20d": result.get("target_2_hit_20d"),
                "text": result.get("text") or "",
            })

    total_equity_signals = sum(int(payload.get("signals_evaluated", 0) or 0) for _, payload in available_equity_channels)
    total_equity_extracted = sum(int(payload.get("signals_extracted", 0) or 0) for _, payload in available_equity_channels)

    open_equity_positions: list[dict[str, Any]] = []
    closed_equity_positions: list[dict[str, Any]] = []
    unresolved_equity_positions: list[dict[str, Any]] = []

    if equity_recent_rows:
        recent_df = pd.DataFrame(equity_recent_rows)
        recent_df["sort_date"] = pd.to_datetime(recent_df["date"], errors="coerce")
        recent_df = recent_df.sort_values("sort_date", ascending=False, na_position="last")
        recent_df = recent_df.drop_duplicates(subset=["symbol"], keep="first")

        valid_rows = []
        for _, row in recent_df.iterrows():
            entry_ref = safe_num(row.get("entry_ref"))
            spot = get_equity_spot_snapshot(row.get("symbol"))
            current_price = safe_num(spot.get("price"))
            profit_pct = ((current_price / entry_ref) - 1.0) * 100.0 if current_price is not None and entry_ref not in (None, 0) else None
            malformed_extract = profit_pct is not None and abs(profit_pct) > 200
            text = str(row.get("text") or "")
            optionish_noise = any(x in text.lower() for x in ["lot size", "premium", "ce", "pe"]) and (profit_pct is None or abs(profit_pct) > 100)
            if malformed_extract or optionish_noise or entry_ref is None:
                unresolved_equity_positions.append({
                    "symbol": row.get("symbol"),
                    "status": "filtered_out",
                    "reason": "malformed_or_optionlike_equity_extract",
                    "channel_update": text[:140],
                })
                continue
            rec = dict(row)
            rec["entry_ref"] = entry_ref
            rec["current_price"] = current_price
            rec["profit_pct_live"] = profit_pct
            rec["profit_abs_live"] = (current_price - entry_ref) if current_price is not None else None
            rec["spot_date"] = spot.get("date")
            valid_rows.append(rec)

        open_rows = [r for r in valid_rows if all(safe_num(r.get(k)) is None for k in ["ret_5d_pct", "ret_10d_pct", "ret_20d_pct"])]
        closed_rows = [r for r in valid_rows if any(safe_num(r.get(k)) is not None for k in ["ret_5d_pct", "ret_10d_pct", "ret_20d_pct"])]

        starting_equity_capital = 100000.0
        open_alloc = starting_equity_capital / max(len(open_rows), 1) if open_rows else 0.0
        closed_alloc = starting_equity_capital / max(len(closed_rows), 1) if closed_rows else 0.0

        for r in open_rows:
            entry = safe_num(r.get("entry_ref")) or 0.0
            last = safe_num(r.get("current_price")) or entry
            qty = int(open_alloc // entry) if entry > 0 and open_alloc > 0 else 0
            qty = max(qty, 1) if entry > 0 else 0
            invested = round(qty * entry, 2)
            mtm_pnl = round(qty * (last - entry), 2) if qty > 0 else None
            open_equity_positions.append({
                "symbol": r.get("symbol"),
                "tradingsymbol": r.get("channel") or "",
                "entry_price": round(entry, 2),
                "last_price": round(last, 2),
                "qty": int(qty),
                "invested": invested,
                "market_value": round(qty * last, 2) if qty > 0 else None,
                "mtm_return_pct": safe_num(r.get("profit_pct_live")),
                "mtm_pnl": mtm_pnl,
                "targets_hit": [],
                "stop_loss": infer_equity_stop(r.get("text") or ""),
                "targets_text": infer_equity_targets(r.get("text") or ""),
                "posted_at": r.get("date"),
                "snapshot_at": r.get("spot_date"),
            })

        for r in closed_rows:
            entry = safe_num(r.get("entry_ref")) or 0.0
            ret = safe_num(r.get("ret_20d_pct"))
            exit_reason = "20d audit window"
            if ret is None:
                ret = safe_num(r.get("ret_10d_pct"))
                exit_reason = "10d audit window"
            if ret is None:
                ret = safe_num(r.get("ret_5d_pct"))
                exit_reason = "5d audit window"
            if ret is None:
                continue
            qty = int(closed_alloc // entry) if entry > 0 and closed_alloc > 0 else 0
            qty = max(qty, 1) if entry > 0 else 0
            invested = round(qty * entry, 2) if qty > 0 else None
            exit_price = round(entry * (1.0 + ret / 100.0), 2) if qty > 0 else None
            pnl = round((ret / 100.0) * entry * qty, 2) if qty > 0 else None
            closed_equity_positions.append({
                "symbol": r.get("symbol"),
                "tradingsymbol": r.get("channel") or "",
                "entry_price": round(entry, 2),
                "exit_price": exit_price,
                "qty": int(qty),
                "invested": invested,
                "return_pct": ret,
                "pnl": pnl,
                "exit_reason": exit_reason,
            })

    eq_unrealized = sum(float(p.get("mtm_pnl") or 0.0) for p in open_equity_positions)
    eq_realized = sum(float(p.get("pnl") or 0.0) for p in closed_equity_positions)
    eq_starting = 100000.0
    eq_net_pnl = eq_realized + eq_unrealized
    eq_net_pct = (eq_net_pnl / eq_starting * 100.0) if eq_starting else 0.0
    eq_portfolio = eq_starting + eq_net_pnl
    eq_open_avg = avg_present([p.get("mtm_return_pct") for p in open_equity_positions])
    eq_open_invested = sum(float(p.get("invested") or 0.0) for p in open_equity_positions)
    eq_open_market_value = sum(float(p.get("market_value") or 0.0) for p in open_equity_positions)
    eq_cash = eq_portfolio - eq_open_market_value

    equity_metrics = html.Div(
        [
            metric_card("Equity portfolio", f"₹{eq_portfolio:,.0f}", f"₹1L book | signals {total_equity_signals}"),
            metric_card("Equity net PnL", fmt_pnl(eq_net_pnl, "₹"), fmt_pct(eq_net_pct)),
            metric_card("Cash", f"₹{eq_cash:,.0f}", "free book cash"),
            metric_card("Open invested", f"₹{eq_open_invested:,.0f}", f"value ₹{eq_open_market_value:,.0f}"),
            metric_card("Realized", fmt_pnl(eq_realized, "₹"), "Booked"),
            metric_card("Unrealized", fmt_pnl(eq_unrealized, "₹"), "Open MTM"),
            metric_card("Equity open avg %", eq_open_avg, "live mark to market"),
            metric_card("Open", len(open_equity_positions), "positions"),
            metric_card("Closed", len(closed_equity_positions), "positions"),
            metric_card("Unresolved", len(unresolved_equity_positions), "filtered or malformed"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Telegram equity", [equity_metrics], "Telegram equity now uses the same portfolio-style section structure as Telegram options, powered by audited equity signals plus current spot prices."))

    if open_equity_positions:
        rows = []
        for p in open_equity_positions:
            mtm = p.get("mtm_return_pct", 0) or 0
            pnl = p.get("mtm_pnl", 0) or 0
            targets_text = p.get("targets_text") or "-"
            sl = p.get("stop_loss", "-")
            color_style = {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_GREEN} if mtm > 0 else {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_RED} if mtm < 0 else {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_ORANGE}
            rows.append(html.Div([
                html.Div([html.Span(f"{p.get('symbol', '?')} ", style={"fontWeight": "700", "fontSize": "15px"}), html.Span(p.get('tradingsymbol', ''), style={"fontSize": "12px", "color": "#9ca3af"})], style={"flex": "1"}),
                html.Div(f"Entry ₹{p.get('entry_price')} × Qty {p.get('qty')} → Last ₹{p.get('last_price')}", style={"fontSize": "12px", "color": "#9ca3af"}),
                html.Div(f"{mtm:+.2f}% (₹{pnl:+,.0f})", style=color_style),
                html.Div(f"Invested ₹{friendly(p.get('invested'))} | Value ₹{friendly(p.get('market_value'))}", style={"fontSize": "11px", "color": "#6b7280"}),
                html.Div(f"SL: {sl} | Targets: {targets_text}", style={"fontSize": "11px", "color": "#6b7280"}),
            ], style={**CARD_STYLE, "marginBottom": "8px"}))
        children.append(section(f"Telegram equity open positions ({len(open_equity_positions)})", rows))
    else:
        children.append(section("Telegram equity open positions", [empty_message("No open Telegram equity positions")]))

    if closed_equity_positions:
        rows = []
        for p in closed_equity_positions:
            pnl = p.get("pnl", 0) or 0
            ret = p.get("return_pct", 0) or 0
            color_style = {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_RED} if pnl < 0 else {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_GREEN}
            rows.append(html.Div([
                html.Div([html.Span(f"{p.get('symbol', '?')} ", style={"fontWeight": "700", "fontSize": "15px"}), html.Span(p.get('tradingsymbol', ''), style={"fontSize": "12px", "color": "#9ca3af"})], style={"flex": "1"}),
                html.Div(f"Entry ₹{p.get('entry_price')} × Qty {p.get('qty')} → Exit ₹{p.get('exit_price')}", style={"fontSize": "12px", "color": "#9ca3af"}),
                html.Div(f"{ret:+.2f}% (₹{pnl:+,.0f})", style=color_style),
                html.Div(f"Invested ₹{friendly(p.get('invested'))} | Exit: {p.get('exit_reason', '-')}", style={"fontSize": "11px", "color": "#6b7280"}),
            ], style={**CARD_STYLE, "marginBottom": "8px"}))
        children.append(section(f"Telegram equity closed positions ({len(closed_equity_positions)})", rows))
    else:
        children.append(section("Telegram equity closed positions", [empty_message("No closed Telegram equity positions")]))

    if unresolved_equity_positions:
        rows = []
        for p in unresolved_equity_positions:
            rows.append(html.Div([
                html.Div([html.Span(f"{p.get('symbol', '?')} ", style={"fontWeight": "700", "fontSize": "15px"}), html.Span("equity extract", style={"fontSize": "12px", "color": "#9ca3af"})], style={"flex": "1"}),
                html.Div(f"Status: {p.get('status', '-')}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE}),
                html.Div(f"Reason: {p.get('reason', '-')}", style={"fontSize": "11px", "color": "#6b7280"}),
                html.Div((p.get('channel_update') or '')[:140], style={"fontSize": "11px", "color": "#9ca3af"}),
            ], style={**CARD_STYLE, "marginBottom": "8px"}))
        children.append(section(f"Telegram equity unresolved tracked calls ({len(unresolved_equity_positions)})", rows, "These were captured by the equity audit flow but filtered out because the extract looked malformed or option-like, so they were not shown as open equity positions."))

    # ── TELEGRAM OPTIONS ──
    equity = ledger.get("equity", 0)
    cash = ledger.get("cash", 0)
    starting = ledger.get("starting_capital", 100000)
    realized = ledger.get("realized_pnl", 0)
    unrealized = ledger.get("unrealized_pnl", 0)
    net_pnl = (realized or 0) + (unrealized or 0)
    net_pct = net_pnl / starting * 100 if starting else 0
    open_pos = ledger.get("open_positions", [])
    closed_pos = ledger.get("closed_positions", [])
    unresolved_pos = ledger.get("unresolved_positions", [])
    updated_at = to_ist(ledger.get("updated_at"))
    sunil_options = pick_audit_channel("financewithsunil_options", "finance_with_sunil_options")
    opt_summary = sunil_options.get("summary") or {}

    option_metrics = html.Div(
        [
            metric_card("Options portfolio", f"₹{equity:,.0f}" if isinstance(equity, (int, float)) else "-", f"Updated {updated_at}"),
            metric_card("Options net PnL", fmt_pnl(net_pnl, "₹"), fmt_pct(net_pct)),
            metric_card("Realized", fmt_pnl(realized, "₹"), "Booked"),
            metric_card("Unrealized", fmt_pnl(unrealized, "₹"), "Open MTM"),
            metric_card("Option signals", sunil_options.get("signals_evaluated", 0), f"extracted {sunil_options.get('signals_extracted', 0)}"),
            metric_card("Options 20d dir avg %", audit_stat(opt_summary, "dir_ret_20d_pct", "avg"), f"positive rate {friendly(audit_stat(opt_summary, 'dir_ret_20d_pct', 'positive_rate'))}"),
            metric_card("Open", len(open_pos), "positions"),
            metric_card("Closed", len(closed_pos), "positions"),
            metric_card("Unresolved", len(unresolved_pos), "tracked but not opened"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Telegram options", [option_metrics], "Live Telegram options paper ledger kept separate from Telegram equity signal audit."))

    if open_pos:
        rows = []
        for p in open_pos:
            entry = p.get("entry_price", 0)
            last = p.get("last_price", 0)
            mtm = p.get("mtm_return_pct", 0) or 0
            pnl = p.get("mtm_pnl", 0) or 0
            tgt = p.get("targets_hit", [])
            sl = p.get("stop_loss", "-")
            color_style = {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_GREEN} if mtm > 0 else {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_RED} if mtm < 0 else {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_ORANGE}
            rows.append(html.Div([
                html.Div([html.Span(f"{p.get('symbol', '?')} ", style={"fontWeight": "700", "fontSize": "15px"}), html.Span(p.get('tradingsymbol', ''), style={"fontSize": "12px", "color": "#9ca3af"})], style={"flex": "1"}),
                html.Div(f"Entry ₹{entry} → Last ₹{last}", style={"fontSize": "12px", "color": "#9ca3af"}),
                html.Div(f"{mtm:+.2f}% (₹{pnl:+,.0f})", style=color_style),
                html.Div(f"SL: {sl} | Targets hit: {tgt if tgt else '-'}", style={"fontSize": "11px", "color": "#6b7280"}),
            ], style={**CARD_STYLE, "marginBottom": "8px"}))
        children.append(section(f"Telegram options open positions ({len(open_pos)})", rows))
    else:
        children.append(section("Telegram options open positions", [empty_message("No open Telegram options positions")]))

    if closed_pos:
        rows = []
        for p in closed_pos:
            pnl = p.get("pnl", 0) or 0
            ret = p.get("return_pct", 0) or 0
            color_style = {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_RED} if pnl < 0 else {"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_GREEN}
            rows.append(html.Div([
                html.Div([html.Span(f"{p.get('symbol', '?')} ", style={"fontWeight": "700", "fontSize": "15px"}), html.Span(p.get('tradingsymbol', ''), style={"fontSize": "12px", "color": "#9ca3af"})], style={"flex": "1"}),
                html.Div(f"{ret:+.2f}% (₹{pnl:+,.0f})", style=color_style),
                html.Div(f"Exit: {p.get('exit_reason', '-')}", style={"fontSize": "11px", "color": "#6b7280"}),
            ], style={**CARD_STYLE, "marginBottom": "8px"}))
        children.append(section(f"Telegram options closed positions ({len(closed_pos)})", rows))

    if unresolved_pos:
        rows = []
        for p in unresolved_pos:
            rows.append(html.Div([
                html.Div([html.Span(f"{p.get('symbol', '?')} ", style={"fontWeight": "700", "fontSize": "15px"}), html.Span(f"{p.get('option_side', '')} {p.get('option_strike', '')}", style={"fontSize": "12px", "color": "#9ca3af"})], style={"flex": "1"}),
                html.Div(f"Status: {p.get('status', '-')}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE}),
                html.Div(f"Reason: {p.get('reason', '-')}", style={"fontSize": "11px", "color": "#6b7280"}),
                html.Div((p.get('channel_update') or '')[:140], style={"fontSize": "11px", "color": "#9ca3af"}),
            ], style={**CARD_STYLE, "marginBottom": "8px"}))
        children.append(section(f"Telegram options unresolved tracked calls ({len(unresolved_pos)})", rows, "These were captured from Telegram and tracked, but the paper ledger could not open them yet, usually because the exact option contract could not be resolved."))

    if not history.empty:
        fig = px.line(history, x="timestamp", y=[c for c in ["equity", "cash"] if c in history.columns], markers=False)
        fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827", margin=dict(l=40, r=20, t=30, b=30), legend=dict(orientation="h", yanchor="bottom", y=1.02), xaxis_title="", yaxis_title="₹", title=dict(text="Telegram options equity curve", font=dict(size=14)))
        fig.update_xaxes(tickformat="%b %d %H:%M")
        children.append(section("Telegram options equity curve", [dcc.Graph(figure=fig)]))

    if not backtests.empty:
        show = backtests.sort_values("generated_at", ascending=False).copy()
        children.append(section("Telegram options backtests", [table_from_df(show, "tg-options-backtests-table", page_size=8)], "Historical options paper reports, kept separate from live Telegram options ledger."))

    # ── Shared channel visibility ──
    if not latest_by_chat.empty:
        display = latest_by_chat.copy()
        for col in ["date", "captured_at"]:
            if col in display.columns:
                display[col] = display[col].apply(lambda x: to_ist(str(x)) if pd.notna(x) else "-")
        if "text_excerpt" in display.columns:
            display["text_excerpt"] = display["text_excerpt"].fillna("").astype(str).str.slice(0, 120)
        cols = [c for c in ["chat", "date", "text_excerpt"] if c in display.columns]
        children.append(section("Latest channel posts", [table_from_df(display[cols], "tg-latest-by-chat-table", page_size=8)], "Newest post from each tracked channel."))

    if not updates.empty:
        display = updates.head(30).copy()
        for col in ["date", "captured_at"]:
            if col in display.columns:
                display[col] = display[col].apply(lambda x: to_ist(str(x)) if pd.notna(x) else "-")
        if "text_excerpt" in display.columns:
            display["text_excerpt"] = display["text_excerpt"].fillna("").astype(str).str.slice(0, 100)
        cols = [c for c in ["chat", "date", "message_id", "text_excerpt"] if c in display.columns]
        children.append(section("Recent updates", [table_from_df(display[cols], "tg-channel-updates-table", page_size=15)], "Last 30 channel messages."))

    channel_scores = load_json(Path(ROOT) / "reports" / "channel_learning_scores.json") if hasattr(ROOT, 'exists') else None
    if channel_scores and isinstance(channel_scores, dict):
        channels = channel_scores.get("channels") or {}
        if channels:
            score_rows = []
            for chat, s in channels.items():
                option_audit = s.get("options_audit") or {}
                equity_audit = s.get("equity_audit") or {}
                paper_stats = s.get("paper_stats") or {}
                score_rows.append({
                    "channel": chat,
                    "confidence": s.get("confidence", "-"),
                    "action": s.get("action", "-"),
                    "sizing_mult": s.get("sizing_mult", "-"),
                    "win_rate%": s.get("win_rate", "-"),
                    "avg_ret%": s.get("avg_return_pct", "-"),
                    "opt_10d_avg%": option_audit.get("dir_ret_10d_avg", "-"),
                    "opt_20d_pos%": option_audit.get("dir_ret_20d_positive_rate", "-"),
                    "eq_5d_pos%": equity_audit.get("ret_5d_positive_rate", "-"),
                    "eq_10d_pos%": equity_audit.get("ret_10d_positive_rate", "-"),
                    "resolve_rate%": paper_stats.get("contract_resolution_rate", "-"),
                    "n_trades": s.get("n_trades", 0),
                    "ladder": s.get("ladder_style", "-"),
                    "sl": s.get("sl_style", "-"),
                })
            score_df = pd.DataFrame(score_rows)
            recs = channel_scores.get("recommendations") or []
            rec_text = " | ".join(recs) if recs else "No recommendations yet"
            children.append(section("Channel learning", [table_from_df(score_df, "tg-channel-learning-table", page_size=10)], rec_text))

    return children


def build_research_tab(data: dict[str, Any]) -> list[Any]:
    combined = data["combined_labs"]
    improvement = data["improvement"]
    options_supervisor = data["options_supervisor"]
    five_year = data["five_year"]
    exposure_sweep = data.get("exposure_sweep") or {}
    hourly = data["hourly_lab"]
    children: list[Any] = []

    five_vol = five_year.get("vol_sizing") or {}
    five_base = five_year.get("baseline") or {}
    exposure_best = exposure_sweep.get("best") or {}
    cards = html.Div(
        [
            metric_card("5y vol-sizing CAGR %", five_vol.get("cagr_pct", "-"), data.get("five_year_path") or "5y validation"),
            metric_card("5y vol-sizing return %", five_vol.get("return_pct", "-"), f"drawdown {friendly(five_vol.get('drawdown_pct'))}%"),
            metric_card("Exposure sweep best CAGR %", exposure_best.get("cagr_pct", "-"), exposure_best.get("name", data.get("exposure_sweep_path") or "5y exposure sweep")),
            metric_card("Exposure best drawdown %", exposure_best.get("drawdown_pct", "-"), f"return {friendly(exposure_best.get('return_pct'))}%"),
            metric_card("5y baseline return %", five_base.get("return_pct", "-"), f"CAGR {friendly(five_base.get('cagr_pct'))}%"),
            metric_card("Improvement %", (five_year.get("improvement") or {}).get("return_pct", "-"), "vol sizing minus baseline"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Research scoreboard", [cards]))

    # ── 30% CAGR HUNT STATUS ──
    hunt_path = ROOT / "reports" / "thirty_cagr_hunt_latest.json"
    hunt_data = None
    if hunt_path.exists():
        try:
            hunt_data = json.loads(hunt_path.read_text())
        except Exception:
            pass
    if hunt_data:
        hunt_status = hunt_data.get("status", "?")
        hunt_phase = hunt_data.get("phase", "?")
        hunt_total = hunt_data.get("variants_total", 0)
        hunt_done = hunt_data.get("variants_done", 0)
        hunt_best = hunt_data.get("best_variant", "-")
        hunt_best_cagr = hunt_data.get("best_cagr_pct")
        hunt_best_ret = hunt_data.get("best_return_pct")
        hunt_best_dd = hunt_data.get("best_drawdown_pct")
        hunt_progress = hunt_data.get("progress_pct") or (hunt_done / max(hunt_total, 1) * 100 if hunt_total else 0)
        hunt_current = hunt_data.get("current_variant", "-")
        hunt_output = hunt_data.get("output_json")

        # Color based on CAGR relative to target
        if hunt_best_cagr is not None and hunt_best_cagr >= 30:
            cagr_color = BLOOMBERG_GREEN
        elif hunt_best_cagr is not None and hunt_best_cagr >= 25:
            cagr_color = BLOOMBERG_ORANGE
        else:
            cagr_color = "#9ca3af"

        hunt_cards = html.Div(
            [
                metric_card("Hunt status", hunt_status.upper(), hunt_phase),
                metric_card("Progress", f"{hunt_done}/{hunt_total}", f"{hunt_progress:.0f}%"),
                metric_card("Best CAGR %", hunt_best_cagr, hunt_best),
                metric_card("Best return %", hunt_best_ret, f"DD {friendly(hunt_best_dd)}%"),
            ],
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
        )

        # Build progress bar
        bar_bg = html.Div(style={"height": "6px", "backgroundColor": "#1e2a3a", "borderRadius": "3px", "width": "100%"})
        bar_fill_pct = min(100, hunt_progress) if hunt_progress else 0
        bar_fill = html.Div(style={"height": "6px", "backgroundColor": cagr_color, "borderRadius": "3px", "width": f"{bar_fill_pct}%"})
        progress_bar = html.Div([bar_bg, bar_fill], style={"position": "relative", "height": "6px", "marginBottom": "8px"})

        # Build CAGR target bar (visual gauge toward 30%)
        if hunt_best_cagr is not None and hunt_best_cagr > 0:
            cagr_fill = min(100, hunt_best_cagr / 30 * 100)
            cagr_bar_bg = html.Div(style={"height": "8px", "backgroundColor": "#1e2a3a", "borderRadius": "4px", "width": "100%"})
            cagr_bar_fill = html.Div(style={"height": "8px", "backgroundColor": cagr_color, "borderRadius": "4px", "width": f"{cagr_fill:.0f}%"})
            cagr_gauge = html.Div([
                html.Div(f"CAGR progress toward 30% target: {hunt_best_cagr:.1f}% / 30%", style={"fontSize": "10px", "color": "#6b7280", "marginBottom": "3px"}),
                html.Div([cagr_bar_bg, cagr_bar_fill], style={"position": "relative", "height": "8px"}),
            ], style={"marginBottom": "8px"})
        else:
            cagr_gauge = html.Div()

        # Load full ranked results if hunt completed
        hunt_rows = []
        if hunt_output and Path(hunt_output).exists():
            try:
                full_hunt = json.loads(Path(hunt_output).read_text())
                ranked = full_hunt.get("ranked", [])
                for r in ranked[:10]:
                    cagr = r.get("cagr_pct", 0) or 0
                    hunt_rows.append({
                        "variant": r.get("name", "?"),
                        "CAGR %": round(cagr, 2),
                        "Return %": round(r.get("return_pct", 0) or 0, 1),
                        "DD %": round(r.get("drawdown_pct", 0) or 0, 1),
                        "Trades": r.get("trades", 0),
                        "Win %": round(r.get("win_rate_pct", 0) or 0, 0),
                        "Sharpe": round(r.get("sharpe", 0) or 0, 2),
                    })
            except Exception:
                pass

        hunt_children = [hunt_cards, progress_bar, cagr_gauge]
        if hunt_rows:
            hunt_children.append(table_from_df(pd.DataFrame(hunt_rows), "hunt-results-table", page_size=10))
        if hunt_current and hunt_status == "running":
            hunt_children.append(html.Div(f"Evaluating: {hunt_current}", style={"fontSize": "11px", "color": "#6b7280"}))

        children.append(section("30% CAGR hunt", hunt_children, "5Y live-parity validation. Target: >=30% CAGR. Progress bar shows completion; CAGR gauge shows best result vs 30% target."))

    exposure_rows_raw = exposure_sweep.get("ranked") or exposure_sweep.get("results") or []
    if exposure_rows_raw:
        exposure_rows = pd.DataFrame(exposure_rows_raw).sort_values("cagr_pct", ascending=False)
        children.append(section("5 year exposure sweep", [table_from_df(exposure_rows, "research-exposure-sweep-table", page_size=10)]))

    if not combined.empty:
        show = combined.sort_values("generated_at", ascending=False).copy()
        children.append(section("Completed labs", [table_from_df(show, "research-labs-table", page_size=12)]))

    issues = to_df(improvement.get("issues") or [])
    improvements = to_df(improvement.get("improvement_areas") or [])
    iteration_items = to_df(((options_supervisor.get("iteration_plan") or {}).get("items") or []))
    if not issues.empty:
        children.append(section("Improvement audit issues", [table_from_df(issues, "research-issues-table", page_size=8)]))
    if not improvements.empty:
        children.append(section("Improvement audit next moves", [table_from_df(improvements, "research-improvements-table", page_size=8)]))
    if not iteration_items.empty:
        children.append(section("Options iteration plan", [table_from_df(iteration_items, "research-iteration-table", page_size=8)]))

    # Hourly lab status as clean card
    if hourly:
        lab_status = (hourly.get("status") or {}).get("status", "?")
        lab_msg = (hourly.get("status") or {}).get("message", "")
        children.append(section("Lab status", [
            html.Div([
                html.Div(lab_status.upper(), style={"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_GREEN if lab_status == "running" else BLOOMBERG_ORANGE}),
                html.Div(lab_msg, style={"fontSize": "11px", "color": "#5a6a7a", "marginTop": "2px"}),
            ], style=CARD_STYLE),
        ]))

    # Options supervisor as clean card
    if options_supervisor:
        opt_fetch = (options_supervisor.get("fetch") or {}).get("ok", "?")
        opt_paper = (options_supervisor.get("paper_shadow") or {}).get("ok", "?")
        opt_lab = (options_supervisor.get("options_lab") or {}).get("ok", "?")
        children.append(section("Options supervisor", [
            html.Div([
                html.Span(f"Fetch: {opt_fetch}  ", style={"fontSize": "12px", "color": BLOOMBERG_GREEN if opt_fetch else BLOOMBERG_RED}),
                html.Span(f"Paper: {opt_paper}  ", style={"fontSize": "12px", "color": BLOOMBERG_GREEN if opt_paper else BLOOMBERG_RED}),
                html.Span(f"Lab: {opt_lab}", style={"fontSize": "12px", "color": BLOOMBERG_GREEN if opt_lab else BLOOMBERG_RED}),
            ], style=CARD_STYLE),
        ]))
    return children


def build_world_map_figure(macro: dict[str, Any], rotation_lon: float = 78.0, pulse_phase: int = 0) -> go.Figure:
    markets = macro.get("markets") or []
    events = macro.get("events") or []
    pulse = 1.0 + 0.08 * math.sin(pulse_phase * 0.8)

    map_lats, map_lons, map_texts, map_colors, map_sizes = [], [], [], [], []
    for m in markets:
        if m.get("status") != "ok" or m.get("lat") is None:
            continue
        ch = safe_num(m.get("change_pct"))
        map_lats.append(float(m["lat"]))
        map_lons.append(float(m["lon"]))
        map_texts.append(f"{m['label']}<br>{ch:+.2f}%" if ch is not None else f"{m['label']}<br>N/A")
        map_colors.append(ch if ch is not None else 0)
        base_size = 18 if m.get("kind") == "equity" else 14
        map_sizes.append(round(base_size * pulse, 1))

    ev_lats, ev_lons, ev_texts, ev_colors, ev_sizes = [], [], [], [], []
    for e in events:
        sev = e.get("severity", 0)
        if sev < 15:
            continue
        ev_lats.append(float(e.get("lat", 0)))
        ev_lons.append(float(e.get("lon", 0)))
        ev_texts.append(f"⚡ {e['label']}<br>severity {sev}")
        ev_colors.append(-sev / 10.0)
        ev_sizes.append(round(max(12, min(30, sev)) * (1.0 + 0.12 * math.sin((pulse_phase + sev) * 0.6)), 1))

    fig = go.Figure()

    if map_lats:
        fig.add_trace(go.Scattergeo(
            lon=map_lons, lat=map_lats, text=map_texts,
            marker=dict(
                size=map_sizes, color=map_colors,
                colorscale=[[0, BLOOMBERG_RED], [0.5, BLOOMBERG_ORANGE], [1, BLOOMBERG_GREEN]],
                cmin=-3, cmax=3, line=dict(width=1, color="#1e2a3a"),
                colorbar=dict(title="% chg", thickness=10, x=1.12, xpad=24, tickfont=dict(size=9, color=BLOOMBERG_GRAY), title_font=dict(size=9, color=BLOOMBERG_GRAY)),
            ),
            mode="markers+text", textposition="top center",
            textfont=dict(size=9, color="#c0c0c0"),
            name="Markets",
        ))

    if ev_lats:
        fig.add_trace(go.Scattergeo(
            lon=ev_lons, lat=ev_lats, text=ev_texts,
            marker=dict(
                size=ev_sizes, color=ev_colors,
                colorscale=[[0, BLOOMBERG_RED], [0.5, "#ff8800"], [1, "#ffcc00"]],
                cmin=-10, cmax=0, symbol="diamond", line=dict(width=1, color="#ff4444"),
            ),
            mode="markers+text", textposition="bottom center",
            textfont=dict(size=9, color="#ff8800"),
            name="Events",
        ))

    fig.update_layout(
        geo=dict(
            projection_type="orthographic",
            projection_rotation=dict(lon=rotation_lon, lat=12),
            showland=True, landcolor="#111827", showocean=True, oceancolor="#0a0e17",
            showcountries=True, countrycolor="#1e2a3a", showlakes=False,
            coastlinecolor="#1e2a3a", bgcolor="#030712", showframe=False,
        ),
        paper_bgcolor="#030712", plot_bgcolor="#111827",
        margin=dict(l=0, r=0, t=30, b=0),
        height=720,
        legend=dict(font=dict(size=10, color=BLOOMBERG_GRAY), x=1.02, y=1, xanchor="left"),
        title_font=dict(size=12, color=BLOOMBERG_ORANGE),
        uirevision="macro-world-map",
    )
    return fig


def build_global_macro_tab(data: dict[str, Any]) -> list[Any]:
    macro = load_global_macro()
    if not macro:
        return [empty_message("Global macro data not yet available. Refreshing in ~30s.")]

    children: list[Any] = []

    # ── 1. LIVE MARKET RIBBON ────────────────────────────────
    markets = macro.get("markets") or []
    events = macro.get("events") or []
    ribbon_cards = []
    for m in markets:
        if m.get("status") != "ok":
            continue
        ch = safe_num(m.get("change_pct"))
        color = BLOOMBERG_GREEN if (ch or 0) > 0 else BLOOMBERG_RED if (ch or 0) < 0 else BLOOMBERG_ORANGE
        label = m.get("label", "?")
        kind_badge = m.get("kind", "")[:3].upper()
        ribbon_cards.append(html.Div(
            [
                html.Div(f"{kind_badge} {label}", style={"fontSize": "9px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px", "fontWeight": "600"}),
                html.Div(f"{ch:+.2f}%" if ch is not None else "-", style={"fontSize": "18px", "fontWeight": "700", "color": color, "fontFamily": "'JetBrains Mono', monospace"}),
                html.Div(friendly(m.get("last")), style={"fontSize": "11px", "color": "#5a6a7a"}),
            ],
            style={**CARD_STYLE, "flex": "1", "minWidth": "120px", "maxWidth": "180px"},
        ))
    children.append(section("GLOBAL MARKETS RIBBON", [html.Div(ribbon_cards, style={"display": "flex", "gap": "8px", "flexWrap": "wrap"})], "Live index & macro moves, refreshed every 10 min"))

    # ── 2. WORLD MAP ────────────────────────────────────────
    try:
        fig = build_world_map_figure(macro)
        children.append(section(
            "WORLD MAP — animated globe & event hotspots",
            [
                dcc.Graph(
                    id="macro-world-map",
                    figure=fig,
                    animate=False,
                    config={"displayModeBar": False},
                    style={"height": "78vh", "minHeight": "720px"},
                )
            ],
            "Auto-rotating globe. Green = up, Red = down, ◆ = geopolitical event severity.",
        ))
    except Exception as exc:
        children.append(section("WORLD MAP", [empty_message(f"Map render error: {exc}")]))

    # ── 3. REGION SUMMARY ───────────────────────────────────
    regions = macro.get("region_summary") or []
    if regions:
        r_cards = []
        for r in regions:
            ch = safe_num(r.get("avg_change_pct"))
            color = BLOOMBERG_GREEN if (ch or 0) > 0 else BLOOMBERG_RED if (ch or 0) < 0 else BLOOMBERG_ORANGE
            r_cards.append(html.Div(
                [
                    html.Div(r["region"].upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "1px", "fontWeight": "600"}),
                    html.Div(f"{ch:+.2f}%" if ch is not None else "-", style={"fontSize": "18px", "fontWeight": "700", "color": color}),
                    html.Div(f"{r.get('count', '?')} indices", style={"fontSize": "11px", "color": "#5a6a7a"}),
                ],
                style={**CARD_STYLE, "flex": "1", "minWidth": "130px"},
            ))
        children.append(section("REGION BREADTH", [html.Div(r_cards, style={"display": "flex", "gap": "8px", "flexWrap": "wrap"})], "Average index move by region"))

    # ── 4. GEOPOLITICAL & MACRO EVENTS ──────────────────────
    event_rows = []
    for e in events:
        sev = e.get("severity", 0)
        sent = e.get("weighted_sentiment", 0.0)
        sev_color = BLOOMBERG_RED if sev >= 35 else BLOOMBERG_ORANGE if sev >= 20 else BLOOMBERG_GRAY
        sent_color = BLOOMBERG_RED if sent < -0.15 else BLOOMBERG_GREEN if sent > 0.15 else BLOOMBERG_GRAY
        status_badge = e.get("status", "quiet").upper()
        headline = e.get("headline") or e.get("summary") or "No recent headline"
        event_rows.append(html.Div([
            html.Div([
                html.Span(f"◆ {e['label']}", style={"fontSize": "13px", "fontWeight": "700", "color": sev_color}),
                html.Span(f"  [{status_badge}]", style={"fontSize": "10px", "color": BLOOMBERG_GRAY}),
            ], style={"marginBottom": "2px"}),
            html.Div(headline, style={"fontSize": "12px", "color": "#c0c0c0", "marginBottom": "4px"}),
            html.Div([
                html.Span(f"severity {sev}", style={"fontSize": "11px", "color": sev_color, "marginRight": "12px"}),
                html.Span(f"sentiment {sent:+.2f}", style={"fontSize": "11px", "color": sent_color, "marginRight": "12px"}),
                html.Span(f"sectors: {', '.join(e.get('sectors') or [])}", style={"fontSize": "10px", "color": BLOOMBERG_GRAY}),
            ], style={"marginBottom": "4px"}),
            html.Div(f"India: {e.get('india_impact', '-')}", style={"fontSize": "11px", "color": BLOOMBERG_BLUE, "fontStyle": "italic", "marginBottom": "4px"}),
            html.Div(f"Market: {', '.join(e.get('market_impacts') or [])}", style={"fontSize": "10px", "color": "#5a6a7a"}),
        ], style={**CARD_STYLE, "marginBottom": "8px"}))
    if event_rows:
        children.append(section("GEOPOLITICAL & MACRO EVENTS", event_rows, "Severity: 5-100 based on headline keywords, recency, and event type. ◆ = hotspot on map."))

    # ── 5. TOP MACRO DRIVERS ────────────────────────────────
    drivers = macro.get("drivers") or []
    if drivers:
        d_rows = []
        for d in drivers:
            impact_color = BLOOMBERG_RED if d.get("impact") == "risk_off" else BLOOMBERG_GREEN if d.get("impact") == "risk_on" else BLOOMBERG_ORANGE
            icon = "📉" if d.get("impact") == "risk_off" else "📈" if d.get("impact") == "risk_on" else "📊"
            d_rows.append(html.Div([
                html.Span(f"{icon} {d['label']}", style={"fontSize": "12px", "fontWeight": "700", "color": impact_color}),
                html.Span(f"  — {d.get('headline', '')}", style={"fontSize": "12px", "color": "#c0c0c0"}),
                html.Span(f"  [strength {d.get('strength', '?')} | {d.get('impact', '?')}]", style={"fontSize": "10px", "color": BLOOMBERG_GRAY}),
            ], style={**CARD_STYLE, "marginBottom": "4px"}))
        children.append(section("WHY MARKETS ARE MOVING", d_rows, "Top drivers right now: market moves + event severity combined and ranked"))

    # ── 6. TIMESTAMP ────────────────────────────────────────
    gen_at = macro.get("generated_at", "?")
    try:
        gen_dt = pd.Timestamp(gen_at)
        if gen_dt.tzinfo is None:
            gen_dt = gen_dt.tz_localize("UTC")
        gen_at = gen_dt.tz_convert(IST).strftime("%H:%M IST")
    except Exception:
        pass
    children.append(html.Div(f"Macro data updated: {gen_at}", style={"fontSize": "10px", "color": "#5a6a7a", "marginTop": "8px"}))

    return children


def build_calendar_tab(data: dict[str, Any]) -> list[Any]:
    eco = load_eco_calendar()
    earnings_pipeline = data.get("earnings_pipeline") or {}
    if not eco:
        return [empty_message("Calendar + sector data not yet available. Refreshing in ~30s.")]

    children: list[Any] = []

    # ── 1. ECONOMIC EVENTS ─────────────────────────────────
    eco_events = eco.get("economic_events") or []
    if eco_events:
        high_events = [e for e in eco_events if e.get("impact") == "high"]
        med_events = [e for e in eco_events if e.get("impact") == "medium"]
        low_events = [e for e in eco_events if e.get("impact") == "low"]

        event_cards = html.Div([
            metric_card("Total events", len(eco_events), "from RSS feeds"),
            metric_card("High impact", len(high_events), "Fed, RBI, CPI, GDP..."),
            metric_card("Medium impact", len(med_events), "PMI, IP, trade..."),
            metric_card("Low impact", len(low_events), "confidence, housing..."),
        ], style={"display": "flex", "gap": "12px", "flexWrap": "wrap"})

        event_rows = []
        for e in eco_events[:25]:
            impact = e.get("impact", "low")
            impact_color = BLOOMBERG_RED if impact == "high" else BLOOMBERG_ORANGE if impact == "medium" else BLOOMBERG_GRAY
            sent = e.get("sentiment", 0.0)
            event_rows.append(html.Div([
                html.Span(f"{'🔴' if impact=='high' else '🟡' if impact=='medium' else '⚪'} {e.get('title','')[:100]}", style={"fontSize": "12px", "fontWeight": "600", "color": impact_color}),
                html.Span(f"  [{e.get('region','')}] [{impact}]", style={"fontSize": "10px", "color": BLOOMBERG_GRAY}),
                html.Span(f"  sent {sent:+.2f}" if sent else "", style={"fontSize": "10px", "color": BLOOMBERG_GRAY}),
            ], style={**CARD_STYLE, "marginBottom": "4px"}))

        children.append(section("ECONOMIC CALENDAR", [event_cards] + event_rows, "🔴 high  🟡 medium  ⚪ low — from Investing.com RSS"))
    else:
        children.append(section("ECONOMIC CALENDAR", [empty_message("No economic events found in RSS feeds.")]))

    # ── 2. EARNINGS ────────────────────────────────────────
    earnings = eco.get("earnings") or []
    if earnings:
        earn_rows = []
        for e in earnings:
            raw_date = e.get("earnings_date")
            parsed_date = pd.to_datetime(raw_date, errors="coerce")
            if pd.notna(parsed_date):
                display_date = parsed_date.strftime("%b %d, %Y")
            else:
                display_date = str(raw_date) if raw_date is not None else "-"
            earn_rows.append({
                "symbol": e.get("symbol"),
                "earnings_date": display_date,
                "type": e.get("type"),
                "_sort_date": parsed_date,
            })
        earn_df = pd.DataFrame(earn_rows)
        if not earn_df.empty and "_sort_date" in earn_df.columns:
            earn_df = earn_df.sort_values("_sort_date", na_position="last").drop(columns=["_sort_date"])
        children.append(section("EARNINGS CALENDAR", [table_from_df(earn_df, "earnings-table", page_size=10)], "Upcoming earnings for tracked universe symbols"))
    else:
        children.append(section("EARNINGS CALENDAR", [empty_message("No upcoming earnings found for tracked symbols.")]))

    earnings_context = earnings_pipeline.get("upcoming_with_context") or []
    earnings_scoreboard = earnings_pipeline.get("symbol_scoreboard") or []
    if earnings_context:
        children.append(section("EARNINGS PIPELINE", [table_from_df(pd.DataFrame(earnings_context[:12]), "earnings-pipeline-upcoming-table", page_size=12)], "Upcoming earnings enriched with how each symbol behaved after prior earnings-related news/results events."))
    if earnings_scoreboard:
        children.append(section("POST-EARNINGS BEHAVIOR", [table_from_df(pd.DataFrame(earnings_scoreboard[:12]), "post-earnings-behavior-table", page_size=12)], "Historical reaction of symbols after earnings/results news in the recent archive."))

    # ── 3. SECTOR HEATMAP ──────────────────────────────────
    sectors = eco.get("sectors") or []
    ok_sectors = [s for s in sectors if s.get("status") == "ok" and s.get("change_pct") is not None]
    if ok_sectors:
        # Treemap
        labels = [s["label"] for s in ok_sectors]
        parents = ["" for _ in ok_sectors]
        values = [max(0.1, abs(float(s["change_pct"]))) for s in ok_sectors]
        colors = [float(s["change_pct"]) for s in ok_sectors]
        hover_texts = [f"{s['label']}<br>{s['change_pct']:+.2f}%" for s in ok_sectors]

        fig = go.Figure(go.Treemap(
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(
                colors=colors,
                colorscale=[[0, BLOOMBERG_RED], [0.5, "#1a1a2e"], [1, BLOOMBERG_GREEN]],
                cmin=-4, cmax=4,
                line=dict(width=2, color="#0a0e17"),
            ),
            text=hover_texts,
            textinfo="text",
            textfont=dict(size=13, color="#e0e0e0"),
            hoverinfo="text",
        ))
        fig.update_layout(
            paper_bgcolor="#030712", plot_bgcolor="#111827",
            margin=dict(l=0, r=0, t=30, b=0),
            height=450,
        )
        children.append(section("SECTOR HEATMAP", [dcc.Graph(figure=fig)], "Green = up, Red = down. Size = absolute move. NSE sector indices via yfinance."))

        # Also show sector cards as simple bar list
        sector_cards = []
        ok_sectors_sorted = sorted(ok_sectors, key=lambda x: float(x.get("change_pct", 0)), reverse=True)
        for s in ok_sectors_sorted:
            ch = float(s["change_pct"])
            color = BLOOMBERG_GREEN if ch > 0 else BLOOMBERG_RED if ch < 0 else BLOOMBERG_ORANGE
            sector_cards.append(html.Div([
                html.Div(s["label"].upper(), style={"fontSize": "9px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px", "fontWeight": "600"}),
                html.Div(f"{ch:+.2f}%", style={"fontSize": "16px", "fontWeight": "700", "color": color}),
            ], style={**CARD_STYLE, "flex": "1", "minWidth": "110px", "maxWidth": "160px"}))
        children.append(section("SECTOR RANKING", [html.Div(sector_cards, style={"display": "flex", "gap": "6px", "flexWrap": "wrap"})], "Ranked by today's move"))
    else:
        children.append(section("SECTOR HEATMAP", [empty_message("No sector data available yet.")]))

    # ── 4. TIMESTAMP ────────────────────────────────────────
    gen_at = eco.get("generated_at", "?")
    try:
        gen_dt = pd.Timestamp(gen_at)
        if gen_dt.tzinfo is None:
            gen_dt = gen_dt.tz_localize("UTC")
        gen_at = gen_dt.tz_convert(IST).strftime("%H:%M IST")
    except Exception:
        pass
    children.append(html.Div(f"Calendar + sector data updated: {gen_at}", style={"fontSize": "10px", "color": "#5a6a7a", "marginTop": "8px"}))

    return children


def build_reports_tab(data: dict[str, Any]) -> list[Any]:
    reports_df = data["reports_df"]
    children: list[Any] = [section("Recent report files", [table_from_df(reports_df, "reports-table", page_size=20)], "All generated reports on disk.")]
    # Clean summary cards for key reports
    for title, payload in [
        ("Scorecard", data["scorecard"]),
        ("Portfolio", data["portfolio"]),
        ("Paper shadow", data["paper"]),
    ]:
        if payload and isinstance(payload, dict):
            items = []
            for k, v in list(payload.items())[:6]:
                if isinstance(v, (str, int, float, bool, type(None))):
                    items.append(html.Div([html.Span(k.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY}), html.Span(f" {v}", style={"fontSize": "13px", "fontWeight": "700"})], style={"display": "inline-block", "marginRight": "16px"}))
            if items:
                children.append(section(f"{title} summary", [html.Div(items, style=CARD_STYLE)]))
    return children


def render_tab(tab: str, data: dict[str, Any]) -> list[Any]:
    if tab == "runtime":
        return build_runtime_tab(data)
    if tab == "portfolio":
        return build_portfolio_tab(data)
    if tab == "paper":
        return build_paper_tab(data)
    if tab == "news":
        return build_news_tab(data)
    if tab == "global_macro":
        return build_global_macro_tab(data)
    if tab == "calendar":
        return build_calendar_tab(data)
    if tab == "telegram":
        return build_telegram_tab(data)
    if tab == "research":
        return build_research_tab(data)
    if tab == "reports":
        return build_reports_tab(data)
    return build_overview_tab(data)


def empty_figure(title: str) -> dict[str, Any]:
    return {
        "data": [],
        "layout": {
            "template": "plotly_dark",
            "paper_bgcolor": "#030712",
            "plot_bgcolor": "#111827",
            "title": {"text": title},
        },
    }


def legacy_lab_figure(data: dict[str, Any]) -> dict[str, Any]:
    combined = data["combined_labs"]
    if combined.empty:
        return empty_figure("Completed lab returns")
    ordered = combined.sort_values("generated_at")
    line_cols = [c for c in ["best_return_pct", "baseline_return_pct"] if c in ordered.columns]
    if not line_cols:
        return empty_figure("Completed lab returns")
    fig = px.line(ordered, x="generated_at", y=line_cols, markers=True, title="Completed lab returns")
    fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
    return fig


def legacy_telegram_figure(data: dict[str, Any]) -> dict[str, Any]:
    history = data["telegram_history"]
    if history.empty:
        return empty_figure("Telegram live paper equity")
    cols = [c for c in ["equity", "cash"] if c in history.columns]
    if not cols:
        return empty_figure("Telegram live paper equity")
    fig = px.line(history, x="timestamp", y=cols, markers=True, title="Telegram live paper equity")
    fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
    return fig


def table_payload(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if df is None or df.empty:
        return [], []
    show = df.copy()
    for col in show.columns:
        show[col] = _maybe_format_timeish_series(col, show[col], verbose=False)
    return show.to_dict("records"), [{"name": c, "id": c} for c in show.columns]


app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Auto Trader Ops"
app.config.suppress_callback_exceptions = True
app.index_string = """<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
        <style>
            /* Bloomberg-style tab overrides */
            .tab-container .tab {
                padding: 8px 14px !important;
                font-family: 'JetBrains Mono', monospace !important;
                font-size: 11px !important;
                letter-spacing: 1px !important;
                font-weight: 600 !important;
                color: #8899aa !important;
                border: 1px solid #1e2a3a !important;
                border-bottom: none !important;
                background: #0a0e17 !important;
                cursor: pointer !important;
                transition: all 0.15s ease !important;
            }
            .tab-container .tab--selected {
                color: #f5a623 !important;
                background: #111827 !important;
                border-color: #f5a623 !important;
                border-bottom: 2px solid #f5a623 !important;
            }
            .tab-container .tab:hover {
                color: #f5a623 !important;
                background: #111827 !important;
            }
            .tab-container {
                border-bottom: 1px solid #1e2a3a !important;
                margin-bottom: 4px !important;
            }
            /* Fix Dash table styling */
            .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner th {
                font-family: 'JetBrains Mono', monospace !important;
                font-size: 11px !important;
                color: #f5a623 !important;
            }
            .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner td {
                font-family: 'JetBrains Mono', monospace !important;
                font-size: 12px !important;
            }
            /* Remove Dash default margins */
            ._dash-app-content { padding: 0 !important; }
            body { margin: 0 !important; }
            /* Scrollbar styling */
            ::-webkit-scrollbar { width: 6px; height: 6px; }
            ::-webkit-scrollbar-track { background: #0a0e17; }
            ::-webkit-scrollbar-thumb { background: #2a3a4a; border-radius: 3px; }
            ::-webkit-scrollbar-thumb:hover { background: #3a4a5a; }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>"""
app.layout = html.Div(
    style=PAGE_STYLE,
    children=[
        dcc.Interval(id="refresh", interval=30_000, n_intervals=0),
        dcc.Interval(id="globe-anim", interval=2200, n_intervals=0),
        dcc.Store(id="data-version", data={"ts": 0.0}),
        html.H1("AUTO TRADER OPS", style={"fontSize": "18px", "fontWeight": "700", "letterSpacing": "2px", "color": BLOOMBERG_ORANGE, "marginBottom": "2px"}),
        html.Div("Live dashboard — service health, portfolios, paper trading, research, Telegram.", style={"color": BLOOMBERG_GRAY, "marginBottom": "8px", "fontSize": "11px"}),
        html.Div(id="last-updated", style={"color": "#5a6a7a", "marginBottom": "8px", "fontSize": "11px"}),
        html.Div(id="hero-row", style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        dcc.Tabs(
            id="main-tab",
            value="telegram",
            children=[
                dcc.Tab(label="OVERVIEW", value="overview"),
                dcc.Tab(label="RUNTIME", value="runtime"),
                dcc.Tab(label="PORTFOLIO", value="portfolio"),
                dcc.Tab(label="PAPER", value="paper"),
                dcc.Tab(label="NEWS", value="news"),
                dcc.Tab(label="GLOBAL MACRO", value="global_macro"),
                dcc.Tab(label="CALENDAR + SECTORS", value="calendar"),
                dcc.Tab(label="TELEGRAM", value="telegram"),
                dcc.Tab(label="RESEARCH", value="research"),
                dcc.Tab(label="REPORTS", value="reports"),
            ],
            colors={"border": "#1e2a3a", "primary": BLOOMBERG_ORANGE, "background": "#111827"},
            style={"borderBottom": f"1px solid #1e2a3a"},
        ),
        dcc.Loading(
            id="tab-loading",
            type="default",
            color=BLOOMBERG_ORANGE,
            children=html.Div(id="tab-content", style={"marginTop": "12px"}),
            style={"marginTop": "12px"},
        ),
    ],
)


def _safe_render(tab: str, data: dict[str, Any]) -> list[Any]:
    """Render a tab with error fallback so one broken tab doesn't kill the page."""
    try:
        return render_tab(tab, data)
    except Exception as exc:
        return [html.Div(
            [html.Div("ERROR LOADING TAB", style={"fontSize": "14px", "fontWeight": "700", "color": BLOOMBERG_RED}),
             html.Pre(str(exc), style={"fontSize": "11px", "color": "#9ca3af", "whiteSpace": "pre-wrap"})],
            style=CARD_STYLE,
        )]


@app.callback(
    Output("last-updated", "children"),
    Output("hero-row", "children"),
    Output("data-version", "data"),
    Input("refresh", "n_intervals"),
)
def refresh(_: int):
    try:
        data = collect_data()
        DASH_DATA_CACHE["ts"] = time.time()
        DASH_DATA_CACHE["data"] = data
        updated = f"⟳ {data['generated_at']} IST | refresh 30s | port 8504"
        return updated, build_hero(data), {"ts": DASH_DATA_CACHE["ts"]}
    except Exception as exc:
        import traceback
        traceback.print_exc()
        err_msg = f"Error at {datetime.now(IST).strftime('%H:%M:%S')} IST — {exc}"
        return err_msg, [], no_update


@app.callback(
    Output("tab-content", "children"),
    Input("main-tab", "value"),
    Input("data-version", "data"),
)
def render_active_tab(tab: str, _data_version: dict[str, Any] | None):
    try:
        data = DASH_DATA_CACHE.get("data")
        if not data:
            data = collect_data()
            DASH_DATA_CACHE["ts"] = time.time()
            DASH_DATA_CACHE["data"] = data
        return _safe_render(tab, data)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        err_msg = f"Error at {datetime.now(IST).strftime('%H:%M:%S')} IST — {exc}"
        return [html.Div(err_msg, style={**CARD_STYLE, "color": BLOOMBERG_RED})]


@app.callback(
    Output("macro-world-map", "figure"),
    Input("globe-anim", "n_intervals"),
    Input("main-tab", "value"),
    prevent_initial_call=True,
)
def animate_macro_world_map(n_intervals: int, active_tab: str):
    if active_tab != "global_macro":
        raise PreventUpdate
    macro = load_global_macro()
    if not macro:
        raise PreventUpdate
    rotation_lon = (78 + (n_intervals * 12)) % 360
    return build_world_map_figure(macro, rotation_lon=rotation_lon, pulse_phase=n_intervals)


# Legacy hidden-table callback removed — refresh and tab rendering are now decoupled


if __name__ == "__main__":
    host = os.environ.get("DASH_HOST", "0.0.0.0")
    port = int(os.environ.get("DASH_PORT", "8504"))
    app.run(host=host, port=port, debug=False)
