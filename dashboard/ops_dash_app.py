from __future__ import annotations

import json
import math
import os
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html, dash_table

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
INTERMEDIARY_DIR = ROOT / "intermediary_files"
TWITTER_DIR = INTERMEDIARY_DIR / "twitter_sentiment"
LAB_STATUS_PATH = INTERMEDIARY_DIR / "lab_status" / "weekly_strategy_lab_status.json"
LIVE_TELEGRAM_LEDGER_PATH = REPORTS_DIR / "live_telegram_options_paper_latest.json"
LIVE_TELEGRAM_LEDGER_HISTORY = REPORTS_DIR / "live_telegram_options_paper_equity_history.jsonl"
TELEGRAM_TRADE_AUDIT_PATH = REPORTS_DIR / "telegram_trade_audit_latest.json"
GLOBAL_MACRO_PATH = REPORTS_DIR / "global_macro_latest.json"
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
LAST_COMPACT_TS: float = 0.0
COMPACT_INTERVAL_SECONDS = 300  # compact JSONL every 5 min
SSH_TTL_SECONDS = 45
GLOBAL_MACRO_TTL_SECONDS = 600
IST = timezone(timedelta(hours=5, minutes=30))

def to_ist(dt_str: str | None) -> str:
    """Convert UTC/ISO timestamp to IST display string."""
    if not dt_str:
        return "-"
    try:
        dt = pd.Timestamp(dt_str)
        if pd.isna(dt):
            return "-"
        if dt.tzinfo is None:
            dt = dt.tz_localize("UTC")
        dt = dt.tz_convert(IST)
        return dt.strftime("%b %d, %H:%M")
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
        val_str_color = str(value).lower()
        if val_str_color in ("active", "running", "ok", "true", "yes"):
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


def table_from_df(df: pd.DataFrame, table_id: str, page_size: int = 10) -> dash_table.DataTable | html.Div:
    if df.empty:
        return empty_message("No data yet.")
    show = df.copy()
    for col in show.columns:
        if pd.api.types.is_datetime64_any_dtype(show[col]):
            try:
                show[col] = show[col].dt.tz_convert(IST)
            except Exception:
                pass
            show[col] = show[col].dt.strftime("%b %d, %H:%M")
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
    payload = load_json(TELEGRAM_TRADE_AUDIT_PATH)
    return payload if isinstance(payload, dict) else {}


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
    rulesets_path, telegram_rulesets = latest_report("telegram_channel_rulesets_*.json")
    paper = load_json(REPORTS_DIR / "paper_shadow_latest.json") or {}
    live_paper = load_json(REPORTS_DIR / "paper_shadow_live_latest.json") or {}
    options_paper = load_json(REPORTS_DIR / "paper_shadow_options_latest.json") or {}
    news_payload = load_json(REPORTS_DIR / "news_sentiment_latest.json") or {}
    topics_payload = load_json(REPORTS_DIR / "market_topics_latest.json") or {}
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
        "telegram_rulesets": telegram_rulesets or {},
        "paper": paper,
        "live_paper": live_paper,
        "options_paper": options_paper,
        "news_payload": news_payload,
        "topics_payload": topics_payload,
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
    return [
        metric_card("auto_trade.service", server.get("service", "unknown"), server.get("substate", "")),
        metric_card("Paper decision", data["paper"].get("decision", data["live_paper"].get("mode", "-")), data["paper"].get("symbol", "paper shadow")),
        metric_card("Telegram paper equity", data["telegram_ledger"].get("equity", "-"), f"cash {friendly(data['telegram_ledger'].get('cash'))}"),
        metric_card("Live portfolio value", data["portfolio"].get("total_value", "-"), data.get("portfolio_path") or "portfolio_intel"),
        metric_card("Latest lab return %", latest_lab.get("best_return_pct", "-"), latest_lab.get("best_name", "-")),
        metric_card("Best validated 5y CAGR %", validated_cagr, validated_name),
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
    return children


def build_paper_tab(data: dict[str, Any]) -> list[Any]:
    paper = data["paper"]
    live_paper = data["live_paper"]
    options_paper = data["options_paper"]
    ledger = data["telegram_ledger"]
    children: list[Any] = []

    cards = html.Div(
        [
            metric_card("Equity paper decision", paper.get("decision", "-"), paper.get("symbol", "paper shadow")),
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

    # Paper snapshot as clean cards instead of raw JSON
    paper_items = []
    if paper:
        for label, key, fmt in [('Decision', 'decision', str), ('Symbol', 'symbol', str), ('Score', 'selection_score', lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else str(x))]:
            val = paper.get(key)
            if val is not None:
                paper_items.append(html.Div([html.Span(label.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px"}), html.Span(f" {fmt(val)}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE})], style={"display": "inline-block", "marginRight": "16px"}))
    if live_paper:
        for label, key in [('Live mode', 'mode'), ('Time', 'time')]:
            val = live_paper.get(key)
            if val:
                paper_items.append(html.Div([html.Span(label.upper(), style={"fontSize": "10px", "color": BLOOMBERG_GRAY, "letterSpacing": "0.5px"}), html.Span(f" {val}", style={"fontSize": "13px", "fontWeight": "700", "color": BLOOMBERG_ORANGE})], style={"display": "inline-block", "marginRight": "16px"}))
    if paper_items:
        children.append(section("Paper trading state", [html.Div(paper_items, style={**CARD_STYLE, "display": "flex", "flexWrap": "wrap", "gap": "8px"})]))

    children.append(section("Open Telegram paper positions", [table_from_df(open_df, "paper-open-table", page_size=10)]))
    children.append(section("Closed Telegram paper positions", [table_from_df(closed_df, "paper-closed-table", page_size=10)]))
    children.append(section("Options paper near misses", [table_from_df(near_miss_df, "paper-nearmiss-table", page_size=10)]))
    return children


def build_news_tab(data: dict[str, Any]) -> list[Any]:
    paper = data["paper"]
    sentiment_df = data["sentiment_df"]
    topics_df = data["topics_df"]
    news_payload = data["news_payload"]
    topics_payload = data["topics_payload"]
    children: list[Any] = []

    symbol_news = paper.get("symbol_news_sentiment") or {}
    cards = html.Div(
        [
            metric_card("Tracked sentiment symbols", len(sentiment_df), data.get("news_payload", {}).get("generated_at", "news feed")),
            metric_card("Market topics", len(topics_df), data.get("topics_payload", {}).get("generated_at", "topic feeds")),
            metric_card("Current paper symbol sentiment", symbol_news.get("weighted_sentiment", "-"), paper.get("symbol", "NIFTYETF")),
            metric_card("Paper symbol headlines", symbol_news.get("item_count", "-"), ", ".join(symbol_news.get("dominant_types") or [])),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("News and sentiment", [cards], "This covers both symbol-level sentiment and macro topic feeds used by the paper overlay."))
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

    # ── TELEGRAM EQUITY ──
    shortterm = audit.get("shortterm01") or {}
    sunil_cash = audit.get("finance_with_sunil") or {}
    equity_rows = []
    equity_cards = html.Div(
        [
            metric_card("Equity channels", len([x for x in [shortterm, sunil_cash] if x]), "tracked"),
            metric_card("Shortterm01 signals", shortterm.get("signals_evaluated", 0), f"extracted {shortterm.get('signals_extracted', 0)}"),
            metric_card("Sunil cash signals", sunil_cash.get("signals_evaluated", 0), f"extracted {sunil_cash.get('signals_extracted', 0)}"),
            metric_card("Shortterm01 20d avg %", audit_stat(shortterm.get("summary") or {}, "ret_20d_pct", "avg"), f"positive rate {friendly(audit_stat(shortterm.get('summary') or {}, 'ret_20d_pct', 'positive_rate'))}"),
            metric_card("Shortterm01 tgt1 hit %", (audit_stat(shortterm.get("summary") or {}, "target_1_hit_20d", "hit_rate") or 0) * 100 if audit_stat(shortterm.get("summary") or {}, "target_1_hit_20d", "hit_rate") is not None else None, "within 20d"),
            metric_card("Sunil cash 20d avg %", audit_stat(sunil_cash.get("summary") or {}, "ret_20d_pct", "avg"), f"positive rate {friendly(audit_stat(sunil_cash.get('summary') or {}, 'ret_20d_pct', 'positive_rate'))}"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    equity_rows.append(equity_cards)

    equity_audit_rows = []
    for label, payload in [("Shortterm01 cash/equity", shortterm), ("FinanceWithSunil cash/equity", sunil_cash)]:
        if not payload:
            continue
        summary = payload.get("summary") or {}
        best_examples = summary.get("best_examples") or []
        sample = best_examples[0] if best_examples else {}
        equity_audit_rows.append({
            "channel": label,
            "signals_extracted": payload.get("signals_extracted", 0),
            "signals_evaluated": payload.get("signals_evaluated", 0),
            "20d_avg_%": audit_stat(summary, "ret_20d_pct", "avg"),
            "20d_positive_rate": audit_stat(summary, "ret_20d_pct", "positive_rate"),
            "max_20d_avg_%": audit_stat(summary, "max_20d_pct", "avg"),
            "best_symbol": sample.get("symbol"),
            "best_date": to_ist(sample.get("date")) if sample.get("date") else "-",
        })
    if equity_audit_rows:
        children.append(section("Telegram equity", [equity_cards, table_from_df(pd.DataFrame(equity_audit_rows), "tg-equity-audit-table", page_size=6)], "Separate cash/equity signal audit. If 20d stats are blank, the watched signal is too recent to score yet."))
    else:
        children.append(section("Telegram equity", [equity_cards, empty_message("No Telegram equity audit rows yet.")]))

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
    updated_at = to_ist(ledger.get("updated_at"))
    sunil_options = audit.get("finance_with_sunil_options") or {}
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
                score_rows.append({
                    "channel": chat,
                    "confidence": s.get("confidence", "-"),
                    "action": s.get("action", "-"),
                    "sizing_mult": s.get("sizing_mult", "-"),
                    "win_rate%": s.get("win_rate", "-"),
                    "avg_ret%": s.get("avg_return_pct", "-"),
                    "n_trades": s.get("n_trades", 0),
                    "ladder": s.get("ladder_style", "-"),
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


def build_global_macro_tab(data: dict[str, Any]) -> list[Any]:
    macro = load_global_macro()
    if not macro:
        return [empty_message("Global macro data not yet available. Refreshing in ~30s.")]

    children: list[Any] = []

    # ── 1. LIVE MARKET RIBBON ────────────────────────────────
    markets = macro.get("markets") or []
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
        map_lats, map_lons, map_texts, map_colors, map_sizes = [], [], [], [], []
        for m in markets:
            if m.get("status") != "ok" or m.get("lat") is None:
                continue
            ch = safe_num(m.get("change_pct"))
            map_lats.append(float(m["lat"]))
            map_lons.append(float(m["lon"]))
            map_texts.append(f"{m['label']}<br>{ch:+.2f}%" if ch is not None else f"{m['label']}<br>N/A")
            map_colors.append(ch if ch is not None else 0)
            map_sizes.append(18 if m.get("kind") == "equity" else 14)

        # Add event markers
        events = macro.get("events") or []
        ev_lats, ev_lons, ev_texts, ev_colors, ev_sizes = [], [], [], [], []
        for e in events:
            sev = e.get("severity", 0)
            if sev < 15:
                continue
            ev_lats.append(float(e.get("lat", 0)))
            ev_lons.append(float(e.get("lon", 0)))
            ev_texts.append(f"⚡ {e['label']}<br>severity {sev}")
            ev_colors.append(-sev / 10.0)  # negative = warm = risk
            ev_sizes.append(max(12, min(30, sev)))

        fig = go.Figure()

        # Market markers
        if map_lats:
            fig.add_trace(go.Scattergeo(
                lon=map_lons, lat=map_lats, text=map_texts,
                marker=dict(
                    size=map_sizes, color=map_colors,
                    colorscale=[[0, BLOOMBERG_RED], [0.5, BLOOMBERG_ORANGE], [1, BLOOMBERG_GREEN]],
                    cmin=-3, cmax=3, line=dict(width=1, color="#1e2a3a"),
                    colorbar=dict(title="% chg", thickness=10, tickfont=dict(size=9, color=BLOOMBERG_GRAY), titlefont=dict(size=9, color=BLOOMBERG_GRAY)),
                ),
                mode="markers+text", textposition="top center",
                textfont=dict(size=9, color="#c0c0c0"),
                name="Markets",
            ))

        # Event markers
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
                projection_type="natural earth",
                showland=True, landcolor="#111827", showocean=True, oceancolor="#0a0e17",
                showcountries=True, countrycolor="#1e2a3a", showlakes=False,
                coastlinecolor="#1e2a3a",
            ),
            paper_bgcolor="#030712", plot_bgcolor="#111827",
            margin=dict(l=0, r=0, t=30, b=0),
            height=420,
            legend=dict(font=dict(size=10, color=BLOOMBERG_GRAY)),
        )
        fig.update_layout(title_font=dict(size=12, color=BLOOMBERG_ORANGE))
        children.append(section("WORLD MAP — markets & event hotspots", [dcc.Graph(figure=fig)], "Green = up, Red = down ◆ = geopolitical event severity"))
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
        if pd.api.types.is_datetime64_any_dtype(show[col]):
            try:
                show[col] = show[col].dt.tz_convert(IST)
            except Exception:
                pass
            show[col] = show[col].dt.strftime("%b %d, %H:%M")
    return show.to_dict("records"), [{"name": c, "id": c} for c in show.columns]


app = Dash(__name__)
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
                dcc.Tab(label="TELEGRAM", value="telegram"),
                dcc.Tab(label="RESEARCH", value="research"),
                dcc.Tab(label="REPORTS", value="reports"),
            ],
            colors={"border": "#1e2a3a", "primary": BLOOMBERG_ORANGE, "background": "#111827"},
            style={"borderBottom": f"1px solid #1e2a3a"},
        ),
        html.Div(id="tab-content", style={"marginTop": "12px"}),
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
    Output("tab-content", "children"),
    Input("refresh", "n_intervals"),
    Input("main-tab", "value"),
)
def refresh(_: int, tab: str):
    try:
        data = collect_data()
        updated = f"⟳ {data['generated_at']} IST | refresh 30s | port 8504"
        return updated, build_hero(data), _safe_render(tab, data)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        err_msg = f"Error at {datetime.now(IST).strftime('%H:%M:%S')} IST — {exc}"
        return err_msg, [], [html.Div(err_msg, style={**CARD_STYLE, "color": BLOOMBERG_RED})]


# Legacy hidden-table callback removed — single refresh callback handles all rendering


if __name__ == "__main__":
    host = os.environ.get("DASH_HOST", "0.0.0.0")
    port = int(os.environ.get("DASH_PORT", "8504"))
    app.run(host=host, port=port, debug=False)
