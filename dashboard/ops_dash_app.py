from __future__ import annotations

import json
import math
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html, dash_table

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
INTERMEDIARY_DIR = ROOT / "intermediary_files"
TWITTER_DIR = INTERMEDIARY_DIR / "twitter_sentiment"
LAB_STATUS_PATH = INTERMEDIARY_DIR / "lab_status" / "weekly_strategy_lab_status.json"
LIVE_TELEGRAM_LEDGER_PATH = REPORTS_DIR / "live_telegram_options_paper_latest.json"
LIVE_TELEGRAM_LEDGER_HISTORY = REPORTS_DIR / "live_telegram_options_paper_equity_history.jsonl"
SERVER_KEY = Path(os.getenv("AT_SERVER_KEY", os.path.expanduser("~/Desktop/Sahil_Oracle_Keys/ssh-key-2024-10-12.key")))
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
SSH_TTL_SECONDS = 45

PAGE_STYLE = {
    "background": "#030712",
    "color": "#f9fafb",
    "minHeight": "100vh",
    "padding": "20px",
    "fontFamily": "Inter, sans-serif",
}
CARD_STYLE = {
    "background": "#111827",
    "border": "1px solid #1f2937",
    "borderRadius": "14px",
    "padding": "16px",
}
TABLE_STYLE = {"overflowX": "auto"}
TABLE_CELL_STYLE = {
    "backgroundColor": "#111827",
    "color": "#f9fafb",
    "border": "1px solid #1f2937",
    "textAlign": "left",
    "padding": "8px",
    "whiteSpace": "normal",
    "height": "auto",
    "fontSize": "13px",
}
TABLE_HEADER_STYLE = {"backgroundColor": "#0f172a", "fontWeight": "bold", "color": "#f9fafb"}


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


def load_jsonl(path: Path) -> list[dict[str, Any]]:
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
    return rows


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


def safe_num(v: Any) -> float | None:
    try:
        if v is None:
            return None
        out = float(v)
        return None if math.isnan(out) else out
    except Exception:
        return None


def metric_card(title: str, value: Any, subtitle: str | None = None) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "14px", "color": "#9ca3af"}),
            html.Div(friendly(value), style={"fontSize": "28px", "fontWeight": "700", "marginTop": "6px"}),
            html.Div(subtitle or "", style={"fontSize": "12px", "color": "#94a3b8", "marginTop": "4px"}),
        ],
        style={**CARD_STYLE, "flex": "1", "minWidth": "180px"},
    )


def section(title: str, children: list[Any], subtitle: str | None = None) -> html.Div:
    header = [html.H3(title, style={"marginBottom": "6px"})]
    if subtitle:
        header.append(html.Div(subtitle, style={"color": "#94a3b8", "marginBottom": "12px"}))
    return html.Div(header + children, style={"marginTop": "18px"})


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
            show[col] = show[col].dt.strftime("%Y-%m-%d %H:%M:%S")
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
    rows = load_jsonl(LIVE_TELEGRAM_LEDGER_HISTORY)
    if not rows:
        return pd.DataFrame(columns=["timestamp", "equity", "cash"])
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce")
    for col in ["equity", "cash"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])


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
    scorecard_path, scorecard = latest_report("daily_scorecard_*.json")
    ops_path, daily_ops = latest_report("daily_ops_supervisor_*.json")
    portfolio_path, portfolio = latest_report("portfolio_intel_*.json")
    options_supervisor_path, options_supervisor = latest_report("options_research_supervisor_*.json")
    improvement_path, improvement = latest_report("daily_improvement_audit_*.json")
    five_year_path, five_year = latest_report("five_year_validation_*.json")
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
    server = fetch_server_snapshot()
    reports_df = recent_report_files(limit=120)
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        "server": server,
        "sentiment_df": load_sentiment_rows(),
        "topics_df": load_market_topics_rows(),
        "reports_df": reports_df,
    }


def build_hero(data: dict[str, Any]) -> list[Any]:
    combined = data["combined_labs"]
    latest_lab = combined.iloc[-1].to_dict() if not combined.empty else {}
    best_lab = combined.sort_values("best_return_pct", na_position="last").iloc[-1].to_dict() if not combined.empty else {}
    server = data["server"]
    return [
        metric_card("auto_trade.service", server.get("service", "unknown"), server.get("substate", "")),
        metric_card("Paper decision", data["paper"].get("decision", data["live_paper"].get("mode", "-")), data["paper"].get("symbol", "paper shadow")),
        metric_card("Telegram paper equity", data["telegram_ledger"].get("equity", "-"), f"cash {friendly(data['telegram_ledger'].get('cash'))}"),
        metric_card("Live portfolio value", data["portfolio"].get("total_value", "-"), data.get("portfolio_path") or "portfolio_intel"),
        metric_card("Latest lab return %", latest_lab.get("best_return_pct", "-"), latest_lab.get("best_name", "-")),
        metric_card("Best known 5y CAGR %", (data["five_year"].get("vol_sizing") or {}).get("cagr_pct", "-"), data.get("five_year_path") or "5y validation"),
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

    snapshots = html.Div(
        [
            html.Div([html.H4("Latest daily ops"), html.Pre(json.dumps(daily_ops, indent=2, default=str), style={"whiteSpace": "pre-wrap"})], style={**CARD_STYLE, "flex": "1", "minWidth": "320px"}),
            html.Div([html.H4("Latest hourly lab snapshot"), html.Pre(json.dumps(hourly, indent=2, default=str), style={"whiteSpace": "pre-wrap"})], style={**CARD_STYLE, "flex": "1", "minWidth": "320px"}),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Current snapshots", [snapshots]))
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
    children.append(section("Latest daily ops", [table_from_df(to_df([{
        "trade_date": daily_ops.get("trade_date"),
        "market_open": daily_ops.get("market_open"),
        "strategy_ok": (daily_ops.get("strategy_test") or {}).get("ok"),
        "paper_executed": (daily_ops.get("paper_trader") or {}).get("paper_executed"),
        "paper_decision": (daily_ops.get("paper_trader") or {}).get("decision"),
        "autopromote_reason": (daily_ops.get("autopromote") or {}).get("reason"),
    }]), "runtime-dailyops", page_size=5)]))
    children.append(section("Latest options supervisor", [table_from_df(to_df([{
        "trade_date": options_supervisor.get("trade_date"),
        "fetch_ok": (options_supervisor.get("fetch") or {}).get("ok"),
        "paper_ok": (options_supervisor.get("paper_shadow") or {}).get("ok"),
        "lab_ok": (options_supervisor.get("options_lab") or {}).get("ok"),
    }]), "runtime-options-supervisor", page_size=5)]))
    if server.get("journal"):
        children.append(section("Recent service logs", [html.Pre("\n".join(server["journal"]), style={**CARD_STYLE, "whiteSpace": "pre-wrap", "overflowX": "auto"})]))
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

    two_col = html.Div(
        [
            html.Div([html.H4("Equity paper snapshot"), html.Pre(json.dumps(paper, indent=2, default=str), style={"whiteSpace": "pre-wrap"})], style={**CARD_STYLE, "flex": "1", "minWidth": "340px"}),
            html.Div([html.H4("Live paper snapshot"), html.Pre(json.dumps(live_paper, indent=2, default=str), style={"whiteSpace": "pre-wrap"})], style={**CARD_STYLE, "flex": "1", "minWidth": "340px"}),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Snapshots", [two_col]))

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

    active = (news_payload.get("active") or [])[:3] if isinstance(news_payload, dict) else []
    for idx, item in enumerate(active, start=1):
        children.append(json_block(item, f"Sentiment snapshot {idx}: {item.get('symbol', 'unknown')}"))
    topics = (topics_payload.get("topics") or [])[:2] if isinstance(topics_payload, dict) else []
    for idx, item in enumerate(topics, start=1):
        children.append(json_block(item, f"Market topic {idx}: {item.get('label', item.get('topic', 'unknown'))}"))
    return children


def build_telegram_tab(data: dict[str, Any]) -> list[Any]:
    ledger = data["telegram_ledger"]
    history = data["telegram_history"]
    backtests = data["telegram_backtests"]
    rulesets = data["telegram_rulesets"]
    children: list[Any] = []

    cards = html.Div(
        [
            metric_card("Live equity", ledger.get("equity", "-"), "Telegram paper ledger"),
            metric_card("Cash", ledger.get("cash", "-"), "Telegram paper ledger"),
            metric_card("Unrealized PnL", ledger.get("unrealized_pnl", "-"), "Telegram paper ledger"),
            metric_card("Open positions", len(ledger.get("open_positions") or []), "Telegram paper ledger"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Telegram options paper", [cards]))

    if not history.empty:
        fig = px.line(history, x="timestamp", y=[c for c in ["equity", "cash"] if c in history.columns], markers=True, title="Telegram live paper equity")
        fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
        children.append(section("Equity curve", [dcc.Graph(figure=fig)]))

    children.append(section("Open positions", [table_from_df(to_df(ledger.get("open_positions") or []), "tg-open-table", page_size=10)]))
    children.append(section("Closed positions", [table_from_df(to_df(ledger.get("closed_positions") or []), "tg-closed-table", page_size=10)]))
    children.append(section("Backtest archive", [table_from_df(backtests.sort_values("generated_at", ascending=False) if not backtests.empty else backtests, "tg-backtests-table", page_size=12)]))

    if isinstance(rulesets, dict) and rulesets.get("rulesets"):
        rules_rows = []
        for name, payload in (rulesets.get("rulesets") or {}).items():
            rules_rows.append(
                {
                    "ruleset": name,
                    "use_case": payload.get("use_case"),
                    "confidence": payload.get("confidence"),
                    "signals": (payload.get("evidence") or {}).get("signals"),
                }
            )
        children.append(section("Channel scoring rules", [table_from_df(pd.DataFrame(rules_rows), "tg-rulesets-table", page_size=10)]))
        children.append(json_block(rulesets, f"Telegram rulesets raw, {data.get('rulesets_path') or 'latest'}"))
    return children


def build_research_tab(data: dict[str, Any]) -> list[Any]:
    combined = data["combined_labs"]
    improvement = data["improvement"]
    options_supervisor = data["options_supervisor"]
    five_year = data["five_year"]
    hourly = data["hourly_lab"]
    children: list[Any] = []

    five_vol = five_year.get("vol_sizing") or {}
    five_base = five_year.get("baseline") or {}
    cards = html.Div(
        [
            metric_card("5y vol-sizing CAGR %", five_vol.get("cagr_pct", "-"), data.get("five_year_path") or "5y validation"),
            metric_card("5y vol-sizing return %", five_vol.get("return_pct", "-"), f"drawdown {friendly(five_vol.get('drawdown_pct'))}%"),
            metric_card("5y baseline return %", five_base.get("return_pct", "-"), f"CAGR {friendly(five_base.get('cagr_pct'))}%"),
            metric_card("Improvement %", (five_year.get("improvement") or {}).get("return_pct", "-"), "vol sizing minus baseline"),
        ],
        style={"display": "flex", "gap": "12px", "flexWrap": "wrap"},
    )
    children.append(section("Research scoreboard", [cards]))

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

    children.append(json_block(hourly, f"Hourly lab status raw, {REPORTS_DIR / 'hourly_lab_status_latest.json'}"))
    children.append(json_block(options_supervisor, f"Options supervisor raw, {data.get('options_supervisor_path') or 'latest'}"))
    children.append(json_block(improvement, f"Improvement audit raw, {data.get('improvement_path') or 'latest'}"))
    return children


def build_reports_tab(data: dict[str, Any]) -> list[Any]:
    reports_df = data["reports_df"]
    children: list[Any] = [section("Recent report files", [table_from_df(reports_df, "reports-table", page_size=20)], "Everything currently being generated locally ends up here." )]
    latest_jsons = [
        ("Daily scorecard", data["scorecard"]),
        ("Daily ops supervisor", data["daily_ops"]),
        ("Portfolio intel", data["portfolio"]),
        ("Paper shadow", data["paper"]),
        ("Live paper shadow", data["live_paper"]),
        ("Options paper shadow", data["options_paper"]),
    ]
    for title, payload in latest_jsons:
        if payload:
            children.append(json_block(payload, title))
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
    if tab == "telegram":
        return build_telegram_tab(data)
    if tab == "research":
        return build_research_tab(data)
    if tab == "reports":
        return build_reports_tab(data)
    return build_overview_tab(data)


app = Dash(__name__)
app.title = "Auto Trader Ops"
app.layout = html.Div(
    style=PAGE_STYLE,
    children=[
        dcc.Interval(id="refresh", interval=30_000, n_intervals=0),
        html.H1("Auto Trader Ops"),
        html.Div("All-inclusive dashboard for service health, portfolios, paper trading, news, labs, Telegram, and reports.", style={"color": "#94a3b8", "marginBottom": "8px"}),
        html.Div(id="last-updated", style={"color": "#94a3b8", "marginBottom": "14px"}),
        html.Div(id="hero-row", style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        dcc.Tabs(
            id="main-tab",
            value="overview",
            children=[
                dcc.Tab(label="Overview", value="overview"),
                dcc.Tab(label="Runtime", value="runtime"),
                dcc.Tab(label="Portfolio", value="portfolio"),
                dcc.Tab(label="Paper", value="paper"),
                dcc.Tab(label="News", value="news"),
                dcc.Tab(label="Telegram", value="telegram"),
                dcc.Tab(label="Research", value="research"),
                dcc.Tab(label="Reports", value="reports"),
            ],
            colors={"border": "#1f2937", "primary": "#60a5fa", "background": "#0f172a"},
        ),
        html.Div(id="tab-content", style={"marginTop": "12px"}),
    ],
)


@app.callback(
    Output("last-updated", "children"),
    Output("hero-row", "children"),
    Output("tab-content", "children"),
    Input("refresh", "n_intervals"),
    Input("main-tab", "value"),
)
def refresh(_: int, tab: str):
    data = collect_data()
    updated = f"Last refresh: {data['generated_at']} | server cache TTL {SSH_TTL_SECONDS}s | default dashboard now points here on port 8504"
    return updated, build_hero(data), render_tab(tab, data)


if __name__ == "__main__":
    host = os.environ.get("DASH_HOST", "0.0.0.0")
    port = int(os.environ.get("DASH_PORT", "8504"))
    app.run(host=host, port=port, debug=False)
