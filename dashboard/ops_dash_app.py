from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, dcc, html, dash_table

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
INTERMEDIARY_DIR = ROOT / "intermediary_files"
LAB_STATUS_PATH = INTERMEDIARY_DIR / "lab_status" / "weekly_strategy_lab_status.json"
LIVE_TELEGRAM_LEDGER_PATH = REPORTS_DIR / "live_telegram_options_paper_latest.json"
LIVE_TELEGRAM_LEDGER_HISTORY = REPORTS_DIR / "live_telegram_options_paper_equity_history.jsonl"
COMBINED_LAB_STATUS_FILES = [
    "sizing_exit_sweep_latest.json",
    "volatility_sizing_lab_latest.json",
    "regime_filter_lab_latest.json",
    "focused_cluster_lab_latest.json",
]


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def recent_strategy_reports(limit: int = 20) -> list[tuple[Path, dict[str, Any]]]:
    rows: list[tuple[Path, dict[str, Any]]] = []
    for p in sorted(REPORTS_DIR.glob("strategy_lab_*.json"))[-limit:]:
        data = load_json(p)
        if data:
            rows.append((p, data))
    return rows


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
        payload = load_json(REPORTS_DIR / file_name) or {}
        if not payload:
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


def load_live_telegram_ledger() -> dict[str, Any]:
    return load_json(LIVE_TELEGRAM_LEDGER_PATH) or {}


def load_live_telegram_equity_history() -> pd.DataFrame:
    if not LIVE_TELEGRAM_LEDGER_HISTORY.exists():
        return pd.DataFrame(columns=["timestamp", "equity", "cash"])
    rows = []
    for line in LIVE_TELEGRAM_LEDGER_HISTORY.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["timestamp", "equity", "cash"])
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df.get("timestamp"), errors="coerce")
    for col in ["equity", "cash"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp")


def metric_card(title: str, value: Any, subtitle: str | None = None) -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "14px", "color": "#888"}),
            html.Div(str(value), style={"fontSize": "28px", "fontWeight": "700", "marginTop": "6px"}),
            html.Div(subtitle or "", style={"fontSize": "12px", "color": "#aaa", "marginTop": "4px"}),
        ],
        style={
            "background": "#111827",
            "border": "1px solid #1f2937",
            "borderRadius": "14px",
            "padding": "16px",
            "flex": "1",
            "minWidth": "180px",
        },
    )


app = Dash(__name__)
app.title = "Auto Trader Ops"
app.layout = html.Div(
    style={"background": "#030712", "color": "#f9fafb", "minHeight": "100vh", "padding": "20px", "fontFamily": "Inter, sans-serif"},
    children=[
        dcc.Interval(id="refresh", interval=20_000, n_intervals=0),
        html.H1("Auto Trader Ops"),
        html.Div("Dash migration, focused on cleaner research and Telegram analytics."),
        html.Br(),
        html.Div(id="hero-row", style={"display": "flex", "gap": "12px", "flexWrap": "wrap"}),
        html.Br(),
        dcc.Graph(id="lab-line"),
        dcc.Graph(id="telegram-line"),
        html.H3("Completed lab winners"),
        dash_table.DataTable(
            id="lab-table",
            page_size=12,
            sort_action="native",
            style_table={"overflowX": "auto"},
            style_cell={"backgroundColor": "#111827", "color": "#f9fafb", "border": "1px solid #1f2937", "textAlign": "left", "padding": "8px"},
            style_header={"backgroundColor": "#0f172a", "fontWeight": "bold"},
        ),
        html.Br(),
        html.H3("Live Telegram paper ledger"),
        dash_table.DataTable(
            id="telegram-open-table",
            page_size=10,
            style_table={"overflowX": "auto"},
            style_cell={"backgroundColor": "#111827", "color": "#f9fafb", "border": "1px solid #1f2937", "textAlign": "left", "padding": "8px"},
            style_header={"backgroundColor": "#0f172a", "fontWeight": "bold"},
        ),
    ],
)


@app.callback(
    Output("hero-row", "children"),
    Output("lab-line", "figure"),
    Output("telegram-line", "figure"),
    Output("lab-table", "data"),
    Output("lab-table", "columns"),
    Output("telegram-open-table", "data"),
    Output("telegram-open-table", "columns"),
    Input("refresh", "n_intervals"),
)
def refresh(_: int):
    combined = load_combined_lab_table()
    ledger = load_live_telegram_ledger()
    history = load_live_telegram_equity_history()
    latest_lab = combined.iloc[-1].to_dict() if not combined.empty else {}
    best_lab = combined.sort_values("best_return_pct").iloc[-1].to_dict() if not combined.empty else {}

    hero = [
        metric_card("Latest completed lab", latest_lab.get("best_name", "-"), latest_lab.get("source", "-")),
        metric_card("Latest return %", latest_lab.get("best_return_pct", "-"), latest_lab.get("report", "-")),
        metric_card("Best known return %", best_lab.get("best_return_pct", "-"), best_lab.get("best_name", "-")),
        metric_card("Telegram equity", ledger.get("equity", "-"), f"cash {ledger.get('cash', '-') }"),
    ]

    if combined.empty:
        lab_fig = px.scatter(title="No completed lab reports yet")
        lab_table_data, lab_table_cols = [], []
    else:
        line_cols = [c for c in ["best_return_pct", "baseline_return_pct"] if c in combined.columns]
        lab_fig = px.line(combined, x="generated_at", y=line_cols, markers=True, color_discrete_sequence=px.colors.qualitative.Set2, title="Completed lab returns")
        lab_fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")
        show = combined.sort_values("generated_at", ascending=False).copy()
        show["generated_at"] = show["generated_at"].dt.strftime("%Y-%m-%d %H:%M")
        cols = [c for c in ["generated_at", "source", "best_name", "best_return_pct", "best_score", "best_drawdown_pct", "tested_variants", "report"] if c in show.columns]
        lab_table_data = show[cols].to_dict("records")
        lab_table_cols = [{"name": c, "id": c} for c in cols]

    if history.empty:
        telegram_fig = px.scatter(title="No Telegram equity history yet")
    else:
        y_cols = [c for c in ["equity", "cash"] if c in history.columns]
        telegram_fig = px.line(history, x="timestamp", y=y_cols, markers=True, title="Telegram paper equity")
    telegram_fig.update_layout(template="plotly_dark", paper_bgcolor="#030712", plot_bgcolor="#111827")

    open_df = pd.DataFrame(ledger.get("open_positions") or [])
    if open_df.empty:
        telegram_data, telegram_cols = [], []
    else:
        telegram_data = open_df.to_dict("records")
        telegram_cols = [{"name": c, "id": c} for c in open_df.columns]

    return hero, lab_fig, telegram_fig, lab_table_data, lab_table_cols, telegram_data, telegram_cols


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)
