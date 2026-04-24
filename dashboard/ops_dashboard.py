from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from mf_app_core import render_mf_fire_app

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
INTERMEDIARY_DIR = ROOT / "intermediary_files"
TWITTER_DIR = INTERMEDIARY_DIR / "twitter_sentiment"
LAB_STATUS_PATH = INTERMEDIARY_DIR / "lab_status" / "weekly_strategy_lab_status.json"
LIVE_TELEGRAM_LEDGER_PATH = REPORTS_DIR / "live_telegram_options_paper_latest.json"
LIVE_TELEGRAM_LEDGER_HISTORY = REPORTS_DIR / "live_telegram_options_paper_equity_history.jsonl"
SERVER_KEY = Path(os.getenv("AT_SERVER_KEY", os.path.expanduser("~/.openclaw/credentials/oracle_ssh_key")))
SERVER_HOST = os.getenv("AT_SERVER_HOST", os.getenv("AT_ORACLE", ""))
SERVER_REPO = os.getenv("AT_SERVER_REPO", "/home/ubuntu/Auto_Trader")
COMBINED_LAB_STATUS_FILES = [
    "sizing_exit_sweep_latest.json",
    "volatility_sizing_lab_latest.json",
    "regime_filter_lab_latest.json",
    "focused_cluster_lab_latest.json",
]

st.set_page_config(page_title="Auto Trader Ops Dashboard", layout="wide")


@st.cache_data(ttl=10)
def load_json(path: str | Path) -> dict | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


@st.cache_data(ttl=20)
def latest_report(pattern: str) -> tuple[Path | None, dict | None]:
    paths = sorted(REPORTS_DIR.glob(pattern))
    if not paths:
        return None, None
    path = paths[-1]
    return path, load_json(path)


@st.cache_data(ttl=20)
def recent_strategy_reports(limit: int = 20) -> list[tuple[Path, dict]]:
    rows: list[tuple[Path, dict]] = []
    for p in sorted(REPORTS_DIR.glob("strategy_lab_*.json"))[-limit:]:
        data = load_json(p)
        if data:
            rows.append((p, data))
    return rows


@st.cache_data(ttl=20)
def load_lab_table(limit: int = 20) -> pd.DataFrame:
    rows = []
    for path, data in recent_strategy_reports(limit=limit):
        rec = data.get("recommendation") or {}
        best = rec.get("best") or {}
        baseline = rec.get("baseline") or {}
        rnn_ctx = rec.get("rnn_context") or {}
        rows.append(
            {
                "report": path.name,
                "generated_at": rec.get("generated_at"),
                "best_name": best.get("name"),
                "best_return_pct": best.get("total_return_pct"),
                "best_score": best.get("selection_score"),
                "best_drawdown_pct": best.get("max_drawdown_pct"),
                "best_rnn_enabled": best.get("rnn_enabled", False),
                "best_rnn_accuracy": best.get("rnn_avg_test_accuracy", 0.0),
                "baseline_return_pct": baseline.get("total_return_pct"),
                "improvement_return_pct": rec.get("improvement_return_pct"),
                "improvement_score": rec.get("improvement_score"),
                "should_promote": rec.get("should_promote"),
                "universe_size": len((rec.get("data_context") or {}).get("loaded_symbols") or []),
                "rnn_models_built": rnn_ctx.get("models_built", 0),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(ttl=20)
def load_combined_lab_table(limit: int = 20) -> pd.DataFrame:
    rows = []
    for path, data in recent_strategy_reports(limit=limit):
        rec = data.get("recommendation") or {}
        best = rec.get("best") or {}
        baseline = rec.get("baseline") or {}
        rows.append(
            {
                "report": path.name,
                "lab_type": rec.get("lab_type") or "strategy_lab",
                "source": "strategy_lab",
                "generated_at": rec.get("generated_at"),
                "best_name": best.get("name"),
                "best_return_pct": best.get("total_return_pct"),
                "best_score": best.get("selection_score"),
                "best_drawdown_pct": best.get("max_drawdown_pct"),
                "baseline_return_pct": baseline.get("total_return_pct"),
                "improvement_return_pct": rec.get("improvement_return_pct"),
                "improvement_score": rec.get("improvement_score"),
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
                "lab_type": payload.get("message") or file_name.removesuffix("_latest.json"),
                "source": file_name.removesuffix("_latest.json"),
                "generated_at": payload.get("generated_at"),
                "best_name": payload.get("best_variant"),
                "best_return_pct": payload.get("best_return_pct"),
                "best_score": payload.get("best_score"),
                "best_drawdown_pct": payload.get("best_drawdown_pct"),
                "baseline_return_pct": None,
                "improvement_return_pct": None,
                "improvement_score": None,
                "tested_variants": payload.get("variants_done") or payload.get("variants_total"),
            }
        )

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "generated_at" in df.columns:
        df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    return df.dropna(subset=["generated_at"]).sort_values("generated_at")


@st.cache_data(ttl=20)
def load_ranked_variants(report_name: str) -> pd.DataFrame:
    data = load_json(REPORTS_DIR / report_name)
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data.get("ranked") or [])


@st.cache_data(ttl=20)
def recent_telegram_options_reports(limit: int = 20) -> list[tuple[Path, dict]]:
    rows: list[tuple[Path, dict]] = []
    for p in sorted(REPORTS_DIR.glob("telegram_options_paper*.json"))[-limit:]:
        data = load_json(p)
        if data:
            rows.append((p, data))
    return rows


@st.cache_data(ttl=20)
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
                "target_style": data.get("target_style"),
                "max_hold_bars": data.get("max_hold_bars"),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(ttl=10)
def load_live_telegram_ledger() -> dict:
    return load_json(LIVE_TELEGRAM_LEDGER_PATH) or {}


@st.cache_data(ttl=10)
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
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for col in ["equity", "cash"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])


@st.cache_data(ttl=10)
def load_sentiment_rows() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if TWITTER_DIR.exists():
        for p in sorted(TWITTER_DIR.glob("*.json")):
            if p.name == "latest.json":
                continue
            data = load_json(p)
            if not data:
                continue
            bias = data.get("trade_bias") or {}
            rows.append(
                {
                    "symbol": data.get("symbol", p.stem),
                    "status": data.get("status"),
                    "tweet_count": data.get("tweet_count", 0),
                    "weighted_sentiment": data.get("weighted_sentiment", 0.0),
                    "dominant_types": ", ".join(data.get("dominant_types") or []),
                    "block_buy": bool(bias.get("block_buy")),
                    "force_sell": bool(bias.get("force_sell")),
                    "path": str(p),
                }
            )
    return pd.DataFrame(rows)


@st.cache_data(ttl=10)
def load_lab_status() -> dict:
    return load_json(LAB_STATUS_PATH) or {}


@st.cache_data(ttl=30)
def ssh_fetch() -> dict[str, Any]:
    if not SERVER_KEY.exists():
        return {"ok": False, "error": f"Missing SSH key: {SERVER_KEY}"}
    cmd = (
        f"cd {SERVER_REPO} && "
        "echo '[service]' && systemctl is-active auto_trade.service || true && "
        "echo '[reports]' && ls reports | tail -20 && "
        "echo '[paper]' && cat reports/paper_shadow_latest.json 2>/dev/null || true && "
        "echo '[livepaper]' && cat reports/paper_shadow_live_latest.json 2>/dev/null || true"
    )
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
                cmd,
            ],
            capture_output=True,
            text=True,
            timeout=25,
        )
        return {"ok": proc.returncode == 0, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def status_badge_text(status: dict) -> str:
    phase = status.get("phase") or "idle"
    state = status.get("status") or "idle"
    pct = status.get("progress_pct")
    if pct is None:
        return f"{state} • {phase}"
    return f"{state} • {phase} • {pct}%"


st.title("Auto Trader Ops Dashboard")
st.caption("A clearer view of live, paper, labs, RNN sweeps, sentiment, server state, and MF planning")

with st.sidebar:
    st.header("Controls")
    auto_refresh = st.toggle("Auto refresh", value=True)
    if auto_refresh:
        st.caption("Refresh the page every 15 to 30s for live lab status.")
    if st.button("Refresh cached data"):
        load_json.clear()
        latest_report.clear()
        recent_strategy_reports.clear()
        load_lab_table.clear()
        load_combined_lab_table.clear()
        load_ranked_variants.clear()
        load_sentiment_rows.clear()
        load_lab_status.clear()
        ssh_fetch.clear()
    st.divider()
    st.code("streamlit run dashboard/ops_dashboard.py")

scorecard_path, scorecard = latest_report("daily_scorecard_*.json")
portfolio_path, portfolio = latest_report("portfolio_intel_*.json")
paper = load_json(REPORTS_DIR / "paper_shadow_latest.json") or {}
live_paper = load_json(REPORTS_DIR / "paper_shadow_live_latest.json") or {}
lab_df = load_lab_table()
combined_lab_df = load_combined_lab_table()
lab_status = load_lab_status()
sentiment_df = load_sentiment_rows()
telegram_options_df = load_telegram_options_table()
live_telegram_ledger = load_live_telegram_ledger()
live_telegram_history = load_live_telegram_equity_history()
latest_lab = combined_lab_df.iloc[-1].to_dict() if not combined_lab_df.empty else {}
best_lab = combined_lab_df.sort_values("best_return_pct").iloc[-1].to_dict() if not combined_lab_df.empty else {}

hero1, hero2, hero3, hero4, hero5, hero6 = st.columns(6)
hero1.metric("Lab status", status_badge_text(lab_status) if lab_status else "idle")
hero2.metric("Latest best return %", latest_lab.get("best_return_pct", "-"))
hero3.metric("Best known return %", best_lab.get("best_return_pct", "-"))
hero4.metric("Paper decision", paper.get("decision", live_paper.get("mode", "-")))
hero5.metric("Sentiment symbols", len(sentiment_df))
hero6.metric("Telegram paper equity", live_telegram_ledger.get("equity", "-"))

if lab_status:
    progress = float(lab_status.get("progress_pct", 0.0) or 0.0)
    if lab_status.get("status") == "running":
        st.progress(min(100, int(progress)), text=f"Lab running, {lab_status.get('current_variant') or lab_status.get('current_symbol') or lab_status.get('message', 'working')}")
    elif lab_status.get("status") == "failed":
        st.error(f"Latest lab run failed: {lab_status.get('error', 'unknown error')}")

if latest_lab:
    st.success(f"Latest completed lab: {latest_lab.get('best_name', 'n/a')} | best return {latest_lab.get('best_return_pct', '-')} | source {latest_lab.get('source', latest_lab.get('report', 'n/a'))}")
if best_lab and latest_lab.get('report') != best_lab.get('report'):
    st.info(f"Best known completed lab: {best_lab.get('best_name', 'n/a')} | return {best_lab.get('best_return_pct', '-')} | source {best_lab.get('source', best_lab.get('report', 'n/a'))}")

quick1, quick2, quick3, quick4 = st.columns(4)
if scorecard:
    quick1.metric("Daily orders", scorecard.get("orders", "-"))
    quick2.metric("Daily trades", scorecard.get("trades", "-"))
    quick3.metric("Realized PnL", scorecard.get("estimated_realized_pnl", "-"))
    quick4.metric("Scorecard verdict", scorecard.get("verdict", "-"))

summary_tab, trader_tab, mf_tab, telegram_tab, labs_tab, sentiment_tab, server_tab = st.tabs([
    "Mission Control",
    "Live + Paper",
    "MF FIRE",
    "Telegram Options",
    "Labs + RNN",
    "Twitter Sentiment",
    "Server",
])

with summary_tab:
    left, right = st.columns([1.15, 0.85])
    with left:
        st.subheader("Lab timeline")
        if combined_lab_df.empty:
            st.info("No lab reports found yet.")
        else:
            ordered = combined_lab_df.sort_values("generated_at")
            line_cols = [c for c in ["best_return_pct", "baseline_return_pct"] if c in ordered.columns]
            fig = px.line(
                ordered,
                x="generated_at",
                y=line_cols,
                markers=True,
                color_discrete_sequence=px.colors.qualitative.Set2,
                title="Best vs baseline return across recent lab runs",
            )
            st.plotly_chart(fig, use_container_width=True)
            fig2 = px.scatter(
                ordered,
                x="best_drawdown_pct",
                y="best_return_pct",
                color="source",
                size=ordered.get("tested_variants", pd.Series([1] * len(ordered))).fillna(1) + 0.1,
                hover_name="report",
                title="Completed lab winners, return vs drawdown",
            )
            st.plotly_chart(fig2, use_container_width=True)
            st.dataframe(
                ordered.sort_values("generated_at", ascending=False)[[c for c in ["generated_at", "source", "best_name", "best_return_pct", "best_score", "best_drawdown_pct", "tested_variants", "report"] if c in ordered.columns]],
                use_container_width=True,
                hide_index=True,
            )
    with right:
        st.subheader("What is happening now")
        if lab_status:
            st.json(lab_status)
        else:
            st.info("No active lab status file yet.")
        st.subheader("Latest important snapshots")
        if live_paper:
            st.markdown("**Live paper snapshot**")
            st.json(live_paper)
        if paper:
            st.markdown("**Paper snapshot**")
            st.json(paper)

with trader_tab:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Paper trader")
        st.json(paper or {"status": "missing"})
    with c2:
        st.subheader("Live paper trader")
        st.json(live_paper or {"status": "missing"})
    st.subheader("Available local report files")
    st.dataframe(pd.DataFrame({"report": sorted([p.name for p in REPORTS_DIR.glob("*.json")], reverse=True)}), use_container_width=True, hide_index=True)

with mf_tab:
    render_mf_fire_app(embed=True)

with telegram_tab:
    st.subheader("Telegram options paper returns")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Live equity", live_telegram_ledger.get("equity", "-"))
    t2.metric("Cash", live_telegram_ledger.get("cash", "-"))
    t3.metric("Realized PnL", live_telegram_ledger.get("realized_pnl", "-"))
    t4.metric("Unrealized PnL", live_telegram_ledger.get("unrealized_pnl", "-"))

    left, right = st.columns([1.2, 0.8])
    with left:
        st.markdown("**Live equity curve**")
        if live_telegram_history.empty:
            st.info("No live Telegram equity history yet. Run the live ledger a few times to accumulate snapshots.")
        else:
            fig = px.line(live_telegram_history, x="timestamp", y=[c for c in ["equity", "cash"] if c in live_telegram_history.columns], markers=True, title="Live Telegram paper ledger")
            st.plotly_chart(fig, use_container_width=True)
    with right:
        st.markdown("**Current live ledger snapshot**")
        if live_telegram_ledger:
            st.json(live_telegram_ledger)
        else:
            st.info("Live Telegram ledger not found yet.")

    open_df = pd.DataFrame(live_telegram_ledger.get("open_positions") or [])
    closed_df = pd.DataFrame(live_telegram_ledger.get("closed_positions") or [])
    if not open_df.empty:
        st.markdown("**Open paper positions**")
        st.dataframe(open_df, use_container_width=True, hide_index=True)
    if not closed_df.empty:
        st.markdown("**Closed paper positions**")
        st.dataframe(closed_df, use_container_width=True, hide_index=True)

    weekly = pd.DataFrame(live_telegram_ledger.get("weekly_returns") or [])
    monthly = pd.DataFrame(live_telegram_ledger.get("monthly_returns") or [])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Weekly returns**")
        if weekly.empty:
            st.info("No weekly return snapshots yet.")
        else:
            st.plotly_chart(px.bar(weekly, x="period", y="return_pct", title="Weekly return %"), use_container_width=True)
            st.dataframe(weekly, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Monthly returns**")
        if monthly.empty:
            st.info("No monthly return snapshots yet.")
        else:
            st.plotly_chart(px.bar(monthly, x="period", y="return_pct", title="Monthly return %"), use_container_width=True)
            st.dataframe(monthly, use_container_width=True, hide_index=True)

    st.markdown("**Historical Telegram options paper backtests**")
    if telegram_options_df.empty:
        st.info("No Telegram options paper backtest reports found.")
    else:
        ordered = telegram_options_df.sort_values("generated_at", ascending=False)
        st.dataframe(ordered, use_container_width=True, hide_index=True)
        fig = px.bar(ordered, x="report", y="total_return_pct", color="channel", title="Backtest return by report")
        st.plotly_chart(fig, use_container_width=True)
        fig2 = px.scatter(ordered, x="win_rate_pct", y="total_return_pct", size=ordered["trades_simulated"].fillna(0) + 0.1, hover_name="report", title="Return vs win rate")
        st.plotly_chart(fig2, use_container_width=True)

with labs_tab:
    st.subheader("Lab reports")
    if combined_lab_df.empty:
        st.info("No completed lab reports found.")
    else:
        st.markdown("**Completed lab summary**")
        st.dataframe(combined_lab_df.sort_values("generated_at", ascending=False), use_container_width=True, hide_index=True)

    st.markdown("**Strategy lab deep dive**")
    if lab_df.empty:
        st.info("No strategy lab reports found.")
    else:
        st.dataframe(lab_df.sort_values("generated_at", ascending=False), use_container_width=True)
        report_options = list(reversed([p.name for p, _ in recent_strategy_reports(limit=25)]))
        selected = st.selectbox("Inspect strategy lab report", options=report_options)
        ranked_df = load_ranked_variants(selected)
        if ranked_df.empty:
            st.warning("No ranked variants in that report.")
        else:
            cols = [c for c in ["name", "total_return_pct", "selection_score", "max_drawdown_pct", "trades", "win_rate_pct", "rnn_enabled", "rnn_avg_test_accuracy"] if c in ranked_df.columns]
            st.dataframe(ranked_df[cols].head(25), use_container_width=True)
            top = ranked_df.head(15).copy()
            st.plotly_chart(px.bar(top, x="name", y="selection_score", color="rnn_enabled", title="Top variants by score"), use_container_width=True)
            st.plotly_chart(px.scatter(top, x="max_drawdown_pct", y="total_return_pct", color="rnn_enabled", size="trades", hover_name="name", title="Variants, return vs drawdown"), use_container_width=True)

with sentiment_tab:
    st.subheader("Sentiment overview")
    if sentiment_df.empty:
        st.info("No sentiment snapshots found yet.")
    else:
        st.dataframe(sentiment_df[["symbol", "status", "tweet_count", "weighted_sentiment", "dominant_types", "block_buy", "force_sell"]], use_container_width=True)
        st.plotly_chart(px.bar(sentiment_df, x="symbol", y="weighted_sentiment", color="status", title="Weighted sentiment by symbol"), use_container_width=True)
        pick = st.selectbox("Inspect sentiment snapshot", options=sentiment_df["symbol"].tolist())
        row = sentiment_df[sentiment_df["symbol"] == pick].iloc[0].to_dict()
        st.json(load_json(row["path"]))

with server_tab:
    st.subheader("Oracle server")
    st.caption(f"Host: {SERVER_HOST} | Repo: {SERVER_REPO}")
    if st.button("Refresh server snapshot"):
        ssh_fetch.clear()
    server = ssh_fetch()
    if not server.get("ok"):
        st.warning(server.get("error") or server.get("stderr") or "Server fetch failed")
    else:
        raw = server.get("stdout", "")
        lines = raw.splitlines()
        service_state = "unknown"
        if "[service]" in lines:
            try:
                service_state = lines[lines.index("[service]") + 1].strip()
            except Exception:
                pass
        st.metric("auto_trade.service", service_state)
        st.text_area("Raw server snapshot", raw, height=400)

if auto_refresh:
    st.markdown(
        "<meta http-equiv='refresh' content='20'>",
        unsafe_allow_html=True,
    )
