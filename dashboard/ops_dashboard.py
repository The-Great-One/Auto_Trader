from __future__ import annotations

import glob
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
INTERMEDIARY_DIR = ROOT / "intermediary_files"
TWITTER_DIR = INTERMEDIARY_DIR / "twitter_sentiment"
SERVER_KEY = Path("/Users/sahilgoel/Desktop/Sahil_Oracle_Keys/ssh-key-2024-10-12.key")
SERVER_HOST = os.getenv("AT_SERVER_HOST", "ubuntu@168.138.114.147")
SERVER_REPO = os.getenv("AT_SERVER_REPO", "/home/ubuntu/Auto_Trader")

st.set_page_config(page_title="Auto Trader Ops Dashboard", layout="wide")


@st.cache_data(ttl=15)
def load_json(path: str | Path) -> dict | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


@st.cache_data(ttl=15)
def latest_report(pattern: str) -> tuple[Path | None, dict | None]:
    paths = sorted(REPORTS_DIR.glob(pattern))
    if not paths:
        return None, None
    p = paths[-1]
    return p, load_json(p)


@st.cache_data(ttl=15)
def recent_strategy_reports(limit: int = 12) -> list[tuple[Path, dict]]:
    out: list[tuple[Path, dict]] = []
    for p in sorted(REPORTS_DIR.glob("strategy_lab_*.json"))[-limit:]:
        data = load_json(p)
        if data:
            out.append((p, data))
    return out


@st.cache_data(ttl=15)
def load_sentiment_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not TWITTER_DIR.exists():
        return rows
    for p in sorted(TWITTER_DIR.glob("*.json")):
        if p.name == "latest.json":
            continue
        data = load_json(p)
        if not data:
            continue
        rows.append(
            {
                "symbol": data.get("symbol", p.stem),
                "status": data.get("status"),
                "tweet_count": data.get("tweet_count", 0),
                "weighted_sentiment": data.get("weighted_sentiment", 0.0),
                "dominant_types": ", ".join(data.get("dominant_types") or []),
                "block_buy": bool((data.get("trade_bias") or {}).get("block_buy")),
                "force_sell": bool((data.get("trade_bias") or {}).get("force_sell")),
                "path": str(p),
            }
        )
    return rows


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
    ssh_cmd = [
        "ssh",
        "-i",
        str(SERVER_KEY),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=10",
        SERVER_HOST,
        cmd,
    ]
    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=25)
        return {
            "ok": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@st.cache_data(ttl=30)
def load_lab_table(limit: int = 12) -> pd.DataFrame:
    rows = []
    for path, data in recent_strategy_reports(limit=limit):
        rec = data.get("recommendation") or {}
        best = rec.get("best") or {}
        baseline = rec.get("baseline") or {}
        rows.append(
            {
                "report": path.name,
                "generated_at": rec.get("generated_at"),
                "best_name": best.get("name"),
                "best_return_pct": best.get("total_return_pct"),
                "best_score": best.get("selection_score"),
                "best_drawdown_pct": best.get("max_drawdown_pct"),
                "rnn_enabled": best.get("rnn_enabled", False),
                "baseline_return_pct": baseline.get("total_return_pct"),
                "baseline_score": baseline.get("selection_score"),
                "improvement_return_pct": rec.get("improvement_return_pct"),
                "improvement_score": rec.get("improvement_score"),
                "should_promote": rec.get("should_promote"),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data(ttl=30)
def load_ranked_variants(report_name: str) -> pd.DataFrame:
    data = load_json(REPORTS_DIR / report_name)
    if not data:
        return pd.DataFrame()
    ranked = data.get("ranked") or []
    return pd.DataFrame(ranked)


st.title("Auto Trader Ops Dashboard")
st.caption("Live, paper, labs, sentiment, and server visibility in one place")

col_a, col_b, col_c, col_d = st.columns(4)
scorecard_path, scorecard = latest_report("daily_scorecard_*.json")
portfolio_path, portfolio = latest_report("portfolio_intel_*.json")
paper = load_json(REPORTS_DIR / "paper_shadow_latest.json") or {}
live_paper = load_json(REPORTS_DIR / "paper_shadow_live_latest.json") or {}
lab_df = load_lab_table()
latest_lab_row = lab_df.iloc[-1].to_dict() if not lab_df.empty else {}

col_a.metric("Latest lab best return %", latest_lab_row.get("best_return_pct", "-"))
col_b.metric("Latest lab best score", latest_lab_row.get("best_score", "-"))
col_c.metric("Paper decision", paper.get("decision", live_paper.get("mode", "-")))
col_d.metric("Live paper BUY/SELL", f"{live_paper.get('buy_count', 0)}/{live_paper.get('sell_count', 0)}")

if scorecard or portfolio:
    sc1, sc2, sc3, sc4 = st.columns(4)
    if scorecard:
        sc1.metric("Daily orders", scorecard.get("orders", "-"))
        sc2.metric("Daily trades", scorecard.get("trades", "-"))
        sc3.metric("Realized PnL", scorecard.get("estimated_realized_pnl", "-"))
        sc4.metric("Scorecard verdict", scorecard.get("verdict", "-"))


tab_overview, tab_trader, tab_labs, tab_sentiment, tab_server = st.tabs(
    ["Overview", "Live + Paper", "Labs", "Twitter Sentiment", "Server"]
)

with tab_overview:
    left, right = st.columns([1.1, 0.9])
    with left:
        st.subheader("Recent lab runs")
        if lab_df.empty:
            st.info("No lab reports found yet.")
        else:
            st.dataframe(lab_df.sort_values("generated_at", ascending=False), use_container_width=True)
            fig = px.scatter(
                lab_df,
                x="best_drawdown_pct",
                y="best_return_pct",
                color="rnn_enabled",
                hover_name="report",
                size=lab_df["improvement_score"].abs().fillna(0) + 0.1,
                title="Lab best variant, return vs drawdown",
            )
            st.plotly_chart(fig, use_container_width=True)
    with right:
        st.subheader("Latest snapshots")
        if paper:
            st.markdown("**Paper snapshot**")
            st.json(paper)
        if live_paper:
            st.markdown("**Live paper stream snapshot**")
            st.json(live_paper)
        if portfolio:
            st.markdown("**Portfolio intelligence**")
            st.json(portfolio)

with tab_trader:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Paper trader")
        if paper:
            st.json(paper)
        else:
            st.info("No local paper snapshot found.")
    with c2:
        st.subheader("Live paper decision stream")
        if live_paper:
            st.json(live_paper)
        else:
            st.info("No live paper snapshot found.")

    st.subheader("Available local reports")
    report_files = sorted([p.name for p in REPORTS_DIR.glob("*.json")], reverse=True)
    st.dataframe(pd.DataFrame({"report": report_files}), use_container_width=True, hide_index=True)

with tab_labs:
    st.subheader("Strategy lab explorer")
    report_options = [p.name for p, _ in recent_strategy_reports(limit=20)]
    if not report_options:
        st.info("No strategy lab reports found.")
    else:
        selected = st.selectbox("Choose lab report", options=list(reversed(report_options)))
        ranked_df = load_ranked_variants(selected)
        if ranked_df.empty:
            st.warning("That report has no ranked variants.")
        else:
            top_n = min(20, len(ranked_df))
            show = ranked_df.head(top_n).copy()
            st.dataframe(show[[c for c in ["name", "total_return_pct", "selection_score", "max_drawdown_pct", "trades", "win_rate_pct", "rnn_enabled", "rnn_avg_test_accuracy"] if c in show.columns]], use_container_width=True)
            fig_bar = px.bar(show.head(12), x="name", y="selection_score", color="rnn_enabled", title="Top variants by selection score")
            st.plotly_chart(fig_bar, use_container_width=True)
            fig_scatter = px.scatter(show, x="max_drawdown_pct", y="total_return_pct", color="rnn_enabled", size="trades", hover_name="name", title="Variants, return vs drawdown")
            st.plotly_chart(fig_scatter, use_container_width=True)

with tab_sentiment:
    st.subheader("Twitter sentiment cache")
    sentiment_rows = load_sentiment_rows()
    if not sentiment_rows:
        st.info("No sentiment snapshots found yet.")
    else:
        sdf = pd.DataFrame(sentiment_rows).sort_values(["status", "weighted_sentiment"], ascending=[True, False])
        st.dataframe(sdf[["symbol", "status", "tweet_count", "weighted_sentiment", "dominant_types", "block_buy", "force_sell"]], use_container_width=True)
        fig = px.bar(sdf, x="symbol", y="weighted_sentiment", color="status", title="Cached sentiment by symbol")
        st.plotly_chart(fig, use_container_width=True)
        selected_symbol = st.selectbox("Inspect symbol snapshot", options=sdf["symbol"].tolist())
        chosen = next((r for r in sentiment_rows if r["symbol"] == selected_symbol), None)
        if chosen:
            st.json(load_json(chosen["path"]))

with tab_server:
    st.subheader("Oracle server status")
    st.caption(f"Host: {SERVER_HOST} | Repo: {SERVER_REPO}")
    if st.button("Refresh server state"):
        ssh_fetch.clear()
    server = ssh_fetch()
    if not server.get("ok"):
        st.warning(server.get("error") or server.get("stderr") or "Server fetch failed")
    else:
        text = server.get("stdout", "")
        lines = text.splitlines()
        service_state = "unknown"
        if "[service]" in lines:
            try:
                service_state = lines[lines.index("[service]") + 1].strip()
            except Exception:
                pass
        st.metric("auto_trade.service", service_state)
        st.text_area("Raw server snapshot", value=text, height=400)

st.sidebar.header("Run")
st.sidebar.code("streamlit run dashboard/ops_dashboard.py")
st.sidebar.caption("Tip: refresh the page after new lab or paper runs.")
