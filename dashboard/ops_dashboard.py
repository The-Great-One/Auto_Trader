from __future__ import annotations

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
LAB_STATUS_PATH = INTERMEDIARY_DIR / "lab_status" / "weekly_strategy_lab_status.json"
SERVER_KEY = Path("REDACTED_KEY_PATH")
SERVER_HOST = os.getenv("AT_SERVER_HOST", "REDACTED_SERVER")
SERVER_REPO = os.getenv("AT_SERVER_REPO", "/home/ubuntu/Auto_Trader")

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
def load_ranked_variants(report_name: str) -> pd.DataFrame:
    data = load_json(REPORTS_DIR / report_name)
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data.get("ranked") or [])


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
st.caption("A clearer view of live, paper, labs, RNN sweeps, sentiment, and server state")

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
lab_status = load_lab_status()
sentiment_df = load_sentiment_rows()
latest_lab = lab_df.sort_values("generated_at").iloc[-1].to_dict() if not lab_df.empty else {}

hero1, hero2, hero3, hero4, hero5 = st.columns(5)
hero1.metric("Lab status", status_badge_text(lab_status) if lab_status else "idle")
hero2.metric("Latest best return %", latest_lab.get("best_return_pct", "-"))
hero3.metric("Latest best score", latest_lab.get("best_score", "-"))
hero4.metric("Paper decision", paper.get("decision", live_paper.get("mode", "-")))
hero5.metric("Sentiment symbols", len(sentiment_df))

if lab_status:
    progress = float(lab_status.get("progress_pct", 0.0) or 0.0)
    if lab_status.get("status") == "running":
        st.progress(min(100, int(progress)), text=f"Lab running, {lab_status.get('current_variant') or lab_status.get('current_symbol') or lab_status.get('message', 'working')}")
    elif lab_status.get("status") == "failed":
        st.error(f"Latest lab run failed: {lab_status.get('error', 'unknown error')}")
    elif lab_status.get("status") == "done":
        st.success(f"Latest lab finished: {lab_status.get('best_variant', 'n/a')} | best return {lab_status.get('best_return_pct', '-')}")

quick1, quick2, quick3, quick4 = st.columns(4)
if scorecard:
    quick1.metric("Daily orders", scorecard.get("orders", "-"))
    quick2.metric("Daily trades", scorecard.get("trades", "-"))
    quick3.metric("Realized PnL", scorecard.get("estimated_realized_pnl", "-"))
    quick4.metric("Scorecard verdict", scorecard.get("verdict", "-"))

summary_tab, trader_tab, labs_tab, sentiment_tab, server_tab = st.tabs([
    "Mission Control",
    "Live + Paper",
    "Labs + RNN",
    "Twitter Sentiment",
    "Server",
])

with summary_tab:
    left, right = st.columns([1.15, 0.85])
    with left:
        st.subheader("Lab timeline")
        if lab_df.empty:
            st.info("No lab reports found yet.")
        else:
            ordered = lab_df.sort_values("generated_at")
            fig = px.line(
                ordered,
                x="generated_at",
                y=["best_return_pct", "baseline_return_pct"],
                markers=True,
                title="Best vs baseline return across recent lab runs",
            )
            st.plotly_chart(fig, use_container_width=True)
            fig2 = px.scatter(
                ordered,
                x="best_drawdown_pct",
                y="best_return_pct",
                color="best_rnn_enabled",
                size=ordered["improvement_score"].abs().fillna(0) + 0.1,
                hover_name="report",
                title="Best variant, return vs drawdown",
            )
            st.plotly_chart(fig2, use_container_width=True)
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

with labs_tab:
    st.subheader("Lab reports")
    if lab_df.empty:
        st.info("No strategy lab reports found.")
    else:
        st.dataframe(lab_df.sort_values("generated_at", ascending=False), use_container_width=True)
        report_options = list(reversed([p.name for p, _ in recent_strategy_reports(limit=25)]))
        selected = st.selectbox("Inspect lab report", options=report_options)
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
