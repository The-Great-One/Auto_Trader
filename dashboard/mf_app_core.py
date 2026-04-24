# fire_sip_swp_app.py
# SIP → SWP planner for Indian mutual funds
# - Historical chart + CSV downloads
# - Compare up to 5 funds (persistent selection)
# - 🧮 P&C universe scanner — diversified top 5 with progress bar
#   * Aggressive decorrelation (Pearson + Spearman + residual corr), one-per-style-bucket optional
#   * FIX: use outer-join monthly returns for correlations (prevents collapsing to 1 fund)
# - Derived NAV (weighted mix) + trailing returns matrix (3M/6M/12M/36M/60M)
# - SWP: auto-picked realistic projection CAGR (Blended EWMA+Long); no user method picker
# - Longevity gain if withdrawing fewer percentage points of corpus/month than max

from __future__ import annotations
import datetime as dt
import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Callable

import numpy as np
import pandas as pd
import requests
import streamlit as st

# --------------------------------------------------------------------
# Data fetchers (MFAPI)
# --------------------------------------------------------------------

SCHEME_LIST_URL = "https://api.mfapi.in/mf"
NAV_URL_TEMPLATE = "https://api.mfapi.in/mf/{scheme_code}"
NAV_CACHE_DIR = Path(__file__).resolve().parent / ".nav_cache"
NAV_DISK_CACHE_MAX_AGE_HOURS = 24
PORTFOLIO_TRACKER_PATH = Path(__file__).resolve().parents[1] / "reports" / "portfolio_tracker_latest.json"
MAX_COMPARE_FUNDS = 20
FUND_NAME_STOPWORDS = {
    "fund", "plan", "direct", "regular", "growth", "option", "idcw", "income", "distribution",
    "cum", "capital", "withdrawal", "dividend", "daily", "weekly", "monthly", "quarterly",
    "yearly", "annual", "bonus", "payout", "reinvestment",
}

def _nav_cache_path(scheme_code: int) -> Path:
    return NAV_CACHE_DIR / f"{int(scheme_code)}.csv"

def _load_nav_from_disk_cache(scheme_code: int, max_age_hours: int = NAV_DISK_CACHE_MAX_AGE_HOURS) -> Optional[pd.DataFrame]:
    path = _nav_cache_path(scheme_code)
    if not path.exists():
        return None
    try:
        age_sec = dt.datetime.now().timestamp() - path.stat().st_mtime
        if age_sec > max_age_hours * 3600:
            return None
        df = pd.read_csv(path, parse_dates=["date"])
        if "date" not in df.columns or "nav" not in df.columns:
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["date", "nav"]).sort_values("date").reset_index(drop=True)
        if df.empty:
            return None
        return df[["date", "nav"]]
    except Exception:
        return None

def _save_nav_to_disk_cache(scheme_code: int, nav_df: pd.DataFrame) -> None:
    if nav_df is None or nav_df.empty:
        return
    try:
        NAV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = _nav_cache_path(scheme_code)
        tmp = path.with_suffix(".tmp")
        nav_df[["date", "nav"]].to_csv(tmp, index=False)
        tmp.replace(path)
    except Exception:
        # Disk cache is best-effort; ignore cache write failures.
        pass

@st.cache_data(show_spinner=False, ttl=3 * 3600)
def fetch_scheme_list() -> pd.DataFrame:
    r = requests.get(SCHEME_LIST_URL, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json()).rename(
        columns={"schemeCode": "scheme_code", "schemeName": "scheme_name"}
    )
    df["scheme_code"] = pd.to_numeric(df["scheme_code"], errors="coerce")
    df = df.dropna(subset=["scheme_code", "scheme_name"]).reset_index(drop=True)
    df["scheme_code"] = df["scheme_code"].astype(int)
    df["scheme_name_lc"] = df["scheme_name"].str.lower()
    return df

@st.cache_data(show_spinner=False, ttl=3 * 3600)
def fetch_nav_history(scheme_code: int) -> pd.DataFrame:
    cached = _load_nav_from_disk_cache(scheme_code)
    if cached is not None:
        return cached

    r = requests.get(NAV_URL_TEMPLATE.format(scheme_code=scheme_code), timeout=30)
    r.raise_for_status()
    jd = r.json()
    df = pd.DataFrame(jd["data"])
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna(subset=["nav"]).reset_index(drop=True)
    out = df[["date", "nav"]]
    _save_nav_to_disk_cache(scheme_code, out)
    return out

@st.cache_data(show_spinner=False, ttl=3 * 3600)
def fetch_monthly_returns(scheme_code: int) -> pd.DataFrame:
    h = fetch_nav_history(scheme_code)
    if h.empty:
        return pd.DataFrame(columns=["date", "ret"])
    me = resample_month_end(h)
    me["ret"] = me["nav"].pct_change()
    return me[["date", "ret"]].dropna().reset_index(drop=True)

def _clean_tracker_text(text: str) -> str:
    return str(text or "").replace("u0026", "&").replace("\\u0026", "&")

def _normalize_fund_text(text: str) -> str:
    cleaned = _clean_tracker_text(text).lower().replace("&", " and ")
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()

def _fund_tokens(text: str) -> list[str]:
    return [tok for tok in _normalize_fund_text(text).split() if tok not in FUND_NAME_STOPWORDS and len(tok) >= 3]

@st.cache_data(show_spinner=False, ttl=15 * 60)
def load_current_mf_holdings() -> pd.DataFrame:
    cols = ["fund", "current_value", "weight_pct", "mf_weight_pct", "gain_pct", "recommendation", "category", "risk_level", "tradingsymbol"]
    if not PORTFOLIO_TRACKER_PATH.exists():
        return pd.DataFrame(columns=cols)
    try:
        payload = json.loads(PORTFOLIO_TRACKER_PATH.read_text())
    except Exception:
        return pd.DataFrame(columns=cols)

    total_mf_weight = max(1e-9, float((payload.get("portfolio_summary") or {}).get("mf_weight_pct") or 0.0))
    rows = []
    for holding in payload.get("mf_holdings") or []:
        portfolio_weight = float(holding.get("weight_pct") or 0.0)
        rows.append({
            "fund": _clean_tracker_text(holding.get("fund") or holding.get("tradingsymbol") or ""),
            "current_value": float(holding.get("current_value") or 0.0),
            "weight_pct": portfolio_weight,
            "mf_weight_pct": (portfolio_weight / total_mf_weight) * 100.0,
            "gain_pct": float(holding.get("gain_pct") or 0.0),
            "recommendation": str(holding.get("recommendation") or "hold"),
            "category": str(holding.get("category") or ""),
            "risk_level": str(holding.get("risk_level") or ""),
            "tradingsymbol": str(holding.get("tradingsymbol") or ""),
        })
    return pd.DataFrame(rows)

def match_holding_to_scheme_name(holding_name: str, scheme_df: pd.DataFrame) -> Optional[str]:
    if scheme_df.empty:
        return None
    name_norm = _normalize_fund_text(holding_name)
    tokens = _fund_tokens(holding_name)
    if not tokens:
        return None

    candidates = scheme_df.copy()
    candidates["scheme_name_norm"] = candidates["scheme_name"].map(_normalize_fund_text)

    strong_tokens = [tok for tok in tokens if len(tok) >= 4][:3] or tokens[:2]
    for tok in strong_tokens[:2]:
        mask = candidates["scheme_name_norm"].str.contains(rf"\b{re.escape(tok)}\b", regex=True, na=False)
        if mask.any():
            candidates = candidates[mask].copy()

    if candidates.empty:
        candidates = scheme_df.copy()
        candidates["scheme_name_norm"] = candidates["scheme_name"].map(_normalize_fund_text)

    desired_direct = "direct" in name_norm
    token_set = set(tokens)
    scored: list[tuple[float, str]] = []
    for _, row in candidates.iterrows():
        scheme_name = str(row["scheme_name"])
        scheme_norm = str(row["scheme_name_norm"])
        scheme_tokens = set(_fund_tokens(scheme_norm))
        overlap = len(token_set & scheme_tokens)
        if overlap == 0:
            continue
        score = overlap * 12.0 + SequenceMatcher(None, name_norm, scheme_norm).ratio() * 10.0
        if desired_direct and "direct" in scheme_norm:
            score += 3.0
        if "growth" in scheme_norm:
            score += 2.0
        if "bonus" in scheme_norm:
            score -= 2.0
        if "idcw" in scheme_norm or "dividend" in scheme_norm:
            score -= 3.0
        scored.append((score, scheme_name))

    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    top_score, top_name = scored[0]
    return top_name if top_score >= 12.0 else None

def resolve_current_mf_defaults(scheme_df: pd.DataFrame) -> tuple[list[str], dict[str, float], pd.DataFrame, list[str]]:
    holdings_df = load_current_mf_holdings()
    if holdings_df.empty:
        return [], {}, holdings_df, []

    selected: list[str] = []
    weight_map: dict[str, float] = {}
    unmatched: list[str] = []
    matched_scheme_names: list[str] = []

    for _, row in holdings_df.sort_values("mf_weight_pct", ascending=False).iterrows():
        fund_name = str(row["fund"])
        matched = match_holding_to_scheme_name(fund_name, scheme_df)
        matched_scheme_names.append(matched or "")
        if matched:
            if matched not in selected:
                selected.append(matched)
                weight_map[matched] = float(row["mf_weight_pct"])
        else:
            unmatched.append(fund_name)

    holdings_df = holdings_df.copy()
    holdings_df["matched_scheme_name"] = matched_scheme_names
    return selected[:MAX_COMPARE_FUNDS], weight_map, holdings_df, unmatched

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def month_range(start, end):
    s = pd.Timestamp(start).normalize().replace(day=1)
    e = pd.Timestamp(end).normalize().replace(day=1)
    out = []
    while s <= e:
        out.append(s)
        s = s + pd.offsets.MonthBegin(1)
    return out

def nearest_nav_on_or_after(nav_df: pd.DataFrame, when) -> Optional[pd.Series]:
    d = pd.Timestamp(when)
    idx = nav_df.index[nav_df["date"] >= d]
    if len(idx) == 0:
        return None
    return nav_df.loc[idx[0]]

def normalize_nav(nav_df: pd.DataFrame, base: float = 10.0) -> pd.DataFrame:
    if nav_df.empty:
        return nav_df.copy()
    start = float(nav_df["nav"].iloc[0])
    if start <= 0:
        return nav_df.copy()
    out = nav_df.copy()
    out["nav_norm"] = out["nav"] * (base / start)
    return out

def resample_month_end(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["ME"] = x["date"].dt.to_period("M").dt.to_timestamp("M")
    x = x.groupby("ME", as_index=False)["nav"].last().rename(columns={"ME": "date"})
    return x

def monthly_returns(me_df: pd.DataFrame) -> pd.Series:
    me_df = me_df.sort_values("date")
    return me_df["nav"].pct_change()

def compute_cagr_from_history(nav_df: pd.DataFrame, lookback_years: float | None = None) -> float:
    if nav_df.empty or len(nav_df) < 2:
        return 0.0
    end_date = pd.Timestamp(nav_df["date"].iloc[-1])
    end_nav = float(nav_df["nav"].iloc[-1])
    if lookback_years:
        start_target = end_date - pd.DateOffset(years=int(round(lookback_years)))
        window = nav_df[nav_df["date"] >= start_target]
        if window.empty:
            start_nav, start_date = nav_df["nav"].iloc[0], nav_df["date"].iloc[0]
        else:
            start_nav, start_date = window["nav"].iloc[0], window["date"].iloc[0]
    else:
        start_nav, start_date = nav_df["nav"].iloc[0], nav_df["date"].iloc[0]
    if start_nav <= 0 or end_nav <= 0:
        return 0.0
    years = max(1e-9, (end_date - start_date).days / 365.25)
    return (end_nav / start_nav) ** (1 / years) - 1

def extend_nav_with_projection(nav_df: pd.DataFrame, years_forward: float, assumed_annual_return: float):
    if nav_df.empty:
        return nav_df.copy()
    df = nav_df.copy()
    if years_forward <= 0:
        return df
    last_date = df["date"].iloc[-1]
    months = int(round(years_forward * 12))
    monthly_rate = (1 + assumed_annual_return) ** (1 / 12) - 1
    last_nav = float(df["nav"].iloc[-1])
    future_dates = [last_date + pd.offsets.MonthBegin(i + 1) for i in range(months)]
    projected_navs = [last_nav * ((1 + monthly_rate) ** (i + 1)) for i in range(months)]
    return pd.concat([df, pd.DataFrame({"date": future_dates, "nav": projected_navs})], ignore_index=True)

# --------------------------------------------------------------------
# Risk profile → weights from vol buckets
# --------------------------------------------------------------------

def compute_vol_buckets(histories: Dict[str, pd.DataFrame], lookback: str = "3Y") -> Tuple[pd.DataFrame, Dict[str, str]]:
    vols = []
    for nm, df in histories.items():
        me = resample_month_end(df)
        if me.empty:
            continue
        if lookback != "Full":
            last = me["date"].iloc[-1]
            cutoff = {
                "1Y": last - pd.DateOffset(years=1),
                "3Y": last - pd.DateOffset(years=3),
                "5Y": last - pd.DateOffset(years=5),
            }[lookback]
            me = me[me["date"] >= cutoff].reset_index(drop=True)
        r = monthly_returns(me).dropna()
        if len(r) < 6:
            continue
        vols.append((nm, float(r.std())))
    if not vols:
        return pd.DataFrame(columns=["fund", "vol"]), {}
    vol_df = pd.DataFrame(vols, columns=["fund", "vol"]).sort_values("vol").reset_index(drop=True)
    n = len(vol_df)
    if n == 1:
        bucket = ["MidVol"]
    elif n == 2:
        bucket = ["LowVol", "HighVol"]
    else:
        t1 = n // 3
        t2 = 2 * n // 3
        bucket = (["LowVol"] * t1) + (["MidVol"] * (t2 - t1)) + (["HighVol"] * (n - t2))
    vol_df["bucket"] = bucket
    bucket_map = dict(zip(vol_df["fund"], vol_df["bucket"]))
    return vol_df, bucket_map

def profile_bucket_weights(profile: str) -> Dict[str, float]:
    if profile == "Low":
        return {"LowVol": 0.60, "MidVol": 0.30, "HighVol": 0.10}
    if profile == "High":
        return {"LowVol": 0.10, "MidVol": 0.30, "HighVol": 0.60}
    return {"LowVol": 0.33, "MidVol": 0.34, "HighVol": 0.33}

def derive_fund_weights_from_buckets(funds: List[str], bucket_map: Dict[str, str], target_bucket_wts: Dict[str, float]) -> Dict[str, float]:
    bucket_members: Dict[str, List[str]] = {"LowVol": [], "MidVol": [], "HighVol": []}
    for f in funds:
        b = bucket_map.get(f, "MidVol")
        bucket_members[b].append(f)
    non_empty = {b: bucket_members[b] for b in bucket_members if bucket_members[b]}
    total_target = sum(target_bucket_wts[b] for b in non_empty.keys())
    if total_target == 0:
        return {f: 1.0 / max(1, len(funds)) for f in funds}
    eff_bucket_wt = {b: (target_bucket_wts[b] / total_target) if b in non_empty else 0.0 for b in target_bucket_wts}
    out: Dict[str, float] = {}
    for b, members in non_empty.items():
        share = eff_bucket_wt[b]
        k = len(members)
        for f in members:
            out[f] = share / k if k > 0 else 0.0
    s = sum(out.values())
    if s > 0:
        out = {k: v / s for k, v in out.items()}
    return out

def apply_min_weight_floor(weights: Dict[str, float], min_weight: float = 0.20) -> Tuple[Dict[str, float], bool]:
    if not weights:
        return {}, False
    keys = list(weights.keys())
    w = np.array([max(0.0, float(weights.get(k, 0.0))) for k in keys], dtype=float)
    if float(w.sum()) <= 0:
        w = np.ones(len(keys), dtype=float) / len(keys)
    else:
        w = w / float(w.sum())

    if len(keys) * float(min_weight) > 1.0 + 1e-12:
        # Infeasible floor; fallback to equal weights.
        eq = np.ones(len(keys), dtype=float) / len(keys)
        return {k: float(v) for k, v in zip(keys, eq)}, False

    base = np.full(len(keys), float(min_weight), dtype=float)
    rem = 1.0 - float(base.sum())
    extra = np.maximum(w - float(min_weight), 0.0)
    if float(extra.sum()) > 0:
        alloc = rem * (extra / float(extra.sum()))
    else:
        alloc = np.full(len(keys), rem / len(keys), dtype=float)
    out = base + alloc
    out = out / float(out.sum())
    return {k: float(v) for k, v in zip(keys, out)}, True

# --------------------------------------------------------------------
# Build derived portfolio series
# --------------------------------------------------------------------

def build_portfolio_history(histories: Dict[str, pd.DataFrame], weights: Dict[str, float], base: float = 10.0) -> pd.DataFrame:
    if not histories or not weights:
        return pd.DataFrame(columns=["date", "nav"])
    starts = [df["date"].iloc[0] for df in histories.values() if not df.empty]
    common_start = max(starts) if starts else None
    if common_start is None:
        return pd.DataFrame(columns=["date", "nav"])
    frames = []
    for nm, df in histories.items():
        d = df[df["date"] >= common_start].reset_index(drop=True).copy()
        if d.empty:
            continue
        base_row = nearest_nav_on_or_after(d, common_start)
        if base_row is None or float(base_row["nav"]) <= 0:
            continue
        factor = base / float(base_row["nav"])
        col = nm
        d[col] = d["nav"] * factor
        frames.append(d[["date", col]])
    if len(frames) == 0:
        return pd.DataFrame(columns=["date", "nav"])
    all_df = frames[0]
    for fr in frames[1:]:
        all_df = all_df.merge(fr, on="date", how="inner")
    if all_df.empty:
        return pd.DataFrame(columns=["date", "nav"])
    fund_cols = [c for c in all_df.columns if c != "date"]
    w_vec = np.array([weights.get(c, 0.0) for c in fund_cols], dtype=float)
    w_sum = float(w_vec.sum())
    if w_sum <= 0:
        w_vec = np.ones(len(fund_cols)) / len(fund_cols)
    else:
        w_vec = w_vec / w_sum
    values = all_df[fund_cols].to_numpy()
    derived = values.dot(w_vec)
    return pd.DataFrame({"date": all_df["date"], "nav": derived})

def cagr_of_series(df: pd.DataFrame, years: Optional[int]) -> float:
    if df.empty or len(df) < 2:
        return 0.0
    if years is None:
        return compute_cagr_from_history(df, None)
    return compute_cagr_from_history(df, years)

# --------------------------------------------------------------------
# Robust correlations (warning-free)
# --------------------------------------------------------------------

def _build_corr_views_from_returns(rets: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # sanitize
    if rets is None:
        rets = pd.DataFrame()
    rets = rets.replace([np.inf, -np.inf], np.nan)
    rets = rets.dropna(how="all", axis=1).dropna(how="all", axis=0)

    cols = list(rets.columns)
    if len(cols) < 2 or len(rets) < 3:
        z = pd.DataFrame(0.0, index=cols, columns=cols, dtype=float)
        return {"pearson": z, "spearman": z, "residual": z, "composite": z}

    pear = rets.corr().astype(float)
    spear = rets.corr(method="spearman").astype(float)

    X = rets.to_numpy(dtype=float)
    with np.errstate(invalid="ignore"):
        mu = np.nanmean(X, axis=0, keepdims=True)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    X = np.nan_to_num(X - mu, nan=0.0, posinf=0.0, neginf=0.0)

    try:
        if min(X.shape) < 2 or not np.isfinite(X).any():
            raise ValueError("insufficient")
        U, S, Vt = np.linalg.svd(X, full_matrices=False)
        pc1 = U[:, 0] * S[0]
        denom = float(pc1 @ pc1) or 1e-12
        beta = (X.T @ pc1) / denom
        R = X - np.outer(pc1, beta)
        resid = pd.DataFrame(np.corrcoef(R, rowvar=False), index=cols, columns=cols).astype(float)
    except Exception:
        resid = pd.DataFrame(0.0, index=cols, columns=cols, dtype=float)

    comp = (0.4 * pear.abs() + 0.3 * spear.abs() + 0.3 * resid.abs()).astype(float)
    return {"pearson": pear, "spearman": spear, "residual": resid, "composite": comp}

def correlation_views_from_chart_df(chart_df: pd.DataFrame, cols: List[str]) -> Dict[str, pd.DataFrame]:
    if chart_df.empty or len(cols) < 2:
        z = pd.DataFrame(0.0, index=cols, columns=cols, dtype=float)
        return {"Pearson |r|": z, "Spearman |ρ|": z, "Residual |r| (after PC1)": z, "Composite |C|": z}
    df = chart_df[["date"] + cols].dropna().copy()
    df["ME"] = pd.to_datetime(df["date"]).dt.to_period("M").dt.to_timestamp("M")
    me = df.groupby("ME", as_index=False)[cols].last().rename(columns={"ME": "date"})
    rets = me.set_index("date")[cols].pct_change()
    views = _build_corr_views_from_returns(rets)
    return {
        "Pearson |r|":  views["pearson"].abs(),
        "Spearman |ρ|": views["spearman"].abs(),
        "Residual |r| (after PC1)": views["residual"].abs(),
        "Composite |C|": views["composite"],
    }

# --------------------------------------------------------------------
# Robust CAGR estimators for projection (used by SWP)
# --------------------------------------------------------------------

def _winsorize(x: np.ndarray, p: float) -> np.ndarray:
    if len(x) == 0 or p <= 0:
        return x
    lo = np.nanpercentile(x, p * 100.0)
    hi = np.nanpercentile(x, 100.0 - p * 100.0)
    return np.clip(x, lo, hi)

def estimate_proj_cagr(
    nav_df: pd.DataFrame,
    end_date,
    method: str = "Blended (EWMA + Long)",
    lookback_years: Optional[int] = 10,
    winsorize_pct: float = 0.015,
    ewma_half_life_months: int = 12,
    blend_alpha: float = 0.65,
) -> Tuple[float, Dict[str, float]]:
    if nav_df.empty:
        return 0.0, {"months": 0, "ann_vol": 0.0, "geo": 0.0, "ewma": 0.0, "simple": 0.0, "blended": 0.0}
    me = resample_month_end(nav_df[nav_df["date"] <= pd.Timestamp(end_date)]).dropna()
    if lookback_years is not None:
        cutoff = pd.Timestamp(end_date) - pd.DateOffset(years=int(round(lookback_years)))
        me = me[me["date"] >= cutoff].reset_index(drop=True)
    if len(me) < 3:
        return 0.0, {"months": len(me), "ann_vol": 0.0, "geo": 0.0, "ewma": 0.0, "simple": 0.0, "blended": 0.0}

    log_nav = np.log(me["nav"].to_numpy(dtype=float))
    lr = np.diff(log_nav)  # monthly log-returns
    months = len(lr)
    lr_w = _winsorize(lr, winsorize_pct) if winsorize_pct and winsorize_pct > 0 else lr

    lr_mean = float(np.nanmean(lr_w))
    geo_annual = np.exp(lr_mean * 12.0) - 1.0

    start_nav = float(me["nav"].iloc[0]); end_nav = float(me["nav"].iloc[-1])
    years = max(1e-9, (me["date"].iloc[-1] - me["date"].iloc[0]).days / 365.25)
    simple = (end_nav / max(start_nav, 1e-9)) ** (1.0 / years) - 1.0 if start_nav > 0 and end_nav > 0 else 0.0

    lam = 0.5 ** (1.0 / max(1, int(ewma_half_life_months)))
    w = np.array([lam ** (months - 1 - i) for i in range(months)], dtype=float); w = w / w.sum()
    lr_ewma = float(np.nansum(w * lr_w))
    ewma_annual = np.exp(lr_ewma * 12.0) - 1.0

    blended = blend_alpha * ewma_annual + (1.0 - blend_alpha) * geo_annual
    ann_vol = float(np.nanstd(lr_w, ddof=1)) * np.sqrt(12.0)

    diag = {"months": months, "ann_vol": ann_vol, "geo": geo_annual, "ewma": ewma_annual, "simple": simple, "blended": blended}
    return float(blended), diag

# --------------------------------------------------------------------
# SWP simulation (inflation step-ups)
# --------------------------------------------------------------------

@dataclass
class ExitLoadRule:
    days: int
    pct: float

def infer_tax_profile_from_name(name: str) -> str:
    n = str(name).lower()
    debt_rx = r"(liquid|overnight|ultra\s*short|low\s*duration|short\s*duration|corporate\s*bond|gilt|money market|debt)"
    if re.search(debt_rx, n):
        return "Debt"
    return "Equity"

def _tax_rate_for_holding_days(profile: str, holding_days: int, cfg: Dict[str, float]) -> float:
    if profile == "Debt":
        th = int(cfg.get("debt_ltcg_days", 1095))
        st = float(cfg.get("debt_stcg_rate", 0.30))
        lt = float(cfg.get("debt_ltcg_rate", 0.20))
        return lt if holding_days >= th else st
    th = int(cfg.get("equity_ltcg_days", 365))
    st = float(cfg.get("equity_stcg_rate", 0.15))
    lt = float(cfg.get("equity_ltcg_rate", 0.10))
    return lt if holding_days >= th else st

def build_joint_monthly_return_panel(histories: Dict[str, pd.DataFrame], end_date) -> pd.DataFrame:
    series = {}
    end_ts = pd.Timestamp(end_date)
    for nm, h in histories.items():
        if h is None or h.empty:
            continue
        me = resample_month_end(h[h["date"] <= end_ts])
        if len(me) < 3:
            continue
        r = me.set_index("date")["nav"].pct_change().dropna()
        if not r.empty:
            series[nm] = r
    if not series:
        return pd.DataFrame()
    panel = pd.concat(series, axis=1, join="inner").dropna()
    return panel

def simulate_swp_inflation(
    nav_series: pd.DataFrame,
    start_date,
    corpus_value: float,
    start_withdrawal_monthly: float,
    annual_inflation: float,
    max_years: Optional[int] = None,
    max_extra_years: int = 80,
) -> pd.DataFrame:
    if nav_series.empty or corpus_value <= 0 or start_withdrawal_monthly < 0:
        return pd.DataFrame(columns=["date","nav","withdrawal","units_left","portfolio_value"])
    start_month = pd.Timestamp(start_date).normalize().replace(day=1)
    df = nav_series.copy()
    df = df[df["date"] >= start_month].reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(columns=["date","nav","withdrawal","units_left","portfolio_value"])
    if max_years is None:
        horizon_months = 12 * max_extra_years
    else:
        horizon_months = 12 * (max_years + max_extra_years)
    need_until = start_month + pd.offsets.MonthBegin(max(0, horizon_months - 1))
    while df["date"].iloc[-1] < need_until:
        growth = (float(df["nav"].iloc[-1]) / max(float(df["nav"].iloc[-2]) if len(df) > 1 else float(df["nav"].iloc[-1]), 1e-9)) - 1.0
        next_date = df["date"].iloc[-1] + pd.offsets.MonthBegin(1)
        next_nav = float(df["nav"].iloc[-1]) * (1.0 + growth)
        df = pd.concat([df, pd.DataFrame({"date":[next_date], "nav":[next_nav]})], ignore_index=True)
    first_row = nearest_nav_on_or_after(df, start_month)
    if first_row is None or float(first_row["nav"]) <= 0:
        return pd.DataFrame(columns=["date","nav","withdrawal","units_left","portfolio_value"])
    units_left = corpus_value / float(first_row["nav"])
    rows = []
    start_w = float(start_withdrawal_monthly)
    infl = float(annual_inflation)
    for i, m in enumerate(month_range(start_month, need_until)):
        row = nearest_nav_on_or_after(df, m)
        if row is None:
            break
        nav_val = float(row["nav"])
        year_no = i // 12
        w_this = start_w * ((1.0 + infl) ** year_no)
        units_to_sell = w_this / nav_val if nav_val > 0 else np.inf
        if units_to_sell > units_left + 1e-12:
            rows.append({"date": pd.Timestamp(m), "nav": nav_val, "withdrawal": w_this, "units_left": 0.0, "portfolio_value": 0.0})
            break
        units_left -= units_to_sell
        portfolio_value = units_left * nav_val
        rows.append({"date": pd.Timestamp(m), "nav": nav_val, "withdrawal": w_this, "units_left": units_left, "portfolio_value": portfolio_value})
    return pd.DataFrame(rows)

def simulate_swp_multifund_inflation(
    nav_map: Dict[str, pd.DataFrame],
    start_date,
    corpus_value: float,
    start_withdrawal_monthly: float,
    annual_inflation: float,
    target_weights: Dict[str, float],
    rebalance_every_months: int = 24,
    max_years: Optional[int] = None,
    max_extra_years: int = 80,
    withdrawal_mode: str = "lowest_return",
    fund_tax_profiles: Optional[Dict[str, str]] = None,
    tax_cfg: Optional[Dict[str, float]] = None,
    exit_load_days: int = 365,
    exit_load_pct: float = 0.01,
    use_guardrails: bool = False,
    guardrail_upper_mult: float = 1.20,
    guardrail_lower_mult: float = 0.80,
    guardrail_cut_pct: float = 0.10,
    guardrail_raise_pct: float = 0.05,
) -> pd.DataFrame:
    out_cols = [
        "date",
        "nav",
        "withdrawal",
        "units_left",
        "portfolio_value",
        "draw_from",
        "rebalanced",
        "tax_paid",
        "exit_load_paid",
        "spending_mult",
        "stop_reason",
    ]
    if (not nav_map) or corpus_value <= 0 or start_withdrawal_monthly < 0:
        return pd.DataFrame(columns=out_cols)

    start_month = pd.Timestamp(start_date).normalize().replace(day=1)
    if max_years is None:
        horizon_months = 12 * max_extra_years
    else:
        horizon_months = 12 * (max_years + max_extra_years)
    need_until = start_month + pd.offsets.MonthBegin(max(0, horizon_months - 1))

    start_navs: Dict[str, float] = {}
    for nm, df in nav_map.items():
        if df is None or df.empty:
            continue
        row = nearest_nav_on_or_after(df, start_month)
        if row is None:
            continue
        nv = float(row["nav"])
        if nv > 0:
            start_navs[nm] = nv
    if not start_navs:
        return pd.DataFrame(columns=out_cols)

    active = list(start_navs.keys())
    w = np.array([max(0.0, float(target_weights.get(nm, 0.0))) for nm in active], dtype=float)
    if float(w.sum()) <= 0:
        w = np.ones(len(active), dtype=float) / len(active)
    else:
        w = w / float(w.sum())
    w_map = {nm: float(wi) for nm, wi in zip(active, w)}
    if tax_cfg is None:
        tax_cfg = {
            "equity_stcg_rate": 0.15,
            "equity_ltcg_rate": 0.10,
            "equity_ltcg_days": 365,
            "debt_stcg_rate": 0.30,
            "debt_ltcg_rate": 0.20,
            "debt_ltcg_days": 1095,
        }
    if fund_tax_profiles is None:
        fund_tax_profiles = {nm: infer_tax_profile_from_name(nm) for nm in active}

    lots: Dict[str, List[Dict[str, object]]] = {
        nm: [
            {
                "units": (float(corpus_value) * w_map[nm] / start_navs[nm]),
                "buy_date": pd.Timestamp(start_month),
                "buy_nav": float(start_navs[nm]),
            }
        ]
        for nm in active
    }

    def _fund_units(nm: str) -> float:
        return float(sum(float(l["units"]) for l in lots.get(nm, [])))

    def _fund_value(nm: str, navs_now: Dict[str, float]) -> float:
        return _fund_units(nm) * float(navs_now.get(nm, 0.0))

    def _cleanup_lots(nm: str):
        lots[nm] = [l for l in lots.get(nm, []) if float(l["units"]) > 1e-12]

    def _marginal_cost_score(nm: str, nav_now: float, ts) -> float:
        ll = lots.get(nm, [])
        if not ll:
            return 1e9
        lot0 = ll[0]
        hold_days = max(0, int((pd.Timestamp(ts) - pd.Timestamp(lot0["buy_date"])).days))
        profile = fund_tax_profiles.get(nm, "Equity")
        tax_rate = _tax_rate_for_holding_days(profile, hold_days, tax_cfg)
        gain_ratio = max(0.0, (float(nav_now) - float(lot0["buy_nav"])) / max(float(nav_now), 1e-9))
        load_rate = float(exit_load_pct) if hold_days < int(exit_load_days) else 0.0
        return float(tax_rate * gain_ratio + load_rate)

    def _sell_gross_fifo(nm: str, gross_target: float, nav_now: float, ts) -> Tuple[float, float, float]:
        rem = float(max(0.0, gross_target))
        sold = 0.0
        tax_paid = 0.0
        exit_paid = 0.0
        profile = fund_tax_profiles.get(nm, "Equity")
        ll = lots.get(nm, [])
        for lot in ll:
            if rem <= 1e-9:
                break
            u = float(lot["units"])
            if u <= 1e-12:
                continue
            gross_avail = u * float(nav_now)
            gross_part = min(gross_avail, rem)
            if gross_part <= 0:
                continue
            units_sell = gross_part / max(float(nav_now), 1e-9)
            buy_nav = float(lot["buy_nav"])
            hold_days = max(0, int((pd.Timestamp(ts) - pd.Timestamp(lot["buy_date"])).days))
            gain = max(0.0, (float(nav_now) - buy_nav) * units_sell)
            tax_rate = _tax_rate_for_holding_days(profile, hold_days, tax_cfg)
            tax_paid += gain * tax_rate
            load_rate = float(exit_load_pct) if hold_days < int(exit_load_days) else 0.0
            exit_paid += gross_part * load_rate
            lot["units"] = u - units_sell
            sold += gross_part
            rem -= gross_part
        _cleanup_lots(nm)
        return float(sold), float(tax_paid), float(exit_paid)

    spending_mult = 1.0
    rows = []
    start_w = float(start_withdrawal_monthly)
    infl = float(annual_inflation)
    initial_wr = (12.0 * start_w) / max(float(corpus_value), 1e-9)

    for i, m in enumerate(month_range(start_month, need_until)):
        navs: Dict[str, float] = {}
        for nm in active:
            row = nearest_nav_on_or_after(nav_map[nm], m)
            if row is None:
                continue
            nv = float(row["nav"])
            if nv > 0:
                navs[nm] = nv
        if not navs:
            break

        did_rebalance = bool(rebalance_every_months > 0 and i > 0 and (i % rebalance_every_months == 0))
        if did_rebalance:
            total_pre = sum(_fund_value(nm, navs) for nm in active)
            if total_pre <= 0:
                break
            for nm in active:
                nv = float(navs.get(nm, 0.0))
                if nv > 0:
                    target_units = (total_pre * w_map[nm]) / nv
                    lots[nm] = [{"units": target_units, "buy_date": pd.Timestamp(m), "buy_nav": nv}]

        year_no = i // 12
        base_w = start_w * ((1.0 + infl) ** year_no)
        if use_guardrails and i > 0 and (i % 12 == 0):
            total_now_raw = sum(_fund_value(nm, navs) for nm in active)
            total_now = max(1e-9, total_now_raw)
            wr_now = (12.0 * base_w * spending_mult) / total_now
            if wr_now > initial_wr * float(guardrail_upper_mult):
                spending_mult = max(0.0, spending_mult * (1.0 - float(guardrail_cut_pct)))
            elif wr_now < initial_wr * float(guardrail_lower_mult):
                spending_mult = spending_mult * (1.0 + float(guardrail_raise_pct))

        w_this = base_w * spending_mult
        sold_amt = {nm: 0.0 for nm in active}
        tax_paid_m = 0.0
        exit_paid_m = 0.0
        amt_left = float(w_this)

        while amt_left > 1e-9:
            candidates = [nm for nm in active if _fund_units(nm) > 1e-12 and float(navs.get(nm, 0.0)) > 0.0]
            if not candidates:
                break
            if withdrawal_mode == "tax_aware":
                candidates.sort(
                    key=lambda nm: (
                        _marginal_cost_score(nm, float(navs[nm]), m),
                        (float(navs[nm]) / max(float(start_navs[nm]), 1e-9)) - 1.0,
                    )
                )
            else:
                candidates.sort(key=lambda nm: ((float(navs[nm]) / max(float(start_navs[nm]), 1e-9)) - 1.0))
            draw_nm = candidates[0]
            max_amt = _fund_value(draw_nm, navs)
            sell_amt = min(max_amt, amt_left)
            if sell_amt <= 0:
                break
            sold, tx, ld = _sell_gross_fifo(draw_nm, sell_amt, float(navs[draw_nm]), m)
            sold_amt[draw_nm] += sold
            tax_paid_m += tx
            exit_paid_m += ld
            amt_left -= sold

        total_value_raw = sum(_fund_value(nm, navs) for nm in active)
        total_value = max(0.0, total_value_raw)
        denom = sum(max(0.0, _fund_units(nm)) for nm in active)
        nav_proxy = (total_value_raw / denom) if denom > 1e-12 else 0.0
        draw_from = max(sold_amt, key=sold_amt.get) if float(max(sold_amt.values())) > 0 else ""
        rows.append(
            {
                "date": pd.Timestamp(m),
                "nav": nav_proxy,
                "withdrawal": w_this,
                "units_left": denom,
                "portfolio_value": total_value,
                "draw_from": draw_from,
                "rebalanced": did_rebalance,
                "tax_paid": tax_paid_m,
                "exit_load_paid": exit_paid_m,
                "spending_mult": spending_mult,
                "stop_reason": "",
            }
        )
        if amt_left > 1e-9:
            rows[-1]["stop_reason"] = "insufficient_liquidity_for_withdrawal"
            rows[-1]["portfolio_value"] = 0.0
            break
        if total_value <= 0:
            rows[-1]["stop_reason"] = "corpus_depleted"
            rows[-1]["portfolio_value"] = 0.0
            break

    return pd.DataFrame(rows)

def find_max_starting_withdrawal_percent(
    nav_series: pd.DataFrame,
    start_date,
    corpus_value: float,
    annual_inflation: float,
    years_needed: int,
    sim_fn: Optional[Callable[[float], pd.DataFrame]] = None,
) -> Tuple[float, pd.DataFrame]:
    if corpus_value <= 0:
        return 0.0, pd.DataFrame()
    horizon_end = pd.Timestamp(start_date).normalize().replace(day=1) + pd.offsets.MonthBegin(max(0, years_needed * 12 - 1))
    lo, hi = 0.0, 0.10
    best_pct = 0.0
    best_df = pd.DataFrame()
    for _ in range(12):
        start_w = hi * corpus_value
        sim = sim_fn(start_w) if sim_fn is not None else simulate_swp_inflation(nav_series, start_date, corpus_value, start_w, annual_inflation, max_years=years_needed)
        lasted_to = sim["date"].iloc[-1] if not sim.empty else pd.Timestamp(start_date)
        if not sim.empty and lasted_to >= horizon_end:
            best_pct = hi; best_df = sim; hi *= 2.0
            if hi > 0.50: break
        else:
            break
    for _ in range(36):
        mid = 0.5 * (lo + hi)
        start_w = mid * corpus_value
        sim = sim_fn(start_w) if sim_fn is not None else simulate_swp_inflation(nav_series, start_date, corpus_value, start_w, annual_inflation, max_years=years_needed)
        lasted_to = sim["date"].iloc[-1] if not sim.empty else pd.Timestamp(start_date)
        if not sim.empty and lasted_to >= horizon_end:
            best_pct = mid; best_df = sim; lo = mid
        else:
            hi = mid
    return best_pct, best_df

def longevity_for_withdrawal(
    nav_series: pd.DataFrame,
    start_date,
    corpus_value: float,
    start_withdrawal_monthly: float,
    annual_inflation: float,
    sim_fn: Optional[Callable[[float], pd.DataFrame]] = None,
) -> Tuple[int, pd.DataFrame]:
    sim = sim_fn(start_withdrawal_monthly) if sim_fn is not None else simulate_swp_inflation(nav_series, start_date, corpus_value, start_withdrawal_monthly, annual_inflation, max_years=None, max_extra_years=100)
    return len(sim), sim

def monte_carlo_swp_survival(
    start_date,
    years_needed: int,
    paths: int,
    random_seed: int,
    start_nav_map: Dict[str, float],
    return_panel: pd.DataFrame,
    corpus_value: float,
    start_withdrawal_monthly: float,
    annual_inflation: float,
    target_weights: Dict[str, float],
    withdrawal_mode: str,
    fund_tax_profiles: Dict[str, str],
    tax_cfg: Dict[str, float],
    exit_load_days: int,
    exit_load_pct: float,
    use_guardrails: bool,
    guardrail_upper_mult: float,
    guardrail_lower_mult: float,
    guardrail_cut_pct: float,
    guardrail_raise_pct: float,
) -> Dict[str, float]:
    months = int(max(1, years_needed * 12))
    if return_panel is None or return_panel.empty or len(return_panel) < 12 or not start_nav_map:
        return {"survival_prob": np.nan, "p10_end": np.nan, "p50_end": np.nan, "p90_end": np.nan, "paths": 0}

    funds = [f for f in start_nav_map.keys() if f in return_panel.columns]
    if not funds:
        return {"survival_prob": np.nan, "p10_end": np.nan, "p50_end": np.nan, "p90_end": np.nan, "paths": 0}

    rp = return_panel[funds].dropna()
    if rp.empty:
        return {"survival_prob": np.nan, "p10_end": np.nan, "p50_end": np.nan, "p90_end": np.nan, "paths": 0}

    rng = np.random.default_rng(int(random_seed))
    start_month = pd.Timestamp(start_date).normalize().replace(day=1)
    horizon_end = start_month + pd.offsets.MonthBegin(max(0, months - 1))
    successes = 0
    endings = []

    for _ in range(int(max(1, paths))):
        idx = rng.integers(0, len(rp), size=months)
        sampled = rp.iloc[idx].reset_index(drop=True)
        nav_map = {}
        dates = [start_month + pd.offsets.MonthBegin(i) for i in range(months)]
        for nm in funds:
            navs = [float(start_nav_map[nm])]
            for t in range(1, months):
                r = float(sampled.loc[t, nm])
                navs.append(max(1e-9, navs[-1] * (1.0 + r)))
            nav_map[nm] = pd.DataFrame({"date": dates, "nav": navs})

        sim = simulate_swp_multifund_inflation(
            nav_map=nav_map,
            start_date=start_date,
            corpus_value=corpus_value,
            start_withdrawal_monthly=start_withdrawal_monthly,
            annual_inflation=annual_inflation,
            target_weights=target_weights,
            rebalance_every_months=24,
            max_years=years_needed,
            max_extra_years=0,
            withdrawal_mode=withdrawal_mode,
            fund_tax_profiles=fund_tax_profiles,
            tax_cfg=tax_cfg,
            exit_load_days=exit_load_days,
            exit_load_pct=exit_load_pct,
            use_guardrails=use_guardrails,
            guardrail_upper_mult=guardrail_upper_mult,
            guardrail_lower_mult=guardrail_lower_mult,
            guardrail_cut_pct=guardrail_cut_pct,
            guardrail_raise_pct=guardrail_raise_pct,
        )
        lasted_to = sim["date"].iloc[-1] if not sim.empty else pd.Timestamp(start_date)
        survived = (not sim.empty) and (lasted_to >= horizon_end)
        if survived:
            successes += 1
        endings.append(float(sim["portfolio_value"].iloc[-1]) if not sim.empty else 0.0)

    endings_arr = np.array(endings, dtype=float)
    return {
        "survival_prob": float(successes / max(1, len(endings))),
        "p10_end": float(np.nanpercentile(endings_arr, 10)),
        "p50_end": float(np.nanpercentile(endings_arr, 50)),
        "p90_end": float(np.nanpercentile(endings_arr, 90)),
        "paths": int(len(endings)),
    }

# --------------------------------------------------------------------
# NEW: P&C universe scan — diversified top 5 with PROGRESS + decorrelation modes
# --------------------------------------------------------------------

def risk_profile_params(profile: str) -> Tuple[float, float]:
    if profile == "Low":
        return 1.0, 0.80
    if profile == "High":
        return 1.0, 0.20
    return 1.0, 0.50  # Medium

def _clean_monthly_returns(ret: np.ndarray) -> np.ndarray:
    x = np.asarray(ret, dtype=float).reshape(-1)
    x = x[np.isfinite(x)]
    # Monthly MF returns <= -100% are invalid for NAV-based series.
    x = x[x > -0.999999]
    return x

def annualize_from_monthlies(ret: np.ndarray) -> Tuple[float, float]:
    x = _clean_monthly_returns(ret)
    if len(x) == 0:
        return np.nan, np.nan
    years = len(x) / 12.0
    with np.errstate(invalid="ignore", over="ignore", divide="ignore"):
        log_growth = float(np.sum(np.log1p(x)))
        ann_ret = np.exp(log_growth / max(years, 1e-9)) - 1.0
    ann_vol = float(np.std(x, ddof=1)) * np.sqrt(12.0) if len(x) > 1 else 0.0
    if not np.isfinite(ann_ret) or not np.isfinite(ann_vol):
        return np.nan, np.nan
    return float(ann_ret), float(ann_vol)

def infer_style_bucket(name: str) -> str:
    n = name.lower()
    buckets = [
        ("International", r"(international|global|us|world|emerging|asia|china|japan|europe)"),
        ("Precious/Gold", r"(gold|silver)"),
        ("Sector/Thematic", r"(bank|financial|pharma|health|it|tech|consum|energy|psu|infra|auto|mfg|value|quality|momentum|dividend)"),
        ("Small", r"(small\s*cap)"),
        ("Mid", r"(mid\s*cap)"),
        ("Large/Bluechip", r"(large\s*cap|blue\s*chip|bluechip)"),
        ("Flexi/Multi", r"(flexi|multi\s*cap|multicap)"),
        ("Index", r"(index|nifty|sensex)"),
        ("Hybrid/Multi-asset", r"(multi\s*asset|balanced|aggressive hybrid|conservative hybrid|equity & debt|equity and debt)"),
    ]
    for label, rx in buckets:
        if re.search(rx, n):
            return label
    return "Other"

def universe_scan_top5(
    df_schemes: pd.DataFrame,
    include_regex: str,
    exclude_regex: str,
    lookback_months: int,
    min_months: int,
    universe_cap: Optional[int],
    profile: str,
    corr_cap: float = 0.88,
    one_per_bucket: bool = True,
    progress_cb=None,
) -> Tuple[List[str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def upd(frac: float, msg: str):
        if progress_cb:
            try:
                progress_cb(float(max(0.0, min(1.0, frac))), str(msg))
            except Exception:
                pass

    def profile_penalties(p: str) -> Tuple[float, float]:
        if p == "Low":
            return 1.00, 0.90
        if p == "High":
            return 0.55, 0.35
        return 0.75, 0.55

    upd(0.02, "Preparing universe")
    try:
        inc = re.compile(include_regex, re.IGNORECASE) if include_regex.strip() else None
    except re.error as e:
        raise ValueError(f"Invalid include regex: {e}") from e
    try:
        exc = re.compile(exclude_regex, re.IGNORECASE) if exclude_regex.strip() else None
    except re.error as e:
        raise ValueError(f"Invalid exclude regex: {e}") from e

    universe = df_schemes.copy()
    if inc:
        universe = universe[universe["scheme_name"].str.contains(inc, na=False, regex=True)]
    if exc:
        universe = universe[~universe["scheme_name"].str.contains(exc, na=False, regex=True)]
    universe = universe.sort_values("scheme_code").reset_index(drop=True)
    if universe_cap is not None and int(universe_cap) > 0:
        universe = universe.head(min(int(universe_cap), len(universe))).reset_index(drop=True)

    if universe.empty:
        upd(1.0, "No funds after filters")
        return [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    vol_pen, down_pen = profile_penalties(profile)
    rows = []
    rets_map: Dict[int, pd.Series] = {}
    names_map: Dict[int, str] = {}
    style_map: Dict[int, str] = {}
    total = max(1, len(universe))

    # Stage 1: risk-adjusted ranking
    for i, (_, row) in enumerate(universe.iterrows(), start=1):
        name = str(row["scheme_name"])
        code = int(row["scheme_code"])
        upd(0.05 + 0.55 * (i / total), f"Scanning: {name}")

        mr = fetch_monthly_returns(code)
        if mr.empty:
            continue
        end = mr["date"].iloc[-1]
        start_cut = end - pd.DateOffset(months=lookback_months)
        mrw = mr[mr["date"] > start_cut].copy()
        if len(mrw) < min_months:
            continue

        ret_arr = _clean_monthly_returns(mrw["ret"].to_numpy(dtype=float))
        if len(ret_arr) < min_months:
            continue
        ann_ret, ann_vol = annualize_from_monthlies(ret_arr)
        if not np.isfinite(ann_ret) or not np.isfinite(ann_vol):
            continue
        neg = ret_arr[ret_arr < 0]
        down_vol = float(np.std(neg, ddof=1)) * np.sqrt(12.0) if len(neg) > 1 else 0.0
        if not np.isfinite(down_vol):
            down_vol = 0.0

        score = float(ann_ret - vol_pen * ann_vol - down_pen * down_vol)
        style = infer_style_bucket(name)
        rows.append(
            {
                "Fund": name,
                "scheme_code": code,
                "Style": style,
                "months": len(mrw),
                "AnnRet": ann_ret,
                "AnnVol": ann_vol,
                "DownVol": down_vol,
                "Score": score,
            }
        )
        rets_map[code] = mrw.set_index("date")["ret"]
        names_map[code] = name
        style_map[code] = style

    if not rows:
        upd(1.0, "No viable funds found")
        return [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    cand = pd.DataFrame(rows).sort_values("Score", ascending=False).reset_index(drop=True)
    # Keep a practical top slice for correlation math.
    cand_top = cand.head(min(150, len(cand))).copy()

    # Stage 2: pairwise absolute Pearson correlation on aligned panel
    upd(0.65, "Computing correlations")
    series_list = []
    for code in cand_top["scheme_code"]:
        s = rets_map.get(int(code))
        if s is not None:
            s = s.copy()
            s.name = int(code)
            series_list.append(s)
    if not series_list:
        upd(1.0, "No aligned returns")
        return [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    ret_df = pd.concat(series_list, axis=1, join="outer").sort_index()
    pearson_abs = ret_df.corr().abs().fillna(0.0)

    codes = [int(c) for c in pearson_abs.index]
    score_map = cand_top.drop_duplicates(subset=["scheme_code"]).set_index("scheme_code")["Score"].to_dict()

    # Stage 3: deterministic diversified greedy selection
    upd(0.78, "Selecting diversified top 5")
    selected: List[int] = []
    for code in cand_top["scheme_code"]:
        c = int(code)
        if c in codes:
            selected.append(c)
            break

    while len(selected) < 5:
        best_code = None
        best_eff = -1e9
        for c in codes:
            if c in selected:
                continue
            corr_vals = [float(pearson_abs.loc[c, s]) for s in selected] if selected else [0.0]
            max_corr = max(corr_vals)
            if max_corr > corr_cap:
                continue
            avg_corr = float(np.mean(corr_vals))
            style_pen = 0.12 if (one_per_bucket and any(style_map.get(c) == style_map.get(s) for s in selected)) else 0.0
            eff = float(score_map.get(c, 0.0)) - 0.60 * avg_corr - style_pen
            if eff > best_eff:
                best_eff = eff
                best_code = c

        if best_code is None:
            # Fallback: relax hard cap to avoid empty outcomes.
            candidates = []
            for c in codes:
                if c in selected:
                    continue
                corr_vals = [float(pearson_abs.loc[c, s]) for s in selected] if selected else [0.0]
                max_corr = max(corr_vals)
                style_pen = 0.12 if (one_per_bucket and any(style_map.get(c) == style_map.get(s) for s in selected)) else 0.0
                eff = float(score_map.get(c, 0.0)) - 0.60 * float(np.mean(corr_vals)) - style_pen
                candidates.append((max_corr, -eff, c))
            if not candidates:
                break
            candidates.sort()
            best_code = int(candidates[0][2])

        selected.append(best_code)
        upd(0.78 + 0.04 * len(selected), f"Selecting {len(selected)}/5")

    sel_codes = selected[:5]
    if not sel_codes:
        upd(1.0, "Selection empty")
        return [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    metrics = cand[cand["scheme_code"].isin(sel_codes)].copy().sort_values("Score", ascending=False).reset_index(drop=True)
    metrics["AnnRet%"] = (metrics["AnnRet"] * 100).round(2)
    metrics["AnnVol%"] = (metrics["AnnVol"] * 100).round(2)
    metrics["DownVol%"] = (metrics["DownVol"] * 100).round(2)
    metrics = metrics[["Fund", "scheme_code", "Style", "months", "AnnRet%", "AnnVol%", "DownVol%", "Score"]]

    corr_sel = pearson_abs.loc[sel_codes, sel_codes].astype(float).round(3)
    corr_sel.index = [f"{names_map.get(int(c), str(c))} [{int(c)}]" for c in corr_sel.index]
    corr_sel.columns = [f"{names_map.get(int(c), str(c))} [{int(c)}]" for c in corr_sel.columns]

    # Stage 4: normalized chart
    upd(0.90, "Preparing chart")
    histories = {}
    for k, code in enumerate(sel_codes, start=1):
        h = fetch_nav_history(int(code))
        if not h.empty:
            histories[int(code)] = h
        upd(0.90 + 0.02 * (k / max(1, len(sel_codes))), f"Fetching {k}/{len(sel_codes)} histories")

    starts = [df["date"].iloc[0] for df in histories.values()] if histories else []
    if not starts:
        upd(1.0, "Done")
        picks = [f"{names_map.get(int(code), str(code))} [{int(code)}]" for code in sel_codes]
        return picks, metrics, corr_sel, pd.DataFrame()

    common_start = max(starts)
    frames = []
    for code, h in histories.items():
        dfh = h[h["date"] >= common_start].reset_index(drop=True).copy()
        base_row = nearest_nav_on_or_after(dfh, common_start)
        if base_row is not None and float(base_row["nav"]) > 0:
            label = f"{names_map.get(int(code), str(code))} [{int(code)}]"
            dfh[label] = dfh["nav"] * (10.0 / float(base_row["nav"]))
            frames.append(dfh[["date", label]])

    if not frames:
        upd(1.0, "Done")
        picks = [f"{names_map.get(int(code), str(code))} [{int(code)}]" for code in sel_codes]
        return picks, metrics, corr_sel, pd.DataFrame()

    chart_df = frames[0]
    for fr in frames[1:]:
        chart_df = chart_df.merge(fr, on="date", how="inner")

    upd(1.0, "Done")
    picks = [f"{names_map.get(int(code), str(code))} [{int(code)}]" for code in sel_codes]
    return picks, metrics, corr_sel, chart_df

# --------------------------------------------------------------------
# Streamlit UI
# --------------------------------------------------------------------

st.set_page_config(page_title="MF FIRE Planner", layout="wide")
st.title("🪙 MF FIRE Planner — SIP → SWP")
st.caption("Compare, derive a custom NAV, P&C your best 5 with diversification (progress + robust correlations), then run an inflation-aware SWP.")

col1, col2 = st.columns([2, 1])

with col1:
    df_schemes = fetch_scheme_list()
    q = st.text_input("Search fund (type 3+ chars)")
    if q and len(q) >= 3:
        matches = df_schemes[df_schemes["scheme_name_lc"].str.contains(q.lower())].head(25)
    else:
        matches = df_schemes.head(0)
    chosen = st.selectbox("Choose fund", options=matches["scheme_name"].tolist(), index=None)

# === Historical chart + download for chosen ===
nav_hist = None
scode = None
if chosen:
    try:
        scode = int(df_schemes.loc[df_schemes["scheme_name"] == chosen, "scheme_code"].iloc[0])
        nav_hist = fetch_nav_history(scode)
        with st.expander("📈 Historical NAV & Downloads", expanded=False):
            c1, c2, c3 = st.columns([1.2, 1, 1])
            with c1:
                tf_choice = st.selectbox("Timeframe", ["Full","YTD","1Y","3Y","5Y","10Y"], index=0)
            with c2:
                use_normalized = st.checkbox("Normalize to ₹10", value=True)
            with c3:
                show_points = st.checkbox("Show monthly table", value=False)
            nav_to_show = nav_hist.copy()
            if not nav_to_show.empty:
                last_date = nav_to_show["date"].iloc[-1]
                start_map = {
                    "YTD": pd.Timestamp(year=last_date.year, month=1, day=1),
                    "1Y": last_date - pd.DateOffset(years=1),
                    "3Y": last_date - pd.DateOffset(years=3),
                    "5Y": last_date - pd.DateOffset(years=5),
                    "10Y": last_date - pd.DateOffset(years=10),
                }
                if tf_choice != "Full":
                    nav_to_show = nav_to_show[nav_to_show["date"] >= start_map[tf_choice]].reset_index(drop=True)
            ycol = "nav_norm" if use_normalized else "nav"
            if use_normalized:
                nav_to_show = normalize_nav(nav_to_show, base=10.0)
            if not nav_to_show.empty:
                st.line_chart(nav_to_show.set_index("date")[[ycol]], height=260)
                if show_points:
                    me = nav_to_show.copy()
                    me["ME"] = me["date"].dt.to_period("M").dt.to_timestamp("M")
                    me = me.groupby("ME", as_index=False)[[ycol]].last().rename(columns={"ME":"month"})
                    st.dataframe(me, use_container_width=True, height=220)
                full_csv = nav_hist.to_csv(index=False).encode("utf-8")
                file_slug = f"{scode}_{chosen.replace(' ','_').replace('/','_')}"
                st.download_button("⬇️ Download full history (CSV)", data=full_csv, file_name=f"{file_slug}_history.csv", mime="text/csv", use_container_width=True)
    except Exception as e:
        st.warning(f"Could not load NAV history for the selected fund. {e}")

# === Compare panel (stateful) ===
if "compare_funds" not in st.session_state:
    st.session_state.compare_funds = []
if "weight_map" not in st.session_state:
    st.session_state.weight_map = {}
if "current_mf_defaults_loaded" not in st.session_state:
    st.session_state.current_mf_defaults_loaded = False

current_default_funds, current_default_weights, current_holdings_df, unmatched_current_holdings = resolve_current_mf_defaults(df_schemes)
if not st.session_state.current_mf_defaults_loaded and current_default_funds:
    st.session_state.compare_funds = current_default_funds.copy()
    st.session_state.weight_map = current_default_weights.copy()
    st.session_state.current_mf_defaults_loaded = True

with st.expander("📊 Compare / analyze funds", expanded=True):
    code_map = df_schemes.set_index("scheme_name")["scheme_code"].to_dict()
    c1, c2, c3 = st.columns([1.3,1,1])
    with c1:
        comp_q = st.text_input("Find a fund (type 3+ chars)", key="comp_q")
        if comp_q and len(comp_q) >= 3:
            comp_matches = df_schemes[df_schemes["scheme_name_lc"].str.contains(comp_q.lower())].head(200)
        else:
            comp_matches = df_schemes.head(0)
        comp_pick = st.selectbox("Pick from results", options=comp_matches["scheme_name"].tolist(), index=None, key="comp_pick")
    with c2:
        if st.button("➕ Add picked", use_container_width=True):
            if comp_pick and comp_pick not in st.session_state.compare_funds:
                if len(st.session_state.compare_funds) < MAX_COMPARE_FUNDS:
                    st.session_state.compare_funds.append(comp_pick)
                else:
                    st.warning(f"You can compare at most {MAX_COMPARE_FUNDS} funds.")
    with c3:
        if st.button("↺ Reload current MF holdings", use_container_width=True):
            if current_default_funds:
                st.session_state.compare_funds = current_default_funds.copy()
                st.session_state.weight_map = current_default_weights.copy()
                st.success("Reloaded current MF holdings into MF FIRE.")
            else:
                st.warning("Current MF holdings were not available to load.")

    if not current_holdings_df.empty:
        st.caption(f"Loaded {len(current_default_funds)} current MF holdings by default from your portfolio tracker. Weights below are normalized within the MF sleeve.")
        show_cols = ["fund", "current_value", "weight_pct", "mf_weight_pct", "gain_pct", "recommendation", "category", "risk_level"]
        pretty = current_holdings_df[show_cols].copy()
        pretty = pretty.rename(columns={
            "fund": "Fund",
            "current_value": "Current Value ₹",
            "weight_pct": "Portfolio %",
            "mf_weight_pct": "MF Sleeve %",
            "gain_pct": "Gain %",
            "recommendation": "Reco",
            "category": "Category",
            "risk_level": "Risk",
        })
        st.dataframe(pretty, use_container_width=True, height=260)
    if unmatched_current_holdings:
        st.warning("Could not map these holdings cleanly into MF API scheme names: " + ", ".join(unmatched_current_holdings[:5]))

    if chosen and chosen not in st.session_state.compare_funds:
        if st.button("➕ Add currently selected", use_container_width=True):
            if len(st.session_state.compare_funds) < MAX_COMPARE_FUNDS:
                st.session_state.compare_funds.append(chosen)
            else:
                st.warning(f"You can compare at most {MAX_COMPARE_FUNDS} funds.")

    if st.session_state.compare_funds:
        st.caption("Selected funds (click to remove):")
        cols = st.columns(min(5, len(st.session_state.compare_funds)))
        to_remove = None
        for i, nm in enumerate(st.session_state.compare_funds):
            if cols[i % len(cols)].button(f"❌ {nm}", key=f"rm_{i}"):
                to_remove = nm
        if to_remove:
            st.session_state.compare_funds = [x for x in st.session_state.compare_funds if x != to_remove]
            st.session_state.weight_map.pop(to_remove, None)

# === 🧮 P&C: Best 5 diversified funds ===
with st.expander("🧮 P&C: Best 5 diversified funds", expanded=False):
    st.caption("Simple flow: score funds by return vs risk, then pick a diversified top 5 with correlation control.")
    preset_map = {
        "Conservative": {
            "include_regex": "Direct|Growth|Index|Large|Flexi|Balanced|Hybrid",
            "exclude_regex": "Regular|Dividend|IDCW|Sector|Thematic|Small|Mid|Liquid|Overnight|Debt|Gilt|Arbitrage",
            "lookback": "120M",
            "min_months": 60,
            "profile": "Low",
            "corr_cap": 0.75,
            "one_per_bucket": True,
        },
        "Balanced": {
            "include_regex": "Direct|Growth",
            "exclude_regex": "Regular|Dividend|IDCW|Liquid|Overnight|Debt|Gilt|Arbitrage",
            "lookback": "60M",
            "min_months": 36,
            "profile": "Medium",
            "corr_cap": 0.88,
            "one_per_bucket": True,
        },
        "Aggressive": {
            "include_regex": "Direct|Growth|Flexi|Mid|Small|International|Global|Sector|Thematic",
            "exclude_regex": "Regular|Dividend|IDCW|Liquid|Overnight|Debt|Gilt|Arbitrage",
            "lookback": "36M",
            "min_months": 24,
            "profile": "High",
            "corr_cap": 0.93,
            "one_per_bucket": False,
        },
    }

    pnc_defaults = {
        "pnc_include_regex": preset_map["Balanced"]["include_regex"],
        "pnc_exclude_regex": preset_map["Balanced"]["exclude_regex"],
        "pnc_lookback": preset_map["Balanced"]["lookback"],
        "pnc_min_months": preset_map["Balanced"]["min_months"],
        "pnc_profile": preset_map["Balanced"]["profile"],
        "pnc_corr_cap": preset_map["Balanced"]["corr_cap"],
        "pnc_one_per_bucket": preset_map["Balanced"]["one_per_bucket"],
        "pnc_show_adv": False,
        "pnc_preset_choice": "Balanced",
    }
    for k, v in pnc_defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
    if "pnc_last_picks" not in st.session_state:
        st.session_state["pnc_last_picks"] = []

    pr1, pr2 = st.columns([1.5, 1])
    with pr1:
        preset_choice = st.selectbox("Preset", ["Conservative", "Balanced", "Aggressive"], key="pnc_preset_choice")
    with pr2:
        if st.button("Apply preset", use_container_width=True, key="pnc_apply_preset"):
            cfg = preset_map[preset_choice]
            st.session_state["pnc_include_regex"] = cfg["include_regex"]
            st.session_state["pnc_exclude_regex"] = cfg["exclude_regex"]
            st.session_state["pnc_lookback"] = cfg["lookback"]
            st.session_state["pnc_min_months"] = int(cfg["min_months"])
            st.session_state["pnc_profile"] = cfg["profile"]
            st.session_state["pnc_corr_cap"] = float(cfg["corr_cap"])
            st.session_state["pnc_one_per_bucket"] = bool(cfg["one_per_bucket"])
            st.session_state["pnc_show_adv"] = True
            try:
                st.rerun()
            except Exception:
                st.experimental_rerun()

    p1, p2, p3 = st.columns(3)
    with p1:
        include_regex = st.text_input("Include names (regex)", key="pnc_include_regex")
        exclude_regex = st.text_input("Exclude names (regex)", key="pnc_exclude_regex")
    with p2:
        lookback = st.selectbox("Lookback window", ["36M", "60M", "120M"], key="pnc_lookback")
        lbm = {"36M": 36, "60M": 60, "120M": 120}[lookback]
        min_months = st.number_input("Minimum months of history", 12, 240, step=6, key="pnc_min_months")
    with p3:
        profile = st.selectbox("Risk profile", ["Low", "Medium", "High"], key="pnc_profile")
        st.caption("Universe: all matching mutual funds (no cap)")

    show_adv_pnc = st.checkbox("Show advanced diversification settings", key="pnc_show_adv")
    corr_cap = float(st.session_state.get("pnc_corr_cap", 0.88))
    one_per_bucket = bool(st.session_state.get("pnc_one_per_bucket", True))
    if show_adv_pnc:
        a1, a2 = st.columns(2)
        with a1:
            corr_cap = st.slider("Max pairwise absolute correlation", 0.60, 0.99, step=0.01, key="pnc_corr_cap")
        with a2:
            one_per_bucket = st.checkbox("Prefer one fund per style bucket", key="pnc_one_per_bucket")

    if st.button("🔍 Run Best-5 scan", use_container_width=True):
        prog = st.progress(0, text="Starting scan…")
        def _cb(frac: float, msg: str): prog.progress(frac, text=f"{msg} ({int(frac*100)}%)")
        scan_error = False
        try:
            picks, metrics, corr_sel, chart_df = universe_scan_top5(
                df_schemes=df_schemes,
                include_regex=include_regex,
                exclude_regex=exclude_regex,
                lookback_months=lbm,
                min_months=int(min_months),
                universe_cap=None,
                profile=profile,
                corr_cap=float(corr_cap),
                progress_cb=_cb,
                one_per_bucket=one_per_bucket,
            )
        except ValueError as e:
            _cb(1.0, "Scan stopped")
            st.warning(str(e))
            scan_error = True
            st.session_state["pnc_last_picks"] = []
            picks, metrics, corr_sel, chart_df = [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        except Exception as e:
            _cb(1.0, "Scan failed")
            st.error(f"Scan failed: {e}")
            scan_error = True
            st.session_state["pnc_last_picks"] = []
            picks, metrics, corr_sel, chart_df = [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        _cb(1.0, "Scan complete")

        if not picks and not scan_error:
            st.warning("No suitable combination found with the current filters.")
            st.session_state["pnc_last_picks"] = []
        elif picks:
            st.session_state["pnc_last_picks"] = list(picks)
            st.success(f"Selected 5: {', '.join(picks)}")
            if not metrics.empty:
                st.dataframe(metrics, use_container_width=True, height=240)
                st.download_button(
                    "⬇️ Download selected metrics (CSV)",
                    data=metrics.to_csv(index=False).encode("utf-8"),
                    file_name="pnc_selected_metrics.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            if not corr_sel.empty:
                st.markdown("**Pairwise absolute Pearson correlation (selected funds)**")
                st.dataframe(corr_sel, use_container_width=True, height=220)

            if not chart_df.empty:
                st.markdown("**Normalized performance (base ₹10 at common start)**")
                st.line_chart(chart_df.set_index("date"), height=320, use_container_width=True)

    last_picks = st.session_state.get("pnc_last_picks", [])
    if last_picks:
        st.caption(f"Last scan picks: {', '.join(last_picks)}")
        if st.button("➕ Push last Best-5 into Compare", use_container_width=True, key="pnc_push_last"):
            picked_codes = []
            for p in last_picks:
                m = re.search(r"\[(\d+)\]\s*$", str(p))
                if m:
                    picked_codes.append(int(m.group(1)))
            if not picked_codes:
                st.warning("Could not parse scheme codes from selected picks.")
            else:
                code_to_name = df_schemes.set_index("scheme_code")["scheme_name"].to_dict()
                to_add = [code_to_name[c] for c in picked_codes if c in code_to_name]
                st.session_state.compare_funds = to_add[:5]
                st.success("Loaded into Compare panel (up to 5).")

# === 🔁 DERIVED NAV (weighted mix) — above SWP ===
with st.expander("🧪 Build derived NAV (weighted mix)", expanded=False):
    selected = st.session_state.compare_funds
    if len(selected) < 2:
        st.info("Pick at least two funds in the comparison panel above to build a derived NAV.")
    else:
        code_map = df_schemes.set_index("scheme_name")["scheme_code"].to_dict()
        histories = {}
        for nm in selected:
            try:
                sc = int(code_map[nm]); h = fetch_nav_history(sc)
                if not h.empty: histories[nm] = h
            except Exception: pass

        if len(histories) < 2:
            st.warning("Could not fetch enough histories.")
        else:
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1: tf_derived = st.selectbox("Timeframe", ["Full","YTD","1Y","3Y","5Y","10Y"], index=0)
            with c2: overlay_const = st.checkbox("Overlay constituents", value=True)
            with c3: base_val = st.number_input("Base index value", min_value=1.0, value=10.0, step=1.0)

            starts = [df["date"].iloc[0] for df in histories.values()]
            common_start = max(starts)

            def _apply_tf(df: pd.DataFrame, label: str) -> pd.DataFrame:
                if df.empty or label == "Full": return df
                last_date = df["date"].iloc[-1]
                start_map = {
                    "YTD": pd.Timestamp(year=last_date.year, month=1, day=1),
                    "1Y": last_date - pd.DateOffset(years=1),
                    "3Y": last_date - pd.DateOffset(years=3),
                    "5Y": last_date - pd.DateOffset(years=5),
                    "10Y": last_date - pd.DateOffset(years=10),
                }
                return df[df["date"] >= start_map[label]].reset_index(drop=True)

            norm_frames = []
            for nm, h in histories.items():
                dfh = h[h["date"] >= common_start].reset_index(drop=True).copy()
                if dfh.empty: continue
                base_row = nearest_nav_on_or_after(dfh, common_start)
                if base_row is None or float(base_row["nav"]) <= 0: continue
                factor = float(base_val) / float(base_row["nav"])
                col = f"{nm}"
                dfh[col] = dfh["nav"] * factor
                dfh = _apply_tf(dfh[["date", col]], tf_derived)
                if not dfh.empty: norm_frames.append(dfh)

            if len(norm_frames) < 2:
                st.warning("Not enough overlapping data in the chosen timeframe.")
            else:
                df_all = norm_frames[0]
                for dfh in norm_frames[1:]:
                    df_all = df_all.merge(dfh, on="date", how="inner")

                if df_all.empty or df_all.shape[1] < 3:
                    st.warning("No overlapping dates after alignment/timeframe.")
                else:
                    st.subheader("Weights (percent)")
                    cols = st.columns(min(5, len(selected)))
                    percents = []
                    for i, nm in enumerate(selected):
                        default_pct = st.session_state.weight_map.get(nm, round(100.0 / len(selected), 2))
                        val = cols[i % len(cols)].number_input(nm, min_value=0.0, max_value=100.0, value=float(default_pct), step=1.0, key=f"w_{nm}")
                        st.session_state.weight_map[nm] = val
                        percents.append(val)

                    w_raw = np.array(percents, dtype=float); w_sum = float(w_raw.sum())
                    w_norm = (np.ones_like(w_raw) / len(w_raw)) if w_sum <= 0 else (w_raw / w_sum)

                    fund_cols = [nm for nm in selected if nm in df_all.columns]
                    W = np.array([w_norm[selected.index(nm)] for nm in fund_cols], dtype=float)
                    w_present_sum = float(W.sum())
                    if w_present_sum <= 0:
                        W = np.ones(len(fund_cols), dtype=float) / max(1, len(fund_cols))
                    else:
                        W = W / w_present_sum
                    values = df_all[fund_cols].to_numpy()
                    derived = values.dot(W)
                    out = pd.DataFrame({"date": df_all["date"], f"Derived (base ₹{int(base_val)})": derived})

                    plot_df = out.merge(df_all[["date"] + fund_cols], on="date", how="left") if overlay_const else out
                    st.line_chart(plot_df.set_index("date"), height=330)

                    applied = {nm: round(float(w)*100, 2) for nm, w in zip(fund_cols, W)}
                    dropped = [nm for nm in selected if nm not in fund_cols]
                    cap = f"Applied weights (normalized over available funds): {applied}"
                    if dropped:
                        cap += f" | Dropped (no overlap): {dropped}"
                    st.caption(cap)

                    st.download_button("⬇️ Download derived NAV (CSV)", data=out.to_csv(index=False).encode("utf-8"),
                                       file_name="derived_nav_weighted.csv", mime="text/csv", use_container_width=True)

                    # Trailing returns matrix
                    st.subheader("Trailing returns (CAGR) — Derived + constituents")
                    dcol = out.columns[1]; end_date = out["date"].iloc[-1]
                    periods = [("3M", 3), ("6M", 6), ("12M", 12), ("36M", 36), ("60M", 60)]

                    def cagr_for(df: pd.DataFrame, col: str, months: int) -> float:
                        if df.empty or months <= 0 or col not in df.columns: return np.nan
                        target = end_date - pd.DateOffset(months=months)
                        idxs = df.index[df["date"] >= target]
                        if len(idxs) == 0: return np.nan
                        start_idx = int(idxs[0]); start_val = float(df.loc[start_idx, col]); end_val_local = float(df[col].iloc[-1])
                        if start_val <= 0 or end_val_local <= 0: return np.nan
                        start_dt = pd.Timestamp(df.loc[start_idx, "date"])
                        end_dt = pd.Timestamp(df["date"].iloc[-1])
                        years = max(1e-9, (end_dt - start_dt).days / 365.25)
                        return (end_val_local / start_val) ** (1.0 / years) - 1.0

                    rows = []
                    derived_row = {"Instrument": dcol}
                    for label, m in periods: derived_row[label] = cagr_for(out, dcol, m)
                    rows.append(derived_row)
                    for nm in fund_cols:
                        r = {"Instrument": nm}
                        for label, m in periods: r[label] = cagr_for(df_all, nm, m)
                        rows.append(r)

                    ret_df = pd.DataFrame(rows); fmt_df = ret_df.copy()
                    for label, _ in periods: fmt_df[label] = fmt_df[label].apply(lambda x: "—" if pd.isna(x) else f"{x * 100:.2f}%")
                    st.dataframe(fmt_df, use_container_width=True, height=260)
                    st.download_button("⬇️ Download trailing returns matrix (CSV)",
                                       data=ret_df.to_csv(index=False).encode("utf-8"),
                                       file_name="trailing_returns_matrix.csv", mime="text/csv",
                                       use_container_width=True)

# === 🔧 SWP Framework (inflation + risk-weighted portfolio) — AUTO projection CAGR ===
with st.expander("🔧 SWP Framework (inflation + risk-weighted portfolio)", expanded=True):
    colA, colB, colC, colD = st.columns(4)
    with colA: swp_years = st.number_input("Years funds needed", min_value=1, max_value=60, value=30, step=1)
    with colB: annual_infl = st.number_input("Annual inflation step-up (%)", min_value=0.0, max_value=20.0, value=6.0, step=0.5) / 100.0
    with colC: risk_profile = st.selectbox("Risk profile by volatility", ["Low","Medium","High"], index=1)
    with colD: risk_lookback = st.selectbox("Vol lookback", ["3Y","5Y","1Y","Full"], index=0)

    colE, colF = st.columns(2)
    with colE: start_date = st.date_input("SWP start month", value=dt.date.today().replace(day=1))
    with colF:
        corpus_mode = st.radio("Corpus reference", ["NAV at start (units=1)", "Custom amount (₹)"], index=0)
        custom_corpus = st.number_input("Custom starting amount (₹)", min_value=1.0, value=1_000_000.0, step=50_000.0) if corpus_mode == "Custom amount (₹)" else None

    colG, colH, colI = st.columns(3)
    with colG:
        withdrawal_source_ui = st.selectbox(
            "Withdrawal source policy",
            ["Tax/exit-load aware", "Lowest-return first"],
            index=0,
        )
    with colH:
        use_guardrails = st.checkbox("Enable guardrail spending", value=True)
    with colI:
        mc_paths = st.selectbox("Monte Carlo paths", [0, 200, 500, 1000], index=2, help="0 disables MC simulation.")

    guardrail_band = 0.20
    guardrail_cut_pct = 0.10
    guardrail_raise_pct = 0.05
    if use_guardrails:
        g1, g2, g3 = st.columns(3)
        with g1:
            guardrail_band = st.slider("Guardrail band (%)", 5, 50, value=20, step=5) / 100.0
        with g2:
            guardrail_cut_pct = st.slider("Cut spending when above upper rail (%)", 5, 40, value=10, step=1) / 100.0
        with g3:
            guardrail_raise_pct = st.slider("Raise spending when below lower rail (%)", 2, 25, value=5, step=1) / 100.0

    equity_stcg_rate = 0.15
    equity_ltcg_rate = 0.10
    equity_ltcg_days = 365
    debt_stcg_rate = 0.30
    debt_ltcg_rate = 0.20
    debt_ltcg_days = 1095
    exit_load_days = 365
    exit_load_pct = 0.01
    show_tax_exit_assumptions = st.checkbox("Show tax and exit-load assumptions", value=False, key="show_tax_exit_assumptions")
    if show_tax_exit_assumptions:
        t1, t2, t3, t4 = st.columns(4)
        with t1:
            equity_stcg_rate = st.number_input("Equity STCG rate (%)", min_value=0.0, max_value=50.0, value=equity_stcg_rate * 100.0, step=0.5) / 100.0
            equity_ltcg_days = st.number_input("Equity LTCG threshold (days)", min_value=1, max_value=3650, value=equity_ltcg_days, step=1)
        with t2:
            equity_ltcg_rate = st.number_input("Equity LTCG rate (%)", min_value=0.0, max_value=50.0, value=equity_ltcg_rate * 100.0, step=0.5) / 100.0
            debt_ltcg_days = st.number_input("Debt LTCG threshold (days)", min_value=1, max_value=3650, value=debt_ltcg_days, step=1)
        with t3:
            debt_stcg_rate = st.number_input("Debt STCG rate (%)", min_value=0.0, max_value=50.0, value=debt_stcg_rate * 100.0, step=0.5) / 100.0
            exit_load_days = st.number_input("Exit load window (days)", min_value=0, max_value=3650, value=exit_load_days, step=1)
        with t4:
            debt_ltcg_rate = st.number_input("Debt LTCG rate (%)", min_value=0.0, max_value=50.0, value=debt_ltcg_rate * 100.0, step=0.5) / 100.0
            exit_load_pct = st.number_input("Exit load rate (%)", min_value=0.0, max_value=10.0, value=exit_load_pct * 100.0, step=0.1) / 100.0

    selected = st.session_state.compare_funds.copy()
    if not selected and chosen: selected = [chosen]
    st.caption(f"Funds in SWP portfolio: {', '.join(selected) if selected else '—'}")

    code_map = df_schemes.set_index("scheme_name")["scheme_code"].to_dict()
    histories: Dict[str, pd.DataFrame] = {}
    for nm in selected:
        try:
            sc = int(code_map[nm]); h = fetch_nav_history(sc)
            if not h.empty: histories[nm] = h
        except Exception: pass

    if not histories:
        st.info("Add at least one fund in the compare panel or choose a fund above.")
    else:
        vol_table, bucket_map = compute_vol_buckets(histories, lookback=risk_lookback)
        tgt_bucket = profile_bucket_weights(risk_profile)
        fund_weights_raw = derive_fund_weights_from_buckets(list(histories.keys()), bucket_map, tgt_bucket)
        fund_weights, floor_feasible = apply_min_weight_floor(fund_weights_raw, min_weight=0.20)

        if not vol_table.empty:
            vt = vol_table.copy(); vt["weight_%"] = vt["fund"].map(lambda f: round(fund_weights.get(f, 0.0)*100.0, 2)); vt["vol"] = vt["vol"].round(4)
            st.dataframe(vt.rename(columns={"fund":"Fund","vol":"StdDev (monthly)","bucket":"Vol Bucket","weight_%":"Weight %"}),
                         use_container_width=True, height=220)
        if not floor_feasible:
            st.warning("Min-20% floor was infeasible for this fund count; using equal weights.")
        else:
            st.caption("SWP rules: minimum **20%** per fund, configurable withdrawal-source policy, and rebalance to target weights every **24 months**.")

            start_ts = pd.Timestamp(start_date)
            start_month = start_ts.normalize().replace(day=1)
            nav_forward_map: Dict[str, pd.DataFrame] = {}
            est_cagr_map: Dict[str, float] = {}

            for nm, h in histories.items():
                hist_upto_start = h[h["date"] <= start_ts].copy()
                if hist_upto_start.empty:
                    continue
                months_avail = max(0, (hist_upto_start["date"].iloc[-1] - hist_upto_start["date"].iloc[0]).days // 30) if len(hist_upto_start) > 1 else 0
                years_avail = months_avail / 12.0
                lb_years = 10 if years_avail >= 10 else (5 if years_avail >= 5 else None)
                est_cagr_i, _diag_i = estimate_proj_cagr(
                    hist_upto_start,
                    end_date=start_ts,
                    method="Blended (EWMA + Long)",
                    lookback_years=lb_years,
                    winsorize_pct=0.015,
                    ewma_half_life_months=12,
                    blend_alpha=0.65,
                )
                est_cagr_map[nm] = est_cagr_i
                h_proj = hist_upto_start
                if h_proj["date"].iloc[-1] < start_ts:
                    h_proj = extend_nav_with_projection(h_proj, years_forward=5.0, assumed_annual_return=est_cagr_i)
                nav_forward_map[nm] = extend_nav_with_projection(h_proj, years_forward=swp_years + 50.0, assumed_annual_return=est_cagr_i)

            if not nav_forward_map:
                st.warning("Could not prepare forward NAV series for the selected funds.")
            else:
                active = []
                nav_at_start_map: Dict[str, float] = {}
                for nm, df in nav_forward_map.items():
                    row = nearest_nav_on_or_after(df, start_month)
                    if row is None:
                        continue
                    nv = float(row["nav"])
                    if nv > 0:
                        active.append(nm)
                        nav_at_start_map[nm] = nv
                if not active:
                    st.warning("No NAV available at/after SWP start for selected funds.")
                else:
                    w_act = np.array([max(0.0, float(fund_weights.get(nm, 0.0))) for nm in active], dtype=float)
                    if float(w_act.sum()) <= 0:
                        w_act = np.ones(len(active), dtype=float) / len(active)
                    else:
                        w_act = w_act / float(w_act.sum())
                    effective_weights = {nm: float(wi) for nm, wi in zip(active, w_act)}
                    est_cagr = float(sum(effective_weights[nm] * est_cagr_map.get(nm, 0.0) for nm in active))
                    withdrawal_mode = "tax_aware" if withdrawal_source_ui == "Tax/exit-load aware" else "lowest_return"
                    fund_tax_profiles = {nm: infer_tax_profile_from_name(nm) for nm in active}
                    tax_cfg = {
                        "equity_stcg_rate": float(equity_stcg_rate),
                        "equity_ltcg_rate": float(equity_ltcg_rate),
                        "equity_ltcg_days": int(equity_ltcg_days),
                        "debt_stcg_rate": float(debt_stcg_rate),
                        "debt_ltcg_rate": float(debt_ltcg_rate),
                        "debt_ltcg_days": int(debt_ltcg_days),
                    }
                    guardrail_upper_mult = 1.0 + float(guardrail_band)
                    guardrail_lower_mult = max(0.0, 1.0 - float(guardrail_band))

                    nav_at_start = float(sum(effective_weights[nm] * nav_at_start_map[nm] for nm in active))
                    corpus_value = float(custom_corpus) if (custom_corpus is not None and custom_corpus > 0) else nav_at_start

                    sim_nav_map = {nm: nav_forward_map[nm] for nm in active}

                    def _sim_for_withdrawal(start_w: float, years: Optional[int], extra_years: int) -> pd.DataFrame:
                        return simulate_swp_multifund_inflation(
                            nav_map=sim_nav_map,
                            start_date=start_date,
                            corpus_value=corpus_value,
                            start_withdrawal_monthly=float(start_w),
                            annual_inflation=annual_infl,
                            target_weights=effective_weights,
                            rebalance_every_months=24,
                            max_years=years,
                            max_extra_years=extra_years,
                            withdrawal_mode=withdrawal_mode,
                            fund_tax_profiles=fund_tax_profiles,
                            tax_cfg=tax_cfg,
                            exit_load_days=int(exit_load_days),
                            exit_load_pct=float(exit_load_pct),
                            use_guardrails=bool(use_guardrails),
                            guardrail_upper_mult=float(guardrail_upper_mult),
                            guardrail_lower_mult=float(guardrail_lower_mult),
                            guardrail_cut_pct=float(guardrail_cut_pct),
                            guardrail_raise_pct=float(guardrail_raise_pct),
                        )

                    pct, path_best = find_max_starting_withdrawal_percent(
                        nav_series=pd.DataFrame(),
                        start_date=start_date,
                        corpus_value=corpus_value,
                        annual_inflation=annual_infl,
                        years_needed=swp_years,
                        sim_fn=lambda w: _sim_for_withdrawal(w, swp_years, 80),
                    )
                    start_w_best = pct * corpus_value

                    red_pp = st.number_input("Withdraw less by (percentage points of corpus per month)", min_value=0.00, max_value=10.00, value=0.10, step=0.01)
                    red_dec = red_pp / 100.0
                    reduced_pct = max(0.0, pct - red_dec)
                    start_w_reduced = reduced_pct * corpus_value
                    months_reduced, path_reduced = longevity_for_withdrawal(
                        nav_series=pd.DataFrame(),
                        start_date=start_date,
                        corpus_value=corpus_value,
                        start_withdrawal_monthly=start_w_reduced,
                        annual_inflation=annual_infl,
                        sim_fn=lambda w: _sim_for_withdrawal(w, None, 100),
                    )

                    horizon_months = swp_years * 12
                    extra_months = max(0, months_reduced - horizon_months)
                    extra_years = extra_months // 12
                    extra_rem_m = extra_months % 12

                    ca, cb, cc, cd = st.columns(4)
                    with ca: st.metric("Projected CAGR (weighted auto)", f"{est_cagr*100:.2f}% p.a.")
                    with cb: st.metric("Max % of corpus / month", f"{pct*100:.3f}%")
                    with cc:
                        if custom_corpus is not None and custom_corpus > 0:
                            st.metric("Max start withdrawal", f"₹{start_w_best:,.0f}/mo")
                        else:
                            st.metric("Max amount per NAV unit", f"₹{start_w_best:,.4f}/mo")
                    with cd: st.metric("Longevity gain", f"+{extra_years}y {extra_rem_m}m")

                    st.caption(
                        "Auto projection per fund: **Blended (EWMA+Long)** with winsorization **1.5%** and EWMA half-life **12m**.  \n"
                        f"Withdrawal policy: **{withdrawal_source_ui}**; rebalanced every **24 months** back to target weights.  \n"
                        f"If you withdraw **−{red_pp:.2f} pp** of corpus/month (start at **{reduced_pct*100:.3f}%**), "
                        f"your money lasts about **+{extra_years} years {extra_rem_m} months** longer than {swp_years} years.  \n"
                        f"**Annual inflation:** {annual_infl*100:.2f}% p.a."
                    )

                    if mc_paths > 0 and st.button("Run Monte Carlo survival analysis", use_container_width=True):
                        hist_active = {nm: histories[nm] for nm in active if nm in histories}
                        ret_panel = build_joint_monthly_return_panel(hist_active, start_ts)
                        if ret_panel.empty or len(ret_panel) < 24:
                            st.warning("Not enough overlapping monthly history for Monte Carlo (need at least 24 months).")
                        else:
                            mc = monte_carlo_swp_survival(
                                start_date=start_date,
                                years_needed=int(swp_years),
                                paths=int(mc_paths),
                                random_seed=42,
                                start_nav_map={nm: nav_at_start_map[nm] for nm in active},
                                return_panel=ret_panel,
                                corpus_value=corpus_value,
                                start_withdrawal_monthly=start_w_best,
                                annual_inflation=annual_infl,
                                target_weights=effective_weights,
                                withdrawal_mode=withdrawal_mode,
                                fund_tax_profiles=fund_tax_profiles,
                                tax_cfg=tax_cfg,
                                exit_load_days=int(exit_load_days),
                                exit_load_pct=float(exit_load_pct),
                                use_guardrails=bool(use_guardrails),
                                guardrail_upper_mult=float(guardrail_upper_mult),
                                guardrail_lower_mult=float(guardrail_lower_mult),
                                guardrail_cut_pct=float(guardrail_cut_pct),
                                guardrail_raise_pct=float(guardrail_raise_pct),
                            )
                            m1, m2, m3 = st.columns(3)
                            with m1:
                                st.metric("MC survival probability", "—" if pd.isna(mc["survival_prob"]) else f"{mc['survival_prob']*100:.1f}%")
                            with m2:
                                st.metric("MC median end corpus", "—" if pd.isna(mc["p50_end"]) else f"₹{mc['p50_end']:,.0f}")
                            with m3:
                                st.metric("MC P10/P90 end corpus", "—" if (pd.isna(mc["p10_end"]) or pd.isna(mc["p90_end"])) else f"₹{mc['p10_end']:,.0f} / ₹{mc['p90_end']:,.0f}")

                    if not path_best.empty:
                        st.markdown("**Portfolio value — Max % path**")
                        st.area_chart(path_best.set_index("date")["portfolio_value"], height=220, use_container_width=True)
                    if not path_reduced.empty:
                        st.markdown(f"**Portfolio value — Reduced path (−{red_pp:.2f} pp)**")
                        st.area_chart(path_reduced.set_index("date")["portfolio_value"], height=220, use_container_width=True)

                    show_cashflow_tables = st.checkbox("Show SWP cashflow tables", value=False, key="show_swp_cashflow_tables")
                    if show_cashflow_tables:
                        if not path_best.empty:
                            t1 = path_best.copy(); t1["withdrawal"] = t1["withdrawal"].round(4); t1["portfolio_value"] = t1["portfolio_value"].round(4)
                            st.markdown("**Max withdrawal path**"); st.dataframe(t1.head(12), use_container_width=True, height=240); st.dataframe(t1.tail(12), height=240)
                        if not path_reduced.empty:
                            t2 = path_reduced.copy(); t2["withdrawal"] = t2["withdrawal"].round(4); t2["portfolio_value"] = t2["portfolio_value"].round(4)
                            st.markdown(f"**Reduced withdrawal path (−{red_pp:.2f} pp)**"); st.dataframe(t2.head(12), height=240); st.dataframe(t2.tail(12), height=240)

                    if not path_best.empty:
                        st.download_button("⬇️ Download SWP path (max) CSV", data=path_best.to_csv(index=False).encode("utf-8"),
                                           file_name="swp_path_max.csv", mime="text/csv", use_container_width=True)
                    if not path_reduced.empty:
                        st.download_button("⬇️ Download SWP path (reduced) CSV", data=path_reduced.to_csv(index=False).encode("utf-8"),
                                           file_name="swp_path_reduced.csv", mime="text/csv", use_container_width=True)

# Optional legacy inputs
with col2:
    st.subheader("Legacy Inputs (optional)")
    lump_sum = st.number_input("Lump sum invested (₹)", min_value=0, value=0, step=1000)
    lump_date = st.date_input("Lump sum date", value=dt.date.today().replace(day=1))
    sip_amt = st.number_input("SIP monthly (₹)", min_value=0, value=10000, step=1000)
    sip_start = st.date_input("SIP start", value=dt.date.today().replace(day=1))
    sip_end = st.date_input("SIP end (inclusive)", value=dt.date.today().replace(day=1))

st.caption("Diversified P&C uses pairwise correlations on an outer-joined returns panel, so selections won’t collapse. SWP now supports a 20% per-fund floor, tax/exit-load aware drawdown, guardrail spending, biennial rebalance, and Monte Carlo survival checks.")
