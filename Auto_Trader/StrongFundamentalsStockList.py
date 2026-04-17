from Fundamentals import Tickertape
import json
import logging
import os
import pandas as pd
import traceback

logger = logging.getLogger("Auto_Trade_Logger")
ETF_PREFS_PATH = "intermediary_files/etf_preferences.json"

NIFTY50_SYMBOLS = {
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL",
    "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "INDIGO", "INFY", "ITC",
    "JIOFIN", "JSWSTEEL", "KOTAKBANK", "LT", "M&M",
    "MARUTI", "MAXHEALTH", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TMPV", "TATASTEEL",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
}

LARGE_CAP_THRESHOLD_CR = 50000
MID_CAP_THRESHOLD_CR = 5000
SMALL_CAP_THRESHOLD_CR = 500


def _normalize_text(value) -> str:
    return str(value or "").strip().upper()


def _infer_etf_theme(symbol: str, sector: str) -> str:
    text = f"{_normalize_text(symbol)} {_normalize_text(sector)}"
    if "SILVER" in text:
        return "SILVER"
    if "GOLD" in text:
        return "GOLD"
    if "BANK" in text:
        return "BANK"
    if "NIFTY" in text and "NEXT" in text:
        return "NIFTY_NEXT"
    if "NIFTY" in text:
        return "NIFTY_50"
    if "MIDCAP" in text:
        return "MIDCAP"
    if "SMALLCAP" in text:
        return "SMALLCAP"
    if "IT" in text:
        return "IT"
    return _normalize_text(sector) or "ETF_GENERIC"


def _classify_cap_bucket(market_cap_cr) -> str:
    try:
        value = float(market_cap_cr)
    except Exception:
        return "UNKNOWN"
    if not pd.notna(value):
        return "UNKNOWN"
    if value > LARGE_CAP_THRESHOLD_CR:
        return "LARGE_CAP"
    if value > MID_CAP_THRESHOLD_CR:
        return "MID_CAP"
    if value > SMALL_CAP_THRESHOLD_CR:
        return "SMALL_CAP"
    return "MICRO_CAP"


def _load_etf_preferences() -> dict:
    try:
        with open(ETF_PREFS_PATH, "r") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("Failed loading ETF preferences from %s", ETF_PREFS_PATH)
        return {}


def _save_etf_preferences(preferences: dict) -> None:
    try:
        os.makedirs(os.path.dirname(ETF_PREFS_PATH), exist_ok=True)
        with open(ETF_PREFS_PATH, "w") as f:
            json.dump(preferences, f, indent=2, sort_keys=True)
    except Exception:
        logger.exception("Failed saving ETF preferences to %s", ETF_PREFS_PATH)


def _select_persistent_etfs(etf_df: pd.DataFrame) -> pd.DataFrame:
    if etf_df.empty:
        return etf_df

    prefs = _load_etf_preferences()
    selected_rows = []
    updated = False

    for theme, group in etf_df.groupby("ETFTheme", sort=False):
        group = group.sort_values(
            by="advancedRatios.mrktCapf", ascending=False, na_position="last"
        )
        preferred_symbol = prefs.get(theme)
        chosen = None
        if preferred_symbol:
            match = group[group["info.ticker"] == preferred_symbol]
            if not match.empty:
                chosen = match.iloc[0]
        if chosen is None:
            chosen = group.iloc[0]
            prefs[theme] = str(chosen["info.ticker"])
            updated = True
        selected_rows.append(chosen)

    if updated:
        _save_etf_preferences(prefs)
    return pd.DataFrame(selected_rows)


def _parse_csv_env(name: str) -> set[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return set()
    return {_normalize_text(part) for part in raw.split(",") if part.strip()}


def goodStocks():
    ttp = Tickertape()

    try:
        # Fetch required columns, including info.sector
        filtered_list_df = ttp.get_equity_screener_data(
            filters=[
                "mrktCapf",  # Market Cap
                "apef",  # P/E Ratio
                "indpe",  # Sector PE
            ],
            sortby="mrktCapf",
            number_of_records=7000,
        )

        # Keep ETFs aside first (sector name contains 'ETF')
        etf_df = filtered_list_df[
            filtered_list_df["info.sector"].str.contains("ETF", case=False, na=False)
        ][["info.ticker", "sid", "info.sector", "advancedRatios.mrktCapf"]]
        etf_df = etf_df.assign(
            ETFTheme=etf_df.apply(
                lambda row: _infer_etf_theme(row["info.ticker"], row["info.sector"]),
                axis=1,
            )
        )
        etf_df = _select_persistent_etfs(etf_df)

        # Apply numeric filters for non-ETF stocks
        non_etf_df = filtered_list_df[
            (filtered_list_df["advancedRatios.apef"] <= 40)
            & (filtered_list_df["advancedRatios.apef"] > 0)
            & (filtered_list_df["advancedRatios.mrktCapf"] >= 500)
            & (
                filtered_list_df["advancedRatios.apef"]
                <= filtered_list_df["advancedRatios.indpe"]
            )
        ][["info.ticker", "sid", "info.sector", "advancedRatios.mrktCapf"]]
        non_etf_df = non_etf_df.assign(ETFTheme="")

        # Combine both
        combined_df = pd.concat([non_etf_df, etf_df], ignore_index=True)

        # Rename for clarity
        combined_df = combined_df.rename(
            columns={
                "info.ticker": "Symbol",
                "info.sector": "Sector",
                "advancedRatios.mrktCapf": "MarketCapCr",
            }
        )
        combined_df["AssetClass"] = combined_df["Sector"].apply(
            lambda x: "ETF" if "ETF" in _normalize_text(x) else "EQUITY"
        )
        combined_df["ETFTheme"] = combined_df["ETFTheme"].fillna("")
        combined_df["MarketCapCr"] = pd.to_numeric(combined_df["MarketCapCr"], errors="coerce")
        combined_df["CapBucket"] = combined_df.apply(
            lambda row: "ETF" if row["AssetClass"] == "ETF" else _classify_cap_bucket(row["MarketCapCr"]),
            axis=1,
        )
        combined_df["IsNifty50"] = combined_df["Symbol"].apply(lambda x: _normalize_text(x) in NIFTY50_SYMBOLS)

        requested_buckets = _parse_csv_env("AT_UNIVERSE_CAP_BUCKETS")
        if not requested_buckets:
            requested_buckets = {"LARGE_CAP", "MID_CAP", "ETF"}
        if requested_buckets:
            combined_df = combined_df[
                combined_df["CapBucket"].astype(str).str.upper().isin(requested_buckets)
            ].copy()

        if os.getenv("AT_UNIVERSE_NIFTY50_ONLY", "0").strip().lower() in {"1", "true", "yes"}:
            combined_df = combined_df[combined_df["IsNifty50"]].copy()

        min_mcap_raw = os.getenv("AT_UNIVERSE_MIN_MCAP_CR", "").strip()
        if min_mcap_raw:
            try:
                min_mcap = float(min_mcap_raw)
                combined_df = combined_df[
                    (combined_df["AssetClass"] == "ETF")
                    | (combined_df["MarketCapCr"].fillna(0) >= min_mcap)
                ].copy()
            except Exception:
                logger.warning("Ignoring invalid AT_UNIVERSE_MIN_MCAP_CR=%r", min_mcap_raw)

        # Final clean output
        return combined_df[["Symbol", "Sector", "AssetClass", "ETFTheme", "MarketCapCr", "CapBucket", "IsNifty50"]]

    except Exception as e:
        logger.error(f"An error occurred: {e}, Traceback: {traceback.format_exc()}")
        raise e
