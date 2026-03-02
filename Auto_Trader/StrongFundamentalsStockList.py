from Fundamentals import Tickertape
import json
import logging
import os
import pandas as pd
import traceback

logger = logging.getLogger("Auto_Trade_Logger")
ETF_PREFS_PATH = "intermediary_files/etf_preferences.json"


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
        ][["info.ticker", "sid", "info.sector"]]
        non_etf_df = non_etf_df.assign(ETFTheme="")

        # Combine both
        combined_df = pd.concat([non_etf_df, etf_df], ignore_index=True)

        # Rename for clarity
        combined_df = combined_df.rename(
            columns={"info.ticker": "Symbol", "info.sector": "Sector"}
        )
        combined_df["AssetClass"] = combined_df["Sector"].apply(
            lambda x: "ETF" if "ETF" in _normalize_text(x) else "EQUITY"
        )
        combined_df["ETFTheme"] = combined_df["ETFTheme"].fillna("")

        # Final clean output
        return combined_df[["Symbol", "Sector", "AssetClass", "ETFTheme"]]

    except Exception as e:
        logger.error(f"An error occurred: {e}, Traceback: {traceback.format_exc()}")
        raise e
