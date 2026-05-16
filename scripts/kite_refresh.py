#!/usr/bin/env python3
"""Refresh Kite access token using the hardened Auto_Trader token generator.

This script intentionally delegates login to Auto_Trader.Request_Token instead of
maintaining a second HTTP-login implementation. The older duplicate flow used a
large Chrome-like header set (Sec-Fetch/Accept-Encoding/etc.) and was observed to
trigger Zerodha's CAPTCHA branch before TOTP.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TOKEN_PATH = ROOT / "intermediary_files" / "access_token.json"

# Load secrets without importing the Auto_Trader package __init__; local laptops
# can have optional dependency drift, but token refresh only needs Kite secrets.
_secrets: dict[str, Any] = {}
exec((ROOT / "Auto_Trader" / "my_secrets.py").read_text(encoding="utf-8"), _secrets)
API_KEY = _secrets["API_KEY"]
API_SECRET = _secrets["API_SECRET"]


def _load_module(name: str, path: Path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _read_token() -> dict[str, Any]:
    try:
        return json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


def _token_is_today(payload: dict[str, Any]) -> bool:
    return bool(payload.get("access_token")) and str(payload.get("date")) == datetime.now().strftime("%Y-%m-%d")


def _write_token(access_token: str) -> dict[str, str]:
    payload = {"access_token": access_token, "date": datetime.now().strftime("%Y-%m-%d")}
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = TOKEN_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=4), encoding="utf-8")
    tmp.replace(TOKEN_PATH)
    return payload


def refresh(force: bool = False) -> dict[str, Any]:
    existing = _read_token()
    if _token_is_today(existing) and not force:
        print(f"Using existing Kite access token for {existing['date']} at {TOKEN_PATH}")
        return existing

    # Load the isolated login implementation directly. It uses the minimal
    # header set documented in Auto_Trader/Request_Token.py and avoids the stale
    # duplicate header path that caused CAPTCHA responses.
    request_token_mod = _load_module("autotrader_request_token_isolated", ROOT / "Auto_Trader" / "Request_Token.py")

    from kiteconnect import KiteConnect  # type: ignore

    kite = KiteConnect(api_key=API_KEY)
    request_token = request_token_mod.get_request_token(
        {
            "api_key": API_KEY,
            "username": _secrets["USER_NAME"],
            "password": _secrets["PASS"],
            "totp_key": _secrets["TOTP_KEY"],
        }
    )
    data = kite.generate_session(request_token=request_token, api_secret=API_SECRET)
    payload = _write_token(data["access_token"])
    print(f"Wrote fresh Kite access token for {payload['date']} to {TOKEN_PATH}")
    return payload


def check_token() -> None:
    payload = _read_token()
    if not payload.get("access_token"):
        raise SystemExit(f"No access token found at {TOKEN_PATH}")
    from kiteconnect import KiteConnect  # type: ignore

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(payload["access_token"])
    holdings = kite.holdings()
    mf_holdings = kite.mf_holdings()
    print(
        json.dumps(
            {
                "token_date": payload.get("date"),
                "equity_holdings": len(holdings),
                "mf_holdings": len(mf_holdings),
            },
            indent=2,
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh/check Kite access token")
    parser.add_argument("--force", action="store_true", help="Force a fresh login even if today's token exists")
    parser.add_argument("--check", action="store_true", help="Validate existing token by fetching holdings")
    args = parser.parse_args()

    if args.check:
        check_token()
        return 0
    refresh(force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
