#!/usr/bin/env python3
"""Refresh Kite access token using a real Playwright browser session.

This intentionally uses a visible browser by default so any broker-side human
challenge can be solved by the operator instead of bypassed or brute-forced.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import onetimepass as otp
from kiteconnect import KiteConnect
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

_secrets: dict = {}
with open(ROOT / "Auto_Trader" / "my_secrets.py", "r", encoding="utf-8") as fh:
    exec(fh.read(), _secrets)
API_KEY = _secrets["API_KEY"]
API_SECRET = _secrets["API_SECRET"]
PASS = _secrets["PASS"]
TOTP_KEY = _secrets["TOTP_KEY"]
USER_NAME = _secrets["USER_NAME"]

TOKEN_PATH = ROOT / "intermediary_files" / "access_token.json"
FLAG_PATH = ROOT / "intermediary_files" / "kite_manual_login_required.json"
AUTH_DIR = ROOT / "intermediary_files" / "playwright_kite_profile"
CHROME_FOR_TESTING = Path.home() / "Library/Caches/ms-playwright/chromium-1223/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
HEADLESS_SHELL = Path.home() / "Library/Caches/ms-playwright/chromium_headless_shell-1223/chrome-headless-shell-mac-arm64/chrome-headless-shell"
SYSTEM_CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")


def _mask(value: str) -> str:
    if not value:
        return ""
    return value[:2] + "***" + value[-2:]


def _safe_url(url: str) -> str:
    return re.sub(r"(api_key=)[^&]+", r"\1***", url)


def _find_request_token(url: str) -> str | None:
    parsed = urlparse(url)
    token = parse_qs(parsed.query).get("request_token")
    if token:
        return token[0]
    match = re.search(r"request_token=([A-Za-z0-9]+)", url)
    return match.group(1) if match else None


def _first_visible(page, selectors: list[str], timeout_ms: int = 2500):
    deadline = time.time() + timeout_ms / 1000
    last = None
    while time.time() < deadline:
        for selector in selectors:
            loc = page.locator(selector).first
            try:
                if loc.count() and loc.is_visible(timeout=250):
                    return loc
            except Exception as exc:  # noqa: BLE001 - best-effort selector probe
                last = exc
        time.sleep(0.1)
    if last:
        raise last
    return None


def _click_first(page, selectors: list[str], timeout_ms: int = 5000) -> bool:
    loc = _first_visible(page, selectors, timeout_ms)
    if not loc:
        return False
    loc.click()
    return True


def refresh(*, headless: bool, slow_mo_ms: int, wait_human_seconds: int) -> dict:
    kite = KiteConnect(api_key=API_KEY)
    login_url = kite.login_url()
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Opening Kite Connect login for api_key={_mask(API_KEY)} headless={headless}")
    with sync_playwright() as p:
        browser_type = p.chromium
        executable_path = None
        if headless and HEADLESS_SHELL.exists():
            executable_path = str(HEADLESS_SHELL)
        elif SYSTEM_CHROME.exists():
            executable_path = str(SYSTEM_CHROME)
        elif CHROME_FOR_TESTING.exists():
            executable_path = str(CHROME_FOR_TESTING)
        launch_kwargs = {"executable_path": executable_path} if executable_path else {}
        context = browser_type.launch_persistent_context(
            str(AUTH_DIR),
            headless=headless,
            slow_mo=slow_mo_ms,
            viewport={"width": 1365, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
            **launch_kwargs,
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(15000)

        token: str | None = None
        try:
            page.goto(login_url, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeoutError:
            pass

        token = _find_request_token(page.url)
        if not token:
            # Fill user id/password on whichever Kite login DOM is currently rendered.
            user_input = _first_visible(
                page,
                [
                    "input#userid",
                    "input[name='user_id']",
                    "input[name='userid']",
                    "input[type='text']",
                    "input[placeholder*='User']",
                    "input[placeholder*='user']",
                ],
                timeout_ms=10000,
            )
            pass_input = _first_visible(
                page,
                [
                    "input#password",
                    "input[name='password']",
                    "input[type='password']",
                    "input[placeholder*='Password']",
                    "input[placeholder*='password']",
                ],
                timeout_ms=10000,
            )
            if not user_input or not pass_input:
                raise RuntimeError(f"Could not locate Kite login fields; url={_safe_url(page.url)}")
            user_input.fill(USER_NAME)
            pass_input.fill(PASS)
            _click_first(
                page,
                [
                    "button[type='submit']",
                    "button:has-text('Login')",
                    "button:has-text('Continue')",
                    "input[type='submit']",
                ],
                timeout_ms=5000,
            )

            # If the broker shows CAPTCHA/challenge, allow a bounded human solve in
            # the visible browser. The script continues once the TOTP field or token appears.
            deadline = time.time() + wait_human_seconds
            while time.time() < deadline:
                token = _find_request_token(page.url)
                if token:
                    break
                otp_field = _first_visible(
                    page,
                    [
                        "input#userid",  # placeholder: keep loop resilient after redirects
                        "input[name='twofa_value']",
                        "input[name='totp']",
                        "input[type='number']",
                        "input[type='tel']",
                        "input[placeholder*='TOTP']",
                        "input[placeholder*='totp']",
                        "input[placeholder*='PIN']",
                        "input[placeholder*='pin']",
                    ],
                    timeout_ms=1200,
                )
                if otp_field:
                    # Avoid refilling the userid if a login error bounced us back.
                    try:
                        name = (otp_field.get_attribute("name") or "").lower()
                        placeholder = (otp_field.get_attribute("placeholder") or "").lower()
                        typ = (otp_field.get_attribute("type") or "").lower()
                    except Exception:
                        name = placeholder = typ = ""
                    if "user" not in name and "user" not in placeholder and typ != "text":
                        otp_field.fill(str(otp.get_totp(TOTP_KEY)).zfill(6))
                        _click_first(
                            page,
                            [
                                "button[type='submit']",
                                "button:has-text('Continue')",
                                "button:has-text('Submit')",
                                "button:has-text('Login')",
                                "input[type='submit']",
                            ],
                            timeout_ms=5000,
                        )
                        break
                time.sleep(1)

            # Visiting the connect login URL after web login usually performs the
            # app authorization redirect containing request_token.
            for _ in range(3):
                token = _find_request_token(page.url)
                if token:
                    break
                try:
                    page.goto(login_url, wait_until="domcontentloaded")
                    page.wait_for_load_state("networkidle", timeout=12000)
                except PlaywrightTimeoutError:
                    pass
                token = _find_request_token(page.url)
                if token:
                    break
                time.sleep(2)

            if not token:
                screenshot = ROOT / "reports" / f"kite_playwright_refresh_failed_{datetime.now():%Y%m%d_%H%M%S}.png"
                screenshot.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(screenshot), full_page=True)
                raise RuntimeError(f"No request_token found after browser login; url={_safe_url(page.url)}; screenshot={screenshot}")

        context.close()

    data = kite.generate_session(token, api_secret=API_SECRET)
    payload = {"access_token": data["access_token"], "date": datetime.now().strftime("%Y-%m-%d")}
    tmp = TOKEN_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=4), encoding="utf-8")
    tmp.replace(TOKEN_PATH)
    try:
        FLAG_PATH.unlink()
    except FileNotFoundError:
        pass
    print(f"Wrote fresh Kite access token for {payload['date']} to {TOKEN_PATH}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true", help="Run without a visible browser")
    parser.add_argument("--slow-mo-ms", type=int, default=80)
    parser.add_argument("--wait-human-seconds", type=int, default=300)
    args = parser.parse_args()
    refresh(headless=args.headless, slow_mo_ms=args.slow_mo_ms, wait_human_seconds=args.wait_human_seconds)


if __name__ == "__main__":
    main()
