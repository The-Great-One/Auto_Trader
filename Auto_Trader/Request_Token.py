import os
import requests
import onetimepass as otp
import time as _time
import random
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY, PASS, TOTP_KEY, USER_NAME
import logging

logger = logging.getLogger("Auto_Trade_Logger")

# Minimal browser-like headers — proven to work against Cloudflare
# on this server. Adding Sec-Fetch-*, Upgrade-Insecure-Requests, or
# Accept-Encoding triggers Cloudflare's bot detection and returns
# empty-success (200 OK with {} body) instead of a request_id.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_API_HEADERS = {
    "Referer": "https://kite.zerodha.com/",
    "Origin": "https://kite.zerodha.com",
    "Content-Type": "application/x-www-form-urlencoded",
}


def _json_or_empty(response: requests.Response) -> dict:
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _response_summary(response: requests.Response, payload: dict) -> str:
    message = str(payload.get("message") or "").strip()
    data = payload.get("data")
    data_keys = sorted(data.keys()) if isinstance(data, dict) else []
    content_type = response.headers.get("content-type", "")
    return (
        f"status_code={response.status_code}, "
        f"api_status={payload.get('status')!r}, "
        f"message={message!r}, "
        f"data_keys={data_keys}, "
        f"content_type={content_type!r}"
    )


def _looks_like_captcha(payload: dict) -> bool:
    message = str(payload.get("message") or "").lower()
    data = payload.get("data")
    data_keys = set(data.keys()) if isinstance(data, dict) else set()
    return "captcha" in message or "captcha" in data_keys


def _do_kite_login(kite, auth):
    """Perform the full Kite login dance with retry on transient CF blocks."""

    _max_login_attempts = int(os.getenv("AT_KITE_LOGIN_MAX_ATTEMPTS", "3"))
    _max_captcha_attempts = int(os.getenv("AT_KITE_CAPTCHA_MAX_ATTEMPTS", "2"))
    _login_attempt = 0
    _captcha_attempt = 0

    while True:
        session = requests.Session()
        session.headers.update(_BROWSER_HEADERS)

        # Small random initial delay to avoid synchronized hits
        _time.sleep(random.uniform(1.0, 4.0))

        # Step 1: GET login page to set cookies
        session.get(kite.login_url(), timeout=10)

        # Step 2: POST credentials
        login_payload = {
            "user_id": auth["username"],
            "password": auth["password"],
        }
        login_response = session.post(
            "https://kite.zerodha.com/api/login",
            data=login_payload,
            headers=_API_HEADERS,
            timeout=10,
        )
        login_payload_json = _json_or_empty(login_response)
        login_data = login_payload_json.get("data", {}) or {}

        if login_response.status_code == 200 and "request_id" in login_data:
            break  # success

        summary = _response_summary(login_response, login_payload_json)
        _login_attempt += 1

        captcha_like = _looks_like_captcha(login_payload_json)
        if captcha_like:
            _captcha_attempt += 1
            logger.warning(
                "Kite login returned CAPTCHA-like challenge before TOTP "
                "(attempt %d/%d, captcha_attempt %d/%d): %s",
                _login_attempt,
                _max_login_attempts,
                _captcha_attempt,
                _max_captcha_attempts,
                summary,
            )
            if _captcha_attempt >= _max_captcha_attempts:
                raise RuntimeError(
                    "Kite login requires manual intervention: CAPTCHA challenge "
                    f"persisted before TOTP after {_captcha_attempt} attempts: {summary}"
                )

        if _login_attempt >= _max_login_attempts:
            logger.error(
                "Kite login failed before TOTP after %d attempts: %s",
                _max_login_attempts, summary,
            )
            raise RuntimeError(
                f"Kite login failed after {_max_login_attempts} attempts: {summary}"
            )

        # CAPTCHA-like failures need a cooler retry than empty/CF responses.
        base = 15 if captcha_like else 5
        backoff = min(180, base * (2 ** (_login_attempt - 1)))
        jitter = backoff * 0.2 * random.random()
        delay = backoff + jitter
        logger.warning(
            "Kite login retry scheduled (attempt %d/%d, captcha_like=%s): "
            "%s - retrying in %.1fs",
            _login_attempt, _max_login_attempts, captcha_like, summary, delay,
        )
        _time.sleep(delay)

    request_id = login_data["request_id"]

    # Step 3: POST TOTP
    totp_payload = {
        "user_id": auth["username"],
        "request_id": request_id,
        "twofa_value": otp.get_totp(auth["totp_key"]),
        "twofa_type": "totp",
        "skip_session": True,
    }
    totp_response = session.post(
        "https://kite.zerodha.com/api/twofa",
        data=totp_payload,
        headers=_API_HEADERS,
        timeout=10,
    )

    if totp_response.status_code != 200:
        summary = _response_summary(totp_response, _json_or_empty(totp_response))
        logger.error("TOTP verification failed: %s", summary)
        raise RuntimeError(f"TOTP verification failed: {summary}")

    # Step 4: Extract request token from redirect
    try:
        redirect_response = session.get(
            kite.login_url(), timeout=10, allow_redirects=True
        )
        query_params = parse_qs(urlparse(redirect_response.url).query)
    except Exception:
        query_params = {}

    tokens = query_params.get("request_token")
    if not tokens:
        logger.error("API not authorized. Unable to obtain request token.")
        raise RuntimeError(
            "Kite API not authorized — unable to extract request token."
        )

    return tokens[0]


def get_request_token(credentials: dict | None = None) -> str:
    auth = credentials or {
        "api_key": API_KEY,
        "username": USER_NAME,
        "password": PASS,
        "totp_key": TOTP_KEY,
    }
    kite = KiteConnect(api_key=auth["api_key"])
    return _do_kite_login(kite, auth)
