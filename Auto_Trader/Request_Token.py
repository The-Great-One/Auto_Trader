import requests
import onetimepass as otp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY, PASS, TOTP_KEY, USER_NAME
import logging

logger = logging.getLogger("Auto_Trade_Logger")

# Chrome-on-Linux headers matching the actual server OS.
# Previously we used Mac/Chrome headers here which caused daily CAPTCHA
# challenges because Kite's bot detection flags OS-mismatched user agents
# combined with non-browser Sec-Fetch hints.
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
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
}

# Headers used only for the login/TOTP POST calls — must look like a
# normal in-page form submission from the same origin, not an AJAX call.
_API_HEADERS = {
    "Referer": "https://kite.zerodha.com/",
    "Origin": "https://kite.zerodha.com",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
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


def get_request_token(credentials: dict | None = None) -> str:
    """Use provided credentials and return request token.
    Args:
        credentials: Login credentials for Kite
    Returns:
        Request token for the provided credentials
    """

    auth = credentials or {
        "api_key": API_KEY,
        "username": USER_NAME,
        "password": PASS,
        "totp_key": TOTP_KEY,
    }
    kite = KiteConnect(api_key=auth["api_key"])

    # Initialize session and fetch the login URL. A single request is
    # enough to set cookies; two rapid GETs look automated to Kite.
    session = requests.Session()
    session.headers.update(_BROWSER_HEADERS)
    session.get(kite.login_url(), timeout=10)

    # User login POST request
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

    # Check if login was successful.
    if login_response.status_code != 200 or "request_id" not in login_data:
        summary = _response_summary(login_response, login_payload_json)
        if _looks_like_captcha(login_payload_json):
            logger.error(
                "Kite login blocked by CAPTCHA challenge before TOTP: %s", summary
            )
        else:
            logger.error(
                "Kite login failed before TOTP; no request_id returned: %s", summary
            )
        # Raise instead of sys.exit so initialize_kite's retry loop can handle it
        raise RuntimeError(f"Kite login failed: {summary}")

    # TOTP POST request
    totp_payload = {
        "user_id": auth["username"],
        "request_id": login_data["request_id"],
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

    # Check if TOTP verification was successful
    if totp_response.status_code != 200:
        summary = _response_summary(totp_response, _json_or_empty(totp_response))
        logger.error("TOTP verification failed: %s", summary)
        raise RuntimeError(f"TOTP verification failed: {summary}")

    # Extract request token from the response by following the redirect
    # naturally (allow_redirects=True), which is normal browser behavior.
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
