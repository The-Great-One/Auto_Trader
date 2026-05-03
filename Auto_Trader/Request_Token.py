import re
import requests
import onetimepass as otp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY, PASS, TOTP_KEY, USER_NAME
import sys
import logging

logger = logging.getLogger("Auto_Trade_Logger")

# Browser-like headers to avoid Kite returning a non-login/API response before
# the TOTP step.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

_API_HEADERS = {
    "Referer": "https://kite.zerodha.com/",
    "Origin": "https://kite.zerodha.com",
    "Content-Type": "application/x-www-form-urlencoded",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "X-Requested-With": "XMLHttpRequest",
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

    # Initialize session with browser headers and warm the Kite cookies before
    # posting credentials.
    session = requests.Session()
    session.headers.update(_BROWSER_HEADERS)
    session.get("https://kite.zerodha.com/", timeout=10)
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

    # Check if login was successful. Kite sometimes returns a CAPTCHA challenge
    # before TOTP, which is not a credential or TOTP failure and needs manual
    # request-token recovery rather than repeated automated retries.
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
        sys.exit(1)

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
        logger.error(
            "TOTP verification failed: %s",
            _response_summary(totp_response, _json_or_empty(totp_response)),
        )
        sys.exit(1)

    # Extract request token from the redirect URL
    query_params = {}
    try:
        redirect_response = session.get(
            kite.login_url(), timeout=10, allow_redirects=False
        )
        redirect_url = (
            redirect_response.headers.get("Location") or redirect_response.url
        )
        parse_result = urlparse(redirect_url)
        query_params = parse_qs(parse_result.query)

        if "request_token" not in query_params:
            redirect_response = session.get(
                kite.login_url(), timeout=10, allow_redirects=True
            )
            parse_result = urlparse(redirect_response.url)
            query_params = parse_qs(parse_result.query)
    except Exception as e:
        pattern = r"request_token=[A-Za-z0-9]+"
        match = re.search(pattern, str(e))
        if match:
            query_params = parse_qs(match.group())
        else:
            logger.error("Failed to extract request token.")

    tokens = query_params.get("request_token")
    if tokens:
        return tokens[0]

    logger.error("API not authorized. Unable to obtain request token.")
    print(
        "API not Authorized. Open this Link in your browser: "
        f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}"
    )
    sys.exit(1)
