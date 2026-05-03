import requests
import re
import onetimepass as otp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY, PASS, TOTP_KEY, USER_NAME
import sys
import logging

logger = logging.getLogger("Auto_Trade_Logger")

# Firefox-like headers to avoid CAPTCHA/bot detection on Kite login.
_FIREFOX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


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

    # Initialize session with Firefox headers and get the login URL
    session = requests.Session()
    session.headers.update(_FIREFOX_HEADERS)
    session.get(kite.login_url(), timeout=10)

    # User login POST request
    login_payload = {
        "user_id": auth["username"],
        "password": auth["password"],
    }
    login_response = session.post(
        "https://kite.zerodha.com/api/login",
        data=login_payload,
        timeout=10,
    )
    login_payload_json = {}
    try:
        login_payload_json = login_response.json()
        login_data = login_payload_json.get("data", {}) or {}
    except ValueError:
        login_data = {}

    # Check if login was successful. Kite sometimes returns a CAPTCHA challenge
    # before TOTP, which is not a credential or TOTP failure and needs manual
    # request-token recovery rather than repeated automated retries.
    if login_response.status_code != 200 or "request_id" not in login_data:
        error_message = str(login_payload_json.get("message") or "").strip()
        login_data_keys = set(login_data.keys()) if isinstance(login_data, dict) else set()
        if "captcha" in error_message.lower() or "captcha" in login_data_keys:
            logger.error("Kite login blocked by CAPTCHA challenge before TOTP.")
        elif error_message:
            logger.error("Kite login failed before TOTP: %s", error_message)
        else:
            logger.error("Kite login failed before TOTP; no request_id returned.")
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
        timeout=10,
    )

    # Check if TOTP verification was successful
    if totp_response.status_code != 200:
        logger.error("TOTP verification failed, please check your TOTP key.")
        sys.exit(1)

    # Extract request token from the redirect URL
    query_params = {}
    try:
        redirect_response = session.get(kite.login_url(), timeout=10)
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
        f"API not Authorized. Open this Link in your browser: https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}"
    )
    sys.exit(1)
