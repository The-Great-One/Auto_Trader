import requests
import re
import onetimepass as otp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY, PASS, TOTP_KEY, USER_NAME
import sys
import logging
import time

logger = logging.getLogger("Auto_Trade_Logger")


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

    # Initialize session and get the login URL
    session = requests.Session()
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
    try:
        login_data = login_response.json().get("data", {})
    except ValueError:
        login_data = {}

    # Check if login was successful
    if login_response.status_code != 200 or "request_id" not in login_data:
        logger.error("Login failed, please check your credentials.")
        time.sleep(60)
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
