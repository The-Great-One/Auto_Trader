import requests
import re
import onetimepass as otp
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from my_secrets import *

def get_request_token(credentials={
    "api_key": API_KEY,
    "username": USER_NAME,
    "password": PASS,
    "totp_key": TOTP_KEY
}) -> str:
    """Use provided credentials and return request token.
    Args:
        credentials: Login credentials for Kite
    Returns:
        Request token for the provided credentials
    """

    kite = KiteConnect(api_key=credentials["api_key"])

    # Initialize session and get the login URL
    session = requests.Session()
    response = session.get(kite.login_url())

    # User login POST request
    login_payload = {
        "user_id": credentials["username"],
        "password": credentials["password"],
    }
    login_response = session.post("https://kite.zerodha.com/api/login", data=login_payload)

    # Check if login was successful
    if login_response.status_code != 200 or "data" not in login_response.json():
        raise Exception("Login failed, please check your credentials.")

    # TOTP POST request
    totp_payload = {
        "user_id": credentials["username"],
        "request_id": login_response.json()["data"]["request_id"],
        "twofa_value": otp.get_totp(credentials["totp_key"]),
        "twofa_type": "totp",
        "skip_session": True,
    }
    totp_response = session.post("https://kite.zerodha.com/api/twofa", data=totp_payload)

    # Check if TOTP verification was successful
    if totp_response.status_code != 200:
        raise Exception("TOTP verification failed, please check your TOTP key.")

    # Extract request token from the redirect URL
    try:
        redirect_response = session.get(kite.login_url())
        parse_result = urlparse(redirect_response.url)
        query_params = parse_qs(parse_result.query)
    except Exception as e:
        pattern = r"request_token=[A-Za-z0-9]+"
        match = re.search(pattern, str(e))
        if match:
            query_params = parse_qs(match.group())
        else:
            raise Exception("Failed to extract request token.")

    request_token = query_params["request_token"][0]
    return request_token