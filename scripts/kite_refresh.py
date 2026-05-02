#!/usr/bin/env python3
"""Refresh Kite access token using TOTP authentication."""
import requests
import json
import re
import pyotp
import datetime
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect

secrets = {}
with open("Auto_Trader/my_secrets.py") as f:
    exec(f.read(), secrets)

API_KEY = secrets["API_KEY"]
API_SECRET = secrets["API_SECRET"]
TOTP_KEY = secrets["TOTP_KEY"]
USER_NAME = secrets["USER_NAME"]
PASS = secrets["PASS"]

kite = KiteConnect(api_key=API_KEY)

# Use Chrome-like headers to minimize CAPTCHA risk
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

session = requests.Session()
session.headers.update(BROWSER_HEADERS)

# Step 1: Visit homepage
r1 = session.get("https://kite.zerodha.com/", timeout=10)
print(f"Homepage: {r1.status_code}, cookies: {list(session.cookies.keys())}")

# Step 2: Login URL
r2 = session.get(kite.login_url(), timeout=10)
print(f"Login page: {r2.status_code}, url: {r2.url[:100]}")

# Step 3: Login
login_headers = {
    "Referer": "https://kite.zerodha.com/",
    "Origin": "https://kite.zerodha.com",
    "Content-Type": "application/x-www-form-urlencoded",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "X-Requested-With": "XMLHttpRequest",
}
login_payload = {"user_id": USER_NAME, "password": PASS}
login_response = session.post(
    "https://kite.zerodha.com/api/login",
    data=login_payload,
    headers=login_headers,
    timeout=10,
)
resp = login_response.json()
print(f"Login: status={resp.get('status')}, message={resp.get('message', 'ok')[:100]}")

if "request_id" in resp.get("data", {}):
    request_id = resp["data"]["request_id"]
    print(f"request_id: {request_id}")

    # Step 4: TOTP
    totp_code = str(pyotp.TOTP(TOTP_KEY).now())
    print(f"TOTP: {totp_code}")

    totp_payload = {
        "user_id": USER_NAME,
        "request_id": request_id,
        "twofa_value": totp_code,
        "twofa_type": "totp",
        "skip_session": True,
    }
    totp_response = session.post(
        "https://kite.zerodha.com/api/twofa",
        data=totp_payload,
        headers=login_headers,
        timeout=10,
    )
    resp2 = totp_response.json()
    print(f"2FA: status={resp2.get('status')}, message={resp2.get('message', 'ok')[:100]}")

    if totp_response.status_code == 200:
        # Try getting enctoken from response
        enctoken = resp2.get("data", {}).get("enctoken")
        if enctoken:
            print(f"enctoken found: {enctoken[:20]}...")
            # Save enctoken for auto_trade.py
            enc_data = {"enctoken": enctoken, "date": datetime.datetime.now().strftime("%Y-%m-%d")}
            with open("intermediary_files/enctoken.json", "w") as f:
                json.dump(enc_data, f, indent=4)
            print("enctoken saved!")

        # Try redirect for request_token
        try:
            redirect_response = session.get(kite.login_url(), timeout=10, allow_redirects=False)
            location = redirect_response.headers.get("Location", "")
            print(f"Redirect: {redirect_response.status_code}, Location: {location[:200]}")
            parse_result = urlparse(location)
            query_params = parse_qs(parse_result.query)
        except Exception as e:
            print(f"Redirect error (trying to extract token): {e}")
            pattern = r"request_token=[A-Za-z0-9]+"
            match = re.search(pattern, str(e))
            if match:
                query_params = parse_qs(match.group())
            else:
                query_params = {}

        tokens = query_params.get("request_token")
        if tokens:
            request_token = tokens[0]
            print(f"request_token: {request_token[:15]}...")

            # Step 6: Generate session
            data = kite.generate_session(request_token, api_secret=API_SECRET)
            print(f"access_token: {data['access_token'][:15]}...")

            # Save
            session_data = {
                "access_token": data["access_token"],
                "date": datetime.datetime.now().strftime("%Y-%m-%d"),
            }
            with open("intermediary_files/access_token.json", "w") as f:
                json.dump(session_data, f, indent=4)
            print("Token saved!")
        else:
            # Try following the redirect chain
            redirect2 = session.get(kite.login_url(), timeout=10, allow_redirects=True)
            print(f"Final URL: {redirect2.url[:200]}")
            parse_result = urlparse(redirect2.url)
            query_params = parse_qs(parse_result.query)
            tokens = query_params.get("request_token")
            if tokens:
                request_token = tokens[0]
                data = kite.generate_session(request_token, api_secret=API_SECRET)
                print(f"access_token: {data['access_token'][:15]}...")
                session_data = {
                    "access_token": data["access_token"],
                    "date": datetime.datetime.now().strftime("%Y-%m-%d"),
                }
                with open("intermediary_files/access_token.json", "w") as f:
                    json.dump(session_data, f, indent=4)
                print("Token saved!")
            else:
                print(f"No request_token found. Final URL: {redirect2.url}")
    else:
        print(f"2FA failed: {json.dumps(resp2)[:500]}")
else:
    print(f"Login failed. Full response: {json.dumps(resp)[:500]}")