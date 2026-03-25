#!/usr/bin/env python3
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data
from Auto_Trader.portfolio_intelligence import build_report, format_markdown


def main():
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_session_data())

    report = build_report(kite)
    md = format_markdown(report)

    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "reports"))
    os.makedirs(out_dir, exist_ok=True)
    date = datetime.now().strftime("%Y-%m-%d")

    json_path = os.path.join(out_dir, f"portfolio_intel_{date}.json")
    md_path = os.path.join(out_dir, f"portfolio_intel_{date}.md")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    with open(md_path, "w") as f:
        f.write(md)

    print(md)


if __name__ == "__main__":
    main()
