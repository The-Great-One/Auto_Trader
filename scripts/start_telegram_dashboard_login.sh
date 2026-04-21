#!/bin/bash
nohup /bin/bash "$(dirname "$0")/start_telegram_dashboard.sh" >/dev/null 2>&1 &
