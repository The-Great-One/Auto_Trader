#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

WATCH_UPDATES = Path.home() / '.openclaw' / 'telegram-user' / 'watch_channel_updates.jsonl'
REPORTS = Path('/Users/sahilgoel/Desktop/Stocks/reports')
INPUT_PATH = REPORTS / 'telegram_trade_audit_input.json'
OUTPUT_PATH = REPORTS / 'telegram_trade_audit_latest.json'
AUDIT_TOOL = Path('/Users/sahilgoel/.openclaw/workspace/tools/telegram_trade_audit.py')
PYTHON = Path('/Users/sahilgoel/Desktop/Stocks/venv/bin/python')


def build_input() -> None:
    chats: dict[str, list[dict]] = defaultdict(list)
    if not WATCH_UPDATES.exists():
        raise FileNotFoundError(f'missing {WATCH_UPDATES}')
    for line in WATCH_UPDATES.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        chat = obj.get('chat')
        if not chat:
            continue
        chats[chat].append({
            'date': obj.get('date') or obj.get('captured_at'),
            'text': obj.get('text') or obj.get('text_excerpt') or '',
            'message_id': obj.get('message_id'),
        })
    payload = {
        'source': str(WATCH_UPDATES),
        'chats': dict(chats),
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    INPUT_PATH.write_text(json.dumps(payload))


def main() -> int:
    build_input()
    proc = subprocess.run(
        [str(PYTHON), str(AUDIT_TOOL), '--input', str(INPUT_PATH), '--output', str(OUTPUT_PATH)],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr or proc.stdout)
        return proc.returncode
    print(str(OUTPUT_PATH))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
