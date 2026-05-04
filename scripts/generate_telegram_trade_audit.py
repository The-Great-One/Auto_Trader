#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

WATCH_UPDATES = Path.home() / '.openclaw' / 'telegram-user' / 'watch_channel_updates.jsonl'
EXPORT_PATH = Path(os.getenv('AT_TELEGRAM_EXPORT', str(Path.home() / '.openclaw' / 'telegram-user' / 'exports' / 'telegram_export_latest.json')))
REPORTS = Path('/Users/sahilgoel/Desktop/Stocks/reports')
INPUT_PATH = REPORTS / 'telegram_trade_audit_input.json'
OUTPUT_PATH = REPORTS / 'telegram_trade_audit_latest.json'
AUDIT_TOOL = Path('/Users/sahilgoel/.openclaw/workspace/tools/telegram_trade_audit.py')
PYTHON = Path('/Users/sahilgoel/Desktop/Stocks/venv/bin/python')

CANONICAL_CHATS = {
    'financewithsunil': '@FinanceWithSunil',
    'shortterm01': '@Shortterm01',
    'darkhorseofstockmarket': '@DarkHorseOfStockMarket',
    'milind4profits': '@Milind4Profits',
}


def canonical_chat(chat: str | None) -> str | None:
    if not chat:
        return None
    key = chat.strip().lstrip('@').lower()
    return CANONICAL_CHATS.get(key, chat if chat.startswith('@') else f'@{chat}')


def add_message(chats: dict[str, list[dict[str, Any]]], seen: set[tuple[str, int]], chat: str | None, msg: dict[str, Any]) -> None:
    canon = canonical_chat(chat)
    if not canon:
        return
    message_id = msg.get('message_id')
    try:
        mid = int(message_id)
    except Exception:
        mid = -1
    key = (canon.lower(), mid)
    if mid >= 0 and key in seen:
        return
    if mid >= 0:
        seen.add(key)
    text = msg.get('text') or msg.get('text_excerpt') or ''
    chats[canon].append({
        'date': msg.get('date') or msg.get('captured_at'),
        'text': text,
        'message_id': message_id,
    })


def load_watch_updates(chats: dict[str, list[dict[str, Any]]], seen: set[tuple[str, int]]) -> int:
    if not WATCH_UPDATES.exists():
        return 0
    count = 0
    for line in WATCH_UPDATES.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        add_message(chats, seen, obj.get('chat'), obj)
        count += 1
    return count


def load_export(chats: dict[str, list[dict[str, Any]]], seen: set[tuple[str, int]]) -> int:
    if not EXPORT_PATH.exists():
        return 0
    data = json.loads(EXPORT_PATH.read_text())
    count = 0
    for chat, messages in data.get('chats', {}).items():
        if not isinstance(messages, list):
            continue
        for msg in messages:
            add_message(chats, seen, chat, msg)
            count += 1
    return count


def build_input() -> None:
    chats: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, int]] = set()
    export_count = load_export(chats, seen)
    watch_count = load_watch_updates(chats, seen)
    payload = {
        'source': {
            'export': str(EXPORT_PATH),
            'watch_updates': str(WATCH_UPDATES),
            'export_rows_seen': export_count,
            'watch_rows_seen': watch_count,
            'deduped_messages': sum(len(v) for v in chats.values()),
        },
        'chats': {k: sorted(v, key=lambda m: (m.get('date') or '', int(m.get('message_id') or 0))) for k, v in chats.items()},
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
