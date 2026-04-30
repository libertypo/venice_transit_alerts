"""
Telegram → disruptions.json pipeline
=====================================
Fetches recent messages from the ACTV Telegram channel using the Bot API,
normalises them into the ServiceDisruption JSON schema used by the Venice
Transit app, and writes the result to disruptions.json.

Setup:
  1. Create a Telegram bot via @BotFather and obtain the token.
  2. Add the bot as a member of the @ACTVofficial channel
     (or forward messages to a private channel the bot can read).
  3. Store the token as a GitHub Actions secret: TELEGRAM_BOT_TOKEN
  4. Run via GitHub Actions (see fetch_alerts.yml).

The bot reads channel updates using getUpdates (long-polling offset stored in
offset.txt) so only new messages are processed on subsequent runs.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

# ── Configuration ────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
# Channel username (without @) or numeric chat id (negative for channels).
# The bot must be a member of this channel to receive forwarded updates via
# getUpdates, OR you must configure a webhook.  If using a private relay
# channel, set this to that channel's id.
CHANNEL_ID: str = os.environ.get("ACTV_CHANNEL_ID", "@ACTVofficial")

MAX_MESSAGE_AGE_HOURS: int = 48
OUTPUT_FILE: Path = Path("disruptions.json")
OFFSET_FILE: Path = Path("offset.txt")

# Response size cap: refuse bodies larger than 2 MB from the Telegram API.
MAX_RESPONSE_BYTES: int = 2 * 1024 * 1024

# ── Severity keyword mapping (Italian + English) ──────────────────────────────

CRITICAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bsospensione\b", re.IGNORECASE),
    re.compile(r"\bsospeso\b", re.IGNORECASE),
    re.compile(r"\binterrott[ao]\b", re.IGNORECASE),
    re.compile(r"\bfermo\b", re.IGNORECASE),
    re.compile(r"\bsuspended\b", re.IGNORECASE),
    re.compile(r"\binterrupted\b", re.IGNORECASE),
]

WARNING_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bsciopero\b", re.IGNORECASE),
    re.compile(r"\bacqua alta\b", re.IGNORECASE),
    re.compile(r"\britar[do|di]\b", re.IGNORECASE),
    re.compile(r"\bmodific[ah]\b", re.IGNORECASE),
    re.compile(r"\bvariazione\b", re.IGNORECASE),
    re.compile(r"\bstrike\b", re.IGNORECASE),
    re.compile(r"\bdelay\b", re.IGNORECASE),
    re.compile(r"\bmodified\b", re.IGNORECASE),
]

# Extracts line numbers / names from message text (e.g. "linea 1", "linea DM")
LINE_PATTERN: re.Pattern[str] = re.compile(
    r"\bline[a]?\s+([A-Z0-9]{1,4})\b", re.IGNORECASE
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _safe_get(url: str, timeout: int = 15) -> dict[str, Any] | None:
    """Performs a GET request and returns parsed JSON, or None on error.

    Enforces a response-size cap to prevent memory exhaustion.
    """
    try:
        with requests.get(url, timeout=timeout, stream=True) as resp:
            resp.raise_for_status()
            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_content(chunk_size=4096):
                total += len(chunk)
                if total > MAX_RESPONSE_BYTES:
                    print(
                        f"[ERROR] Response from {url!r} exceeded {MAX_RESPONSE_BYTES} bytes — skipping.",
                        file=sys.stderr,
                    )
                    return None
                chunks.append(chunk)
            body = b"".join(chunks)
            return json.loads(body)
    except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
        print(f"[ERROR] GET {url!r}: {exc}", file=sys.stderr)
        return None


def _detect_severity(text: str) -> str:
    for pattern in CRITICAL_PATTERNS:
        if pattern.search(text):
            return "critical"
    for pattern in WARNING_PATTERNS:
        if pattern.search(text):
            return "warning"
    return "info"


def _extract_lines(text: str) -> list[str]:
    matches = LINE_PATTERN.findall(text)
    return list({m.upper().strip() for m in matches})


def _stable_id(text: str, ts: int) -> str:
    digest = hashlib.sha1(f"{ts}:{text[:200]}".encode()).hexdigest()[:12]
    return f"tg-{digest}"


def _read_offset() -> int:
    try:
        return int(OFFSET_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return 0


def _write_offset(offset: int) -> None:
    OFFSET_FILE.write_text(str(offset))


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    # Validate token format (basic sanity check — does not expose the token)
    if ":" not in TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN) < 20:
        print("[ERROR] TELEGRAM_BOT_TOKEN appears malformed.", file=sys.stderr)
        sys.exit(1)

    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    offset = _read_offset()

    params: dict[str, Any] = {"timeout": 0, "limit": 100, "allowed_updates": ["channel_post", "message"]}
    if offset:
        params["offset"] = offset

    data = _safe_get(f"{base_url}/getUpdates?{_encode_params(params)}")
    if data is None or not data.get("ok"):
        print("[ERROR] getUpdates failed.", file=sys.stderr)
        sys.exit(1)

    updates: list[dict[str, Any]] = data.get("result", [])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_MESSAGE_AGE_HOURS)

    disruptions: list[dict[str, Any]] = []
    new_offset = offset

    for update in updates:
        update_id: int = update["update_id"]
        new_offset = max(new_offset, update_id + 1)

        post = update.get("channel_post") or update.get("message")
        if post is None:
            continue

        # Only process messages from the configured chat (channel or group)
        chat = post.get("chat", {})
        chat_username = chat.get("username", "")
        chat_id = str(chat.get("id", ""))
        target = CHANNEL_ID.lstrip("@").lower()
        if chat_username.lower() != target and chat_id != CHANNEL_ID:
            continue

        date_ts: int = post.get("date", 0)
        msg_dt = datetime.fromtimestamp(date_ts, tz=timezone.utc)
        if msg_dt < cutoff:
            continue

        text: str = post.get("text") or post.get("caption") or ""
        if not text.strip():
            continue

        severity = _detect_severity(text)
        lines = _extract_lines(text)
        title = text.split("\n")[0][:120].strip() or "Service notice"

        disruptions.append(
            {
                "id": _stable_id(text, date_ts),
                "title": title,
                "description": text[:800].strip(),
                "severity": severity,
                "lineAliases": lines,
                "affectedStops": [],
                "lastUpdated": msg_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "startAt": msg_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endAt": None,
                "source": "actv_telegram",
                "url": None,
            }
        )

    # Merge with existing disruptions that have not yet expired
    existing: list[dict[str, Any]] = []
    if OUTPUT_FILE.exists():
        try:
            payload = json.loads(OUTPUT_FILE.read_text())
            existing = payload.get("disruptions", [])
        except (json.JSONDecodeError, ValueError):
            existing = []

    # Replace existing entries with same id; keep others still within window
    new_ids = {d["id"] for d in disruptions}
    surviving = [d for d in existing if d["id"] not in new_ids and _is_recent(d, cutoff)]
    merged = surviving + disruptions
    # Sort by lastUpdated descending
    merged.sort(key=lambda d: d.get("lastUpdated", ""), reverse=True)

    output = {
        "version": 1,
        "source": "actv_telegram",
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "disruptions": merged,
    }

    OUTPUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2))
    _write_offset(new_offset)
    print(f"[OK] Wrote {len(merged)} disruption(s) to {OUTPUT_FILE}.")


def _is_recent(d: dict[str, Any], cutoff: datetime) -> bool:
    ts_str = d.get("lastUpdated", "")
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return ts >= cutoff
    except (ValueError, AttributeError):
        return False


def _encode_params(params: dict[str, Any]) -> str:
    from urllib.parse import urlencode
    return urlencode(params)


if __name__ == "__main__":
    main()
