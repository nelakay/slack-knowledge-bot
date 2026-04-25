#!/usr/bin/env python3
"""Backfill processing_history.jsonl from past daily-digest messages in Slack.

The bot only began persisting processing history when /show-failures was added.
This script reads daily-digest messages the bot already posted to a Slack channel
and reconstructs failure records so /show-failures can answer "what failed in the
last N days" for periods predating the persistence change.

Usage:
    python3 backfill_failures.py --channel C0A99TH4Y2V --days 21
    python3 backfill_failures.py --channel C0A99TH4Y2V --days 30 --dry-run
    python3 backfill_failures.py --channel C0A99TH4Y2V --days 21 --include-successes

Requires SLACK_BOT_TOKEN in the environment (same token the bot uses).
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

HISTORY_FILE = Path(__file__).parent / "processing_history.jsonl"

DIGEST_HEADER = "*Daily Knowledge Base Digest*"
FAILED_HEADER_RE = re.compile(r"^\d+ item\(s\) failed:$")
FAILED_LINE_RE = re.compile(r"^•\s+\[([^\]]+)\]\s+(.+?):\s+(.*)$")
PLATFORM_HEADER_RE = re.compile(r"^\*([A-Za-z]+)\s+\(\d+\):\*$")
SUCCESS_TITLE_RE = re.compile(r"^•\s+\*(.+?)\*$")

PLATFORM_LETTER_TO_NAME = {"y": "youtube", "i": "instagram", "l": "linkedin"}


def parse_retry_button(message):
    """Pull the retry-button JSON value out of a digest message's blocks.

    Returns a list of {p, u, s, t?} dicts (the same compact shape the bot uses)
    or [] if no retry button is present.
    """
    for block in message.get("blocks", []) or []:
        if block.get("type") != "actions":
            continue
        for el in block.get("elements", []) or []:
            if el.get("action_id") == "retry_failed_items":
                value = el.get("value") or ""
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return []
    return []


def parse_digest_text(text):
    """Parse a digest message's text into (failures, successes).

    Each failure: {platform, title, error}
    Each success: {platform, title, detail}
    URL is not in the text — the caller fills it in from the retry button.
    """
    if not text or DIGEST_HEADER not in text:
        return [], []

    lines = text.split("\n")
    failures = []
    successes = []

    in_failed = False
    current_platform = None  # for success sections

    for raw in lines:
        line = raw.strip()
        if not line:
            in_failed = False  # blank line ends the failed section
            continue

        # Failed section header
        if FAILED_HEADER_RE.match(line):
            in_failed = True
            current_platform = None
            continue

        if in_failed:
            m = FAILED_LINE_RE.match(line)
            if m:
                platform_raw, title, error = m.group(1), m.group(2), m.group(3)
                failures.append({
                    "platform": platform_raw.strip().lower(),
                    "title": title.strip(),
                    "error": error.strip(),
                })
                continue
            if not line.startswith("•"):
                in_failed = False
            continue

        # Success section header (e.g. "*YouTube (3):*")
        m = PLATFORM_HEADER_RE.match(line)
        if m:
            current_platform = m.group(1).lower()
            continue

        if current_platform:
            sm = SUCCESS_TITLE_RE.match(line)
            if sm:
                successes.append({
                    "platform": current_platform,
                    "title": sm.group(1).strip(),
                    "detail": "",
                })

    return failures, successes


def attach_urls_from_retry_button(failures, retry_items):
    """Pair failures (from text) with retry-button entries (which carry URLs).

    The bot generates both lists in the same order, so we zip by index. If a
    LinkedIn entry was truncated out of the retry button due to size, the
    corresponding failure won't get a URL.
    """
    for idx, f in enumerate(failures):
        if idx < len(retry_items):
            r = retry_items[idx]
            f["url"] = r.get("u", "")
            f["slack_message_url"] = r.get("s", "")
            # If the retry's platform letter disagrees, prefer the retry button (it's structured)
            mapped = PLATFORM_LETTER_TO_NAME.get((r.get("p") or "").lower())
            if mapped:
                f["platform"] = mapped
        else:
            f["url"] = ""
            f["slack_message_url"] = ""
    return failures


def fetch_digest_messages(channel, oldest_ts, token):
    """Yield bot messages whose text contains the digest header, paginated."""
    from slack_sdk import WebClient
    client = WebClient(token=token)
    cursor = None
    while True:
        kwargs = {"channel": channel, "oldest": str(oldest_ts), "limit": 200}
        if cursor:
            kwargs["cursor"] = cursor
        resp = client.conversations_history(**kwargs)
        for msg in resp.get("messages", []):
            text = msg.get("text", "") or ""
            if DIGEST_HEADER in text:
                yield msg
        if not resp.get("has_more"):
            break
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break


def existing_keys():
    """Set of (date, platform, title) tuples already in HISTORY_FILE for dedup."""
    if not HISTORY_FILE.exists():
        return set()
    keys = set()
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            ts = row.get("timestamp", "")[:10]
            keys.add((ts, row.get("platform", ""), row.get("title", "")))
    return keys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", required=True, help="Slack channel ID where digests are posted (e.g. C0A99TH4Y2V)")
    parser.add_argument("--days", type=int, default=21, help="How many days back to scan (default 21)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be written without modifying the file")
    parser.add_argument("--include-successes", action="store_true",
                        help="Also backfill successes (URLs unavailable, so entries are title-only)")
    args = parser.parse_args()

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("SLACK_BOT_TOKEN not set in environment", file=sys.stderr)
        sys.exit(1)

    oldest = (datetime.now() - timedelta(days=args.days)).timestamp()

    try:
        from slack_sdk.errors import SlackApiError
        digests = list(fetch_digest_messages(args.channel, oldest, token))
    except SlackApiError as e:
        print(f"Slack API error: {e.response.get('error', e)}", file=sys.stderr)
        sys.exit(2)

    print(f"Found {len(digests)} digest message(s) in channel {args.channel} for the last {args.days} day(s)")

    seen = existing_keys()
    new_entries = []

    for msg in digests:
        msg_ts = float(msg.get("ts", "0"))
        msg_dt = datetime.fromtimestamp(msg_ts)
        timestamp = msg_dt.isoformat()
        date_key = timestamp[:10]

        text = msg.get("text", "") or ""
        failures, successes = parse_digest_text(text)
        retry_items = parse_retry_button(msg)
        failures = attach_urls_from_retry_button(failures, retry_items)

        for f in failures:
            key = (date_key, f["platform"], f["title"])
            if key in seen:
                continue
            seen.add(key)
            new_entries.append({
                "timestamp": timestamp,
                "platform": f["platform"],
                "title": f["title"],
                "url": f.get("url", ""),
                "channel": "",
                "success": False,
                "error": f.get("error", ""),
                "filepath": None,
                "slack_message_url": f.get("slack_message_url", ""),
                "categories": [],
                "backfilled": True,
            })

        if args.include_successes:
            for s in successes:
                key = (date_key, s["platform"], s["title"])
                if key in seen:
                    continue
                seen.add(key)
                new_entries.append({
                    "timestamp": timestamp,
                    "platform": s["platform"],
                    "title": s["title"],
                    "url": "",
                    "channel": "",
                    "success": True,
                    "error": None,
                    "filepath": None,
                    "slack_message_url": "",
                    "categories": [],
                    "backfilled": True,
                })

    failed_count = sum(1 for e in new_entries if not e["success"])
    success_count = sum(1 for e in new_entries if e["success"])
    print(f"Reconstructed {len(new_entries)} new entries ({failed_count} failures, {success_count} successes) "
          f"after dedup against existing {HISTORY_FILE.name}")

    if args.dry_run:
        print("\n--- DRY RUN: would append the following lines: ---")
        for e in new_entries:
            print(json.dumps(e, ensure_ascii=False))
        return

    if not new_entries:
        print("Nothing to write.")
        return

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        for e in new_entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"Appended {len(new_entries)} entries to {HISTORY_FILE}")


if __name__ == "__main__":
    main()
