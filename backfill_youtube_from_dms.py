#!/usr/bin/env python3
"""
Backfill YouTube videos from Slack DM / channel history.

Uses a Slack USER token (xoxp-) via search.messages so it can read every
conversation you have access to — including 1:1 DMs where bots cannot be
added. Processes nothing by default; you review the discovered list first,
then run again with --process to feed each unique, non-duplicate video
through the existing pipeline (markdown to DOWNLOAD_DIR, no Slack posts).

Flow (recommended):

  1) Dry-run — discover and list URLs, no OpenAI cost:
        python3 backfill_youtube_from_dms.py --dry-run

     Review /path/to/Knowledger/.backfill/discovered_urls.jsonl
     (and the printed count / cost estimate).

  2) Process everything that survived dedup against the vault:
        python3 backfill_youtube_from_dms.py --process

     Interrupted? Just re-run; already-processed videos are skipped.

Env vars:
  SLACK_USER_TOKEN  xoxp-...  user token with 'search:read' scope
  OPENAI_API_KEY             required for --process (same key the bot uses)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from youtube_knowledge_bot import (  # noqa: E402
    YOUTUBE_PATTERNS,
    extract_video_id,
    check_youtube_already_processed,
    process_video_bulk,
    get_slack_message_url,
    DOWNLOAD_DIR,
)

BACKFILL_DIR = DOWNLOAD_DIR / ".backfill"
BACKFILL_DIR.mkdir(parents=True, exist_ok=True)

DISCOVERED_FILE = BACKFILL_DIR / "discovered_urls.jsonl"
PROCESSED_FILE = BACKFILL_DIR / "processed.jsonl"
FAILED_FILE = BACKFILL_DIR / "failed.jsonl"

SEARCH_URL = "https://slack.com/api/search.messages"
COST_PER_VIDEO = 0.17  # rough, per README

EXTENDED_YOUTUBE_PATTERNS = YOUTUBE_PATTERNS + [
    r'(?:https?://)?m\.youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
    r'(?:https?://)?music\.youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
]


def slack_search(token, query, page=1, count=100):
    """search.messages with backoff on 429 / ratelimited."""
    while True:
        r = requests.get(
            SEARCH_URL,
            params={
                "query": query,
                "count": count,
                "page": page,
                "sort": "timestamp",
                "sort_dir": "desc",
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "5"))
            print(f"  rate-limited, sleeping {retry}s")
            time.sleep(retry)
            continue
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            err = data.get("error", "unknown")
            if err == "ratelimited":
                time.sleep(5)
                continue
            raise RuntimeError(f"Slack API error: {err}")
        return data


def extract_video_ids(match):
    """Every YouTube video_id found in a search match's text + attachments + blocks."""
    ids = set()

    def scan(s):
        if not s:
            return
        for pattern in EXTENDED_YOUTUBE_PATTERNS:
            for m in re.finditer(pattern, s):
                ids.add(m.group(1))

    scan(match.get("text", ""))
    scan(match.get("permalink", ""))

    for att in match.get("attachments", []) or []:
        for f in ("title_link", "original_url", "from_url", "text", "fallback"):
            scan(att.get(f, ""))

    for block in match.get("blocks", []) or []:
        if block.get("type") == "rich_text":
            for el in block.get("elements", []) or []:
                for item in el.get("elements", []) or []:
                    if item.get("type") == "link":
                        scan(item.get("url", ""))
    return ids


def year_windows(start_year, end_year):
    """Query windows to stay under search's 10k-match-per-query cap."""
    if end_year is None:
        end_year = datetime.now().year
    for y in range(start_year, end_year + 1):
        yield y, f"(youtube.com OR youtu.be) after:{y - 1}-12-31 before:{y + 1}-01-01"


def load_seen_video_ids():
    seen = set()
    if DISCOVERED_FILE.exists():
        with open(DISCOVERED_FILE) as f:
            for line in f:
                try:
                    seen.add(json.loads(line)["video_id"])
                except Exception:
                    pass
    return seen


def discover(token, start_year, end_year):
    seen = load_seen_video_ids()
    if seen:
        print(f"Resuming — {len(seen)} video IDs already in {DISCOVERED_FILE.name}")

    total_matches = 0
    with open(DISCOVERED_FILE, "a") as out:
        for year, query in year_windows(start_year, end_year):
            print(f"\n=== {year} ===")
            page = 1
            first_page = slack_search(token, query, page=1, count=100)
            paging = first_page.get("messages", {}).get("paging", {}) or {}
            pages = paging.get("pages", 1)
            total = first_page.get("messages", {}).get("total", 0)
            print(f"  {total} matches across {pages} page(s)")

            if total >= 9500:
                print("  WARNING: approaching Slack's 10k-per-query cap. "
                      "Consider narrowing the year with --start-year / --end-year or "
                      "editing year_windows() to split by quarter.")

            data = first_page
            while True:
                for match in data.get("messages", {}).get("matches", []) or []:
                    total_matches += 1
                    for vid in extract_video_ids(match):
                        if vid in seen:
                            continue
                        seen.add(vid)
                        ch = match.get("channel") or {}
                        rec = {
                            "video_id": vid,
                            "channel_id": ch.get("id", ""),
                            "channel_name": ch.get("name", ""),
                            "is_im": bool(ch.get("is_im")),
                            "is_mpim": bool(ch.get("is_mpim")),
                            "ts": match.get("ts", ""),
                            "permalink": match.get("permalink", ""),
                            "username": match.get("username", ""),
                            "text_preview": (match.get("text", "") or "")[:300],
                            "year": year,
                        }
                        out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        out.flush()

                if page >= pages or page >= 100:
                    break
                page += 1
                time.sleep(2)  # polite pacing for search tier-2
                data = slack_search(token, query, page=page, count=100)

    print(f"\nDiscovery done.")
    print(f"  Unique videos: {len(seen)}")
    print(f"  Total search matches scanned: {total_matches}")
    print(f"  Review: {DISCOVERED_FILE}")


def load_processed_ids():
    done = set()
    if PROCESSED_FILE.exists():
        with open(PROCESSED_FILE) as f:
            for line in f:
                try:
                    done.add(json.loads(line)["video_id"])
                except Exception:
                    pass
    return done


def process_all(yes):
    if not DISCOVERED_FILE.exists():
        print("No discovered_urls.jsonl yet — run --dry-run first.")
        sys.exit(1)

    entries = []
    with open(DISCOVERED_FILE) as f:
        for line in f:
            try:
                entries.append(json.loads(line))
            except Exception:
                pass

    done = load_processed_ids()

    to_do = []
    vault_dups = 0
    for entry in entries:
        vid = entry["video_id"]
        if vid in done:
            continue
        dup, path, title = check_youtube_already_processed(vid)
        if dup:
            vault_dups += 1
            with open(PROCESSED_FILE, "a") as pf:
                pf.write(json.dumps({"video_id": vid, "status": "skipped-vault-dup", "path": path}) + "\n")
            continue
        to_do.append(entry)

    print(f"Discovered: {len(entries)}")
    print(f"Already in vault: {vault_dups}")
    print(f"Already processed this run: {len(done)}")
    print(f"To process now: {len(to_do)}")
    print(f"Rough OpenAI cost: ~${len(to_do) * COST_PER_VIDEO:.2f} (at ${COST_PER_VIDEO:.2f}/video)")

    if not to_do:
        print("Nothing to do.")
        return

    if not yes:
        ans = input("Proceed? [y/N] ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return

    with open(PROCESSED_FILE, "a") as pf, open(FAILED_FILE, "a") as ff:
        for i, entry in enumerate(to_do, 1):
            vid = entry["video_id"]
            slack_url = entry.get("permalink") or get_slack_message_url(
                entry.get("channel_id", ""), entry.get("ts", "")
            )
            print(f"[{i}/{len(to_do)}] {vid} ({entry.get('channel_name') or entry.get('channel_id')})")
            try:
                ok, filepath, err = process_video_bulk(
                    video_id=vid,
                    channel=entry.get("channel_id", ""),
                    ts=entry.get("ts", ""),
                    client=None,
                    slack_message_url=slack_url,
                    slack_message_text=entry.get("text_preview", ""),
                )
            except Exception as e:
                ok, filepath, err = False, None, f"exception: {e}"

            if ok:
                pf.write(json.dumps({"video_id": vid, "status": "ok", "path": filepath}) + "\n")
                pf.flush()
                print(f"  -> {filepath}")
            else:
                ff.write(json.dumps({"video_id": vid, "error": err, "entry": entry}) + "\n")
                ff.flush()
                print(f"  ! {err}")

    print(f"\nDone.")
    print(f"  Results: {PROCESSED_FILE}")
    print(f"  Failures: {FAILED_FILE}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="discover + list only (default)")
    g.add_argument("--process", action="store_true", help="process the discovered list")
    ap.add_argument("--start-year", type=int, default=2021)
    ap.add_argument("--end-year", type=int, default=None, help="default: current year")
    ap.add_argument("-y", "--yes", action="store_true", help="skip the cost-confirmation prompt on --process")
    args = ap.parse_args()

    if args.process:
        if not os.environ.get("OPENAI_API_KEY"):
            print("OPENAI_API_KEY not set — needed for --process")
            sys.exit(1)
        process_all(args.yes)
        return

    token = os.environ.get("SLACK_USER_TOKEN")
    if not token:
        print("SLACK_USER_TOKEN not set. Need a xoxp- USER token (not xoxb-).")
        print("See README section 'Backfilling DM history' for how to get one.")
        sys.exit(1)
    if not token.startswith("xoxp-"):
        print(f"Warning: token prefix is {token[:5]}..., expected xoxp-. Continuing anyway.")
    discover(token, args.start_year, args.end_year)


if __name__ == "__main__":
    main()
