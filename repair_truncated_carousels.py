#!/usr/bin/env python3
"""Repair carousels that were truncated by the old single-slide parser bug.

Reads truncated_carousels.txt (produced by scan_truncated_carousels.py) and
for each entry:
- Deletes the existing markdown so the bot can re-create it without
  appending a _N duplicate suffix
- Runs process_instagram_content to re-download all slides, re-write the
  markdown with all embeds, and post a fresh Slack reply

The original media files in assets/ are LEFT IN PLACE — they're harmless and
removing them risks affecting other notes that reference them.
"""

import os
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from youtube_knowledge_bot import process_instagram_content

CHANNEL = "C0A99TH4Y2V"
LIST_FILE = Path(__file__).parent / "truncated_carousels.txt"


def main():
    rows = []
    with LIST_FILE.open() as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 5:
                continue
            path, shortcode, vault_n, api_n, slack_url = parts[0], parts[1], parts[2], parts[3], parts[4]
            rows.append((path, shortcode, int(vault_n), int(api_n), slack_url))

    print(f"Repairing {len(rows)} truncated carousels.")
    print()

    successes = []
    failures = []

    for i, (md_path, shortcode, vault_n, api_n, slack_url) in enumerate(rows, 1):
        ig_url = f"https://www.instagram.com/p/{shortcode}/"
        print(f"\n[{i}/{len(rows)}] {shortcode}  (expected {api_n} slides, had {vault_n})")
        print(f"    md:    {md_path}")
        print(f"    slack: {slack_url}")

        # Delete the existing markdown so the new one keeps the original name
        try:
            p = Path(md_path)
            if p.exists():
                p.unlink()
                print(f"    deleted old md")
        except Exception as e:
            print(f"    WARNING: could not delete old md: {e}")

        # If slack_url is missing for some reason, fall back to a placeholder
        smu = slack_url or f"https://cocoworkshq.slack.com/archives/{CHANNEL}/"
        try:
            process_instagram_content(ig_url, CHANNEL, smu)
            successes.append(shortcode)
            print(f"    done")
        except Exception as e:
            failures.append((shortcode, str(e)))
            print(f"    FAILED: {e}")
            traceback.print_exc()

        time.sleep(2.5)  # pace API + Slack

    print("\n" + "=" * 70)
    print(f"SUCCESSES: {len(successes)}")
    print(f"FAILURES:  {len(failures)}")
    print("=" * 70)
    if failures:
        print("\nFailed items:")
        for sc, err in failures:
            print(f"  {sc} -> {err}")


if __name__ == "__main__":
    main()
