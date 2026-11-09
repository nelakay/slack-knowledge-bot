#!/usr/bin/env python3
"""Reprocess Instagram URLs found missing by audit_channel.py.

Runs each URL through the full process_instagram_content flow (download +
markdown + Slack reply). Benefits from retry logic and single-image fallback
in download_instagram_via_rapidapi.

Current run (2026-05-23): 7 recoverable items — 4 transient failures from
the last 5 days, plus the 3 single-image posts the parser now handles.
"""

import os
import sys
import time
import traceback

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

MISSING = [
    # New transient failures since 2026-05-18
    ("https://www.instagram.com/p/DYfI3uHDUeX/", "1779546529.827379"),
    ("https://www.instagram.com/p/DYclDojjt2L/", "1779541278.067279"),
    ("https://www.instagram.com/p/DUqk90kDHmX/", "1779541105.967979"),
    ("https://www.instagram.com/reel/DX2eGrNhpBN/", "1779208176.727199"),
    # Single-image posts now recoverable via display_url fallback
    ("https://www.instagram.com/p/DYAoAb-EX4h/", "1778948829.095329"),
    ("https://www.instagram.com/p/DX0clfxDnMF/", "1778948754.805239"),
    ("https://www.instagram.com/p/DYPlQADEl9c/", "1778948725.011849"),
]


def slack_link(ts):
    return f"https://cocoworkshq.slack.com/archives/{CHANNEL}/p{ts.replace('.', '')}"


def main():
    successes = []
    failures = []

    for i, (url, ts) in enumerate(MISSING, 1):
        smu = slack_link(ts)
        print(f"\n[{i}/{len(MISSING)}] {url}")
        print(f"    slack: {smu}")
        try:
            process_instagram_content(url, CHANNEL, smu)
            successes.append(url)
            print(f"    done")
        except Exception as e:
            failures.append((url, str(e)))
            print(f"    FAILED: {e}")
            traceback.print_exc()

        time.sleep(2)

    print("\n" + "=" * 70)
    print(f"SUCCESSES: {len(successes)}")
    print(f"FAILURES:  {len(failures)}")
    print("=" * 70)
    if failures:
        print("\nFailed items:")
        for url, err in failures:
            print(f"  {url}")
            print(f"    -> {err}")


if __name__ == "__main__":
    main()
