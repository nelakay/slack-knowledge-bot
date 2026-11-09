#!/usr/bin/env python3
"""Retry the 2 Instagram items that failed in the previous reprocess run."""

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

from youtube_knowledge_bot import process_instagram_content, download_instagram_content, extract_instagram_id

CHANNEL = "C0A99TH4Y2V"

ITEMS = [
    ("https://www.instagram.com/p/DVHbhyJjkGW/", "1774114357.055289"),
    ("https://www.instagram.com/p/DTRCE69jKx9/", "1774035168.759069"),
]


def main():
    for url, ts in ITEMS:
        smu = f"https://cocoworkshq.slack.com/archives/{CHANNEL}/p{ts.replace('.', '')}"
        post_id = extract_instagram_id(url)
        print(f"\n=== {url} (id={post_id}) ===")

        # First, try the download directly to see the actual error
        print("Direct download attempt:")
        try:
            success, media_path, metadata, error = download_instagram_content(url, post_id)
            print(f"  success={success}")
            print(f"  media_path={media_path}")
            print(f"  metadata={metadata!r}")
            print(f"  error={error}")
        except Exception as e:
            print(f"  exception: {e}")
            traceback.print_exc()

        # Now run through the full handler
        print("Full handler:")
        try:
            process_instagram_content(url, CHANNEL, smu)
            print("  done")
        except Exception as e:
            print(f"  exception: {e}")
            traceback.print_exc()

        time.sleep(3)


if __name__ == "__main__":
    main()
