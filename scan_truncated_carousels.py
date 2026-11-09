#!/usr/bin/env python3
"""Scan vault Instagram .md files, identify carousels that were truncated by
the old parser bug (only first slide was downloaded).

Writes the list of truncated posts to truncated_carousels.txt so the repair
script can act on it without re-querying the API.
"""

import os
import re
import sys
import time
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

import requests

VAULT = Path("/Volumes/Knowledger/vault/Knowledger/instagram")
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"]
RAPIDAPI_IG_HOST = "social-media-video-downloader.p.rapidapi.com"

IG_URL_RE = re.compile(r'instagram_url:\s*"([^"]+)"')
SLACK_URL_RE = re.compile(r'slack_message_url:\s*"([^"]+)"')
SHORTCODE_RE = re.compile(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)")
EMBED_RE = re.compile(r"!\[\[")


def fetch_contents_len(shortcode, max_attempts=3):
    api_url = f"https://{RAPIDAPI_IG_HOST}/instagram/v3/media/post/details"
    params = {"shortcode": shortcode, "renderableFormats": "720p,highres"}
    headers = {
        "x-rapidapi-host": RAPIDAPI_IG_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
    }
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(api_url, headers=headers, params=params, timeout=30)
            if r.status_code == 429:
                wait = 5 * attempt
                print(f"    429 rate-limited; sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code in (401, 403, 404):
                return None, f"http {r.status_code}"
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                return None, f"api err: {data['error']}"
            return len(data.get("contents", []) or []), None
        except Exception as e:
            if attempt < max_attempts:
                time.sleep(2 ** attempt)
                continue
            return None, str(e)
    return None, "exhausted retries"


def main():
    md_files = sorted(p for p in VAULT.glob("*.md") if not p.name.startswith("._"))
    print(f"Scanning {len(md_files)} markdown files...")

    candidates = []  # (path, shortcode, vault_embed_count, slack_url)
    for md in md_files:
        try:
            text = md.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        ig_m = IG_URL_RE.search(text)
        if not ig_m:
            continue
        ig_url = ig_m.group(1)
        sc_m = SHORTCODE_RE.search(ig_url)
        if not sc_m:
            continue
        shortcode = sc_m.group(1)

        # Skip reels — they're always single-video, no carousel possible
        if "/reel" in ig_url:
            continue

        embed_count = len(EMBED_RE.findall(text))
        # Only investigate posts that currently show ≤1 media — anything with
        # more is either already a multi-slide or a video post (which has 1 embed).
        if embed_count > 1:
            continue

        slack_m = SLACK_URL_RE.search(text)
        slack_url = slack_m.group(1) if slack_m else ""
        candidates.append((md, shortcode, embed_count, slack_url))

    print(f"Found {len(candidates)} candidate /p/ posts with <=1 embed.")
    print()

    truncated = []
    for i, (path, shortcode, vault_n, slack_url) in enumerate(candidates, 1):
        print(f"[{i}/{len(candidates)}] {shortcode}  (vault embeds={vault_n})")
        api_n, err = fetch_contents_len(shortcode)
        if err:
            print(f"    skip: {err}")
        elif api_n is None:
            print(f"    skip: no data")
        elif api_n > 1 and api_n > vault_n:
            print(f"    TRUNCATED: api={api_n} slides, vault has {vault_n}")
            truncated.append((str(path), shortcode, vault_n, api_n, slack_url))
        else:
            print(f"    ok: api={api_n}")
        time.sleep(1.2)  # gentle pacing to avoid 429

    out = Path(__file__).parent / "truncated_carousels.txt"
    with out.open("w") as f:
        f.write("# path\tshortcode\tvault_n\tapi_n\tslack_url\n")
        for row in truncated:
            f.write("\t".join(str(x) for x in row) + "\n")

    print()
    print("=" * 70)
    print(f"TRUNCATED CAROUSELS: {len(truncated)}")
    print(f"Wrote: {out}")
    print("=" * 70)


if __name__ == "__main__":
    main()
