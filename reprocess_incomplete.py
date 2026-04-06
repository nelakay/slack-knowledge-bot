#!/usr/bin/env python3
"""Reprocess 47 incomplete YouTube markdown files that have missing transcripts/summaries."""

import sys
import os
import time
import glob
import re

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load env manually from .env file
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

from youtube_knowledge_bot import process_youtube_video

# Scan vault for incomplete files
vault = "/Volumes/Knowledger/vault/Knowledger"
files = glob.glob(os.path.join(vault, "*.md"))

incomplete = []
for f in files:
    if "/._" in f:
        continue
    try:
        content = open(f).read()
        if any(s in content for s in ["Transcript not available", "Content not available", "Summary could not be generated"]):
            m = re.search(r'youtube_url:\s*"?https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]+)', content)
            sm = re.search(r'slack_message_url:\s*"?([^"\s]+)', content)
            if m:
                incomplete.append((m.group(1), sm.group(1) if sm else "", os.path.basename(f)))
    except Exception as e:
        print(f"Error reading {f}: {e}")

print(f"\n{'='*60}")
print(f"Found {len(incomplete)} incomplete files to reprocess")
print(f"{'='*60}\n")

success = 0
failed = 0
for i, (vid_id, slack_url, filename) in enumerate(incomplete, 1):
    print(f"\n[{i}/{len(incomplete)}] Processing: {filename}")
    print(f"  Video ID: {vid_id}")
    try:
        process_youtube_video(vid_id, "C0A99TH4Y2V", slack_url)
        success += 1
        print(f"  ✓ SUCCESS")
    except Exception as e:
        failed += 1
        print(f"  ✗ FAILED: {e}")

    # Small delay between videos to avoid rate limits
    if i < len(incomplete):
        time.sleep(3)

print(f"\n{'='*60}")
print(f"DONE: {success} succeeded, {failed} failed out of {len(incomplete)}")
print(f"{'='*60}")
