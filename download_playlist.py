#!/usr/bin/env python3
"""Download every video in a public YouTube playlist through the knowledge-base pipeline.

For each video in the playlist, this script:
- Extracts the video id via yt-dlp
- Routes it through process_youtube_video() (video download, Whisper transcript,
  Claude summary, markdown file, digest tracking)
- Records progress to a state file so a crash mid-playlist can resume

All output — markdown files and downloaded video assets — is placed in a
playlist-specific subfolder of the vault:
  - "{Channel} - {Playlist}" when every sampled video comes from the same channel
  - "{Playlist}" when videos are from multiple channels (or channel can't be determined)

Usage:
    python download_playlist.py <playlist_url> [--channel <slack_channel_id>]
    python download_playlist.py --reset <playlist_url>   # clear progress and start over
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load .env before importing the bot module (Slack/OpenAI tokens)
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

import yt_dlp
import youtube_knowledge_bot as ykb
from youtube_knowledge_bot import process_youtube_video, sanitize_filename

# Default Slack channel (knowledger). Only used for the daily digest bucket —
# process_youtube_video() does not post per-video.
DEFAULT_CHANNEL = "C0A99TH4Y2V"

# Delay between videos to be polite to YouTube / avoid bot detection
SLEEP_BETWEEN = 3.0

# How many videos to sample when checking whether the playlist is single-channel
CHANNEL_SAMPLE_SIZE = 3

STATE_DIR = Path(__file__).parent / ".playlist_state"
STATE_DIR.mkdir(exist_ok=True)


def state_path(playlist_id: str) -> Path:
    return STATE_DIR / f"{playlist_id}.json"


def load_state(playlist_id: str) -> dict:
    p = state_path(playlist_id)
    if p.exists():
        return json.loads(p.read_text())
    return {"done": [], "failed": [], "folder": None}


def save_state(playlist_id: str, state: dict) -> None:
    state_path(playlist_id).write_text(json.dumps(state, indent=2))


def extract_playlist_id(playlist_url: str) -> str:
    import re
    m = re.search(r"[?&]list=([A-Za-z0-9_-]+)", playlist_url)
    if not m:
        raise SystemExit(f"Could not find playlist id in URL: {playlist_url}")
    return m.group(1)


def fetch_playlist(playlist_url: str) -> dict:
    """Return the raw flat-extract info dict (title, uploader, entries, ...)."""
    with yt_dlp.YoutubeDL({"extract_flat": True, "quiet": True}) as ydl:
        return ydl.extract_info(playlist_url, download=False)


def get_video_channel(video_id: str) -> str | None:
    """Fetch the channel/uploader name for one video. Returns None on failure."""
    try:
        with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}", download=False
            )
        return info.get("channel") or info.get("uploader")
    except Exception as e:
        print(f"  (couldn't fetch channel for {video_id}: {e})")
        return None


def pick_sample_indices(n: int, k: int) -> list[int]:
    """Return up to k evenly-spread indices from 0..n-1."""
    if n <= k:
        return list(range(n))
    step = n / k
    return sorted({int(i * step) for i in range(k)})


def determine_folder_name(playlist_title: str, entries: list[dict]) -> str:
    """Sample a few videos to see if they all share one channel.

    - Single channel  → "{Channel} - {Playlist}"
    - Multiple/unknown → "{Playlist}"
    """
    playlist_title = playlist_title.strip() or "Playlist"

    if not entries:
        return sanitize_filename(playlist_title)

    sample_ids = [entries[i]["id"] for i in pick_sample_indices(len(entries), CHANNEL_SAMPLE_SIZE)]
    print(f"Sampling {len(sample_ids)} video(s) to determine channel(s)...")
    channels: set[str] = set()
    for vid in sample_ids:
        ch = get_video_channel(vid)
        if ch:
            channels.add(ch.strip())
        # If we've already seen 2 different channels we can stop early
        if len(channels) > 1:
            break

    if len(channels) == 1:
        (only,) = channels
        folder = f"{only} - {playlist_title}"
    else:
        folder = playlist_title

    return sanitize_filename(folder)


def redirect_output_to(folder: Path) -> None:
    """Point the bot module's output dirs at the playlist folder.

    process_youtube_video() writes markdown to DOWNLOAD_DIR and videos to
    YOUTUBE_VIDEO_DIR; we redirect both under `folder` for the run.
    """
    folder.mkdir(parents=True, exist_ok=True)
    assets = folder / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    ykb.DOWNLOAD_DIR = folder
    ykb.YOUTUBE_VIDEO_DIR = assets
    ykb.ASSETS_DIR = assets


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("playlist_url")
    parser.add_argument("--channel", default=DEFAULT_CHANNEL,
                        help="Slack channel id for the daily-digest bucket")
    parser.add_argument("--reset", action="store_true",
                        help="Clear saved progress for this playlist and start over")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only process the first N remaining videos (0 = all)")
    args = parser.parse_args()

    if not os.environ.get("SLACK_BOT_TOKEN"):
        print("ERROR: SLACK_BOT_TOKEN not set (needed to import the bot module)")
        sys.exit(1)

    playlist_id = extract_playlist_id(args.playlist_url)

    if args.reset:
        p = state_path(playlist_id)
        if p.exists():
            p.unlink()
        print(f"Cleared progress for playlist {playlist_id}")

    state = load_state(playlist_id)
    done_ids = set(state["done"])

    print(f"Fetching playlist metadata ({playlist_id})...")
    info = fetch_playlist(args.playlist_url)
    playlist_title = info.get("title") or playlist_id
    entries = [
        {"id": e["id"], "title": e.get("title", "")}
        for e in (info.get("entries") or [])
        if e and e.get("id")
    ]
    total = len(entries)
    remaining = [e for e in entries if e["id"] not in done_ids]
    print(f"Playlist: {playlist_title!r}")
    print(f"  {total} videos; {len(done_ids)} already done; {len(remaining)} to process")

    # Decide folder name once per playlist and remember it (avoids re-sampling
    # on resume, and guarantees resumed runs land in the same folder).
    if state.get("folder"):
        folder_name = state["folder"]
        print(f"Resuming into existing folder: {folder_name}")
    else:
        folder_name = determine_folder_name(playlist_title, entries)
        state["folder"] = folder_name
        save_state(playlist_id, state)
        print(f"Output folder: {folder_name}")

    folder_path = Path(ykb.DOWNLOAD_DIR) / folder_name
    redirect_output_to(folder_path)
    print(f"  Writing markdown to: {folder_path}")
    print(f"  Writing assets to:   {folder_path / 'assets'}")

    if args.limit and args.limit > 0:
        remaining = remaining[:args.limit]
        print(f"--limit {args.limit} → processing {len(remaining)} this run")

    if not remaining:
        print("Nothing to do.")
        return

    started = datetime.now()

    for i, entry in enumerate(remaining, 1):
        vid = entry["id"]
        title = entry["title"] or vid
        watch_url = f"https://www.youtube.com/watch?v={vid}"

        print(f"\n[{i}/{len(remaining)}] {vid}  {title[:70]}")
        try:
            process_youtube_video(vid, args.channel, watch_url)
            state["done"].append(vid)
            state["failed"] = [f for f in state["failed"] if f["id"] != vid]
            save_state(playlist_id, state)
            print("    done")
        except KeyboardInterrupt:
            print("\nInterrupted — progress saved.")
            save_state(playlist_id, state)
            sys.exit(130)
        except Exception as e:
            print(f"    FAILED: {e}")
            traceback.print_exc()
            state["failed"] = [f for f in state["failed"] if f["id"] != vid]
            state["failed"].append({"id": vid, "title": title, "error": str(e)})
            save_state(playlist_id, state)

        time.sleep(SLEEP_BETWEEN)

    elapsed = datetime.now() - started
    print("\n" + "=" * 70)
    print(f"Playlist:  {playlist_id}  ({playlist_title})")
    print(f"Folder:    {folder_path}")
    print(f"Elapsed:   {elapsed}")
    print(f"Done:      {len(state['done'])}/{total}")
    print(f"Failed:    {len(state['failed'])}")
    print("=" * 70)
    if state["failed"]:
        print("\nFailed items (re-run the script to retry these):")
        for f in state["failed"]:
            print(f"  {f['id']}  {f['title'][:60]}")
            print(f"    -> {f['error']}")


if __name__ == "__main__":
    main()
