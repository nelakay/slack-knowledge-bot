#!/usr/bin/env python3
"""Reprocess the large Business video by splitting audio with ffmpeg before Whisper."""

import sys
import os
import subprocess
import tempfile
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load env
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

import openai
from youtube_knowledge_bot import (
    get_video_metadata, download_youtube_video, download_audio,
    generate_summary_and_toc, assign_categories, create_markdown_file
)

openai_client = openai.OpenAI()

VIDEO_ID = "aFpYtCbYl2o"
SLACK_URL = "https://cocoworkshq.slack.com/archives/C0A99TH4Y2V/p1770459858361839"
CHANNEL = "C0A99TH4Y2V"

print("=== Reprocessing large video: The Business ===")

# Step 1: Get metadata
print("Fetching metadata...")
metadata = get_video_metadata(VIDEO_ID)
if not metadata:
    print("Could not fetch metadata, using defaults")
    metadata = {
        'title': 'Men, Marriage and Monogamy Is a Mess:  Winter Storm 2026',
        'channel': 'The Business',
        'duration': '5:54:32',
        'upload_date': 'Unknown',
        'description': '',
    }

print(f"Title: {metadata['title']}")
print(f"Channel: {metadata['channel']}")

# Step 2: Download video (likely already exists)
print("Checking video...")
video_downloaded, video_path = download_youtube_video(VIDEO_ID, metadata)

# Step 3: Download audio
print("Downloading audio...")
audio_path = download_audio(VIDEO_ID)
if not audio_path:
    print("ERROR: Could not download audio")
    sys.exit(1)

print(f"Audio downloaded: {audio_path}")
file_size_mb = os.path.getsize(audio_path) / (1024 * 1024)
print(f"Audio size: {file_size_mb:.1f} MB")

# Step 4: Split with ffmpeg into 10-minute MP3 chunks (small enough for Whisper)
print("Splitting audio into 10-minute chunks with ffmpeg...")
chunk_dir = tempfile.mkdtemp(prefix="business_chunks_")
chunk_pattern = os.path.join(chunk_dir, "chunk_%03d.mp3")

result = subprocess.run([
    'ffmpeg', '-i', audio_path,
    '-f', 'segment',
    '-segment_time', '600',  # 10 minutes
    '-c:a', 'libmp3lame',
    '-b:a', '64k',
    '-ac', '1',  # mono to reduce size
    chunk_pattern
], capture_output=True, text=True)

if result.returncode != 0:
    print(f"ffmpeg error: {result.stderr}")
    sys.exit(1)

chunks = sorted(glob.glob(os.path.join(chunk_dir, "chunk_*.mp3")))
print(f"Created {len(chunks)} chunks")

# Step 5: Transcribe each chunk with Whisper
print("Transcribing chunks...")
all_segments = []
cumulative_time = 0.0

for i, chunk_path in enumerate(chunks):
    chunk_size = os.path.getsize(chunk_path) / (1024 * 1024)
    print(f"  Chunk {i+1}/{len(chunks)} ({chunk_size:.1f} MB)...")

    try:
        with open(chunk_path, "rb") as f:
            chunk_transcript = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=f,
                response_format="verbose_json"
            )

        if hasattr(chunk_transcript, 'segments'):
            for seg in chunk_transcript.segments:
                adjusted = type('Segment', (), {
                    'start': getattr(seg, 'start', 0) + cumulative_time,
                    'end': getattr(seg, 'end', 0) + cumulative_time,
                    'text': getattr(seg, 'text', '')
                })()
                all_segments.append(adjusted)

        # Get actual chunk duration from ffprobe
        probe = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', chunk_path],
            capture_output=True, text=True
        )
        chunk_duration = float(probe.stdout.strip()) if probe.returncode == 0 else 600.0
        cumulative_time += chunk_duration
        print(f"    Done ({len(chunk_transcript.segments) if hasattr(chunk_transcript, 'segments') else 0} segments)")

    except Exception as e:
        print(f"    ERROR: {e}")
        cumulative_time += 600.0  # assume 10 min

    # Clean up chunk
    os.remove(chunk_path)

# Clean up
os.rmdir(chunk_dir)
try:
    os.remove(audio_path)
    os.rmdir(os.path.dirname(audio_path))
except:
    pass

# Build combined transcript
transcript = type('CombinedTranscript', (), {
    'segments': all_segments,
    'text': ' '.join([getattr(seg, 'text', '') for seg in all_segments])
})()

print(f"\nTotal segments: {len(all_segments)}")
print(f"Total transcript length: {len(transcript.text)} chars")

# Step 6: Generate summary (use first ~15000 chars to stay within GPT limits)
print("Generating summary and TOC...")
transcript_text = transcript.text
summary_and_toc = generate_summary_and_toc(transcript_text, metadata)

# Step 7: Assign categories
print("Assigning categories...")
categories = assign_categories(transcript_text, metadata)
print(f"Categories: {categories}")

# Step 8: Create markdown
print("Creating markdown file...")
filepath = create_markdown_file(VIDEO_ID, metadata, transcript, summary_and_toc, SLACK_URL, categories, video_path)
print(f"\nDone! File: {filepath}")
