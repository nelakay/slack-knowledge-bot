#!/usr/bin/env python3
"""
YouTube Slack Bot - Markdown Knowledge Base Version
Creates rich markdown files with video metadata, table of contents, summary, and Whisper transcripts
Supports forwarded messages - links back to original message
"""

import os
import re
import tempfile
import threading
from pathlib import Path
from datetime import datetime, timedelta
import time
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from pytubefix import YouTube
from openai import OpenAI
import yt_dlp

# Initialize Slack app
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Configuration
DOWNLOAD_DIR = Path("/Users/nelagueye/Downloads/Knowledger")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Your Slack workspace URL (update this with your workspace)
SLACK_WORKSPACE = "cocoworkshq"

# Daily digest configuration
DIGEST_HOUR = 22  # 10 PM
DIGEST_CHANNEL = None  # Will be set to the last channel where a video was processed

# Track processed videos for daily digest
daily_digest_videos = []
digest_lock = threading.Lock()

# YouTube URL patterns
YOUTUBE_PATTERNS = [
    r'(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
    r'(?:https?://)?(?:www\.)?youtu\.be/([a-zA-Z0-9_-]{11})',
    r'(?:https?://)?(?:www\.)?youtube\.com/shorts/([a-zA-Z0-9_-]{11})'
]

# Instagram URL patterns
INSTAGRAM_PATTERNS = [
    r'(?:https?://)?(?:www\.)?instagram\.com/p/([a-zA-Z0-9_-]+)',      # Posts
    r'(?:https?://)?(?:www\.)?instagram\.com/reel/([a-zA-Z0-9_-]+)',   # Reels
    r'(?:https?://)?(?:www\.)?instagram\.com/tv/([a-zA-Z0-9_-]+)',     # IGTV
]

# Instagram media save directory
INSTAGRAM_DIR = DOWNLOAD_DIR / "instagram"
INSTAGRAM_DIR.mkdir(parents=True, exist_ok=True)

# YouTube video save directory
YOUTUBE_VIDEO_DIR = DOWNLOAD_DIR / "assets"
YOUTUBE_VIDEO_DIR.mkdir(parents=True, exist_ok=True)

# Allowed categories for tagging (no spaces)
ALLOWED_CATEGORIES = [
    "faith",           # religious/spiritual content
    "engineering",     # software, mechanical, electrical engineering
    "tutorials",       # how-to, guides, walkthroughs
    "news",            # current events, journalism
    "technology",      # tech reviews, gadgets, AI
    "isms",            # red pill, manosphere, ideological content
    "business",        # entrepreneurship, startups, finance
    "productivity",    # self-improvement, habits, systems
    "health",          # fitness, nutrition, mental health
    "science",         # research, experiments, explanations
    "entertainment",   # movies, games, pop culture
    "education",       # lectures, courses, academic
    "interviews",      # podcasts, conversations, Q&A
    "reviews",         # product reviews, critiques
    "creative",        # art, music, design
    "career",          # job advice, professional development
    "undefined",       # for manual categorization when unsure
]

def extract_video_id(text):
    """Extract YouTube video ID from text"""
    for pattern in YOUTUBE_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def extract_instagram_id(text):
    """Extract Instagram post/reel ID from text"""
    for pattern in INSTAGRAM_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def get_instagram_url(text):
    """Extract full Instagram URL from text"""
    for pattern in INSTAGRAM_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    return None


def download_instagram_content(url, post_id):
    """Download Instagram content using yt-dlp. Returns (success, filepath, metadata, error)"""
    try:
        temp_dir = tempfile.mkdtemp()
        output_template = os.path.join(temp_dir, '%(title)s.%(ext)s')

        ydl_opts = {
            'outtmpl': output_template,
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract info first
            info = ydl.extract_info(url, download=True)

            if not info:
                return False, None, None, "Could not extract info"

            # Get metadata
            metadata = {
                'title': info.get('title') or info.get('description', '')[:50] or f'Instagram_{post_id}',
                'uploader': info.get('uploader') or info.get('channel') or 'Unknown',
                'duration': info.get('duration') or 0,
                'description': info.get('description') or '',
                'timestamp': info.get('timestamp'),
                'view_count': info.get('view_count'),
                'like_count': info.get('like_count'),
                'media_type': 'video' if info.get('ext') in ['mp4', 'webm', 'mov'] else 'image',
            }

            # Format duration
            if metadata['duration']:
                mins = int(metadata['duration']) // 60
                secs = int(metadata['duration']) % 60
                metadata['duration_string'] = f"{mins}:{secs:02d}"
            else:
                metadata['duration_string'] = "N/A"

            # Find downloaded file
            downloaded_file = None
            for f in os.listdir(temp_dir):
                downloaded_file = os.path.join(temp_dir, f)
                break

            if not downloaded_file or not os.path.exists(downloaded_file):
                return False, None, metadata, "Download file not found"

            # Move to Instagram folder with sanitized name
            ext = os.path.splitext(downloaded_file)[1]
            safe_title = sanitize_filename(metadata['title'][:50])
            safe_uploader = sanitize_filename(metadata['uploader'])
            final_filename = f"{safe_uploader} - {safe_title}{ext}"
            final_path = INSTAGRAM_DIR / final_filename

            # Handle duplicate names
            counter = 1
            while final_path.exists():
                final_filename = f"{safe_uploader} - {safe_title}_{counter}{ext}"
                final_path = INSTAGRAM_DIR / final_filename
                counter += 1

            import shutil
            shutil.move(downloaded_file, final_path)

            # Clean up temp dir
            try:
                os.rmdir(temp_dir)
            except:
                pass

            return True, str(final_path), metadata, None

    except Exception as e:
        print(f"Error downloading Instagram content: {e}")
        return False, None, None, str(e)


def create_instagram_markdown(post_id, url, metadata, media_path, slack_message_url):
    """Create a simple markdown file for Instagram content"""
    try:
        safe_title = sanitize_filename(metadata['title'][:50])
        safe_uploader = sanitize_filename(metadata['uploader'])
        filename = f"{safe_uploader} - {safe_title}.md"
        filepath = INSTAGRAM_DIR / filename

        # Handle duplicate names
        counter = 1
        while filepath.exists():
            filename = f"{safe_uploader} - {safe_title}_{counter}.md"
            filepath = INSTAGRAM_DIR / filename
            counter += 1

        # Get relative path to media file
        media_filename = os.path.basename(media_path) if media_path else "N/A"

        markdown_content = f"""---
platform: "instagram"
uploader: "{metadata['uploader']}"
title: "{metadata['title']}"
instagram_url: "{url}"
slack_message_url: "{slack_message_url}"
duration: "{metadata['duration_string']}"
media_type: "{metadata['media_type']}"
tags: [instagram]
---

# {metadata['title']}

**Uploader:** {metadata['uploader']}
**Duration:** {metadata['duration_string']}
**Type:** {metadata['media_type']}
**Instagram:** [{url}]({url})
**Slack Reference:** [View in Slack]({slack_message_url})

---

## Description

{metadata['description'] or 'No description available.'}

---

## Media File

`{media_filename}`
"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        return str(filepath)

    except Exception as e:
        print(f"Error creating Instagram markdown: {e}")
        return None


def get_video_metadata(video_id):
    """Fetch video metadata using pytubefix"""
    try:
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")

        # Format duration
        duration_seconds = yt.length or 0
        minutes = duration_seconds // 60
        seconds = duration_seconds % 60
        duration_string = f"{minutes}:{seconds:02d}"

        return {
            'title': yt.title or 'Unknown Title',
            'channel': yt.author or 'Unknown Channel',
            'duration': duration_seconds,
            'duration_string': duration_string,
            'view_count': yt.views or 0,
            'tags': yt.keywords or [],
            'description': yt.description or '',
            'upload_date': yt.publish_date.strftime('%Y%m%d') if yt.publish_date else 'Unknown',
        }
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return None

def sanitize_filename(filename):
    """Remove invalid characters from filename"""
    return re.sub(r'[<>:"/\\|?*]', '', filename).strip()

def verify_audio_file(file_path):
    """Verify that an audio file is valid and complete"""
    try:
        # Check file exists and has content
        if not os.path.exists(file_path):
            return False, "File does not exist"

        file_size = os.path.getsize(file_path)
        if file_size < 1000:  # Less than 1KB is suspicious
            return False, f"File too small: {file_size} bytes"

        # Try to probe with ffmpeg to check file integrity
        import subprocess
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            return False, f"ffprobe failed: {result.stderr}"

        # Check if we got a valid duration
        try:
            duration = float(result.stdout.strip())
            if duration <= 0:
                return False, "Invalid duration"
            return True, f"Valid file, duration: {duration:.1f}s"
        except (ValueError, TypeError):
            return False, "Could not parse duration"

    except subprocess.TimeoutExpired:
        return False, "ffprobe timed out"
    except FileNotFoundError:
        # ffprobe not available, skip verification
        return True, "Skipping verification (ffprobe not found)"
    except Exception as e:
        return False, str(e)

def download_audio(video_id):
    """Download audio from YouTube video for transcription using pytubefix"""
    try:
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}")

        # Try different stream types in order of preference
        audio_stream = None

        # First try: audio-only streams sorted by bitrate
        audio_streams = yt.streams.filter(only_audio=True).order_by('abr').desc()

        # Try progressive streams as backup (these are more reliable but larger)
        progressive_streams = yt.streams.filter(progressive=True).order_by('resolution').desc()

        # Combine streams, preferring audio-only first
        all_streams = list(audio_streams) + list(progressive_streams)

        if not all_streams:
            print("No suitable streams found")
            return None

        temp_dir = tempfile.mkdtemp()

        # Try each stream until one works
        for i, stream in enumerate(all_streams[:3]):  # Try up to 3 streams
            try:
                print(f"Trying stream {i+1}: {stream.mime_type}, {getattr(stream, 'abr', 'N/A')} bitrate")
                output_file = stream.download(output_path=temp_dir)

                # Verify the downloaded file
                is_valid, message = verify_audio_file(output_file)
                if is_valid:
                    print(f"Downloaded audio to: {output_file}")
                    print(f"Verification: {message}")
                    return output_file
                else:
                    print(f"Download verification failed: {message}")
                    # Clean up bad file and try next stream
                    try:
                        os.remove(output_file)
                    except:
                        pass

            except Exception as stream_error:
                print(f"Stream {i+1} failed: {stream_error}")
                continue

        print("All stream download attempts failed")
        return None

    except Exception as e:
        print(f"Error downloading audio: {e}")
        return None

def download_youtube_video(video_id, metadata):
    """Download YouTube video file using yt-dlp, capped at 1080p.

    Returns (success, filepath) tuple.
    """
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"

        channel_name = sanitize_filename(metadata['channel'])
        video_title = sanitize_filename(metadata['title'])
        base_filename = f"{channel_name} - {video_title}"

        ydl_opts = {
            'format': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best',
            'merge_output_format': 'mp4',
            'outtmpl': os.path.join(str(YOUTUBE_VIDEO_DIR), f'{base_filename}.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
        }

        print(f"Downloading video (1080p max): {metadata['title']}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        # Find the downloaded file
        final_path = YOUTUBE_VIDEO_DIR / f"{base_filename}.mp4"
        if final_path.exists() and final_path.stat().st_size > 1024:
            print(f"Video downloaded: {final_path}")
            return True, str(final_path)

        # Fallback: check for any file matching the base name
        for f in YOUTUBE_VIDEO_DIR.iterdir():
            if f.stem == base_filename and f.suffix in ['.mp4', '.mkv', '.webm']:
                print(f"Video downloaded: {f}")
                return True, str(f)

        print("Video download completed but file not found")
        return False, None

    except Exception as e:
        print(f"Error downloading video: {e}")
        return False, None


def transcribe_with_whisper(audio_path):
    """Transcribe audio using OpenAI Whisper API with chunking for large files"""
    try:
        print(f"Transcribing audio file: {audio_path}")

        # Check file size - Whisper API has 25MB limit
        file_size = os.path.getsize(audio_path)
        print(f"Audio file size: {file_size / (1024*1024):.2f} MB")

        # If file is under 20MB, transcribe directly (using 20MB threshold for safety margin
        # since multipart form encoding adds overhead and can push files over the 25MB API limit)
        if file_size <= 20 * 1024 * 1024:
            with open(audio_path, "rb") as audio_file:
                transcript = openai_client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    response_format="verbose_json"
                )
            return transcript

        # For large files, use pydub to split into chunks
        print("File larger than 20MB, splitting into chunks...")
        try:
            from pydub import AudioSegment
        except ImportError:
            print("pydub not installed, attempting direct transcription...")
            with open(audio_path, "rb") as audio_file:
                transcript = openai_client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    response_format="verbose_json"
                )
            return transcript

        # Detect actual audio format using ffprobe (file extensions can be wrong)
        import subprocess
        actual_format = None
        try:
            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries', 'format=format_name',
                 '-of', 'default=noprint_wrappers=1:nokey=1', audio_path],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                format_names = result.stdout.strip().split(',')
                # Map ffprobe format names to pydub format names
                format_map = {
                    'webm': 'webm', 'matroska': 'webm',
                    'mp4': 'mp4', 'mov': 'mp4', 'm4a': 'mp4',
                    'mp3': 'mp3', 'ogg': 'ogg', 'wav': 'wav'
                }
                for fmt in format_names:
                    if fmt in format_map:
                        actual_format = format_map[fmt]
                        break
                print(f"Detected audio format: {format_names[0]} -> using {actual_format or 'auto'}")
        except Exception as e:
            print(f"Could not detect format: {e}")

        # Load audio file with detected or auto format
        try:
            if actual_format:
                audio = AudioSegment.from_file(audio_path, format=actual_format)
            else:
                # Let pydub/ffmpeg auto-detect
                audio = AudioSegment.from_file(audio_path)
        except Exception as load_error:
            print(f"Failed to load with format {actual_format}, trying auto-detect...")
            audio = AudioSegment.from_file(audio_path)

        # Split into 10-minute chunks (600000 ms)
        chunk_length_ms = 10 * 60 * 1000
        chunks = [audio[i:i + chunk_length_ms] for i in range(0, len(audio), chunk_length_ms)]

        print(f"Split into {len(chunks)} chunks")

        all_segments = []
        cumulative_time = 0.0

        for i, chunk in enumerate(chunks):
            print(f"Transcribing chunk {i+1}/{len(chunks)}...")

            # Export chunk to temp file
            chunk_path = audio_path + f".chunk{i}.mp3"
            chunk.export(chunk_path, format="mp3", bitrate="64k")

            try:
                with open(chunk_path, "rb") as chunk_file:
                    chunk_transcript = openai_client.audio.transcriptions.create(
                        model="whisper-1",
                        file=chunk_file,
                        response_format="verbose_json"
                    )

                # Adjust timestamps and add to all_segments
                if hasattr(chunk_transcript, 'segments'):
                    for seg in chunk_transcript.segments:
                        # Create a dict-like object with adjusted timestamps
                        adjusted_seg = type('Segment', (), {
                            'start': getattr(seg, 'start', 0) + cumulative_time,
                            'end': getattr(seg, 'end', 0) + cumulative_time,
                            'text': getattr(seg, 'text', '')
                        })()
                        all_segments.append(adjusted_seg)

                # Update cumulative time (chunk duration in seconds)
                cumulative_time += len(chunk) / 1000.0

            finally:
                # Clean up chunk file
                try:
                    os.remove(chunk_path)
                except:
                    pass

        # Create a combined transcript object
        combined_transcript = type('CombinedTranscript', (), {
            'segments': all_segments,
            'text': ' '.join([getattr(seg, 'text', '') for seg in all_segments])
        })()

        print(f"Transcription complete: {len(all_segments)} segments")
        return combined_transcript

    except Exception as e:
        print(f"Error transcribing with Whisper: {e}")
        return None

def assign_categories(transcript_text, metadata):
    """Use GPT to auto-assign categories from the allowed list"""
    try:
        categories_str = ", ".join(ALLOWED_CATEGORIES)
        prompt = f"""Based on the following video title and transcript, assign 1-3 categories from this EXACT list:
{categories_str}

Video Title: "{metadata['title']}"
Channel: {metadata['channel']}

Transcript excerpt:
{transcript_text[:5000]}

Rules:
- ONLY use categories from the list above
- Return 1-3 categories that best fit the content
- If truly unsure, include "undefined"
- Return ONLY the category names, comma-separated, nothing else

Example response: technology, tutorials
"""

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a categorization assistant. Return only category names from the provided list, comma-separated."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50
        )

        # Parse and validate categories
        raw_categories = response.choices[0].message.content.strip().lower()
        assigned = [cat.strip() for cat in raw_categories.split(",")]

        # Filter to only allowed categories
        valid_categories = [cat for cat in assigned if cat in ALLOWED_CATEGORIES]

        # If none are valid, use undefined
        if not valid_categories:
            valid_categories = ["undefined"]

        return valid_categories
    except Exception as e:
        print(f"Error assigning categories: {e}")
        return ["undefined"]

def generate_summary_and_toc(transcript_text, metadata):
    """Use GPT to generate a summary and table of contents from the transcript"""
    try:
        prompt = f"""Based on the following transcript from a YouTube video titled "{metadata['title']}" by {metadata['channel']}, please provide:

1. A TABLE OF CONTENTS with timestamps (if discernible from the flow of topics) listing the main sections/topics covered
2. A comprehensive SUMMARY of the video (2-3 paragraphs)

Transcript:
{transcript_text[:15000]}

Please format your response as:

## Table of Contents
- [Topic 1]
- [Topic 2]
...

## Summary
[Your summary here]
"""

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that creates concise summaries and tables of contents for video transcripts."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2000
        )

        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating summary: {e}")
        return None

def format_transcript(transcript):
    """Format the Whisper transcript with timestamps every 60 seconds"""
    if not transcript or not hasattr(transcript, 'segments'):
        return "Transcript not available."

    formatted_lines = []
    last_timestamp_minute = -1  # Track when we last added a timestamp

    for segment in transcript.segments:
        # Access as object attributes, not dict
        start_time = getattr(segment, 'start', 0)
        text = getattr(segment, 'text', '').strip()

        if not text:
            continue

        # Calculate which 60-second interval this segment falls into
        current_minute = int(start_time // 60)

        # Only add timestamp when entering a new 60-second interval
        if current_minute > last_timestamp_minute:
            # Convert seconds to MM:SS format
            minutes = int(start_time // 60)
            seconds = int(start_time % 60)
            timestamp = f"[{minutes:02d}:{seconds:02d}]"
            formatted_lines.append(f"{timestamp} {text}")
            last_timestamp_minute = current_minute
        else:
            # No timestamp, just the text
            formatted_lines.append(text)

    return "\n\n".join(formatted_lines)

def create_markdown_file(video_id, metadata, transcript, summary_and_toc, slack_message_url, categories, video_path=None):
    """Create the markdown file with all content"""
    try:
        channel_name = sanitize_filename(metadata['channel'])
        video_title = sanitize_filename(metadata['title'])
        filename = f"{channel_name} - {video_title}.md"
        filepath = DOWNLOAD_DIR / filename

        youtube_url = f"https://www.youtube.com/watch?v={video_id}"

        # Video embed: use local file if downloaded, otherwise fall back to iframe
        if video_path:
            try:
                relative_video = Path(video_path).relative_to(DOWNLOAD_DIR)
                video_embed = f'![[{relative_video}]]'
                video_file_line = f'\nvideo_file: "{relative_video}"'
            except ValueError:
                video_embed = f'![[{video_path}]]'
                video_file_line = f'\nvideo_file: "{video_path}"'
        else:
            video_embed = f'<iframe width="560" height="315" src="https://www.youtube.com/embed/{video_id}" title="{metadata["title"]}" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>'
            video_file_line = ""

        # Format the transcript
        formatted_transcript = format_transcript(transcript) if transcript else "Transcript not available."

        # Get the raw transcript text for the summary (without timestamps)
        transcript_text = ""
        if transcript and hasattr(transcript, 'segments'):
            transcript_text = " ".join([getattr(seg, 'text', '') for seg in transcript.segments])
        elif transcript and hasattr(transcript, 'text'):
            transcript_text = transcript.text

        # Generate summary and TOC if we have transcript
        if not summary_and_toc and transcript_text:
            summary_and_toc = generate_summary_and_toc(transcript_text, metadata)

        if not summary_and_toc:
            summary_and_toc = """## Table of Contents
- Content not available

## Summary
Summary could not be generated."""

        # Format tags for frontmatter
        tags_str = ", ".join(categories)

        # Create the markdown content
        markdown_content = f"""---
channel: "{metadata['channel']}"
title: "{metadata['title']}"
youtube_url: "{youtube_url}"
slack_message_url: "{slack_message_url}"
duration: "{metadata['duration_string']}"
upload_date: "{metadata.get('upload_date', 'Unknown')}"
tags: [{tags_str}]{video_file_line}
---

# {metadata['title']}

{video_embed}

**Channel:** [{metadata['channel']}](https://www.youtube.com/results?search_query={metadata['channel'].replace(' ', '+')})
**Duration:** {metadata['duration_string']}
**Tags:** {tags_str}
**YouTube:** [{youtube_url}]({youtube_url})
**Slack Reference:** [View in Slack]({slack_message_url})

---

{summary_and_toc}

---

## Full Transcript

{formatted_transcript}
"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        return str(filepath)

    except Exception as e:
        print(f"Error creating markdown file: {e}")
        return None

def get_slack_message_url(channel, message_ts):
    """Construct a Slack message URL"""
    ts_for_url = message_ts.replace('.', '')
    return f"https://{SLACK_WORKSPACE}.slack.com/archives/{channel}/p{ts_for_url}"

def extract_original_message_info(event, client):
    """
    Extract the original message URL from forwarded messages.
    Returns (text_to_search, slack_url) tuple.
    """
    text = event.get('text', '')
    channel = event.get('channel')
    ts = event.get('ts')

    attachments = event.get('attachments', [])
    slack_link_pattern = r'https://([a-zA-Z0-9-]+)\.slack\.com/archives/([A-Z0-9]+)/p(\d+)'
    slack_link_match = re.search(slack_link_pattern, text)

    original_url = None
    search_text = text

    # Method 1: Check attachments for forwarded messages
    for attachment in attachments:
        if attachment.get('is_msg_unfurl'):
            original_url = attachment.get('original_url') or attachment.get('from_url')
            if attachment.get('text'):
                search_text = attachment.get('text')
            elif attachment.get('fallback'):
                search_text = attachment.get('fallback')
            break

        if attachment.get('channel_id') and attachment.get('ts'):
            orig_channel = attachment.get('channel_id')
            orig_ts = attachment.get('ts')
            original_url = get_slack_message_url(orig_channel, orig_ts)
            if attachment.get('text'):
                search_text = attachment.get('text')
            break

        if attachment.get('text') and extract_video_id(attachment.get('text', '')):
            search_text = attachment.get('text')
            if attachment.get('original_url'):
                original_url = attachment.get('original_url')
            break

    # Method 2: Check for Slack message link in text
    if not original_url and slack_link_match:
        workspace = slack_link_match.group(1)
        orig_channel = slack_link_match.group(2)
        orig_ts_raw = slack_link_match.group(3)
        orig_ts = orig_ts_raw[:-6] + '.' + orig_ts_raw[-6:]
        original_url = f"https://{workspace}.slack.com/archives/{orig_channel}/p{orig_ts_raw}"

        try:
            result = client.conversations_history(
                channel=orig_channel,
                latest=orig_ts,
                limit=1,
                inclusive=True
            )
            if result.get('messages'):
                orig_message = result['messages'][0]
                if orig_message.get('text'):
                    search_text = orig_message.get('text')
                for att in orig_message.get('attachments', []):
                    if att.get('text') and extract_video_id(att.get('text', '')):
                        search_text = att.get('text')
                        break
        except Exception as e:
            print(f"Could not fetch original message: {e}")

    # Method 3: Check blocks for shared messages
    blocks = event.get('blocks', [])
    for block in blocks:
        if block.get('type') == 'rich_text':
            for element in block.get('elements', []):
                if element.get('type') == 'rich_text_section':
                    for item in element.get('elements', []):
                        if item.get('type') == 'link':
                            url = item.get('url', '')
                            match = re.search(slack_link_pattern, url)
                            if match and not original_url:
                                original_url = url

    if not original_url:
        original_url = get_slack_message_url(channel, ts)

    return search_text, original_url

@app.event("message")
def handle_message(event, say, client):
    """Handle incoming messages and detect YouTube/Instagram links (including forwarded messages)

    Content is processed silently and added to a daily digest sent at 22:00.
    """
    global DIGEST_CHANNEL
    channel = event.get('channel')
    ts = event.get('ts')

    # Skip bot messages
    if event.get('bot_id'):
        return

    # Skip message subtypes we don't want
    subtype = event.get('subtype')
    if subtype in ['message_changed', 'message_deleted', 'channel_join', 'channel_leave']:
        return

    # Extract text and original URL (handles forwarded messages)
    search_text, slack_message_url = extract_original_message_info(event, client)
    main_text = event.get('text', '')

    # Try to find YouTube video ID
    video_id = extract_video_id(search_text)
    if not video_id:
        video_id = extract_video_id(main_text)

    # Also check attachments directly for YouTube links
    if not video_id:
        for attachment in event.get('attachments', []):
            title_link = attachment.get('title_link', '')
            if title_link:
                video_id = extract_video_id(title_link)
                if video_id:
                    break
            orig_url = attachment.get('original_url', '')
            if orig_url:
                video_id = extract_video_id(orig_url)
                if video_id:
                    break
            att_text = attachment.get('text', '')
            if att_text:
                video_id = extract_video_id(att_text)
                if video_id:
                    break

    # If YouTube found, check for duplicates then process
    if video_id:
        is_duplicate, existing_path, existing_title = check_youtube_already_processed(video_id)

        if is_duplicate:
            print(f"Duplicate YouTube video detected: {video_id} -> {existing_path}")
            # Send a reply to the thread notifying about the duplicate
            try:
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=ts,
                    text=f"This video has already been processed.\n\n"
                         f"*{existing_title}*\n"
                         f"File: `{existing_path}`"
                )
            except Exception as e:
                print(f"Error sending duplicate notification: {e}")
            return

        process_youtube_video(video_id, channel, slack_message_url)
        return

    # Try to find Instagram post/reel
    instagram_url = get_instagram_url(search_text)
    if not instagram_url:
        instagram_url = get_instagram_url(main_text)

    # Check attachments for Instagram links
    if not instagram_url:
        for attachment in event.get('attachments', []):
            for field in ['title_link', 'original_url', 'text']:
                content = attachment.get(field, '')
                if content:
                    instagram_url = get_instagram_url(content)
                    if instagram_url:
                        break
            if instagram_url:
                break

    # If Instagram found, check for duplicates then process
    if instagram_url:
        post_id = extract_instagram_id(instagram_url)
        is_duplicate, existing_path, existing_title = check_instagram_already_processed(post_id)

        if is_duplicate:
            print(f"Duplicate Instagram content detected: {post_id} -> {existing_path}")
            # Send a reply to the thread notifying about the duplicate
            try:
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=ts,
                    text=f"This Instagram content has already been downloaded.\n\n"
                         f"*{existing_title}*\n"
                         f"File: `{existing_path}`"
                )
            except Exception as e:
                print(f"Error sending duplicate notification: {e}")
            return

        process_instagram_content(instagram_url, channel, slack_message_url)
        return


def process_youtube_video(video_id, channel, slack_message_url):
    """Process a YouTube video: download video, transcribe, summarize, and add to daily digest."""
    global DIGEST_CHANNEL

    print(f"YouTube video detected: {video_id}")
    print(f"Slack reference URL: {slack_message_url}")

    # Remember channel for daily digest
    DIGEST_CHANNEL = channel

    # Get metadata
    print("Fetching video metadata...")
    metadata = get_video_metadata(video_id)
    if not metadata:
        print(f"Could not fetch metadata for {video_id}")
        with digest_lock:
            daily_digest_videos.append({
                'video_id': video_id,
                'title': f"Unknown (ID: {video_id})",
                'channel': "Unknown",
                'duration': "Unknown",
                'categories': [],
                'filepath': None,
                'video_path': None,
                'success': False,
                'error': "Could not fetch video metadata",
                'timestamp': datetime.now(),
                'platform': 'youtube'
            })
        return

    # Download full video
    print("Downloading video...")
    video_downloaded, video_path = download_youtube_video(video_id, metadata)
    if video_downloaded:
        print(f"Video saved to: {video_path}")
    else:
        print("Video download failed, continuing with transcription only")

    # Download audio for transcription
    print("Downloading audio for transcription...")
    audio_path = download_audio(video_id)

    transcript = None
    if audio_path:
        print("Transcribing with Whisper...")
        transcript = transcribe_with_whisper(audio_path)

        try:
            os.remove(audio_path)
            os.rmdir(os.path.dirname(audio_path))
        except:
            pass
    else:
        print("Could not download audio, proceeding without transcript")

    # Generate summary and TOC
    print("Generating summary and TOC...")
    transcript_text = ""
    if transcript and hasattr(transcript, 'segments'):
        transcript_text = " ".join([getattr(seg, 'text', '') for seg in transcript.segments])
    elif transcript and hasattr(transcript, 'text'):
        transcript_text = transcript.text

    summary_and_toc = None
    if transcript_text:
        summary_and_toc = generate_summary_and_toc(transcript_text, metadata)

    # Auto-assign categories
    print("Assigning categories...")
    categories = assign_categories(transcript_text, metadata)
    print(f"Assigned categories: {categories}")

    # Create markdown file
    print("Creating markdown file...")
    filepath = create_markdown_file(video_id, metadata, transcript, summary_and_toc, slack_message_url, categories, video_path)

    # Track for daily digest
    with digest_lock:
        daily_digest_videos.append({
            'video_id': video_id,
            'title': metadata['title'],
            'channel': metadata['channel'],
            'duration': metadata['duration_string'],
            'categories': categories,
            'filepath': filepath,
            'video_path': video_path,
            'success': filepath is not None,
            'error': None if filepath else "Failed to create markdown file",
            'timestamp': datetime.now(),
            'platform': 'youtube'
        })

    if filepath:
        print(f"Successfully processed: {metadata['title']} -> {filepath}")
        if video_path:
            print(f"Video file: {video_path}")
    else:
        print(f"Failed to create file for: {metadata['title']}")


def process_instagram_content(instagram_url, channel, slack_message_url):
    """Process Instagram content silently and add to daily digest."""
    global DIGEST_CHANNEL

    post_id = extract_instagram_id(instagram_url)
    print(f"Instagram content detected: {instagram_url} (ID: {post_id})")
    print(f"Slack reference URL: {slack_message_url}")

    # Remember channel for daily digest
    DIGEST_CHANNEL = channel

    # Download content
    print("Downloading Instagram content...")
    success, media_path, metadata, error = download_instagram_content(instagram_url, post_id)

    if not success or not metadata:
        print(f"Failed to download Instagram content: {error}")
        with digest_lock:
            daily_digest_videos.append({
                'video_id': post_id,
                'title': f"Instagram_{post_id}",
                'channel': "Unknown",
                'duration': "N/A",
                'categories': ['instagram'],
                'filepath': None,
                'success': False,
                'error': error or "Download failed",
                'timestamp': datetime.now(),
                'platform': 'instagram'
            })
        return

    # Create markdown file
    print("Creating Instagram markdown file...")
    md_filepath = create_instagram_markdown(post_id, instagram_url, metadata, media_path, slack_message_url)

    # Track for daily digest
    with digest_lock:
        daily_digest_videos.append({
            'video_id': post_id,
            'title': metadata['title'][:50] if metadata['title'] else f"Instagram_{post_id}",
            'channel': metadata['uploader'],
            'duration': metadata['duration_string'],
            'categories': ['instagram'],
            'filepath': media_path,
            'success': media_path is not None,
            'error': None if media_path else "Failed to save media",
            'timestamp': datetime.now(),
            'platform': 'instagram'
        })

    if media_path:
        print(f"Successfully downloaded: {metadata['title'][:50]} -> {media_path}")
    else:
        print(f"Failed to download Instagram content: {post_id}")


def check_youtube_already_processed(video_id):
    """
    Check if a YouTube video has already been processed with valid content.
    Returns (is_duplicate, filepath, title) tuple.

    Validates that:
    - A markdown file exists with this video ID
    - The file has actual transcript content (not empty/placeholder)
    """
    # Search all .md files in the Knowledger folder and subfolders
    for md_file in DOWNLOAD_DIR.rglob("*.md"):
        try:
            with open(md_file, 'r', encoding='utf-8') as f:
                content = f.read()

                # Check if this file contains our video ID
                if f'youtube.com/watch?v={video_id}' not in content and f'youtube.com/embed/{video_id}' not in content:
                    continue

                # Found a file with this video ID - now validate content
                # Check for actual transcript content (not placeholder)
                has_transcript = '## Full Transcript' in content

                # Check transcript isn't empty or placeholder
                transcript_section = content.split('## Full Transcript')[-1] if has_transcript else ''

                # Valid if transcript has actual timestamped content [MM:SS]
                has_valid_transcript = bool(re.search(r'\[\d{2}:\d{2}\]', transcript_section))

                # Also check if summary exists and isn't placeholder
                has_summary = '## Summary' in content
                summary_section = ''
                if has_summary:
                    try:
                        summary_start = content.index('## Summary')
                        summary_end = content.index('---', summary_start + 1) if '---' in content[summary_start:] else len(content)
                        summary_section = content[summary_start:summary_end]
                    except ValueError:
                        pass

                has_valid_summary = len(summary_section) > 100  # More than just the header

                # Extract title from frontmatter or heading
                title_match = re.search(r'title:\s*"([^"]+)"', content)
                title = title_match.group(1) if title_match else md_file.stem

                # Consider valid if has either valid transcript OR valid summary
                if has_valid_transcript or has_valid_summary:
                    return True, str(md_file), title
                else:
                    # File exists but content is empty/placeholder - should reprocess
                    print(f"Found file for {video_id} but content appears incomplete: {md_file}")
                    return False, None, None

        except Exception as e:
            print(f"Error checking {md_file}: {e}")
            continue

    return False, None, None


def check_instagram_already_processed(post_id):
    """
    Check if Instagram content has already been processed.
    Returns (is_duplicate, filepath, title) tuple.

    Validates that:
    - A media file exists for this post ID
    - OR a markdown file exists with this post ID
    """
    # Check for files in the instagram folder
    for file in INSTAGRAM_DIR.glob("*"):
        try:
            # Check markdown files for the post ID
            if file.suffix == '.md':
                with open(file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if f'instagram.com/p/{post_id}' in content or \
                       f'instagram.com/reel/{post_id}' in content or \
                       f'instagram.com/tv/{post_id}' in content:
                        # Found matching markdown - check if media file also exists
                        title_match = re.search(r'title:\s*"([^"]+)"', content)
                        title = title_match.group(1) if title_match else file.stem

                        # Look for corresponding media file
                        media_pattern = file.stem + ".*"
                        media_files = [f for f in INSTAGRAM_DIR.glob(media_pattern) if f.suffix != '.md']

                        if media_files:
                            return True, str(media_files[0]), title
                        else:
                            # Markdown exists but no media - might need reprocessing
                            print(f"Found markdown for {post_id} but no media file")
                            return False, None, None
        except Exception as e:
            print(f"Error checking {file}: {e}")
            continue

    # Also search all markdown files in case it was moved
    for md_file in DOWNLOAD_DIR.rglob("*.md"):
        try:
            with open(md_file, 'r', encoding='utf-8') as f:
                content = f.read()
                if f'instagram.com/p/{post_id}' in content or \
                   f'instagram.com/reel/{post_id}' in content or \
                   f'instagram.com/tv/{post_id}' in content:
                    title_match = re.search(r'title:\s*"([^"]+)"', content)
                    title = title_match.group(1) if title_match else md_file.stem
                    return True, str(md_file), title
        except Exception as e:
            continue

    return False, None, None


def get_processed_video_ids():
    """
    Scan the Knowledger folder (including subfolders) for already processed videos.
    Returns a set of video IDs that have been processed.
    """
    processed_ids = set()

    # Search all .md files in the Knowledger folder and subfolders
    for md_file in DOWNLOAD_DIR.rglob("*.md"):
        try:
            with open(md_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # Look for youtube_url in frontmatter or YouTube embed
                youtube_match = re.search(r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})', content)
                if youtube_match:
                    processed_ids.add(youtube_match.group(1))
                # Also check for embed URLs
                embed_match = re.search(r'youtube\.com/embed/([a-zA-Z0-9_-]{11})', content)
                if embed_match:
                    processed_ids.add(embed_match.group(1))
        except Exception as e:
            print(f"Error reading {md_file}: {e}")

    return processed_ids


def extract_youtube_links_from_messages(messages):
    """
    Extract all YouTube video IDs from a list of Slack messages.
    Returns a list of (video_id, message_info) tuples.
    """
    videos = []

    for msg in messages:
        text = msg.get('text', '')
        ts = msg.get('ts', '')

        # Check main text
        video_id = extract_video_id(text)
        if video_id:
            videos.append((video_id, {'ts': ts, 'text': text}))
            continue

        # Check attachments
        for attachment in msg.get('attachments', []):
            for field in ['title_link', 'original_url', 'text', 'from_url']:
                content = attachment.get(field, '')
                if content:
                    video_id = extract_video_id(content)
                    if video_id:
                        videos.append((video_id, {'ts': ts, 'text': text}))
                        break
            if video_id:
                break

        # Check blocks for links
        for block in msg.get('blocks', []):
            if block.get('type') == 'rich_text':
                for element in block.get('elements', []):
                    for item in element.get('elements', []):
                        if item.get('type') == 'link':
                            url = item.get('url', '')
                            video_id = extract_video_id(url)
                            if video_id:
                                videos.append((video_id, {'ts': ts, 'text': text}))
                                break

    return videos


def process_video_bulk(video_id, channel, ts, client, slack_message_url):
    """
    Process a single video for bulk processing (without Slack status updates).
    Returns (success: bool, filepath: str or None, error: str or None)
    """
    try:
        # Get metadata
        metadata = get_video_metadata(video_id)
        if not metadata:
            return False, None, "Could not fetch video metadata"

        # Download full video
        video_downloaded, video_path = download_youtube_video(video_id, metadata)
        if not video_downloaded:
            print(f"Video download failed for {video_id}, continuing with transcription only")

        # Download audio for transcription
        audio_path = download_audio(video_id)

        transcript = None
        if audio_path:
            # Transcribe with Whisper
            transcript = transcribe_with_whisper(audio_path)

            # Clean up audio file
            try:
                os.remove(audio_path)
                os.rmdir(os.path.dirname(audio_path))
            except:
                pass

        # Get transcript text
        transcript_text = ""
        if transcript and hasattr(transcript, 'segments'):
            transcript_text = " ".join([getattr(seg, 'text', '') for seg in transcript.segments])
        elif transcript and hasattr(transcript, 'text'):
            transcript_text = transcript.text

        # Generate summary and TOC
        summary_and_toc = None
        if transcript_text:
            summary_and_toc = generate_summary_and_toc(transcript_text, metadata)

        # Auto-assign categories
        categories = assign_categories(transcript_text, metadata)

        # Create markdown file
        filepath = create_markdown_file(video_id, metadata, transcript, summary_and_toc, slack_message_url, categories, video_path)

        if filepath:
            return True, filepath, None
        else:
            return False, None, "Failed to create markdown file"

    except Exception as e:
        return False, None, str(e)


def parse_date_range(date_str):
    """
    Parse a date range string into (oldest_datetime, newest_datetime) tuple.
    Supports formats:
    - YYYY-MM (e.g., 2024-08 for August 2024)
    - Q1-YYYY, Q2-YYYY, etc. (e.g., Q1-2025 for Jan-Mar 2025)
    - YYYY (e.g., 2024 for full year)
    - Integer (e.g., 30 for last 30 days)
    Returns (oldest, newest, description) tuple.
    """
    date_str = date_str.strip().upper()

    # Quarter format: Q1-2025, Q2-2024, etc.
    quarter_match = re.match(r'Q([1-4])-?(\d{4})', date_str)
    if quarter_match:
        quarter = int(quarter_match.group(1))
        year = int(quarter_match.group(2))
        quarter_starts = {1: 1, 2: 4, 3: 7, 4: 10}
        quarter_ends = {1: 3, 2: 6, 3: 9, 4: 12}
        start_month = quarter_starts[quarter]
        end_month = quarter_ends[quarter]
        oldest = datetime(year, start_month, 1)
        # Last day of quarter
        if end_month == 12:
            newest = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            newest = datetime(year, end_month + 1, 1) - timedelta(seconds=1)
        return oldest, newest, f"Q{quarter} {year}"

    # Month format: 2024-08, 2024-12, etc.
    month_match = re.match(r'(\d{4})-(\d{1,2})', date_str)
    if month_match:
        year = int(month_match.group(1))
        month = int(month_match.group(2))
        oldest = datetime(year, month, 1)
        # Last day of month
        if month == 12:
            newest = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        else:
            newest = datetime(year, month + 1, 1) - timedelta(seconds=1)
        month_name = oldest.strftime('%B %Y')
        return oldest, newest, month_name

    # Year format: 2024
    year_match = re.match(r'^(\d{4})$', date_str)
    if year_match:
        year = int(year_match.group(1))
        oldest = datetime(year, 1, 1)
        newest = datetime(year + 1, 1, 1) - timedelta(seconds=1)
        return oldest, newest, str(year)

    # Days back format: 30, 60, 90, etc.
    try:
        days = int(date_str)
        newest = datetime.now()
        oldest = newest - timedelta(days=days)
        return oldest, newest, f"last {days} days"
    except ValueError:
        pass

    # Default: last 30 days
    newest = datetime.now()
    oldest = newest - timedelta(days=30)
    return oldest, newest, "last 30 days"


@app.command("/process-history")
def handle_process_history(ack, command, client, respond):
    """
    Slash command to bulk process YouTube videos from conversation history.
    Usage: /process-history [channel_id] [date_range]
    Examples:
        /process-history C1234567890 2024-08      (August 2024)
        /process-history C1234567890 Q1-2025      (Q1 2025: Jan-Mar)
        /process-history C1234567890 2024         (Full year 2024)
        /process-history C1234567890 30           (Last 30 days)
    """
    ack()

    args = command.get('text', '').strip().split()

    # Parse arguments
    if len(args) >= 1:
        target_channel = args[0]
    else:
        # Default to the channel where command was issued
        target_channel = command.get('channel_id')

    if len(args) >= 2:
        date_arg = args[1]
    else:
        date_arg = "30"  # Default to last 30 days

    # Parse the date range
    oldest_date, newest_date, date_description = parse_date_range(date_arg)

    respond(f"🔍 Starting to scan conversation `{target_channel}` for **{date_description}**...\n"
            f"📅 Date range: {oldest_date.strftime('%Y-%m-%d')} to {newest_date.strftime('%Y-%m-%d')}\n"
             "This may take a while. I'll update you on progress.")

    # Convert to timestamps
    oldest_ts = str(oldest_date.timestamp())
    latest_ts = str(newest_date.timestamp())

    # Get already processed video IDs
    processed_ids = get_processed_video_ids()
    respond(f"📚 Found {len(processed_ids)} already processed videos in your knowledge base.")

    # Fetch conversation history
    try:
        all_messages = []
        cursor = None

        while True:
            kwargs = {
                'channel': target_channel,
                'oldest': oldest_ts,
                'latest': latest_ts,
                'limit': 200
            }
            if cursor:
                kwargs['cursor'] = cursor

            result = client.conversations_history(**kwargs)
            all_messages.extend(result.get('messages', []))

            cursor = result.get('response_metadata', {}).get('next_cursor')
            if not cursor:
                break

            time.sleep(0.5)  # Rate limiting

        respond(f"📨 Fetched {len(all_messages)} messages from history.")

    except Exception as e:
        respond(f"❌ Error fetching conversation history: {e}\n"
                "Make sure the bot is a member of the channel/DM and has `channels:history` or `groups:history` scope.")
        return

    # Extract YouTube links
    youtube_videos = extract_youtube_links_from_messages(all_messages)

    # Filter out already processed
    new_videos = [(vid, info) for vid, info in youtube_videos if vid not in processed_ids]

    # Remove duplicates (keep first occurrence)
    seen = set()
    unique_new_videos = []
    for vid, info in new_videos:
        if vid not in seen:
            seen.add(vid)
            unique_new_videos.append((vid, info))

    respond(f"🎬 Found {len(youtube_videos)} total YouTube links.\n"
            f"✅ {len(youtube_videos) - len(unique_new_videos)} already processed.\n"
            f"🆕 {len(unique_new_videos)} new videos to process.")

    if not unique_new_videos:
        respond("✨ All videos have already been processed! Nothing to do.")
        return

    # Process each video
    successes = 0
    failures = 0

    for i, (video_id, msg_info) in enumerate(unique_new_videos, 1):
        respond(f"⏳ Processing video {i}/{len(unique_new_videos)}: `{video_id}`...")

        # Construct Slack message URL
        slack_message_url = get_slack_message_url(target_channel, msg_info['ts'])

        success, filepath, error = process_video_bulk(
            video_id, target_channel, msg_info['ts'], client, slack_message_url
        )

        if success:
            successes += 1
            respond(f"✅ [{i}/{len(unique_new_videos)}] Created: `{filepath}`")
        else:
            failures += 1
            respond(f"❌ [{i}/{len(unique_new_videos)}] Failed: {error}")

        # Rate limiting between videos
        if i < len(unique_new_videos):
            time.sleep(2)

    # Final summary
    respond(f"\n🎉 **Bulk processing complete!**\n"
            f"✅ Successfully processed: {successes}\n"
            f"❌ Failed: {failures}\n"
            f"📁 Files saved to: `{DOWNLOAD_DIR}`")


def send_daily_digest(client):
    """Send the daily digest of processed content to Slack."""
    global daily_digest_videos, DIGEST_CHANNEL

    with digest_lock:
        if not daily_digest_videos:
            print(f"[{datetime.now()}] No content to include in daily digest")
            return

        # Get videos and clear the list
        videos = daily_digest_videos.copy()
        daily_digest_videos = []

    if not DIGEST_CHANNEL:
        print(f"[{datetime.now()}] No channel set for digest - content processed but not reported")
        return

    # Separate by platform and success
    successful = [v for v in videos if v['success']]
    failed = [v for v in videos if not v['success']]

    youtube_success = [v for v in successful if v.get('platform') == 'youtube']
    instagram_success = [v for v in successful if v.get('platform') == 'instagram']

    message_parts = [f"*Daily Knowledge Base Digest* - {datetime.now().strftime('%A, %B %d, %Y')}"]
    message_parts.append(f"\n{len(successful)} item(s) processed today\n")

    if youtube_success:
        message_parts.append(f"*YouTube ({len(youtube_success)}):*")
        for v in youtube_success:
            tags = ', '.join(v['categories']) if v['categories'] else 'none'
            video_status = "downloaded" if v.get('video_path') else "transcript only"
            message_parts.append(
                f"• *{v['title']}*\n"
                f"  Channel: {v['channel']} | Duration: {v['duration']} | Tags: {tags} | Video: {video_status}"
            )

    if instagram_success:
        message_parts.append(f"\n*Instagram ({len(instagram_success)}):*")
        for v in instagram_success:
            message_parts.append(
                f"• *{v['title']}*\n"
                f"  Uploader: {v['channel']} | Duration: {v['duration']}"
            )

    if failed:
        message_parts.append(f"\n{len(failed)} item(s) failed:")
        for v in failed:
            platform = v.get('platform', 'unknown')
            message_parts.append(f"• [{platform}] {v['title']}: {v['error']}")

    message_parts.append(f"\nFiles saved to: `{DOWNLOAD_DIR}`")

    try:
        client.chat_postMessage(
            channel=DIGEST_CHANNEL,
            text='\n'.join(message_parts)
        )
        print(f"[{datetime.now()}] Daily digest sent to {DIGEST_CHANNEL}")
    except Exception as e:
        print(f"[{datetime.now()}] Error sending daily digest: {e}")


def run_digest_scheduler(client):
    """Background thread that sends daily digest at DIGEST_HOUR."""
    print(f"[{datetime.now()}] Digest scheduler started - will send digest daily at {DIGEST_HOUR}:00")

    last_digest_date = None

    while True:
        now = datetime.now()

        # Check if it's digest time and we haven't sent today
        if now.hour == DIGEST_HOUR and now.date() != last_digest_date:
            print(f"[{datetime.now()}] Digest time! Sending daily digest...")
            send_daily_digest(client)
            last_digest_date = now.date()

        # Sleep for 60 seconds before checking again
        time.sleep(60)


if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    print("Knowledge Bot is running!")
    print(f"Files will be saved to: {DOWNLOAD_DIR}")
    print("Supported platforms: YouTube, Instagram")
    print("YouTube: Video download (1080p) + Whisper transcription + GPT summaries")
    print("Instagram: Download only (no transcription)")
    print(f"Daily digest will be sent at {DIGEST_HOUR}:00")
    print("Use /process-history to bulk process YouTube videos from conversation history")

    # Start digest scheduler in background thread
    slack_client = app.client
    digest_thread = threading.Thread(target=run_digest_scheduler, args=(slack_client,), daemon=True)
    digest_thread.start()

    handler.start()
