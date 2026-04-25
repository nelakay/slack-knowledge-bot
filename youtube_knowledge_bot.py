#!/usr/bin/env python3
"""
YouTube Slack Bot - Markdown Knowledge Base Version
Creates rich markdown files with video metadata, table of contents, summary, and Whisper transcripts
Supports forwarded messages - links back to original message
"""

import os
import re
import json
import tempfile
import threading
from pathlib import Path
from datetime import datetime, timedelta
import time
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from pytubefix import YouTube
from openai import OpenAI
import requests
import urllib.parse
import yt_dlp
import instaloader

from backfill_failures import (
    parse_digest_text,
    parse_retry_button,
    attach_urls_from_retry_button,
    DIGEST_HEADER,
)

# RapidAPI config for Instagram downloads
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_IG_HOST = "social-media-video-downloader.p.rapidapi.com"

# Apify config for LinkedIn post fetching (optional — manual paste works without it)
APIFY_API_TOKEN = os.environ.get("APIFY_API_TOKEN", "")
# Actor ID in `username/actor-name` form. The default targets a public LinkedIn post
# scraper; override via env if you prefer a different actor. Verify your actor's
# input schema matches APIFY_LINKEDIN_INPUT_KEY below.
APIFY_LINKEDIN_ACTOR = os.environ.get("APIFY_LINKEDIN_ACTOR", "curious_coder/linkedin-post-scraper")
# JSON key the actor expects for the URL list. Most LinkedIn post actors use
# "postUrls" or "urls". Change if your chosen actor uses a different field.
APIFY_LINKEDIN_INPUT_KEY = os.environ.get("APIFY_LINKEDIN_INPUT_KEY", "postUrls")

# Initialize Slack app
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Configuration
DOWNLOAD_DIR = Path("/Volumes/Knowledger/vault/Knowledger")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Your Slack workspace URL (update this with your workspace)
SLACK_WORKSPACE = "cocoworkshq"

# Daily digest configuration
DIGEST_HOUR = 22  # 10 PM
DIGEST_CHANNEL = None  # Will be set to the last channel where a video was processed

# Auto-catchup configuration
CATCHUP_CHANNELS = ["C0A99TH4Y2V"]  # #knowledger - channels to scan for missed content
CATCHUP_LOOKBACK_HOURS = 24  # How far back to scan

# Track processed videos for daily digest
daily_digest_videos = []
digest_lock = threading.Lock()

# Persistent log of every processed item (one JSON object per line, appended at digest time).
# Used by /show-failures to answer "what failed in the last N days".
HISTORY_FILE = Path(__file__).parent / "processing_history.jsonl"
history_lock = threading.Lock()

# YouTube URL patterns
YOUTUBE_PATTERNS = [
    r'(?:https?://)?(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
    r'(?:https?://)?(?:www\.)?youtu\.be/([a-zA-Z0-9_-]{11})',
    r'(?:https?://)?(?:www\.)?youtube\.com/shorts/([a-zA-Z0-9_-]{11})'
]

# Instagram URL patterns
INSTAGRAM_PATTERNS = [
    r'(?:https?://)?(?:www\.)?instagram\.com/p/([a-zA-Z0-9_-]+)',      # Posts
    r'(?:https?://)?(?:www\.)?instagram\.com/reels?/([a-zA-Z0-9_-]+)',  # Reels (reel/ or reels/)
    r'(?:https?://)?(?:www\.)?instagram\.com/tv/([a-zA-Z0-9_-]+)',     # IGTV
]

# LinkedIn URL patterns
LINKEDIN_PATTERNS = [
    r'(?:https?://)?(?:www\.)?linkedin\.com/posts/[^\s>|]+',
    r'(?:https?://)?(?:www\.)?linkedin\.com/feed/update/[^\s>|]+',
    r'(?:https?://)?(?:www\.)?linkedin\.com/pulse/[^\s>|]+',
]

# Instagram markdown save directory
INSTAGRAM_DIR = DOWNLOAD_DIR / "instagram"
INSTAGRAM_DIR.mkdir(parents=True, exist_ok=True)

# LinkedIn markdown save directory
LINKEDIN_DIR = DOWNLOAD_DIR / "linkedin"
LINKEDIN_DIR.mkdir(parents=True, exist_ok=True)

# YouTube video save directory
YOUTUBE_VIDEO_DIR = DOWNLOAD_DIR / "assets"
YOUTUBE_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
ASSETS_DIR = YOUTUBE_VIDEO_DIR  # Alias used by Instagram download functions

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
    "ip",              # intellectual property, patents, trademarks
    "undefined",       # for manual categorization when unsure
]

# Resources index file
RESOURCES_FILE = DOWNLOAD_DIR / "resources.md"

# Image extensions we consider static images
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.tiff', '.svg'}


# ---------------------------------------------------------------------------
# Resources index (resources.md) — standalone link collector
# ---------------------------------------------------------------------------

# URL patterns that are already handled by platform-specific processors
PLATFORM_URL_PATTERNS = YOUTUBE_PATTERNS + INSTAGRAM_PATTERNS + LINKEDIN_PATTERNS

def is_platform_url(url):
    """Return True if the URL belongs to a platform we already handle (YouTube, Instagram, LinkedIn)."""
    for pattern in PLATFORM_URL_PATTERNS:
        if re.search(pattern, url):
            return True
    return False


def extract_generic_urls(text):
    """Extract all HTTP(S) URLs from text, excluding platform-specific ones and Slack internal links."""
    urls = re.findall(r'https?://[^\s>|)\]]+', text)
    filtered = []
    for url in urls:
        url = url.rstrip('.,;:!?')  # strip trailing punctuation
        if is_platform_url(url):
            continue
        if 'slack.com/archives/' in url:
            continue
        filtered.append(url)
    return filtered


def fetch_url_metadata(url):
    """Fetch the page title and meta description for a URL. Returns (title, description)."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text[:50000]  # limit to first 50k chars

        # Extract <title>
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""

        # Extract meta description (og:description or name="description")
        desc = ""
        og_match = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']*)["\']', html, re.IGNORECASE)
        if not og_match:
            og_match = re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+property=["\']og:description["\']', html, re.IGNORECASE)
        if og_match:
            desc = og_match.group(1).strip()
        else:
            meta_match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']', html, re.IGNORECASE)
            if not meta_match:
                meta_match = re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description["\']', html, re.IGNORECASE)
            if meta_match:
                desc = meta_match.group(1).strip()

        # Clean HTML entities
        import html as html_mod
        title = html_mod.unescape(title)
        desc = html_mod.unescape(desc)

        return title, desc
    except Exception as e:
        print(f"Error fetching metadata for {url}: {e}")
        return "", ""


def assign_resource_tags(name, description, url, message_text):
    """Use GPT to assign tags to a resource link."""
    try:
        categories_str = ", ".join(ALLOWED_CATEGORIES)
        context = f"Title: {name}\nDescription: {description}\nURL: {url}"
        if message_text:
            context += f"\nUser note: {message_text}"

        prompt = f"""Based on the following resource, assign 1-3 categories from this EXACT list:
{categories_str}

Resource:
{context[:3000]}

Rules:
- ONLY use categories from the list above
- Return 1-3 categories that best fit the content
- ONLY use "undefined" if NO other category fits at all. Never combine "undefined" with other categories.
- Return ONLY the category names, comma-separated, nothing else
"""
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a categorization assistant. Return only category names from the provided list, comma-separated."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50
        )
        raw = response.choices[0].message.content.strip().lower()
        assigned = [cat.strip() for cat in raw.split(",")]
        valid = [cat for cat in assigned if cat in ALLOWED_CATEGORIES]
        if not valid:
            return ["undefined"]
        return [cat for cat in valid if cat != "undefined"] or ["undefined"]
    except Exception as e:
        print(f"Error assigning resource tags: {e}")
        return ["undefined"]


def update_resources_md(name, url, description="", tags=None, slack_message_url="", message_text=""):
    """Append an entry to the running resources.md table."""
    try:
        tags_str = ", ".join(tags) if tags else ""
        # Escape pipe characters in fields for markdown table
        name = name.replace("|", "-").replace("\n", " ")
        description = description.replace("|", "-").replace("\n", " ")[:200]
        tags_str = tags_str.replace("|", "-")
        clean_text = re.sub(r'https?://\S+', '', message_text).strip().replace("|", "-").replace("\n", " ")[:200]

        if not RESOURCES_FILE.exists():
            header = (
                "# Resources\n\n"
                "| # | Name | URL | Description | Tags | Slack Message | Message Text |\n"
                "| - | ---- | --- | ----------- | ---- | ------------- | ------------ |\n"
            )
            with open(RESOURCES_FILE, 'w', encoding='utf-8') as f:
                f.write(header)

        # Check for duplicate URL
        try:
            existing = RESOURCES_FILE.read_text(encoding='utf-8')
            if url in existing:
                print(f"Resource already in resources.md: {url}")
                return False
        except Exception:
            pass

        # Count existing data rows to determine next row number
        try:
            existing_lines = RESOURCES_FILE.read_text(encoding='utf-8').splitlines()
            row_count = sum(1 for l in existing_lines if l.startswith("|") and not l.startswith("| #") and not l.startswith("| -"))
        except Exception:
            row_count = 0
        next_num = row_count + 1

        slack_link = f"[Slack]({slack_message_url})" if slack_message_url else ""
        row = f"| {next_num} | {name} | [{url}]({url}) | {description} | {tags_str} | {slack_link} | {clean_text} |\n"
        with open(RESOURCES_FILE, 'a', encoding='utf-8') as f:
            f.write(row)
        print(f"Added to resources.md: {name}")
        return True
    except Exception as e:
        print(f"Error updating resources.md: {e}")
        return False


def process_resource_links(urls, message_text, channel, slack_message_url):
    """Process one or more generic URLs: fetch metadata, tag, and add to resources.md."""
    added = 0
    for url in urls:
        print(f"Processing resource link: {url}")
        name, description = fetch_url_metadata(url)
        if not name:
            # Fallback: use domain + path as name
            parsed = urllib.parse.urlparse(url)
            name = parsed.netloc + parsed.path.rstrip('/')

        # Strip the accompanying message of URLs for cleaner context
        clean_text = re.sub(r'https?://\S+', '', message_text).strip()

        tags = assign_resource_tags(name, description, url, clean_text)
        if update_resources_md(name=name, url=url, description=description, tags=tags, slack_message_url=slack_message_url, message_text=message_text):
            added += 1

    if added:
        print(f"Added {added} resource(s) to resources.md")


# ---------------------------------------------------------------------------
# Static image download from Slack messages
# ---------------------------------------------------------------------------

def download_slack_image(file_info, bot_token):
    """Download a single image file from Slack to the assets folder. Returns the saved path or None."""
    try:
        url = file_info.get('url_private_download') or file_info.get('url_private')
        if not url:
            return None

        original_name = file_info.get('name', 'image')
        safe_name = sanitize_filename(Path(original_name).stem)
        ext = Path(original_name).suffix.lower()
        if ext not in IMAGE_EXTENSIONS:
            ext = '.png'  # fallback

        filename = f"{safe_name}{ext}"
        filepath = ASSETS_DIR / filename

        # Deduplicate
        counter = 1
        while filepath.exists():
            filename = f"{safe_name}_{counter}{ext}"
            filepath = ASSETS_DIR / filename
            counter += 1

        headers = {'Authorization': f'Bearer {bot_token}'}
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()

        with open(filepath, 'wb') as f:
            f.write(resp.content)

        print(f"Downloaded image: {filepath}")
        return str(filepath)
    except Exception as e:
        print(f"Error downloading Slack image: {e}")
        return None


def generate_image_title(message_text, filenames):
    """Use GPT to generate a short title for an image post."""
    try:
        context = message_text if message_text else f"Images: {', '.join(filenames)}"
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Generate a concise title (max 10 words) for a post containing these images. Return ONLY the title, nothing else."},
                {"role": "user", "content": context[:2000]}
            ],
            max_tokens=30
        )
        return response.choices[0].message.content.strip().strip('"\'')
    except Exception as e:
        print(f"Error generating image title: {e}")
        return "Shared Images"


def assign_image_categories(message_text, filenames):
    """Use GPT to assign categories to an image post."""
    try:
        categories_str = ", ".join(ALLOWED_CATEGORIES)
        context = message_text if message_text else f"Images: {', '.join(filenames)}"
        prompt = f"""Based on the following message/image post, assign 1-3 categories from this EXACT list:
{categories_str}

Content:
{context[:3000]}

Rules:
- ONLY use categories from the list above
- Return 1-3 categories that best fit the content
- ONLY use "undefined" if NO other category fits at all. Never combine "undefined" with other categories.
- Return ONLY the category names, comma-separated, nothing else
"""
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a categorization assistant. Return only category names from the provided list, comma-separated."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50
        )
        raw = response.choices[0].message.content.strip().lower()
        assigned = [cat.strip() for cat in raw.split(",")]
        valid = [cat for cat in assigned if cat in ALLOWED_CATEGORIES]
        if not valid:
            return ["undefined"]
        return [cat for cat in valid if cat != "undefined"] or ["undefined"]
    except Exception as e:
        print(f"Error assigning image categories: {e}")
        return ["undefined"]


def create_image_markdown(title, image_paths, message_text, slack_message_url, categories):
    """Create a markdown file for a message with images."""
    try:
        safe_title = sanitize_filename(title[:60])
        filename = f"Images - {safe_title}.md"
        filepath = DOWNLOAD_DIR / filename

        counter = 1
        while filepath.exists():
            filename = f"Images - {safe_title}_{counter}.md"
            filepath = DOWNLOAD_DIR / filename
            counter += 1

        tags_str = ", ".join(["images"] + categories)

        # Build image embeds
        embeds = []
        for p in image_paths:
            try:
                relative = Path(p).relative_to(DOWNLOAD_DIR)
            except ValueError:
                relative = Path(p).name
            embeds.append(f"![[{relative}]]")
        embed_section = "\n\n".join(embeds)

        text_section = ""
        if message_text and message_text.strip():
            text_section = f"""---

## Message

{message_text.strip()}
"""

        markdown_content = f"""---
platform: "images"
title: "{sanitize_frontmatter(title)}"
slack_message_url: "{slack_message_url}"
tags: [{tags_str}]
---

# {title}

**Tags:** {tags_str}
**Slack Reference:** [View in Slack]({slack_message_url})

---

{embed_section}
{text_section}"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        return str(filepath)
    except Exception as e:
        print(f"Error creating image markdown: {e}")
        return None


def process_slack_images(event, channel, slack_message_url):
    """Process static images from a Slack message. Downloads images and optionally creates markdown."""
    global DIGEST_CHANNEL

    files = event.get('files', [])
    message_text = event.get('text', '').strip()
    bot_token = os.environ.get("SLACK_BOT_TOKEN")

    # Filter to image files only
    image_files = [f for f in files if f.get('mimetype', '').startswith('image/') or
                   Path(f.get('name', '')).suffix.lower() in IMAGE_EXTENSIONS]

    if not image_files:
        return False  # no images to process

    DIGEST_CHANNEL = channel

    # Download all images
    downloaded_paths = []
    filenames = []
    for file_info in image_files:
        path = download_slack_image(file_info, bot_token)
        if path:
            downloaded_paths.append(path)
            filenames.append(file_info.get('name', 'image'))

    if not downloaded_paths:
        print("No images were successfully downloaded")
        return False

    # Remove URLs from message text for cleaner content
    clean_text = re.sub(r'https?://\S+', '', message_text).strip()

    # Determine if we need a markdown file (only if there's text)
    has_text = bool(clean_text)
    md_filepath = None

    if has_text:
        title = generate_image_title(clean_text, filenames)
        categories = assign_image_categories(clean_text, filenames)
        md_filepath = create_image_markdown(title, downloaded_paths, clean_text, slack_message_url, categories)
    else:
        title = generate_image_title("", filenames)
        categories = assign_image_categories("", filenames)

    # Track for daily digest
    with digest_lock:
        daily_digest_videos.append({
            'video_id': f"img_{event.get('ts', '')}",
            'title': title,
            'channel': 'Slack Images',
            'duration': 'N/A',
            'categories': categories,
            'filepath': md_filepath or downloaded_paths[0],
            'video_path': None,
            'success': True,
            'error': None,
            'timestamp': datetime.now(),
            'platform': 'images',
            'url': slack_message_url,
            'slack_message_url': slack_message_url
        })

    action = "downloaded" if not has_text else "downloaded + markdown created"
    print(f"Processed {len(downloaded_paths)} image(s) ({action}): {title}")
    return True


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


def strip_emojis(text):
    """Remove emoji characters from text"""
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002700-\U000027BF"  # dingbats
        "\U0000FE00-\U0000FE0F"  # variation selectors
        "\U0000200D"             # zero width joiner
        "\U00002702-\U000027B0"  # dingbats
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U0001FA00-\U0001FA6F"  # chess symbols
        "\U0001FA70-\U0001FAFF"  # symbols extended-A
        "\U00002600-\U000026FF"  # misc symbols
        "\U0000203C-\U00003299"  # misc symbols
        "]+", flags=re.UNICODE
    )
    return emoji_pattern.sub('', text).strip()


def extract_linkedin_url(text):
    """Extract LinkedIn URL from text"""
    for pattern in LINKEDIN_PATTERNS:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    return None


def generate_linkedin_title(post_text):
    """Use GPT-4o-mini to generate a short title for a LinkedIn post"""
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Generate a concise title (max 10 words) for this LinkedIn post. Return ONLY the title, nothing else."},
                {"role": "user", "content": post_text[:2000]}
            ],
            max_tokens=30
        )
        title = response.choices[0].message.content.strip().strip('"\'')
        return title
    except Exception as e:
        print(f"Error generating LinkedIn title: {e}")
        return "LinkedIn Post"


def assign_linkedin_categories(post_text):
    """Use GPT-4o-mini to assign categories to a LinkedIn post"""
    try:
        categories_str = ", ".join(ALLOWED_CATEGORIES)
        prompt = f"""Based on the following LinkedIn post, assign 1-3 categories from this EXACT list:
{categories_str}

Post content:
{post_text[:3000]}

Rules:
- ONLY use categories from the list above
- Return 1-3 categories that best fit the content
- ONLY use "undefined" if NO other category fits at all. Never combine "undefined" with other categories.
- Return ONLY the category names, comma-separated, nothing else
"""
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a categorization assistant. Return only category names from the provided list, comma-separated."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50
        )
        raw_categories = response.choices[0].message.content.strip().lower()
        assigned = [cat.strip() for cat in raw_categories.split(",")]
        valid_categories = [cat for cat in assigned if cat in ALLOWED_CATEGORIES]
        if not valid_categories:
            return ["undefined"]
        return [cat for cat in valid_categories if cat != "undefined"] or ["undefined"]
    except Exception as e:
        print(f"Error assigning LinkedIn categories: {e}")
        return ["undefined"]


def assign_instagram_categories(metadata):
    """Use GPT-4o-mini to assign categories to an Instagram post based on its metadata"""
    try:
        categories_str = ", ".join(ALLOWED_CATEGORIES)
        # Build context from available metadata
        context_parts = []
        if metadata.get('title'):
            context_parts.append(f"Title: {metadata['title']}")
        if metadata.get('description'):
            context_parts.append(f"Description: {metadata['description'][:3000]}")
        if metadata.get('uploader'):
            context_parts.append(f"Uploader: {metadata['uploader']}")
        content_text = "\n".join(context_parts) if context_parts else "No content available"

        prompt = f"""Based on the following Instagram post, assign 1-3 categories from this EXACT list:
{categories_str}

Post content:
{content_text}

Rules:
- ONLY use categories from the list above
- Return 1-3 categories that best fit the content
- ONLY use "undefined" if NO other category fits at all. Never combine "undefined" with other categories.
- Return ONLY the category names, comma-separated, nothing else
"""
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a categorization assistant. Return only category names from the provided list, comma-separated."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50
        )
        raw_categories = response.choices[0].message.content.strip().lower()
        assigned = [cat.strip() for cat in raw_categories.split(",")]
        valid_categories = [cat for cat in assigned if cat in ALLOWED_CATEGORIES]
        if not valid_categories:
            return ["undefined"]
        return [cat for cat in valid_categories if cat != "undefined"] or ["undefined"]
    except Exception as e:
        print(f"Error assigning Instagram categories: {e}")
        return ["undefined"]


def create_linkedin_markdown(url, post_text, title, categories, slack_message_url,
                             extraction=None, author="", posted_at=""):
    """Create a markdown file for a LinkedIn post.

    `extraction` is the dict returned by extract_linkedin_tools_and_methods (may be None).
    Tools, methods, and project tags are written into YAML frontmatter as lists so
    Obsidian Dataview / search can find them when working on a specific project.
    """
    try:
        safe_title = sanitize_filename(title[:60])
        filename = f"LinkedIn - {safe_title}.md"
        filepath = LINKEDIN_DIR / filename

        counter = 1
        while filepath.exists():
            filename = f"LinkedIn - {safe_title}_{counter}.md"
            filepath = LINKEDIN_DIR / filename
            counter += 1

        extraction = extraction or {"tools": [], "methods": [], "projects_applicable_to": [], "summary": ""}
        tools = extraction.get("tools", [])
        methods = extraction.get("methods", [])
        projects = extraction.get("projects_applicable_to", [])
        summary = extraction.get("summary", "")

        tag_list = ["linkedin"] + categories
        tags_yaml = "[" + ", ".join(tag_list) + "]"
        tool_names_yaml = "[" + ", ".join(json.dumps(t.get("name", "")) for t in tools) + "]"
        method_names_yaml = "[" + ", ".join(json.dumps(m.get("name", "")) for m in methods) + "]"
        projects_yaml = "[" + ", ".join(json.dumps(p) for p in projects) + "]"

        # Body sections
        if tools:
            tools_section = "\n".join(
                f"- **{t.get('name','')}**"
                + (f" — [{t.get('url')}]({t.get('url')})" if t.get('url') else "")
                + (f": {t.get('purpose')}" if t.get('purpose') else "")
                for t in tools
            )
        else:
            tools_section = "_None extracted._"

        if methods:
            methods_section = "\n".join(
                f"- **{m.get('name','')}**"
                + (f": {m.get('description')}" if m.get('description') else "")
                for m in methods
            )
        else:
            methods_section = "_None extracted._"

        projects_section = ", ".join(f"`{p}`" for p in projects) if projects else "_None extracted._"
        author_line = f"**Author:** {author}\n" if author else ""
        posted_line = f"**Posted:** {posted_at}\n" if posted_at else ""

        markdown_content = f"""---
platform: "linkedin"
title: "{sanitize_frontmatter(title)}"
author: "{sanitize_frontmatter(author)}"
posted_at: "{sanitize_frontmatter(posted_at)}"
linkedin_url: "{url}"
slack_message_url: "{slack_message_url}"
tags: {tags_yaml}
tools: {tool_names_yaml}
methods: {method_names_yaml}
projects: {projects_yaml}
---

# {title}

{author_line}{posted_line}**Tags:** {", ".join(tag_list)}
**LinkedIn:** [{url}]({url})
**Slack Reference:** [View in Slack]({slack_message_url})

---

## Summary

{summary or "_No summary generated._"}

## Tools

{tools_section}

## Methods

{methods_section}

## Applicable Projects

{projects_section}

---

## Original Post

{post_text}
"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        return str(filepath)

    except Exception as e:
        print(f"Error creating LinkedIn markdown: {e}")
        return None


def check_linkedin_already_processed(url):
    """Check if a LinkedIn post has already been processed. Returns (is_duplicate, filepath, title)."""
    try:
        for md_file in DOWNLOAD_DIR.rglob("*.md"):
            try:
                content = md_file.read_text(encoding='utf-8')
                if url in content:
                    # Extract title from frontmatter
                    title_match = re.search(r'^title:\s*"(.+)"', content, re.MULTILINE)
                    title = title_match.group(1) if title_match else md_file.stem
                    return True, str(md_file), title
            except Exception:
                continue
    except Exception as e:
        print(f"Error checking LinkedIn duplicates: {e}")
    return False, None, None


def fetch_linkedin_post_via_apify(url):
    """Fetch a LinkedIn post's text and metadata via Apify.

    Returns dict with keys: text, author, posted_at, raw (the first dataset item).
    Returns None on failure or when APIFY_API_TOKEN is not configured.
    """
    if not APIFY_API_TOKEN:
        print("APIFY_API_TOKEN not set; skipping Apify fetch")
        return None

    actor_path = APIFY_LINKEDIN_ACTOR.replace("/", "~")
    api_url = f"https://api.apify.com/v2/acts/{actor_path}/run-sync-get-dataset-items"
    body = {APIFY_LINKEDIN_INPUT_KEY: [url]}

    try:
        resp = requests.post(
            api_url,
            params={"token": APIFY_API_TOKEN},
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        items = resp.json()
    except Exception as e:
        print(f"Apify fetch failed for {url}: {e}")
        return None

    if not items:
        print(f"Apify returned no items for {url}")
        return None

    item = items[0]
    # Different actors expose the post text under different keys; try the common ones.
    text = (
        item.get("text")
        or item.get("postText")
        or item.get("content")
        or item.get("description")
        or item.get("commentary")
        or ""
    )
    author = (
        item.get("authorName")
        or item.get("author")
        or item.get("userName")
        or item.get("authorFullName")
        or ""
    )
    posted_at = (
        item.get("postedAt")
        or item.get("publishedAt")
        or item.get("date")
        or item.get("postedAtIso")
        or ""
    )

    if not text:
        print(f"Apify item had no recognizable text field. Keys present: {list(item.keys())}")
        return None

    return {"text": text, "author": author, "posted_at": posted_at, "raw": item}


def extract_linkedin_tools_and_methods(post_text):
    """Use GPT-4o to extract tools, methods, and project applicability from a LinkedIn post.

    Returns dict with keys: tools, methods, projects_applicable_to, summary.
    Each list item has the shape documented in the prompt schema.
    """
    empty = {"tools": [], "methods": [], "projects_applicable_to": [], "summary": ""}
    if not post_text or not post_text.strip():
        return empty

    schema_hint = """{
  "summary": "1-3 sentence plain-English summary of what the post is teaching",
  "tools": [
    {"name": "...", "url": "" , "purpose": "what it is used for in this post"}
  ],
  "methods": [
    {"name": "short name for the technique", "description": "1-2 sentences"}
  ],
  "projects_applicable_to": [
    "short tag describing a project type or scenario where these tools/methods apply"
  ]
}"""

    prompt = f"""You are extracting reusable knowledge from a LinkedIn post so it can be retrieved later when working on specific projects.

Return a JSON object exactly matching this schema:
{schema_hint}

Rules:
- Only include tools that are concretely named in the post (software, libraries, services, frameworks, products). Do NOT invent or infer.
- If a URL for a tool is not in the post, leave url as an empty string.
- Methods are reusable techniques, processes, frameworks, or workflows described in the post.
- projects_applicable_to should be 2-6 short lowercase tags (kebab-case ok) like "ai-agents", "lead-gen", "video-editing", "data-pipelines".
- If the post contains no tools or no methods, return empty arrays. Never fabricate.
- summary must be plain text, no markdown.

Post content:
\"\"\"
{post_text[:8000]}
\"\"\"
"""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You extract structured knowledge as strict JSON. Never add commentary."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            max_tokens=1200,
        )
        data = json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Error extracting LinkedIn tools/methods: {e}")
        return empty

    return {
        "tools": data.get("tools", []) or [],
        "methods": data.get("methods", []) or [],
        "projects_applicable_to": data.get("projects_applicable_to", []) or [],
        "summary": data.get("summary", "") or "",
    }


def download_instagram_via_rapidapi(url, post_id):
    """Download Instagram content via RapidAPI. Returns (success, filepath, metadata, error)"""
    if not RAPIDAPI_KEY:
        return False, None, None, "RAPIDAPI_KEY not set"

    try:
        api_url = f"https://{RAPIDAPI_IG_HOST}/instagram/v3/media/post/details"
        params = {"shortcode": post_id, "renderableFormats": "720p,highres"}
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-host": RAPIDAPI_IG_HOST,
            "x-rapidapi-key": RAPIDAPI_KEY,
        }

        resp = requests.get(api_url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("error"):
            return False, None, None, f"API error: {data['error']}"

        contents = data.get("contents", [])
        if not contents:
            return False, None, None, "No contents in API response"

        item = contents[0]

        # Find best video with audio
        video_url = None
        for v in item.get("videos", []):
            if v.get("metadata", {}).get("has_audio"):
                video_url = v["url"]
                break
        # Fallback to first video if none has audio
        if not video_url and item.get("videos"):
            video_url = item["videos"][0]["url"]

        # If no video, try images
        image_urls = [img.get("url") for img in item.get("images", []) if img.get("url")]

        if not video_url and not image_urls:
            return False, None, None, "No downloadable media found"

        # Build metadata from API response — metadata lives at top level, not inside contents
        api_meta = data.get("metadata", {})
        author = api_meta.get("author", {})
        additional = api_meta.get("additionalData", {})
        caption_edges = additional.get("edge_media_to_caption", {}).get("edges", [])
        caption_text = caption_edges[0]["node"]["text"] if caption_edges else api_meta.get("title", "")

        metadata = {
            "title": (caption_text or "")[:50] or f"Instagram_{post_id}",
            "uploader": author.get("full_name") or author.get("username") or "Unknown",
            "duration": additional.get("video_duration") or 0,
            "description": caption_text or "",
            "timestamp": additional.get("taken_at_timestamp"),
            "view_count": additional.get("video_view_count"),
            "like_count": additional.get("edge_media_preview_like", {}).get("count"),
            "media_type": "video" if video_url else "image",
        }

        if metadata["duration"]:
            mins = int(metadata["duration"]) // 60
            secs = int(metadata["duration"]) % 60
            metadata["duration_string"] = f"{mins}:{secs:02d}"
        else:
            metadata["duration_string"] = "N/A"

        safe_title = sanitize_filename(metadata["title"][:50])
        safe_uploader = sanitize_filename(metadata["uploader"])

        if video_url:
            # Download the video
            print(f"Downloading Instagram video via RapidAPI: {post_id}")
            dl_resp = requests.get(video_url, timeout=120)
            dl_resp.raise_for_status()

            final_filename = f"{safe_uploader} - {safe_title}.mp4"
            final_path = ASSETS_DIR / final_filename
            counter = 1
            while final_path.exists():
                final_filename = f"{safe_uploader} - {safe_title}_{counter}.mp4"
                final_path = ASSETS_DIR / final_filename
                counter += 1

            with open(final_path, "wb") as f:
                f.write(dl_resp.content)

            return True, str(final_path), metadata, None
        else:
            # Download images
            print(f"Downloading Instagram images via RapidAPI: {post_id}")
            paths = []
            for i, img_url in enumerate(image_urls):
                dl_resp = requests.get(img_url, timeout=60)
                dl_resp.raise_for_status()
                suffix = f"_{i}" if len(image_urls) > 1 else ""
                ext = ".jpg"
                final_filename = f"{safe_uploader} - {safe_title}{suffix}{ext}"
                final_path = ASSETS_DIR / final_filename
                with open(final_path, "wb") as f:
                    f.write(dl_resp.content)
                paths.append(str(final_path))

            return True, paths if len(paths) > 1 else paths[0], metadata, None

    except Exception as e:
        print(f"RapidAPI Instagram download failed: {e}")
        return False, None, None, str(e)


def download_instagram_content(url, post_id):
    """Download Instagram content via RapidAPI. Returns (success, filepath, metadata, error)"""
    return download_instagram_via_rapidapi(url, post_id)


def download_instagram_images(url, post_id):
    """Download Instagram image/carousel posts using instaloader. Returns (success, filepaths, metadata, error)"""
    try:
        L = instaloader.Instaloader(
            download_videos=True,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            dirname_pattern=tempfile.mkdtemp(),
        )

        # Load Instagram session from Chrome cookies for authenticated access
        try:
            import browser_cookie3
            cj = browser_cookie3.chrome(domain_name='.instagram.com')
            session_id = None
            for c in cj:
                if c.name == 'sessionid':
                    session_id = c.value
                    break
            if session_id:
                L.context._session.cookies.set('sessionid', session_id, domain='.instagram.com')
                print("Loaded Instagram session from Chrome cookies")
        except Exception as e:
            print(f"Could not load Chrome cookies (continuing without auth): {e}")

        post = instaloader.Post.from_shortcode(L.context, post_id)

        metadata = {
            'title': (post.caption or '')[:50] or f'Instagram_{post_id}',
            'uploader': post.owner_username or 'Unknown',
            'duration': 0,
            'description': post.caption or '',
            'timestamp': post.date_utc.isoformat() if post.date_utc else None,
            'view_count': None,
            'like_count': post.likes,
            'media_type': 'carousel' if post.typename == 'GraphSidecar' else 'image',
        }
        metadata['duration_string'] = 'N/A'

        # Download the post
        L.download_post(post, target=Path(L.dirname_pattern))

        # Find downloaded image files
        temp_dir = L.dirname_pattern
        downloaded_files = [
            os.path.join(temp_dir, f) for f in os.listdir(temp_dir)
            if f.endswith(('.jpg', '.jpeg', '.png', '.webp', '.mp4', '.webm', '.mov'))
        ]

        if not downloaded_files:
            return False, None, metadata, "No images downloaded"

        import shutil
        safe_uploader = sanitize_filename(metadata['uploader'])
        safe_title = sanitize_filename(metadata['title'][:50])
        saved_paths = []

        for i, src_file in enumerate(sorted(downloaded_files)):
            ext = os.path.splitext(src_file)[1]
            if len(downloaded_files) > 1:
                final_filename = f"{safe_uploader} - {safe_title}_{i+1}{ext}"
            else:
                final_filename = f"{safe_uploader} - {safe_title}{ext}"
            final_path = ASSETS_DIR / final_filename

            counter = 1
            base_name = final_filename.rsplit('.', 1)[0]
            while final_path.exists():
                final_filename = f"{base_name}_dup{counter}{ext}"
                final_path = ASSETS_DIR / final_filename
                counter += 1

            shutil.move(src_file, final_path)
            saved_paths.append(str(final_path))

        # Clean up temp dir
        try:
            import shutil as sh
            sh.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

        # Return first path for single images, or join for carousels
        # We store all paths joined by | for the markdown to embed all
        media_path = "|".join(saved_paths)
        return True, media_path, metadata, None

    except Exception as e:
        print(f"Error downloading Instagram images: {e}")
        return False, None, None, str(e)


def create_instagram_markdown(post_id, url, metadata, media_path, slack_message_url, forwarder_text="", original_text="", categories=None):
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

        # Build media embed section (handles single files and carousel with | separator)
        if media_path:
            media_files = media_path.split("|")
            media_embeds = [f"![[{os.path.basename(f).replace(chr(10), ' ').replace(chr(13), ' ')}]]" for f in media_files]
            media_embed = "\n\n".join(media_embeds)
        else:
            media_embed = "Media not available."

        safe_title = sanitize_frontmatter(metadata['title'])
        safe_uploader = sanitize_frontmatter(metadata['uploader'])

        context_section = build_context_section(forwarder_text=forwarder_text, original_text=original_text)

        tags_str = ", ".join(["instagram"] + (categories or []))

        markdown_content = f"""---
platform: "instagram"
uploader: "{safe_uploader}"
title: "{safe_title}"
instagram_url: "{url}"
slack_message_url: "{slack_message_url}"
duration: "{metadata['duration_string']}"
media_type: "{metadata['media_type']}"
tags: [{tags_str}]
---

# {safe_title}

**Uploader:** {metadata['uploader']}
**Duration:** {metadata['duration_string']}
**Type:** {metadata['media_type']}
**Tags:** {tags_str}
**Instagram:** [{url}]({url})
**Slack Reference:** [View in Slack]({slack_message_url})

---

{context_section}{media_embed}

---

## Description

{metadata['description'] or 'No description available.'}
"""

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

        return str(filepath)

    except Exception as e:
        print(f"Error creating Instagram markdown: {e}")
        return None


def _yt_dlp_base_opts():
    """Common yt-dlp options for YouTube anti-bot bypass."""
    return {
        'cookiefile': str(Path(__file__).parent / 'www.instagram.com_cookies.txt'),
        'quiet': True,
        'no_warnings': True,
        'remote_components': ['ejs:github'],
    }


def get_video_metadata(video_id):
    """Fetch video metadata using yt-dlp (with pytubefix fallback)"""
    # Primary: yt-dlp with cookies
    try:
        ydl_opts = {
            **_yt_dlp_base_opts(),
            'skip_download': True,
            'ignore_no_formats_error': True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            if info:
                duration_seconds = info.get('duration') or 0
                minutes = duration_seconds // 60
                seconds = duration_seconds % 60
                return {
                    'title': info.get('title') or 'Unknown Title',
                    'channel': info.get('uploader') or info.get('channel') or 'Unknown Channel',
                    'duration': duration_seconds,
                    'duration_string': f"{minutes}:{seconds:02d}",
                    'view_count': info.get('view_count') or 0,
                    'tags': info.get('tags') or [],
                    'description': info.get('description') or '',
                    'upload_date': info.get('upload_date') or 'Unknown',
                }
    except Exception as e:
        print(f"yt-dlp metadata failed: {e}, trying pytubefix fallback...")

    # Fallback: pytubefix
    try:
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}", client='WEB')

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
    return re.sub(r'[<>:"/\\|?*#]', '', filename).replace('\n', ' ').replace('\r', ' ').strip()


def sanitize_frontmatter(value):
    """Sanitize a string for use in YAML frontmatter"""
    return value.replace('\n', ' ').replace('\r', ' ').replace('"', '\\"').strip()


def _clean_slack_text(text):
    """Strip URLs and Slack formatting from a message, return cleaned text or empty string."""
    cleaned = re.sub(r'<https?://[^>|]+(?:\|[^>]*)?>',  '', text)
    cleaned = re.sub(r'https?://\S+', '', cleaned)
    return cleaned.strip()


def build_context_section(forwarder_text="", original_text=""):
    """Build a ## Context section from the forwarder's note and/or the original message text."""
    forwarder_clean = _clean_slack_text(forwarder_text) if forwarder_text else ""
    original_clean = _clean_slack_text(original_text) if original_text else ""

    # Don't repeat if they're the same
    if forwarder_clean and original_clean and forwarder_clean == original_clean:
        forwarder_clean = ""

    parts = []
    if original_clean:
        parts.append(original_clean)
    if forwarder_clean:
        parts.append(f"*Note:* {forwarder_clean}")

    if not parts:
        return ""

    body = "\n\n".join(parts)
    return f"""## Context

{body}

---

"""


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
    """Download audio from YouTube video for transcription using yt-dlp (with pytubefix fallback)"""
    temp_dir = tempfile.mkdtemp()

    # Primary method: yt-dlp (more reliable against YouTube protections)
    try:
        print("Downloading audio with yt-dlp...")
        ydl_opts = {
            **_yt_dlp_base_opts(),
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)

        # Find the downloaded file
        for f in os.listdir(temp_dir):
            output_file = os.path.join(temp_dir, f)
            is_valid, message = verify_audio_file(output_file)
            if is_valid:
                print(f"Downloaded audio to: {output_file}")
                print(f"Verification: {message}")
                return output_file
            else:
                print(f"yt-dlp download verification failed: {message}")

    except Exception as e:
        print(f"yt-dlp download failed: {e}, trying pytubefix fallback...")

    # Fallback method: pytubefix
    try:
        yt = YouTube(f"https://www.youtube.com/watch?v={video_id}", client='WEB')
        audio_streams = yt.streams.filter(only_audio=True).order_by('abr').desc()
        progressive_streams = yt.streams.filter(progressive=True).order_by('resolution').desc()
        all_streams = list(audio_streams) + list(progressive_streams)

        for i, stream in enumerate(all_streams[:3]):
            try:
                print(f"Trying pytubefix stream {i+1}: {stream.mime_type}, {getattr(stream, 'abr', 'N/A')} bitrate")
                output_file = stream.download(output_path=temp_dir)
                is_valid, message = verify_audio_file(output_file)
                if is_valid:
                    print(f"Downloaded audio to: {output_file}")
                    print(f"Verification: {message}")
                    return output_file
                else:
                    print(f"Download verification failed: {message}")
                    try:
                        os.remove(output_file)
                    except:
                        pass
            except Exception as stream_error:
                print(f"Pytubefix stream {i+1} failed: {stream_error}")
                continue

    except Exception as e:
        print(f"Pytubefix fallback also failed: {e}")

    print("All audio download attempts failed")
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

        # Skip download if video already exists
        existing_path = YOUTUBE_VIDEO_DIR / f"{base_filename}.mp4"
        if existing_path.exists() and existing_path.stat().st_size > 1024:
            print(f"Video already exists: {existing_path}")
            return True, str(existing_path)
        for f in YOUTUBE_VIDEO_DIR.iterdir():
            if f.stem == base_filename and f.suffix in ['.mp4', '.mkv', '.webm']:
                print(f"Video already exists: {f}")
                return True, str(f)

        ydl_opts = {
            **_yt_dlp_base_opts(),
            'format': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best',
            'merge_output_format': 'mp4',
            'outtmpl': os.path.join(str(YOUTUBE_VIDEO_DIR), f'{base_filename}.%(ext)s'),
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
- ONLY use "undefined" if NO other category fits at all. Never combine "undefined" with other categories.
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
            return ["undefined"]
        return [cat for cat in valid_categories if cat != "undefined"] or ["undefined"]
    except Exception as e:
        print(f"Error assigning categories: {e}")
        return ["undefined"]

def generate_summary_and_toc(transcript_text, metadata):
    """Use GPT to generate a summary and table of contents from the transcript"""
    try:
        prompt = f"""Based on the following transcript from a YouTube video titled "{metadata['title']}" by {metadata['channel']}, please provide:

1. A TABLE OF CONTENTS listing the main sections/topics covered, with approximate timestamps from the transcript
2. A comprehensive SUMMARY of the video (2-3 paragraphs)

Transcript:
{transcript_text[:15000]}

Please format your response EXACTLY as shown below. Each TOC entry must be a plain text bullet with a timestamp in [MM:SS] format followed by the topic name. Do NOT use markdown links or anchor tags.

## Table of Contents
- [00:00] Introduction and opening remarks
- [03:45] Second topic discussed
- [12:30] Third topic discussed
...

## Summary
Your summary here.
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
    """Format the Whisper transcript with timestamps every 60 seconds.
    Merges short fragments into sentences while preserving natural sentence breaks."""
    if not transcript or not hasattr(transcript, 'segments'):
        return "Transcript not available."

    paragraphs = []
    current_sentences = []
    current_sentence_parts = []
    last_timestamp_minute = -1
    timestamp_prefix = ""

    def flush_sentence():
        """Join accumulated fragments into one sentence."""
        if current_sentence_parts:
            current_sentences.append(" ".join(current_sentence_parts))
            current_sentence_parts.clear()

    def flush_paragraph():
        """Join sentences into a paragraph with the timestamp."""
        flush_sentence()
        if current_sentences:
            text = " ".join(current_sentences)
            paragraphs.append(f"{timestamp_prefix} {text}" if timestamp_prefix else text)
            current_sentences.clear()

    for segment in transcript.segments:
        start_time = getattr(segment, 'start', 0)
        text = getattr(segment, 'text', '').strip()

        if not text:
            continue

        current_minute = int(start_time // 60)

        # New 60-second interval: start a new paragraph
        if current_minute > last_timestamp_minute:
            flush_paragraph()
            minutes = int(start_time // 60)
            seconds = int(start_time % 60)
            timestamp_prefix = f"**[{minutes:02d}:{seconds:02d}]**"
            last_timestamp_minute = current_minute

        current_sentence_parts.append(text)

        # If the segment ends with sentence-ending punctuation, close the sentence
        if text and text[-1] in '.!?':
            flush_sentence()

    flush_paragraph()

    return "\n\n".join(paragraphs)

def create_markdown_file(video_id, metadata, transcript, summary_and_toc, slack_message_url, categories, video_path=None, slack_message_text="", original_message_text=""):
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

        # Build context section from Slack messages
        context_section = build_context_section(forwarder_text=slack_message_text, original_text=original_message_text)

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

{context_section}{summary_and_toc}

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
    Returns (text_to_search, slack_url, original_message_text) tuple.
    original_message_text is the text from the forwarded/original message (if different from the forwarder's text).
    """
    text = event.get('text', '')
    channel = event.get('channel')
    ts = event.get('ts')

    attachments = event.get('attachments', [])
    slack_link_pattern = r'https://([a-zA-Z0-9-]+)\.slack\.com/archives/([A-Z0-9]+)/p(\d+)'
    slack_link_match = re.search(slack_link_pattern, text)

    original_url = None
    search_text = text
    original_message_text = ""

    # Method 1: Check attachments for forwarded messages
    for attachment in attachments:
        if attachment.get('is_msg_unfurl'):
            original_url = attachment.get('original_url') or attachment.get('from_url')
            forwarded_text = attachment.get('text') or attachment.get('fallback') or ''
            if forwarded_text:
                search_text = forwarded_text
                original_message_text = forwarded_text
            break

        if attachment.get('channel_id') and attachment.get('ts'):
            orig_channel = attachment.get('channel_id')
            orig_ts = attachment.get('ts')
            original_url = get_slack_message_url(orig_channel, orig_ts)
            if attachment.get('text'):
                search_text = attachment.get('text')
                original_message_text = attachment.get('text')
            break

        if attachment.get('text') and extract_video_id(attachment.get('text', '')):
            search_text = attachment.get('text')
            original_message_text = attachment.get('text')
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
                    original_message_text = orig_message.get('text')
                for att in orig_message.get('attachments', []):
                    if att.get('text') and extract_video_id(att.get('text', '')):
                        search_text = att.get('text')
                        original_message_text = att.get('text')
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

    return search_text, original_url, original_message_text


@app.action("retry_failed_items")
def handle_retry_button(ack, body, client):
    """Handle the retry button click from the daily digest."""
    ack()

    channel = body['channel']['id']
    user = body['user']['id']

    # Get retry data from button value
    try:
        retry_data = json.loads(body['actions'][0]['value'])
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Error parsing retry data: {e}")
        client.chat_postMessage(
            channel=channel,
            text="❌ Error: Could not parse retry data."
        )
        return

    if not retry_data:
        client.chat_postMessage(
            channel=channel,
            text="No items to retry."
        )
        return

    # Notify user that retry is starting
    client.chat_postMessage(
        channel=channel,
        text=f"🔄 Retrying {len(retry_data)} failed item(s)..."
    )

    # Process each failed item, tracking results
    succeeded = []
    still_failed = []

    for item in retry_data:
        platform = item.get('p', '')
        url = item.get('u', '')
        slack_message_url = item.get('s', '')

        if not url:
            continue

        try:
            if platform == 'y':  # YouTube
                video_id = extract_video_id(url)
                if video_id:
                    print(f"Retrying YouTube: {video_id}")
                    # Snapshot digest length before processing
                    with digest_lock:
                        before = len(daily_digest_videos)
                    process_youtube_video(video_id, channel, slack_message_url)
                    # Check if the item that was just added succeeded or failed
                    with digest_lock:
                        if len(daily_digest_videos) > before:
                            entry = daily_digest_videos[-1]
                            if entry.get('success'):
                                succeeded.append(entry.get('title', video_id))
                            else:
                                still_failed.append((entry.get('title', video_id), entry.get('error', 'Unknown error')))
                        else:
                            still_failed.append((video_id, 'No result recorded'))
            elif platform == 'i':  # Instagram
                print(f"Retrying Instagram: {url}")
                with digest_lock:
                    before = len(daily_digest_videos)
                process_instagram_content(url, channel, slack_message_url)
                with digest_lock:
                    if len(daily_digest_videos) > before:
                        entry = daily_digest_videos[-1]
                        if entry.get('success'):
                            succeeded.append(entry.get('title', url))
                        else:
                            still_failed.append((entry.get('title', url), entry.get('error', 'Unknown error')))
                    else:
                        still_failed.append((url, 'No result recorded'))
            elif platform == 'l':  # LinkedIn
                post_text = item.get('t', '')
                if post_text:
                    print(f"Retrying LinkedIn: {url}")
                    process_linkedin_post(url, post_text, channel, slack_message_url)
                    succeeded.append(url)
        except Exception as e:
            print(f"Error retrying {platform} item {url}: {e}")
            still_failed.append((url, str(e)))

    # Build summary message
    lines = []
    if succeeded:
        lines.append(f"*{len(succeeded)} succeeded:*")
        for title in succeeded:
            lines.append(f"  • {title}")
    if still_failed:
        lines.append(f"\n*{len(still_failed)} still failed:*")
        for title, error in still_failed:
            lines.append(f"  • {title}: {error}")

    if not succeeded and not still_failed:
        summary = "No items were retried."
    elif still_failed and not succeeded:
        summary = f"❌ All {len(still_failed)} item(s) failed again.\n" + "\n".join(lines)
    elif succeeded and not still_failed:
        summary = f"✅ All {len(succeeded)} item(s) succeeded!\n" + "\n".join(lines)
    else:
        summary = f"Retry complete: {len(succeeded)} succeeded, {len(still_failed)} still failed.\n" + "\n".join(lines)

    client.chat_postMessage(channel=channel, text=summary)


@app.event({"type": "message", "subtype": "file_share"})
def handle_file_share(event, say, client):
    """Route file_share messages to the main handler."""
    print(f"[FILE_SHARE] event received, keys={list(event.keys())}")
    handle_message(event, say, client)


@app.event("file_shared")
def handle_file_shared_events(body, logger, client):
    """Handle file_shared events (Slack v2 upload API)."""
    event = body.get('event', {})
    print(f"[FILE_SHARED] event={event}")
    file_id = event.get('file_id')
    if not file_id:
        return
    try:
        file_info = client.files_info(file=file_id)
        file_data = file_info.get('file', {})
        mimetype = file_data.get('mimetype', '')
        name = file_data.get('name', '')
        print(f"[FILE_SHARED] file={name} mime={mimetype}")

        if not (mimetype.startswith('image/') or Path(name).suffix.lower() in IMAGE_EXTENSIONS):
            print(f"[FILE_SHARED] Not an image, skipping")
            return

        # Build a synthetic event for process_slack_images
        channels = file_data.get('channels', [])
        groups = file_data.get('groups', [])
        ims = file_data.get('ims', [])
        channel = (channels or groups or ims or [None])[0] or event.get('channel_id', '')
        ts = file_data.get('timestamp', '')
        slack_message_url = get_slack_message_url(channel, str(ts)) if channel else ""

        # Check for accompanying message text from shares
        shares = file_data.get('shares', {})
        message_text = ""
        for share_type in shares.values():
            for chan_shares in share_type.values():
                for share in chan_shares:
                    if share.get('ts'):
                        # Fetch the actual message to get any text
                        try:
                            result = client.conversations_history(
                                channel=channel,
                                latest=share['ts'],
                                limit=1,
                                inclusive=True
                            )
                            if result.get('messages'):
                                message_text = result['messages'][0].get('text', '')
                                slack_message_url = get_slack_message_url(channel, share['ts'])
                        except Exception:
                            pass
                        break
                break
            break

        synthetic_event = {
            'channel': channel,
            'ts': str(ts),
            'text': message_text,
            'files': [file_data],
        }

        # Check if any platform URLs in the message — if so, skip image processing
        all_text = message_text
        if extract_video_id(all_text) or get_instagram_url(all_text) or extract_linkedin_url(all_text):
            print(f"[FILE_SHARED] Message contains platform URL, skipping image-only processing")
            return

        # Check for generic resource URLs
        resource_urls = extract_generic_urls(all_text)
        if resource_urls:
            process_resource_links(resource_urls, all_text, channel, slack_message_url)

        process_slack_images(synthetic_event, channel, slack_message_url)
        print(f"[FILE_SHARED] Processing complete")
    except Exception as e:
        print(f"[FILE_SHARED] Error: {e}")
        import traceback
        traceback.print_exc()


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
    search_text, slack_message_url, main_text_from_forward = extract_original_message_info(event, client)
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

        process_youtube_video(video_id, channel, slack_message_url, slack_message_text=main_text, original_message_text=main_text_from_forward)
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

        process_instagram_content(instagram_url, channel, slack_message_url, forwarder_text=main_text, original_text=main_text_from_forward)
        return

    # Try to find LinkedIn post
    linkedin_url = extract_linkedin_url(search_text) or extract_linkedin_url(main_text)
    if not linkedin_url:
        for attachment in event.get('attachments', []):
            for field in ['title_link', 'original_url', 'from_url', 'text']:
                content = attachment.get(field, '')
                if content:
                    linkedin_url = extract_linkedin_url(content)
                    if linkedin_url:
                        break
            if linkedin_url:
                break

    if linkedin_url:
        is_duplicate, existing_path, existing_title = check_linkedin_already_processed(linkedin_url)
        if is_duplicate:
            print(f"Duplicate LinkedIn post detected: {linkedin_url} -> {existing_path}")
            try:
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=ts,
                    text=f"This LinkedIn post has already been processed.\n\n"
                         f"*{existing_title}*\n"
                         f"File: `{existing_path}`"
                )
            except Exception as e:
                print(f"Error sending duplicate notification: {e}")
            return

        # Strip the LinkedIn URL itself from the message text so we don't pass the URL
        # back in as "post text" and confuse the title/extraction prompts.
        post_text_for_processing = re.sub(re.escape(linkedin_url), "", main_text or "").strip()
        if main_text_from_forward:
            post_text_for_processing = (post_text_for_processing + "\n" + main_text_from_forward).strip()

        process_linkedin_post(linkedin_url, post_text_for_processing, channel, slack_message_url)
        return

    # --- Generic Resource Links (checked before images — picks up GitHub repos, articles, etc.) ---
    all_text = f"{main_text} {search_text}"
    # Also pull URLs from attachment fields (unfurled links like LinkedIn, articles, etc.)
    for attachment in event.get('attachments', []):
        for field in ['title_link', 'original_url', 'from_url', 'text']:
            val = attachment.get(field, '')
            if val:
                all_text = f"{all_text} {val}"
    resource_urls = extract_generic_urls(all_text)
    if resource_urls:
        process_resource_links(resource_urls, main_text, channel, slack_message_url)
        # Don't return — message may also contain images to download

    # --- Static Image Detection (checked LAST — only if no platform URL matched) ---
    if event.get('files'):
        image_files = [f for f in event['files'] if f.get('mimetype', '').startswith('image/') or
                       Path(f.get('name', '')).suffix.lower() in IMAGE_EXTENSIONS]
        if image_files:
            process_slack_images(event, channel, slack_message_url)
            return


def process_youtube_video(video_id, channel, slack_message_url, slack_message_text="", original_message_text=""):
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
                'platform': 'youtube',
                'url': f"https://www.youtube.com/watch?v={video_id}",
                'slack_message_url': slack_message_url
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
    filepath = create_markdown_file(video_id, metadata, transcript, summary_and_toc, slack_message_url, categories, video_path, slack_message_text, original_message_text)

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
            'platform': 'youtube',
            'url': f"https://www.youtube.com/watch?v={video_id}",
            'slack_message_url': slack_message_url
        })

    if filepath:
        print(f"Successfully processed: {metadata['title']} -> {filepath}")
        if video_path:
            print(f"Video file: {video_path}")
    else:
        print(f"Failed to create file for: {metadata['title']}")


def process_instagram_content(instagram_url, channel, slack_message_url, forwarder_text="", original_text=""):
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
        try:
            client.chat_postMessage(
                channel=channel,
                text=f"Failed to download Instagram content: {instagram_url}\nError: {error or 'Download failed'}"
            )
        except Exception as e:
            print(f"Error sending failure notification: {e}")
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
                'platform': 'instagram',
                'url': instagram_url,
                'slack_message_url': slack_message_url
            })
        return

    # Assign categories using AI
    print("Assigning Instagram categories...")
    categories = assign_instagram_categories(metadata)
    print(f"Assigned categories: {categories}")

    # Create markdown file
    print("Creating Instagram markdown file...")
    md_filepath = create_instagram_markdown(post_id, instagram_url, metadata, media_path, slack_message_url, forwarder_text, original_text, categories=categories)

    # Track for daily digest
    with digest_lock:
        daily_digest_videos.append({
            'video_id': post_id,
            'title': metadata['title'][:50] if metadata['title'] else f"Instagram_{post_id}",
            'channel': metadata['uploader'],
            'duration': metadata['duration_string'],
            'categories': ['instagram'] + categories,
            'filepath': media_path,
            'success': media_path is not None,
            'error': None if media_path else "Failed to save media",
            'timestamp': datetime.now(),
            'platform': 'instagram',
            'url': instagram_url,
            'slack_message_url': slack_message_url
        })

    if media_path:
        print(f"Successfully downloaded: {metadata['title'][:50]} -> {media_path}")
    else:
        print(f"Failed to download Instagram content: {post_id}")


def process_linkedin_post(linkedin_url, post_text, channel, slack_message_url):
    """Process a LinkedIn post: fetch content if missing, extract tools/methods, write markdown.

    `post_text` is whatever text accompanied the URL in Slack. If empty, the bot tries
    to fetch the post via Apify. If that also fails, the post is logged as failed so
    the user can retry by re-sharing with the post text pasted.
    """
    global DIGEST_CHANNEL

    print(f"LinkedIn post detected: {linkedin_url}")
    print(f"Slack reference URL: {slack_message_url}")

    DIGEST_CHANNEL = channel

    post_text = strip_emojis(post_text or "")
    author = ""
    posted_at = ""

    # If we don't have substantive text from Slack, try Apify.
    # 80 chars is roughly the length where a message looks like just the URL + a comment,
    # below which extraction quality would be poor.
    if len(post_text.strip()) < 80:
        print("Post text not provided in Slack message; attempting Apify fetch...")
        fetched = fetch_linkedin_post_via_apify(linkedin_url)
        if fetched:
            post_text = strip_emojis(fetched["text"])
            author = fetched.get("author", "")
            posted_at = fetched.get("posted_at", "")
            print(f"Apify fetch succeeded: author={author!r}, {len(post_text)} chars")
        else:
            error_msg = (
                "No post text found in Slack message and Apify fetch unavailable/failed. "
                "Re-share the link with the post text pasted, or set APIFY_API_TOKEN."
            )
            print(f"Failed to obtain LinkedIn post content: {error_msg}")
            try:
                from slack_sdk import WebClient
                slack_client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
                slack_client.chat_postMessage(
                    channel=channel,
                    text=f"Couldn't process LinkedIn post: {linkedin_url}\n{error_msg}",
                )
            except Exception as notify_err:
                print(f"Could not notify channel: {notify_err}")
            with digest_lock:
                daily_digest_videos.append({
                    'video_id': linkedin_url,
                    'title': linkedin_url,
                    'channel': 'LinkedIn',
                    'duration': 'N/A',
                    'categories': [],
                    'filepath': None,
                    'success': False,
                    'error': error_msg,
                    'timestamp': datetime.now(),
                    'platform': 'linkedin',
                    'url': linkedin_url,
                    'slack_message_url': slack_message_url,
                    'post_text': post_text,
                })
            return

    # Generate title, categories, and structured tool/method extraction
    print("Generating LinkedIn title, categories, and tool/method extraction...")
    title = generate_linkedin_title(post_text)
    categories = assign_linkedin_categories(post_text)
    extraction = extract_linkedin_tools_and_methods(post_text)

    print(f"Title: {title}")
    print(f"Categories: {categories}")
    print(f"Tools: {[t.get('name') for t in extraction['tools']]}")
    print(f"Methods: {[m.get('name') for m in extraction['methods']]}")
    print(f"Projects: {extraction['projects_applicable_to']}")

    md_filepath = create_linkedin_markdown(
        linkedin_url, post_text, title, categories, slack_message_url,
        extraction=extraction, author=author, posted_at=posted_at,
    )

    with digest_lock:
        daily_digest_videos.append({
            'video_id': linkedin_url,
            'title': title,
            'channel': author or 'LinkedIn',
            'duration': 'N/A',
            'categories': categories,
            'filepath': md_filepath,
            'success': md_filepath is not None,
            'error': None if md_filepath else "Failed to create markdown",
            'timestamp': datetime.now(),
            'platform': 'linkedin',
            'url': linkedin_url,
            'slack_message_url': slack_message_url,
            'post_text': post_text,
            'tools': [t.get('name') for t in extraction['tools']],
            'methods': [m.get('name') for m in extraction['methods']],
            'projects': extraction['projects_applicable_to'],
        })

    # Add to resources.md
    update_resources_md(
        name=title,
        url=linkedin_url,
        description=(extraction.get("summary") or post_text)[:200],
        tags=["linkedin"] + categories,
        slack_message_url=slack_message_url,
        message_text=post_text,
    )

    if md_filepath:
        print(f"Successfully processed LinkedIn post: {title} -> {md_filepath}")
    else:
        print(f"Failed to process LinkedIn post: {linkedin_url}")


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
    - A markdown file exists with this post ID
    - AND a corresponding media file exists in assets/ or instagram/
    """
    # Search all markdown files in the vault
    for md_file in DOWNLOAD_DIR.rglob("*.md"):
        try:
            content = md_file.read_text(encoding='utf-8')
            if f'instagram.com/p/{post_id}' in content or \
               f'instagram.com/reel/{post_id}' in content or \
               f'instagram.com/tv/{post_id}' in content:
                title_match = re.search(r'title:\s*"([^"]+)"', content)
                title = title_match.group(1) if title_match else md_file.stem

                # Check for embedded media references (![[filename]])
                embed_matches = re.findall(r'!\[\[([^\]]+)\]\]', content)
                if embed_matches:
                    # Verify at least one embedded file exists in assets
                    for embed_name in embed_matches:
                        if (ASSETS_DIR / embed_name).exists():
                            return True, str(md_file), title

                # Also check for media files with matching stem in assets/ and instagram/
                for search_dir in [ASSETS_DIR, INSTAGRAM_DIR]:
                    media_files = [f for f in search_dir.glob(f"{md_file.stem}.*") if f.suffix != '.md']
                    if media_files:
                        return True, str(md_file), title

                # Markdown exists but no media found - allow reprocessing
                print(f"Found markdown for {post_id} but no media file - allowing reprocessing")
                return False, None, None
        except Exception:
            continue

    return False, None, None


def parse_frontmatter(content):
    """Extract frontmatter fields from markdown content."""
    frontmatter = {}
    fm_match = re.match(r'^---\s*\n(.*?)\n---', content, re.DOTALL)
    if not fm_match:
        return frontmatter
    fm_text = fm_match.group(1)
    for key in ['youtube_url', 'instagram_url', 'platform', 'title',
                'channel', 'uploader', 'slack_message_url']:
        match = re.search(rf'^{key}:\s*"?([^"\n]+)"?', fm_text, re.MULTILINE)
        if match:
            frontmatter[key] = match.group(1).strip()
    return frontmatter


def scan_vault_for_incomplete_files():
    """
    Scan all markdown files in the vault and identify incomplete ones.
    Returns a list of dicts with keys: filepath, platform, url, title, slack_message_url, issue
    """
    incomplete = []

    # Scan YouTube/LinkedIn files in root
    md_files = list(DOWNLOAD_DIR.glob("*.md"))
    # Scan Instagram files
    md_files.extend(INSTAGRAM_DIR.glob("*.md"))

    for md_file in md_files:
        try:
            content = md_file.read_text(encoding='utf-8')
            fm = parse_frontmatter(content)

            # Determine platform
            if 'youtube_url' in fm:
                platform = 'youtube'
                url = fm['youtube_url']
            elif fm.get('platform') == 'instagram' or 'instagram_url' in fm:
                platform = 'instagram'
                url = fm.get('instagram_url', '')
            else:
                continue  # LinkedIn or unknown - skip

            title = fm.get('title', md_file.stem)
            slack_url = fm.get('slack_message_url', '')

            if platform == 'youtube':
                missing = []

                # Check transcript
                if '## Full Transcript' in content:
                    transcript_section = content.split('## Full Transcript')[-1]
                    has_valid_transcript = bool(re.search(r'\[\d{2}:\d{2}\]', transcript_section))
                    if not has_valid_transcript:
                        missing.append('transcript')
                else:
                    missing.append('transcript')

                # Check summary
                if '## Summary' in content:
                    try:
                        s_start = content.index('## Summary')
                        s_end = content.index('---', s_start + 1) if '---' in content[s_start:] else len(content)
                        summary_section = content[s_start:s_end]
                        if len(summary_section) < 100 or 'Summary could not be generated' in summary_section:
                            missing.append('summary')
                    except ValueError:
                        missing.append('summary')
                else:
                    missing.append('summary')

                # Check TOC
                if '## Table of Contents' in content:
                    toc_start = content.index('## Table of Contents')
                    toc_end = content.index('## Summary', toc_start) if '## Summary' in content[toc_start:] else len(content)
                    toc_section = content[toc_start:toc_end]
                    if 'Content not available' in toc_section:
                        missing.append('TOC')
                else:
                    missing.append('TOC')

                if missing:
                    incomplete.append({
                        'filepath': str(md_file),
                        'platform': platform,
                        'url': url,
                        'title': title,
                        'slack_message_url': slack_url,
                        'issue': f"Missing: {', '.join(missing)}"
                    })

            elif platform == 'instagram':
                # Check for media embed
                embed_matches = re.findall(r'!\[\[([^\]]+)\]\]', content)
                if not embed_matches:
                    incomplete.append({
                        'filepath': str(md_file),
                        'platform': platform,
                        'url': url,
                        'title': title,
                        'slack_message_url': slack_url,
                        'issue': 'Missing: media embed'
                    })
                    continue

                # Check that embedded files exist on disk
                media_missing = True
                for embed_name in embed_matches:
                    if (ASSETS_DIR / embed_name).exists():
                        media_missing = False
                        break
                if media_missing:
                    incomplete.append({
                        'filepath': str(md_file),
                        'platform': platform,
                        'url': url,
                        'title': title,
                        'slack_message_url': slack_url,
                        'issue': 'Missing: media file not found on disk'
                    })

        except Exception as e:
            print(f"Error scanning {md_file}: {e}")
            continue

    return incomplete


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


def process_video_bulk(video_id, channel, ts, client, slack_message_url, slack_message_text=""):
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
        filepath = create_markdown_file(video_id, metadata, transcript, summary_and_toc, slack_message_url, categories, video_path, slack_message_text)

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


# Lock to prevent concurrent vault repairs
repair_vault_lock = threading.Lock()


def repair_vault_worker(respond, channel, client, scan_only=False):
    """Background worker for /repair-vault command."""
    if not repair_vault_lock.acquire(blocking=False):
        respond("A vault repair is already in progress. Please wait for it to finish.")
        return

    try:
        # Phase 1: Scan
        respond("Scanning vault for incomplete files...")
        incomplete_files = scan_vault_for_incomplete_files()

        if not incomplete_files:
            respond("Vault scan complete. All files have valid content!")
            return

        # Phase 2: Report
        youtube_incomplete = [f for f in incomplete_files if f['platform'] == 'youtube']
        instagram_incomplete = [f for f in incomplete_files if f['platform'] == 'instagram']

        report_lines = [
            f"*Vault Scan Complete*",
            f"Found *{len(incomplete_files)}* incomplete file(s):",
        ]

        if youtube_incomplete:
            report_lines.append(f"\n*YouTube ({len(youtube_incomplete)}):*")
            for f in youtube_incomplete:
                report_lines.append(f"  • {f['title']} | {f['issue']}")

        if instagram_incomplete:
            report_lines.append(f"\n*Instagram ({len(instagram_incomplete)}):*")
            for f in instagram_incomplete:
                report_lines.append(f"  • {f['title']} | {f['issue']}")

        respond('\n'.join(report_lines))

        if scan_only:
            respond("_Scan-only mode. No files were reprocessed. Run `/repair-vault` to fix them._")
            return

        # Phase 3: Reprocess
        reprocessable = [f for f in incomplete_files if f.get('url')]

        if not reprocessable:
            respond("No files can be automatically reprocessed (missing source URLs).")
            return

        respond(f"Starting reprocessing of {len(reprocessable)} file(s)...")

        successes = 0
        failures = 0

        for i, item in enumerate(reprocessable, 1):
            try:
                respond(f"Processing {i}/{len(reprocessable)}: _{item['title']}_...")

                if item['platform'] == 'youtube':
                    video_id = extract_video_id(item['url'])
                    if video_id:
                        # Delete old incomplete file so pipeline recreates it
                        try:
                            os.remove(item['filepath'])
                        except OSError:
                            pass
                        process_youtube_video(
                            video_id,
                            channel,
                            item.get('slack_message_url', '')
                        )
                        successes += 1
                    else:
                        failures += 1
                        respond(f"  Could not extract video ID from: {item['url']}")

                elif item['platform'] == 'instagram':
                    try:
                        os.remove(item['filepath'])
                    except OSError:
                        pass
                    process_instagram_content(
                        item['url'],
                        channel,
                        item.get('slack_message_url', '')
                    )
                    successes += 1

            except Exception as e:
                failures += 1
                print(f"Repair failed for {item['title']}: {e}")
                respond(f"  Failed: {item['title']} - {e}")

            # Rate limiting between items
            if i < len(reprocessable):
                time.sleep(2)

        respond(
            f"\n*Vault Repair Complete*\n"
            f"Successfully reprocessed: {successes}\n"
            f"Failed: {failures}\n"
            f"Files saved to: `{DOWNLOAD_DIR}`"
        )

    except Exception as e:
        print(f"Vault repair error: {e}")
        respond(f"Vault repair error: {e}")
    finally:
        repair_vault_lock.release()


@app.command("/repair-vault")
def handle_repair_vault(ack, command, client, respond):
    """
    Slash command to scan the vault for incomplete files and reprocess them.
    Usage:
        /repair-vault           (scan and reprocess)
        /repair-vault scan      (scan only, report without reprocessing)
    """
    ack()
    print(f"[repair-vault] Command received. Text: '{command.get('text', '')}' Channel: {command.get('channel_id')}")

    args = command.get('text', '').strip().lower()
    scan_only = args == 'scan'
    channel = command.get('channel_id')

    mode_label = "scan-only" if scan_only else "scan and repair"
    respond(
        f"Starting vault repair ({mode_label})...\n"
        f"Scanning all markdown files in `{DOWNLOAD_DIR}`\n"
        "This may take a while. I'll update you on progress."
    )

    thread = threading.Thread(
        target=repair_vault_worker,
        args=(respond, channel, client, scan_only),
        daemon=True
    )
    thread.start()


@app.command("/show-failures")
def handle_show_failures(ack, command, respond):
    """Show items that failed to process within the last N days.

    Usage:
        /show-failures           (last 7 days)
        /show-failures 30        (last 30 days)
        /show-failures 21d       (last 21 days)
        /show-failures all       (entire history)
    """
    ack()
    arg = command.get('text', '').strip().lower()

    if not arg:
        days = 7
    elif arg == 'all':
        days = None
    else:
        m = re.match(r'^(\d+)\s*d?$', arg)
        if not m:
            respond("Usage: `/show-failures [days|all]` — e.g. `/show-failures 30`")
            return
        days = int(m.group(1))

    since = (datetime.now() - timedelta(days=days)) if days is not None else None
    rows = read_processing_history(since=since)
    failed = [r for r in rows if not r.get('success')]

    window_label = f"last {days} day(s)" if days is not None else "all time"

    if not rows:
        respond(f"No processing history recorded yet (file: `{HISTORY_FILE}`). "
                f"Entries are written at the daily digest (22:00) — anything from before "
                f"this feature was added won't appear here.")
        return

    if not failed:
        respond(f"No failures in the {window_label}. ({len(rows)} item(s) succeeded.)")
        return

    # Group by platform for readability
    by_platform = {}
    for f in failed:
        by_platform.setdefault(f.get('platform', 'unknown'), []).append(f)

    lines = [f"*Failures in the {window_label}: {len(failed)} of {len(rows)} item(s)*\n"]
    for platform, items in sorted(by_platform.items()):
        lines.append(f"*{platform.title()} ({len(items)}):*")
        for f in items[:50]:  # cap per platform to keep message under Slack limits
            ts = f.get('timestamp', '')[:10]  # YYYY-MM-DD
            title = f.get('title') or f.get('url') or '(no title)'
            err = (f.get('error') or '').strip().replace('\n', ' ')
            if len(err) > 140:
                err = err[:137] + '...'
            url = f.get('url', '')
            lines.append(f"• `{ts}` *{title[:80]}*\n  {err or 'no error message'}\n  {url}")
        if len(items) > 50:
            lines.append(f"  _...and {len(items) - 50} more (truncated)_")
        lines.append("")

    respond("\n".join(lines))


def _existing_history_keys():
    """Set of (date, platform, title) tuples already in HISTORY_FILE for dedup."""
    keys = set()
    if not HISTORY_FILE.exists():
        return keys
    with history_lock:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                ts = row.get("timestamp", "")[:10]
                keys.add((ts, row.get("platform", ""), row.get("title", "")))
    return keys


def _backfill_worker(respond, client, channel, days, include_successes):
    """Walk the channel's digest messages, parse failures (and optionally successes),
    dedupe against HISTORY_FILE, and append new rows. Mirrors backfill_failures.py
    but runs in-process with the bot's Slack client.
    """
    try:
        oldest = (datetime.now() - timedelta(days=days)).timestamp() if days is not None else 0
        cursor = None
        digests = []
        while True:
            kwargs = {"channel": channel, "oldest": str(oldest), "limit": 200}
            if cursor:
                kwargs["cursor"] = cursor
            resp = client.conversations_history(**kwargs)
            for msg in resp.get("messages", []):
                if DIGEST_HEADER in (msg.get("text") or ""):
                    digests.append(msg)
            if not resp.get("has_more"):
                break
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

        seen = _existing_history_keys()
        new_entries = []
        for msg in digests:
            try:
                msg_dt = datetime.fromtimestamp(float(msg.get("ts", "0")))
            except Exception:
                continue
            timestamp = msg_dt.isoformat()
            date_key = timestamp[:10]

            failures, successes = parse_digest_text(msg.get("text", "") or "")
            failures = attach_urls_from_retry_button(failures, parse_retry_button(msg))

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

            if include_successes:
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

        if new_entries:
            with history_lock:
                HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(HISTORY_FILE, "a", encoding="utf-8") as f:
                    for e in new_entries:
                        f.write(json.dumps(e, ensure_ascii=False) + "\n")

        failed_count = sum(1 for e in new_entries if not e["success"])
        success_count = sum(1 for e in new_entries if e["success"])

        window = f"last {days} day(s)" if days is not None else "all available history"
        if not digests:
            respond(f"No digest messages found in this channel for the {window}. "
                    f"Make sure you ran this in the channel where daily digests are posted.")
            return

        if not new_entries:
            respond(f"Scanned {len(digests)} digest message(s) for the {window}. "
                    f"Nothing new to add — every entry was already in `processing_history.jsonl`.")
            return

        msg_lines = [
            f"*Backfill complete* — {window}",
            f"• Scanned {len(digests)} digest message(s)",
            f"• Added {failed_count} failure(s)",
        ]
        if include_successes:
            msg_lines.append(f"• Added {success_count} success(es)")
        msg_lines.append(f"\nRun `/show-failures {days}d` to view them." if days else "\nRun `/show-failures all` to view them.")
        respond("\n".join(msg_lines))
    except Exception as e:
        print(f"[/backfill] Error: {e}")
        respond(f"Backfill failed: {e}")


@app.command("/backfill")
def handle_backfill(ack, command, client, respond):
    """Reconstruct processing_history.jsonl from past daily-digest messages.

    Usage:
        /backfill                       (last 21 days, current channel, failures only)
        /backfill 30                    (last 30 days)
        /backfill 21d                   (last 21 days)
        /backfill all                   (entire channel history)
        /backfill 21 C0A99TH4Y2V        (specific channel id)
        /backfill 21 successes          (also include successes)
        /backfill 21 C0A99TH4Y2V successes
    """
    ack()

    args = (command.get("text") or "").strip().split()
    days = 21
    channel = command.get("channel_id")
    include_successes = False

    for token_arg in args:
        t = token_arg.strip().lower()
        if not t:
            continue
        if t == "all":
            days = None
        elif t in ("successes", "with-successes", "+successes"):
            include_successes = True
        elif re.match(r"^\d+\s*d?$", t):
            m = re.match(r"^(\d+)", t)
            days = int(m.group(1))
        elif re.match(r"^[CGD][A-Z0-9]{8,}$", token_arg.strip()):
            channel = token_arg.strip()
        else:
            respond(f"Couldn't parse argument `{token_arg}`. "
                    f"Usage: `/backfill [days|all] [channel_id] [successes]`")
            return

    if not channel:
        respond("No channel id available. Run this command in the channel where digests post, "
                "or pass an explicit channel id: `/backfill 21 C0A99TH4Y2V`.")
        return

    window = f"last {days} day(s)" if days is not None else "all available history"
    respond(f"Starting backfill for the {window} from <#{channel}>"
            f"{' (including successes)' if include_successes else ''}. "
            f"This may take a moment for large windows...")

    threading.Thread(
        target=_backfill_worker,
        args=(respond, client, channel, days, include_successes),
        daemon=True,
    ).start()


def _serialize_history_entry(entry):
    """Compact a daily-digest entry into a JSON-safe dict for processing_history.jsonl."""
    serialized = {
        'timestamp': entry.get('timestamp').isoformat() if isinstance(entry.get('timestamp'), datetime) else str(entry.get('timestamp', '')),
        'platform': entry.get('platform', 'unknown'),
        'title': entry.get('title', ''),
        'url': entry.get('url', ''),
        'channel': entry.get('channel', ''),
        'success': bool(entry.get('success')),
        'error': entry.get('error') or None,
        'filepath': entry.get('filepath') or None,
        'slack_message_url': entry.get('slack_message_url', ''),
        'categories': entry.get('categories', []) or [],
    }
    # LinkedIn-specific extras (kept short to keep the JSONL grep-friendly)
    if entry.get('platform') == 'linkedin':
        serialized['tools'] = entry.get('tools', []) or []
        serialized['methods'] = entry.get('methods', []) or []
        serialized['projects'] = entry.get('projects', []) or []
    return serialized


def append_processing_history(entries):
    """Append a batch of digest entries to the JSONL history file."""
    if not entries:
        return
    try:
        with history_lock:
            HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(HISTORY_FILE, 'a', encoding='utf-8') as f:
                for entry in entries:
                    f.write(json.dumps(_serialize_history_entry(entry), ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Error writing processing history: {e}")


def read_processing_history(since=None):
    """Read all history entries newer than `since` (datetime). Returns a list of dicts."""
    if not HISTORY_FILE.exists():
        return []
    rows = []
    try:
        with history_lock:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if since:
                        try:
                            ts = datetime.fromisoformat(row.get('timestamp', ''))
                        except Exception:
                            continue
                        if ts < since:
                            continue
                    rows.append(row)
    except Exception as e:
        print(f"Error reading processing history: {e}")
    return rows


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

    # Persist before posting so we never lose data even if Slack posting fails
    append_processing_history(videos)

    if not DIGEST_CHANNEL:
        print(f"[{datetime.now()}] No channel set for digest - content processed but not reported")
        return

    # Separate by platform and success
    successful = [v for v in videos if v['success']]
    failed = [v for v in videos if not v['success']]

    youtube_success = [v for v in successful if v.get('platform') == 'youtube']
    instagram_success = [v for v in successful if v.get('platform') == 'instagram']
    linkedin_success = [v for v in successful if v.get('platform') == 'linkedin']
    images_success = [v for v in successful if v.get('platform') == 'images']

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

    if linkedin_success:
        message_parts.append(f"\n*LinkedIn ({len(linkedin_success)}):*")
        for v in linkedin_success:
            tags = ', '.join(v['categories']) if v['categories'] else 'none'
            message_parts.append(
                f"• *{v['title']}*\n"
                f"  Tags: {tags}"
            )

    if images_success:
        message_parts.append(f"\n*Images ({len(images_success)}):*")
        for v in images_success:
            tags = ', '.join(v['categories']) if v['categories'] else 'none'
            message_parts.append(
                f"• *{v['title']}*\n"
                f"  Tags: {tags}"
            )

    if failed:
        message_parts.append(f"\n{len(failed)} item(s) failed:")
        for v in failed:
            platform = v.get('platform', 'unknown')
            message_parts.append(f"• [{platform}] {v['title']}: {v['error']}")

    message_parts.append(f"\nFiles saved to: `{DOWNLOAD_DIR}`")

    # Build retry data for failed items (compact format to fit in button value)
    retry_data = []
    for v in failed:
        item = {
            'p': v.get('platform', 'unknown')[:1],  # y/i/l for youtube/instagram/linkedin
            'u': v.get('url', ''),
            's': v.get('slack_message_url', '')
        }
        # For LinkedIn, include post_text (truncated if needed)
        if v.get('platform') == 'linkedin' and v.get('post_text'):
            item['t'] = v.get('post_text', '')[:500]
        retry_data.append(item)

    try:
        # Use blocks for interactive message if there are failures
        if failed and retry_data:
            # Encode retry data as JSON (must fit in 2000 chars)
            retry_json = json.dumps(retry_data)
            if len(retry_json) > 2000:
                # Truncate to fit - remove items from end
                while len(retry_json) > 1900 and retry_data:
                    retry_data.pop()
                    retry_json = json.dumps(retry_data)

            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": '\n'.join(message_parts)
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": f"🔄 Retry Failed ({len(retry_data)})",
                                "emoji": True
                            },
                            "style": "primary",
                            "action_id": "retry_failed_items",
                            "value": retry_json
                        }
                    ]
                }
            ]
            client.chat_postMessage(
                channel=DIGEST_CHANNEL,
                text='\n'.join(message_parts),
                blocks=blocks
            )
        else:
            client.chat_postMessage(
                channel=DIGEST_CHANNEL,
                text='\n'.join(message_parts)
            )
        print(f"[{datetime.now()}] Daily digest sent to {DIGEST_CHANNEL}")
    except Exception as e:
        print(f"[{datetime.now()}] Error sending daily digest: {e}")


def run_catchup_scan(client):
    """Scan configured channels for missed content and process any new items."""
    print(f"[{datetime.now()}] Running catchup scan for missed content...")

    processed_ids = get_processed_video_ids()
    oldest = datetime.now() - timedelta(hours=CATCHUP_LOOKBACK_HOURS)
    oldest_ts = str(oldest.timestamp())
    total_found = 0
    total_processed = 0

    for channel_id in CATCHUP_CHANNELS:
        try:
            # Fetch recent messages from the channel
            all_messages = []
            cursor = None
            while True:
                kwargs = {
                    "channel": channel_id,
                    "oldest": oldest_ts,
                    "limit": 200,
                }
                if cursor:
                    kwargs["cursor"] = cursor

                result = client.conversations_history(**kwargs)
                messages = result.get("messages", [])
                all_messages.extend(messages)

                cursor = result.get("response_metadata", {}).get("next_cursor")
                if not cursor:
                    break

            # Extract YouTube links and filter already processed
            youtube_videos = extract_youtube_links_from_messages(all_messages)
            new_videos = [(vid, info) for vid, info in youtube_videos if vid not in processed_ids]

            # Remove duplicates
            seen = set()
            unique_new = []
            for vid, info in new_videos:
                if vid not in seen:
                    seen.add(vid)
                    unique_new.append((vid, info))

            total_found += len(youtube_videos)

            if not unique_new:
                print(f"[{datetime.now()}] Catchup: No new videos in channel {channel_id}")
                continue

            print(f"[{datetime.now()}] Catchup: Found {len(unique_new)} unprocessed videos in channel {channel_id}")

            for vid, info in unique_new:
                ts = info.get('ts', '')
                slack_message_url = f"https://{SLACK_WORKSPACE}.slack.com/archives/{channel_id}/p{ts.replace('.', '')}"
                try:
                    success, filepath, error = process_video_bulk(vid, channel_id, ts, client, slack_message_url, slack_message_text=info.get('text', ''))
                    if success:
                        total_processed += 1
                        print(f"[{datetime.now()}] Catchup: Processed {vid} -> {filepath}")
                    else:
                        print(f"[{datetime.now()}] Catchup: Failed {vid}: {error}")
                except Exception as e:
                    print(f"[{datetime.now()}] Catchup: Error processing {vid}: {e}")
                time.sleep(2)  # Rate limit between videos

        except Exception as e:
            print(f"[{datetime.now()}] Catchup: Error scanning channel {channel_id}: {e}")

    print(f"[{datetime.now()}] Catchup scan complete: {total_found} total links found, {total_processed} newly processed")


def run_digest_scheduler(client):
    """Background thread that sends daily digest at DIGEST_HOUR."""
    print(f"[{datetime.now()}] Digest scheduler started - will send digest daily at {DIGEST_HOUR}:00")

    last_digest_date = None
    last_catchup_date = None

    # Run catchup on startup (handles wake from sleep)
    try:
        run_catchup_scan(client)
    except Exception as e:
        print(f"[{datetime.now()}] Startup catchup failed: {e}")

    while True:
        now = datetime.now()

        # Run catchup scan once daily, 1 hour before digest
        catchup_hour = (DIGEST_HOUR - 1) % 24
        if now.hour == catchup_hour and now.date() != last_catchup_date:
            print(f"[{datetime.now()}] Running scheduled catchup scan...")
            try:
                run_catchup_scan(client)
            except Exception as e:
                print(f"[{datetime.now()}] Scheduled catchup failed: {e}")
            last_catchup_date = now.date()

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
    print("Supported platforms: YouTube, Instagram, LinkedIn, Static Images")
    print("YouTube: Video download (1080p) + Whisper transcription + GPT summaries")
    print("Instagram: Download only (no transcription)")
    print(f"Daily digest will be sent at {DIGEST_HOUR}:00")
    print("Use /process-history to bulk process YouTube videos from conversation history")
    print("Use /repair-vault to scan and fix incomplete files in the knowledge base")

    # Start digest scheduler in background thread
    slack_client = app.client
    digest_thread = threading.Thread(target=run_digest_scheduler, args=(slack_client,), daemon=True)
    digest_thread.start()

    handler.start()
