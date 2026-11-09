#!/usr/bin/env python3
"""Audit knowledger channel: list URLs sent to Slack that have no corresponding md file."""

import os
import re
import time
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load .env
env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from slack_sdk import WebClient

CHANNEL_ID = "C0A99TH4Y2V"
VAULT = Path("/Volumes/Knowledger/vault/Knowledger")
RESOURCES_FILE = VAULT / "resources.md"

YOUTUBE_RE = re.compile(r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/shorts/)([a-zA-Z0-9_-]{11})')
INSTAGRAM_RE = re.compile(r'instagram\.com/(?:p|reel|reels)/([a-zA-Z0-9_-]+)')
LINKEDIN_RE = re.compile(r'linkedin\.com/(?:posts|feed/update|pulse)/[^\s>|)\]]+')
URL_RE = re.compile(r'https?://[^\s>|)\]]+')


def fetch_all_messages(client, channel):
    msgs = []
    cursor = None
    while True:
        kwargs = {'channel': channel, 'limit': 200}
        if cursor:
            kwargs['cursor'] = cursor
        resp = client.conversations_history(**kwargs)
        msgs.extend(resp.get('messages', []))
        cursor = resp.get('response_metadata', {}).get('next_cursor')
        if not cursor:
            break
        time.sleep(0.5)
    return msgs


import html as html_mod


def clean_url(url):
    """Strip trailing artifacts and normalize."""
    url = url.rstrip('.,;:!?\'")>]}')
    url = html_mod.unescape(url)
    return url


def extract_text_and_urls(msg):
    """Get all text content from a message including attachments and blocks."""
    parts = [msg.get('text', '') or '']
    for att in msg.get('attachments', []):
        for f in ('title_link', 'original_url', 'from_url', 'title', 'text', 'pretext', 'fallback'):
            v = att.get(f)
            if v:
                parts.append(str(v))
    for blk in msg.get('blocks', []):
        parts.append(str(blk))
    text = ' '.join(parts)
    urls = [clean_url(u) for u in URL_RE.findall(text)]
    return text, urls


def is_savable_linkedin(url):
    """Only LinkedIn post/feed/pulse URLs would be saved by the bot."""
    return bool(re.search(r'linkedin\.com/(?:posts|feed/update|pulse)/', url))


def vault_index():
    """Build a set of URL fragments that are already in the vault."""
    youtube_ids = set()
    instagram_ids = set()
    linkedin_urls = set()
    other_urls = set()

    # Scan all md files
    for md in VAULT.rglob("*.md"):
        try:
            content = md.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            continue
        for vid in YOUTUBE_RE.findall(content):
            youtube_ids.add(vid)
        for pid in INSTAGRAM_RE.findall(content):
            instagram_ids.add(pid)
        for lurl in LINKEDIN_RE.findall(content):
            linkedin_urls.add(clean_url(lurl).rstrip('/').lower().split('?')[0])
        # Generic URLs in resources.md
        if md.name == 'resources.md':
            for u in URL_RE.findall(content):
                u = clean_url(u)
                base = u.split('#')[0].split('?')[0].rstrip('/').lower()
                other_urls.add(base)

    return youtube_ids, instagram_ids, linkedin_urls, other_urls


def classify_url(url):
    if YOUTUBE_RE.search(url):
        m = YOUTUBE_RE.search(url)
        return ('youtube', m.group(1))
    if INSTAGRAM_RE.search(url):
        m = INSTAGRAM_RE.search(url)
        return ('instagram', m.group(1))
    if 'linkedin.com' in url:
        if not is_savable_linkedin(url):
            return ('skip', None)  # profile/search/hashtag — bot doesn't save these
        return ('linkedin', url.rstrip('/').lower().split('?')[0])
    if 'slack.com/archives/' in url:
        return ('skip', None)
    if 'lnkd.in' in url:
        return ('skip', None)  # LinkedIn shortlinks — not resolved
    # Normalize generic URL: drop fragment, query, trailing slash, lowercase host
    base = url.split('#')[0].split('?')[0].rstrip('/').lower()
    return ('other', base)


def dismiss_key(kind, key):
    """Canonical string used to dedupe + persist dismissals."""
    return f"{kind}:{key}"


def audit(dismissed=None):
    """Run the audit and return structured data.

    Args:
      dismissed: optional set of "kind:key" strings (see dismiss_key) to mark
        as status='dismissed' instead of 'missing'.

    Returns a dict with:
      vault_counts: {youtube, instagram, linkedin, other}
      items: list of {kind, url, slack_link, ts, status, dismiss_key} where
        status in {'vaulted', 'missing', 'dismissed'}.
      message_count: total Slack messages scanned
    """
    dismissed = set(dismissed or ())
    token = os.environ.get('SLACK_BOT_TOKEN')
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN not set")

    client = WebClient(token=token)
    messages = fetch_all_messages(client, CHANNEL_ID)
    yt_ids, ig_ids, li_urls, other_urls = vault_index()

    items = []
    seen_urls = set()

    for m in messages:
        if m.get('bot_id'):
            continue
        if m.get('subtype') in ('channel_join', 'channel_leave', 'message_changed', 'message_deleted'):
            continue
        _, urls = extract_text_and_urls(m)
        ts = m.get('ts', '')
        slack_link = f"https://cocoworkshq.slack.com/archives/{CHANNEL_ID}/p{ts.replace('.', '')}"

        for url in urls:
            kind, key = classify_url(url)
            if kind == 'skip' or key is None:
                continue
            dedup_key = (kind, key)
            if dedup_key in seen_urls:
                continue
            seen_urls.add(dedup_key)

            if kind == 'youtube':
                vaulted = key in yt_ids
            elif kind == 'instagram':
                vaulted = key in ig_ids
            elif kind == 'linkedin':
                vaulted = any(key.split('?')[0] in v or v in key for v in li_urls)
            elif kind == 'other':
                vaulted = any(key in v or v in key for v in other_urls)
            else:
                continue

            dk = dismiss_key(kind, key)
            if vaulted:
                status = 'vaulted'
            elif dk in dismissed:
                status = 'dismissed'
            else:
                status = 'missing'

            items.append({
                'kind': kind,
                'url': url,
                'slack_link': slack_link,
                'ts': ts,
                'status': status,
                'dismiss_key': dk,
            })

    return {
        'vault_counts': {
            'youtube': len(yt_ids),
            'instagram': len(ig_ids),
            'linkedin': len(li_urls),
            'other': len(other_urls),
        },
        'items': items,
        'message_count': len(messages),
    }


def main():
    try:
        print(f"Fetching messages from {CHANNEL_ID}...")
        result = audit()
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"Fetched {result['message_count']} messages\n")
    vc = result['vault_counts']
    print(f"Vault has: {vc['youtube']} YouTube, {vc['instagram']} Instagram, {vc['linkedin']} LinkedIn, {vc['other']} other URLs\n")

    missing = {'youtube': [], 'instagram': [], 'linkedin': [], 'other': []}
    for it in result['items']:
        if it['status'] == 'missing':
            missing[it['kind']].append((it['url'], it['slack_link'], it['ts']))

    print("=" * 70)
    print("MISSING (URLs in Slack but no corresponding md/resource entry)")
    print("=" * 70)
    for kind, msgs in missing.items():
        print(f"\n## {kind.upper()} — {len(msgs)} missing")
        for url, slack_link, _ in msgs[:50]:
            print(f"  - {url}")
            print(f"    via {slack_link}")
        if len(msgs) > 50:
            print(f"  ... and {len(msgs) - 50} more")

    total = sum(len(v) for v in missing.values())
    print(f"\n{'=' * 70}\nTOTAL MISSING: {total}\n{'=' * 70}")


if __name__ == '__main__':
    main()
