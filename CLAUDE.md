# CLAUDE.md - Slack Knowledge Bot

## Project Overview

A Slack bot that builds an Obsidian-compatible knowledge base by processing content shared in Slack channels. It downloads, transcribes, summarizes, and categorizes content from YouTube, Instagram, LinkedIn, images, and generic links into structured markdown files.

## Repository Structure

```
slack-knowledge-bot/
├── youtube_knowledge_bot.py   # Main application (monolithic, ~3200 lines)
├── run_bot.sh                 # Startup script (loads .env, validates tokens)
├── requirements.txt           # Python dependencies
├── env.example                # Environment variable template
├── README.md                  # User documentation
└── CLAUDE.md                  # This file
```

## Tech Stack

- **Language**: Python 3.9+
- **Slack framework**: slack-bolt (Socket Mode)
- **AI**: OpenAI API (Whisper for transcription, GPT-4o for summaries, GPT-4o-mini for categorization)
- **Media**: yt-dlp (primary downloader), pytubefix (fallback), pydub (audio chunking), ffmpeg/ffprobe (required)
- **Optional**: instaloader + browser_cookie3 (Instagram fallback)

## Running the Bot

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment (copy env.example to .env and fill in values)
cp env.example .env

# Run
./run_bot.sh
```

**Required environment variables** (in `.env`):
- `SLACK_BOT_TOKEN` - Bot OAuth token (xoxb-...)
- `SLACK_APP_TOKEN` - App-level token for Socket Mode (xapp-...)
- `OPENAI_API_KEY` - OpenAI API key

## Architecture

### Single-file application

All logic lives in `youtube_knowledge_bot.py`. Key sections:

| Lines (approx) | Section |
|---|---|
| 1-100 | Imports, configuration constants, allowed categories |
| 100-500 | Utility functions (filename sanitization, frontmatter, URL extraction) |
| 500-800 | YouTube processing (download, transcribe, summarize, markdown generation) |
| 800-1100 | Instagram processing (yt-dlp + instaloader fallback) |
| 1100-1400 | LinkedIn processing |
| 1400-1700 | Image handling, resource link collection |
| 1700-1900 | Slack event handlers and message dispatcher |
| 1900-2700 | Content processing pipelines (YouTube, Instagram, LinkedIn) |
| 2700-3000 | Slash commands (/process-history, /repair-vault) |
| 3000-3230 | Daily digest, scheduling, app startup |

### Content processing flow

1. Slack message received → `handle_message()` dispatches by URL type
2. Duplicate check (URL + file existence)
3. Download/extract content
4. AI processing: transcription (Whisper) → summarization (GPT-4o) → categorization (GPT-4o-mini)
5. Generate markdown file with YAML frontmatter
6. Track in daily digest → send at 22:00

### Key patterns

- **Defensive error handling**: Every processing function wraps in try-except with Slack error reporting
- **Fallback chains**: yt-dlp → pytubefix for YouTube; yt-dlp → instaloader for Instagram
- **Thread safety**: Locks protect daily digest state; background thread for scheduling
- **Rate limiting**: Sleeps between API calls (0.5-2s)
- **Large file chunking**: Audio >20MB split into 10-minute segments for Whisper API

## Hardcoded Configuration

These values in the source code may need updating for different environments:

- `DOWNLOAD_DIR` (~line 28): Output directory path (currently `/Volumes/Knowledger/vault/Knowledger`)
- `SLACK_WORKSPACE` (~line 29): Workspace name for URL construction
- `DIGEST_HOUR` (~line 30): Daily digest send time (default: 22)
- `CATCHUP_CHANNELS` (~line 34): Channel IDs for history processing
- `ALLOWED_TAGS` (~line 81): 17 allowed categories for content classification

## Development Notes

- **No test suite** — there are no automated tests
- **No linting config** — no eslint, flake8, or similar configured
- **No build step** — runs directly as a Python script
- **Monolithic structure** — all functionality in one file; be careful with large changes
- **macOS-oriented** — `run_bot.sh` sets ffmpeg PATH for Homebrew on macOS

## Slack App Requirements

**Event subscriptions**: `message.channels`, `message.groups`, `message.im`, `message.mpim`, `file_shared`

**OAuth scopes**: `channels:history`, `channels:read`, `chat:write`, `files:read`, `groups:history`, `groups:read`, `im:history`, `im:read`, `mpim:history`, `mpim:read`, `commands`

## Output Format

All markdown files include YAML frontmatter with metadata (channel, title, URL, tags, etc.) and are organized for Obsidian compatibility. YouTube files include embedded video, AI-generated TOC with timestamps, summary, and full transcript. A `resources.md` file aggregates generic links.

## Common Tasks

- **Add a new content type**: Follow the pattern of existing processors — add URL regex detection, a processing function, duplicate check, markdown generator, and wire into `handle_message()`
- **Modify AI prompts**: Search for `openai.chat.completions.create` calls to find summarization and categorization prompts
- **Change categories**: Update the `ALLOWED_TAGS` list and the categorization prompt
- **Adjust transcript formatting**: Look for timestamp formatting logic near Whisper API calls
