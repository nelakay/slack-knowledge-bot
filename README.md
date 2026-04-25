# YouTube, Instagram & LinkedIn Knowledge Bot for Slack

A Slack bot that automatically processes YouTube videos, Instagram content, and LinkedIn posts shared in your workspace, creating rich markdown files for your Obsidian knowledge base.

## Features

### YouTube Processing
- **Audio extraction** using pytubefix
- **Transcription** with OpenAI Whisper API
- **AI-generated summaries** and table of contents using GPT-4o
- **Auto-categorization** from predefined tags using GPT-4o-mini
- **Timestamped transcripts** with `[MM:SS]` markers every 60 seconds
- **Embedded video player** in markdown

### Instagram Processing
- **Video/image download** using yt-dlp
- **Metadata extraction** (uploader, description, duration)
- **No transcription** (designed for quick manual review)

### LinkedIn Processing
- **Auto-fetch** post content via Apify (when `APIFY_API_TOKEN` is set)
- **Manual-paste fallback** — paste the post text into the Slack message alongside the URL and the bot uses that
- **Tools/methods extraction** via GPT-4o into structured YAML frontmatter so Obsidian Dataview can find every post that mentions a given tool or method
- **Project tagging** — each post gets `projects:` tags (e.g. `ai-agents`, `lead-gen`) describing the kind of work it applies to, making retrieval per-project trivial

### Smart Features
- **Duplicate detection** - won't reprocess videos already in your vault
- **Content validation** - checks for valid transcripts, not just file existence
- **Daily digest** - single summary at 10 PM instead of per-video notifications
- **Download flag** - add "download" to your message to tag videos for manual download
- **Forwarded message support** - links back to original message, not the forward

## File Structure

```
Knowledger/
├── Channel Name - Video Title.md      # YouTube files
├── Another Channel - Another Video.md
├── instagram/
│   ├── Uploader - Post Title.mp4      # Instagram media
│   └── Uploader - Post Title.md       # Instagram metadata
└── linkedin/
    └── LinkedIn - Post Title.md       # LinkedIn post + extracted tools/methods
```

### LinkedIn markdown example

```markdown
---
platform: "linkedin"
title: "Building AI agents with LangGraph"
author: "Jane Doe"
linkedin_url: "https://www.linkedin.com/posts/..."
tags: [linkedin, engineering, ai]
tools: ["LangGraph", "LangSmith", "Pinecone"]
methods: ["ReAct loop", "Tool-call routing"]
projects: ["ai-agents", "rag-systems"]
---

## Summary
...

## Tools
- **LangGraph** — https://...: orchestrates multi-step agent state
- **LangSmith**: tracing and eval

## Methods
- **ReAct loop**: alternating reasoning and tool calls
- **Tool-call routing**: deterministic routing based on tool args

## Applicable Projects
`ai-agents`, `rag-systems`
```

In Obsidian, Dataview queries like `LIST FROM "linkedin" WHERE contains(projects, "ai-agents")` will surface every post relevant to a given project.

## Markdown Output Example

```markdown
---
channel: "Channel Name"
title: "Video Title"
youtube_url: "https://www.youtube.com/watch?v=..."
slack_message_url: "https://workspace.slack.com/archives/..."
duration: "12:34"
upload_date: "20240115"
tags: [technology, tutorials]
---

# Video Title

<iframe>...</iframe>

**Channel:** Channel Name
**Duration:** 12:34
**Tags:** technology, tutorials

---

## Table of Contents
- Topic 1
- Topic 2

## Summary
AI-generated summary...

---

## Full Transcript

[00:00] First segment of transcript...

More text from the first minute...

[01:00] Content from the second minute...
```

## Prerequisites

- Python 3.9+
- macOS (for launchd service) or Linux (with systemd adaptation)
- ffmpeg and ffprobe installed
- Slack workspace with admin access
- OpenAI API key

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/slack-knowledge-bot.git
cd slack-knowledge-bot
```

### 2. Install Dependencies

```bash
# Install Python packages
pip3 install -r requirements.txt

# Install ffmpeg (macOS)
brew install ffmpeg

# Install ffmpeg (Ubuntu/Debian)
# sudo apt install ffmpeg
```

### 3. Create Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps)
2. Click **Create New App** → **From scratch**
3. Name it (e.g., "Knowledge Bot") and select your workspace

#### OAuth & Permissions

Add these **Bot Token Scopes**:
- `channels:history` - Read messages in public channels
- `channels:read` - View basic channel info
- `chat:write` - Send messages
- `groups:history` - Read messages in private channels
- `groups:read` - View basic private channel info
- `im:history` - Read direct messages
- `im:read` - View basic DM info
- `mpim:history` - Read group DMs
- `mpim:read` - View basic group DM info
- `commands` - Add slash commands (for /process-history)

#### Socket Mode

1. Go to **Socket Mode** in the sidebar
2. Enable Socket Mode
3. Create an App-Level Token with `connections:write` scope
4. Save the token (starts with `xapp-`)

#### Event Subscriptions

1. Go to **Event Subscriptions**
2. Enable Events
3. Subscribe to bot events:
   - `message.channels`
   - `message.groups`
   - `message.im`
   - `message.mpim`

#### Slash Commands (Optional)

1. Go to **Slash Commands**
2. Create new command: `/process-history`
3. Description: "Bulk process YouTube videos from channel history"

#### Install App

1. Go to **Install App**
2. Click **Install to Workspace**
3. Copy the **Bot User OAuth Token** (starts with `xoxb-`)

### 4. Configure Environment

```bash
cp env.example .env
```

Edit `.env`:
```bash
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-token
OPENAI_API_KEY=sk-your-openai-key
```

### 5. Configure the Bot

Edit `youtube_knowledge_bot.py`:

```python
# Line 28: Set your download directory
DOWNLOAD_DIR = Path("/path/to/your/Knowledger")

# Line 31: Set your Slack workspace URL slug
SLACK_WORKSPACE = "yourworkspace"

# Line 35: Set digest time (24-hour format)
DIGEST_HOUR = 22  # 10 PM
```

### 6. Update run_bot.sh

```bash
#!/bin/bash
cd /path/to/slack-knowledge-bot

# Load environment
source .env
export SLACK_BOT_TOKEN
export SLACK_APP_TOKEN
export OPENAI_API_KEY

# Ensure ffmpeg is in PATH
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

# Run with unbuffered output for real-time logs
python3 -u youtube_knowledge_bot.py
```

Make it executable:
```bash
chmod +x run_bot.sh
```

### 7. Test Run

```bash
./run_bot.sh
```

You should see:
```
Knowledge Bot is running!
Files will be saved to: /path/to/Knowledger
Supported platforms: YouTube, Instagram
YouTube: Whisper transcription + GPT summaries
Instagram: Download only (no transcription)
Daily digest will be sent at 22:00
```

### 8. Set Up as Background Service (macOS)

Create the launchd plist:

```bash
cat > ~/Library/LaunchAgents/com.yourusername.knowledge-bot.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.yourusername.knowledge-bot</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/slack-knowledge-bot/run_bot.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/slack-knowledge-bot</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/path/to/slack-knowledge-bot/bot.log</string>
    <key>StandardErrorPath</key>
    <string>/path/to/slack-knowledge-bot/bot_error.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF
```

Load the service:
```bash
launchctl load ~/Library/LaunchAgents/com.yourusername.knowledge-bot.plist
launchctl start com.yourusername.knowledge-bot
```

### Service Management Commands

```bash
# Start
launchctl start com.yourusername.knowledge-bot

# Stop
launchctl stop com.yourusername.knowledge-bot

# Restart
launchctl stop com.yourusername.knowledge-bot && sleep 2 && launchctl start com.yourusername.knowledge-bot

# View logs
tail -f /path/to/slack-knowledge-bot/bot.log

# Check status
launchctl list | grep knowledge-bot
```

## Usage

### Automatic Processing

Just share a YouTube or Instagram link in any channel where the bot is present. The bot will:

1. Detect the link
2. Check if it's already processed (duplicate detection)
3. Process silently (no per-message notifications)
4. Add to daily digest

### Daily Digest

At 10 PM (configurable), the bot sends a summary:

```
*Daily Knowledge Base Digest* - Monday, January 27, 2026

3 item(s) processed today

*YouTube (2):*
• *Video Title 1*
  Channel: Channel Name | Duration: 12:34 | Tags: technology, tutorials
• *Video Title 2*
  Channel: Another Channel | Duration: 5:21 | Tags: business

*Instagram (1):*
• *Post Title*
  Uploader: username | Duration: 0:45

Files saved to: `/path/to/Knowledger`
```

### Download Flag

Include "download" in your message to tag a video for manual download:

```
download https://youtube.com/watch?v=xyz123
```

This adds `download` to the tags, making it easy to find in Obsidian.

### Bulk Processing (Slash Command)

Process historical messages:

```
/process-history C1234567890 2024-08     # August 2024
/process-history C1234567890 Q1-2025     # Q1 2025
/process-history C1234567890 30          # Last 30 days
```

### Showing Failures (Slash Command)

Every item the bot processes is appended to `processing_history.jsonl` (next to the bot script) at digest time. Query it via:

```
/show-failures           # last 7 days
/show-failures 30        # last 30 days
/show-failures 21d       # last 21 days
/show-failures all       # entire recorded history
```

The bot replies with each failure grouped by platform: timestamp, title, error message, and the original URL.

To enable in Slack: api.slack.com/apps → Your App → **Slash Commands** → create `/show-failures` (description: "Show items that failed to process recently").

### Backfilling Old Failures

`processing_history.jsonl` only starts populating after the `/show-failures` feature was added. To recover older failures, run the backfill script — it parses past daily-digest messages from a Slack channel and reconstructs failure records (URL is pulled from the retry button's embedded JSON, error message from the digest text):

```bash
# Dry run to preview what will be added
python3 backfill_failures.py --channel C0A99TH4Y2V --days 21 --dry-run

# Append for real
python3 backfill_failures.py --channel C0A99TH4Y2V --days 21

# Also include successes (URL won't be available, title-only)
python3 backfill_failures.py --channel C0A99TH4Y2V --days 21 --include-successes
```

The script:
- Dedupes by `(date, platform, title)` so re-running is safe
- Tags each backfilled row with `"backfilled": true` so you can distinguish reconstructed entries from live ones
- Has a known limitation: titles containing a colon get truncated (the digest format uses `:` as the title/error separator without escaping)

After running, `/show-failures 21d` will return the reconstructed entries alongside any live ones.

### Duplicate Detection

If someone shares an already-processed video, the bot replies:

```
This video has already been processed.

*Video Title*
File: `/path/to/Knowledger/Channel - Title.md`
```

## Categories

Videos are auto-categorized into:

| Category | Description |
|----------|-------------|
| faith | Religious/spiritual content |
| engineering | Software, mechanical, electrical engineering |
| tutorials | How-to, guides, walkthroughs |
| news | Current events, journalism |
| technology | Tech reviews, gadgets, AI |
| isms | Red pill, manosphere, ideological content |
| business | Entrepreneurship, startups, finance |
| productivity | Self-improvement, habits, systems |
| health | Fitness, nutrition, mental health |
| science | Research, experiments, explanations |
| entertainment | Movies, games, pop culture |
| education | Lectures, courses, academic |
| interviews | Podcasts, conversations, Q&A |
| reviews | Product reviews, critiques |
| creative | Art, music, design |
| career | Job advice, professional development |
| undefined | For manual categorization when unsure |

## Cost Estimation

| Service | Rate | Avg Usage (22 min video) | Cost |
|---------|------|--------------------------|------|
| Whisper | $0.006/min | ~22 min | ~$0.13 |
| GPT-4o (summary) | ~$5/1M tokens | ~5,000 tokens | ~$0.025 |
| GPT-4o-mini (tags) | ~$0.15/1M tokens | ~2,000 tokens | ~$0.0003 |

**Average per video:** ~$0.15-0.18

Instagram downloads are free (no API calls).

## Troubleshooting

### Bot not responding
```bash
# Check if running
launchctl list | grep knowledge-bot

# Check logs
tail -50 /path/to/bot.log
tail -50 /path/to/bot_error.log
```

### "moov atom not found" error
Audio file downloaded incompletely. The bot now auto-retries with different streams.

### Whisper 413 error (file too large)
Files over 25MB are automatically chunked into 10-minute segments.

### ffmpeg not found
Ensure ffmpeg is installed and in PATH:
```bash
which ffmpeg
# Should return: /opt/homebrew/bin/ffmpeg or /usr/local/bin/ffmpeg
```

### Instagram download fails
- Account might be private
- Content might be deleted
- Rate limiting (wait and retry)

## License

MIT License - feel free to modify and use as needed.

## Acknowledgments

- [pytubefix](https://github.com/JuanBindez/pytubefix) for YouTube audio extraction
- [yt-dlp](https://github.com/yt-dlp/yt-dlp) for Instagram downloads
- [OpenAI Whisper](https://openai.com/research/whisper) for transcription
- [Slack Bolt](https://slack.dev/bolt-python/) for the Slack SDK
