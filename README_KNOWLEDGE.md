# YouTube Knowledge Bot

Automatically creates rich markdown knowledge files from YouTube videos shared in Slack.

## What It Does

When someone posts a YouTube link in your Slack channel, the bot:

1. **Extracts metadata** (title, channel, duration, tags)
2. **Downloads the audio** (temporarily, for transcription)
3. **Transcribes with Whisper** (OpenAI's speech-to-text)
4. **Generates a summary & table of contents** (using GPT-4)
5. **Creates a markdown file** with everything organized

## Output File Structure

```markdown
---
channel: "Channel Name"
title: "Video Title"
youtube_url: "https://youtube.com/watch?v=..."
slack_message_url: "https://workspace.slack.com/..."
duration: "10:30"
---

# Video Title

**Channel:** Channel Name
**Duration:** 10:30
**YouTube:** [link]
**Slack Reference:** [link]

---

## Table of Contents
- Topic 1
- Topic 2
- Topic 3

## Summary
[AI-generated summary of the video]

---

## Full Transcript

[00:00] First words of the video...
[00:15] More content...
```

## Setup

### 1. Install dependencies

```bash
cd ~/Downloads/coding_projects/slackbot_to_yt
pip3 install -r requirements.txt
```

### 2. Update your .env file

Add your OpenAI API key:

```
SLACK_BOT_TOKEN=xoxb-your-token
SLACK_APP_TOKEN=xapp-your-token
OPENAI_API_KEY=sk-your-openai-key
```

### 3. Update the Slack workspace URL

Edit `youtube_knowledge_bot.py` line 26:
```python
SLACK_WORKSPACE = "your-workspace"  # e.g., "mycompany" for mycompany.slack.com
```

### 4. Install FFmpeg (required for audio extraction)

```bash
brew install ffmpeg
```

### 5. Run the bot

```bash
chmod +x run_knowledge_bot.sh
./run_knowledge_bot.sh
```

## Costs

- **Whisper**: ~$0.006 per minute of audio
- **GPT-4**: ~$0.01-0.03 per summary

For a typical 10-minute video: **~$0.10**

## Files Location

All markdown files are saved to:
```
/Users/nelagueye/Downloads/Knowledger/
```

Filename format: `{Channel Name} - {Video Title}.md`

## Troubleshooting

### "Could not download audio"
- Video may be restricted or private
- Try a different video

### "Transcription failed"
- Check your OpenAI API key
- Ensure you have credits in your OpenAI account

### "Summary not generated"
- GPT request may have timed out
- The transcript will still be saved

### Bot not responding
- Make sure bot is running (`./run_knowledge_bot.sh`)
- Invite bot to the channel (`/invite @YourBotName`)

## What's NOT Included

- ❌ Video/MP4 downloads (YouTube blocks this)
- ❌ Audio/MP3 downloads (same reason)

This bot focuses on **knowledge extraction** - getting the valuable content (transcript, summary) into a searchable format.

Enjoy your knowledge base! 📚
