# YouTube Knowledge Bot - Complete Setup Guide

This guide will walk you through setting up the YouTube Knowledge Bot from scratch.

---

## What You'll Have When Done

When someone posts a YouTube link in your Slack channel, the bot will automatically create a markdown file containing:
- YouTube iframe embed (watchable in Obsidian/Notion)
- Video metadata (channel, title, duration, tags)
- Table of contents
- AI-generated summary
- Full transcript with timestamps
- Link back to the original Slack message

---

## Prerequisites

- A Mac computer
- A Slack workspace where you can create apps
- An OpenAI account with API access
- About 30 minutes

---

## Step 1: Create a Folder for the Bot

Open Terminal and run:

```bash
mkdir -p ~/Downloads/coding_projects/youtube_knowledge_bot
cd ~/Downloads/coding_projects/youtube_knowledge_bot
```

---

## Step 2: Install Required Software

### Install Homebrew (if you don't have it)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Install Python 3 (if needed)
```bash
brew install python3
```

### Install FFmpeg (required for audio extraction)
```bash
brew install ffmpeg
```

### Verify installations
```bash
python3 --version
ffmpeg -version
```

---

## Step 3: Create the Bot Files

### 3.1 Create the main bot file

Create a file called `youtube_knowledge_bot.py`:

```bash
nano youtube_knowledge_bot.py
```

Paste the entire contents of the `youtube_knowledge_bot.py` file I provided, then:
- Press `Ctrl+X`
- Press `Y` to save
- Press `Enter` to confirm

**IMPORTANT:** Find this line near the top of the file (around line 26):
```python
SLACK_WORKSPACE = "your-workspace"
```

Change `your-workspace` to your actual Slack workspace name. For example, if your Slack URL is `mycompany.slack.com`, change it to:
```python
SLACK_WORKSPACE = "mycompany"
```

### 3.2 Create requirements.txt

```bash
nano requirements.txt
```

Paste this content:
```
slack-bolt==1.18.0
yt-dlp>=2024.1.0
openai>=1.0.0
requests>=2.31.0
```

Save and exit (`Ctrl+X`, `Y`, `Enter`).

### 3.3 Create the run script

```bash
nano run_bot.sh
```

Paste this content:
```bash
#!/bin/bash

# YouTube Knowledge Bot Runner

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "❌ Error: .env file not found!"
    echo "Please create a .env file with your tokens."
    exit 1
fi

# Check if tokens are set
if [ -z "$SLACK_BOT_TOKEN" ] || [ -z "$SLACK_APP_TOKEN" ]; then
    echo "❌ Error: Slack tokens not found in .env file!"
    exit 1
fi

if [ -z "$OPENAI_API_KEY" ]; then
    echo "❌ Error: OpenAI API key not found in .env file!"
    echo "Add OPENAI_API_KEY=sk-... to your .env file"
    exit 1
fi

echo "🚀 Starting YouTube Knowledge Bot..."
echo "📁 Files will be saved to: /Users/nelagueye/Downloads/Knowledger"
echo "🎤 Whisper transcription: enabled"
echo "📝 GPT summaries: enabled"
echo ""
echo "Press Ctrl+C to stop the bot"
echo ""

# Run the bot
python3 youtube_knowledge_bot.py
```

Save and exit.

### 3.4 Make the run script executable

```bash
chmod +x run_bot.sh
```

---

## Step 4: Install Python Dependencies

```bash
pip3 install -r requirements.txt
```

If you get permission errors, try:
```bash
pip3 install --user -r requirements.txt
```

---

## Step 5: Create the Slack App

### 5.1 Go to Slack API

1. Open your browser and go to: https://api.slack.com/apps
2. Click **"Create New App"**
3. Choose **"From scratch"**
4. Name it: `YouTube Knowledge Bot`
5. Select your workspace
6. Click **"Create App"**

### 5.2 Configure Bot Permissions

1. In the left sidebar, click **"OAuth & Permissions"**
2. Scroll down to **"Scopes"** → **"Bot Token Scopes"**
3. Click **"Add an OAuth Scope"** and add these scopes:
   - `channels:history` - View messages in public channels
   - `channels:read` - View basic channel info
   - `chat:write` - Send messages
   - `groups:history` - View messages in private channels
   - `im:history` - View messages in DMs
   - `mpim:history` - View messages in group DMs
   - `reactions:write` - Add emoji reactions
   - `reactions:read` - View emoji reactions

4. Scroll up and click **"Install to Workspace"**
5. Click **"Allow"**
6. **Copy the "Bot User OAuth Token"** (starts with `xoxb-`) - you'll need this!

### 5.3 Enable Socket Mode

1. In the left sidebar, click **"Socket Mode"**
2. Toggle **"Enable Socket Mode"** to ON
3. Give it a token name: `YouTube Bot Socket`
4. Click **"Generate"**
5. **Copy the "App-Level Token"** (starts with `xapp-`) - you'll need this!

### 5.4 Subscribe to Events

1. In the left sidebar, click **"Event Subscriptions"**
2. Toggle **"Enable Events"** to ON
3. Expand **"Subscribe to bot events"**
4. Click **"Add Bot User Event"** and add:
   - `message.channels` - Listen to messages in public channels
   - `message.groups` - Listen to messages in private channels
   - `message.im` - Listen to DMs
   - `message.mpim` - Listen to group DMs
5. Click **"Save Changes"** at the bottom

---

## Step 6: Get Your OpenAI API Key

1. Go to: https://platform.openai.com/api-keys
2. Click **"Create new secret key"**
3. Name it: `YouTube Knowledge Bot`
4. Click **"Create secret key"**
5. **Copy the key** (starts with `sk-`) - you'll need this!

**Note:** Make sure you have billing set up in OpenAI. Transcription costs about $0.10 per 10-minute video.

---

## Step 7: Create the Environment File

```bash
nano .env
```

Paste this content, replacing the placeholder values with your actual tokens:

```
# Slack Bot Configuration
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_APP_TOKEN=xapp-your-app-token-here

# OpenAI API Key
OPENAI_API_KEY=sk-your-openai-key-here
```

Save and exit (`Ctrl+X`, `Y`, `Enter`).

**IMPORTANT:** Keep this file secret! Never share it or commit it to git.

---

## Step 8: Create the Output Folder

```bash
mkdir -p /Users/nelagueye/Downloads/Knowledger
```

---

## Step 9: Test the Bot

### 9.1 Start the bot

```bash
./run_bot.sh
```

You should see:
```
🚀 Starting YouTube Knowledge Bot...
📁 Files will be saved to: /Users/nelagueye/Downloads/Knowledger
🎤 Whisper transcription: enabled
📝 GPT summaries: enabled

⚡️ YouTube Knowledge Bot is running!
```

### 9.2 Test in Slack

1. Go to a public channel in your Slack workspace
2. Invite the bot: `/invite @YouTube Knowledge Bot`
3. Post a YouTube link, like: `https://www.youtube.com/watch?v=jNQXAC9IVRw`
4. Wait for the bot to process (it will show a ⏳ reaction, then ✅ when done)
5. Check the `/Users/nelagueye/Downloads/Knowledger` folder for your markdown file!

---

## Step 10: Run the Bot in Background (Optional)

To keep the bot running even when you close Terminal:

```bash
nohup ./run_bot.sh > bot.log 2>&1 &
```

To check if it's running:
```bash
ps aux | grep youtube_knowledge_bot
```

To stop it:
```bash
pkill -f youtube_knowledge_bot
```

To see logs:
```bash
tail -f bot.log
```

---

## Troubleshooting

### "No such file or directory" error
Make sure you're in the right folder:
```bash
cd ~/Downloads/coding_projects/youtube_knowledge_bot
```

### "SLACK_BOT_TOKEN not found" error
Check your `.env` file exists and has the correct tokens:
```bash
cat .env
```

### Bot not responding to messages
1. Make sure the bot is running (`./run_bot.sh`)
2. Invite the bot to the channel (`/invite @YouTube Knowledge Bot`)
3. Check Event Subscriptions are enabled in Slack API settings

### "Could not download audio" error
- Make sure FFmpeg is installed: `ffmpeg -version`
- Some videos may be restricted or private

### "OpenAI API error"
- Check your API key is correct
- Make sure you have billing enabled on OpenAI
- Check your usage limits

---

## Costs

| Item | Cost |
|------|------|
| Whisper (transcription) | ~$0.006 per minute |
| GPT-4 (summary) | ~$0.01-0.03 per video |
| **Total per 10-min video** | **~$0.10** |

---

## File Output Example

Here's what a generated markdown file looks like:

```markdown
---
channel: "TED"
title: "How to speak so that people want to listen"
youtube_url: "https://www.youtube.com/watch?v=eIho2S0ZahI"
slack_message_url: "https://mycompany.slack.com/archives/C123/p456"
duration: "9:58"
---

# How to speak so that people want to listen

<iframe width="560" height="315" src="https://www.youtube.com/embed/eIho2S0ZahI" ...></iframe>

**Channel:** TED
**Duration:** 9:58
**YouTube:** [link]
**Slack Reference:** [View in Slack]

---

## Table of Contents
- The human voice as an instrument
- Seven deadly sins of speaking
- Four powerful cornerstones of speech
- Vocal toolbox techniques

## Summary
Julian Treasure explores the power of the human voice and how to use it effectively...

---

## Full Transcript

[00:00] The human voice: It's the instrument we all play...
[00:15] It's the most powerful sound in the world...
```

---

## You're Done! 🎉

Your YouTube Knowledge Bot is now running. Every time someone posts a YouTube link in your Slack channels, it will automatically create a rich markdown file with the video content.

Enjoy building your knowledge base! 📚
