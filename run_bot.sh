#!/bin/bash

# YouTube Knowledge Bot Runner

# Add Homebrew paths for ffmpeg (needed for audio chunking)
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

# Change to script directory
cd "$(dirname "$0")"

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
echo "📁 Files will be saved to: /Volumes/Knowledger/vault/Knowledger"
echo "🎤 Whisper transcription: enabled"
echo "📝 GPT summaries: enabled"
echo ""
echo "Press Ctrl+C to stop the bot"
echo ""

# Kill any existing bot instances to avoid duplicates
existing_pids=$(pgrep -f "youtube_knowledge_bot.py" 2>/dev/null)
if [ -n "$existing_pids" ]; then
    echo "Stopping existing bot instances..."
    pkill -f "youtube_knowledge_bot.py" 2>/dev/null
    sleep 2
fi

# Activate virtual environment
VENV_DIR="$(dirname "$0")/.venv"
if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo "❌ Error: Virtual environment not found at $VENV_DIR"
    echo "Create it with: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt instaloader audioop-lts"
    exit 1
fi

# Run the bot with unbuffered output so logs appear immediately
python3 -u youtube_knowledge_bot.py
