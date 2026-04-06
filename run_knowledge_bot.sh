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
