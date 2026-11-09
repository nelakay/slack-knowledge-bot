#!/bin/bash
# Wrapper for launchd. Logs every spawn + exit code so we can see what's failing.
set -u

PROJECT_DIR="/Users/nelagueye/Downloads/coding_projects/slack/slackbot_yt_to_md"
LOG="${PROJECT_DIR}/launchd_debug.log"

echo "==== $(date '+%Y-%m-%d %H:%M:%S') spawn pid=$$ ====" >> "$LOG"
echo "USER=${USER:-?} HOME=${HOME:-?} PWD=$(pwd)" >> "$LOG"
echo "PATH=${PATH:-?}" >> "$LOG"
echo "SLACK_BOT_TOKEN set: $([ -n "${SLACK_BOT_TOKEN:-}" ] && echo yes || echo no)" >> "$LOG"
echo "SLACK_APP_TOKEN set: $([ -n "${SLACK_APP_TOKEN:-}" ] && echo yes || echo no)" >> "$LOG"
echo "OPENAI_API_KEY set: $([ -n "${OPENAI_API_KEY:-}" ] && echo yes || echo no)" >> "$LOG"
echo "RAPIDAPI_KEY set: $([ -n "${RAPIDAPI_KEY:-}" ] && echo yes || echo no)" >> "$LOG"

cd "$PROJECT_DIR" || { echo "ERR cd failed" >> "$LOG"; exit 78; }

# HOME is often missing under launchd; many Python libs need it.
export HOME="${HOME:-/Users/nelagueye}"

"${PROJECT_DIR}/.venv/bin/python3" -u "${PROJECT_DIR}/youtube_knowledge_bot.py"
EC=$?
echo "==== $(date '+%Y-%m-%d %H:%M:%S') exit=$EC pid=$$ ====" >> "$LOG"
exit $EC
