#!/usr/bin/env python3
"""Reprocess URLs that were sent to the knowledger channel but never made it to the vault.

For each missing URL, this script:
- Reconstructs the original Slack message URL
- Routes the URL to the appropriate platform handler (YouTube/Instagram/LinkedIn/generic)
- Captures success/failure with the reason
- Writes a report at the end so failures can be addressed individually
"""

import os
import re
import sys
import time
import traceback
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
from youtube_knowledge_bot import (
    process_youtube_video,
    process_instagram_content,
    process_linkedin_post,
    process_resource_links,
    extract_video_id,
    get_instagram_url,
    extract_linkedin_url,
)

CHANNEL = "C0A99TH4Y2V"

# Missing URLs from the audit, paired with the Slack message timestamp where they appeared
MISSING = [
    # (kind, url, slack_ts)
    ("youtube", "https://www.youtube.com/watch?v=4krZlpzaI94", "1777115418.603939"),
    ("youtube", "https://www.youtube.com/watch?v=UHzdkVRSGAQ", "1776523350.523329"),
    ("youtube", "https://www.youtube.com/watch?v=r6J8VSVD3fA", "1776523335.386289"),
    ("youtube", "https://www.youtube.com/watch?v=szmcS7TlIY0", "1776522031.732179"),
    ("youtube", "https://www.youtube.com/watch?v=b31FVaqRPSE", "1774033713.509259"),
    ("youtube", "https://www.youtube.com/watch?v=lNAvZcXALKc", "1774032933.791299"),
    ("youtube", "https://www.youtube.com/watch?v=SvZs6bK3yt4", "1774031775.789369"),
    ("youtube", "https://www.youtube.com/watch?v=bI4UmqQ1ngQ", "1768576112.156829"),
    ("youtube", "https://www.youtube.com/watch?v=TTIlbcWURHY", "1768496269.957719"),

    ("instagram", "https://www.instagram.com/p/DXPH5p1DIvH/", "1776594971.206379"),
    ("instagram", "https://www.instagram.com/p/DXE20pWiAXi/", "1776593500.066249"),
    ("instagram", "https://www.instagram.com/reel/DW-qL89jNwI/", "1776522014.787799"),
    ("instagram", "https://www.instagram.com/reel/DXQF_FRDkI5/", "1776521990.378029"),
    ("instagram", "https://www.instagram.com/reel/DTbXsMcDHGw/", "1776053721.057119"),
    ("instagram", "https://www.instagram.com/p/DVHbhyJjkGW/", "1774114357.055289"),
    ("instagram", "https://www.instagram.com/p/DTRCE69jKx9/", "1774035168.759069"),

    ("linkedin", "https://www.linkedin.com/posts/shubhamsaboo_introducing-agents-cli-in-agent-platform-activity-7452911611297599488-dLNl", "1777121650.201759"),
    ("linkedin", "https://www.linkedin.com/posts/vincentius-liong_harvard-study-ugcPost-7448205260352847872-5pr6", "1775836717.939269"),
    ("linkedin", "https://www.linkedin.com/posts/vincentius-liong_neuroplasticity-stressresponsesystems-evolvedchimp-activity-7424314480991428608-At3Y", "1770499335.487989"),
    ("linkedin", "https://www.linkedin.com/feed/update/urn:li:activity:7403111084938915840/", "1769870957.624899"),

    # "Other" resource URLs — these go through process_resource_links
    ("resource", "https://traveltime.com/blog/location-based-recommendation-system-e-commerce", "1775808506.671629"),
    ("resource", "https://github.com/langchain-ai/deepagents", "1775220388.040759"),
    ("resource", "https://www.reavion.com/", "1775220351.699299"),
    ("resource", "https://github.com/farion1231/cc-switch", "1775134843.692319"),
    ("resource", "https://github.com/Panniantong/Agent-Reach", "1775134673.091479"),
    ("resource", "https://www.officialpiggypower.com/category/thermoelectric-generators", "1774783918.879639"),
    ("resource", "https://db.cs.cmu.edu/papers/2024/whatgoesaround-sigmodrec2024.pdf", "1774783844.333479"),
    ("resource", "https://github.com/666ghj/MiroFish", "1774762496.215479"),
    ("resource", "https://github.com/Donchitos/Claude-Code-Game-Studios", "1774698194.158129"),
    ("resource", "https://github.com/BlockRunAI/ClawRouter", "1774230272.147789"),
    ("resource", "https://www.mordorintelligence.com/about-us", "1774198526.673949"),
    ("resource", "https://youtube.com/live/NAgeNn6ICwM", "1774090364.452239"),
    ("resource", "https://youtube.com/post/UgkxVUld0D6BrsMZAX-c29c3cDq-0ipcJrYI", "1774088936.055989"),
    ("resource", "https://linas.substack.com/p/claudecowork", "1774088093.845649"),
    ("resource", "https://youtube.com/live/5AfzMX0oLE0", "1774032889.925159"),
    ("resource", "https://youtube.com/post/UgkxBmfaGivR5IkzC6cIwo5lvP95OWvkM7gc", "1771420224.129599"),
    ("resource", "https://youtube.com/live/5hHAj1zCfgY", "1770086626.281869"),
    ("resource", "https://how-to-ai.guide/", "1769868993.136729"),
    ("resource", "https://tr.ee/yEg4hY", "1769866981.865389"),
    ("resource", "https://typeset.io/", "1769866981.865389"),
    ("resource", "https://llm.codes/", "1769865623.845659"),
    ("resource", "https://github.com/microsoft/agent-lightning", "1769863593.859629"),
]


def slack_link(ts):
    return f"https://cocoworkshq.slack.com/archives/{CHANNEL}/p{ts.replace('.', '')}"


def fetch_message_text(client, ts):
    """Fetch the text + attachment text for a single message."""
    try:
        resp = client.conversations_history(
            channel=CHANNEL,
            latest=ts,
            oldest=ts,
            inclusive=True,
            limit=1,
        )
        msgs = resp.get("messages", [])
        if not msgs:
            return ""
        m = msgs[0]
        parts = [m.get("text", "") or ""]
        for att in m.get("attachments", []):
            for f in ("title", "text", "pretext", "fallback"):
                v = att.get(f)
                if v:
                    parts.append(str(v))
        return "\n".join(parts).strip()
    except Exception as e:
        print(f"  ! Could not fetch message text for {ts}: {e}")
        return ""


def main():
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("ERROR: SLACK_BOT_TOKEN not set")
        sys.exit(1)

    client = WebClient(token=token)

    successes = []
    failures = []

    for i, (kind, url, ts) in enumerate(MISSING, 1):
        smu = slack_link(ts)
        print(f"\n[{i}/{len(MISSING)}] {kind}: {url}")
        print(f"    slack: {smu}")
        try:
            if kind == "youtube":
                vid = extract_video_id(url)
                if not vid:
                    raise RuntimeError(f"could not extract video id from {url}")
                process_youtube_video(vid, CHANNEL, smu)
            elif kind == "instagram":
                process_instagram_content(url, CHANNEL, smu)
            elif kind == "linkedin":
                # Need post_text for LinkedIn — fetch from Slack
                post_text = fetch_message_text(client, ts)
                if not post_text:
                    post_text = url  # fallback so title generation has something
                process_linkedin_post(url, post_text, CHANNEL, smu)
            elif kind == "resource":
                msg_text = fetch_message_text(client, ts) or url
                process_resource_links([url], msg_text, CHANNEL, smu)
            else:
                raise RuntimeError(f"unknown kind: {kind}")
            successes.append((kind, url))
            print(f"    ✓ done")
        except Exception as e:
            failures.append((kind, url, str(e)))
            print(f"    ✗ FAILED: {e}")
            traceback.print_exc()

        # Be nice to APIs
        time.sleep(1.5)

    print("\n" + "=" * 70)
    print(f"SUCCESSES: {len(successes)}")
    print(f"FAILURES:  {len(failures)}")
    print("=" * 70)
    if failures:
        print("\nFailed items:")
        for kind, url, err in failures:
            print(f"  [{kind}] {url}")
            print(f"    -> {err}")


if __name__ == "__main__":
    main()
