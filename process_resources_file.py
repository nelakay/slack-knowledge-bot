#!/usr/bin/env python3
"""Run every link in a markdown resources table through the web-link processor.

    python3 process_resources_file.py "<vault>/00. resources.md"                 # dry run: lists the links
    python3 process_resources_file.py "<vault>/00. resources.md" --apply          # summarise each link into <vault>/web/
    python3 process_resources_file.py "<vault>/00. resources.md" --apply --delete # ...then delete the file

--delete only removes the file when no link ended in an error. A copy is kept in
~/ig_enrich_backup/ first. Links the processor skips on purpose (Instagram, LinkedIn,
YouTube) are listed so you can check they already have a note.
"""
import argparse
import ast
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

URL_CELL = re.compile(r"\[(https?://[^\]\s]+)\]\(")
SLACK = re.compile(r"\((https://[^)\s]*slack\.com/archives/[^)\s]+)\)")


def parse_rows(text):
    """One entry per table row that has a link: (url, name, slack_url). Order kept, duplicates dropped."""
    out, seen = [], set()
    for line in text.splitlines():
        if not line.startswith("|") or set(line.replace("|", "").strip()) <= set("-: "):
            continue
        m = URL_CELL.search(line)
        if not m:
            continue
        url = m.group(1)
        if url in seen:
            continue
        seen.add(url)
        name = line.split("|")[1].strip() if line.count("|") > 1 else ""
        s = SLACK.search(line)
        out.append((url, name, s.group(1) if s else ""))
    return out


def allowed_categories(bot_path):
    tree = ast.parse(Path(bot_path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "ALLOWED_CATEGORIES" for t in node.targets):
            return ast.literal_eval(node.value)
    sys.exit("ALLOWED_CATEGORIES not found in %s" % bot_path)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file")
    ap.add_argument("--vault", default="")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--delete", action="store_true", help="delete the file afterwards if no link failed")
    args = ap.parse_args()

    f = Path(args.file).expanduser()
    if not f.is_file():
        sys.exit("File not found: %s" % f)
    vault = Path(args.vault).expanduser() if args.vault else f.parent
    rows = parse_rows(f.read_text(encoding="utf-8", errors="ignore"))
    print("%d links in %s\n" % (len(rows), f.name))
    for i, (u, n, _) in enumerate(rows, 1):
        print("%2d. %s" % (i, u))
    if not args.apply:
        print("\nDry run. Add --apply to process them (uses OpenAI) and --delete to remove the file after.")
        return

    import web_resources
    import ig_enrich
    ig_enrich.load_env_file(HERE / ".env")
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("OPENAI_API_KEY not set (looked in the environment and the bot's .env).")
    from openai import OpenAI
    client = OpenAI()
    cats = allowed_categories(HERE / "youtube_knowledge_bot.py")

    results = []
    for i, (u, n, slack) in enumerate(rows, 1):
        r = web_resources.process_url(u, n, slack, client, vault, cats)
        results.append((u, r))
        note = Path(r.get("note", "")).name if r.get("note") else ""
        print("[%d/%d] %-9s %s %s" % (i, len(rows), r["status"], u[:70], ("-> " + note) if note else ""))

    by = {}
    for u, r in results:
        by.setdefault(r["status"], []).append((u, r))
    print("\n" + ", ".join("%s: %d" % (k, len(v)) for k, v in sorted(by.items())))
    for u, r in by.get("skipped", []):
        print("  skipped (own pipeline or media file, make sure it has a note): %s" % u)
    for u, r in by.get("stub", []):
        print("  stub (page unreadable, marked so it is retried when shared again): %s  %s" % (u, r.get("reason", "")))
    for u, r in by.get("error", []):
        print("  ERROR: %s  %s" % (u, r.get("reason", "")))

    if args.delete:
        if by.get("error"):
            print("\nNOT deleting %s: %d link(s) failed. Re-run to retry them." % (f.name, len(by["error"])))
            return
        bdir = Path.home() / "ig_enrich_backup"
        bdir.mkdir(exist_ok=True)
        keep = bdir / ("%s_%s" % (datetime.now().strftime("%Y%m%d_%H%M%S"), f.name))
        shutil.copy2(str(f), str(keep))
        f.unlink()
        print("\nDeleted %s (copy kept at %s)" % (f, keep))


if __name__ == "__main__":
    main()
