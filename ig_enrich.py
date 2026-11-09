#!/usr/bin/env python3
"""
ig_enrich.py - make Instagram notes queryable.

Adds to an existing Instagram markdown note:
  * ## Transcript   - speech from video media (ffmpeg -> OpenAI Whisper)
  * ## Image Text   - text visible on carousel / image slides (OpenAI vision model)
  * frontmatter     - transcript_status, image_text_status, links, tools_mentioned,
                      summary_auto, enriched_at

Two ways to use it:

1. Live bot hook (see youtube_knowledge_bot.py):
       ig_enrich.enrich_note(md_path, [media paths], openai_client)

2. One-off backfill of the notes already in the vault (dry-run by default):
       python3 ig_enrich.py backfill --vault /Volumes/Knowledger/vault/Knowledger
       python3 ig_enrich.py backfill --vault ... --limit 3 --apply
       python3 ig_enrich.py backfill --vault ... --apply

Design rules:
  * Idempotent: a note that already has a section / a final status is skipped.
  * Never invents content: silent videos get transcript_status "no_speech",
    not an empty or hallucinated section.
  * Never raises into the bot: enrich_note() reports errors in its return value.
  * Backfill backs up every note before changing it (outside the vault).
"""

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unicodedata
from datetime import datetime
from pathlib import Path

VIDEO_EXT = {".mp4", ".mov", ".m4v", ".webm"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

WHISPER_MODEL = os.environ.get("IG_WHISPER_MODEL", "whisper-1")
# Set IG_WHISPER_LANGUAGE=en (ISO-639-1) to skip auto-detection altogether.
WHISPER_LANGUAGE = os.environ.get("IG_WHISPER_LANGUAGE", "").strip().lower()
# Languages whisper-1 tends to report for English spoken with a strong accent.
MISDETECTED_LANGS = {"welsh", "cy", "nynorsk", "nn", "latin", "la", "maori", "mi", "breton", "br"}
VISION_MODEL = os.environ.get("IG_VISION_MODEL", "gpt-4o-mini")
TEXT_MODEL = os.environ.get("IG_TEXT_MODEL", "gpt-4o-mini")

MAX_AUDIO_BYTES = 20 * 1024 * 1024   # Whisper API limit is 25MB; keep a margin
SEGMENT_SECONDS = 600
MIN_SPEECH_CHARS = 20
MAX_IMAGE_BYTES = 18 * 1024 * 1024
MAX_TEXT_FOR_TOOLS = 12000

# Whisper is known to emit stock phrases on silence/music. Treat these as no speech.
WHISPER_STOCK_PHRASES = {
    "thank you", "thanks", "thank you.", "thanks for watching",
    "thank you for watching", "thanks for watching!", "you", "bye", "bye.",
}

EMBED_RE = re.compile(r"!\[\[(.+?)\]\]")
URL_RE = re.compile(r"https?://[^\s<>\]\)\"'`]+")

VISION_PROMPT = (
    "Transcribe all text visible in this image exactly as written, in reading order. "
    "Keep a line break between separate text blocks. "
    "If the image shows a screenshot, chart or diagram, add one final line starting with "
    "'Visual:' that states only what is shown, with no interpretation. "
    "Write [illegible] for text you cannot read; never guess. "
    "If the image contains no readable text, reply exactly: NO_TEXT"
)

TOOLS_PROMPT = (
    "You are given text from an Instagram post: the caption, and possibly a video transcript "
    "and text read from slide images. List the named software tools, libraries, frameworks, "
    "products, AI models and GitHub projects that are EXPLICITLY named in the text. "
    "Rules: only names that literally appear in the text; no inferred or implied tools; "
    "no generic categories (e.g. 'AI', 'database'); do not translate names. "
    "Also write one factual sentence saying what the post is about. "
    'Return JSON exactly like {"tools": ["..."], "summary": "..."} . '
    'If there is nothing, return {"tools": [], "summary": ""}.'
)


# --------------------------------------------------------------------------
# small utilities
# --------------------------------------------------------------------------

def _retry(fn, attempts=3, base_delay=2.0):
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # network / rate limit / transient API errors
            last = e
            if i < attempts - 1:
                time.sleep(base_delay * (2 ** i))
    raise last


def _nfc(s):
    return unicodedata.normalize("NFC", s)


def load_env_file(path):
    """Load KEY=VALUE lines into os.environ without overriding existing values."""
    path = Path(path)
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _atomic_write(path, content):
    path = Path(path)
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(str(tmp), str(path))


# --------------------------------------------------------------------------
# note parsing / editing
# --------------------------------------------------------------------------

def _split_note(text):
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    if not m:
        return None, text
    return m.group(1), text[m.end():]


def _fm_get(fm, key):
    m = re.search(r"^%s:\s*\"?([^\"\n]*)\"?\s*$" % re.escape(key), fm, re.M)
    return m.group(1).strip() if m else None


def _fm_set(fm, key, value_literal):
    line = "%s: %s" % (key, value_literal)
    pat = re.compile(r"^%s:.*$" % re.escape(key), re.M)
    if pat.search(fm):
        return pat.sub(lambda _m: line, fm, count=1)
    return fm + "\n" + line


def _q(s):
    return '"' + str(s).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ") + '"'


def _qlist(items):
    return "[" + ", ".join(_q(i) for i in items) + "]"


def _has_section(body, title):
    return re.search(r"^## %s\s*$" % re.escape(title), body, re.M) is not None


def _drop_section(body, title):
    pat = re.compile(r"\n*^## %s\s*$.*?(?=^## |\Z)" % re.escape(title), re.M | re.S)
    return pat.sub("\n", body)


def _description(body):
    # Instagram notes keep the caption under "Description"; LinkedIn notes under "Post Content".
    return _section_text(body, "Description") or _section_text(body, "Post Content")


def _section_text(body, title):
    m = re.search(r"^## %s\s*$(.*?)(?=^## |\Z)" % re.escape(title), body, re.M | re.S)
    return m.group(1).strip() if m else ""


def extract_links(text):
    # a line "short -> resolved" lists the same link twice: keep only the destination
    text = re.sub(r"https?://\S+\s+\u2192\s+(?=https?://)", "", text or "")
    seen, out = set(), []
    for u in URL_RE.findall(text or ""):
        u = u.rstrip(".,;:!?)")
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def build_index(*dirs):
    """Map NFC-normalised filename -> real Path. macOS stores accented names decomposed
    (NFD) while Obsidian embeds are usually NFC, so plain string matching misses files."""
    idx = {}
    for d in dirs:
        d = Path(d)
        if not d.is_dir():
            continue
        for p in d.iterdir():
            if p.name.startswith("._"):
                continue
            idx.setdefault(_nfc(p.name), p)
    return idx


def resolve_media(note_text, idx):
    found, missing = [], []
    for name in EMBED_RE.findall(note_text):
        p = idx.get(_nfc(name.strip()))
        (found if p else missing).append(p if p else name)
    return found, missing


# --------------------------------------------------------------------------
# media -> text
# --------------------------------------------------------------------------

def _ffmpeg(args, timeout=300):
    r = subprocess.run(["ffmpeg", "-y", "-v", "error"] + args,
                       capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError("ffmpeg failed: " + (r.stderr or "")[-300:])


def extract_audio(video_path, out_dir):
    out = Path(out_dir) / "audio.mp3"
    _ffmpeg(["-i", str(video_path), "-vn", "-ac", "1", "-ar", "16000", "-b:a", "48k", str(out)])
    if not out.exists() or out.stat().st_size == 0:
        raise RuntimeError("no audio extracted")
    return out


def _split_audio(audio, out_dir):
    _ffmpeg(["-i", str(audio), "-f", "segment", "-segment_time", str(SEGMENT_SECONDS),
             "-c", "copy", str(Path(out_dir) / "seg_%03d.mp3")])
    return sorted(Path(out_dir).glob("seg_*.mp3"))


def _resp_text(resp):
    if resp is None:
        return ""
    if isinstance(resp, str):
        return resp
    if isinstance(resp, dict):
        return resp.get("text", "") or ""
    return getattr(resp, "text", "") or ""


def _seg_val(seg, key, default=None):
    if isinstance(seg, dict):
        return seg.get(key, default)
    return getattr(seg, key, default)


# Whisper invents text (often repeated, often in another script) for music and noise. Its own
# per-segment scores tell us when that happened: drop a segment when it is a repetition loop
# (compression_ratio > 2.4), low confidence (avg_logprob < -1.0), or probably not speech.
# Phrases Whisper is known to produce from silence or music (subtitle credits, sign-offs).
HALLUCINATION_RE = re.compile(
    r"(субтитры\s+(предоставил|сделал|создавал)|продолжение следует|редактор субтитров|корректор"
    r"|amara\.org|subtitles? (by|provided by)|thanks? for watching|thank you for watching"
    r"|please subscribe|don'?t forget to (like|subscribe)|字幕由|ご視聴ありがとう|사랑해요|구독과 좋아요)",
    re.I)

MAX_COMPRESSION = 2.4
MIN_LOGPROB = -1.0
MAX_NO_SPEECH = 0.6


def _resp_language(resp):
    lang = resp.get("language") if isinstance(resp, dict) else getattr(resp, "language", None)
    return (lang or "").strip().lower()


def _speech_segments(resp):
    """[(start, end, text)] of a verbose_json response with unreliable segments removed.
    start/end are None when the response carries no timing."""
    segs = resp.get("segments") if isinstance(resp, dict) else getattr(resp, "segments", None)
    if not segs:
        t = _resp_text(resp).strip()
        words = t.split()
        # no scores available: still catch an obvious repetition loop
        if len(words) >= 12 and len(set(words)) / len(words) < 0.3:
            return []
        return [(None, None, t)] if t else []
    kept = []
    for s in segs:
        text = (_seg_val(s, "text", "") or "").strip()
        if not text:
            continue
        cr = _seg_val(s, "compression_ratio", 0) or 0
        lp = _seg_val(s, "avg_logprob", 0) or 0
        ns = _seg_val(s, "no_speech_prob", 0) or 0
        if cr > MAX_COMPRESSION or lp < MIN_LOGPROB or ns > MAX_NO_SPEECH:
            continue
        if HALLUCINATION_RE.search(text) and len(text) < 120:
            continue
        kept.append((_seg_val(s, "start"), _seg_val(s, "end"), text))
    return kept


def _speech_text(resp):
    """Plain text of a verbose_json response with unreliable segments removed."""
    return " ".join(t for _, _, t in _speech_segments(resp)).strip()


PARA_MIN_CHARS = 280     # never break a paragraph before this length
PARA_MAX_CHARS = 900     # break at the next sentence end after this length
PARA_PAUSE = 1.2         # seconds of silence that count as a pause


def _stamp(seconds):
    s = int(max(0, seconds or 0))
    return "%d:%02d:%02d" % (s // 3600, s % 3600 // 60, s % 60) if s >= 3600 else "%02d:%02d" % (s // 60, s % 60)


def format_paragraphs(segs):
    """Group [(start, end, text)] (times in seconds from the start of the video) into paragraphs,
    each starting with a **[mm:ss]** marker. Breaks fall on pauses and sentence ends, never mid-sentence."""
    timed = bool(segs) and all(s is not None for s, _, _ in segs)
    paras, cur, cur_start, last_end = [], [], None, None
    for start, end, text in segs:
        length = sum(len(t) + 1 for t in cur)
        gap = (start - last_end) if (timed and last_end is not None) else 0
        ended = bool(cur) and cur[-1].rstrip().endswith((".", "?", "!", "…"))
        if cur and ended and (length >= PARA_MAX_CHARS or (length >= PARA_MIN_CHARS and gap >= PARA_PAUSE)):
            paras.append((cur_start, " ".join(cur)))
            cur, cur_start = [], None
        if not cur:
            cur_start = start
        cur.append(text)
        last_end = end
    if cur:
        paras.append((cur_start, " ".join(cur)))
    if not timed:   # no timing at all: keep the text, just break it up every few sentences
        out = []
        for _, t in paras:
            sents = re.split(r"(?<=[.!?])\s+", t)
            out += [" ".join(sents[i:i + 5]) for i in range(0, len(sents), 5)]
        return "\n\n".join(out)
    return "\n\n".join("**[%s]** %s" % (_stamp(s), t) for s, t in paras)


def has_audio_stream(video_path):
    """True/False, or None if ffprobe could not tell."""
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "a",
                            "-show_entries", "stream=index", "-of", "csv=p=0", str(video_path)],
                           capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            return None
        return bool(r.stdout.strip())
    except Exception:
        return None


def transcribe_video(video_path, client):
    """Return (status, text). status: ok | no_speech (also for a video with no audio track)."""
    if has_audio_stream(video_path) is False:
        return "no_speech", ""
    with tempfile.TemporaryDirectory() as td:
        try:
            audio = extract_audio(video_path, td)
        except RuntimeError as e:
            # ffprobe could not tell, but ffmpeg found nothing to extract: the video has no audio track
            if "does not contain any stream" in str(e) or "no audio extracted" in str(e):
                return "no_speech", ""
            raise
        parts = [audio]
        if audio.stat().st_size > MAX_AUDIO_BYTES:
            parts = _split_audio(audio, td)
        segs, forced = [], WHISPER_LANGUAGE or None
        for idx, part in enumerate(parts):
            def call(p=part, lang=None):
                kw = {"language": lang} if lang else {}
                with open(str(p), "rb") as fh:
                    return client.audio.transcriptions.create(
                        model=WHISPER_MODEL, file=fh, response_format="verbose_json", **kw)
            resp = _retry(lambda: call(lang=forced))
            # Auto-detect sometimes labels accented English (British, Indian, ...) as Welsh, Nynorsk, Latin...
            # and then writes phonetic nonsense in that language. Redo that part as English.
            if not forced and _resp_language(resp) in MISDETECTED_LANGS:
                forced = "en"
                resp = _retry(lambda: call(lang="en"))
            off = idx * SEGMENT_SECONDS if len(parts) > 1 else 0
            for s, e, t in _speech_segments(resp):
                segs.append((None if s is None else s + off, None if e is None else e + off, t))
    raw = " ".join(t for _, _, t in segs).strip()
    if len(raw) < MIN_SPEECH_CHARS or raw.lower() in WHISPER_STOCK_PHRASES:
        return "no_speech", ""
    return "ok", format_paragraphs(segs)


def read_image(image_path, client):
    """Return the text read from one image, or '' if none."""
    p = Path(image_path)
    if p.stat().st_size > MAX_IMAGE_BYTES:
        raise RuntimeError("image too large")
    mime = {".png": "image/png", ".webp": "image/webp", ".gif": "image/gif"}.get(p.suffix.lower(), "image/jpeg")
    b64 = base64.b64encode(p.read_bytes()).decode("ascii")

    def call():
        return client.chat.completions.create(
            model=VISION_MODEL,
            temperature=0,
            max_tokens=1200,
            messages=[{"role": "user", "content": [
                {"type": "text", "text": VISION_PROMPT},
                {"type": "image_url", "image_url": {"url": "data:%s;base64,%s" % (mime, b64)}},
            ]}],
        )
    r = _retry(call)
    out = (r.choices[0].message.content or "").strip()
    return "" if out == "NO_TEXT" else out


def extract_tools(text, client):
    """Return (tools, summary). LLM pass; names are only kept if they literally appear in text."""
    text = (text or "").strip()
    if len(text) < 30:
        return [], ""

    def call():
        return client.chat.completions.create(
            model=TEXT_MODEL,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": TOOLS_PROMPT},
                      {"role": "user", "content": text[:MAX_TEXT_FOR_TOOLS]}],
        )
    r = _retry(call)
    data = json.loads(r.choices[0].message.content or "{}")
    lowered = text.lower()
    tools, seen = [], set()
    for t in data.get("tools", []) or []:
        t = str(t).strip()
        # guard against hallucinated names: keep only names present in the source text
        if t and t.lower() in lowered and t.lower() not in seen:
            seen.add(t.lower())
            tools.append(t)
    return tools, str(data.get("summary", "") or "").strip()


# --------------------------------------------------------------------------
# the main entry point
# --------------------------------------------------------------------------

def _needs(fm, body, videos, images, force):
    done_t = _has_section(body, "Transcript") or _fm_get(fm, "transcript_status") in ("ok", "no_speech")
    done_i = _has_section(body, "Image Text") or _fm_get(fm, "image_text_status") in ("ok", "no_text")
    need_t = bool(videos) and (force or not done_t)
    need_i = bool(images) and (force or not done_i)
    need_tools = force or _fm_get(fm, "enriched_at") is None
    return need_t, need_i, need_tools


def enrich_note(md_path, media_paths, client, force=False, do_tools=True):
    """Enrich one note in place. Never raises; returns a report dict."""
    md_path = Path(md_path)
    report = {"note": md_path.name, "status": "unchanged", "errors": []}
    try:
        text = md_path.read_text(encoding="utf-8")
        fm, body = _split_note(text)
        if fm is None:
            report["status"] = "skipped: no frontmatter"
            return report

        media = [Path(p) for p in media_paths]
        videos = [p for p in media if p.suffix.lower() in VIDEO_EXT]
        images = [p for p in media if p.suffix.lower() in IMAGE_EXT]
        need_t, need_i, need_tools = _needs(fm, body, videos, images, force)
        if not (need_t or need_i or (do_tools and need_tools)):
            return report

        transcript_text, image_text = "", ""

        # ---- video -> transcript
        if need_t:
            if force:
                body = _drop_section(body, "Transcript")
            results, errs = [], []
            for i, v in enumerate(videos, 1):
                try:
                    results.append((i, transcribe_video(v, client)))
                except Exception as e:
                    errs.append("video %d: %s" % (i, e))
            report["errors"] += errs
            ok = [(i, t) for i, (s, t) in results if s == "ok"]
            if ok:
                label = lambda i: ("### Video %d\n\n" % i) if len(videos) > 1 else ""
                transcript_text = "\n\n".join(label(i) + t for i, t in ok)
                body = body.rstrip() + (
                    "\n\n## Transcript\n\n> Auto-transcribed with %s. May contain errors.\n\n%s\n"
                    % (WHISPER_MODEL, transcript_text))
                fm = _fm_set(fm, "transcript_status", _q("ok"))
            elif results and not errs:
                fm = _fm_set(fm, "transcript_status", _q("no_speech"))
            else:
                fm = _fm_set(fm, "transcript_status", _q("failed"))

        # ---- images -> on-image text
        if need_i:
            if force:
                body = _drop_section(body, "Image Text")
            blocks, errs, empty = [], [], 0
            for i, img in enumerate(images, 1):
                try:
                    t = read_image(img, client)
                    if t:
                        blocks.append("### Slide %d\n\n%s" % (i, t))
                    else:
                        empty += 1
                except Exception as e:
                    errs.append("image %d: %s" % (i, e))
            report["errors"] += errs
            if blocks:
                image_text = "\n\n".join(blocks)
                body = body.rstrip() + (
                    "\n\n## Image Text\n\n> Auto-read from images with %s. May contain errors.\n\n%s\n"
                    % (VISION_MODEL, image_text))
                fm = _fm_set(fm, "image_text_status", _q("partial" if errs else "ok"))
            elif errs:
                fm = _fm_set(fm, "image_text_status", _q("failed"))
            else:
                fm = _fm_set(fm, "image_text_status", _q("no_text"))

        # ---- links (deterministic) + tools/summary (LLM)
        if do_tools and need_tools:
            if not transcript_text:
                transcript_text = _section_text(body, "Transcript")
            if not image_text:
                image_text = _section_text(body, "Image Text")
            corpus = "\n\n".join(x for x in (_description(body), _section_text(body, "Links on the page"),
                                             transcript_text, image_text) if x)
            links = extract_links(corpus)
            tools, summary = [], ""
            try:
                tools, summary = extract_tools(corpus, client)
            except Exception as e:
                report["errors"].append("tools: %s" % e)
            fm = _fm_set(fm, "links", _qlist(links))
            fm = _fm_set(fm, "tools_mentioned", _qlist(tools))
            if summary:
                fm = _fm_set(fm, "summary_auto", _q(summary))

        # Only stamp enriched_at when no stage failed, so a rerun retries failures.
        if not report["errors"]:
            fm = _fm_set(fm, "enriched_at", _q(datetime.now().strftime("%Y-%m-%dT%H:%M")))

        new_text = "---\n" + fm + "\n---\n" + body
        if new_text != text:
            _atomic_write(md_path, new_text)
            report["status"] = "enriched" if not report["errors"] else "enriched_with_errors"
        return report
    except Exception as e:  # never break the caller
        report["status"] = "failed"
        report["errors"].append(str(e))
        return report


# --------------------------------------------------------------------------
# backfill CLI
# --------------------------------------------------------------------------

def _video_minutes(path):
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                            "-of", "default=nw=1:nk=1", str(path)],
                           capture_output=True, text=True, timeout=30)
        return float(r.stdout.strip()) / 60.0
    except Exception:
        return 0.0


def cmd_backfill(args):
    vault = Path(args.vault)
    inst, assets = vault / args.folder, vault / "assets"
    if not inst.is_dir():
        sys.exit("Note folder not found: %s" % inst)
    idx = build_index(assets, inst)

    notes = sorted(p for p in inst.glob("*.md") if not p.name.startswith("._"))
    if args.only:
        needle = _nfc(args.only).lower()
        notes = [p for p in notes if needle in _nfc(p.name).lower()]
    if args.limit:
        notes = notes[:args.limit]

    client = None
    backup_dir = None
    if args.apply:
        load_env_file(Path(__file__).resolve().parent / ".env")
        if not os.environ.get("OPENAI_API_KEY"):
            sys.exit("OPENAI_API_KEY not set (looked in the environment and the bot's .env).")
        from openai import OpenAI
        client = OpenAI()
        backup_dir = Path(args.backup_dir or Path.home() / "ig_enrich_backup" / datetime.now().strftime("%Y%m%d_%H%M%S"))
        backup_dir.mkdir(parents=True, exist_ok=True)
        print("Backups of changed notes go to:", backup_dir)

    totals = {"notes": len(notes), "pending": 0, "videos": 0, "minutes": 0.0, "images": 0,
              "missing_media": 0, "enriched": 0, "failed": 0}
    rows = []
    for n, note in enumerate(notes, 1):
        text = note.read_text(encoding="utf-8", errors="ignore")
        fm, body = _split_note(text)
        if fm is None:
            continue
        media, missing = resolve_media(text, idx)
        totals["missing_media"] += len(missing)
        videos = [p for p in media if p.suffix.lower() in VIDEO_EXT]
        images = [p for p in media if p.suffix.lower() in IMAGE_EXT]
        need_t, need_i, need_tools = _needs(fm, body, videos, images, args.force)
        pending = need_t or need_i or ((not args.no_tools) and need_tools)
        if not pending:
            continue
        totals["pending"] += 1
        if need_t:
            totals["videos"] += len(videos)
            if not args.apply:
                totals["minutes"] += sum(_video_minutes(v) for v in videos)
        if need_i:
            totals["images"] += len(images)
        row = {"note": note.name, "videos": len(videos) if need_t else 0,
               "images": len(images) if need_i else 0, "missing": missing}
        if args.apply:
            shutil.copy2(str(note), str(backup_dir / note.name))
            rep = enrich_note(note, media, client, force=args.force, do_tools=not args.no_tools)
            row["result"] = rep["status"]
            row["errors"] = rep["errors"]
            if rep["status"] == "enriched" and not rep["errors"]:
                totals["enriched"] += 1
            elif rep["status"] == "unchanged":
                totals["unchanged"] = totals.get("unchanged", 0) + 1
            else:
                totals["failed"] += 1
            print("[%d/%d] %s -> %s %s" % (n, len(notes), note.name[:60], rep["status"],
                                           ("; ".join(rep["errors"])[:120] if rep["errors"] else "")))
            time.sleep(args.sleep)
        rows.append(row)

    report_path = Path(args.report)
    report_path.write_text(json.dumps({"totals": totals, "rows": rows}, indent=2, ensure_ascii=False), encoding="utf-8")
    print("\n" + ("APPLIED" if args.apply else "DRY RUN (nothing written)"))
    print(json.dumps(totals, indent=2))
    if not args.apply:
        print("Video minutes to transcribe: %.1f -> multiply by the CURRENT Whisper price per minute." % totals["minutes"])
        print("Re-run with --limit 3 --apply to test on a few notes first.")
    print("Report:", report_path)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill", help="enrich existing notes (dry run unless --apply)")
    b.add_argument("--vault", default="/Volumes/Knowledger/vault/Knowledger")
    b.add_argument("--folder", default="instagram", help="note folder inside the vault: instagram or linkedin")
    b.add_argument("--apply", action="store_true", help="actually call the APIs and write notes")
    b.add_argument("--limit", type=int, default=0, help="only the first N notes")
    b.add_argument("--only", default="", help="only notes whose filename contains this text")
    b.add_argument("--force", action="store_true", help="redo notes that were already enriched")
    b.add_argument("--no-tools", action="store_true", help="skip the links/tools/summary pass")
    b.add_argument("--sleep", type=float, default=0.3, help="seconds between notes")
    b.add_argument("--backup-dir", default="")
    b.add_argument("--report", default="ig_enrich_report.json")
    args = ap.parse_args()
    if args.cmd == "backfill":
        cmd_backfill(args)


if __name__ == "__main__":
    main()
