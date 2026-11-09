#!/usr/bin/env python3
"""
linkedin_capture.py - local receiver for the "Save to Knowledger" Chrome extension.

The extension runs on the LinkedIn post you are viewing and POSTs its URL, text and
images to http://127.0.0.1:<port>/capture. This module then:
  1. de-duplicates by canonical post URL,
  2. writes <vault>/linkedin/<author> - <title>.md and saves images to <vault>/assets/,
  3. reuses ig_enrich to read text off the images and extract links / tools / a summary.

Run it either way:
  * inside the bot:   linkedin_capture.start_in_thread(openai_client)   (hooked in main)
  * standalone:       python3 linkedin_capture.py
  * first-time setup: python3 linkedin_capture.py --init   (creates the token file)

Security model (this is a localhost service that writes files, so it is deliberately strict):
  * binds to 127.0.0.1 only;
  * every request needs the secret token (header X-Capture-Token) - a random web page
    cannot guess it, and without CORS headers the browser blocks its preflight anyway;
  * rejects any Origin that is not a chrome-extension://;
  * validates the URL, caps body/image sizes and sniffs image bytes before writing.
"""

import argparse
import ast
import base64
import subprocess
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, urljoin, parse_qs, parse_qsl, urlencode, urlunsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ig_enrich  # noqa: E402

HERE = Path(__file__).resolve().parent
DEFAULT_VAULT = Path("/Volumes/Knowledger/vault/Knowledger")
HOST = "127.0.0.1"
# NOTE: 8765 is taken by kb_menubar.py's dismiss API, so this uses a different port.
DEFAULT_PORT = int(os.environ.get("LINKEDIN_CAPTURE_PORT", "8791"))
TOKEN_FILE = HERE / "linkedin_capture.token"
EXT_CONFIG = HERE / "linkedin_capture_extension" / "config.js"

MAX_BODY = 60 * 1024 * 1024
MAX_IMAGES = 20
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_TEXT = 60000
LINKEDIN_URL_RE = re.compile(r"^https://([a-z0-9-]+\.)?linkedin\.com/(posts/|feed/update/|pulse/)", re.I)

_capture_lock = threading.Lock()


class CaptureError(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, message)
        self.code = code
        self.message = message


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def read_token():
    tok = os.environ.get("LINKEDIN_CAPTURE_TOKEN", "").strip()
    if not tok and TOKEN_FILE.exists():
        tok = TOKEN_FILE.read_text(encoding="utf-8").strip()
    if len(tok) < 16:
        raise RuntimeError("no capture token (run: python3 linkedin_capture.py --init)")
    return tok


def sanitize(name, limit):
    name = re.sub(r'[<>:"/\\|?*#\[\]\^]', "", name or "").replace("\n", " ").replace("\r", " ")
    name = re.sub(r"\s+", " ", name).strip(" .")
    return name[:limit].strip(" .")


def canon_url(u):
    p = urlsplit((u or "").strip())
    return ("%s%s" % (p.netloc, p.path.rstrip("/"))).lower()


def sniff_ext(data):
    if data[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return ".gif"
    return None


def load_allowed_categories():
    """Read ALLOWED_CATEGORIES from the bot source without importing (or running) the bot."""
    try:
        tree = ast.parse((HERE / "youtube_knowledge_bot.py").read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "ALLOWED_CATEGORIES" for t in node.targets):
                return list(ast.literal_eval(node.value))
    except Exception:
        pass
    return ["technology", "business", "tutorials", "news", "career", "undefined"]


def find_duplicate(vault, key):
    """Return the filename of an existing note with the same canonical LinkedIn URL."""
    candidates = list((vault / "linkedin").glob("*.md")) + list(vault.glob("LinkedIn - *.md"))
    for p in candidates:
        if p.name.startswith("._"):
            continue
        try:
            head = p.read_text(encoding="utf-8", errors="ignore")[:1500]
        except Exception:
            continue
        m = re.search(r'^linkedin_url:\s*"?([^"\n]+)"?', head, re.M)
        if m and canon_url(m.group(1)) == key:
            return p
    return None


PLACEHOLDER = ("_No external links were found in the post or in the comments that were loaded on the page. "
               "If the link is in a comment, scroll down until that comment is visible and click the "
               "extension again; new links will be added to this note._")


def _flat(s, limit):
    return re.sub(r"\s+", " ", str(s or "")).strip()[:limit]


def _clean_links(payload):
    out, seen = [], set()
    for item in (payload.get("links_detail") or [])[:30]:
        if not isinstance(item, dict):
            continue
        u = _unwrap_li(str(item.get("url", "")).strip())
        if not re.match(r"^https?://", u, re.I) or len(u) > 500 or u in seen:
            continue
        seen.add(u)
        where = item.get("where") if item.get("where") in ("post", "comment", "page") else "page"
        out.append({"url": u, "where": where, "by": _flat(item.get("by"), 60),
                    "context": _flat(item.get("context"), 140)})
    return out


SHORT_HOSTS = ("lnkd.in",)
_LI_HOSTS = ("lnkd.in", "linkedin.com", "www.linkedin.com")
_short_cache = {}


LI_WRAPPERS = ("/redir/", "/safety/go")


def _unwrap_li(url):
    """LinkedIn wraps outbound links in /redir/redirect?url=.. and /safety/go/?url=..; return the real target."""
    try:
        q = urlsplit(url)
        if (q.hostname or "").lower().endswith("linkedin.com") and q.path.startswith(LI_WRAPPERS):
            t = parse_qs(q.query).get("url")
            if t and t[0].startswith(("http://", "https://")):
                return t[0]
    except ValueError:
        pass
    return url


def _get_page(url, timeout=8):
    """One request, no redirect following. Returns (location_header_or_None, body_text)."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=timeout) as r:
            return None, r.read(200000).decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        loc = e.headers.get("Location") if e.headers else None
        e.close()
        return loc, ""


_SKIP_HOSTS = ("linkedin.com", "licdn.com", "lnkd.in", "w3.org", "schema.org", "google.com", "gstatic.com")


def _host_ok(u):
    try:
        sp = urlsplit(u)
    except ValueError:
        return False
    h = (sp.hostname or "").lower()
    return sp.scheme in ("http", "https") and bool(h) and not any(h == s or h.endswith("." + s) for s in _SKIP_HOSTS)


def _extract_target(body):
    """lnkd.in answers with a small "you are leaving LinkedIn" page that contains the destination.
    Look in a meta refresh, then in links, then in any URL written in the page (incl. JSON-escaped)."""
    import html as _html
    cands = []
    m = re.search(r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]*content=["\'][^"\']*?url=([^"\']+)', body, re.I)
    if m:
        cands.append(_html.unescape(m.group(1)).strip())
    cands += [_html.unescape(x).strip() for x in re.findall(r'href=["\']([^"\']+)["\']', body, re.I)]
    for x in re.findall(r'https?:\\?/\\?/[^"\'\s<>]+', body):
        cands.append(_html.unescape(x.replace("\\/", "/").replace("\\u0026", "&").replace("\\u003d", "=")).strip())
    for c in cands:
        q = urlsplit(c)
        if (q.hostname or "").lower().endswith("linkedin.com") and q.path.startswith(LI_WRAPPERS):
            t = parse_qs(q.query).get("url")
            if t:
                c = t[0]
        if _host_ok(c):
            return c
    return None


def _save_page_debug(url, body):
    try:
        d = HERE / "linkedin_capture_debug"
        d.mkdir(exist_ok=True)
        code = re.sub(r"[^A-Za-z0-9]", "", urlsplit(url).path)[:20] or "x"
        (d / ("lnkd_%s.html" % code)).write_text(body[:30000], encoding="utf-8")
    except Exception:
        pass


# Affiliate trackers and shorteners: only these are asked for their redirect (never the final page).
_TRACKER_RE = re.compile(r"^(imp\.i\d+\.net|[\w-]+\.(pxf\.io|sjv\.io|ojrq\.net|prf\.hn)|bit\.ly|tinyurl\.com|buff\.ly|ow\.ly|rb\.gy)$")
_TRACK_PARAMS = re.compile(r"^(utm_.*|irclickid|irgwc|clickid|sharedid|mpid|subid\d*|ref|referrer)$", re.I)


def _strip_tracking(u):
    sp = urlsplit(u)
    keep = [(k, v) for k, v in parse_qsl(sp.query, keep_blank_values=True) if not _TRACK_PARAMS.match(k)]
    return urlunsplit((sp.scheme, sp.netloc, sp.path, urlencode(keep), ""))


def _follow_trackers(url, head, max_hops=4):
    """Follow redirect headers across known tracker hosts, then drop tracking parameters.
    The final destination is NOT requested. If a tracker will not give a plain redirect, keep what we have."""
    cur = url
    for _ in range(max_hops):
        h = (urlsplit(cur).hostname or "").lower()
        if urlsplit(cur).scheme != "https" or not _TRACKER_RE.match(h):
            break
        res = head(cur)
        loc, body = res if isinstance(res, tuple) else (res, "")
        if not loc:
            if body:
                _save_page_debug(cur, body)
            return url
        cur = urljoin(cur, loc)
    return _strip_tracking(cur) if cur != url else url


def resolve_short(url, head=None, max_hops=4):
    """Expand a lnkd.in short link WITHOUT visiting the destination: only lnkd.in / linkedin.com
    pages are requested; the destination is read out of their redirect header or page text.
    `head(url)` returns (location, body). Returns the destination URL or None."""
    head = head or _get_page
    if url in _short_cache:
        return _short_cache[url]
    cur, result = url, None
    try:
        for _ in range(max_hops):
            sp = urlsplit(cur)
            if sp.scheme != "https" or (sp.hostname or "").lower() not in _LI_HOSTS:
                result = cur if cur != url else None
                break
            res = head(cur)
            loc, body = res if isinstance(res, tuple) else (res, "")
            if loc:
                nxt = urljoin(cur, loc)
                q = urlsplit(nxt)
                if (q.hostname or "").lower().endswith("linkedin.com") and q.path.startswith(LI_WRAPPERS):
                    t = parse_qs(q.query).get("url")
                    if t:
                        nxt = t[0]
                cur = nxt
                continue
            if body:
                result = _extract_target(body)
                if not result:
                    _save_page_debug(url, body)
            break
        if result:
            result = _follow_trackers(result, head)
    except Exception as e:
        print("LinkedIn capture: could not resolve %s: %s" % (url, e))
        result = None
    _short_cache[url] = result
    return result


def _is_short(u):
    try:
        return (urlsplit(u).hostname or "").lower() in SHORT_HOSTS
    except ValueError:
        return False


def _link_line(l, author):
    if l["where"] == "post":
        label = "in the post"
    elif l["where"] == "comment":
        label = "in a comment" + (" by %s" % l["by"] if l["by"] else "")
        if l["by"] and author and l["by"].lower() == author.lower():
            label += " (the author)"
    else:
        label = "elsewhere on the page (probably a comment)"
    ctx = (' - "%s"' % l["context"]) if l["context"] and l["context"] != l["url"] else ""
    shown = ("%s \u2192 %s" % (l["url"], l["resolved"])) if l.get("resolved") else l["url"]
    return "- %s | %s%s" % (shown, label, ctx)


def _merge_into_existing(path, links, author):
    """Same post captured again: add links that the note does not have yet (e.g. after the
    user scrolled to a comment). Returns a result dict."""
    text = path.read_text(encoding="utf-8")
    fm, body = ig_enrich._split_note(text)
    if fm is None:
        return {"status": "duplicate", "note": path.name}
    existing = set(ig_enrich.extract_links(text))
    new = [l for l in links if l["url"] not in existing]
    if not new:
        return {"status": "duplicate", "note": path.name}
    lines = "\n".join(_link_line(l, author) for l in new)
    pat = re.compile(r"(^## Links on the page\s*$)(.*?)(?=^## |\Z)", re.M | re.S)
    if pat.search(body):
        body = pat.sub(lambda m: m.group(1) + m.group(2).replace(PLACEHOLDER, "").rstrip() + "\n" + lines + "\n\n",
                       body, count=1)
    else:
        body = body.rstrip() + "\n\n## Links on the page\n\n" + lines + "\n"
    old = re.findall(r'"([^"]+)"', (re.search(r"^links:\s*(\[.*\])\s*$", fm, re.M) or [None, ""])[1]) if "links:" in fm else []
    dest = [l.get("resolved") or l["url"] for l in new]     # the real destination, not the lnkd.in short link
    fm = ig_enrich._fm_set(fm, "links", ig_enrich._qlist(old + [u for u in dict.fromkeys(dest) if u not in old]))
    try:
        prev = int(ig_enrich._fm_get(fm, "links_in_comments") or 0)
    except ValueError:
        prev = 0
    fm = ig_enrich._fm_set(fm, "links_in_comments", str(prev + sum(1 for l in new if l["where"] != "post")))
    fm = ig_enrich._fm_set(fm, "page_links_checked", ig_enrich._q(datetime.now().strftime("%Y-%m-%dT%H:%M")))
    ig_enrich._atomic_write(path, "---\n" + fm + "\n---\n" + body)
    return {"status": "updated", "note": path.name, "links_added": len(new)}


def title_and_categories(text, client, allowed):
    fallback_title = sanitize((text or "").strip().splitlines()[0] if (text or "").strip() else "LinkedIn post", 60) or "LinkedIn post"
    if client is None or len((text or "").strip()) < 20:
        return fallback_title, []
    try:
        r = client.chat.completions.create(
            model=ig_enrich.TEXT_MODEL,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": (
                    "Given a LinkedIn post, return JSON: "
                    '{"title": "a descriptive title of at most 8 words, no emojis, no quotes", '
                    '"categories": ["1 to 3 values chosen ONLY from this list: ' + ", ".join(allowed) + '"]}')},
                {"role": "user", "content": text[:3000]},
            ],
        )
        data = json.loads(r.choices[0].message.content or "{}")
        title = sanitize(str(data.get("title", "")), 60) or fallback_title
        cats = [c for c in (data.get("categories") or []) if c in allowed][:3]
        return title, cats
    except Exception as e:
        print("LinkedIn capture: title/category call failed: %s" % e)
        return fallback_title, []


def _save_diagnostics(url, text_source, diag):
    """Save a small structural description of the page (no full text) so selectors can be tuned."""
    try:
        blob = json.dumps({"url": url, "text_source": text_source,
                           "saved_at": datetime.now().isoformat(timespec="seconds"), "diagnostics": diag},
                          ensure_ascii=False, indent=1)
        if len(blob) > 200 * 1024:
            return False
        d = HERE / "linkedin_capture_debug"
        d.mkdir(exist_ok=True)
        (d / (datetime.now().strftime("%Y%m%d_%H%M%S") + ".json")).write_text(blob, encoding="utf-8")
        return True
    except Exception as e:
        print("LinkedIn capture: could not save diagnostics: %s" % e)
        return False


def _decode_images(payload):
    out = []
    for item in (payload.get("images") or [])[:MAX_IMAGES]:
        try:
            data = base64.b64decode(item.get("b64", ""), validate=False)
        except Exception:
            continue
        if not data or len(data) > MAX_IMAGE_BYTES:
            continue
        ext = sniff_ext(data)
        if ext:
            out.append((data, ext, bool(item.get("poster"))))
        else:
            # keep the first bytes so an unknown format can be identified instead of vanishing silently
            payload.setdefault("_skipped", []).append("%s (%d bytes)" % (data[:12].hex(), len(data)))
    return out


# --------------------------------------------------------------------------
# video download (LinkedIn streams video through a blob: URL, so the extension sends the
# stream addresses it saw; the real file is fetched here with ffmpeg)
# --------------------------------------------------------------------------

MAX_VIDEO_BYTES = 400 * 1024 * 1024
MAX_SEGMENTS = 800
SEG_RE = re.compile(r"^(?P<dir>.*/)(?P<n>\d+)/(?P<v>\d+)$")
VIDEO_TIMEOUT = 900


def _video_candidates(payload):
    """Only https addresses on *.licdn.com; manifests (m3u8/mpd) before plain mp4 files."""
    out, seen = [], set()
    for u in (payload.get("video_candidates") or [])[:800]:
        u = str(u or "").strip()
        try:
            sp = urlsplit(u)
        except ValueError:
            continue
        host = (sp.hostname or "").lower()
        if sp.scheme != "https" or "@" in sp.netloc or not (host == "licdn.com" or host.endswith(".licdn.com")):
            continue
        if u in seen:
            continue
        seen.add(u)
        path = sp.path.lower()
        rank = 0 if path.endswith((".m3u8", ".mpd")) else (1 if path.endswith(".mp4") else 2)
        out.append((rank, u))
    out.sort(key=lambda t: t[0])
    return [u for _, u in out]


def _segment_group(candidates):
    """LinkedIn's player fetches numbered fragments .../<rendition>/<n>/<id>. Group them by
    rendition and return the largest group as [(n, url)] sorted by n (n=1 is usually the init part)."""
    groups = {}
    for u in candidates:
        sp = urlsplit(u)
        m = SEG_RE.match(sp.path)
        if not m:
            continue
        groups.setdefault((sp.netloc, m.group("dir"), m.group("v")), {})[int(m.group("n"))] = u
    if not groups:
        return []
    best = max(groups.values(), key=len)
    return sorted(best.items())[:MAX_SEGMENTS]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


def _fetch(url, opener=None, timeout=60):
    opener = opener or urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.linkedin.com/"})
    with opener.open(req, timeout=timeout) as r:
        return r.read()


def download_segments(segs, dest, fetch=None, runner=None):
    """Join init + fragments into one file and remux it with ffmpeg. Returns (ok, reason)."""
    fetch = fetch or _fetch
    runner = runner or subprocess.run
    raw = Path(str(dest) + ".raw.mp4")
    tmp = Path(str(dest) + ".part.mp4")
    got, missing, total = [], [], 0
    try:
        with open(raw, "wb") as fh:
            for n, u in segs:
                data = None
                for _ in range(2):
                    try:
                        data = fetch(u)
                        break
                    except Exception as e:
                        err = "%s %s" % (type(e).__name__, str(e)[:80])
                if data is None:
                    missing.append(n)
                    if not got:
                        return False, "first fragment could not be fetched (%s)" % err
                    continue
                total += len(data)
                if total > MAX_VIDEO_BYTES:
                    return False, "video larger than %d MB" % (MAX_VIDEO_BYTES // 1048576)
                fh.write(data)
                got.append(n)
        cmd = ["ffmpeg", "-y", "-v", "error", "-i", str(raw), "-c", "copy", "-movflags", "+faststart", str(tmp)]
        r = runner(cmd, capture_output=True, text=True, timeout=VIDEO_TIMEOUT)
        if r.returncode != 0 or not tmp.exists() or tmp.stat().st_size < 20 * 1024:
            err = _scrub((r.stderr or "").strip())[:300].replace("\n", " | ")
            _log_video_failure(["segments %s..%s fetched=%d missing=%s rc=%s\n%s" % (
                segs[0][0], segs[-1][0], len(got), missing[:20], r.returncode, err)])
            return False, "could not assemble %d fragments (ffmpeg rc %s): %s" % (len(got), r.returncode, err)
        tmp.replace(dest)
        note = ""
        if missing:
            note = "video is incomplete: fragments %s could not be fetched" % missing[:10]
        elif len(got) <= 3:
            note = "only %d fragments were available, so the video may be cut short" % len(got)
        return True, note
    finally:
        for f in (raw, tmp):
            try:
                if f.exists():
                    f.unlink()
            except Exception:
                pass


def _scrub(s):
    """Drop signed query strings from any URL in ffmpeg's output before it is stored or shown."""
    return re.sub(r"(https?://[^\s?'\"]+)\?[^\s'\"]*", r"\1?<token>", s or "")


def _log_video_failure(attempts):
    try:
        d = HERE / "linkedin_capture_debug"
        d.mkdir(exist_ok=True)
        f = d / ("video_%s.txt" % datetime.now().strftime("%Y%m%d_%H%M%S"))
        f.write_text("\n\n".join(attempts), encoding="utf-8")
    except Exception:
        pass


def download_video(candidates, dest, runner=None):
    """Try each address with ffmpeg (stream copy, no re-encode). Returns (ok, reason).
    Two attempts per address: with a browser-like User-Agent/Referer, then plain."""
    if not candidates:
        return False, "no stream address was seen on the page (press play for a second, then click again)"
    runner = runner or subprocess.run
    tmp = Path(str(dest) + ".part.mp4")
    base = ["ffmpeg", "-y", "-v", "error", "-protocol_whitelist", "http,https,tls,tcp,crypto"]
    tail = ["-c", "copy", "-fs", str(MAX_VIDEO_BYTES), "-movflags", "+faststart", str(tmp)]
    attempts, last = [], "unknown"
    try:
        for u in candidates[:4]:
            for variant, opts in (("with-headers", ["-user_agent", "Mozilla/5.0",
                                                    "-headers", "Referer: https://www.linkedin.com/\r\n"]),
                                  ("plain", [])):
                try:
                    if tmp.exists():
                        tmp.unlink()
                    r = runner(base + opts + ["-i", u] + tail, capture_output=True, text=True, timeout=VIDEO_TIMEOUT)
                    err = _scrub((r.stderr or "").strip())
                    if r.returncode == 0 and tmp.exists() and tmp.stat().st_size >= 20 * 1024:
                        tmp.replace(dest)
                        return True, ""
                    last = ("ffmpeg (rc %s): %s" % (r.returncode, err[:260].replace("\n", " | "))) if r.returncode \
                        else "downloaded file was empty"
                    attempts.append("%s [%s] rc=%s\n%s" % (_scrub(u), variant, r.returncode, err[:2000]))
                except Exception as e:
                    last = "%s: %s" % (type(e).__name__, _scrub(str(e))[:160])
                    attempts.append("%s [%s] exception\n%s" % (_scrub(u), variant, last))
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
    _log_video_failure(attempts)
    return False, last


# --------------------------------------------------------------------------
# the capture itself
# --------------------------------------------------------------------------

def capture(payload, client, vault, allowed):
    vault = Path(vault)
    url = str(payload.get("url", "")).strip()
    if not LINKEDIN_URL_RE.match(url):
        raise CaptureError(400, "Not a LinkedIn post URL (open the post on its own page).")
    sp = urlsplit(url)
    url = "https://%s%s" % (sp.netloc, sp.path)  # drop tracking query string / fragment
    text = str(payload.get("text", "")).strip()[:MAX_TEXT]
    images = _decode_images(payload)
    if len(text) < 1 and not images:
        saved = isinstance(payload.get("diagnostics"), dict) and _save_diagnostics(
            url, str(payload.get("text_source", "")), payload["diagnostics"])
        raise CaptureError(422, "No post text or images found on the page." +
                           (" Page diagnostics saved (linkedin_capture_debug/)." if saved else ""))
    author = sanitize(str(payload.get("author", "")), 40) or "Unknown"
    text_source = str(payload.get("text_source", "") or "")[:80]
    links = _clean_links(payload)
    # links written in the post text are not always <a> elements on the page: list them too
    have = {l["url"] for l in links}
    for u in ig_enrich.extract_links(text):
        if u not in have:
            links.append({"url": u, "where": "post", "by": "", "anchor": "", "context": ""})
            have.add(u)
    deadline = time.time() + 30
    for l in links[:20]:
        if _is_short(l["url"]) and time.time() < deadline:
            r = resolve_short(l["url"])
            if r:
                l["resolved"] = r
    has_video = bool(payload.get("has_video"))
    warnings = []
    if text_source.startswith("og:"):
        warnings.append("Only the truncated preview text was found; the note may be incomplete.")
    found = int(payload.get("image_urls_found") or 0)
    if found and not images:
        sk = payload.get("_skipped")
        warnings.append("Images were found on the page but none could be saved" +
                        ((" (unrecognised format, first bytes: %s)" % "; ".join(sk[:2])) if sk else " (download failed)") + ".")
    elif payload.get("_skipped"):
        warnings.append("%d image(s) skipped: unrecognised format (%s)." %
                        (len(payload["_skipped"]), "; ".join(payload["_skipped"][:2])))

    if not text:
        warnings.append("No post text found on the page.")
    if author == "Unknown":
        warnings.append("Author not found.")
    if text_source.startswith("heuristic"):
        warnings.append("Text came from a fallback heuristic; check it.")
    if not links:
        warnings.append("No external links found; if the link is in a comment, scroll to it and click again.")
    diag = payload.get("diagnostics")
    if isinstance(diag, dict) and _save_diagnostics(url, text_source, diag):
        warnings.append("Page diagnostics saved (linkedin_capture_debug/).")

    with _capture_lock:
        key = canon_url(url)
        dup = find_duplicate(vault, key)
        if dup:
            return _merge_into_existing(dup, links, author)

        title, cats = title_and_categories(text, client, allowed)
        out_dir, assets = vault / "linkedin", vault / "assets"
        out_dir.mkdir(parents=True, exist_ok=True)
        assets.mkdir(parents=True, exist_ok=True)

        base = "%s - %s" % (author, title)
        note_path, n = out_dir / (base + ".md"), 1
        while note_path.exists():
            note_path = out_dir / ("%s_%d.md" % (base, n))
            n += 1

        video_path, video_reason = None, ""
        if has_video:
            vp = assets / ("LinkedIn - %s.mp4" % note_path.stem)
            cands = _video_candidates(payload)
            seg = _segment_group(cands)
            if len(seg) >= 2:
                ok, video_reason = download_segments(seg, vp)
                if not ok:  # fall back to treating the addresses as playlists / whole files
                    ok2, r2 = download_video([u for u in cands if not SEG_RE.match(urlsplit(u).path)], vp)
                    if ok2:
                        ok, video_reason = True, ""
            else:
                ok, video_reason = download_video(cands, vp)
            if ok:
                video_path = vp
                if video_reason:
                    warnings.append("Video saved, but " + video_reason + ".")
            else:
                warnings.append("Video not captured (preview frame kept instead): %s." % video_reason)
        # The video is in the note, so the static preview frame LinkedIn shows before playback is dropped.
        # If the video could not be saved, that frame is kept as the only visual.
        if video_path:
            images = [x for x in images if not x[2]]

        image_paths = []
        for i, (data, ext, _poster) in enumerate(images, 1):
            p = assets / ("LinkedIn - %s_%d%s" % (note_path.stem, i, ext))
            k = 1
            while p.exists():
                p = assets / ("LinkedIn - %s_%d_dup%d%s" % (note_path.stem, i, k, ext))
                k += 1
            p.write_bytes(data)
            image_paths.append(p)

        tags = ", ".join(["linkedin"] + cats)
        embeds = "\n\n".join("![[%s]]" % p.name for p in image_paths + ([video_path] if video_path else []))
        note = (
            "---\n"
            'platform: "linkedin"\n'
            "title: %s\n"
            "author: %s\n"
            "linkedin_url: %s\n"
            'captured_via: "browser-extension"\n'
            "captured_at: %s\n"
            "text_source: %s\n"
            "tags: [%s]\n"
            "---\n\n"
            "# %s\n\n"
            "**Platform:** LinkedIn\n"
            "**Author:** %s\n"
            "**Tags:** %s\n"
            "**LinkedIn:** [%s](%s)\n\n"
            "---\n\n"
            "%s"
            "## Post Content\n\n%s\n"
        ) % (
            ig_enrich._q(title), ig_enrich._q(author), ig_enrich._q(url),
            ig_enrich._q(datetime.now().strftime("%Y-%m-%dT%H:%M")), ig_enrich._q(text_source),
            tags, title, author, tags, url, url,
            (embeds + "\n\n---\n\n") if embeds else "",
            text or "(no text; image-only post)",
        )
        # links section + frontmatter flags, so "no links" is never ambiguous
        fm_, body_ = ig_enrich._split_note(note)
        fm_ = ig_enrich._fm_set(fm_, "page_links_checked", ig_enrich._q(datetime.now().strftime("%Y-%m-%dT%H:%M")))
        fm_ = ig_enrich._fm_set(fm_, "links_in_comments", str(sum(1 for l in links if l["where"] != "post")))
        if has_video:
            fm_ = ig_enrich._fm_set(fm_, "has_video", "true")
            fm_ = ig_enrich._fm_set(fm_, "video_saved", "true" if video_path else "false")
        link_lines = "\n".join(_link_line(l, author) for l in links) if links else PLACEHOLDER
        body_ = body_.rstrip() + "\n\n## Links on the page\n\n" + link_lines + "\n"
        note = "---\n" + fm_ + "\n---\n" + body_
        ig_enrich._atomic_write(note_path, note)

        enrich_status = "skipped"
        if client is not None:
            rep = ig_enrich.enrich_note(note_path, image_paths + ([video_path] if video_path else []), client)
            enrich_status = rep["status"]
            if rep["errors"]:
                warnings.append("Enrichment problems: " + "; ".join(rep["errors"])[:200])

        # capture notes go last: what was and was not captured
        # Only worth a section when something needs the reader's attention.
        cap = []
        if has_video and not video_path:
            cap.append("A video is present on this post. Only its poster frame was captured; "
                       "the video itself (speech, demo) is NOT in this note. Reason: %s." % video_reason)
        ts = text_source or "unknown"
        if ts == "unknown" or ts.startswith(("heuristic", "og:", "user-selection", "none")):
            cap.append("Post text came from a fallback (%s) and may be incomplete or wrong; "
                       "check it against the post." % ts)
        if cap:
            try:
                t = note_path.read_text(encoding="utf-8")
                ig_enrich._atomic_write(note_path, t.rstrip() + "\n\n## Capture notes\n\n" +
                                        "\n".join("- " + c for c in cap) + "\n")
            except Exception as e:
                print("LinkedIn capture: could not append capture notes: %s" % e)

    return {"status": "saved", "note": note_path.name, "images_saved": len(image_paths),
            "images_found": found, "enrich": enrich_status, "warnings": warnings}


# --------------------------------------------------------------------------
# HTTP server
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "KnowledgerCapture/0.1"
    client = None
    vault = DEFAULT_VAULT
    token = ""
    allowed = []

    def log_message(self, fmt, *args):  # keep the bot log quiet
        pass

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self._send(200, {"ok": True, "service": "knowledger-capture"})
        self._send(404, {"error": "not found"})

    def do_OPTIONS(self):  # deliberately no CORS headers -> browsers refuse cross-site preflights
        self._send(403, {"error": "forbidden"})

    def do_POST(self):
        try:
            if self.path != "/capture":
                return self._send(404, {"error": "not found"})
            origin = self.headers.get("Origin", "")
            if origin and not origin.startswith("chrome-extension://"):
                return self._send(403, {"error": "forbidden origin"})
            if not hmac.compare_digest(self.headers.get("X-Capture-Token", ""), self.token):
                return self._send(401, {"error": "bad token"})
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length <= 0 or length > MAX_BODY:
                return self._send(413, {"error": "body missing or too large"})
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict):
                raise CaptureError(400, "payload must be a JSON object")
            result = capture(payload, self.client, self.vault, self.allowed)
            print("LinkedIn capture: %s %s" % (result.get("status"), result.get("note", "")))
            self._send(200, result)
        except CaptureError as e:
            self._send(e.code, {"error": e.message})
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid JSON"})
        except Exception as e:
            print("LinkedIn capture: unexpected error: %s" % e)
            self._send(500, {"error": "internal error (see bot log)"})


def make_server(client, vault=DEFAULT_VAULT, token=None, port=DEFAULT_PORT):
    handler = type("BoundHandler", (Handler,), {
        "client": client, "vault": Path(vault), "token": token or read_token(),
        "allowed": load_allowed_categories(),
    })
    return ThreadingHTTPServer((HOST, port), handler)


def start_in_thread(client, vault=None, port=DEFAULT_PORT):
    """Start the receiver inside the bot process. Never raises."""
    try:
        srv = make_server(client, vault or DEFAULT_VAULT, port=port)
        threading.Thread(target=srv.serve_forever, daemon=True, name="linkedin-capture").start()
        print("LinkedIn capture listening on http://%s:%d/capture" % (HOST, port))
        return srv
    except Exception as e:
        print("LinkedIn capture server not started: %s" % e)
        return None


def init_token():
    if TOKEN_FILE.exists() and len(TOKEN_FILE.read_text().strip()) >= 16:
        tok = TOKEN_FILE.read_text().strip()
        print("Token file already exists:", TOKEN_FILE)
    else:
        tok = secrets.token_urlsafe(24)
        TOKEN_FILE.write_text(tok + "\n", encoding="utf-8")
        os.chmod(str(TOKEN_FILE), 0o600)
        print("Created token file:", TOKEN_FILE)
    EXT_CONFIG.parent.mkdir(exist_ok=True)
    EXT_CONFIG.write_text(
        'const CAPTURE_URL = "http://%s:%d/capture";\nconst CAPTURE_TOKEN = "%s";\n' % (HOST, DEFAULT_PORT, tok),
        encoding="utf-8")
    print("Wrote extension config:", EXT_CONFIG)


def apply_resolutions(text, mapping):
    """Add 'short -> real' lines to a note's "Links on the page" section and the real URLs to the
    frontmatter `links`. mapping = {short_url: real_url}. Returns the new text (unchanged if nothing to do)."""
    fm, body = ig_enrich._split_note(text)
    if fm is None or not mapping:
        return text
    sec = re.search(r"^## Links on the page\s*$(.*?)(?=^## |\Z)", body, re.M | re.S)
    listed = sec.group(1) if sec else ""
    changed = False
    for s, r in mapping.items():
        pat = re.compile(r"^(- %s \u2192 )(\S+)" % re.escape(s), re.M)
        m = pat.search(body)
        if m and m.group(2) != r:
            old_url = m.group(2)
            body = pat.sub(lambda mm: mm.group(1) + r, body, count=1)
            if "links:" in fm:
                fm = fm.replace('"%s"' % old_url, '"%s"' % r)
            changed = True
    sec = re.search(r"^## Links on the page\s*$(.*?)(?=^## |\Z)", body, re.M | re.S)
    listed = sec.group(1) if sec else ""
    new = [(s, r) for s, r in mapping.items() if ("- %s \u2192" % s) not in listed]
    if not new:
        return ("---\n" + fm + "\n---\n" + body) if changed else text
    lines = "\n".join("- %s \u2192 %s | in the post" % (s, r) for s, r in new)
    if sec:
        keep = listed.replace(PLACEHOLDER, "").rstrip()
        body = body[:sec.start()] + "## Links on the page\n" + (keep + "\n" if keep.strip() else "\n") + lines + "\n\n" + body[sec.end():]
    else:
        body = body.rstrip() + "\n\n## Links on the page\n\n" + lines + "\n"
    old = re.findall(r'"([^"]+)"', (re.search(r"^links:\s*(\[.*\])\s*$", fm, re.M) or [None, ""])[1]) if "links:" in fm else []
    add = [r for _, r in new if r not in old]
    fm = ig_enrich._fm_set(fm, "links", ig_enrich._qlist(old + add))
    return "---\n" + fm + "\n---\n" + body


def resolve_in_notes(vault, apply=False, head=None):
    d = Path(vault) / "linkedin"
    done = 0
    for f in sorted(d.glob("*.md")):
        if f.name.startswith("._"):
            continue
        text = f.read_text(encoding="utf-8")
        shorts = [u for u in ig_enrich.extract_links(text) if _is_short(u)]
        mapping = {}
        for u in shorts:
            r = resolve_short(u, head=head)
            if r:
                mapping[u] = r
        out = apply_resolutions(text, mapping)
        print("%s: %d short link(s), %d resolved%s" % (f.name, len(shorts), len(mapping),
                                                       "" if out != text else " (nothing to change)"))
        for s, r in mapping.items():
            print("   %s -> %s" % (s, r))
        if out != text:
            done += 1
            if apply:
                ig_enrich._atomic_write(f, out)
    print("%s %d note(s)." % ("Updated" if apply else "Would update (dry run; add --apply to write)", done))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--vault", default=str(DEFAULT_VAULT))
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--init", action="store_true", help="create the token file and the extension config")
    ap.add_argument("--resolve-links", action="store_true", help="expand lnkd.in short links in existing notes")
    ap.add_argument("--apply", action="store_true", help="with --resolve-links: write the changes")
    args = ap.parse_args()
    if args.resolve_links:
        return resolve_in_notes(args.vault, apply=args.apply)
    if args.init:
        return init_token()
    ig_enrich.load_env_file(HERE / ".env")
    client = None
    if os.environ.get("OPENAI_API_KEY"):
        from openai import OpenAI
        client = OpenAI()
    else:
        print("Warning: OPENAI_API_KEY not set - notes will be saved without titles, image text or tools.")
    srv = make_server(client, args.vault, port=args.port)
    print("Listening on http://%s:%d/capture  (vault: %s)" % (HOST, args.port, args.vault))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
