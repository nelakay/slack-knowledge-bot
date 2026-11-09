"""web_resources.py - turn a shared web link into a searchable knowledge note.

For a link that is NOT YouTube / Instagram / LinkedIn: read the page (or the GitHub repo + README),
ask an LLM what the thing is and what it does, and save <vault>/web/<domain> - <title>.md.
The goal is to be able to search the vault later for "a tool that does X".

Safety and honesty rules baked in:
  * only public http(s) addresses are fetched (private/loopback/link-local ranges are refused,
    every redirect hop is re-checked), with size and time limits;
  * the LLM may only use the text it is given; anything not stated must be "not stated";
  * a page that could not be read produces a clearly marked stub, never an invented summary, and
    the same link is retried the next time it is shared.

Used by youtube_knowledge_bot.process_resource_links(). Never raises out of process_url().
"""

import html as html_mod
import ipaddress
import json
import os
import re
import socket
import unicodedata
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, urljoin, parse_qsl, urlencode, quote

import requests

import ig_enrich

TEXT_MODEL = os.environ.get("WEB_TEXT_MODEL", ig_enrich.TEXT_MODEL)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
MAX_BYTES = 2_500_000
MAX_TEXT_CHARS = 14000
MAX_REDIRECTS = 5
TIMEOUT = (6, 15)
FOLDER = "web"

SKIP_HOSTS = (
    "youtube.com", "youtu.be", "instagram.com", "linkedin.com", "lnkd.in", "licdn.com",
    "slack.com", "slack-files.com", "slack-edge.com",
)
TRACK_PARAMS = re.compile(r"^(utm_.*|fbclid|gclid|dclid|msclkid|mc_cid|mc_eid|igshid|irclickid|irgwc|ref_src|ref|"
                          r"si|feature|spm|_hsenc|_hsmi|yclid)$", re.I)
BINARY_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".mp4", ".mov", ".webm", ".mp3", ".wav",
              ".zip", ".gz", ".tar", ".dmg", ".exe", ".pkg", ".apk")

NOT_STATED = "not stated"


# --------------------------------------------------------------------------
# URLs
# --------------------------------------------------------------------------

def canonical_url(url):
    sp = urlsplit(url.strip())
    host = (sp.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    port = ":%d" % sp.port if sp.port and sp.port not in (80, 443) else ""
    path = sp.path.rstrip("/") or ""
    q = urlencode([(k, v) for k, v in parse_qsl(sp.query, keep_blank_values=True) if not TRACK_PARAMS.match(k)])
    return urlunsplit(("https" if sp.scheme in ("http", "https") else sp.scheme, host + port, path, q, ""))


def is_skipped(url):
    """YouTube, Instagram, LinkedIn (own pipelines), Slack internals, and direct media/archive files."""
    try:
        sp = urlsplit(url)
    except ValueError:
        return True
    host = (sp.hostname or "").lower()
    if sp.scheme not in ("http", "https") or not host:
        return True
    if any(host == h or host.endswith("." + h) for h in SKIP_HOSTS):
        return True
    return sp.path.lower().endswith(BINARY_EXT)


def _public_host(host):
    """True only if every address the name resolves to is a public one."""
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return False
    if not infos:
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0].split("%")[0])
        except ValueError:
            return False
        if not ip.is_global:
            return False
    return True


# --------------------------------------------------------------------------
# fetching
# --------------------------------------------------------------------------

class FetchError(Exception):
    def __init__(self, msg, unsafe=False):
        super().__init__(msg)
        self.unsafe = unsafe


def fetch_page(url, allow_private=False, session=None):
    """Return (final_url, content_type, text). Follows redirects by hand so each hop is checked."""
    s = session or requests
    cur = url
    for _ in range(MAX_REDIRECTS + 1):
        sp = urlsplit(cur)
        if sp.scheme not in ("http", "https") or not sp.hostname:
            raise FetchError("not an http(s) address")
        if not allow_private and not _public_host(sp.hostname):
            raise FetchError("address is not public (blocked for safety)", unsafe=True)
        try:
            r = s.get(cur, headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.5",
                                    "Accept-Language": "en"},
                      timeout=TIMEOUT, allow_redirects=False, stream=True)
        except requests.RequestException as e:
            raise FetchError("%s" % type(e).__name__)
        try:
            if r.status_code in (301, 302, 303, 307, 308) and r.headers.get("Location"):
                cur = urljoin(cur, r.headers["Location"])
                continue
            if r.status_code >= 400:
                raise FetchError("HTTP %d" % r.status_code)
            ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if ctype and not (ctype.startswith("text/") or "html" in ctype or "xml" in ctype or "json" in ctype):
                raise FetchError("not a web page (%s)" % ctype)
            chunks, size = [], 0
            for chunk in r.iter_content(65536):
                chunks.append(chunk)
                size += len(chunk)
                if size > MAX_BYTES:
                    break
            raw = b"".join(chunks)
            enc = r.encoding or "utf-8"
            try:
                text = raw.decode(enc, "replace")
            except LookupError:
                text = raw.decode("utf-8", "replace")
            return cur, ctype, text
        finally:
            r.close()
    raise FetchError("too many redirects")


# --------------------------------------------------------------------------
# HTML -> text
# --------------------------------------------------------------------------

class _Extract(HTMLParser):
    SKIP = {"script", "style", "noscript", "svg", "template", "iframe", "nav", "footer", "aside", "form", "button", "select"}
    BLOCK = {"p", "div", "li", "br", "tr", "section", "article", "main", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "pre", "blockquote", "table"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title = ""
        self.meta = {}
        self.headings = []
        self.parts = []
        self._skip = 0
        self._in_title = False
        self._head = None
        self._jsonld = False
        self.jsonld = []
        self._cur_ld = []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "title":
            self._in_title = True
        elif tag == "meta":
            k = (a.get("property") or a.get("name") or "").lower()
            if k and a.get("content"):
                self.meta.setdefault(k, a["content"].strip())
        elif tag == "script" and (a.get("type") or "").lower() == "application/ld+json":
            self._jsonld = True
            self._cur_ld = []
        if tag in self.SKIP:
            self._skip += 1
        if tag in ("h1", "h2", "h3"):
            self._head = []
        if tag in self.BLOCK:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        if tag == "script" and self._jsonld:
            self._jsonld = False
            self.jsonld.append("".join(self._cur_ld))
        if tag in self.SKIP and self._skip:
            self._skip -= 1
        if tag in ("h1", "h2", "h3") and self._head is not None:
            h = " ".join("".join(self._head).split())
            if h and len(self.headings) < 40:
                self.headings.append(h)
            self._head = None
        if tag in self.BLOCK:
            self.parts.append("\n")

    def handle_data(self, data):
        if self._in_title:
            self.title += data
        if self._jsonld:
            self._cur_ld.append(data)
            return
        if self._skip:
            return
        if self._head is not None:
            self._head.append(data)
        self.parts.append(data)


def extract_page(html_text):
    p = _Extract()
    try:
        p.feed(html_text[:MAX_BYTES])
    except Exception:
        pass
    lines, seen = [], set()
    for ln in "".join(p.parts).splitlines():
        ln = " ".join(ln.split())
        if len(ln) < 2 or ln in seen:
            continue
        seen.add(ln)
        lines.append(ln)
    body = "\n".join(lines)
    ld = []
    for blob in p.jsonld[:5]:
        try:
            data = json.loads(blob)
        except ValueError:
            continue
        for d in (data if isinstance(data, list) else [data]):
            if isinstance(d, dict):
                for k in ("name", "description", "applicationCategory", "operatingSystem"):
                    if isinstance(d.get(k), str):
                        ld.append("%s: %s" % (k, d[k][:400]))
                offers = d.get("offers")
                if isinstance(offers, dict) and offers.get("price") is not None:
                    ld.append("price: %s %s" % (offers.get("price"), offers.get("priceCurrency", "")))
    return {
        "title": html_mod.unescape(" ".join(p.title.split())),
        "description": p.meta.get("og:description") or p.meta.get("description") or p.meta.get("twitter:description") or "",
        "og_title": p.meta.get("og:title", ""),
        "site_name": p.meta.get("og:site_name", ""),
        "headings": p.headings,
        "jsonld": ld,
        "text": body,
    }


BLOCK_MARKERS = ("just a moment", "enable javascript", "verify you are human", "captcha", "access denied",
                 "attention required", "are you a robot", "sign in to continue", "log in to continue")


def page_material(page):
    """Return (material_for_llm, quality) where quality is ok | thin | blocked."""
    text = page["text"]
    low = (page["title"] + " " + text[:600]).lower()
    if len(text) < 1500 and any(m in low for m in BLOCK_MARKERS):
        return "", "blocked"
    bits = []
    if page["title"]:
        bits.append("TITLE: " + page["title"])
    if page["og_title"] and page["og_title"] != page["title"]:
        bits.append("OG TITLE: " + page["og_title"])
    if page["site_name"]:
        bits.append("SITE: " + page["site_name"])
    if page["description"]:
        bits.append("META DESCRIPTION: " + page["description"])
    if page["jsonld"]:
        bits.append("STRUCTURED DATA:\n" + "\n".join(page["jsonld"]))
    if page["headings"]:
        bits.append("HEADINGS:\n" + "\n".join("- " + h for h in page["headings"]))
    bits.append("PAGE TEXT:\n" + text[:MAX_TEXT_CHARS])
    material = "\n\n".join(bits)
    quality = "ok" if len(text) >= 600 else "thin"
    return material[:MAX_TEXT_CHARS + 3000], quality


# --------------------------------------------------------------------------
# GitHub
# --------------------------------------------------------------------------

GH_REPO_RE = re.compile(r"^/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:\.git)?/?$")


def github_repo_of(url):
    sp = urlsplit(url)
    if (sp.hostname or "").lower() not in ("github.com", "www.github.com"):
        return None
    m = GH_REPO_RE.match(sp.path)
    if not m or m.group(1).lower() in ("orgs", "topics", "sponsors", "settings", "marketplace", "features", "about", "pricing", "login"):
        return None
    return m.group(1), m.group(2)


def fetch_github(owner, repo, session=None):
    """Return (meta_dict, readme_text). Raises FetchError."""
    s = session or requests
    h = {"User-Agent": "knowledger-bot", "Accept": "application/vnd.github+json"}
    tok = os.environ.get("GITHUB_TOKEN")
    if tok:
        h["Authorization"] = "Bearer " + tok
    try:
        r = s.get("https://api.github.com/repos/%s/%s" % (quote(owner), quote(repo)), headers=h, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise FetchError(type(e).__name__)
    if r.status_code != 200:
        raise FetchError("GitHub API HTTP %d" % r.status_code)
    j = r.json()
    meta = {
        "full_name": j.get("full_name"), "description": j.get("description") or "", "homepage": j.get("homepage") or "",
        "topics": j.get("topics") or [], "stars": j.get("stargazers_count"), "language": j.get("language") or "",
        "license": ((j.get("license") or {}).get("spdx_id") or ""), "pushed_at": (j.get("pushed_at") or "")[:10],
        "archived": bool(j.get("archived")), "fork": bool(j.get("fork")),
    }
    readme = ""
    try:
        r2 = s.get("https://api.github.com/repos/%s/%s/readme" % (quote(owner), quote(repo)),
                   headers=dict(h, Accept="application/vnd.github.raw"), timeout=TIMEOUT)
        if r2.status_code == 200:
            readme = r2.text[:MAX_TEXT_CHARS]
    except requests.RequestException:
        pass
    return meta, readme


# --------------------------------------------------------------------------
# LLM summary
# --------------------------------------------------------------------------

TYPES = ("tool", "library", "framework", "service", "platform", "model", "dataset", "article", "tutorial",
         "course", "paper", "repository", "directory", "other")

SYSTEM = (
    "You write entries for a personal knowledge base that the owner will later search to find tools and "
    "services for specific use cases. Use ONLY the text supplied. If the text does not say something, "
    "write exactly \"%s\" (or an empty list). Never guess pricing, licence, platforms, integrations or "
    "features. The user's own note is context about why they saved the link, not a fact about the page. "
    "Reply with one JSON object." % NOT_STATED
)


def _prompt(url, material, user_note, allowed):
    return (
        "URL: %s\n" % url +
        ("User's note when saving it: %s\n" % user_note if user_note else "") +
        "\n--- BEGIN SOURCE TEXT ---\n%s\n--- END SOURCE TEXT ---\n\n" % material +
        "Return JSON with these keys:\n"
        '  "title": the name of the thing (max 80 chars, no quotes),\n'
        '  "type": one of %s,\n' % ", ".join(TYPES) +
        '  "one_liner": what it is in one plain sentence (max 160 chars),\n'
        '  "what_it_is": 1-2 sentences,\n'
        '  "what_it_does": list of 3-8 short bullets describing capabilities,\n'
        '  "use_cases": list of 3-8 items, each a concrete task or problem someone could search for, '
        'phrased like "Run an LLM locally on a laptop",\n'
        '  "who_its_for": short phrase,\n'
        '  "pricing": what the text says about price/plans, or "%s",\n' % NOT_STATED +
        '  "open_source": "yes", "no" or "unknown",\n'
        '  "platforms_integrations": list of platforms, languages or integrations the text names,\n'
        '  "alternatives": list of competing/related products the text itself names,\n'
        '  "limitations": list of caveats the text states,\n'
        '  "categories": 1-3 values chosen ONLY from: %s,\n' % ", ".join(allowed) +
        '  "tags": 3-8 lowercase keywords (technologies, task names).'
    )


def _clean_list(v, n=10, width=200):
    if not isinstance(v, list):
        return []
    out = []
    for x in v:
        if isinstance(x, (str, int, float)):
            s = " ".join(str(x).split())[:width]
            if s and s not in out:
                out.append(s)
    return out[:n]


def _slug_tags(v):
    out = []
    for t in _clean_list(v, 12, 40):
        t = re.sub(r"[^a-z0-9+#.\- ]", "", t.lower()).strip().replace(" ", "-")
        if t and t not in out:
            out.append(t)
    return out[:8]


def summarise(url, material, user_note, client, allowed):
    resp = client.chat.completions.create(
        model=TEXT_MODEL, temperature=0, response_format={"type": "json_object"}, max_tokens=1400,
        messages=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": _prompt(url, material, user_note, allowed)}])
    raw = resp.choices[0].message.content or "{}"
    j = json.loads(raw)
    typ = str(j.get("type", "other")).lower()
    cats = [c for c in _clean_list(j.get("categories"), 3, 30) if c.lower() in allowed]
    cats = [c.lower() for c in cats if c.lower() != "undefined"] or ["undefined"]
    oss = str(j.get("open_source", "unknown")).lower()
    return {
        "title": " ".join(str(j.get("title", "")).split())[:80],
        "type": typ if typ in TYPES else "other",
        "one_liner": " ".join(str(j.get("one_liner", "")).split())[:200],
        "what_it_is": " ".join(str(j.get("what_it_is", "")).split())[:500],
        "what_it_does": _clean_list(j.get("what_it_does"), 10, 220),
        "use_cases": _clean_list(j.get("use_cases"), 10, 200),
        "who_its_for": " ".join(str(j.get("who_its_for", "")).split())[:200],
        "pricing": " ".join(str(j.get("pricing", NOT_STATED)).split())[:300] or NOT_STATED,
        "open_source": oss if oss in ("yes", "no", "unknown") else "unknown",
        "platforms": _clean_list(j.get("platforms_integrations"), 12, 60),
        "alternatives": _clean_list(j.get("alternatives"), 10, 80),
        "limitations": _clean_list(j.get("limitations"), 8, 200),
        "categories": cats,
        "tags": _slug_tags(j.get("tags")),
    }


# --------------------------------------------------------------------------
# notes
# --------------------------------------------------------------------------

def _safe_name(s, n=70):
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r'[\\/:*?"<>|#^\[\]\n\r\t]+', " ", s)
    s = " ".join(s.split()).strip(" .")
    return s[:n].rstrip(" .") or "link"


def find_existing(folder, canon):
    """Return (path, is_stub) of a note for this canonical URL, or (None, False)."""
    if not folder.exists():
        return None, False
    needle = 'canonical_url: %s' % ig_enrich._q(canon)
    for p in folder.glob("*.md"):
        if p.name.startswith("._"):
            continue
        try:
            head = p.read_text(encoding="utf-8")[:3000]
        except Exception:
            continue
        if needle in head:
            return p, 'fetch_status: "failed"' in head or 'fetch_status: "blocked"' in head
    return None, False


def _bullets(items):
    return "\n".join("- " + i for i in items)


def build_note(url, canon, host, s, user_note, slack_url, quality, source, gh=None, reason=""):
    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    q = ig_enrich._q
    fm = [
        "---",
        'platform: "web"',
        "title: %s" % q(s["title"]),
        "url: %s" % q(url),
        "canonical_url: %s" % q(canon),
        "domain: %s" % q(host),
        "type: %s" % q(s["type"]),
        "tags: [%s]" % ", ".join(dict.fromkeys(["web"] + s["categories"] + s["tags"])),
        "summary: %s" % q(s["one_liner"]),
        "use_cases: %s" % ig_enrich._qlist(s["use_cases"]),
        "pricing: %s" % q(s["pricing"]),
        "open_source: %s" % q(s["open_source"]),
        "fetch_status: %s" % q(quality),
        "source: %s" % q(source),
        "captured_at: %s" % q(now),
        "summary_model: %s" % q(TEXT_MODEL if quality in ("ok", "thin") else ""),
    ]
    if slack_url:
        fm.append("slack_message: %s" % q(slack_url))
    if gh:
        fm += ["github_stars: %s" % (gh.get("stars") if gh.get("stars") is not None else 0),
               "github_language: %s" % q(gh.get("language", "")),
               "github_license: %s" % q(gh.get("license", "")),
               "github_last_push: %s" % q(gh.get("pushed_at", ""))]
    fm.append("---")
    out = ["\n".join(fm), "", "# %s" % s["title"], "",
           "**URL:** [%s](%s)" % (url, url), "**Type:** %s" % s["type"], ""]
    if quality in ("ok", "thin"):
        out += ["## Summary", "", s["one_liner"], ""]
        if s["what_it_is"]:
            out += ["## What it is", "", s["what_it_is"], ""]
        if s["what_it_does"]:
            out += ["## What it does", "", _bullets(s["what_it_does"]), ""]
        if s["use_cases"]:
            out += ["## Use cases", "", _bullets(s["use_cases"]), ""]
        if s["who_its_for"]:
            out += ["## Who it's for", "", s["who_its_for"], ""]
        out += ["## Pricing", "", s["pricing"], "", "## Open source", "", s["open_source"], ""]
        if s["platforms"]:
            out += ["## Platforms and integrations", "", _bullets(s["platforms"]), ""]
        if s["alternatives"]:
            out += ["## Alternatives mentioned", "", _bullets(s["alternatives"]), ""]
        if s["limitations"]:
            out += ["## Limitations stated", "", _bullets(s["limitations"]), ""]
        if gh:
            out += ["## GitHub", "",
                    "- Stars: %s | Language: %s | License: %s | Last push: %s%s" % (
                        gh.get("stars"), gh.get("language") or "-", gh.get("license") or "-",
                        gh.get("pushed_at") or "-", " | ARCHIVED" if gh.get("archived") else ""),
                    ("- Homepage: %s" % gh["homepage"]) if gh.get("homepage") else "", ""]
        if quality == "thin":
            out += ["> Only a little text could be read from this page (it may load its content with JavaScript), "
                    "so this summary may be incomplete.", ""]
        out += ["> AI-generated from the page text on %s. May contain errors; check the source before relying on it." % now[:10], ""]
    else:
        out += ["## Not summarised", "",
                "The page could not be read (%s). No summary was written, to avoid guessing. "
                "Sharing the link again in Slack will retry." % (reason or quality), ""]
    if user_note:
        out += ["## Your note", "", user_note, ""]
    return "\n".join(x for x in out if x is not None).rstrip() + "\n"


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def process_url(url, user_note, slack_url, client, vault, allowed, allow_private=False, session=None, gh_session=None):
    """Returns a dict {status, note, title, one_liner, categories, ...}; status is one of
    saved | stub | duplicate | skipped | error. Never raises."""
    try:
        url = url.strip().rstrip(".,;:!?)")
        if is_skipped(url):
            return {"status": "skipped", "url": url}
        canon = canonical_url(url)
        host = urlsplit(canon).hostname or ""
        folder = Path(vault) / FOLDER
        existing, is_stub = find_existing(folder, canon)
        if existing and not is_stub:
            return {"status": "duplicate", "note": str(existing), "url": url}
        allowed = [a.lower() for a in allowed]
        user_note = " ".join(re.sub(r"https?://\S+", "", user_note or "").split())[:500]

        gh, material, quality, reason, source = None, "", "failed", "", "page"
        final = url
        repo = github_repo_of(url)
        try:
            if repo:
                gh, readme = fetch_github(repo[0], repo[1], session=gh_session)
                source = "github-api"
                material = "GITHUB REPOSITORY: %s\nDESCRIPTION: %s\nHOMEPAGE: %s\nTOPICS: %s\nLANGUAGE: %s\nLICENSE: %s\n\nREADME:\n%s" % (
                    gh["full_name"], gh["description"], gh["homepage"], ", ".join(gh["topics"]),
                    gh["language"], gh["license"] or NOT_STATED, readme)
                quality = "ok" if len(readme) > 300 or gh["description"] else "thin"
            else:
                final, ctype, body = fetch_page(url, allow_private=allow_private, session=session)
                fcanon = canonical_url(final)
                if fcanon != canon:      # the link redirected: the real page may already be saved
                    other, other_stub = find_existing(folder, fcanon)
                    if other and not other_stub:
                        return {"status": "duplicate", "note": str(other), "url": url}
                    canon = fcanon
                    host = urlsplit(canon).hostname or host
                if "html" in ctype or body.lstrip()[:1] == "<":
                    page = extract_page(body)
                else:
                    page = {"title": "", "description": "", "og_title": "", "site_name": "", "headings": [], "jsonld": [], "text": body}
                material, quality = page_material(page)
                if quality == "blocked":
                    reason = "the site asked for a login, captcha or JavaScript check"
        except FetchError as e:
            if e.unsafe:
                return {"status": "skipped", "url": url, "reason": str(e)}
            reason, quality = str(e), "failed"
        if not material and quality != "blocked":
            quality = "failed"

        if quality in ("ok", "thin"):
            s = summarise(url, material, user_note, client, allowed)
            if not s["title"]:
                s["title"] = (gh or {}).get("full_name") or host
        else:
            s = {"title": host + (urlsplit(url).path.rstrip("/") or ""), "type": "other", "one_liner": "", "what_it_is": "",
                 "what_it_does": [], "use_cases": [], "who_its_for": "", "pricing": NOT_STATED, "open_source": "unknown",
                 "platforms": [], "alternatives": [], "limitations": [], "categories": ["undefined"], "tags": []}

        folder.mkdir(parents=True, exist_ok=True)
        text = build_note(url, canon, host, s, user_note, slack_url, quality, source, gh=gh, reason=reason)
        if existing:                      # re-trying a stub: overwrite it
            path = existing
        else:
            base = "%s - %s" % (_safe_name(host.replace("www.", ""), 30), _safe_name(s["title"]))
            path, n = folder / (base + ".md"), 1
            while path.exists():
                path = folder / ("%s_%d.md" % (base, n))
                n += 1
        ig_enrich._atomic_write(path, text)
        return {"status": "saved" if quality in ("ok", "thin") else "stub", "note": str(path), "url": url,
                "title": s["title"], "one_liner": s["one_liner"], "categories": s["categories"],
                "quality": quality, "reason": reason}
    except Exception as e:
        print("web_resources: failed for %s: %s: %s" % (url, type(e).__name__, str(e)[:200]))
        return {"status": "error", "url": url, "reason": "%s" % type(e).__name__}
