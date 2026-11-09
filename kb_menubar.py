#!/usr/bin/env python3
"""Menu bar app for the Knowledge Bot.

Click the menu bar icon to:
- See if the bot is running (green dot = up, red = down)
- Restart / stop the bot via launchd
- Run the channel audit and view all items in a browser table
- Retry every currently-missing item in bulk

Run with the venv's python:
  /Users/nelagueye/Downloads/coding_projects/slack/slackbot_yt_to_md/.venv/bin/python3 \
    /Users/nelagueye/Downloads/coding_projects/slack/slackbot_yt_to_md/kb_menubar.py
"""

import html
import json
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Make sibling modules importable
PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

# Load .env so audit + retry have Slack/RapidAPI/OpenAI keys
ENV_FILE = PROJECT_DIR / ".env"
if ENV_FILE.exists():
    with ENV_FILE.open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

import rumps

# Hide the Dock icon — we only need the menu bar.
# NSApplicationActivationPolicyAccessory = 1 (status item / menu extra only)
try:
    from AppKit import NSApplication
    NSApplication.sharedApplication().setActivationPolicy_(1)
except Exception:
    pass  # PyObjC missing or running outside a GUI session — degrade gracefully

LAUNCHD_LABEL = "com.nelagueye.knowledge-bot"
ALL_REPORT = Path("/tmp/knowledge_bot_items_all.html")
MISSING_REPORT = Path("/tmp/knowledge_bot_items_missing.html")
CACHE_FILE = Path("/tmp/knowledge_bot_audit_cache.json")

# Dismissed items persist across restarts — Application Support survives reboots.
APP_SUPPORT_DIR = Path.home() / "Library" / "Application Support" / "knowledge-bot"
APP_SUPPORT_DIR.mkdir(parents=True, exist_ok=True)
DISMISSED_FILE = APP_SUPPORT_DIR / "dismissed.json"

# Local-only HTTP server for the dismiss/restore buttons in the HTML report.
DISMISS_API_PORT = 8765
DISMISS_API_HOST = "127.0.0.1"


def load_dismissed():
    if not DISMISSED_FILE.exists():
        return set()
    try:
        return set(json.loads(DISMISSED_FILE.read_text()))
    except Exception:
        return set()


def save_dismissed(s):
    DISMISSED_FILE.write_text(json.dumps(sorted(s), indent=2))


def gui_uid_target():
    return f"gui/{os.getuid()}/{LAUNCHD_LABEL}"


def bot_is_running():
    """Returns (running: bool, pid: int|None)."""
    try:
        out = subprocess.check_output(["pgrep", "-f", "youtube_knowledge_bot.py"], text=True)
        pids = [int(p) for p in out.split() if p.isdigit()]
        if pids:
            return True, pids[0]
    except subprocess.CalledProcessError:
        pass
    return False, None


def kickstart_bot():
    subprocess.run(["launchctl", "kickstart", "-k", gui_uid_target()],
                   capture_output=True, text=True)


def stop_bot():
    subprocess.run(["launchctl", "bootout", gui_uid_target()],
                   capture_output=True, text=True)


def start_bot():
    plist = Path.home() / "Library/LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
    subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(plist)],
                   capture_output=True, text=True)


def write_html_report(items, vault_counts, message_count, out_path, title):
    """Render the audit result as a single-page HTML table."""
    kind_emoji = {"youtube": "🎬", "instagram": "📸", "linkedin": "💼", "other": "🔗"}
    status_color = {"vaulted": "#16a34a", "missing": "#dc2626", "dismissed": "#6b7280"}

    rows = []
    for it in items:
        slack_ts = it.get("ts", "")
        when = ""
        if slack_ts:
            try:
                when = datetime.fromtimestamp(float(slack_ts)).strftime("%Y-%m-%d %H:%M")
            except Exception:
                when = ""
        # Only missing/dismissed items get an action button. Vaulted items are done.
        action_html = ""
        dk = it.get("dismiss_key", "")
        if it["status"] == "missing":
            action_html = f"<button class='act' data-action='dismiss' data-key='{html.escape(dk)}'>Dismiss</button>"
        elif it["status"] == "dismissed":
            action_html = f"<button class='act' data-action='restore' data-key='{html.escape(dk)}'>Restore</button>"
        rows.append(
            "<tr data-status='{status}' data-kind='{kind}' data-key='{key}'>"
            "<td>{emoji} {kind}</td>"
            "<td><a href='{url}' target='_blank'>{url_display}</a></td>"
            "<td>{when}</td>"
            "<td><a href='{slack_link}' target='_blank'>↗</a></td>"
            "<td class='status' style='color:{color};font-weight:600'>{status_display}</td>"
            "<td>{action}</td>"
            "</tr>".format(
                status=html.escape(it["status"]),
                kind=html.escape(it["kind"]),
                key=html.escape(dk),
                emoji=kind_emoji.get(it["kind"], "•"),
                url=html.escape(it["url"]),
                url_display=html.escape(it["url"][:120] + ("…" if len(it["url"]) > 120 else "")),
                when=when,
                slack_link=html.escape(it["slack_link"]),
                color=status_color.get(it["status"], "#666"),
                status_display=html.escape(it["status"].upper()),
                action=action_html,
            )
        )

    missing_n = sum(1 for it in items if it["status"] == "missing")
    vaulted_n = sum(1 for it in items if it["status"] == "vaulted")
    dismissed_n = sum(1 for it in items if it["status"] == "dismissed")
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    api_base = f"http://{DISMISS_API_HOST}:{DISMISS_API_PORT}"
    html_doc = f"""<!doctype html>
<html>
<head>
<meta charset='utf-8'>
<title>{html.escape(title)}</title>
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; margin: 24px; color: #1f2937; }}
  h1 {{ font-size: 20px; margin: 0 0 8px 0; }}
  .meta {{ color: #6b7280; font-size: 13px; margin-bottom: 16px; }}
  .pills {{ margin: 16px 0; }}
  .pill {{ display: inline-block; padding: 4px 10px; border-radius: 999px; font-size: 12px; margin-right: 6px; background: #f3f4f6; cursor: pointer; user-select: none; }}
  .pill.active {{ background: #1f2937; color: #fff; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  th, td {{ text-align: left; padding: 8px 12px; border-bottom: 1px solid #e5e7eb; }}
  th {{ background: #f9fafb; font-weight: 600; }}
  tr.hidden {{ display: none; }}
  tr.row-dismissed td {{ opacity: 0.55; }}
  a {{ color: #2563eb; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  button.act {{ font-size: 12px; padding: 3px 10px; border: 1px solid #d1d5db; background: #fff; border-radius: 6px; cursor: pointer; }}
  button.act:hover {{ background: #f9fafb; }}
  button.act:disabled {{ opacity: 0.5; cursor: wait; }}
  .toast {{ position: fixed; bottom: 24px; right: 24px; background: #1f2937; color: #fff; padding: 10px 16px; border-radius: 8px; font-size: 13px; opacity: 0; transition: opacity .15s; pointer-events: none; }}
  .toast.show {{ opacity: 1; }}
</style>
</head>
<body>
<h1>{html.escape(title)}</h1>
<div class='meta'>
  Generated {generated} · {message_count} Slack messages scanned ·
  Vault has {vault_counts['youtube']} YouTube · {vault_counts['instagram']} Instagram ·
  {vault_counts['linkedin']} LinkedIn · {vault_counts['other']} other
</div>
<div class='pills'>
  <span class='pill' data-filter='all'>All ({len(items)})</span>
  <span class='pill' data-filter='missing'>Missing ({missing_n})</span>
  <span class='pill' data-filter='dismissed'>Dismissed ({dismissed_n})</span>
  <span class='pill' data-filter='vaulted'>Vaulted ({vaulted_n})</span>
  <span class='pill' data-filter='youtube'>🎬 YouTube</span>
  <span class='pill' data-filter='instagram'>📸 Instagram</span>
  <span class='pill' data-filter='linkedin'>💼 LinkedIn</span>
  <span class='pill' data-filter='other'>🔗 Other</span>
</div>
<table>
<thead><tr><th>Platform</th><th>URL</th><th>Posted</th><th>Slack</th><th>Status</th><th></th></tr></thead>
<tbody>
{''.join(rows)}
</tbody>
</table>
<div id='toast' class='toast'></div>
<script>
const API_BASE = "{api_base}";
const pills = document.querySelectorAll('.pill');
const rows = document.querySelectorAll('tbody tr');
const toast = document.getElementById('toast');

function applyFilter(f) {{
  pills.forEach(p => p.classList.toggle('active', p.dataset.filter === f));
  rows.forEach(r => {{
    const show = f === 'all'
      || r.dataset.status === f
      || r.dataset.kind === f;
    r.classList.toggle('hidden', !show);
  }});
}}
pills.forEach(p => p.addEventListener('click', () => applyFilter(p.dataset.filter)));

function showToast(msg) {{
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 1800);
}}

// Update a row in place after a dismiss/restore so the user doesn't have to
// regenerate the report.
function updateRow(tr, newStatus) {{
  tr.dataset.status = newStatus;
  const statusCell = tr.querySelector('.status');
  const actionCell = tr.lastElementChild;
  const colors = {{ missing: '#dc2626', dismissed: '#6b7280', vaulted: '#16a34a' }};
  statusCell.style.color = colors[newStatus] || '#666';
  statusCell.textContent = newStatus.toUpperCase();
  tr.classList.toggle('row-dismissed', newStatus === 'dismissed');
  const key = tr.dataset.key;
  if (newStatus === 'missing') {{
    actionCell.innerHTML = `<button class='act' data-action='dismiss' data-key='${{key}}'>Dismiss</button>`;
  }} else if (newStatus === 'dismissed') {{
    actionCell.innerHTML = `<button class='act' data-action='restore' data-key='${{key}}'>Restore</button>`;
  }} else {{
    actionCell.innerHTML = '';
  }}
}}

document.body.addEventListener('click', async ev => {{
  const btn = ev.target.closest('button.act');
  if (!btn) return;
  const action = btn.dataset.action;
  const key = btn.dataset.key;
  btn.disabled = true;
  btn.textContent = action === 'dismiss' ? 'Dismissing…' : 'Restoring…';
  try {{
    const resp = await fetch(`${{API_BASE}}/${{action}}`, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ key }})
    }});
    if (!resp.ok) throw new Error(`HTTP ${{resp.status}}`);
    const data = await resp.json();
    updateRow(btn.closest('tr'), data.new_status);
    showToast(action === 'dismiss' ? 'Dismissed' : 'Restored');
  }} catch (e) {{
    btn.disabled = false;
    btn.textContent = action === 'dismiss' ? 'Dismiss' : 'Restore';
    showToast('Failed: ' + e.message);
  }}
}});

// Initial row tint for dismissed
document.querySelectorAll("tr[data-status='dismissed']").forEach(r => r.classList.add('row-dismissed'));

applyFilter('{('missing' if missing_n else 'all')}');
</script>
</body>
</html>
"""
    out_path.write_text(html_doc, encoding="utf-8")


class DismissHandler(BaseHTTPRequestHandler):
    # Set by the App via class attribute before serving
    app = None

    def _respond(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.end_headers()

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            data = json.loads(raw.decode("utf-8") or "{}")
            key = data.get("key", "").strip()
            if not key:
                return self._respond(400, {"error": "missing key"})
            if self.path == "/dismiss":
                self.app.add_dismissed(key)
                return self._respond(200, {"key": key, "new_status": "dismissed"})
            if self.path == "/restore":
                self.app.remove_dismissed(key)
                return self._respond(200, {"key": key, "new_status": "missing"})
            return self._respond(404, {"error": "unknown endpoint"})
        except Exception as e:
            return self._respond(500, {"error": str(e)})

    def log_message(self, format, *args):
        # Quiet the default per-request stderr log
        return


class KBApp(rumps.App):
    def __init__(self):
        super().__init__("KB", title="KB ●", quit_button=None)
        # "Dismiss missing ▸" is a parent item with a dynamic submenu rebuilt
        # whenever the audit result or dismissed set changes.
        self._dismiss_submenu = rumps.MenuItem("Dismiss missing ▸")
        self.menu = [
            "Status: checking…",
            "Last audit: never",
            None,
            "Show items in browser",
            "Run audit",
            "Retry all missing",
            self._dismiss_submenu,
            None,
            "Restart bot",
            "Stop bot",
            "Start bot",
            None,
            "Open log",
            "Quit",
        ]
        self._audit_result = None
        self._audit_in_progress = False
        self._retry_in_progress = False
        self._dismissed = load_dismissed()
        self._dismissed_lock = threading.Lock()
        # Load cached audit so the menu shows useful info on startup
        if CACHE_FILE.exists():
            try:
                self._audit_result = json.loads(CACHE_FILE.read_text())
                # Re-tag items based on current dismissed set (in case it
                # changed since the cache was written)
                for it in self._audit_result.get("items", []):
                    dk = it.get("dismiss_key")
                    if it["status"] in ("missing", "dismissed"):
                        it["status"] = "dismissed" if dk in self._dismissed else "missing"
            except Exception:
                pass
        # Start local HTTP server for dismiss/restore from the HTML report
        self._start_dismiss_server()
        self._refresh_status_menu()
        self._rebuild_dismiss_submenu()

    def _start_dismiss_server(self):
        DismissHandler.app = self
        try:
            self._http = HTTPServer((DISMISS_API_HOST, DISMISS_API_PORT), DismissHandler)
        except OSError as e:
            print(f"WARN: could not bind dismiss API on {DISMISS_API_HOST}:{DISMISS_API_PORT}: {e}")
            self._http = None
            return
        t = threading.Thread(target=self._http.serve_forever, daemon=True)
        t.start()

    # ----- dismissed-list management (called from HTTP handler thread) -----

    def add_dismissed(self, key):
        with self._dismissed_lock:
            self._dismissed.add(key)
            save_dismissed(self._dismissed)
            if self._audit_result:
                for it in self._audit_result["items"]:
                    if it.get("dismiss_key") == key and it["status"] == "missing":
                        it["status"] = "dismissed"
                self._persist_cache()
        self._refresh_status_menu()
        self._rebuild_dismiss_submenu()

    def remove_dismissed(self, key):
        with self._dismissed_lock:
            self._dismissed.discard(key)
            save_dismissed(self._dismissed)
            if self._audit_result:
                for it in self._audit_result["items"]:
                    if it.get("dismiss_key") == key and it["status"] == "dismissed":
                        it["status"] = "missing"
                self._persist_cache()
        self._refresh_status_menu()
        self._rebuild_dismiss_submenu()

    def _persist_cache(self):
        try:
            CACHE_FILE.write_text(json.dumps(self._audit_result))
        except Exception:
            pass

    def _rebuild_dismiss_submenu(self):
        """Refresh the 'Dismiss missing ▸' submenu with the current missing items.

        Each entry, when clicked, dismisses that item. Capped at 25 entries —
        beyond that, fall back to the browser UI.
        """
        sub = self._dismiss_submenu
        # Remove every existing child
        for key in list(sub.keys()):
            del sub[key]

        if not self._audit_result:
            sub.add(rumps.MenuItem("(run audit first)"))
            return

        missing = [it for it in self._audit_result["items"] if it["status"] == "missing"]
        if not missing:
            sub.add(rumps.MenuItem("(nothing missing)"))
            return

        cap = 25
        for it in missing[:cap]:
            kind_tag = it["kind"][:2].upper()
            url = it["url"]
            # Strip schema + query for readability; keep ~60 chars
            display = url.replace("https://", "").replace("http://", "")
            display = display.split("?")[0]
            if len(display) > 60:
                display = display[:57] + "…"
            label = f"[{kind_tag}] {display}"
            mi = rumps.MenuItem(label)
            dk = it["dismiss_key"]

            def make_cb(key):
                return lambda _: self._on_submenu_dismiss(key)
            mi.set_callback(make_cb(dk))
            sub.add(mi)

        if len(missing) > cap:
            note = rumps.MenuItem(f"…and {len(missing) - cap} more (use browser)")
            sub.add(note)

    def _on_submenu_dismiss(self, key):
        self.add_dismissed(key)
        rumps.notification("Knowledge Bot", "Dismissed", key)

    # ----- timers -----

    @rumps.timer(5)
    def poll_status(self, _):
        self._refresh_status_menu()

    @rumps.timer(600)  # every 10 minutes
    def periodic_audit(self, _):
        # Fires once shortly after startup and then every 10 minutes.
        # Skip if a manual audit is already running.
        if self._audit_in_progress:
            return
        self._audit_in_progress = True
        threading.Thread(target=self._run_audit_thread, daemon=True).start()

    def _refresh_status_menu(self):
        running, pid = bot_is_running()
        self.title = "KB ●" if running else "KB ○"
        self.menu["Status: checking…"].title = (
            f"Status: running (PID {pid})" if running else "Status: STOPPED"
        )
        if self._audit_result:
            missing = sum(1 for it in self._audit_result["items"] if it["status"] == "missing")
            dismissed = sum(1 for it in self._audit_result["items"] if it["status"] == "dismissed")
            total = len(self._audit_result["items"])
            stamp = self._audit_result.get("_generated_at", "?")
            suffix = f" ({dismissed} dismissed)" if dismissed else ""
            self.menu["Last audit: never"].title = f"Last audit: {missing}/{total} missing{suffix} · {stamp}"

    # ----- menu actions -----

    @rumps.clicked("Restart bot")
    def on_restart(self, _):
        kickstart_bot()
        rumps.notification("Knowledge Bot", "Restarting", "launchctl kickstart sent")
        self._refresh_status_menu()

    @rumps.clicked("Stop bot")
    def on_stop(self, _):
        if not rumps.alert("Stop the Knowledge Bot?",
                            "It will not auto-restart until you choose Start bot.",
                            ok="Stop", cancel="Cancel"):
            return
        stop_bot()
        rumps.notification("Knowledge Bot", "Stopped", "launchd bootout sent")
        self._refresh_status_menu()

    @rumps.clicked("Start bot")
    def on_start(self, _):
        start_bot()
        rumps.notification("Knowledge Bot", "Starting", "launchd bootstrap sent")
        self._refresh_status_menu()

    @rumps.clicked("Open log")
    def on_open_log(self, _):
        subprocess.run(["open", "-a", "Console", "/tmp/knowledge_bot.log"])

    @rumps.clicked("Run audit")
    def on_run_audit(self, _):
        if self._audit_in_progress:
            rumps.notification("Knowledge Bot", "Audit already running", "Please wait")
            return
        self._audit_in_progress = True
        self.menu["Last audit: never"].title = "Last audit: running…"
        threading.Thread(target=self._run_audit_thread, daemon=True).start()

    def _run_audit_thread(self):
        prev_missing = sum(1 for it in (self._audit_result or {}).get("items", []) if it["status"] == "missing")
        try:
            from audit_channel import audit
            with self._dismissed_lock:
                dismissed_snapshot = set(self._dismissed)
            result = audit(dismissed=dismissed_snapshot)
            result["_generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            self._audit_result = result
            self._persist_cache()
            missing = sum(1 for it in result["items"] if it["status"] == "missing")
            # Only notify when there's news: missing changed OR missing > 0.
            # Silent otherwise so the 10-min auto-audit doesn't spam.
            if missing != prev_missing or missing > 0:
                rumps.notification("Knowledge Bot", "Audit complete", f"{missing} missing items")
        except Exception as e:
            rumps.notification("Knowledge Bot", "Audit failed", str(e)[:100])
        finally:
            self._audit_in_progress = False
            self._refresh_status_menu()
            self._rebuild_dismiss_submenu()

    @rumps.clicked("Show items in browser")
    def on_show_items(self, _):
        if not self._audit_result:
            if not rumps.alert("No audit yet",
                                "Run the audit now? Takes about 30 seconds.",
                                ok="Run audit", cancel="Cancel"):
                return
            self.on_run_audit(None)
            return
        write_html_report(
            self._audit_result["items"],
            self._audit_result["vault_counts"],
            self._audit_result["message_count"],
            ALL_REPORT,
            "Knowledge Bot — Channel items",
        )
        subprocess.run(["open", str(ALL_REPORT)])

    @rumps.clicked("Retry all missing")
    def on_retry_missing(self, _):
        if self._retry_in_progress:
            rumps.notification("Knowledge Bot", "Retry already running", "Please wait")
            return
        if not self._audit_result:
            rumps.alert("Run the audit first.")
            return
        missing = [it for it in self._audit_result["items"] if it["status"] == "missing"]
        if not missing:
            rumps.alert("Nothing to retry — no missing items.")
            return
        if not rumps.alert(f"Retry {len(missing)} missing items?",
                            "Each retry will post into the #knowledger channel.",
                            ok="Retry", cancel="Cancel"):
            return
        self._retry_in_progress = True
        threading.Thread(target=self._retry_thread, args=(missing,), daemon=True).start()

    def _retry_thread(self, missing):
        from youtube_knowledge_bot import (
            process_youtube_video, process_instagram_content,
            process_linkedin_post, process_resource_links,
            extract_video_id,
        )
        ok = fail = 0
        for it in missing:
            try:
                kind = it["kind"]
                if kind == "youtube":
                    vid = extract_video_id(it["url"])
                    if vid:
                        process_youtube_video(vid, "C0A99TH4Y2V", it["slack_link"])
                    else:
                        raise RuntimeError("could not extract video id")
                elif kind == "instagram":
                    process_instagram_content(it["url"], "C0A99TH4Y2V", it["slack_link"])
                elif kind == "linkedin":
                    process_linkedin_post(it["url"], it["url"], "C0A99TH4Y2V", it["slack_link"])
                elif kind == "other":
                    process_resource_links([it["url"]], it["url"], "C0A99TH4Y2V", it["slack_link"])
                ok += 1
            except Exception:
                fail += 1
            time.sleep(1.5)
        rumps.notification("Knowledge Bot", "Retry complete", f"{ok} succeeded, {fail} failed")
        self._retry_in_progress = False
        # Re-run audit so the next view reflects new state
        self._run_audit_thread()

    @rumps.clicked("Quit")
    def on_quit(self, _):
        rumps.quit_application()


if __name__ == "__main__":
    KBApp().run()
