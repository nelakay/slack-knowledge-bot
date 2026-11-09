#!/usr/bin/env python3
"""Find out why a YouTube video came out of the bot with no transcript.

    python3 diagnose_youtube.py tTcxVv8HHNw            # a video id or a full URL
    python3 diagnose_youtube.py tTcxVv8HHNw --video    # also test the video download (slow, big)

Runs the same yt-dlp settings the bot uses, one step at a time, and prints which step fails and why.
Nothing is written to the vault and OpenAI is never called.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def line(ok, label, detail=""):
    print("%s %-34s %s" % ({True: "OK  ", False: "FAIL", None: "info"}[ok], label, detail))


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit(__doc__)
    m = re.search(r"(?:v=|youtu\.be/|embed/|shorts/)([\w-]{11})", args[0])
    vid = m.group(1) if m else args[0]
    url = "https://www.youtube.com/watch?v=" + vid
    print("Video:", url, "\n")

    # 1. tools the bot depends on
    try:
        import yt_dlp
        line(True, "yt-dlp", "%s   (python: %s)" % (yt_dlp.version.__version__, sys.executable))
    except Exception as e:
        line(False, "yt-dlp import", repr(e))
        return
    for tool in ("ffmpeg", "ffprobe", "node", "deno"):
        line(None if tool in ("node", "deno") else bool(shutil.which(tool)), tool, shutil.which(tool) or "not found")
    try:
        import pydub  # noqa: F401
        line(True, "pydub (splits files > 20 MB)", "installed")
    except Exception as e:
        line(False, "pydub (splits files > 20 MB)", "NOT installed: long videos cannot be transcribed in chunks")
    cookies = HERE / "www.instagram.com_cookies.txt"
    line(None, "cookie file", "%s (%s)" % (cookies.name, "exists" if cookies.exists() else "missing"))

    base = {"cookiefile": str(cookies), "quiet": True, "no_warnings": True, "remote_components": ["ejs:github"]}

    # 2. metadata exactly as the bot asks for it
    info = None
    try:
        with yt_dlp.YoutubeDL({**base, "skip_download": True, "ignore_no_formats_error": True}) as y:
            info = y.extract_info(url, download=False)
        fmts = info.get("formats") or []
        audio = [f for f in fmts if f.get("vcodec") == "none" and f.get("acodec") not in (None, "none")]
        line(True, "metadata", "%s | %ss | live=%s | availability=%s" % (
            (info.get("title") or "")[:50], info.get("duration"), info.get("live_status"), info.get("availability")))
        line(bool(audio), "audio formats offered", "%d audio-only, %d total" % (len(audio), len(fmts)))
        subs = sorted(set(list((info.get("subtitles") or {}).keys()) + list((info.get("automatic_captions") or {}).keys())))
        line(None, "captions on YouTube", ", ".join(subs[:8]) + (" ..." if len(subs) > 8 else "") if subs else "none")
    except Exception as e:
        line(False, "metadata", str(e)[:400])

    # 3. audio download exactly as the bot does it; if it fails, retry without the cookie file
    def try_audio(opts, label):
        tmp = tempfile.mkdtemp()
        try:
            t0 = time.time()
            with yt_dlp.YoutubeDL({**opts, "format": "bestaudio/best", "outtmpl": os.path.join(tmp, "%(title)s.%(ext)s")}) as y:
                y.extract_info(url, download=True)
            files = list(Path(tmp).iterdir())
            size = sum(f.stat().st_size for f in files)
            ok = bool(files) and size > 1024
            line(ok, label, "%d file(s), %.1f MB, %.0fs" % (len(files), size / 1e6, time.time() - t0))
            for f in files:
                r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration,format_name", "-of", "csv=p=0", str(f)],
                                   capture_output=True, text=True)
                line(r.returncode == 0, "ffprobe on the audio", (r.stdout.strip() or r.stderr.strip())[:200])
                if f.stat().st_size > 20 * 1024 * 1024:
                    try:
                        from pydub import AudioSegment
                        seg = AudioSegment.from_file(str(f))
                        line(True, "pydub can open it", "%.1f min" % (len(seg) / 60000))
                    except Exception as e:
                        line(False, "pydub can open it", repr(e)[:300])
            return ok
        except Exception as e:
            line(False, label, str(e)[:300])
            return False
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    if not try_audio(base, "audio download (bot settings)"):
        nocookie = {k: v for k, v in base.items() if k != "cookiefile"}
        if try_audio(nocookie, "audio download (no cookie file)"):
            print("\n>>> It works WITHOUT the cookie file: the cookies are what breaks it.")
        else:
            print("\n>>> Fails either way: yt-dlp %s is too old for YouTube's current player." % yt_dlp.version.__version__)
            print(">>> Update it with the SAME python shown above:  %s -m pip install -U yt-dlp" % sys.executable)

    # 4. optional: the video file the bot also saves
    if "--video" in sys.argv:
        tmp = tempfile.mkdtemp()
        try:
            with yt_dlp.YoutubeDL({**base, "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
                                   "merge_output_format": "mp4", "outtmpl": os.path.join(tmp, "v.%(ext)s")}) as y:
                y.download([url])
            files = list(Path(tmp).iterdir())
            line(bool(files), "video download", ", ".join("%s %.0f MB" % (f.suffix, f.stat().st_size / 1e6) for f in files))
        except Exception as e:
            line(False, "video download", str(e)[:600])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    # 5. what the bot did about it
    print("\nBot logic: it ALWAYS downloads audio and sends it to Whisper; it does not use YouTube captions.")
    print("If 'audio download' fails here, that is why the note is empty.")


if __name__ == "__main__":
    main()
