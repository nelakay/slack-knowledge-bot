# Save to Knowledger (LinkedIn one-click capture)

Click the toolbar button while viewing a LinkedIn post and it is saved to your vault:
`linkedin/<author> - <title>.md`, images in `assets/`, with image text, links, tools and a
summary added by `ig_enrich.py`.

## Install (once)
1. `python3 linkedin_capture.py --init` in the bot folder (creates the token and `config.js`).
2. Chrome -> `chrome://extensions` -> turn on Developer mode -> **Load unpacked** -> pick this folder.
3. Pin the extension. Restart the bot (it starts the receiver), or run `python3 linkedin_capture.py`.

## Use
Open the post on its own page (click its timestamp), then click the button.
Badge: `OK` saved, `DUP` already saved, `ERR` hover the icon for the reason, `OK!` saved with a warning.

## Limits
- Works on single-post pages (`/posts/`, `/feed/update/`, `/pulse/`), not on the feed.
- LinkedIn changes its markup; text selectors are best-effort. If a note comes out empty or
  truncated, select the text on the page before clicking (the extension uses a selection first
  if its normal selectors fail) and tell me which page broke.
- PDF/document carousels are not captured as images.
