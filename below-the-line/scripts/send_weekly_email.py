#!/usr/bin/env python3
"""
Below The Line - Weekly Email Sender

Reads crossings.json (produced by update_stocks.py), builds the HTML email,
and sends to all subscribers via ZeptoMail.

Designed to run as a SEPARATE GitHub Actions workflow so that:
- Failures are visible (red X in Actions UI)
- Can be re-run independently without re-running the data pipeline
- Always reads from the committed crossings.json (same data, deterministic)

Environment variables required:
  ZOHO_EMAIL          - sender address
  ZEPTOMAIL_API_TOKEN - ZeptoMail API auth token
  NETLIFY_API_TOKEN   - for fetching subscriber list from Netlify Forms
  NETLIFY_SITE_ID     - Netlify site for form lookups
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent / 'assets' / 'data'
CROSSINGS_FILE = DATA_DIR / 'crossings.json'


def load_crossings() -> dict:
    """Load and validate crossings.json."""
    if not CROSSINGS_FILE.exists():
        print(f"ERROR: crossings.json not found at {CROSSINGS_FILE}")
        print("  This script must run AFTER update_stocks.py generates crossings.json.")
        sys.exit(1)

    with open(CROSSINGS_FILE) as f:
        data = json.load(f)

    # Validate required fields
    if 'date' not in data:
        print("ERROR: crossings.json is missing 'date' field.")
        sys.exit(1)

    newly_below = data.get('newly_below', [])
    newly_recovered = data.get('newly_recovered', [])

    if not newly_below and not newly_recovered:
        print(f"No crossings in crossings.json (date: {data['date']}) — nothing to send.")
        print("This is expected when no stocks crossed the 200WMA this week.")
        sys.exit(0)

    # Staleness check: warn if data is more than 3 days old
    try:
        data_date = datetime.strptime(data['date'], '%Y-%m-%d')
        age = datetime.now() - data_date
        if age > timedelta(days=3):
            print(f"WARNING: crossings.json is {age.days} days old (date: {data['date']}).")
            print("  If this is a retry of a failed send, this is expected.")
            print("  If this is a fresh run, check that update_stocks.py ran first.")
    except ValueError:
        pass

    print(f"Loaded crossings.json: {len(newly_below)} below, {len(newly_recovered)} recovered (date: {data['date']})")
    return data


def get_subscribers() -> list[str]:
    """Fetch subscriber emails from Netlify Forms API, minus unsubscribes."""
    netlify_token = os.environ.get('NETLIFY_API_TOKEN')
    site_id = os.environ.get('NETLIFY_SITE_ID')

    if not netlify_token or not site_id:
        print("ERROR: Missing NETLIFY_API_TOKEN or NETLIFY_SITE_ID.")
        sys.exit(1)

    # Get forms to find the "notify" form ID
    try:
        req = urllib.request.Request(
            f"https://api.netlify.com/api/v1/sites/{site_id}/forms",
            headers={"Authorization": f"Bearer {netlify_token}"}
        )
        with urllib.request.urlopen(req) as resp:
            forms = json.loads(resp.read())
    except Exception as e:
        print(f"ERROR: Failed to fetch forms: {e}")
        sys.exit(1)

    # One list, two intake points. "notify" is the original homepage signup;
    # "deepdive-notify" is the form on the deep-dive pages. Both feed the same
    # send, so a reader who signed up at either place gets the same email and
    # is only ever counted once.
    SUBSCRIBE_FORMS = ('notify', 'deepdive-notify')

    form_ids = {f['name']: f['id'] for f in forms if f.get('name') in SUBSCRIBE_FORMS}
    if not form_ids:
        print(f"ERROR: none of {SUBSCRIBE_FORMS} found on Netlify site.")
        sys.exit(1)
    for name in SUBSCRIBE_FORMS:
        if name not in form_ids:
            print(f"  note: form '{name}' not present yet; skipping.")

    # Fetch all submissions across every intake form (paginated)
    emails = set()
    for name, form_id in form_ids.items():
        before = len(emails)
        page = 1
        while True:
            try:
                req = urllib.request.Request(
                    f"https://api.netlify.com/api/v1/forms/{form_id}/submissions?per_page=100&page={page}",
                    headers={"Authorization": f"Bearer {netlify_token}"}
                )
                with urllib.request.urlopen(req) as resp:
                    subs = json.loads(resp.read())
            except Exception as e:
                print(f"WARNING: Failed to fetch '{name}' submissions page {page}: {e}")
                break

            if not subs:
                break

            for sub in subs:
                email = sub.get('data', {}).get('email', '').strip().lower()
                if email:
                    emails.add(email)
            page += 1
        print(f"  {name}: +{len(emails) - before} new addresses ({len(emails)} unique so far)")

    # Fetch unsubscribes from Netlify Blobs API
    unsubscribed = set()
    try:
        req = urllib.request.Request(
            f"https://api.netlify.com/api/v1/blobs/{site_id}/unsubscribes",
            headers={"Authorization": f"Bearer {netlify_token}"}
        )
        with urllib.request.urlopen(req) as resp:
            blob_data = json.loads(resp.read())
            for blob in blob_data.get('blobs', []):
                unsubscribed.add(blob['key'].lower().strip())
    except Exception as e:
        # 404 means no unsubscribes yet — that's fine
        if '404' not in str(e):
            print(f"WARNING: Failed to fetch unsubscribes: {e}")

    active = sorted(emails - unsubscribed)
    print(f"Subscribers: {len(emails)} total, {len(unsubscribed)} unsubscribed, {len(active)} active")

    if not active:
        print("No active subscribers — nothing to send.")
        sys.exit(0)

    return active



CONTENT_DIR = Path(__file__).parent.parent / 'content' / 'deep-dives'


def _frontmatter(path: Path) -> dict:
    """Minimal YAML-ish frontmatter reader: flat `key: "value"` pairs only."""
    out = {}
    try:
        text = path.read_text(encoding='utf-8')
    except Exception:
        return out
    if not text.startswith('---'):
        return out
    body = text.split('---', 2)
    if len(body) < 3:
        return out
    for line in body[1].splitlines():
        if ':' not in line or line.lstrip().startswith('#'):
            continue
        k, _, v = line.partition(':')
        v = v.strip().strip('"').strip("'")
        out[k.strip()] = v
    return out


def latest_deep_dive() -> dict | None:
    """Newest published deep dive, as {title, description, url, date}.

    Stubs that front a standalone HTML article carry `canonical_url`; anything
    else resolves to its own slug.
    """
    if not CONTENT_DIR.exists():
        return None
    best = None
    for md in CONTENT_DIR.glob('*.md'):
        if md.name.startswith('_'):
            continue
        fm = _frontmatter(md)
        if str(fm.get('draft', 'false')).lower() == 'true':
            continue
        date = fm.get('date', '')
        if not date:
            continue
        if best is None or date > best[0]:
            url = fm.get('canonical_url') or f"/deep-dives/{md.stem}/"
            best = (date, {
                'title': fm.get('title', ''),
                'description': fm.get('description', ''),
                'ticker': fm.get('ticker', ''),
                'url': 'https://mungbeans.io' + url,
                'date': date,
            })
    return best[1] if best else None


def build_email_html(crossings: dict, unsub_url: str) -> tuple[str, str]:
    """Build the email subject and HTML body from crossings data.

    Returns (subject, html_body).
    """
    date_display = crossings.get('date_display', crossings['date'])
    slug = crossings.get('blog_slug', f"{crossings['date']}-weekly-signal-report")
    post_url = f"https://mungbeans.io/blog/{slug}/"
    blog_generated = crossings.get('blog_generated', True)  # default True for old crossings.json files

    # Latest deep dive teaser. Silently omitted when nothing is published.
    dive = latest_deep_dive()
    if dive:
        _blurb = dive['description']
        if len(_blurb) > 220:
            _blurb = _blurb[:217].rsplit(' ', 1)[0] + '...'
        _tag = f'<span style="font-family:monospace; font-size:11px; color:#1a1a2e; background-color:#e2b714; padding:2px 7px; border-radius:3px; font-weight:700;">{dive["ticker"]}</span> ' if dive.get('ticker') else ''
        deep_dive_section = f"""
          <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#16162a; border:1px solid #2a2a3e; border-left:2px solid #e2b714; border-radius:8px; margin:0 0 26px 0;">
            <tr><td style="padding:18px 20px;">
              <p style="margin:0 0 8px 0; font-family:monospace; font-size:10px; letter-spacing:2px; text-transform:uppercase; color:#888;">latest deep dive</p>
              <p style="margin:0 0 8px 0; font-size:17px; line-height:1.4; color:#ffffff; font-weight:600;">{_tag}<a href="{dive['url']}" style="color:#ffffff; text-decoration:none;">{dive['title']}</a></p>
              <p style="margin:0 0 12px 0; color:#c9c9d4; font-size:14px; line-height:1.6;">{_blurb}</p>
              <p style="margin:0;"><a href="{dive['url']}" style="color:#e2b714; text-decoration:none; font-size:14px; font-weight:600;">Read the deep dive &rarr;</a></p>
            </td></tr>
          </table>"""
    else:
        deep_dive_section = ''

    newly_below = crossings.get('newly_below', [])
    newly_recovered = crossings.get('newly_recovered', [])

    below_rows = ""
    for s in newly_below[:15]:
        symbol = s['symbol']
        name = s.get('name', symbol)
        pct = abs(s.get('pct_from_wma', 0))
        rsi = s.get('rsi_14') or 0
        below_rows += f"""
        <tr>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#e2b714; font-weight:600;">{symbol}</td>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#ccc;">{name}</td>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#ff6b6b;">{pct:.1f}%</td>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#aaa;">{rsi:.0f}</td>
        </tr>"""

    recovered_rows = ""
    for s in newly_recovered[:10]:
        symbol = s['symbol']
        name = s.get('name', symbol)
        pct = s.get('pct_from_wma', 0)
        recovered_rows += f"""
        <tr>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#e2b714; font-weight:600;">{symbol}</td>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#ccc;">{name}</td>
          <td style="padding:8px 12px; border-bottom:1px solid #2a2a3e; color:#4ecdc4;">+{pct:.1f}%</td>
        </tr>"""

    below_section = ""
    if newly_below:
        below_section = f"""
        <p style="margin:24px 0 12px 0; font-size:18px; color:#ffffff; font-weight:600;">{len(newly_below)} Crossed Below</p>
        <table width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;">
          <tr style="color:#888; font-size:12px; text-transform:uppercase;">
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">Ticker</td>
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">Name</td>
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">Below</td>
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">RSI</td>
          </tr>
          {below_rows}
        </table>"""

    recovered_section = ""
    if newly_recovered:
        recovered_section = f"""
        <p style="margin:24px 0 12px 0; font-size:18px; color:#ffffff; font-weight:600;">{len(newly_recovered)} Recovered Above</p>
        <table width="100%" cellpadding="0" cellspacing="0" style="font-size:14px;">
          <tr style="color:#888; font-size:12px; text-transform:uppercase;">
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">Ticker</td>
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">Name</td>
            <td style="padding:8px 12px; border-bottom:2px solid #2a2a3e;">Above</td>
          </tr>
          {recovered_rows}
        </table>"""

    subject = f"Weekly Signal Report — {date_display}"

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0; padding:0; background-color:#0f0f1a; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#0f0f1a; padding:40px 20px;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;">
        <tr><td style="padding:0 0 24px 0;">
          <span style="font-family:monospace; font-size:28px; font-weight:bold; color:#e2b714;">m</span>
          <span style="font-family:sans-serif; font-size:18px; color:#888; margin-left:8px;">mungbeans.io</span>
        </td></tr>
        <tr><td style="color:#e0e0e0; font-size:16px; line-height:1.6;">
          <p style="margin:0 0 8px 0; font-size:22px; color:#ffffff; font-weight:600;">Weekly Signal Report</p>
          <p style="margin:0 0 20px 0; color:#888; font-size:14px;">{date_display} — 200-week moving average crossings</p>
          {deep_dive_section}
          {below_section}
          {recovered_section}
          <p style="margin:28px 0 0 0;">
            {'<a href="' + post_url + '" style="display:inline-block; background-color:#e2b714; color:#1a1a2e; text-decoration:none; padding:12px 28px; border-radius:6px; font-weight:600; font-size:15px;">View Full Report</a>' if blog_generated else '<a href="https://mungbeans.io/stocks/" style="display:inline-block; background-color:#e2b714; color:#1a1a2e; text-decoration:none; padding:12px 28px; border-radius:6px; font-weight:600; font-size:15px;">Browse All Stocks</a>'}
          </p>
        </td></tr>
        <tr><td style="padding:32px 0 0 0;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#16162a; border:1px solid #2a2a3e; border-left:2px solid #e2b714; border-radius:8px;">
            <tr><td style="padding:20px 22px;">
              <p style="margin:0 0 4px 0; font-family:monospace; font-size:10px; letter-spacing:2px; text-transform:uppercase; color:#666;">the terminal I use &middot; referral link</p>
              <p style="margin:0 0 8px 0; font-size:19px; color:#ffffff; font-weight:600;">Godel Terminal</p>
              <p style="margin:0 0 14px 0; color:#c9c9d4; font-size:14px; line-height:1.6;">
                A browser-based research terminal with a Bloomberg-style command interface, at $996 a year against Bloomberg's roughly $27,000. Real-time quotes, filings, financials and ownership data. Research and analysis only, so it sits alongside a broker rather than replacing one. Fourteen-day trial.
              </p>
              <p style="margin:0 0 12px 0;">
                <a href="https://app.godelterminal.com/?via=mungbeans" style="display:inline-block; background-color:#e2b714; color:#1a1a2e; text-decoration:none; padding:10px 22px; border-radius:5px; font-weight:600; font-size:14px;">Try Godel Terminal &rarr;</a>
              </p>
              <p style="margin:0 0 12px 0; font-family:monospace; font-size:13px; color:#c9c9d4;">code <b style="color:#e2b714;">mungbeans</b> &middot; 30% off your first month</p>
              <p style="margin:0; font-size:11.5px; color:#666; line-height:1.6;">
                Disclosure: referral link. mungbeans.io earns a commission if you subscribe, at no extra cost to you. I pay for it myself. This is the only paid link in this email, which carries no advertising.
              </p>
            </td></tr>
          </table>
        </td></tr>
        <tr><td style="padding:40px 0 0 0; border-top:1px solid #2a2a3e; margin-top:40px;">
          <p style="color:#666; font-size:13px; margin:20px 0 0 0;">
            You're receiving this because you signed up at mungbeans.io.<br>
            <a href="{unsub_url}" style="color:#888;">Unsubscribe</a>
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return subject, html


def send_emails(crossings: dict, subscribers: list[str]):
    """Send the email to all subscribers via ZeptoMail API."""
    zoho_email = os.environ.get('ZOHO_EMAIL')
    api_token = os.environ.get('ZEPTOMAIL_API_TOKEN')

    if not zoho_email or not api_token:
        print("ERROR: Missing ZOHO_EMAIL or ZEPTOMAIL_API_TOKEN.")
        sys.exit(1)

    # Validate token format (ZeptoMail expects "Zoho-enczapikey <key>")
    if not api_token.startswith("Zoho-enczapikey"):
        print(f"WARNING: ZEPTOMAIL_API_TOKEN does not start with 'Zoho-enczapikey'.")
        print(f"  Token starts with: '{api_token[:20]}...'")
        print(f"  ZeptoMail requires format: 'Zoho-enczapikey <your-key>'")

    print(f"\nSending weekly email to {len(subscribers)} subscribers via ZeptoMail...")

    sent = 0
    failed = 0
    failed_recipients = []

    for recipient in subscribers:
        unsub_url = f"https://mungbeans.io/.netlify/functions/unsubscribe?email={urllib.parse.quote(recipient)}"
        subject, html = build_email_html(crossings, unsub_url)

        payload = json.dumps({
            "from": {"address": zoho_email, "name": "mungbeans.io"},
            "to": [{"email_address": {"address": recipient}}],
            "subject": subject,
            "htmlbody": html,
        })

        req = urllib.request.Request(
            "https://api.zeptomail.com/v1.1/email",
            data=payload.encode("utf-8"),
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            method="POST"
        )

        try:
            with urllib.request.urlopen(req) as resp:
                resp.read()
            sent += 1
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode('utf-8', errors='replace')
            except Exception:
                pass
            # Print full body for first failure only to keep logs readable
            if not failed_recipients:
                print(f"  FAILED: {recipient}: {e} — Response body: {body}")
            else:
                print(f"  FAILED: {recipient}: {e}")
            failed += 1
            failed_recipients.append(recipient)
        except Exception as e:
            print(f"  FAILED: {recipient}: {e}")
            failed += 1
            failed_recipients.append(recipient)

        # Small delay to avoid ZeptoMail rate limits
        if sent % 10 == 0:
            time.sleep(1)

    print(f"\nResults: {sent} sent, {failed} failed out of {len(subscribers)} total")

    if failed_recipients:
        print(f"Failed recipients: {', '.join(failed_recipients)}")

    # Exit with error if ANY sends failed so the workflow shows red
    if failed > 0:
        print(f"\nERROR: {failed} email(s) failed to send.")
        sys.exit(1)

    print("\nAll emails sent successfully.")


def main():
    dry_run = '--dry-run' in sys.argv

    print("=" * 60)
    print("Below The Line - Weekly Email Sender" + ("  [DRY RUN]" if dry_run else ""))
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Load and validate crossings data
    crossings = load_crossings()

    # 2. Render the email. In a dry run this is the whole job: write the HTML
    #    to disk for eyeballing, print the subject, and touch no network.
    if dry_run:
        subject, html = build_email_html(
            crossings, 'https://mungbeans.io/.netlify/functions/unsubscribe?email=preview%40example.com')
        out = Path(__file__).parent.parent / 'email-preview.html'
        out.write_text(html, encoding='utf-8')
        dive = latest_deep_dive()
        print(f"\nSubject: {subject}")
        print(f"Deep dive teaser: {dive['title'] if dive else '(none published)'}")
        print(f"  -> {dive['url'] if dive else ''}")
        print(f"Godel block: {'present' if 'godelterminal.com' in html else 'MISSING'}")
        print(f"Crossings: {len(crossings.get('newly_below', []))} below, "
              f"{len(crossings.get('newly_recovered', []))} recovered")
        print(f"\nPreview written to: {out}")
        print("No subscribers fetched and no mail sent.")
        return

    # 3. Fetch subscriber list and send
    subscribers = get_subscribers()
    send_emails(crossings, subscribers)


if __name__ == '__main__':
    main()
