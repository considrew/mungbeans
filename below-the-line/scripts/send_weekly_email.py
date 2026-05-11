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

    form_id = None
    for form in forms:
        if form.get('name') == 'notify':
            form_id = form['id']
            break

    if not form_id:
        print("ERROR: 'notify' form not found on Netlify site.")
        sys.exit(1)

    # Fetch all submissions (paginated)
    emails = set()
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
            print(f"WARNING: Failed to fetch submissions page {page}: {e}")
            break

        if not subs:
            break

        for sub in subs:
            email = sub.get('data', {}).get('email', '').strip().lower()
            if email:
                emails.add(email)
        page += 1

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


def build_email_html(crossings: dict, unsub_url: str) -> tuple[str, str]:
    """Build the email subject and HTML body from crossings data.

    Returns (subject, html_body).
    """
    date_display = crossings.get('date_display', crossings['date'])
    slug = crossings.get('blog_slug', f"{crossings['date']}-weekly-signal-report")
    post_url = f"https://mungbeans.io/blog/{slug}/"

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
          {below_section}
          {recovered_section}
          <p style="margin:28px 0 0 0;">
            <a href="{post_url}" style="display:inline-block; background-color:#e2b714; color:#1a1a2e; text-decoration:none; padding:12px 28px; border-radius:6px; font-weight:600; font-size:15px;">View Full Report</a>
          </p>
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
            "headers": {
                "List-Unsubscribe": f"<{unsub_url}>",
                "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"
            }
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
    print("=" * 60)
    print("Below The Line - Weekly Email Sender")
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Load and validate crossings data
    crossings = load_crossings()

    # 2. Fetch subscriber list
    subscribers = get_subscribers()

    # 3. Send emails
    send_emails(crossings, subscribers)


if __name__ == '__main__':
    main()
