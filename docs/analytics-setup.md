# Referral analytics: setup and runbook

GA4 property `G-KMCNBWBVVE`. Three parts: the site fires the events, the GA4
console has to be told they matter, and the MCP server lets Claude read the
results back.

---

## 1. What the site now does

`static/js/referral-events.js` puts one delegated click listener on the
document. Any anchor with `rel="sponsored"` fires:

```
event: referral_click
  tool         godel | simplywallst | tipranks
  placement    home | tools-page | deep-dive-article | deep-dives-index
               | weekly-report | stock-page | all-stocks | book
               | microcaps | disclaimer | other
  link_style   button | inline-strip | screenshot | tools-page-entry
               | box-link | text
  link_url     full destination
  page_path    page the click came from
  link_domain  destination hostname
```

Nothing needs wiring when a tool is added. The partials put `rel="sponsored"`
on every referral anchor, and the listener derives `tool` from the hostname.
The one maintenance point is the `TOOLS` map at the top of the file: add a new
hostname there so it reports a clean name instead of a guessed one.

Loaded from `layouts/partials/head.html` and, because several templates carry
their own hardcoded `<head>`, also from `index.html`, `book/list.html`,
`stocks/list.html`, `stocks/single.html`, `microcaps/list.html`,
`microcaps/single.html`, and the three standalone articles under
`static/deep-dives/`. Those static articles had no GA4 tag at all before this;
they do now.

---

## 2. GA4 console (one-time, ~10 minutes)

### Register the custom dimensions

Without this the parameters are collected but cannot be used in reports.
GA4 only starts populating a dimension from the moment it is registered, so do
this before the traffic you care about arrives.

**Admin → Data display → Custom definitions → Create custom dimension**, three
times:

| Dimension name | Scope | Event parameter |
|---|---|---|
| Referral tool | Event | `tool` |
| Referral placement | Event | `placement` |
| Referral link style | Event | `link_style` |

### Mark the event as a key event

**Admin → Data display → Key events → New key event**, name it exactly
`referral_click`. It can be created before GA4 has ever seen the event.

### Verify

**Admin → DebugView**, with the GA Debugger extension on, or just load the site
and watch Realtime. DebugView shows events within seconds. Standard reports lag
about 24 hours.

---

## 3. The MCP server

Read-only. It queries the Data and Admin APIs so Claude can pull reports; it
cannot create events or change configuration. Everything in section 2 is manual.

### Prerequisites

Enable both APIs in your Google Cloud project:

- Google Analytics Admin API (`analyticsadmin.googleapis.com`)
- Google Analytics Data API (`analyticsdata.googleapis.com`)

### Credentials

```bash
brew install pipx && pipx ensurepath      # if not already installed

gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/analytics.readonly,https://www.googleapis.com/auth/cloud-platform
```

Copy the `Credentials saved to file: [...]` path it prints.

### Register with Claude Code

```bash
claude mcp add analytics-mcp \
  --env GOOGLE_APPLICATION_CREDENTIALS=/path/from/previous/step.json \
  --env GOOGLE_PROJECT_ID=your-project-id \
  -- pipx run analytics-mcp
```

Confirm with `/mcp` inside Claude Code.

---

## 4. The limit worth knowing

GA4 measures the click and stops there. It cannot see whether the click became
a paid subscription, because the conversion happens on Godel's and Simply Wall
St's systems under their own cookies.

So `referral_click` is a leading indicator, not revenue. Actual sales live in
each program's own referral dashboard. Reconciling the two is manual: clicks by
placement from GA4, sales from the vendor, divide.

That division is the number that matters. It tells you whether the deep-dive
placement converts better than the stock-page placement, which is the decision
the whole setup exists to inform.
