# The Book — position authoring tool

A small **local** form for entering positions into The Book. It runs on your
machine, writes YAML straight into the repo's data files, and (optionally)
commits + pushes so Netlify rebuilds. Nothing here touches a brokerage, asks for
a login, or persists anything in the browser.

This is **Layer A** (authored reasoning). The numbers — prices, P&L, the equity
curve — are generated separately by the build scripts and never typed here.

## Run it

From the Hugo site root (`below-the-line/`):

```bash
npm run admin
```

That installs the one dependency (`js-yaml`) the first time and serves the form
at **http://localhost:8088**. Stop it with `Ctrl-C`.

(Equivalent: `cd tools/admin && npm install && npm start`.)

## What it writes

| Asset type        | File written                       |
|-------------------|------------------------------------|
| Stock / Call / Put | `data/positions/{id}.yml` (one file per position) |
| Cash              | `data/cash.yml` (appends a dated balance) |

The four asset types are the only ones supported: **stock, call, put, cash.**

### The publish gate

A position with an empty **thesis** is written with `status: draft`. Draft
positions are excluded from the build — they never appear in `book.json` or on
the site. **Writing the thesis is the act that publishes a position.** Fill it in
and re-save to flip a draft to `open`.

### Positions vs events

Each position file holds the thesis (the "why I entered") plus an ordered list
of **events**. The first event is usually an `open`; later you append `add`,
`trim`, `note`, or `close`. Adding a `close` event marks the position `closed`
and it flows to the Track Record scoreboard.

- **Stock** events use `shares` + `price` (share price).
- **Call / Put** events use `contracts` + `price` (per-share premium). Committed
  $ for options is premium-at-risk (`contracts × 100 × premium`), not notional.
- **Options** also carry a position-level `current_mark` (per-share premium you
  update by hand on deploys). If absent, the UI shows the last known premium and
  labels it "mark as of {last event date}".

## Commit & push

The form has a **Commit & push** button that shells out to
`git add data/ && git commit && git push` on this repo only. Use it for a
one-click deploy, or skip it and commit yourself. It runs no auth flow — it uses
whatever git credentials your machine already has.

## What this tool will not do

- No brokerage connection, PDF parsing, or stored credentials.
- No `localStorage` / `sessionStorage` — everything goes to repo files on disk.
- The account type/name is never recorded or shown.
