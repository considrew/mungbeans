"""Shared paths and helpers for the weekly automated deep dive.

Pipeline (see .github/workflows/weekly-deep-dive.yml):

A Claude scheduled task (prompts/scheduled-task.md) does the writing, on the
account's subscription. GitHub Actions does the publishing.

  In the scheduled Claude session:
    select_candidates.py  -> .deep-dive/candidates.json   (deterministic shortlist)
    Claude (writer)       -> picks one, writes the page + .deep-dive/meta.json
    dossier.py TICKER     -> .deep-dive/dossier-TICKER.json (numbers Claude writes from)
    Claude (editor)       -> fresh-context fact check and voice pass
    submit.py             -> validates, files the run under docs/deep-dive-runs/TICKER/,
                             commits; the session pushes its claude/* branch

  In .github/workflows/weekly-deep-dive.yml, on that push:
    validate.py           -> hard gate, re-run from the filed run; nothing
                             publishes unless it exits 0
    finalize.py           -> stub, redirect, selection log; then commit to main
                             and deploy
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

SITE = Path(__file__).resolve().parents[2]          # below-the-line/
REPO = SITE.parent
# Scratch while writing (git-ignored). The publish workflow points this at the
# filed run instead: DEEP_DIVE_WORK=docs/deep-dive-runs/<TICKER>.
WORK = Path(os.environ.get("DEEP_DIVE_WORK") or REPO / ".deep-dive").resolve()
RUNS = REPO / "docs" / "deep-dive-runs"               # committed record of each run

STOCKS_JSON = SITE / "assets" / "data" / "stocks.json"
CONTENT_DIR = SITE / "content" / "deep-dives"
STATIC_DIR = SITE / "static" / "deep-dives"
REDIRECTS = SITE / "static" / "_redirects"
TEMPLATE = SITE / "templates" / "deep-dive-v2-template.html"
SELECTION_LOG = SITE / "data" / "deep_dive_log.json"

CANDIDATES = WORK / "candidates.json"
META = WORK / "meta.json"


def dossier_path(ticker: str) -> Path:
    return WORK / f"dossier-{ticker.upper()}.json"


def page_path(ticker: str) -> Path:
    return STATIC_DIR / ticker.lower() / "index.html"


def load_json(path: Path, default=None):
    if not path.exists():
        return default
    with open(path) as f:
        return json.load(f)


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)
        f.write("\n")


def frontmatter(path: Path) -> dict:
    """Flat key: value frontmatter of a markdown stub (no nested YAML needed)."""
    text = path.read_text()
    m = re.match(r"^---\n(.*?)\n---", text, re.DOTALL)
    out: dict = {}
    if not m:
        return out
    for line in m.group(1).splitlines():
        if ":" not in line or line.startswith((" ", "-")):
            continue
        k, _, v = line.partition(":")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def covered_tickers() -> set[str]:
    """Every ticker that already has a deep dive, in any role.

    Sources: stub frontmatter (ticker, ticker_b..e), standalone article
    directories, and the automation's own selection log.
    """
    covered: set[str] = set()
    if CONTENT_DIR.exists():
        for md in CONTENT_DIR.glob("*.md"):
            fm = frontmatter(md)
            # Only the primary ticker blocks a new dive. A stock that appeared
            # as the peer in someone else's head-to-head has not had its own.
            t = fm.get("ticker", "").upper()
            if t:
                covered.add(t)
    if STATIC_DIR.exists():
        for d in STATIC_DIR.iterdir():
            if d.is_dir():
                covered.add(d.name.split("-")[0].upper())
    for row in load_json(SELECTION_LOG, []) or []:
        if row.get("ticker"):
            covered.add(row["ticker"].upper())
    return covered
