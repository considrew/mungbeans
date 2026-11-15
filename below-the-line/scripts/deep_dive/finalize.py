#!/usr/bin/env python3
"""Publish mechanics for a validated automated deep dive.

Deterministic on purpose: the model writes the article and a small meta.json,
and this script does everything the listings, the weekly email and the
performance tracker depend on.

  1. content/deep-dives/<slug>.md   stub with frontmatter (card, badges, tracking)
  2. static/_redirects              forced 301 from the stub URL to the article
  3. data/deep_dive_log.json        append-only record of what was picked and why

Run only after validate.py exits 0.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone

from common import (CANDIDATES, CONTENT_DIR, META, REDIRECTS, SELECTION_LOG, dossier_path,
                    load_json, page_path, write_json)


def q(s) -> str:
    """YAML double-quoted scalar. Inner double quotes become typographic so the
    performance updater's line-based frontmatter regex keeps working."""
    return json.dumps(str(s).replace('"', '”'), ensure_ascii=False)


def slugify(s: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


def main() -> int:
    meta = load_json(META)
    if not meta:
        print(f"ERROR: {META} missing", file=sys.stderr)
        return 1
    ticker = meta["ticker"].upper()
    dossier = load_json(dossier_path(ticker))
    if not dossier or not page_path(ticker).exists():
        print("ERROR: dossier or article missing; run validate.py first", file=sys.stderr)
        return 1

    price = dossier["publish"]["price"]
    date = dossier["publish"]["price_date"]
    article_url = f"/deep-dives/{ticker.lower()}/"

    # The stub must not share the article's URL, or Hugo and the static file
    # would both write public/deep-dives/<ticker>/index.html.
    slug = slugify(meta.get("slug") or f"{meta['company']} {ticker} deep dive")
    if slug == ticker.lower():
        slug += "-deep-dive"
    stub = CONTENT_DIR / f"{slug}.md"
    if stub.exists():
        print(f"ERROR: {stub} already exists", file=sys.stderr)
        return 1

    tags = ["deep-dive", ticker] + [t for t in meta.get("tags", []) if t not in ("deep-dive", ticker)]
    lines = [
        "---",
        f"title: {q(meta['title'])}",
        f"date: {date}",
        "draft: false",
        f"tags: {json.dumps(tags, ensure_ascii=False)}",
        f"description: {q(meta['description'])}",
        f"ticker: {q(ticker)}",
        f"canonical_url: {q(article_url)}",
    ]
    if meta.get("peer_ticker"):
        lines.append(f"ticker_b: {q(meta['peer_ticker'].upper())}")
    lines += [
        f"verdict: {q(meta['verdict'])}",
        f"verdict_label: {q(meta['verdict_label'])}",
        "automated: true",
        f"performance_price_at_publish: {q(f'${price:.2f}')}",
        'performance_since: "+0.0%"',
        f"performance_price_current: {q(f'${price:.2f}')}",
        f"performance_as_of: {q(date)}",
        "---",
        "",
        f"The full deep dive lives at [{article_url}]({article_url}).",
        "",
        meta["description"],
        "",
        "Five tabs: the thesis, the verdict, the overhang ledger, the numbers, and the bear case "
        "given equal room.",
        "",
        f"**Why this stock this week.** {meta['selection_rationale']}",
        "",
    ]
    stub.write_text("\n".join(lines))
    print(f"Stub: {stub}")

    rule = f"/deep-dives/{slug}/  {article_url}  301!"
    existing = REDIRECTS.read_text() if REDIRECTS.exists() else ""
    if rule not in existing:
        REDIRECTS.write_text(existing.rstrip("\n") + "\n" + rule + "\n")
    print(f"Redirect: {rule}")

    cands = load_json(CANDIDATES) or {}
    picked = next((c for c in cands.get("candidates", []) if c["ticker"] == ticker), {})
    log = load_json(SELECTION_LOG, []) or []
    log.append({
        "ticker": ticker,
        "company": meta["company"],
        "published": date,
        "price_at_publish": price,
        "url": article_url,
        "stub": stub.name,
        "verdict": meta["verdict"],
        "shortlist_rank": picked.get("rank"),
        "viewership_score": picked.get("viewership_score"),
        "week_return_pct": picked.get("week_return_pct"),
        "shortlist": [c["ticker"] for c in cands.get("candidates", [])],
        "selection_rationale": meta["selection_rationale"],
        "sources": meta.get("sources", []),
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    write_json(SELECTION_LOG, log)
    print(f"Log: {SELECTION_LOG} ({len(log)} entries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
