#!/usr/bin/env python3
"""Hard gate for an automated deep dive. Exit 0 or nothing publishes.

Checks what a machine can check: the five-tab structure, the depth floor set by
the GENI reference piece, the banned voice tics from docs/deep-dive-framework.md,
and that the publish price, share count and market cap on the page agree with
the dossier. It cannot judge whether the thesis is right. That is what the
editor pass and the public performance tag are for.

Usage:
    python validate.py                 # validates the ticker in .deep-dive/meta.json
    python validate.py --page PATH     # structure/voice checks only, on any page
    python validate.py --check-scope   # also fail if files outside the allowed set changed
"""
from __future__ import annotations

import argparse
import html
import re
import subprocess
import sys
from pathlib import Path

from common import (CANDIDATES, CONTENT_DIR, META, REPO, SELECTION_LOG, dossier_path,
                    frontmatter, load_json, page_path)

# Depth floors. GENI: thesis 6,205 words, 8 tables, 6 overhang cards. PAR: 5,436.
MIN_WORDS = {"thesis": 4500, "verdict": 100, "overhangs": 300, "numbers": 130, "bear": 180}
MIN_THESIS_TABLES = 5
MIN_THESIS_H3 = 10
MIN_RISKS = 5
MIN_OVERHANGS = 5
MIN_INVALIDATIONS = 3
MIN_METRIC_CARDS = 6

VERDICTS = {"deep-value", "value-trap", "speculative-buy", "turnaround", "mixed", "wait"}
META_REQUIRED = ["ticker", "company", "title", "description", "tags", "verdict",
                 "verdict_label", "selection_rationale", "sources"]

BANNED = [
    r"worth noting", r"\bfrankly\b", r"let[’']s unpack", r"\bgenuinely\b", r"\bhonest(?:ly)?\b",
    r"the question is whether", r"\bwe note\b", r"\bwe (?:think|believe|see|expect)\b",
    r"\bin our view\b", r"\bthe user\b", r"\bdelve\b", r"\bit[’']s important to\b",
    r"\bas an ai\b", r"\blanguage model\b",
]
# The contrast pair: "This is not X. It is Y." and "not X, but Y".
CONTRAST = [
    r"[^.!?]*\b(?:is|are|was|were|isn[’']t|aren[’']t|wasn[’']t)\s+not\b[^.!?]{1,90}[.!?]\s+"
    r"(?:It|This|That|They|These)(?:[’']s|[’']re| is| are| was| were)\b[^.!?]*[.!?]",
    r"\bnot\b[^.;:!?]{1,60},\s+but\b[^.!?]*",
]
REQUIRED_H3 = ["Why This Takes A Full Read", "The Company", "The Management Team",
               "The Business", "NPV Estimate", "The Risks", "The Path From Here"]


def text_of(fragment: str) -> str:
    fragment = re.sub(r"<(script|style)\b.*?</\1>", " ", fragment, flags=re.S)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


def money(s: str) -> float | None:
    m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", s)
    return float(m.group(1).replace(",", "")) if m else None


class Report:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def err(self, msg):
        self.errors.append(msg)

    def warn(self, msg):
        self.warnings.append(msg)

    def need(self, cond, msg):
        if not cond:
            self.err(msg)


def check_page(src: str, r: Report) -> dict:
    """Structure, depth and voice. Returns facts read off the page."""
    facts: dict = {}
    if "<body>" not in src:
        r.err("no <body>")
        return facts
    head, body = src.split("<body>", 1)
    visible = re.sub(r"<(script|style)\b.*?</\1>", "", body, flags=re.S)

    r.need("G-KMCNBWBVVE" in head, "GA4 tag missing from <head> (start from the template)")
    r.need("/js/referral-events.js" in head, "referral-events.js missing from <head>")
    r.need("Newsreader" in head and "IBM+Plex+Mono" in head, "template fonts missing")
    r.need("/.netlify/functions/quote" in body, "live-price script missing")
    r.need('href="/"' in visible and "mungbeans" in visible, "masthead brand must link home")
    r.need("A record of reasoning" in visible, "footer must carry the record-of-reasoning line")

    left = re.findall(r"\{\{[^}]*\}\}", visible + head)
    r.need(not left, f"unfilled template placeholders: {left[:5]}")
    r.need("<!--" not in visible, "template guidance comments left in the body; remove them")

    panels = {}
    for chunk in re.split(r'<section class="panel', visible)[1:]:
        m = re.search(r'data-panel="(\w+)"', chunk)
        if m:
            panels[m.group(1)] = chunk.split("</section>")[0]
    for name in ["thesis", "verdict", "overhangs", "numbers", "bear"]:
        if name not in panels:
            r.err(f"missing tab panel: {name}")
            continue
        words = len(text_of(panels[name]).split())
        facts[f"words_{name}"] = words
        r.need(words >= MIN_WORDS[name],
               f"{name} tab is {words} words; the floor is {MIN_WORDS[name]}")
    if len(panels) < 5:
        return facts

    # --- 01 Thesis
    th = panels["thesis"]
    h3 = [text_of(x) for x in re.findall(r"<h3[^>]*>(.*?)</h3>", th, flags=re.S)]
    for want in REQUIRED_H3:
        r.need(any(h.startswith(want) for h in h3), f'thesis is missing the "{want}" section')
    r.need(any(re.search(r"\bvs\.", h) for h in h3), "thesis is missing the head-to-head (\"X vs. Y\") section")
    order = [next((i for i, h in enumerate(h3) if h.startswith(w)), -1) for w in REQUIRED_H3]
    r.need(order == sorted(order), "thesis sections are out of the canonical order")
    r.need(len(h3) >= MIN_THESIS_H3, f"thesis has {len(h3)} sections; GENI has 12, floor is {MIN_THESIS_H3}")
    r.need(th.count("<table") >= MIN_THESIS_TABLES,
           f"thesis has {th.count('<table')} tables; floor is {MIN_THESIS_TABLES}")
    r.need(th.count('class="eyebrow"') >= 2, "revenue lines must be eyebrow blocks (at least two)")
    m = re.search(r"<h3[^>]*>\s*The Risks.*?</h3>(.*?)(?:<h3|$)", th, flags=re.S)
    if m:
        risks = len(re.findall(r"<p[^>]*>\s*<(?:b|strong)>", m.group(1)))
        r.need(risks >= MIN_RISKS, f"The Risks has {risks} bolded risks; never fewer than {MIN_RISKS}")
    m = re.search(r"<h3[^>]*>\s*NPV Estimate.*?</h3>(.*?)(?:<h3|$)", th, flags=re.S)
    if m:
        npv = text_of(m.group(1)).lower()
        r.need("<table" in m.group(1), "NPV Estimate needs the DCF table")
        r.need("fully diluted" in npv, "NPV Estimate must state the FULLY DILUTED share count with components")
        r.need("wacc" in npv or "discount rate" in npv, "NPV Estimate must state the discount rate")
        r.need("terminal" in npv, "NPV Estimate must state the terminal assumption")
    disc = re.search(r"Price at publish:\s*(\$[\d,.]+)", text_of(th))
    r.need(disc, 'thesis disclaimer must end with "Price at publish: $X (date)"')
    facts["disclaimer_price"] = money(disc.group(1)) if disc else None
    r.need("automated research pipeline" in text_of(th) and "without human review" in text_of(th),
           "thesis disclaimer must carry the provenance line (automated research pipeline, "
           "published without human review, no position held)")

    # --- 02 Verdict
    vd = panels["verdict"]
    r.need('class="verdict"' in vd, "verdict tab needs the one-sentence call in display type")
    r.need(vd.count('class="pill') >= 4, "verdict tab needs four pills (Call / Conviction / Lean / Recognition)")
    r.need('class="gauge"' in vd and 'class="gnote"' in vd, "verdict tab needs the recognition-gap gauge and its note")
    gn = re.search(r'class="gnote">(.*?)</div>', vd, flags=re.S)
    r.need(gn and "dcf" in text_of(gn.group(1)).lower(),
           "gauge note must reconcile the anchor with the thesis DCF (never two unexplained fair values)")
    now = re.search(r'class="mk p".*?class="v">(.*?)</div>', vd, flags=re.S)
    facts["gauge_price"] = money(text_of(now.group(1))) if now else None
    for pct in re.findall(r"(?:left|right):\s*(-?[\d.]+)%", vd):
        r.need(0 <= float(pct) <= 100, f"gauge position {pct}% is off the track")

    # --- 03 Overhangs
    oh = panels["overhangs"]
    cards = re.findall(r'class="ohcard ([\w ]+)"', oh)
    r.need(len(cards) >= MIN_OVERHANGS, f"{len(cards)} overhang cards; floor is {MIN_OVERHANGS}")
    r.need(sum("up" in c for c in cards) >= 2, "overhang ledger needs at least two ▲ positives")
    r.need(sum("risk" in c for c in cards) >= 2, "overhang ledger needs at least two ▼ risks")
    r.need(sum("dorm" in c for c in cards) == 1, "overhang ledger needs exactly one dormant ○ card")
    live = len(cards) - 1       # the dormant card carries a trigger only
    r.need(oh.count("Priced in") >= live and oh.count("Trigger") >= live,
           "every live overhang card needs Priced in / Attention / Trigger")
    for sc in re.findall(r'class="oh-score">(.*?)<', oh):
        try:
            r.need(0 <= float(sc) <= 1, f"overhang score {sc} outside 0–1")
        except ValueError:
            r.err(f"overhang score {sc!r} is not a number")

    # --- 04 Numbers
    nm = panels["numbers"]
    r.need(nm.count('class="met"') >= MIN_METRIC_CARDS, f"numbers tab needs at least {MIN_METRIC_CARDS} metric cards")
    nt = text_of(nm)
    r.need(all(re.search(rf"\b{s}\b", nt) for s in ("Bear", "Base", "Bull")), "scenario table needs Bear / Base / Bull")
    r.need("The gaps." in nt, 'numbers tab needs the "The gaps." note')

    # --- 05 Bear case
    br = panels["bear"]
    bt = text_of(br)
    r.need("The steelman, given equal room" in bt, 'bear tab must open "The steelman, given equal room."')
    r.need("Pre-committed invalidations" in bt, "bear tab needs the pre-committed invalidations table")
    inv = br.split("Pre-committed invalidations")[-1]
    r.need(len(re.findall(r"<tr", inv)) >= MIN_INVALIDATIONS,
           f"at least {MIN_INVALIDATIONS} falsifiable invalidations")

    # --- Voice, across everything the reader sees
    full = text_of(visible)
    for pat in BANNED:
        hits = re.findall(pat, full, flags=re.I)
        if hits:
            r.err(f'banned phrase "{hits[0]}" appears {len(hits)}x')
    for pat in CONTRAST:
        for m in re.finditer(pat, full):
            r.err(f'contrast pair (banned outright): "…{m.group(0).strip()[:140]}"')
    prose = " ".join(re.findall(r"<p[^>]*>(.*?)</p>", visible, flags=re.S))
    paras = len(re.findall(r"<p[ >]", visible))
    dashes = prose.count("—")
    facts["em_dashes"], facts["paragraphs"] = dashes, paras
    r.need(dashes <= paras / 2, f"{dashes} em dashes in {paras} paragraphs; budget is one per two paragraphs")
    first_person = len(re.findall(r"\b(?:I|my|I[’']m|I[’']ve|I[’']d)\b", text_of(th)))
    r.need(first_person >= 12, f"thesis reads institutional: only {first_person} first-person uses")

    # --- Masthead facts
    px = re.search(r'<span class="px"[^>]*id="livePx"[^>]*>(.*?)</span>', body, flags=re.S)
    r.need(px, "masthead price span (#livePx) missing")
    if px:
        tag = px.group(0)
        attr = lambda k: (re.search(rf'data-{k}="([^"]*)"', tag) or [None, None])[1]

        def num(k):
            try:
                return float(attr(k) or 0)
            except ValueError:
                r.err(f"data-{k} must be a plain number, got {attr(k)!r}")
                return 0.0
        facts.update(symbol=attr("symbol"), since=attr("since"),
                     publish=num("publish"), shares=num("shares"))
        inner = text_of(px.group(1))
        facts["masthead_price"] = money(inner)
        cap = re.search(r"cap\s*\$\s*([\d.]+)\s*([BMT])", inner)
        if cap:
            facts["masthead_cap"] = float(cap.group(1)) * {"M": 1e6, "B": 1e9, "T": 1e12}[cap.group(2)]
    perf = re.search(r'class="perf-since">(.*?)</div>', body, flags=re.S)
    r.need(perf and 'id="livePerf"' in perf.group(1), "performance tag (.perf-since with #livePerf) missing")
    facts["perf_price"] = money(text_of(perf.group(1))) if perf else None
    return facts


def check_against_dossier(facts: dict, meta: dict, r: Report) -> None:
    ticker = meta["ticker"].upper()
    dossier = load_json(dossier_path(ticker))
    if not dossier:
        r.err(f"no dossier for {ticker}; run dossier.py {ticker}")
        return
    price = dossier["publish"]["price"]
    for label in ("publish", "masthead_price", "perf_price", "disclaimer_price", "gauge_price"):
        got = facts.get(label)
        r.need(got is not None and abs(got - price) < 0.006,
               f"{label} is {got}; the dossier publish price is {price}")
    r.need(facts.get("symbol") == ticker, f"#livePx data-symbol is {facts.get('symbol')}, expected {ticker}")
    r.need(facts.get("since") == dossier["publish"]["price_date"],
           f"data-since is {facts.get('since')}; publish date is {dossier['publish']['price_date']}")

    basic = (dossier.get("info") or {}).get("sharesOutstanding") or 0
    shares = facts.get("shares") or 0
    r.need(shares > 0, "data-shares (fully diluted count, integer) missing")
    if basic and shares:
        r.need(shares >= basic * 0.97,
               f"data-shares {shares:,.0f} is below the basic count {basic:,.0f}; it must be FULLY DILUTED")
        if shares > basic * 1.6:
            r.warn(f"diluted count {shares:,.0f} is {shares / basic:.2f}x basic; confirm the components")
    cap = facts.get("masthead_cap")
    if cap and shares:
        r.need(abs(cap / (price * shares) - 1) < 0.03,
               f"masthead cap ${cap / 1e9:.2f}B disagrees with price × diluted shares "
               f"(${price * shares / 1e9:.2f}B)")


def check_meta(meta: dict, r: Report, skip_shortlist: bool) -> None:
    for k in META_REQUIRED:
        r.need(meta.get(k), f"meta.json is missing {k}")
    r.need(meta.get("verdict") in VERDICTS, f"verdict must be one of {sorted(VERDICTS)}")
    r.need(60 <= len(meta.get("description", "")) <= 400, "description should be 60–400 characters")
    r.need(len(meta.get("sources") or []) >= 6, "list at least six sources actually used")
    r.need(len(meta.get("selection_rationale", "")) >= 120, "selection_rationale should explain the pick in a few sentences")
    ticker = (meta.get("ticker") or "").upper()

    cands = load_json(CANDIDATES)
    if cands and not skip_shortlist:
        short = [c["ticker"] for c in cands["candidates"]]
        r.need(ticker in short, f"{ticker} is not on this week's shortlist {short}")
    already = {frontmatter(md).get("ticker", "").upper() for md in CONTENT_DIR.glob("*.md")}
    already |= {row.get("ticker", "").upper() for row in (load_json(SELECTION_LOG, []) or [])}
    r.need(ticker not in already, f"{ticker} already has a deep dive; every week is a new stock")


def check_scope(ticker: str, r: Report) -> None:
    """The writer may touch one page and nothing else in the repository."""
    out = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"],
                         cwd=REPO, capture_output=True, text=True).stdout
    allowed = f"below-the-line/static/deep-dives/{ticker.lower()}/index.html"
    for line in out.splitlines():
        path = line[3:].strip().strip('"')
        if path != allowed and not path.startswith(".deep-dive/"):
            r.err(f"unexpected change outside the article: {line.strip()}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--page", type=Path, help="validate structure/voice of any page; skips dossier and meta checks")
    ap.add_argument("--check-scope", action="store_true")
    ap.add_argument("--skip-shortlist", action="store_true", help="manual ticker override runs")
    args = ap.parse_args()

    r = Report()
    if args.page:
        facts = check_page(args.page.read_text(), r)
    else:
        meta = load_json(META)
        if not meta or not meta.get("ticker"):
            print(f"FAIL: {META} missing or has no ticker")
            return 1
        check_meta(meta, r, args.skip_shortlist)
        page = page_path(meta["ticker"])
        if not page.exists():
            print(f"FAIL: {page} does not exist")
            return 1
        facts = check_page(page.read_text(), r)
        check_against_dossier(facts, meta, r)
        if args.check_scope:
            check_scope(meta["ticker"], r)

    print("words per tab:", {k[6:]: v for k, v in facts.items() if k.startswith("words_")})
    for w in r.warnings:
        print(f"WARN  {w}")
    for e in r.errors:
        print(f"ERROR {e}")
    print(f"\n{'PASS' if not r.errors else 'FAIL'}: {len(r.errors)} errors, {len(r.warnings)} warnings")
    return 1 if r.errors else 0


if __name__ == "__main__":
    sys.exit(main())
