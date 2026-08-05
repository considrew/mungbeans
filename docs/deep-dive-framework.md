# The mungbeans deep-dive framework

This is the standard for every deep dive on mungbeans.io, set by the GENI piece
(`below-the-line/static/deep-dives/geni/index.html`). It covers voice, format,
and the publishing mechanics. It is written for any model or person drafting or
revising an article. Read the GENI page before writing; it is the reference
implementation.

## What a deep dive is

A record of reasoning. The site's whole trust model is
that every claim is dated, every call carries its price at publish, performance
is tracked in the open, and mistakes get dated correction notes instead of
silent edits. Write like someone who expects to be graded later, because the
page design guarantees they will be.

## Voice

First person, singular. "I researched," "I think," "my read." Never "we note,"
never "the user," never passive institutional distance. The site reads as one
person doing the work.

Say the thing. Don't announce that you're about to say it. No "it's worth
noting," "frankly," "let's unpack," "the question is whether"
as a transition crutch. No hedging that adds words without adding meaning.

Banned tics, learned the hard way:

- The contrast pair, in every form: "This is not X. It is Y.", "not X, but
  Y", "X, not Y" as a rhetorical flourish. Banned outright, no closing-line
  exception. Make the assertion directly; if the negation carries real
  information (a factual "not double-blinded"), state it as fact.
- "genuinely," "worth noting," "honest" and "honestly" in all their uses, "to
  be honest," and their cousins. Delete them; the sentence survives. If a
  section needs a name for what cannot be known, call it "The gaps."
- Em dashes are salt. Budget roughly one per two paragraphs of prose. Prefer colons for definitions, commas for asides,
  parentheses for specifications, periods for emphasis. Tables and labels are
  exempt.

What good looks like: short declaratives doing real work ("Not glamorous.
Deeply sticky."), numbers embedded in sentences rather than displayed at the
reader, a wit that comes from precision rather than flourish ("index flows
don't care what a thing is worth"). Restraint is the style. When a sentence
sounds like an AI summarizing rather than a person thinking, cut it.

Accountability structures are mandatory. A "The gaps" note in the Numbers tab
for what can't be known yet, a full-strength bear case ("The steelman, given
equal room"), and pre-committed invalidations — falsifiable conditions,
written before they're tested.

## Format: the five-tab page

Standalone HTML, dark warm paper theme, Newsreader for prose, IBM Plex Mono for
numbers and labels. Start every new article from
`below-the-line/templates/deep-dive-v2-template.html`, which carries the full
CSS and skeleton.

- **01 Thesis** (default tab): the complete published article. After publish it
  never changes except dated correction notes (`.note` blocks) appended where
  the error lived. Canonical section order: Why This Takes A Full Read → The
  Company (history as capital allocation) → Management → The Business (revenue
  lines as eyebrow blocks with small tables) → the differentiated angle (the
  thing the market misprices) → NPV Estimate → Head-to-Head vs the obvious peer
  → The Risks (five minimum) → The Path From Here. Disclaimer note at the end
  with price at publish.
- **02 Verdict**: one-sentence call in display type, pills (Call / Conviction /
  Net overhang lean / Recognition timing), and the recognition-gap gauge:
  current price vs a driver-adjusted anchor, with the derivation stated in the
  gauge note alongside the author's own DCF number if they differ. Never let
  two fair values sit unreconciled.
- **03 Overhangs**: the ledger of drivers the metrics don't show, scored
  materiality × unpriced × low-attention × linkage. Positives (▲) and risks
  (▼) both, plus one dormant card (○) that "fires on anomaly." Each card:
  mechanism, priced-in level, attention level, trigger.
- **04 Numbers**: metric grids (latest quarter, then guidance), scenario table
  (bear/base/bull with multiple, implied price, and the path), and "The
  gaps."
- **05 Bear case**: the steelman, given equal room. Short thesis, why the
  re-rate may never fire, pre-committed invalidations as a table.

Hard rules baked into the format:

- Per-share figures always use the **fully diluted** share count, components
  stated (warrants, RSUs, deal consideration). This was learned by correction.
- The performance tag ("published \<date\> at $X · +Y% since publish") never
  disappears and never gets massaged. Losses display in the same type as gains.
- The masthead brand links home. Footer carries the record-of-reasoning line.

## Publishing mechanics

1. Article lives at `below-the-line/static/deep-dives/<ticker>/index.html`.
2. A markdown stub in `below-the-line/content/deep-dives/` carries frontmatter
   (title, date, description, ticker, ticker_b, verdict, verdict_label,
   performance_* fields) so the article appears in the deep-dives index,
   homepage cards, and weekly-report links. If the stub predates the HTML page,
   add a forced redirect in `below-the-line/static/_redirects`:
   `/deep-dives/<old-slug>/  /deep-dives/<ticker>/  301!`
3. Refresh `performance_since`, `performance_price_current`, and
   `performance_as_of` in the stub, and the masthead price/tag in the HTML,
   whenever prices are updated. Verify percentages: current/publish − 1.
4. If the author holds the position, it goes in The Book
   (`below-the-line/data/positions/*.yml`) with real fills, and the position
   thesis cites the same DCF number as the article. When the article's numbers
   are corrected, the position files are corrected in the same commit.
5. Build with Hugo 0.139.0 extended and verify before handoff. Git runs in the
   author's terminal, never in a sandbox.

## Corrections

Errors get a dated `.note` block at the point of error ("Correction and update
(4 Jul 2026). ...") explaining what was wrong, what changed, and what cuts the
other way. Downstream numbers are corrected inline. Nothing is silently
rewritten; the correction is part of the record.
