---
title: "Introducing the Micro Cap Screener — and Why Compugen (CGEN) Is Our First Deep Dive"
date: 2026-05-16
draft: false
tags: ["microcap", "screener", "biotech", "CGEN"]
description: "The 200-week moving average works for large caps. Micro caps are a different animal entirely. We built a new screener from scratch, and Compugen is the first stock that made us stop and look twice."
ticker: "CGEN"
---

## Why Micro Caps Need Their Own Framework

The 200-week moving average framework that powers this site was built for established businesses — companies with decades of trading history, consistent cash flows, and enough analyst coverage that mispricings eventually get corrected. It works because large caps are well-studied, widely owned, and mean-reverting. When a $50 billion company crosses below its 200WMA, the market is usually overreacting to a temporary problem. The data says so.

Micro caps operate under entirely different physics.

A $274 million company doesn't have a reliable 200WMA because it may not have existed in its current form four years ago. It doesn't have 15 analysts publishing quarterly estimates. It doesn't have passive index funds backstopping the price. And the risks that matter most — dilution, key-person dependency, cash runway, regulatory binary events — don't show up in moving average signals.

So we built a new screener from scratch. Different inputs, different red flags, different scoring. The methodology is [documented here](/methodology/micro-cap-innovation/), but the short version is: we screen companies between $50M and $500M market cap for patent activity, cash runway, revenue trajectory, and management quality. We auto-reject companies with reverse splits, heavy dilution, excessive stock compensation, or hype-level valuations on sub-$1M revenue. What's left gets scored 0-12 and published to the [screener page](/microcaps/).

This article is the first in a series where we pick a stock from the screener and actually run through it — not with the five-layer forensic framework we use for large caps, but with the 7-step risk assessment that matters for companies at this stage. Think of it as the micro cap equivalent of our "Deep Value or Value Trap?" series. Same rigor, different lens.

---

## Why Compugen?

Out of 82 companies that passed our filters, Compugen (CGEN) stopped us cold. Here's what the screener surfaced:

| Metric | CGEN | Why It Matters |
|---|---|---|
| Market cap | $274M | Squarely in micro cap range |
| Cash on hand | $145.6M | 53% of market cap is cash |
| Revenue | $72.8M | Profitable — unusual for biotech |
| Revenue growth | 4,477% | From ~$1.6M prior year |
| Operating margin | 83.2% | Not a typo |
| FCF yield | 8.87% | Positive FCF in a clinical-stage biotech |
| Gross margin | 87.3% | Licensing economics |
| ROE | 44.8% | Cash-efficient |
| Debt/equity | 2.9% | Virtually unlevered |
| Red flags | 0 | No reverse splits, no dilution, no SBC abuse |
| Screener score | 8/12 | High on revenue + runway, needs patent investigation |

That combination — profitable biotech with 53% of market cap in cash, no red flags, and a near-9% FCF yield — doesn't usually exist. When it does, there's either something wrong with the data or something the market hasn't figured out yet.

Let's find out which one it is.

---

## Step 1: What Does Compugen Actually Do?

Compugen is an Israeli computational biology company that discovers novel immune checkpoint targets using proprietary algorithms. In plain language: they use computers to find proteins on the surface of immune cells that no one else has identified, then develop antibodies that target those proteins to treat cancer.

The company was founded in 1993 as a pure computational genomics business. The pivot to immuno-oncology drug discovery happened in the early 2010s, and it took over a decade to start paying off. Today, Compugen has two validated discoveries that major pharma companies are developing:

**Rilvegostomig** — A PD-1/TIGIT bispecific antibody licensed to AstraZeneca. Currently in **10 active Phase 3 clinical trials** across lung cancer (NSCLC), gastric cancer, and other solid tumors. AstraZeneca has stated a non-risk-adjusted peak annual revenue potential exceeding **$5 billion** for this drug. In early Phase 1/2 data, response rates exceeded 60% in checkpoint-naïve PD-L1-high NSCLC patients, with median progression-free survival of 12-21 months.

**GS-0321** (formerly COM503) — A potential first-in-class anti-IL-18 binding protein antibody licensed to Gilead. Earlier stage than rilvegostomig but in active development.

**Proprietary pipeline** — COM701 (anti-PVRIG, first-in-class) and COM902 (anti-TIGIT, potential best-in-class) in Phase 1. These are wholly owned by Compugen and represent the company's independent value creation beyond the partnerships.

**The one-sentence version:** Compugen discovers cancer drug targets using computation, licenses them to AstraZeneca and Gilead, and collects milestones and royalties while also developing its own drugs.

---

## Step 2: How Big Is the Opportunity?

The global immuno-oncology market was approximately $90 billion in 2025 and growing double digits. But that's the useless TAM number. What matters for Compugen is much more specific:

**Rilvegostomig's addressable market:** First-line NSCLC alone is a ~$30 billion annual market, currently dominated by Merck's Keytruda. AstraZeneca's Phase 3 trials are head-to-head against pembrolizumab (Keytruda). If rilvegostomig shows superiority — which the Phase 1/2 data suggests is plausible — it could capture a significant fraction of this market. AstraZeneca's $5 billion peak revenue estimate implies roughly 15-17% market share across all approved indications.

**What Compugen gets from that:** Tiered royalties of up to mid-single digits on sales, plus up to $195 million in regulatory and commercial milestones. At 4-5% royalties on $5 billion in peak sales, that's $200-250 million per year in royalties alone — on a company with a current market cap of $274 million.

**The Gilead partnership:** Less mature but potentially significant. Combined partnership milestones total up to $1 billion across both deals.

**Proprietary pipeline:** If COM701 or COM902 succeeds in trials, Compugen would either commercialize independently or license at much higher economics than it secured in the earlier AstraZeneca deal (which was signed when the company was more capital-constrained).

---

## Step 3: Is the Technology Defensible?

Our screener scored CGEN 0/3 on patents. The screener was wrong - that's a work in progress that will improve on the next update. Here's what manual investigation turned up:

### The Patent Portfolio the Screener Missed

Compugen holds multiple granted U.S. patents protecting its proprietary pipeline:

| Patent | Coverage | Expiry |
|---|---|---|
| US 10,751,415 | COM902 composition of matter — anti-TIGIT antibody alone or in combination with anti-PD-1 and anti-PVRIG | August 2037 |
| US 11,225,523 | COM701 method of use — anti-PVRIG in triple combination with anti-PD-1 and anti-TIGIT for cancer | August 2037 |
| US 12,152,084 (Nov 2024) | COM902 method of use — triple combination with any anti-PD-1 and any anti-PVRIG for cancer | August 2037 |
| JP 7348072B2 (Oct 2023) | Triple combination antibody therapies — Japanese market protection | ~2037 |
| EU PVRIG patent | Anti-PVRIG antibodies — survived opposition from GSK and a third party (July 2023) | ~2037 |

The European patent is particularly telling. GSK — one of the largest pharma companies in the world — actively challenged Compugen's PVRIG patent and lost. When a competitor spends legal resources trying to invalidate your IP and fails, that's as strong a defensibility signal as you'll find.

**Why the screener missed them:** The automated PatentsView API lookup searches by assignee name, but Compugen is an Israeli company. Variations in entity naming ("Compugen Ltd." vs "Compugen Ltd" vs the Hebrew legal entity) likely caused the lookup to return zero results. This is a known limitation of the automated screen for foreign-domiciled companies — and exactly why the 7-step manual assessment exists as a layer on top of the screener.

### Beyond Patents: Multi-Layer Defensibility

**Proprietary computational discovery platform.** The algorithms that discovered PVRIG and TIGIT's role in immune evasion are trade secrets. Other companies can't replicate the discovery process without building equivalent computational infrastructure and biological databases over a decade-plus timeframe.

**Regulatory moats.** Each drug is protected by data exclusivity and regulatory exclusivity upon approval, layered on top of the composition-of-matter and method-of-use patents above. Biosimilar competition is effectively blocked for 12+ years post-approval.

**First-in-class novelty.** COM701 targets PVRIG — a checkpoint that Compugen discovered. There is no competing anti-PVRIG antibody in clinical development anywhere in the world. This isn't a me-too drug chasing a validated target. It IS the validated target.

**Corrected screener score:** With the patent portfolio confirmed and validated through opposition proceedings, the defensibility score should be 2-3/3 rather than 0/3. Total adjusted score: **10-11/12.**

---

## Step 4: Can They Survive Long Enough to Win?

This is where CGEN shines relative to the typical biotech.

**Cash position:** $145.6 million as of December 31, 2025.

**Cash runway:** Management has explicitly stated the runway extends **into 2029** — well beyond the expected readouts from AstraZeneca's Phase 3 trials (primary completion dates in 2027-2029).

**How they got there:** In December 2025, Compugen monetized a small portion of its rilvegostomig future royalties for $65 million upfront plus a potential $25 million milestone tied to BLA acceptance. This was a strategic move to extend runway without diluting shareholders — they sold future royalty income (at a discount) rather than issuing new shares.

**Operating cash flow:** Positive $31.6 million in 2025, primarily driven by partnership revenue recognition. This won't recur every quarter at this level — it's lumpy and milestone-dependent — but the important point is that Compugen is not purely dependent on its cash pile. Future milestones provide additional runway extensions without dilution.

**The critical caveat:** The $72.8 million in 2025 revenue and the resulting profitability are primarily from the AstraZeneca royalty monetization deal and milestone recognition. This is NOT recurring product revenue. It's a one-time structural event. Future quarters will look very different — likely returning to single-digit-million-dollar revenue from collaboration income, with R&D expenses of $8-12 million per quarter.

The screener scored CGEN 3/3 on runway for good reason: 2029 runway with milestones still outstanding means near-zero dilution risk for at least three years.

---

## Step 5: Is Management Aligned With You?

**Insider ownership:** Relatively low at ~2.3% for a company this size. This is common for Israeli biotechs where founding scientists have sold down over three decades. Not an alarm bell, but not a conviction signal either.

**Dilution history:** Critically, Compugen is NOT diluting. Shares outstanding are stable. No ATM (at-the-market) offering programs running. No shelf registration being tapped. The royalty monetization strategy explicitly avoided equity issuance — a shareholder-friendly decision that management deserves credit for.

**Compensation:** SBC-to-revenue ratio is 0.0% in the screener data. This reflects the unusual revenue spike in 2025. On a normalized basis (against run-rate R&D spend), compensation appears reasonable for a clinical-stage biotech.

**CEO Anat Cohen-Dayag, Ph.D.:** Has led Compugen since 2002. Over two decades at the helm with a clear strategic evolution from pure genomics to drug discovery to clinical development to partnership monetization. The fact that the same team executed the AstraZeneca and Gilead deals — from computational discovery through clinical validation to licensing — is a meaningful track record.

**Red flag check:** No reverse splits, no heavy dilution, no SBC abuse. Clean.

---

## Step 6: What Has to Go Right?

Compugen's investment thesis has a clear hierarchy of catalysts:

**Tier 1 (highest impact):**
- AstraZeneca Phase 3 data readouts for rilvegostomig (expected 2027-2028). Positive results would validate the drug commercially and trigger milestone payments.
- BLA (Biologics License Application) acceptance by FDA, which triggers the additional $25 million milestone from the royalty deal.
- First regulatory approval in any indication.

**Tier 2 (meaningful):**
- COM701 Phase 1 data in ovarian cancer (Compugen-owned asset). Positive data here could lead to a partnership deal at much better terms than the original AstraZeneca deal.
- Gilead advancing GS-0321 to Phase 2/3 trials and hitting development milestones.

**Tier 3 (long-term):**
- Commercial launch of rilvegostomig and first royalty payments to Compugen.
- Additional indications beyond NSCLC expanding the royalty base.

**What happens if the thesis breaks:** If rilvegostomig fails in Phase 3, the stock likely drops 50-70%. The $145 million cash pile provides a floor, but the market would reprice the royalty optionality to near zero. This is the binary biotech risk that no amount of financial analysis can eliminate.

**Important distinction:** Unlike many binary biotechs, CGEN's downside is partially floored by its cash position ($1.54 per share in cash alone) and its proprietary pipeline. A rilvegostomig failure would be devastating but not necessarily terminal — the COM701 and COM902 programs represent independent value.

---

## Step 7: What Is the Market Pricing In?

At $2.90 per share and a $274 million market cap:

- **Net of cash:** $274M - $145.6M = **$128.4 million** for the entire business
- **What that $128 million buys you:** Royalties on a drug with stated $5B+ peak revenue potential across 10 Phase 3 trials, a Gilead partnership, and two wholly-owned clinical-stage assets.

The market is essentially assigning less than $130 million in value to:
1. Mid-single-digit royalties on a potential $5B+ revenue drug (10 Phase 3 trials, AstraZeneca backing)
2. Up to $195 million in remaining milestones from AstraZeneca alone
3. The Gilead partnership (undisclosed milestones)
4. Two proprietary Phase 1 assets (COM701, COM902)

**Risk-adjusted math:** If you assign even a 30% probability to rilvegostomig reaching $3 billion in peak sales (conservative vs AstraZeneca's $5B+ estimate), at 4% royalties, that's $120 million/year in royalties × 30% probability = $36 million expected annual royalties. At a 10x multiple on royalty streams, that's $360 million in present value — for the AstraZeneca asset alone. Against an enterprise value (ex-cash) of $128 million.

### Implied Market Cap If Phase 3 Succeeds

What does Compugen look like if rilvegostomig clears Phase 3 and reaches commercialization? Here's the math by income source, using a 10x multiple on annual royalty/income streams (standard for biotech royalty assets):

| Income Source | Annual Value | Multiple | Implied Value | Notes |
|---|---|---|---|---|
| Rilvegostomig royalties (4-5% on $3B conservative peak sales) | $120-150M | 10x | $1.2-1.5B | AstraZeneca estimates $5B+ peak; we use $3B |
| Remaining AstraZeneca milestones | — | 1x (lump sum) | ~$130M | Regulatory + commercial milestones still outstanding |
| Gilead milestones + royalties | — | 1x | ~$50-100M | Earlier stage, less visibility |
| COM701/COM902 (proprietary, Phase 1) | — | — | $100-200M | Optionality value if trials succeed or assets are licensed |
| Net cash (current) | — | 1x | $146M | Floor value |
| **Total implied market cap** | | | **$1.6-2.1B** | **vs. $274M today (5.8-7.7x upside)** |

At 94.5 million shares outstanding, that's an implied share price of **$17-22** in a Phase 3 success scenario — roughly 6-7x the current $2.90.

For context, this is not a fantasy number. Royalty Pharma (RPRX) — a company whose entire business is owning royalty streams on approved drugs — trades at approximately 12-15x annual royalty income. We used a more conservative 10x.

**Why the market might be right to be skeptical:**
- Phase 3 failure rate in oncology is approximately 40-50%
- Rilvegostomig hasn't won yet — it's competing head-to-head against the most successful cancer drug in history (Keytruda)
- Royalty streams are inherently discounted because Compugen doesn't control the commercialization timeline or strategy
- The company's $72.8M revenue year was a one-time structural event, not evidence of commercial traction
- Primary Phase 3 completion dates are 2027-2029 — this is a multi-year wait with no guarantee

---

## Our Assessment

Compugen sits in a rare category: a micro cap biotech with the balance sheet of a survivor, the partnership validation of a serious platform, and a market price that appears to heavily discount the most likely outcome of its lead partnership.

The automated screener scored CGEN 8/12 — but that score is wrong. The patent lookup failed on a foreign-domiciled assignee name, missing a portfolio of granted composition-of-matter and method-of-use patents that survived GSK opposition proceedings. With the corrected defensibility assessment, the manual score is 10-11/12.

The risk here is binary and non-diversifiable: if rilvegostomig fails Phase 3, the stock loses half its value. If it succeeds, the royalty stream alone justifies a stock price multiples above today's level. The cash position ($1.54/share on a $2.90 stock) provides a partial floor that most binary biotechs don't have.

**Position sizing guidance:** This is a 1-3% portfolio position, not a 5%. The downside is real, the binary risk is unhedgeable, and the timeline is 2-3 years to primary catalysts. But the risk/reward at current prices — with enterprise value below $130 million against potential royalties worth $300-500 million risk-adjusted — is the kind of asymmetry that micro cap investing exists to capture.

### What We're Watching

1. **Q1 2026 results** (May 18, 2026 — two days from now). Cash burn rate and updated runway guidance will confirm whether the 2029 timeline holds.

2. **AstraZeneca ASCO 2026 presentations** (June). Any updated Phase 2/3 data on rilvegostomig, particularly in the head-to-head vs. pembrolizumab cohorts.

3. **COM701 Phase 1 ovarian cancer data**. If the proprietary pipeline shows activity, it reduces dependence on the AstraZeneca partnership for the entire bull case.

4. **Share count stability**. The moment Compugen files an ATM or shelf registration, the "no dilution" thesis is in danger. Watch SEC filings closely.

---

## A Different Ballgame

This is why the micro cap screener exists as a separate tool from the 200WMA framework. The signals that matter here — partnership validation, binary clinical catalysts, cash-as-percentage-of-market-cap, dilution risk — have no parallel in the large cap world. A stock like CGEN would never show up in our 200WMA crossing data because it doesn't have the trading history, the index membership, or the market cap to qualify.

But the underlying question is the same one we ask everywhere on this site: is the market pricing in reality, or pricing in a narrative? For Compugen at $274 million enterprise value, with AstraZeneca betting billions on a drug Compugen discovered, the narrative seems to be "biotech = risky = avoid." The reality appears to be more nuanced.

The screener finds the candidates. The 7-step framework tells you whether they deserve your capital. CGEN passes the framework and goes on our watchlist.

More micro cap deep dives to follow. The screener is live at [/microcaps/](/microcaps/).

---

*Disclaimer: This analysis is for informational purposes only and does not constitute investment advice. Micro cap stocks carry significant risk including potential total loss of investment. The author may hold positions in the securities discussed. Always conduct your own research and consult a financial advisor before making investment decisions.*
