# AI Exposure Ranking — Methodology (Draft v0.1)

> **Status**: Draft for review. Nothing here is implemented yet.
> **Drafted**: 2026-11-20
> **Scope**: Two new sort/screen dimensions for the stocks list. The first is **AI Bust Exposure**: who gets hurt if AI spending doesn't earn its cost of capital. The second is **AI Toll Gate**: who earns rent from AI with little capital at risk.

---

## 1. What we are measuring (and what we are not)

This is **not** a measure of "uses AI" or "talks about AI on earnings calls." It measures the **balance-sheet and contract consequences** of the AI build-out:

| Score | Question it answers | High score means |
|---|---|---|
| **AI Bust Exposure** (0–100) | If AI revenue fails to cover the capex, debt and commitments made on its behalf, how badly is this company hurt? | Large share of value, earnings or solvency depends on the AI spending cycle continuing |
| **AI Toll Gate** (0–100) | Does this company own data or rights that AI systems need, and can it monetize them without spending heavily? | Proprietary, fresh, authoritative data and a low-capex, high-margin model that AI efficiencies widen |

The two scores are **independent, not opposites**. A company can be high on both (e.g. GOOG has a toll-gate data asset *and* a very large capex program). Section 6 combines them into a quadrant.

---

## 2. AI Bust Exposure — the five channels

The bust hits through five channels. Each one is a pillar with its own sub-score.

### Pillar A — Depreciation Bomb (weight 25%)

Capex spent on GPUs, servers and data centers today becomes depreciation tomorrow. When capex is growing quickly, current D&A reflects older and smaller vintages, so **reported earnings overstate steady-state earnings**. If AI revenue doesn't grow into the D&A wave, margins compress mechanically.

| Metric | Formula | Source | Direction |
|---|---|---|---|
| `capex_to_da` | Capex_TTM / D&A_TTM | yfinance cashflow | Higher = more exposure |
| `depreciation_overhang` | (Capex_TTM − D&A_TTM) / EBIT_TTM | yfinance cashflow + income | Higher = more exposure |
| `capex_intensity` | Capex_TTM / Revenue_TTM | yfinance | Higher = more exposure |
| `capex_growth_vs_rev` | 3-yr capex CAGR − 3-yr revenue CAGR (pp) | yfinance annual | Higher = more exposure |

**The headline metric is `depreciation_overhang`.** Under straight-line depreciation, flat capex held for one full useful life brings D&A up to equal capex. So `(Capex − D&A) / EBIT` is the share of today's operating income that disappears once D&A catches up, if revenue doesn't grow. It needs no guess about useful life.

**Caveats:**
- It overstates the hit for companies whose capex is mostly long-lived buildings, land and power infrastructure (30+ year lives). Optional refinement: where the 10-K splits property and equipment by class, apply the overhang only to short-lived equipment (servers and network gear, 5–6 years).
- Useful-life accounting changes go in the overlay (§4) as a flag. Several hyperscalers lengthened server useful lives in 2022–2024, and at least one shortened them again. A lengthened life inflates current EPS. Flag `useful_life_extended = true` and record the disclosed EPS impact where available.
- Negative or near-zero EBIT makes the ratio unstable. Cap at the 95th percentile, and if EBIT ≤ 0 assign the pillar's maximum percentile when capex_to_da > 1.5.

### Pillar B — Leverage & Funding (weight 25%)

The build-out is increasingly funded with debt: corporate bonds, private credit, asset-backed and GPU-collateralized loans, and **off-balance-sheet** structures (joint-venture SPVs, long-dated leases signed but not yet commenced, residual-value guarantees). If AI cash flows disappoint, these obligations remain.

| Metric | Formula | Source | Direction |
|---|---|---|---|
| `capex_funding_gap` | max(0, Capex − OCF) / Capex | yfinance | Higher = more exposure |
| `net_debt_to_ebitda` | (Total debt − cash) / EBITDA | yfinance | Higher = more exposure |
| `debt_growth_yoy` | ΔTotal debt / prior total debt | yfinance balance sheet | Higher = more exposure |
| `interest_coverage_inv` | Interest expense / EBIT | yfinance income | Higher = more exposure |
| `off_bs_obligations_to_mcap` | (Leases not yet commenced + SPV guarantees + non-cancellable AI purchase commitments) / market cap | **Overlay** (10-K/10-Q footnotes) | Higher = more exposure |

`capex_funding_gap` is the cleanest signal from yfinance. It separates a company paying for AI from operating cash flow (bust = lower returns) from one borrowing to pay for it (bust = solvency question).

### Pillar C — Counterparty & Circularity, the "handshake deals" (weight 20%)

Much of the AI backlog is concentrated in a few **pre-profit AI labs** and in **circular arrangements**, where the vendor finances the customer that buys from the vendor. These risks don't appear in standard financial fields. This pillar is **overlay-only**.

Scored 0–10 from a rubric (each item 0–2):

| Item | 0 | 1 | 2 |
|---|---|---|---|
| **C1. Backlog concentration** | <10% of RPO/backlog from pre-profit AI counterparties | 10–40% | >40%, or the single largest counterparty is a pre-profit lab |
| **C2. Vendor financing** | None | Minority equity stake in a customer, or customer credit lines | Large equity investment in a customer that buys the company's product |
| **C3. Equity-for-orders** | None | Warrants or equity granted to a customer in exchange for volume commitments (disclosed) | Material dilution tied to customer purchase volumes |
| **C4. Take-or-pay as payer** | None | The company has committed to multi-year capacity leases (compute, power, data center) | Those commitments exceed 1× annual revenue |
| **C5. Collateral fragility** | None | Debt secured by data center assets | Debt secured by GPUs or other fast-depreciating hardware |

Two kinds of relationship are excluded: ordinary commercial agreements with profitable hyperscalers, and contracts with investment-grade counterparties. Neither is a "handshake." We care about obligations whose payer depends on continued fundraising.

### Pillar D — Revenue Dependence (weight 15%)

The share of revenue that disappears or reprices if AI data center spending falls 30–50%.

| Metric | Source |
|---|---|
| `ai_revenue_share` | **Overlay**: segment disclosures (data center segment, AI-related backlog, data-center-driven load for utilities). Record 0–100% with source and date. |
| `ai_tier` | **Overlay**: tier taxonomy below |

**Exposure tiers** (for labeling and the sector filter):

| Tier | Who | Examples to research (not conclusions) |
|---|---|---|
| **1 — Direct** | Neoclouds, AI accelerators, servers, AI networking and optics, AI-dedicated data center developers | GPU-cloud lessors, accelerator and memory makers, server integrators, optical interconnect |
| **2 — Funders** | Hyperscaler capex programs, alternative asset managers and private-credit lenders to data centers, banks arranging data center debt | Hyperscalers, alt managers with digital-infrastructure credit books |
| **3 — Picks & shovels** | Power generation contracted to data centers, utilities with large load-growth plans, electrical equipment, cooling, specialty construction, data center REITs | IPPs and nuclear operators, grid equipment, thermal management, electrical contractors |
| **4 — Narrative** | Companies whose valuation premium rests on an AI story without the revenue yet | Detected by Pillar E rather than assigned by hand |

### Pillar E — Market-Implied AI Beta (weight 15%)

This pillar lets the market tell us who trades as an AI asset, including names we didn't think to curate. It is computed from the weekly price data the pipeline already fetches.

```
AI_factor_t = EW_return(AI basket)_t − SPY_return_t
r_i,t − rf  = α + β_mkt · (SPY_t − rf) + β_AI · AI_factor_t + ε
```

- **Window**: 104 weeks, rolled weekly.
- **AI basket**: an equal-weight basket of ~15 Tier-1 names, rebalanced quarterly. Exclude stock *i* from the basket when estimating its own β_AI, to avoid self-correlation. Alternative: use SMH − SPY as the factor; it is simpler but semiconductor-biased.
- **Output**: `ai_beta` (β_AI) and its t-stat. Use β_AI only when |t| ≥ 2; otherwise set it to 0 for scoring.
- **Valuation kicker**: multiply the pillar percentile by 1.0–1.2 according to how far the stock is *above* its 200WMA, using the existing `pct_from_wma`. A name 60% above its 200WMA with a high β_AI has more AI hope priced in, and therefore more to lose. This ties the score to the site's core 200WMA signal.

**Fallback**: when the overlay is missing for a ticker, Pillar E's weight absorbs Pillar D's weight. The market-implied signal stands in for curated revenue dependence, and confidence (§5) is reduced.

### Sector handling

- **Financials** (banks, insurers, alternative asset managers): Pillars A and B are meaningless on their reported financials. Score them on Pillar C (credit exposure to data center or AI borrowers, from the overlay), Pillar D (share of fee-related earnings from digital infrastructure) and Pillar E only.
- **REITs**: use AFFO rather than EBIT in Pillar A, and include development pipeline commitments in Pillar B's off-balance-sheet line.
- **Utilities**: Pillar A is structurally high because they are capital-intensive by design. Compare utilities **within sector** (percentile among utilities) so that only the AI-driven increment stands out. The same applies to Pillar B.

---

## 3. AI Toll Gate — who collects rent from AI

The thesis: models are commoditizing, and inference is getting cheaper. Scarce inputs are **proprietary, current and authoritative data**, plus the legal right to use it. Companies that own those inputs and run capital-light models gain on both sides. They can license data to AI developers and buyers, and their own cost base shrinks as AI automates internal work.

Seed names from the brief: **GENI** (official league data rights), **GOOG** (search index, YouTube, Maps), **SPGI** (ratings, indices, commodity benchmarks), **PL** (daily satellite imagery archive).

### Pillar T1 — Data Moat (weight 40%, overlay)

Scored 0–10 (each item 0–2):

| Item | What earns a 2 |
|---|---|
| **T1a. Exclusivity** | Owned outright or under an exclusive license (league data rights, credit files, proprietary index methodologies, satellite constellations). Cannot be scraped from the public web. |
| **T1b. Freshness** | Value decays fast: live odds, prices, imagery, transactions. A one-time training scrape cannot replace a continuous feed. |
| **T1c. Authority / liability** | Regulation or contracts require *this* source (NRSRO ratings, benchmarks referenced in contracts, scores mandated in underwriting). An AI answer must cite it rather than approximate it. |
| **T1d. AI monetization evidence** | Disclosed data-licensing deals with AI developers, an API or feed with growing usage, or AI products sold on top of the data with disclosed revenue. |
| **T1e. Workflow embeddedness** | Data is delivered inside customer workflows (terminals, underwriting systems, betting engines), so switching is costly beyond the data itself. |

### Pillar T2 — Capital Lightness (weight 20%)

| Metric | Formula | Direction |
|---|---|---|
| `capex_intensity` | Capex / Revenue | **Lower** = better |
| `gross_margin` | existing field | Higher = better |
| `depreciation_overhang` | same as Pillar A | **Lower** = better |

This pillar keeps the toll gate honest: a data owner that must build its own GPU fleet to monetize that data is partly a bust-exposure name.

### Pillar T3 — Efficiency Capture (weight 20%)

Is AI already widening margins?

| Metric | Formula | Direction |
|---|---|---|
| `op_margin_delta_3y` | Operating margin (latest FY) − operating margin (FY−3), pp | Higher = better |
| `sga_ratio_delta_3y` | SG&A/Revenue (FY−3) − SG&A/Revenue (latest), pp | Higher = better (opex falling relative to revenue) |
| `revenue_per_employee_growth` | YoY Δ(Revenue / full-time employees) | Higher = better. Employee history needs the overlay or SEC data; yfinance only gives the current count. |

### Pillar T4 — Cash Quality (weight 20%)

| Metric | Source |
|---|---|
| `fcf_margin` | FCF / Revenue (existing data) |
| `fcf_consecutive_positive` | existing field |
| `net_cash_to_mcap` | yfinance balance sheet |

### Disintermediation Penalty (up to −20 points, overlay)

Some data businesses are **eaten** by AI rather than fed by it. Subtract points from the final Toll Gate score:

| Condition | Penalty |
|---|---|
| Product is answers or content an LLM can reproduce from a one-time training scrape (Q&A, tutoring, generic research summaries, stock imagery, boilerplate code help) | −10 |
| Revenue depends on referral traffic that AI answer engines intercept (ad-supported content sites, comparison-shopping lead generation) | −5 |
| Data is public-domain or government-published and merely repackaged | −5 |

---

## 4. The Overlay File

The quantitative pillars come from yfinance (with small additions to `fetch_fundamental_data`: capex, D&A, EBIT, interest expense, cash, OCF). Everything else lives in a hand-curated file:

**Proposed path**: `below-the-line/data/ai_overlay.yml`

```yaml
# One entry per ticker. Every numeric claim carries a source + as-of date.
ORCL:
  ai_tier: 2
  ai_revenue_share: null          # unknown -> falls back to Pillar E
  counterparty:                   # Pillar C rubric, 0-2 each
    c1_backlog_concentration: 2
    c2_vendor_financing: 0
    c3_equity_for_orders: 0
    c4_take_or_pay_payer: 2
    c5_collateral_fragility: 1
  off_bs_obligations_usd: null
  useful_life_extended: false
  sources:
    - {claim: "c1", doc: "10-Q FQ1 2026, RPO footnote", url: "...", asof: "2025-09-09"}
  reviewed: 2026-11-20
  reviewer: drew

GENI:
  toll_gate:
    t1a_exclusivity: 2
    t1b_freshness: 2
    t1c_authority: 1
    t1d_ai_monetization: 1
    t1e_embeddedness: 2
  disintermediation_penalty: 0
  sources: [...]
  reviewed: 2026-11-20
```

> The values above illustrate the schema only. **None of them are verified ratings.** Each must be checked against filings before use.

**Rules**
- Every rubric score needs at least one source line (a filing section, earnings call, or press release with URL and date). If there's no source, the score is null rather than a guess.
- An entry older than 180 days is marked `stale`, and its overlay pillars lose 50% weight until re-reviewed.
- **Curation scope for v1**: about 150 names. Take the top 100 by `ai_beta` plus the top 50 by `capex_to_da`, plus any toll-gate candidates we nominate. Everything else is scored on quantitative pillars only, with a lower confidence label.

**Optional Tier-2 data source**: the SEC XBRL `companyfacts` API is free and structured. It exposes `RevenueRemainingPerformanceObligation`, gross PP&E and accumulated depreciation, operating lease liabilities, and (for some filers) unrecognized purchase commitments. That would automate part of Pillars B and C. SEC requires a User-Agent with contact information on API requests; decide what to use there before building it.

---

## 5. Scoring Mechanics

Same conventions as `dislocation_scores.py`: cross-sectional, robust to outliers, and honest about missing data.

1. **Winsorize** each raw metric at the 5th and 95th percentiles of the universe.
2. **Percentile-rank** each metric among stocks with non-null values (0–100). For sector-relative pillars (utilities, REITs), rank within sector.
3. **Pillar score** = mean of its metrics' percentiles. For overlay rubrics: (rubric total / max) × 100.
4. **Composite** = weighted mean of available pillars, **with weights renormalized** over the pillars present.
5. **Confidence** = share of the nominal weight that was actually available:
   - ≥ 85% → `high`
   - 60–85% → `medium`
   - < 60% → no published score (`insufficient_data`)
6. **Toll Gate** = weighted composite − disintermediation penalty, floored at 0.
7. **Gate**: the label "Toll Gate" (badge and filter) requires T1 Data Moat ≥ 6/10. A capital-light, high-margin company without a data asset is a good business, not a toll gate.

### Output fields (per stock, added to `stocks.json`)

```
ai_bust_score: float|null          # 0-100
ai_bust_pillars: {A, B, C, D, E}   # each 0-100 or null
ai_bust_confidence: str            # high | medium | insufficient_data
ai_toll_score: float|null          # 0-100
ai_toll_pillars: {T1, T2, T3, T4, penalty}
ai_toll_confidence: str
ai_quadrant: str                   # see §6
ai_tier: int|null                  # 1-4 from overlay
ai_beta: float|null
ai_beta_t: float|null
depreciation_overhang: float|null  # surfaced on its own; it's the most legible single number
capex_funding_gap: float|null
ai_overlay_reviewed: str|null      # ISO date
```

---

## 6. The Quadrant

|  | **Low Bust Exposure (<50)** | **High Bust Exposure (≥50)** |
|---|---|---|
| **High Toll Gate (≥60, moat gate passed)** | **Toll Collector**: earns AI rent without underwriting the build-out | **Levered Landlord**: owns the data *and* is paying for the infrastructure. GOOG-type names likely land here. |
| **Low Toll Gate** | **Bystander**: not materially tied to AI either way | **Bag Holder**: carries the capex, debt or counterparty risk without a proprietary data asset |

The quadrant is the primary label on the stocks list. Both raw scores are sortable columns.

---

## 7. Validation Plan

The AI cycle is too young for a conventional backtest, so validation is in three parts:

1. **Event study (natural experiment).** Compute scores as they would have been one week before known AI-scare selloffs. The cleanest is the 27 Jan 2025 DeepSeek selloff. Add later AI-financing scares; dates to be confirmed from the record. Check that high-bust deciles fell more than low-bust deciles, and that Toll Collectors held up. If the spread isn't there, the weights are wrong.
2. **Sanity review.** Before publishing, hand-review the top and bottom 25 of each score. Any name that looks wrong is traced to the pillar responsible.
3. **Stability.** Week-over-week rank correlation should be > 0.9 outside earnings weeks. Big jumps should trace to a new filing or overlay change, not noise.

---

## 8. Integration Plan (for when this moves past draft)

| Step | File | Notes |
|---|---|---|
| 1 | `scripts/update_stocks.py` → `fetch_fundamental_data` | Add capex, D&A, EBIT, interest expense, cash, OCF (TTM and 3 annual points) |
| 2 | `scripts/ai_exposure.py` (new) | Mirrors `dislocation_scores.py`: per-stock raw metrics, then a universe post-pass for percentiles, composites and quadrant |
| 3 | `data/ai_overlay.yml` (new) | Curated rubric scores with sources |
| 4 | `layouts/stocks/list.html` | Two sortable columns ("AI Bust", "Toll Gate") as `data-*` attributes; Special filter chips: *Toll Collectors*, *Bag Holders*, *Depreciation Bomb* (overhang ≥ 30%), *Debt-Funded Capex* (funding gap > 0) |
| 5 | `layouts/stocks/single.html` | Pillar breakdown bar plus overlay sources, so every score is auditable |
| 6 | `content/methodology/ai-exposure.md` | Public version of this doc in the site voice (like `micro-cap-innovation.md`) |
| 7 | `CLAUDE.md` | Document the new fields and script |

---

## 9. Known Limitations

- **yfinance gives roughly four annual statement points.** 3-year deltas are coarse, so present them as pp deltas, not trends. This is the same caveat as `dislocation_scores.py`.
- **The overlay is manual and goes stale.** Pillars C, D and T1 are only as good as the last review. The confidence label and 180-day staleness rule exist to stop stale judgments from passing as data.
- **Depreciation overhang assumes flat revenue.** It is a stress measure, not a forecast. A company whose AI revenue does grow into its D&A will look worse on this metric than it turns out to be. That is intended, because the score measures exposure *if* AI doesn't pay.
- **AI beta is regime-dependent.** It measures how the stock *has* traded, so names can enter or leave the AI trade faster than a 104-week window captures.
- **Private exposure is invisible.** Much of the circularity sits in private labs, SPVs and private credit funds. We see only the public end of each contract.
