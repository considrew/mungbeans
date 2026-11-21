# AI Exposure Ranking — Methodology (Draft v0.2)

> **Status**: Draft. Nothing here is implemented yet.
> **Drafted**: 2026-11-20 · **Revised**: 2026-11-21 (v0.2, incorporating the PR #8 review; see changelog at the end)
> **Scope**: Two new screens. The first is **AI Bust Exposure**: who gets hurt if AI spending doesn't earn its cost of capital. The second is **AI Toll Gate**: who earns rent from AI with little capital at risk. Both live in a dedicated `/ai-exposure/` hub, with a light bridge into `/stocks/`.

---

## 1. What we are measuring (and what we are not)

This is **not** a measure of "uses AI" or "talks about AI on earnings calls." It measures the **balance-sheet and contract consequences** of the AI build-out:

| Score | Question it answers | High score means |
|---|---|---|
| **AI Bust Exposure** (0–100) | If AI revenue fails to cover the capex, debt and commitments made on its behalf, how badly is this company hurt? | Large share of value, earnings or solvency depends on the AI spending cycle continuing |
| **AI Toll Gate** (0–100) | Does this company own data or rights that AI systems need, and can it monetize them without spending heavily? | Proprietary, fresh, authoritative data and a low-capex, high-margin model that AI efficiencies widen |

The two scores are **independent, not opposites**. A company can be high on both (e.g. GOOG has a toll-gate data asset *and* a very large capex program). Section 6 combines them into a quadrant.

**Both scores are gated** (§5). Bust Exposure is published only for **AI-relevant** stocks, and Toll Gate only for **curated** stocks. Without the gate, airlines, drillers, miners, telecoms and shipping companies would rank as AI "Bag Holders" just because they are capital-intensive.

---

## 2. AI Bust Exposure — the five channels

The bust hits through five channels. Each one is a pillar with its own sub-score.

**Data period**: v1 uses **annual statements** (latest fiscal year, "FY") from `ticker.financials`, `ticker.balance_sheet` and `ticker.cashflow`. `get_health_metrics()` in `scripts/update_stocks.py` already fetches all three, so this adds no Yahoo requests. TTM from quarterly statements would add roughly 4,000 requests per weekly run and is deferred.

**Sign convention**: yfinance reports `Capital Expenditure` as a **negative** number (see `bean_score.py`). Every formula below uses `Capex = abs(Capital Expenditure)`.

### Pillar A — Depreciation Bomb (weight 25%)

Capex spent on GPUs, servers and data centers today becomes depreciation tomorrow. When capex is growing quickly, current D&A reflects older and smaller vintages, so **reported earnings overstate steady-state earnings**. If AI revenue doesn't grow into the D&A wave, margins compress mechanically.

| Metric | Formula | Source | Direction |
|---|---|---|---|
| `capex_to_da` | Capex_FY / D&A_FY | yfinance annual cashflow | Higher = more exposure |
| `depreciation_overhang` | (Capex_FY − D&A_FY) / EBITDA_FY | yfinance annual cashflow + income | Higher = more exposure |
| `capex_intensity` | Capex_FY / Revenue_FY | yfinance | Higher = more exposure |
| `capex_growth_vs_rev` | 3-yr capex CAGR − 3-yr revenue CAGR (pp) | yfinance annual | Higher = more exposure |

**The headline metric is `depreciation_overhang`.** Under straight-line depreciation, flat capex held for one full useful life brings D&A up to equal capex. So `(Capex − D&A) / EBITDA` is the share of today's cash operating earnings that will be charged as depreciation once D&A catches up, if revenue doesn't grow. It needs no guess about useful life.

**Why EBITDA rather than EBIT**: EBIT shrinks toward zero for thin-margin businesses. With EBIT as the denominator, a 2%-margin distributor would look more exposed than a hyperscaler spending $80B. EBITDA stays finite and comparable. The EBIT version is kept as a secondary detail field, `depreciation_overhang_ebit`, for the audit drawer only.

**Caveats:**
- It overstates the hit for companies whose capex is mostly long-lived buildings, land and power infrastructure (30+ year lives). Optional refinement: where the 10-K splits property and equipment by class, apply the overhang only to short-lived equipment (servers and network gear, 5–6 years).
- Useful-life accounting changes go in the overlay (§4) as a flag. Several hyperscalers lengthened server useful lives in 2022–2024, and at least one shortened them again. A lengthened life inflates current EPS. Flag `useful_life_extended = true` and record the disclosed EPS impact where available.
- If EBITDA ≤ 0 and capex_to_da > 1.5, assign the pillar's maximum percentile. If EBITDA ≤ 0 otherwise, the metric is null.

### Pillar B — Leverage & Funding (weight 25%)

The build-out is increasingly funded externally: corporate bonds, private credit, asset-backed and GPU-collateralized loans, **convertibles and equity issuance**, and **off-balance-sheet** structures (joint-venture SPVs, long-dated leases signed but not yet commenced, residual-value guarantees). If AI cash flows disappoint, these obligations remain.

| Metric | Formula | Source | Direction |
|---|---|---|---|
| `capex_funding_gap` | max(0, Capex − OCF) / Capex | yfinance | Higher = more exposure |
| `net_debt_to_ebitda` | (Total debt − cash) / EBITDA | yfinance | Higher = more exposure |
| `debt_growth_yoy` | ΔTotal debt / prior total debt | yfinance balance sheet | Higher = more exposure |
| `interest_coverage_inv` | Interest expense / EBIT | yfinance income | Higher = more exposure |
| `shares_change_3yr` | existing `stocks.json` field | already computed | Higher (more dilution) = more exposure |
| `off_bs_obligations_to_mcap` | (Leases not yet commenced + SPV guarantees + non-cancellable AI purchase commitments) / market cap | **Overlay** (10-K/10-Q footnotes) | Higher = more exposure |

`capex_funding_gap` is the cleanest signal from yfinance. It separates a company paying for AI from operating cash flow (bust = lower returns) from one that needs outside money to pay for it (bust = solvency question). Note that `capex_funding_gap` equals `max(0, −FCF) / Capex`, so it misses **borrowing that funds dividends and buybacks while capex is covered**. `debt_growth_yoy` is the metric that catches that case. `shares_change_3yr` catches funding by convertibles and equity, which is common among neoclouds and Tier-1 build-out names. It is already in `stocks.json`.

### Pillar C — Counterparty & Circularity, the "handshake deals" (weight 20%)

Much of the AI backlog is concentrated in a few **pre-profit AI labs** and in **circular arrangements**, where the vendor finances the customer that buys from the vendor. These risks don't appear in standard financial fields. This pillar is **overlay-only**.

Scored 0–10 from a rubric (each item 0–2):

| Item | 0 | 1 | 2 |
|---|---|---|---|
| **C1. Pre-profit backlog concentration** | <10% of RPO/backlog from pre-profit AI counterparties | 10–40% | >40%, or the single largest counterparty is a pre-profit lab |
| **C2. Vendor financing** | None | Minority equity stake in a customer, or customer credit lines | Large equity investment in a customer that buys the company's product |
| **C3. Equity-for-orders** | None | Warrants or equity granted to a customer in exchange for volume commitments (disclosed) | Material dilution tied to customer purchase volumes |
| **C4. Take-or-pay as payer** | None | The company has committed to multi-year capacity leases (compute, power, data center) | Those commitments exceed 1× annual revenue |
| **C5. Collateral fragility** | None | Debt secured by data center assets | Debt secured by GPUs or other fast-depreciating hardware |

**C1 counts only pre-profit counterparties.** A supplier whose revenue is concentrated in a few *profitable* hyperscalers is still exposed if those customers cut capex, but that is a demand risk, not a handshake solvency risk. It belongs in **Pillar D** (revenue dependence), not in C1. Ordinary commercial contracts with investment-grade counterparties are excluded from this pillar entirely.

### Pillar D — Revenue Dependence (weight 15%)

The share of revenue that disappears or reprices if AI data center spending falls 30–50%. This includes concentration in profitable hyperscaler customers (see the C1 note).

| Metric | Source |
|---|---|
| `ai_revenue_share` | **Overlay**: segment disclosures (data center segment, AI-related backlog, data-center-driven load for utilities). Record 0–100% with source and date. |
| `ai_tier` | **Overlay**: tier taxonomy below |

**Exposure tiers** (for labeling and the relevance gate):

| Tier | Who | Examples to research (not conclusions) |
|---|---|---|
| **1 — Direct** | Neoclouds, AI accelerators, servers, AI networking and optics, AI-dedicated data center developers | GPU-cloud lessors, accelerator and memory makers, server integrators, optical interconnect |
| **2 — Funders** | Hyperscaler capex programs, alternative asset managers and private-credit lenders to data centers, banks arranging data center debt | Hyperscalers, alt managers with digital-infrastructure credit books |
| **3 — Picks & shovels** | Power generation contracted to data centers, utilities with large load-growth plans, electrical equipment, cooling, specialty construction, data center REITs | IPPs and nuclear operators, grid equipment, thermal management, electrical contractors |
| **4 — Narrative** | Companies whose valuation premium rests on an AI story without the revenue yet | Detected by Pillar E rather than assigned by hand |

### Pillar E — Market-Implied AI Beta (weight 15%)

This pillar lets the market tell us who trades as an AI asset, including names we didn't think to curate. It is computed from the weekly price data the pipeline already fetches.

```
AI_raw_t   = EW_return(AI basket excluding i)_t
r_i,t − rf = α + β_mkt · (SPY_t − rf) + β_AI · AI_raw_t + ε
β_AI_shrunk = β_AI · t² / (t² + 4)          # t = t-stat of β_AI
```

- **Window**: 104 weeks, rolled weekly.
- **AI basket**: an equal-weight basket of ~15 Tier-1 names, rebalanced quarterly, **excluding stock *i*** when estimating its own β_AI.
- **On orthogonalizing against SPY**: because SPY is already in the regression, β_AI is already the stock's sensitivity to the part of AI returns that SPY doesn't explain (Frisch–Waugh–Lovell). Replacing the AI regressor with its SPY-orthogonalized residual leaves β_AI and its t-stat **unchanged** in-sample; only β_mkt changes. We may still store the orthogonalized factor so that β_mkt reads as total market beta. Either way, collinearity between the AI basket and SPY is real; the 104-week window and the shrinkage handle it.
- **Shrinkage instead of a hard cut-off**: β_AI_shrunk weights β_AI by its statistical strength. At t = 2 it keeps 50%, at t = 4 it keeps 80%, and near t = 0 it goes to ~0. This avoids the jump a hard `|t| ≥ 2` cut-off would cause when t hovers around 2.0, which would break the §7 stability target.
- **Output**: `ai_beta` (raw), `ai_beta_t`, `ai_beta_shrunk`. **The pillar is scored on `ai_beta_shrunk`.**
- **No valuation multiplier.** v0.1 scaled this pillar by `pct_from_wma`. That is dropped, for two reasons. It could push percentiles above 100. And it mixed structural exposure with current valuation, which the site's 200WMA signals already show next to the score.

**Fallback**: when the overlay is missing for a gated ticker, Pillar E's weight absorbs Pillar D's weight. The market-implied signal stands in for curated revenue dependence, and confidence (§5) is reduced.

### Sector handling

- **Financials** (banks, insurers, alternative asset managers): Pillars A and B are meaningless on their reported financials. Score them on Pillar C (credit exposure to data center or AI borrowers, from the overlay), Pillar D (share of fee-related earnings from digital infrastructure) and Pillar E only.
- **REITs**: yfinance does not report AFFO. Approximate **FFO ≈ Net income + D&A** from annual statements and use it in place of EBITDA in Pillar A. Include development pipeline commitments in Pillar B's off-balance-sheet line.
- **Utilities**: Pillar A is structurally high because they are capital-intensive by design. Compare utilities **within sector** (percentile among utilities) so that only the AI-driven increment stands out. The same applies to Pillar B.

---

## 3. AI Toll Gate — who collects rent from AI

The thesis: models are commoditizing, and inference is getting cheaper. Scarce inputs are **proprietary, current and authoritative data**, plus the legal right to use it. Companies that own those inputs and run capital-light models gain on both sides. They can license data to AI developers and buyers, and their own cost base shrinks as AI automates internal work.

Seed names from the brief: **GENI** (official league data rights), **GOOG** (search index, YouTube, Maps), **SPGI** (ratings, indices, commodity benchmarks), **PL** (daily satellite imagery archive).

**Toll Gate is published only for curated names**, those with a T1 entry in `ai_overlay.yml`. All other stocks show "Not yet curated." Without T1, the remaining quantitative weight lands right on the 60% confidence cliff, and every non-curated stock would fail the T1 badge requirement anyway. Quantitative-only Toll Gate scores for ~1,850 stocks would be noise.

### Pillar T1 — Data Moat (weight 45%, overlay)

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

### Pillar T3 — Efficiency Capture (weight 10%)

Is AI already widening margins? The weight is low on purpose. Over 2023–2026, 3-year margin changes mostly reflect recovery from the inflation squeeze, not AI productivity. The weight can rise once the AI-specific signal is distinguishable.

| Metric | Formula | Direction |
|---|---|---|
| `op_margin_delta_3y` | Operating margin (latest FY) − operating margin (FY−3), pp | Higher = better |
| `sga_ratio_delta_3y` | SG&A/Revenue (FY−3) − SG&A/Revenue (latest), pp | Higher = better (opex falling relative to revenue) |

- **SG&A is often missing in yfinance.** When it is, T3 is scored on `op_margin_delta_3y` alone. That counts as T3 present for confidence purposes, so a missing SG&A line doesn't lower the confidence label.
- `revenue_per_employee_growth` is **dropped from v1**. yfinance only exposes the current `fullTimeEmployees`, with no history. A possible v2 metric is the cross-sectional percentile of revenue/employee within sector.

### Pillar T4 — Cash Quality (weight 25%)

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

The quantitative pillars come from yfinance annual statements, extracted from DataFrames `get_health_metrics()` already loads (capex, D&A, EBIT, EBITDA, interest expense, cash, OCF, SG&A), plus `ticker.info` fields (`ebitda`, `totalDebt`, `totalCash`, `operatingCashflow`). Everything else lives in a hand-curated file:

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
    - {claim: "c1", doc: "10-Q, RPO footnote", url: "...", asof: "YYYY-MM-DD"}
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

**Phased curation scope**
- **v1 (launch)**: **35–40 curated names**, about 20 Tier 1–3 build-out names and about 15 Toll Collector candidates. Quantitative fields (`depreciation_overhang`, `ai_beta_shrunk`) are computed universe-wide from day one. Launch is not blocked on 150 manual reviews.
- **v2**: add SEC EDGAR XBRL `companyfacts` automation to scale to ~150 names. It exposes `RevenueRemainingPerformanceObligation`, gross PP&E and accumulated depreciation, operating lease liabilities and, for some filers, unrecognized purchase commitments. That automates part of Pillars B and C. SEC requires a User-Agent with contact information on API requests; decide what to use before building it.

---

## 5. Scoring Mechanics

Same conventions as `dislocation_scores.py`: cross-sectional, robust to outliers, and honest about missing data.

### Gates (applied before anything is published)

**AI-relevance gate (Bust Exposure).** A stock gets a published Bust score and can enter the *Bag Holder* or *Levered Landlord* quadrant only if either:
- (a) it is curated in `ai_overlay.yml` with `ai_tier ∈ {1, 2, 3}`, or
- (b) it has a statistically meaningful positive AI beta. It **enters** at `ai_beta_t ≥ 2.0` and `ai_beta_shrunk ≥ 0.25` and **leaves** only when `ai_beta_t < 1.5`. This hysteresis works like the 200WMA touch logic and stops names flickering in and out. The 0.25 threshold is to be calibrated on the first run.

All other stocks show `ai_quadrant = "unlinked"`. Their `depreciation_overhang` and `ai_beta` are still computed and stored, but no Bust score is shown.

**Curation gate (Toll Gate).** A Toll Gate score is published only when T1 exists in the overlay. The *Toll Gate* badge additionally requires T1 ≥ 6/10. A capital-light, high-margin company without a data asset is a good business, not a toll gate.

### Computation

1. **Winsorize** each raw metric at the 5th and 95th percentiles of the **full universe**.
2. **Percentile-rank** each quantitative metric against the **full universe** (0–100), among stocks with non-null values. The gate decides *who is shown*. The percentile says *how extreme* that stock is relative to the whole market, so a Bust score above 50 means above the market median and is not forced by the size of the gated set. Utilities and REITs are ranked within their sector.
3. **Pillar score** = mean of its metrics' percentiles. For overlay rubrics: (rubric total / max) × 100.
4. **Composite** = weighted mean of available pillars, **with weights renormalized** over the pillars present.
5. **Confidence** = share of the nominal weight that was actually available:
   - ≥ 85% → `high`
   - 60–85% → `medium`
   - < 60% → no published score (`insufficient_data`)
6. **Toll Gate** = weighted composite − disintermediation penalty, floored at 0.

### Output

The full payload is written to its own file, `below-the-line/assets/data/ai_exposure.json`, by `scripts/ai_exposure.py`. That script can re-score in seconds after an `ai_overlay.yml` edit without re-running the full pipeline.

```
# ai_exposure.json, per ticker
ai_bust_score, ai_bust_pillars {A,B,C,D,E}, ai_bust_confidence
ai_toll_score, ai_toll_pillars {T1,T2,T3,T4,penalty}, ai_toll_confidence
ai_quadrant            # toll_collector | levered_landlord | bystander | bag_holder | unlinked
ai_relevant, ai_gate_reason   # overlay_tier | ai_beta | none
ai_tier, ai_beta, ai_beta_t, ai_beta_shrunk
depreciation_overhang, depreciation_overhang_ebit, capex_to_da, capex_funding_gap
overlay: {sources[], reviewed, stale}
```

Only a **slim 5-field summary** is merged into `stocks.json`, which keeps it lean:

```
ai_quadrant, ai_bust_score, ai_toll_score, depreciation_overhang, ai_confidence
```

`ai_confidence` is the lower of the two published confidences.

---

## 6. The Quadrant

Only stocks that pass the gates (§5) are placed. Everything else is `unlinked`.

|  | **Low Bust Exposure (<50)** | **High Bust Exposure (≥50)** |
|---|---|---|
| **High Toll Gate (≥60, moat gate passed)** | **Toll Collector**: earns AI rent without underwriting the build-out | **Levered Landlord**: owns the data *and* is paying for the infrastructure. GOOG-type names likely land here. |
| **Low Toll Gate, or not curated** | **Bystander**: AI-linked, but not materially exposed | **Bag Holder**: carries the capex, debt or counterparty risk without a proprietary data asset |

---

## 7. Validation Plan

The AI cycle is too young for a conventional backtest. Validation therefore uses event studies, a sanity review and a stability check.

1. **Event studies.** Compute scores as they would have stood one week before each event. Measure sector-neutral return spreads (high-bust decile minus low-bust decile, within sector) to strip out the broad-market part of each move. Check that Toll Collectors held up relative to Bag Holders.

   | Window | Event | Notes |
   |---|---|---|
   | ~24 Jul – 5 Aug 2024 | AI capex-ROI scare after Q2 2024 hyperscaler capex guidance, compounded by the yen carry-trade unwind on 5 Aug | Heavy broad-market confound; rely on the sector-neutral spread |
   | 27 Jan 2025 | DeepSeek selloff | Cleanest single-day test for Tier-1 hardware and Tier-3 power/cooling. Predates most of the 2025–26 circular lab-financing deals, so it under-tests Pillar C. |
   | late Feb – Apr 2025 | Reports of data center lease pullbacks, then the tariff drawdown | Tariff confound from early April; split the window at 2 Apr |
   | 13 – 20 Nov 2026 | NVDA −22.9% week (close $162.85 → $125.53 per `stocks.json`, 2026-11-14 vs 2026-11-21 runs) | The most recent sample. The cause is not documented here; record it before using the event. |

   If the spreads aren't there, the weights are wrong.
2. **Sanity review.** Before publishing, hand-review the top and bottom 25 of each score. Any name that looks wrong is traced to the pillar responsible. In particular, check that no non-AI capital-intensive name passed the relevance gate.
3. **Stability.** Week-over-week rank correlation should be > 0.9 outside earnings weeks. Big jumps should trace to a new filing or overlay change, not noise.

---

## 8. Site Organization & Integration Plan

This plan was approved by Drew in the PR #8 review. The scores live in a **dedicated flagship hub**, modeled on `/microcaps/`, instead of adding wide columns and four filter chips to the already 15-column `/stocks/` table.

### 8.1 Hub: `/ai-exposure/`
- **Hero**: an interactive 2×2 quadrant scatter (Chart.js, as already used on stock pages). X axis is AI Bust Exposure and Y axis is AI Toll Gate, for gated names only, with the four quadrants labeled. Clicking a dot highlights its row or opens `/stocks/<sym>/`.
- **Four tabs**: `All AI-Ranked`, `Toll Collectors`, `Bag Holders & Depreciation Bombs`, `Levered Landlords`.
- **Expandable audit drawer per row**: pillar bars (A–E, T1–T4), plus clickable primary-source filing URLs and review dates from `ai_overlay.yml`.

### 8.2 Light bridge into `/stocks/`
- `layouts/stocks/list.html`: **one** compact `AI` badge column (`Toll` / `Landlord` / `Bag` / `—`), **two** Special filter chips (`Toll Collectors`, `AI Bag Holders`), and a prominent link to `/ai-exposure/`.
- `layouts/stocks/single.html`: an AI Exposure pillar card, shown **only** for stocks that pass the relevance or curation gate, so pages for non-AI stocks stay clean.

### 8.3 Static, indexable screens
Reuse the `/screens/` layout from PR #6 (`layouts/screens/`, `content/screens/`) for three server-rendered pages, and register them in `static/llms.txt`:
- `/screens/ai-toll-collectors/`
- `/screens/ai-bag-holders/`
- `/screens/ai-depreciation-bombs/`. The cut-off is to be set from the first run's distribution, e.g. AI-relevant names with `capex_to_da ≥ 1.5` and `depreciation_overhang` in the top quartile.

### 8.4 Build steps

| Step | File | Notes |
|---|---|---|
| 1 | `scripts/update_stocks.py` → `get_health_metrics` / `fetch_fundamental_data` | Extract annual capex (abs), D&A, EBIT, EBITDA, interest expense, cash, OCF and SG&A from DataFrames already fetched. No new requests. |
| 2 | `scripts/ai_exposure.py` (new) | Mirrors `dislocation_scores.py`: per-stock raw metrics and AI-beta regression, then a universe pass for percentiles, gates, composites and quadrant. Writes `ai_exposure.json` and merges the 5-field summary into `stocks.json`. |
| 3 | `data/ai_overlay.yml` (new) | 35–40 curated v1 entries with sources |
| 4 | `layouts/ai-exposure/` + `content/ai-exposure/_index.md` (new) | Hub per 8.1 |
| 5 | `layouts/stocks/list.html`, `layouts/stocks/single.html` | Bridge per 8.2 |
| 6 | `content/screens/ai-*.md`, `static/llms.txt` | Screens per 8.3 |
| 7 | `content/methodology/ai-exposure.md` | Public version of this doc in the site voice (like `micro-cap-innovation.md`) |
| 8 | `.github/workflows/` (Saturday DAG from PR #7) | Run `ai_exposure.py` after `update_stocks.py`; it has its own rerun path for overlay-only edits |
| 9 | `CLAUDE.md` | Document the new script, files and fields |

---

## 9. Known Limitations

- **yfinance gives roughly four annual statement points.** 3-year deltas are coarse, so present them as pp deltas, not trends. This is the same caveat as `dislocation_scores.py`. Annual data also lags; a company mid-way through a capex ramp is up to a year stale.
- **The overlay is manual and goes stale.** Pillars C, D and T1 are only as good as the last review. The confidence label and 180-day staleness rule exist to stop stale judgments from passing as data. v1 covers only 35–40 names.
- **Depreciation overhang assumes flat revenue.** It is a stress measure, not a forecast. A company whose AI revenue does grow into its D&A will look worse on this metric than it turns out to be. That is intended, because the score measures exposure *if* AI doesn't pay.
- **AI beta is regime-dependent.** It measures how the stock *has* traded, so names can enter or leave the AI trade faster than a 104-week window captures. The gate's hysteresis adds a deliberate lag.
- **Private exposure is invisible.** Much of the circularity sits in private labs, SPVs and private credit funds. We see only the public end of each contract.

---

## Changelog

**v0.2 (2026-11-21)**: incorporates the PR #8 review.
- Added an AI-relevance gate on Bust Exposure (overlay tier 1–3, or AI beta with hysteresis). Non-AI capital-intensive stocks are `unlinked`.
- Toll Gate is published only for curated names.
- `depreciation_overhang` now uses `abs(Capex)` and an EBITDA denominator; the EBIT variant is kept as a detail field.
- AI beta uses smooth shrinkage `t²/(t²+4)` instead of a hard `|t| ≥ 2` cut-off. The `pct_from_wma` multiplier is dropped. Added a note that orthogonalizing against SPY does not change β_AI.
- v1 uses annual statements, so no new Yahoo requests. `revenue_per_employee_growth` is dropped. REIT FFO ≈ NI + D&A. T3 falls back gracefully when SG&A is missing.
- Pillar B adds `shares_change_3yr` (equity and convertible funding) and a note on what `capex_funding_gap` misses.
- C1 is restricted to pre-profit counterparties; hyperscaler concentration goes to Pillar D.
- Toll Gate weights T1 40 → 45, T3 20 → 10, T4 20 → 25 (T2 unchanged at 20).
- Added validation events: Jul–Aug 2024, Feb–Apr 2025, Nov 2026.
- §8 replaced with the approved hub-and-bridge site organization. The full payload goes to `ai_exposure.json`, with a 5-field summary in `stocks.json`. v1 curation is 35–40 names; XBRL is v2.

**v0.1 (2026-11-20)**: initial draft.
