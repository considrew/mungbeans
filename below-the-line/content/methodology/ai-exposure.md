---
title: "AI Exposure Ranking Methodology"
description: "How mungbeans.io scores AI Bust Exposure (depreciation overhang, leverage, circular counterparty deals, AI revenue dependence, and AI beta) vs. AI Toll Gate (proprietary data moats and capital lightness)."
---

# AI Exposure Ranking Methodology

Most "AI stock" lists lump together companies buying $200B of GPUs, companies selling those GPUs, utilities financing data-center power, and software companies licensing proprietary data. Those businesses do not face the same outcome if AI capital spending slows.

mungbeans.io scores every AI-linked stock on **two independent dimensions (0–100)** and places them into a 2×2 quadrant.

---

## 1. AI Bust Exposure (0–100)

Answers one question: **if AI revenue fails to cover the capex, debt, and contractual obligations made on its behalf, how badly is this company hurt?**

| Pillar | Weight | What It Measures |
|---|---|---|
| **A. Depreciation Bomb** | 25% | `abs(Capex) / D&A`, `depreciation_overhang = (abs(Capex) − D&A) / EBITDA`, capex intensity, and 3-year capex vs. revenue growth. |
| **B. Leverage & Funding** | 25% | Capex funding gap (`max(0, Capex − OCF) / Capex`), net debt / EBITDA, YoY debt growth, interest burden, 3-year share dilution (convertibles/equity), and off-balance-sheet commitments. |
| **C. Counterparty & Circularity** | 20% | Overlay rubric (0–10) for pre-profit AI lab backlog concentration, vendor financing, equity-for-orders, take-or-pay capacity leases, and GPU-collateralized debt. Profitable hyperscaler customer concentration belongs in Pillar D, not C1. |
| **D. Revenue Dependence** | 15% | Disclosed share of revenue exposed if AI data-center spending falls 30–50% (segment filings and exposure tier). |
| **E. Market-Implied AI Beta** | 15% | Two-factor regression against SPY and an equal-weight Tier-1 AI basket (excluding the stock itself), smoothed with empirical-Bayes shrinkage `β_AI × t² / (t² + 4)`. |

### AI-Relevance Gate (Preventing False Positives)
Capital-intensive non-AI companies (airlines, oil drillers, miners, shippers) never get labeled AI Bag Holders. A stock receives a published Bust Exposure score only if it is curated in our overlay (`Tier 1–3`) or clears the statistical AI-beta gate with entry/exit hysteresis.

---

## 2. AI Toll Gate (0–100)

Answers the other half of the question: **who owns proprietary, fresh, legally authoritative data that AI systems must license—without underwriting the GPU buildout?**

| Pillar | Weight | What It Measures |
|---|---|---|
| **T1. Data Moat** | 45% | Curated 0–10 rubric across exclusivity, freshness/decay, regulatory or contractual authority, AI monetization evidence, and workflow embeddedness. **Published only for curated names; requires T1 ≥ 6/10 for the Toll Gate label.** |
| **T2. Capital Lightness** | 20% | Low capex intensity, high gross margin, and low depreciation overhang. |
| **T3. Efficiency Capture** | 10% | 3-year operating margin and SG&A ratio improvement (kept at 10% while post-inflation margin effects dominate). |
| **T4. Cash Quality** | 25% | Free cash flow margin, consecutive positive FCF years, and net cash / market cap. |
| **Disintermediation Penalty** | 0 to −20 | Subtracted for businesses whose content or search traffic can be scraped or intercepted by LLMs. |

---

## 3. The Four Quadrants

- **Toll Collector** *(Toll Gate ≥ 60 with T1 ≥ 6/10, Bust Exposure < 50)* — Collects recurring AI-era data rent with minimal balance-sheet capex risk.
- **Levered Landlord** *(Toll Gate ≥ 60 with T1 ≥ 6/10, Bust Exposure ≥ 50)* — Owns real data moats while underwriting massive AI infrastructure capex.
- **Bag Holder** *(Bust Exposure ≥ 50, low/uncurated Toll Gate)* — Carries capex, debt, dilution, or circular counterparty risk without a capital-light data toll booth.
- **Bystander** *(AI-linked, Bust Exposure < 50, low/uncurated Toll Gate)* — Participates in the AI trade with manageable balance-sheet bust risk.

Every curated score links directly to its underlying SEC filing or company disclosure and carries a review date. Entries older than 180 days are automatically flagged `stale` and lose 50% overlay weight until re-reviewed.

[Explore the live AI Exposure Screener &rarr;](/ai-exposure/)
