---
title: "How to Use the Line: A Data-Driven Guide to the 200-Week Moving Average"
date: 2026-03-17
draft: true
tags: ["framework", "guide", "200wma", "backtesting", "methodology", "episodes"]
description: "We tracked 3,893 episodes of S&P 500 stocks crossing below their 200-week moving average — including the ones that never came back. Here's what actually predicts recovery, what doesn't, and how to use the framework without fooling yourself."
---

*A mungbeans.io framework guide — March 17, 2026*

## The Promise and the Problem

The 200-week moving average is one of the oldest ideas in technical analysis. The logic is simple: a stock that drops below its ~4-year average price has fallen out of favor with the market. If the business is sound, the price should eventually recover. Buy the dip, wait for the reversion.

It works often enough that people believe in it. But "often enough" isn't a framework. We wanted to know: how often, how fast, how much — and what separates the recoveries from the disasters.

So we ran the data. Every S&P 500 stock that crossed below its 200-week moving average from 2014 through March 2026. Not just the stocks in the index today — we went back and pulled the stocks that were *removed* from the index during that period, the ones survivorship bias usually hides. 3,893 total crossing episodes across 458 companies that crossed below the line, tracked from the moment each stock dipped below to the moment it climbed back above. An estimated 743 unique companies were in the S&P 500 at some point during this window — about 106 current members never crossed below their 200WMA at all, and roughly 178 removed stocks (mostly pre-2019 exits) couldn't be included because historical index change data wasn't available. The missing stocks are disproportionately the ones most likely to have crossed and never recovered, so our recovery rate is probably optimistic.

No fixed measurement windows. No 12-month snapshots. Each episode measured on its own natural timeline — because a stock that recovers in 2 weeks and one that takes 2 years are not the same trade, and pretending they are by measuring both at 12 months is how frameworks lie to you.

Here's what we found.

<iframe src="/graphics/episode-framework-charts.html" width="100%" height="1400" style="border: none; border-radius: 8px;" loading="lazy"></iframe>

---

## Almost Everything Recovers. That's Not the Insight.

Of the 3,893 episodes in our dataset, 3,851 recovered — the stock eventually climbed back above the 200WMA. That's a 98.9% recovery rate. Before you get excited, understand what this means and what it doesn't.

It means that for S&P 500 companies, crossing below the 200WMA is almost never a permanent condition. These are large, liquid businesses that institutional investors will eventually reprice upward. The line is not a death sentence.

What it doesn't mean is that buying at the cross is a free trade. The 42 episodes that *never* recovered — all from stocks later removed from the index — had a median return of **-56%**. Perrigo crossed below the line in February 2016 and never came back; anyone who bought there is sitting on a -90% loss ten years later. SolarEdge, First Republic, Silicon Valley Bank — the unrecovered list reads like a graveyard of broken theses.

The recovery rate tells you the floor is high. The unrecovered episodes tell you the floor has a trap door.

---

## Time Is the Signal

This is the most important finding in the entire analysis, and it's the one that changes how you should use the framework.

The median time below the 200WMA is **3 weeks**. Two-thirds of all episodes resolve within 5 weeks. 83% resolve within 13 weeks. The typical below-the-line event is not a prolonged value trap — it's a brief dip that snaps back quickly.

And the speed of recovery *is* the signal:

| Duration | % of Episodes | Beat S&P 500 | Median Excess Return |
|----------|--------------|-------------|---------------------|
| 1 week | 34% | 89% | +3.2% |
| 2–4 weeks | 28% | 78% | +2.8% |
| 5–12 weeks | 20% | 66% | +2.0% |
| 13–25 weeks | 8% | 57% | +1.3% |
| 26–51 weeks | 5% | 24% | -8.5% |
| 52+ weeks | 5% | 9% | -25% |

Read that table carefully. In the first few weeks, the odds are overwhelmingly in your favor. By week 5, 67% of episodes have resolved and the resolved ones beat the S&P 89–78% of the time. The excess returns are modest in absolute terms — a few percent — but the win rate is extremely high.

Then something breaks. Episodes lasting 26 weeks or more underperform the S&P 76% of the time. By a year, you're beating the index less than 10% of the time with a median -25% excess return. The longer a stock stays below the line, the more likely the market was *right* to sell it.

This creates a natural checkpoint. At 13 weeks, 83% of episodes have resolved. The ones that resolved beat the S&P 79% of the time. The 17% still stuck below the line go on to beat the index only 35% of the time. **Week 13 is where you reassess the thesis.** Not because it's a magic number, but because the data says the distribution of outcomes flips dramatically around that point.

---

## Depth: The Return Engine

How far below the 200WMA a stock falls at the moment of crossing is the strongest predictor of episode return. It's monotonic and it's large:

| Depth Below 200WMA | Median Episode Return | Return Per Week at Risk | Beat S&P |
|--------------------|-----------------------|------------------------|----------|
| 0–2% | +3.2% | 1.5%/wk | 69% |
| 2–5% | +5.6% | 1.8%/wk | 76% |
| 5–10% | +10.2% | 1.5%/wk | 74% |
| 10–20% | +17.9% | 2.3%/wk | 74% |
| 20%+ | +34.2% | — | 76% |

Deeper crossings take longer to recover — median 2 weeks for shallow dips vs. 8–12 weeks for deep drops — but they pay proportionally more per week at risk. A stock that gaps 10-20% below the line in a single week delivers the highest return efficiency in the dataset at +2.3% per week.

The deepest crossing in our S&P 500 data (excluding a merger artifact) was ONEOK at -44% during COVID March 2020. It returned +89% in 12 months. Every crossing deeper than 20% in the dataset recovered, and the median episode return at that depth is +34%.

This doesn't mean deeper is always better — deep crossings are more volatile during the episode, with median max drawdowns of -23% for the 26–51 week episodes. But if you're entering a position at -15% below the line, you have significantly more expected upside than someone buying at -2%.

---

## Crossing Frequency: The Stock's Résumé

How many times a stock has crossed below its 200WMA in the past tells you something fundamental about the business — and it's one of the strongest signals in the dataset.

| Crossing History | Episodes | Beat S&P (12mo) | Median Excess (12mo) |
|-----------------|----------|-----------------|---------------------|
| Rare (1–2 crosses in 11 years) | 41 | 81% | +18.8% |
| Occasional (3–5) | 317 | 62% | +11.6% |
| Frequent (6–9) | 786 | 55% | +4.1% |
| Very Frequent (10–14) | 1,064 | 49% | -0.7% |
| Chronic (15+) | 818 | 42% | -4.0% |

This signal holds within sectors (p<0.0001 for rare vs. chronic within cyclicals alone) and across market regimes. It's not just measuring sector quality in disguise.

The logic is straightforward. A company like Mastercard — which crossed below the 200WMA exactly once in our dataset, in September 2022 — is telling you through its price history that it almost never falls this far out of favor. When it does, it's an unusual event, and unusual events for dominant businesses tend to be temporary. MA returned +38% in that episode.

Compare that with a company that crosses below the line 15+ times in 11 years. It's not experiencing dips — it's living below the line. The market repeatedly tells you this stock underperforms its own long-term trend, and buying each dip hasn't worked well historically.

The removed-stock data reinforces this: stocks eventually removed from the S&P 500 had a median of 10 crossings before removal, versus 8 for survivors. Chronic crossing precedes index removal.

### Who Rarely Crosses?

The rare crossers in our dataset include: AVGO (1 cross, +158% recovery), NVDA (1 cross), MA (1 cross, +38%), V (1 cross), CTAS (1 cross, +115%), HD (2 crosses), LOW (2 crosses, +175% on the deep one), CRWD (2 crosses), FICO (2 crosses, +109%). These are businesses with durable competitive advantages that the market almost never gives up on.

When one of them *does* cross below the line, pay attention.

---

## What the Sector Data Actually Shows

In our [earlier FCF backtest](/deep-dives/below-the-line-fcf-backtest/), we reported a massive sector gap: cyclical stocks (Industrials, Tech, Financial Services, Consumer Cyclical) beat the S&P 60% of the time at 12 months, while defensives (Utilities, Consumer Defensive, Healthcare, Energy) managed just 35%.

The episode-based analysis tells a different story. When you measure each crossing on its own timeline — entry at the cross, exit at the recovery — the sector gap narrows dramatically:

| Sector | Median Episode Return | Return/Week | Beat S&P |
|--------|----------------------|-------------|----------|
| Consumer Cyclical | +6.4% | 1.8%/wk | 76% |
| Technology | +6.0% | 1.9%/wk | 78% |
| Industrials | +5.5% | 2.1%/wk | 74% |
| Communication Services | +5.3% | 1.5%/wk | 73% |
| Financial Services | +5.5% | 1.5%/wk | 65% |
| Basic Materials | +4.9% | 1.4%/wk | 73% |
| Healthcare | +4.9% | 1.3%/wk | 69% |
| Energy | +4.6% | 1.1%/wk | 74% |
| Real Estate | +4.2% | 1.4%/wk | 69% |
| Consumer Defensive | +3.9% | 1.4%/wk | 73% |
| Utilities | +3.8% | 1.5%/wk | 67% |

Cyclicals still lead — Industrials deliver the best return efficiency at 2.1% per week, and Tech has the highest beat rate at 78%. But the median return gap between the best and worst sectors is 2.6 percentage points, not the 22 points the fixed-window analysis suggested.

What happened? The 12-month measurement was catching defensive stocks *mid-episode* — still below the line, not yet recovered — and penalizing them for being slow. The episode view only measures the completed recovery itself, and it turns out defensives recover at similar total returns, just sometimes over a longer period.

This matters for portfolio construction. The sector signal isn't "avoid defensives." It's "Industrials and Tech recover fastest per unit of time at risk, so if you're optimizing for capital efficiency, weight toward those sectors."

---

## What Doesn't Work (Or What We Can't Prove)

Intellectual honesty requires admitting what the data *doesn't* support. Several things we expected to be strong signals weren't.

**Free cash flow direction alone is not significant.** Positive FCF at the time of crossing vs. negative FCF showed a directional advantage but did not reach statistical significance (p=0.22). The FCF yield at crossing was weakly significant at the ≥10% threshold (p=0.07) but not at ≥5%. Cash flow matters, but it's not the primary signal our earlier work suggested — see [the FCF backtest](/deep-dives/below-the-line-fcf-backtest/) for the full discussion.

**FCF yield is partially redundant with depth.** When a stock drops 40%, its FCF yield doubles mechanically — the numerator (cash flow) hasn't changed, but the denominator (market cap) has been halved. A stock screening as "high FCF yield at crossing" is partly just screening as "deep below the line," which we're already measuring directly.

**FCF trajectory is counterintuitive.** "Positive and growing" FCF at the time of crossing was the worst positive category (39% beat rate at 12 months, vs. 44% for positive-declining and 45% for positive-unknown-trend; n=204, 138, 376 respectively). The likely explanation: if cash flow is growing and the stock still dropped below the line, the market is pricing in something the backward-looking FCF number hasn't captured yet. The data on this is explored further in [the FCF backtest](/deep-dives/below-the-line-fcf-backtest/).

**Operating margins, ROE, and ROA cannot be tested properly.** Historical quarterly financial data from Yahoo Finance only extends back ~2 years, so we can't match these metrics to crossings before 2024. Using current fundamentals retroactively introduces look-ahead bias that would inflate the results. We won't publish conclusions we can't defend.

**Capital allocation (buybacks + dividends + debt repayment) is promising but unproven.** Companies actively buying back shares, paying dividends, and paying down debt at the time of crossing showed a 67% beat rate vs 31% for companies doing none of these (p=0.01, n=170). The signal makes intuitive sense — it's management confidence made tangible — but the sample is too small and too recent (2025–2026 only) to build a framework around.

---

## The Survivorship Problem

Every backtest of S&P 500 stocks has the same flaw: you're only testing companies that survived to be in the index today. The ones that failed — and got removed — are invisible.

We went back and pulled crossing data for 62 stocks removed from the S&P 500 between 2019 and 2026 — the period for which we have reliable index change records. Not companies that were acquired at a premium (those are fine outcomes), but companies that declined: Macy's, Nordstrom, Lumen Technologies, SolarEdge, First Republic, Dollar Tree, Walgreens, VF Corp. This is a partial correction — roughly 178 additional stocks were removed between 2014 and 2018 that we couldn't include, meaning the true failure rate is likely higher than what follows.

The results:

| | Survivors | Removed Stocks | Combined |
|---|-----------|---------------|----------|
| Episodes | 3,245 | 648 | 3,893 |
| Recovery rate | 100% | 93.5% | 98.9% |
| Median episode return | +5.1% | +5.5% | +5.1% |
| Unrecovered episodes | 0 | 42 | 42 |
| Unrecovered median return | — | -56% | -56% |

Three things stand out. First, the recovery rate drops from 100% to 93.5% for removed stocks — survivorship bias is real. Second, for the episodes that *did* recover, the removed stocks actually had slightly higher median returns (+5.5% vs +5.1%), because they were more volatile and bounced harder. Third, the unrecovered episodes are catastrophic: median -56%, worst -90%.

The practical takeaway: 42 out of 3,893 episodes (1.1%) resulted in permanent capital destruction. That's a low base rate, but the severity is extreme enough that it matters for portfolio sizing. A single unrecovered position can wipe out the gains from dozens of successful episodes.

The removed stocks had something in common: they were chronic crossers, with a median of 10 crossings before getting booted from the index. The crossing frequency signal isn't just about returns — it's an early warning system for structural decline.

---

## The Framework

All of the above distills into a practical system — three selection signals, a time-based checkpoint, and a risk rule, all observable at the moment a stock crosses below its 200-week moving average with no look-ahead bias.

### 1. When the stock crosses below the line, start the clock.

This is your entry signal. The stock has crossed below its 200-week moving average. You have a candidate.

### 2. How far has it fallen?

Depth below the line is the strongest predictor of episode return. A stock at -2% below the line is barely a signal. A stock at -10% is a real dislocation. A stock at -20%+ is a rare event that has historically delivered +34% median returns through the full episode.

Deeper crossings take longer to resolve — expect 7–12 weeks rather than 2–3 — but pay proportionally more per week at risk.

### 3. Has this stock been here before?

Check the crossing history. A stock that's crossed below the 200WMA once or twice in a decade is a strong business experiencing a rare event. A stock that crosses 10+ times is a structurally weak business experiencing a recurring event. The data shows an 81% beat rate for rare crossers vs 42% for chronic crossers, with a 23-point gap in median excess return.

You can check this by looking at a long-term weekly chart with the 200WMA overlaid. If the stock has spent significant time below the line in recent years, that's a warning.

### 4. What sector is it in?

Not a hard filter, but a weighting signal. Industrials and Technology deliver the highest return per week at risk (2.1% and 1.9% respectively). Energy is the slowest (1.1%/week). On a per-episode basis the differences are smaller than the 12-month data suggested, but for capital efficiency, cyclical sectors have an edge.

### 5. Set your checkpoint at 13 weeks.

If the stock hasn't recovered above the 200WMA by week 13, the distribution of outcomes changes. 83% of episodes resolve by then; the ones that don't beat the S&P only 35% of the time going forward.

This doesn't mean you automatically sell at week 13. It means you *actively reassess* the thesis. Has something structurally changed — a product cycle break, a regulatory threat, a competitive moat eroding? Or is the stock just taking longer to recover in a difficult market? The 13-week checkpoint forces you to ask the question rather than anchoring to your entry price.

### 6. Size for the tail risk.

The base rate of permanent loss is roughly 1% of episodes, but the severity is -56% median for the unrecovered ones. A concentrated bet on a single below-the-line stock can destroy a portfolio if you're wrong. The framework works best as a portfolio approach: multiple positions across different crossing events, sized so that a single failure can't undo the winners.

---

## Applying This to Today

As of March 2026, the Iran-Hormuz crisis has pushed a large number of S&P 500 stocks below their 200-week moving averages simultaneously. 97 names are currently below the line. This is the kind of broad, sudden, geopolitically-driven selloff that has historically produced the strongest recovery episodes — indiscriminate selling creates dislocations in businesses that have nothing to do with oil prices.

A few names that score well on the framework:

**PayPal (PYPL)** — First crossing ever. -36% below the line. A company that has never been this far out of favor in its public history. FCF yield of 7.7%. The rare-crosser signal is about as strong as it gets.

**Global Payments (GPN)** — Also first crossing ever. -33% below. FCF yield of 9.2%. Payments infrastructure business dragged down in a geopolitical selloff.

**Humana (HUM)** — Second crossing. -55% below — one of the deepest in the current S&P 500. FCF yield of 6.5%. The depth here is extraordinary for a healthcare company.

**Charter Communications (CHTR)** — Third crossing in 11 years. -37% below. FCF yield of 9.7%.

**Intuit (INTU)** — Third crossing. -20% below. Software business with durable recurring revenue. PAYX, CPRT, WST, ELV similar profile — rare crossers in the -20% to -30% range.

**Adobe (ADBE)** — Eight crossings but -42% below with 9.0% FCF yield. The crossing frequency is a caution flag, but the depth and cash generation are notable.

Names that carry more risk: LULU (11th crossing), WDAY (16th), GIS (15th), LEN (15th), WY (18th crossing with negative FCF). Chronic crossers in a selloff are not the same trade as rare crossers in a selloff, even if the depth looks similar.

*This is not investment advice. These are observations from historical data applied to current conditions. The framework identifies candidates for further research — your own thesis should determine whether to act. Past patterns do not guarantee future results, and the current geopolitical environment has no direct historical parallel in our dataset.*

---

## Methodology

**Universe:** An estimated 743 unique companies were S&P 500 constituents at some point between 2014 and 2026. Of those, 397 current members crossed below the 200WMA at least once (106 never did), and 62 removed stocks (2019–2026 exits) were included with full crossing data. Roughly 178 stocks removed before 2019 are missing from the dataset — these are likely to have had worse outcomes on average, meaning our recovery and beat rates carry a survivorship bias we can bound but not eliminate. Total analyzed: 458 unique companies, 3,893 crossing episodes.

**Episode definition:** An episode begins when a stock's weekly closing price crosses below its 200-week simple moving average. It ends when the weekly closing price crosses back above the 200WMA. Returns are measured from crossing price to recovery price.

**S&P 500 comparison:** For each episode, we computed the S&P 500 (SPY) return over the identical calendar period — same entry date, same exit date. Excess return is the stock's episode return minus SPY's return over that same window.

**Crossing frequency:** Total number of crossing episodes per ticker across the full 2014–2026 dataset.

**Survivorship correction:** Removed stocks were identified from S&P Dow Jones index change records. We excluded pure ticker changes and acquisitions at premium prices, retaining only stocks removed for market cap decline, financial distress, or index committee discretion.

**FCF data:** Annual free cash flow from Yahoo Finance, matched to the most recent annual report date prior to each crossing. Coverage is strongest for 2022+ crossings (~35% match rate). See [the FCF backtest](/deep-dives/below-the-line-fcf-backtest/) for detailed methodology.

**Data download:** The full datasets are available for independent verification and further analysis:
- [Survivor episode data (3,245 episodes)](/data/crossing_episodes.csv) — ticker, crossing date, recovery date, weeks below, episode return, SPY comparison, sector, crossing frequency
- [Removed stock episodes (648 episodes)](/data/removed_stock_episodes.csv) — same structure for stocks removed from the S&P 500
- [Raw crossings with fixed-window returns](/data/crossings_raw.csv) — the original 3,370 crossings with 3/6/12/24 month returns
- [Current screener (March 2026)](/data/current_below_200wma.csv) — S&P 500 stocks currently below the 200WMA

**Limitations:** Survivorship bias is partially but not fully corrected — we could only pull data for stocks removed from 2019 onward. Stocks removed before 2019, or stocks that were never in the S&P 500, are excluded. The 98.9% recovery rate likely overstates the true rate for the broader market. FCF yield at crossing is mechanically correlated with depth below the 200WMA. Current fundamental data (ROE, ROA, margins) could not be reliably matched to historical crossings and was not used in the episode framework.

---

*This analysis is for informational purposes only and does not constitute investment advice. Past performance does not guarantee future results. Backtested results are inherently biased by survivorship, look-ahead, and data selection effects. Always do your own research before making investment decisions.*

*Framework methodology: [mungbeans.io 200-week moving average screening system](https://mungbeans.io)*
