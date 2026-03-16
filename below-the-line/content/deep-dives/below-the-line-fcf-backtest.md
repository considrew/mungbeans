---
title: "Below the Line: What 3,370 Crossings Tell Us About Free Cash Flow and the 200-Week Moving Average"
date: 2026-03-16
draft: false
tags: ["framework", "backtesting", "200wma", "free-cash-flow", "methodology", "data"]
description: "We backtested every S&P 500 stock that crossed below its 200-week moving average over 15 years and asked: does positive free cash flow predict better outcomes? The answer is more complicated — and more useful — than we expected."
---

*A mungbeans.io framework analysis — March 16, 2026*

## The Question

The mungbeans framework rests on a simple premise: stocks that cross below their 200-week moving average and maintain strong fundamentals are the ones most likely to recover. The 200WMA tells you the market has given up. The fundamentals tell you whether the market is wrong.

But "strong fundamentals" is a hand-wave. What specifically should you look for? The intuitive answer — the one we assumed before running the data — is free cash flow. A company generating cash is a company that can survive whatever pushed it below the line, buy back shares at depressed prices, maintain dividends, and wait for the market to re-rate. A company burning cash is a company that might deserve to be below the line.

We wanted to prove this. So we pulled 15 years of weekly price data for the S&P 500, identified every instance where a stock crossed below its 200-week moving average, matched each crossing with the company's free cash flow at the time, and measured what happened over the next 3, 6, 12, and 24 months.

3,370 crossings. 397 unique companies. 2011 through March 2026 — capturing the post-GFC recovery, the 2015–2016 industrial recession, the COVID crash, the 2022 bear market, and the current cycle.

The results challenged our assumptions.

---

## The Baseline: Crossing Below the 200WMA Is Already a Strong Signal

Before layering in cash flow, here's what happens to the average S&P 500 stock after it crosses below the 200-week moving average:

| Horizon | Median Return | Mean Return | Win Rate | n |
|---------|--------------|------------|----------|---|
| 3 months | +6.9% | +7.9% | 68% | 3,284 |
| 6 months | +11.1% | +13.5% | 72% | 3,215 |
| 12 months | +20.7% | +25.1% | 75% | 3,034 |
| 24 months | +39.4% | +48.8% | 85% | 2,720 |

Read that 12-month line: a 75% win rate with a median return of +20.7%. Over two years, the median return is +39.4% with an 85% win rate. The 200-week moving average cross — on its own, with no fundamental filter — is already a high-probability signal in the S&P 500.

This makes sense. The 200WMA represents roughly four years of price history. For a large-cap stock to cross below it, something significant has to go wrong — an earnings miss, a sector rotation, a macro shock. But S&P 500 companies, by definition, are large, liquid, and profitable enough to remain in the index. Most of them recover. The line catches the capitulation; the recovery is the norm, not the exception.

The question is whether cash flow makes the recovery *better*.

---

## The Surprise: Positive FCF Doesn't Significantly Beat Negative FCF

This was the finding we didn't expect.

We split every crossing into two groups: companies with positive free cash flow at the time of crossing, and companies with negative free cash flow. Here's the comparison:

| Horizon | Positive FCF (n=1,026) | Negative FCF (n=156) | Difference | Significant? |
|---------|----------------------|---------------------|-----------|-------------|
| 3 months | +6.0% median, 65% win | +3.5% median, 62% win | +2.5pp | No (p=0.22) |
| 6 months | +9.2% median, 72% win | +5.5% median, 61% win | +3.7pp | Marginal (p=0.06) |
| 12 months | +16.4% median, 71% win | +16.9% median, 78% win | -0.5pp | No (p=0.67) |
| 24 months | +33.7% median, 80% win | +38.3% median, 93% win | -4.6pp | No (p=0.44) |

At 12 months, there is *no statistically significant difference* between positive and negative FCF stocks that have crossed below the 200WMA. In fact, negative FCF stocks have a slightly higher median return (+16.9% vs. +16.4%) and a meaningfully higher win rate (78% vs. 71%). At 24 months, negative FCF stocks outperform by 4.6 percentage points with a 93% win rate.

This is not a fluke. The Mann-Whitney U test (a non-parametric test that doesn't assume normal distributions) confirms: there is no significant difference at p < 0.05 for any horizon except a marginal signal at 6 months.

Why? Three possible explanations:

**Mean reversion is stronger for beaten-down names.** Negative FCF companies that cross below the 200WMA have typically been hit harder and for longer. The market has already priced in the worst case. When things stabilize — even modestly — the snapback is dramatic. The 93% win rate at 24 months suggests that S&P 500 companies with negative FCF almost always recover, because the ones that don't recover get removed from the index before the 24-month window closes. There's survivorship bias in this number, and we should be honest about it.

**Positive FCF stocks cross the line for different reasons.** When a profitable company crosses below the 200WMA, the market is often seeing something the trailing FCF data hasn't captured yet — a secular shift, a competitive threat, a margin compression that hasn't hit the cash flow statement. The FCF might be positive and *about to turn negative*. The crossing is the market pricing that future deterioration.

**The 200WMA is already doing the fundamental screening.** S&P 500 membership is itself a quality filter. By the time you've restricted the universe to large-cap, index-constituent companies that have crossed below a four-year moving average, you've already filtered out most of the junk. Adding a positive FCF screen on top of that filter doesn't add much incremental signal.

---

## Where the Edge Actually Is: FCF Yield ≥ 10%

If the simple positive/negative split doesn't work, does the *magnitude* of cash flow matter?

We computed FCF yield — free cash flow divided by market capitalization — at the time of each crossing, then tested every threshold from 0% to 15% to find where the edge appears. This is what the data says:

| FCF Yield Threshold | n | Median 12mo Return | Win Rate | p-value |
|--------------------|----|-------------------|---------|---------|
| ≥ 0% (any positive) | 726 | +16.4% | 71% | 0.33 |
| ≥ 3% | 578 | +15.0% | 71% | 0.93 |
| ≥ 5% | 349 | +17.5% | 74% | 0.064 |
| ≥ 8% | 183 | +19.3% | 72% | 0.083 |
| **≥ 10%** | **137** | **+22.4%** | **77%** | **0.007** |
| ≥ 12% | 107 | +24.1% | 85% | 0.0008 |
| ≥ 15% | 75 | +28.2% | 89% | <0.0001 |

The breakpoint is 10%. Below that threshold, there is no statistically significant difference between high-FCF and low-FCF crossings. Above 10%, the edge is real and growing: +22.4% median at ≥10% (p=0.007), +24.1% at ≥12% (p=0.0008), +28.2% at ≥15% (p<0.0001).

The interpretation: it's not enough for a company to be generating cash. It has to be generating *a lot* of cash relative to its beaten-down market cap. A 10%+ FCF yield on a stock below the 200WMA means the market is pricing the company as if it's permanently impaired, while the cash flow statement says otherwise. That gap — between what the market expects and what the company actually generates — is the edge.

At ≥12% FCF yield, the signal gets even cleaner: 85% win rate, +24.1% median return at 12 months. These are companies throwing off cash at rates that would repay their entire market cap in 8 years or less, and the market has pushed them below their four-year price average. That's either a bankruptcy candidate or a screaming buy. In the S&P 500, it's almost always the latter.

---

## The Risk Picture: FCF Yield Reduces Drawdowns

Returns are only half the picture. What about the pain between entry and recovery?

| Signal | Median Max Drawdown (12mo) | Median 12mo Return |
|--------|---------------------------|-------------------|
| All crossings (baseline) | — | +20.7% |
| + FCF yield ≥ 3% | -8.7% | +15.0% |
| + FCF yield ≥ 5% | -6.7% | +17.5% |
| + FCF yield ≥ 8% | -7.8% | +19.3% |

The FCF yield ≥ 5% threshold produces the best risk-adjusted profile: a median max drawdown of only -6.7% with a +17.5% median 12-month return. You're giving up some upside compared to the unfiltered baseline (+17.5% vs. +20.7%), but you're cutting the worst-case drawdown substantially. For a framework that emphasizes not losing money while you wait for mean reversion, this matters.

The ≥5% threshold also makes intuitive sense as a "quality floor." A company generating 5% of its market cap in free cash flow annually has real earnings power. It can fund buybacks, pay dividends, reduce debt, or invest in growth — all of which support the recovery thesis. Below 5%, the cash generation might not be strong enough to anchor the stock's value during extended drawdowns.

---

## The Counterintuitive Finding: Growing FCF Is the Worst Signal

This is the result we keep coming back to because it's so uncomfortable.

We categorized every crossing by the *trend* in free cash flow — was FCF growing year-over-year or declining at the time the stock crossed below the 200WMA?

| FCF Category | n | Median 12mo | Win Rate | Median Max Drawdown |
|-------------|---|------------|---------|-------------------|
| Positive & Growing | 208 | +8.4% | 62% | -12.9% |
| Positive & Declining | 142 | +9.8% | 58% | -14.9% |
| Negative & Improving | 18 | +25.7% | 100% | -2.1% |
| Negative | 102 | +14.9% | 74% | -9.0% |

"Positive & Growing" FCF — the category that should theoretically be the strongest signal — has the *worst* forward returns among all positive-FCF categories: +8.4% median at 12 months with only a 62% win rate and a -12.9% median max drawdown.

Why? Because when a company with growing free cash flow crosses below the 200WMA, the market is telling you something specific: the *future* is worse than the *past*. The cash flow statement is backward-looking. The stock price is forward-looking. A company whose FCF is growing but whose stock is plunging has a market that doesn't believe the growth will continue. And the market, on average, is right — these stocks have the lowest win rate and the deepest drawdowns.

Compare that to "Negative & Improving" — companies with negative FCF that's getting less negative. This tiny cohort (n=18, so treat with caution) has a 100% win rate, +25.7% median return, and only -2.1% max drawdown. The market priced in disaster, the cash flow is stabilizing, and the recovery is nearly certain. The direction of cash flow matters more than the sign.

---

<div style="margin: 2rem 0;">
<iframe src="/graphics/fcf-backtest-charts.html" width="100%" height="1200" style="border: 1px solid #30363d; border-radius: 8px;" frameborder="0" loading="lazy"></iframe>
</div>

---

## What This Means for the Framework

The mungbeans screener uses the 200-week moving average as its primary signal. This backtest validates that signal — a 75% win rate with +20.7% median 12-month returns across 3,034 observations is robust. The line works. (Note: since the data was tested using S&P500 companies, we're already looking at supposed "quality" stocks)

But the cash flow layer is more nuanced than we assumed. Here's what the data actually supports:

**The 10% FCF yield rule.** When a stock crosses below the 200WMA with a free cash flow yield of 10% or higher, the forward returns are statistically significantly better than the baseline: +22.4% median at 12 months with a 77% win rate, versus +15.4% and 71% for everything below that threshold. The p-value is 0.007. This isn't noise. At ≥12% FCF yield, it's +24.1% with an 85% win rate (p=0.0008). This is the framework's highest-conviction signal.

**The 5% FCF yield floor.** If you're looking for the best risk-adjusted returns rather than the highest absolute returns, the ≥5% FCF yield threshold offers the lowest median max drawdown (-6.7%) while preserving most of the upside (+17.5% median 12mo). Use this as the quality floor for the screener.

**Don't trust growing FCF at the cross.** This is the hardest finding to accept, but the data is clear: stocks with positive and growing FCF at the time of the 200WMA cross have the worst forward returns (62% win rate, +8.4% median). The market is forward-looking; the cash flow statement is backward-looking. When a company with growing cash flow crosses below a four-year moving average, the market is pricing in a deterioration that hasn't shown up in the financials yet. Be extra skeptical of these names.

**Direction matters more than sign.** Negative FCF that's improving is a stronger signal than positive FCF that's growing. The market has already priced in the negative cash flow; the improvement signals a turn. Sample sizes are small (n=18), so we can't build a strategy on this alone, but it aligns with the framework's philosophy: buy the recovery, not the perfection.

**Depth below the line amplifies returns.** Positive FCF stocks that are 5–10% below the 200WMA return +20.7% at 12 months (79% win rate) compared to +15.6% for those just barely below (70% win rate). The deeper the dislocation, the stronger the mean reversion — as long as the fundamentals support survival.

---

## What the Screener Finds Right Now

We ran the full S&P 500 through the framework as of March 14, 2026. There are 97 stocks currently below their 200-week moving averages — and the FCF yield tiers map cleanly onto the backtest findings.

### High Conviction (FCF Yield ≥ 10%)

The backtest says this tier delivers +22.4% median 12-month returns with a 77% win rate (p=0.007). Seven S&P 500 stocks currently qualify:

| Ticker | Company | Price | % Below 200WMA | FCF Yield | Fwd P/E | Sector |
|--------|---------|-------|---------------|-----------|---------|--------|
| PRU | Prudential Financial | $92.00 | -3.1% | 38.9% | 6.1x | Financial Services |
| CNC | Centene | $34.45 | -46.9% | 29.6% | 8.5x | Healthcare |
| IFF | Intl Flavors & Fragrances | $69.61 | -15.3% | 17.2% | 14.6x | Basic Materials |
| HPQ | HP Inc. | $18.93 | -29.9% | 16.5% | 6.3x | Technology |
| CI | Cigna Group | $267.19 | -7.8% | 13.6% | 8.0x | Healthcare |
| GIS | General Mills | $39.38 | -35.2% | 11.9% | 11.3x | Consumer Defensive |
| KHC | Kraft Heinz | $22.58 | -23.7% | 10.3% | 10.6x | Consumer Defensive |

Look at what's in this tier. CNC at -46.9% below the line with a 29.6% FCF yield — the market is pricing Centene like it's going bankrupt while the company is generating $5.1 billion in free cash flow. HPQ at -29.9% below with 16.5% FCF yield and a 6.3x forward P/E. GIS at -35.2% below with 11.9% FCF yield. These are massive dislocations between price and cash generation. The backtest says this gap closes 77% of the time within 12 months.

### Strong Signal (FCF Yield ≥ 5%)

The best risk-adjusted tier: -6.7% median max drawdown with a 74% win rate. Thirty-six additional stocks qualify, including:

| Ticker | Company | Price | % Below 200WMA | FCF Yield | Fwd P/E | Sector |
|--------|---------|-------|---------------|-----------|---------|--------|
| CHTR | Charter Communications | $218.19 | -36.8% | 9.7% | 4.6x | Communications |
| GPN | Global Payments | $68.67 | -32.6% | 9.2% | 4.2x | Industrials |
| ACN | Accenture | $196.65 | -33.5% | 9.0% | 13.4x | Technology |
| ADBE | Adobe | $249.32 | -42.1% | 9.0% | 9.6x | Technology |
| PFE | Pfizer | $26.58 | -7.4% | 8.9% | 9.4x | Healthcare |
| CRM | Salesforce | $192.83 | -17.2% | 8.8% | 13.3x | Technology |
| WDAY | Workday | $133.09 | -39.1% | 8.4% | 10.7x | Technology |
| PYPL | PayPal | $44.90 | -35.6% | 7.7% | 7.8x | Financial Services |
| QCOM | Qualcomm | $129.82 | -7.5% | 7.5% | 11.6x | Technology |
| UNH | UnitedHealth | $282.09 | -37.2% | 5.3% | 14.3x | Healthcare |
| UPS | United Parcel Service | $97.21 | -23.2% | 6.3% | 12.2x | Industrials |
| CMCSA | Comcast | $30.16 | -8.8% | 5.1% | 7.6x | Communications |

Adobe at -42.1% below the line generating $9.3 billion in free cash flow. Workday at -39.1% generating $3.0 billion. PayPal at -35.6% generating $3.3 billion. These are the kinds of dislocations the backtest was built to find — profitable companies pushed far below their long-term averages where the market has overpriced a risk that the cash flow doesn't support.

UnitedHealth at -37.2% is notable given the current healthcare sector pressure. $13.9 billion in FCF against a market that's priced in worst-case DOJ/regulatory outcomes. The backtest would call this a strong signal.

### Baseline Signal (Below 200WMA, Positive FCF < 5%)

These have the baseline 71% win rate — the 200WMA cross is doing the work, but the FCF yield isn't strong enough to add statistical edge. Names like Nike (-37.9%, 3.2% FCF yield), Danaher (-16.8%, 3.4%), Intuit (-20.1%, 4.2%), and lululemon (-50.0%, 4.7%) are in this zone. They're below the line with positive cash flow, but the FCF yield isn't screaming that the market has mispriced them.

### Caution: Below the Line with Negative FCF

Four names: Moderna (-42.5%, -$1.3B FCF), Weyerhaeuser (-16.4%), Block (-12.2%), and DOW (-7.8%). The backtest says negative FCF stocks still recover 74% of the time in the S&P 500, but the edge is weaker and the thesis has to come from somewhere other than cash flow — a catalyst, a sector turn, or a macro shift. DOW's petrochemical feedstock advantage from the [Strait of Hormuz thesis](/deep-dives/sesame-seed-trades-strait-of-hormuz/) provides that catalyst. Moderna's does not.

### Previously Covered Names

**DOW (-7.8% below, negative FCF)** — confirmed in the caution tier. The Strait-driven petrochemical thesis provides the catalyst the backtest can't.

**SMR (NuScale)** — no longer in the S&P 500 screener (too small), but the 200WMA signal applies independently. Pre-commercial, no meaningful FCF to measure. Technology bet, not a cash flow play.

**PCG (+12% above the line)** — not below the line yet. If it crosses, its current FCF profile would put it in the baseline tier. Watch for the cross.

---

## Methodology Notes

**Universe:** S&P 500 constituents as of March 2026 (422 tickers with sufficient price history). Survivorship bias is present — stocks removed from the index before our analysis window are excluded. This likely inflates win rates, particularly at the 24-month horizon, because companies that went bankrupt or were delisted were removed from the index before we could observe their failure. The 12-month results are less affected.

**Price data:** Weekly closing prices from January 2011 through March 2026, sourced from Yahoo Finance. The 200-week moving average requires 200 weeks (~3.8 years) of data before it generates a value, so the first crossings appear in late 2014.

**FCF data:** Annual free cash flow from Yahoo Finance company financials. Matched to crossings using the most recent annual report date prior to each crossing. yfinance retains approximately 4 years of historical financial data, so FCF matching is most complete for crossings from 2022 onward. Of 3,370 total crossings, 1,182 (35%) have matched FCF data. Results are based on this subset where noted.

**FCF yield:** Computed as annual free cash flow divided by market capitalization at the time of crossing. Market cap is approximated using share price at crossing multiplied by current shares outstanding (a simplification that introduces modest error for historical crossings due to share issuance/buybacks).

**Statistical tests:** Mann-Whitney U test (non-parametric) and Welch's t-test for group comparisons. Significance levels: * p<0.10, ** p<0.05, *** p<0.01.

**Forward returns:** Measured from the week of the 200WMA crossing. The 3-month return uses the price 13 weeks later; 6-month uses 26 weeks; 12-month uses 52 weeks; 24-month uses 104 weeks.

## Download the Data

We're publishing the full dataset so you can verify these results, run your own analysis, or extend the framework with additional signals.

- [All 200WMA crossings (3,370 events)](/data/crossings_raw.csv) — ticker, crossing date, price at cross, 200WMA value, forward returns at 3/6/12/24 months, max drawdown
- [Crossings enriched with FCF data (1,182 events)](/data/crossings_with_fcf.csv) — adds free cash flow, FCF trend, FCF category classification
- [Crossings with FCF yield (1,182 events)](/data/crossings_with_yield.csv) — adds market cap and computed FCF yield at crossing
- [Current screener results (March 2026)](/data/current_below_200wma.csv) — S&P 500 stocks currently below their 200WMA with fundamentals

The CSVs are plain-text and open in any spreadsheet tool or pandas. If you find something interesting, we'd love to hear about it.

---

*This analysis is for informational purposes only and does not constitute investment advice. The author does not hold positions in any securities discussed. Past performance does not guarantee future results. Backtested results are inherently biased by survivorship, look-ahead, and data selection effects. Always do your own research before making investment decisions.*

*Framework methodology: [mungbeans.io 200-week moving average screening system](https://mungbeans.io)*
