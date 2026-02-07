---
title: "About"
description: "About mungbeans.io and the 200-week moving average methodology."
---

# About mungbeans.io

## The Idea

There's a quote often attributed to Charlie Munger:

> "If all you ever did was buy high-quality stocks on the 200-week moving average, you would beat the S&P 500 by a large margin over time. The problem is, few human beings have that kind of discipline."

Whether or not Munger actually said this, the concept is sound: the 200-week moving average represents roughly 4 years of price history. When a quality company's stock drops to this level, it's often a rare event—the kind of opportunity that might come along only a handful of times per decade for any given stock.

## What This Tool Does

mungbeans.io tracks over 1,600 stocks against their 200-week moving average and answers one simple question for each:

**Is it below the line? Yes or no.**

Beyond that core signal, we provide a range of metrics and screens to help you evaluate what you're looking at:

- **Distance & Direction**: How far from the line, and whether the stock is recovering or deepening this week
- **14-Week RSI**: A momentum gauge for oversold conditions
- **Historical Touches**: Every time this stock has crossed below the 200-week line, and what happened afterward
- **Quality Screens**: Buffett Quality, Wide Moat, Dividend Aristocrats, Yartseva Multibagger candidates
- **Cash Flow Analysis**: Free cash flow yield, FCF trends, and growing-FCF-while-below-line signals
- **Insider Activity**: Conviction insider purchases (open-market buys over $500K)
- **Share Count Tracking**: Cannibals (aggressive buybacks) vs. Diluters (growing share count)

## What This Tool Doesn't Do

- We don't tell you what to buy
- We don't predict future performance
- We don't provide real-time data (updated weekly)

A stock being below its 200-week average could mean opportunity—or it could mean the business is deteriorating. **Always do your own research.**

## The Signals

### Distance from 200WMA

| Zone | Description |
|------|-------------|
| 15%+ above | Far from the line |
| 5–15% above | Approaching range |
| 0–5% above | At the doorstep |
| 0–50% below | **Below the line** |
| 50–70% below | Deep value territory |
| 70%+ below | Distressed |

### Direction

The direction indicator is context-aware:

- **↑ Recovering**: Below the line and moved up this week (heading back toward the line)
- **↓ Deepening**: Below the line and dropped further this week
- **↓ Approaching**: Above the line and moving toward it
- **↑ Away**: Above the line and moving further above

### 14-Week RSI

The Relative Strength Index on weekly data:

- **Below 30**: Oversold (short-term selling may be exhausted)
- **Below 20**: Extremely oversold (rare)
- **Above 70**: Overbought

## Data Sources

- **Stock prices**: [yfinance](https://github.com/ranaroussi/yfinance) (weekly adjusted close)
- **Insider transactions**: SEC Form 4 filings
- **Update schedule**: Weekly (typically Saturday mornings, based on Friday close data)
- **Stocks tracked**: 1,600+ names across all sectors, including S&P 500 constituents, Berkshire Hathaway holdings, Dividend Aristocrats, and additional mid/small-cap stocks

## Built With

This project is a static site built with [Hugo](https://gohugo.io/), with data processing in Python and interactive charts using [Chart.js](https://www.chartjs.org/). Data updates are automated via GitHub Actions and the site is deployed on [Netlify](https://www.netlify.com/).

---

*This is an educational tool, not investment advice. See our [Disclaimer](/disclaimer/) for full details.*
