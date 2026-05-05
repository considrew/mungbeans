# CLAUDE.md -- mungbeans.io Architecture Reference

> **Last updated**: 2026-05-02
> **Site**: https://mungbeans.io/
> **Repo root**: `/Users/considrew/Projects/Mungbeans/mungbeans/`
> **Hugo site root**: `/Users/considrew/Projects/Mungbeans/mungbeans/below-the-line/`

---

## Table of Contents

1. [File Tree](#file-tree)
2. [Data Pipeline](#data-pipeline)
3. [Stock Universe](#stock-universe)
4. [update_stocks.py -- Full Reference](#update_stockspy--full-reference)
5. [bean_score.py -- Full Reference](#bean_scorepy--full-reference)
6. [stocks.json Schema](#stocksjson-schema)
7. [Hugo Architecture](#hugo-architecture)
8. [Content Adapters](#content-adapters)
9. [Template Inventory](#template-inventory)
10. [CSS / Design System](#css--design-system)
11. [GitHub Actions](#github-actions)
12. [Deployment](#deployment)
13. [Email System](#email-system)
14. [Key Patterns](#key-patterns)
15. [Known Gotchas](#known-gotchas)

---

## File Tree

```
below-the-line/
├── hugo.toml                          # Hugo config
├── netlify.toml                       # Netlify deploy config
├── package.json                       # Node deps (Netlify Blobs, Functions, nodemailer)
├── README.md                          # Original readme (outdated -- references Alpha Vantage)
│
├── assets/
│   └── data/
│       ├── stocks.json                # PRIMARY DATA FILE (~8M tokens, all stock data)
│       ├── companies.json             # Ticker -> {name, sector, ir_url} reference
│       ├── bean_score_latest.json     # Most recent Bean Score snapshot
│       ├── bean_score_history.json    # Append-only weekly Bean Score history
│       ├── bean_score_alerts.json     # Bean Score >2sigma alert log
│       └── bean_score_progress.json   # Bean Score computation progress
│
├── content/
│   ├── about.md                       # /about/ page
│   ├── disclaimer.md                  # /disclaimer/ page
│   ├── privacy.md                     # /privacy/ page
│   ├── thanks.md                      # /thanks/ page (post-subscribe redirect)
│   ├── stocks/
│   │   ├── _index.md                  # Section index (title: "All Stocks")
│   │   └── _content.gotmpl           # CONTENT ADAPTER: generates stock pages from stocks.json
│   ├── blog/
│   │   ├── _index.md                  # Section index (title: "Weekly Signal Reports")
│   │   └── 2026-*-weekly-signal-report.md  # Auto-generated weekly posts
│   ├── deep-dives/
│   │   ├── _index.md                  # Section index
│   │   ├── how-to-use-the-line.md
│   │   ├── below-the-line-fcf-backtest.md
│   │   ├── *-deep-value-or-value-trap.md   # Individual stock deep dives
│   │   ├── *-stock-faceoff.md              # Head-to-head comparisons
│   │   └── *.md                            # Thematic pieces (uranium, strait of hormuz, etc.)
│   └── methodology/
│       └── micro-cap-innovation.md    # Micro cap assessment framework
│
├── layouts/
│   ├── _default/
│   │   ├── baseof.html               # BASE TEMPLATE: <html>, header nav, footer, content-page wrapper
│   │   └── single.html               # DEFAULT SINGLE: just {{ .Content }}
│   ├── index.html                     # HOMEPAGE: standalone (does NOT use baseof)
│   ├── stocks/
│   │   ├── list.html                  # ALL STOCKS PAGE: full table with JS filters/sort
│   │   └── single.html               # STOCK DETAIL PAGE: signal box, charts, prose, FAQ
│   ├── blog/
│   │   ├── list.html                  # BLOG LIST: reverse-date card layout
│   │   └── single.html               # BLOG POST: crossing cards, recovered grid
│   └── deep-dives/
│       ├── list.html                  # DEEP DIVES LIST: reverse-date cards with verdict badges
│       └── single.html               # DEEP DIVE POST: article with performance tracking
│
├── static/
│   ├── css/
│   │   └── style.css                  # MAIN STYLESHEET (1167 lines)
│   ├── favicon.svg
│   ├── data/                          # Static copies of bean score data
│   │   ├── bean_score_latest.json
│   │   ├── bean_score_history.json
│   │   └── bean_score_alerts.json
│   └── tools/
│       └── uranium-calculator.html    # Standalone tool
│
├── scripts/
│   ├── update_stocks.py               # MAIN DATA PIPELINE (~2050 lines)
│   ├── bean_score.py                  # Bean Score computation module (~505 lines)
│   ├── test_touches.py                # Test helpers
│   └── microcap/
│       ├── microcap_innovation_screener_v2.py
│       └── patent_data_downloader.py
│
├── data/
│   ├── patent_bulk/api_response.json
│   └── microcap_innovation/
│       ├── patent_lookup.json
│       └── screener_results.json
│
└── .netlify/state.json                # Netlify local state
```

---

## Data Pipeline

### Overview

```
yfinance API
     │
     ▼
scripts/update_stocks.py    (Python, runs weekly via GitHub Actions)
     │
     ├──► assets/data/stocks.json     (consumed by Hugo at build time)
     ├──► assets/data/companies.json  (read by pipeline for name/sector/IR URL)
     ├──► content/blog/*.md           (auto-generated weekly signal reports)
     │
     └──► scripts/bean_score.py       (called as submodule)
              │
              ├──► assets/data/bean_score_latest.json
              ├──► assets/data/bean_score_history.json
              └──► assets/data/bean_score_alerts.json

Hugo build
     │
     ├──► Reads assets/data/stocks.json via `resources.Get "data/stocks.json"`
     ├──► Content adapter (_content.gotmpl) generates ~2000 stock pages
     ├──► Templates render all pages
     └──► Output: public/ directory

Netlify
     │
     └──► Deploys public/ directory
```

### Pipeline Execution Order (in `main()`)

1. `load_company_metadata()` -- reads `companies.json` for name/sector/IR URL
2. `load_previous_stocks()` -- reads prior `stocks.json` for crossing detection
3. `fetch_spy_monthly()` -- one-time SPY benchmark fetch
4. For each ticker in `STOCK_UNIVERSE`:
   - `calculate_stock_signals(symbol, spy_monthly)` which calls:
     - `fetch_weekly_data(symbol)` -- yfinance weekly OHLCV
     - Calculates 200WMA, pct_from_wma, wow_change, RSI_14, volume indicators
     - `find_historical_touches(df)` -- episodes below 200WMA with hysteresis
     - `build_growth_chart(df, spy_monthly, touches)` -- $100 growth vs SPY
     - `build_touch_overlay_chart(df, spy_monthly, touches)` -- what happens after each touch
     - `fetch_fundamental_data(symbol)` -- yfinance info + balance sheet + cashflow
     - `fetch_insider_data(symbol)` -- SEC Form 4 via yfinance
5. `generate_landing_page_data(all_stocks)` -- summary counts and slimmed stock lists
6. Write `stocks.json`
7. `detect_crossings(current, previous)` -- find newly_below and newly_recovered
8. `generate_weekly_blog_post(crossings, date_str, content_dir)` -- markdown with frontmatter
9. `send_weekly_email(crossings, date_str)` -- via ZeptoMail API
10. `weekly_bean_score_snapshot(tickers)` -- Bean Score computation

---

## Stock Universe

**Location**: `scripts/update_stocks.py`, lines 73-278, variable `STOCK_UNIVERSE`

**Count**: ~2,060 tickers (array of strings, alphabetically sorted A through ZWS)

**Composition**: S&P 500 constituents + Berkshire Hathaway holdings + Dividend Aristocrats + additional mid/small-cap stocks of interest.

**How to add a stock**: Append the ticker string to `STOCK_UNIVERSE` in alphabetical position. Also add an entry to `assets/data/companies.json` with `name`, `sector`, and `ir_url` keys. The pipeline will pick it up on next run.

**Dividend Aristocrats list**: Separate `DIVIDEND_ARISTOCRATS` set (lines 62-70) containing ~68 tickers. Used to flag `dividend_aristocrat: true`.

---

## update_stocks.py -- Full Reference

### Classes

- **`NumpyEncoder(json.JSONEncoder)`**: Custom JSON encoder for numpy/pandas types (bool_, integer, floating, ndarray, Timestamp).

### Configuration

- `OUTPUT_DIR`: `<repo>/assets/data/`
- `COMPANIES_FILE`: `<repo>/assets/data/companies.json`

### Functions

#### Data Fetching

| Function | Signature | Returns | Description |
|---|---|---|---|
| `load_company_metadata` | `() -> dict` | `{symbol: {name, sector, ir_url}}` | Reads companies.json |
| `fetch_weekly_data` | `(symbol: str) -> Optional[pd.DataFrame]` | DataFrame with cols: open, high, low, close, volume, adjusted_close | Uses `yf.Ticker(symbol).history(period="max", interval="1wk")` |
| `fetch_fundamental_data` | `(symbol: str) -> dict` | Dict with ~35 keys (see below) | Fetches from yfinance `.info`, `.balance_sheet`, `.cashflow` |
| `fetch_insider_data` | `(symbol: str) -> dict` | Dict with insider_buys list + flags | SEC Form 4 via `yf.Ticker.insider_transactions` |
| `fetch_spy_monthly` | `() -> pd.Series` | Monthly SPY close prices | One-time fetch for benchmark |

#### Computation

| Function | Signature | Returns | Description |
|---|---|---|---|
| `calculate_rsi` | `(prices: pd.Series, periods=14) -> pd.Series` | RSI series | Standard RSI calculation |
| `find_historical_touches` | `(df: DataFrame, recovery_weeks=2) -> List[dict]` | List of touch episodes | Hysteresis-based: requires 2 consecutive weeks above to end episode |
| `get_share_change` | `(ticker) -> (yoy_change, three_yr_change, current_shares)` | Tuple of 3 | From balance sheet `Ordinary Shares Number` |
| `get_fcf_trend` | `(ticker) -> dict` | `{fcf_trend, fcf_cagr_3yr, fcf_consecutive_positive, fcf_history}` | Annual cashflow analysis |
| `get_health_metrics` | `(ticker) -> dict` | `{years, revenue, net_income, fcf, total_debt, roic, gross_margin, shares, fcf_yield}` | Annual time-series for dashboard charts |
| `build_growth_chart` | `(df, spy_monthly, touches) -> Optional[dict]` | `{start, s, b, t, stock_total_return, spy_total_return, ...}` | Normalized $100 growth chart data |
| `build_touch_overlay_chart` | `(df, spy_monthly, touches, months=24) -> Optional[dict]` | `{episodes, stock_avg, spy_avg, ...stats...}` | Overlay of all touch episodes |

#### Pipeline

| Function | Signature | Description |
|---|---|---|
| `calculate_stock_signals` | `(symbol, spy_monthly=None) -> Optional[dict]` | Master function: combines all data for one stock |
| `generate_landing_page_data` | `(stocks: List[dict]) -> dict` | Builds summary with counts and slimmed stock arrays |
| `load_previous_stocks` | `(output_dir) -> dict` | Reads prior stocks.json for crossing detection |
| `detect_crossings` | `(current_stocks, previous_stocks) -> dict` | Returns `{newly_below, newly_recovered}` |
| `generate_weekly_blog_post` | `(crossings, date_str, content_dir)` | Writes markdown file to content/blog/ |
| `_slim_stock` | `(stock: dict) -> dict` | Strips heavy fields (growth_chart, touch_chart, health_chart, historical_touches, insider_buys) |

#### Email

| Function | Signature | Description |
|---|---|---|
| `get_subscribers` | `() -> list[str]` | Fetches from Netlify Forms API minus unsubscribes from Netlify Blobs |
| `send_weekly_email` | `(crossings, date_str)` | Sends HTML email via ZeptoMail API to each subscriber individually |

### Zone Classification (in `calculate_stock_signals`)

| pct_from_wma | Zone |
|---|---|
| <= -10% | `extreme_value` |
| -10% to -5% | `deep_value` |
| -5% to 0% | `below_line` |
| 0% to 5% | `at_doorstep` |
| 5% to 10% | `getting_close` |
| 10% to 15% | `approaching` |
| > 15% | `above` |

### Volume Signal Classification

| Condition | Signal |
|---|---|
| rvol < 0.5 AND accum_ratio > 1.2 | `quiet_accumulation` |
| rvol > 2.0 AND week was down | `capitulation` |
| rvol > 2.0 AND week was up | `breakout_volume` |
| accum_ratio < 0.7 | `distribution` |
| accum_ratio > 1.5 | `accumulation` |
| else | `neutral` |

### Quality Flag Definitions

| Flag | Criteria |
|---|---|
| `yartseva_candidate` | market_cap < $2B AND positive_equity AND positive_fcf AND fcf_yield >= 5% AND book_to_market >= 0.4 |
| `buffett_quality` | ROE > 15% AND D/E < 50% AND positive_fcf AND profit_margin > 0 |
| `wide_moat` | gross_margin > 40% AND ROE > 15% |
| `is_cannibal` | shares_change_3yr < -5% |
| `is_buying_back` | shares_change_3yr < -2% |
| `is_diluting` | shares_change_3yr > 2% |
| `dividend_aristocrat` | ticker in DIVIDEND_ARISTOCRATS set |

### Environment Variables

| Variable | Used By | Purpose |
|---|---|---|
| `SKIP_BLOG` | main() | Set to "true" to skip blog post generation (manual runs) |
| `SKIP_EMAIL` | main() | Set to "true" to skip email sending |
| `SKIP_BEAN_SCORE` | main() | Set to "true" to skip Bean Score computation |
| `ZOHO_EMAIL` | send_weekly_email | Sender email address |
| `ZEPTOMAIL_API_TOKEN` | send_weekly_email | ZeptoMail API auth token |
| `NETLIFY_API_TOKEN` | get_subscribers | Netlify API for form submissions |
| `NETLIFY_SITE_ID` | get_subscribers | Netlify site for form lookups |

---

## bean_score.py -- Full Reference

### Concept

Bean Score measures how far a stock's FCF yield has deviated from its quarterly baseline, normalized by the stock's own historical intra-quarter deviation sigma. It is a z-score: positive = unusually cheap, negative = unusually expensive.

### Classes

- **`NumpyEncoder`**: Same as update_stocks.py (handles numpy/pandas types)

### Constants

- `DATA_DIR`: `<repo>/assets/data/`
- `HISTORY_FILE`: `bean_score_history.json`
- `LATEST_FILE`: `bean_score_latest.json`
- `ALERTS_FILE`: `bean_score_alerts.json`
- `MAX_ABS_YIELD`: 100.0% (data quality gate)
- `MAX_HIST_STD`: 20.0pp (data quality gate)

### Functions

| Function | Signature | Returns |
|---|---|---|
| `_get_quarterly_fcf` | `(ticker_obj) -> Optional[pd.Series]` | Quarterly FCF series (tries multiple field names, fallback to OCF - CapEx) |
| `_get_annual_fcf` | `(ticker_obj) -> Optional[pd.Series]` | Annual FCF series |
| `compute_bean_score` | `(ticker: str, verbose=False) -> Optional[dict]` | Single ticker score dict (see below) |
| `compute_bean_scores_batch` | `(tickers: list[str], verbose=False) -> list[dict]` | Batch computation |
| `weekly_bean_score_snapshot` | `(tickers: list[str], verbose=False) -> dict` | Full snapshot + persist to disk |
| `load_latest_scores` | `() -> Optional[dict]` | Read bean_score_latest.json |
| `load_score_history` | `() -> list[dict]` | Read bean_score_history.json |
| `load_alerts` | `() -> list[dict]` | Read bean_score_alerts.json |

### Bean Score Output Fields (per ticker)

```
ticker, bean_score, current_fcf_yield, baseline_fcf_yield,
deviation_pp, hist_dev_mean, hist_dev_std, ttm_fcf, n_quarters,
n_observations, last_report_date, percentile, velocity_4w,
velocity_13w, sector, computed_at
```

### Alert Threshold

Tickers with `abs(bean_score) >= 2.0` are logged. Direction: "CHEAP" if positive, "EXPENSIVE" if negative.

### Persistence Files

- **bean_score_latest.json**: `{date, n_scored, n_attempted, scores: {ticker: full_score_dict}}`
- **bean_score_history.json**: Array of compact weekly entries: `{date, scores: {ticker: {bean_score, current_fcf_yield, baseline_fcf_yield, deviation_pp, hist_dev_std, velocity_13w}}}`
- **bean_score_alerts.json**: Array of alert objects: `{date, ticker, bean_score, direction, current_fcf_yield, baseline_fcf_yield, deviation_pp, hist_dev_std, sector}`

---

## stocks.json Schema

### Top Level

```json
{
  "summary": { ... },        // Landing page summary data
  "stocks": [ ... ],          // Array of all stock objects
  "generated_readable": "May 02, 2026",
  "generated_iso": "2026-05-02"
}
```

### summary Object

```
total_stocks, below_line_count, approaching_count, oversold_count,
yartseva_count, buffett_count, aristocrat_count, cannibal_count,
diluting_count, insider_buying_count, insider_below_count,
fcf_growing_below_count,

below_line_stocks: [slimmed stock objects],
approaching_stocks: [slimmed, max 20, sorted by proximity],
insider_buying_stocks: [slimmed, sorted by largest_buy_value],
insider_below_stocks: [slimmed],
fcf_growing_below_stocks: [slimmed, sorted by fcf_cagr_3yr]
```

Slimmed stocks exclude: `growth_chart`, `touch_chart`, `health_chart`, `historical_touches`, `insider_buys`.

### Stock Object (Full -- per entry in stocks array)

#### Core Signal Fields
```
symbol: str                   # Ticker symbol
name: str                     # Company name (from companies.json)
sector: str                   # Sector (from companies.json)
ir_url: str                   # Investor relations URL
close: float                  # Latest adjusted close price
wma_200: float                # 200-week moving average
buy_threshold: float          # Same as wma_200
pct_from_wma: float           # % distance (negative = below)
wow_change: float             # Week-over-week change in pct_from_wma
rsi_14: float                 # 14-week RSI
below_line: bool              # True if close < wma_200
approaching: bool             # True if wow_change < 0
zone: str                     # One of: extreme_value, deep_value, below_line, at_doorstep, getting_close, approaching, above
```

#### Volume Indicators
```
rvol_14: float|null           # Relative volume (current week / 14-week avg)
accumulation_ratio: float|null # Avg up-week volume / avg down-week volume (14w)
volume_signal: str            # One of: quiet_accumulation, capitulation, breakout_volume, distribution, accumulation, neutral
```

#### Historical Touch Data
```
historical_touches: [         # Array of episode dicts
  {
    date: str,                # "Mon YYYY"
    date_iso: str,            # "YYYY-MM-DD"
    recovery_date: str|null,  # "Mon YYYY" or null if ongoing
    weeks_below: int,
    max_depth: float,         # % below WMA at deepest point
    return_1yr: float|null,   # 1-year return from episode start
    return_to_now: float|null,# Return from episode start to current price
    ongoing: bool
  }
]
touch_count: int              # len(historical_touches)
avg_return_after_touch: float|null
avg_weeks_below: float|null
data_weeks: int               # Total weeks of price data available
last_updated: str             # "YYYY-MM-DD" (Friday close date)
```

#### Fundamental Fields
```
market_cap: int|null
fcf: int|null                 # Raw free cash flow
fcf_yield: float|null         # FCF / market_cap * 100
book_value: float|null
price_to_book: float|null
book_to_market: float|null
profit_margin: float|null     # As percentage
operating_margin: float|null
revenue: int|null
roe: float|null               # As percentage
debt_to_equity: float|null    # As percentage
gross_margin: float|null      # As percentage
current_ratio: float|null
dividend_yield: float|null    # As percentage
```

#### Share Buyback / Dilution
```
shares_outstanding: int|null
shares_change_yoy: float|null   # % change
shares_change_3yr: float|null   # % change (negative = buybacks)
```

#### Quality Flags (all bool)
```
is_small_cap, has_positive_fcf, low_debt, high_roe, wide_moat,
buffett_quality, dividend_aristocrat, yartseva_candidate,
is_buying_back, is_diluting, is_cannibal
```

#### Combo Flags (quality + below line, all bool)
```
yartseva_below_line, buffett_below_line, aristocrat_below_line,
cannibal_below_line, insider_below_line, fcf_growing_below_line
```

#### Insider Buying
```
insider_buys: [               # Array of conviction purchases
  {
    name: str,
    title: str,
    date: str,                # "YYYY-MM-DD"
    shares: int,
    value: int,               # Dollar value
    pct_position_increase: float|null
  }
]
has_conviction_buy: bool
has_cluster_buy: bool         # 3+ insiders within 30 days
largest_buy_value: int|null
insider_buy_count_12m: int
insider_buy_total_12m: int
```

#### FCF Trend
```
fcf_trend: str                # "growing" | "declining" | "volatile" | "insufficient_data"
fcf_cagr_3yr: float|null     # Compound annual growth rate %
fcf_consecutive_positive: int # Years of positive FCF
```

#### Chart Data
```
growth_chart: {               # Stock vs SPY normalized growth
  start: str,                 # "YYYY-MM"
  s: [int],                   # Stock growth values ($100 base)
  b: [int],                   # SPY benchmark values
  t: [int],                   # Monthly indices where touches occurred
  stock_total_return: float,
  spy_total_return: float,
  stock_annual_return: float,
  spy_annual_return: float,
  years: float,
  beats_spy: bool
}

touch_chart: {                # What happens after each 200WMA touch
  episodes: [{date, s: [int], months}],  # Up to 10 most recent
  stock_avg: [int],           # Average across all episodes
  spy_avg: [int],
  total_episodes: int,
  episodes_shown: int,
  avg_return_12m, median_return_12m, pct_positive_12m,
  avg_return_24m, median_return_24m, pct_positive_24m,
  spy_avg_return_12m, spy_avg_return_24m
}

health_chart: {               # Business health dashboard
  years: [int],               # Fiscal years
  revenue: [float|null],      # In millions
  net_income: [float|null],
  fcf: [float|null],
  total_debt: [float|null],
  roic: [float|null],         # Percentage
  gross_margin: [float|null], # Percentage
  shares: [float|null],       # In millions
  fcf_yield: [float|null]     # Percentage
}
```

---

## Hugo Architecture

### Configuration (hugo.toml)

```toml
baseURL = 'https://mungbeans.io/'
languageCode = 'en-us'
title = 'mungbeans.io - 200-Week Moving Average Stock Signals'

[params]
  description = "Track when quality stocks drop below their 200-week moving average."
  author = "mungbeans.io"

[markup.goldmark.renderer]
  unsafe = true                     # Allows raw HTML in markdown

[outputs]
  home = ['HTML', 'RSS', 'JSON']    # Homepage outputs all three
  section = ['HTML', 'RSS']
  page = ['HTML']

[sitemap]
  changefreq = 'weekly'
  priority = 0.5

[privacy.googleAnalytics]
  disable = false
  anonymizeIP = true
  respectDoNotTrack = true

[build]
  writeStats = true                 # Generates hugo_stats.json
```

### Content Sections

| Section | Path | Content Adapter | Template |
|---|---|---|---|
| Stocks | `/stocks/` | `_content.gotmpl` generates pages from stocks.json | `stocks/list.html`, `stocks/single.html` |
| Blog | `/blog/` | Regular markdown files | `blog/list.html`, `blog/single.html` |
| Deep Dives | `/deep-dives/` | Regular markdown files | `deep-dives/list.html`, `deep-dives/single.html` |
| Static pages | `/about/`, `/disclaimer/`, etc. | Regular markdown | `_default/baseof.html` + `_default/single.html` |
| Methodology | `/methodology/` | Regular markdown | `_default/baseof.html` + `_default/single.html` |

### Data Flow into Templates

The homepage (`layouts/index.html`) and stock list (`layouts/stocks/list.html`) both read stocks.json directly:

```go
{{ $data := dict }}
{{ with resources.Get "data/stocks.json" }}
  {{ with . | transform.Unmarshal }}
    {{ $data = . }}
  {{ end }}
{{ end }}
```

Stock single pages get their data via the content adapter, which maps JSON fields to `.Params.*`.

---

## Content Adapters

### content/stocks/_content.gotmpl

This is the only content adapter. It reads `assets/data/stocks.json` and generates one page per stock.

**Key mechanics**:
- Reads `resources.Get "data/stocks.json"` and unmarshals
- Iterates over `$data.stocks`
- Creates pages with `kind: "page"`, `path: <symbol lowercase>`, `title: "<Name> (<SYM>) - Below The Line"`
- All stock data fields are mapped to `params` (see the full mapping in the file at `content/stocks/_content.gotmpl`)

**Fields mapped to params** (exhaustive list):
```
symbol, name, sector, ir_url, close, wma_200, buy_threshold,
pct_from_wma, wow_change, rsi_14, rvol_14, accumulation_ratio,
volume_signal, below_line, approaching, zone, historical_touches,
touch_count, avg_return_after_touch, avg_weeks_below, last_updated,
data_weeks, market_cap, fcf_yield, price_to_book, roe,
debt_to_equity, gross_margin, profit_margin, dividend_yield,
dividend_aristocrat, buffett_quality, yartseva_candidate,
is_cannibal, is_buying_back, is_diluting, shares_change_3yr,
has_positive_fcf, growth_chart (+ derived: growth_years, growth_beats_spy,
growth_stock_total, growth_spy_total, growth_stock_annual, growth_spy_annual),
touch_chart (+ derived: touch_total_episodes, touch_avg_return_12m,
touch_median_return_12m, touch_pct_positive_12m, touch_avg_return_24m,
touch_pct_positive_24m, touch_spy_avg_return_12m, touch_spy_avg_return_24m),
insider_buys, has_conviction_buy, has_cluster_buy, largest_buy_value,
insider_buy_count_12m, insider_buy_total_12m, insider_below_line,
fcf_trend, fcf_cagr_3yr, fcf_consecutive_positive,
fcf_growing_below_line, health_chart
```

---

## Template Inventory

### layouts/_default/baseof.html
- **Used by**: About, Disclaimer, Privacy, Thanks, Methodology, and any other content using `_default/single.html`
- **Features**: Google Analytics (G-KMCNBWBVVE), Google AdSense (ca-pub-5600315940410875), nav bar, footer
- **Nav links**: Home, All Stocks, Weekly Reports, Deep Dives, About
- **Inline CSS**: `.content-page` styles (max-width 700px), blockquote, table, hr

### layouts/_default/single.html
- **Minimal**: Just `{{ define "main" }} {{ .Content }} {{ end }}`
- **Extends**: baseof.html

### layouts/index.html (Homepage)
- **STANDALONE** (does not extend baseof.html -- has its own full HTML structure)
- **Reads**: `assets/data/stocks.json` via `resources.Get`
- **Sections displayed**:
  1. Banner image + hero stats (Actionable count, Approaching count, Tracked count)
  2. Email signup form (Netlify form named "notify")
  3. Ad placement (leaderboard)
  4. Below The Line stocks (first 10 of quality picks, -5% to -50%)
  5. Deep Value (first 10, -50% to -70%)
  6. The Waiting Room (first 10 approaching, within 15%)
  7. Oversold + Below Line (RSI < 30 from quality picks)
  8. Insider Buying + Below Line
  9. Growing Cash Flow + Below Line
  10. Distressed (>70% below, in collapsible `<details>`)
  11. Methodology explanation
  12. FAQ section (6 questions with JSON-LD FAQPage schema)
  13. All Stocks CTA button
- **External scripts**: Buy Me a Coffee widget (data-id="drewc")
- **SEO**: Open Graph, Twitter Card, JSON-LD WebSite + FAQPage schemas

### layouts/stocks/list.html (All Stocks)
- **STANDALONE** (own HTML structure)
- **Reads**: stocks.json directly
- **Features**:
  - Search box (debounced, 200ms)
  - Filter groups:
    - Signal: All, Below Line, Quality Zone, Approaching, Oversold, Deep Value, Distressed, Recovering
    - Quality: All, S&P 500, Proven Bouncers, Low Debt, High ROE
    - Cross History: All, First Time, Rare (1-2), Frequent (5+)
    - Special (multi-select): Buffett Quality, Multibagger, Aristocrats, Wide Moat, Cannibals, Diluters, Insider Buying, FCF Growing, Quiet Accumulation
    - Sector: dropdown populated from data
  - Info boxes (hidden by default, shown when special filter is active)
  - Sortable table with columns: Symbol, Signal, Distance, Direction, RSI, Touches, Avg Rtn, ROE, D/E, Shares, FCF%, RVOL, Accum., Sector
  - Expandable detail rows (lazy-built on first click via JS)
  - URL parameter support: `?filter=below`, `?filter=buffett`, etc.
  - S&P 500 constituent list hardcoded in JS
  - Affiliate links: Robinhood, Webull, Interactive Brokers
- **Data attributes on rows**: 30+ data-* attributes for filtering/sorting

### layouts/stocks/single.html (Stock Detail)
- **STANDALONE** (own HTML structure)
- **External JS**: Chart.js v4 + chartjs-plugin-annotation v3 (loaded via CDN, deferred)
- **Sections**:
  1. Breadcrumb (Home > Stocks > SYMBOL)
  2. Company info (name, sector, IR link)
  3. Signal box (YES/NO, % above/below, direction)
  4. Proximity bar (gradient bar with marker)
  5. Key metrics (Buy Threshold, RSI, Rel. Volume, Buyers vs Sellers)
  6. Stock analysis prose (7 auto-generated paragraphs covering signal, volume, history, fundamentals, quality flags, growth vs SPY, insider buying, FCF trend)
  7. Business Health Dashboard (7 Chart.js bar charts: Cash Flow, Revenue, Debt, ROIC, FCF Yield, Gross Margin, Shares Outstanding)
  8. Growth of $100 chart (line chart: stock vs S&P 500 with touch annotations)
  9. Touch overlay chart (line chart: all episodes normalized to $100)
  10. Ad placement
  11. Insider buying table (if has_conviction_buy)
  12. Historical touches table
  13. FAQ section (dynamic, 4-6 questions with JSON-LD)
  14. Disclaimer
- **SEO**: Full OG, Twitter Card, JSON-LD WebPage + BreadcrumbList + FinancialProduct + FAQPage

### layouts/blog/list.html
- Uses baseof-style structure (own HTML)
- Lists blog posts in reverse date order as cards
- Each card shows: date, title, description, newly_below_count badge, newly_recovered_count badge

### layouts/blog/single.html
- Full article template with:
  - Breadcrumb
  - Post header with summary stats badges
  - Markdown content (intro paragraphs)
  - Theory primer (collapsible, explains deep value territory)
  - Crossing cards for each newly_below stock (with header, historical context, quality flags, things to watch, CTA link)
  - Recovered grid for newly_recovered stocks
  - Post navigation (prev/next)
- **SEO**: OG, Twitter Card, JSON-LD Article + BreadcrumbList

### layouts/deep-dives/list.html
- Lists deep dive posts in reverse date order
- Cards show: date, title, description, ticker badge, verdict badge, performance_since badges

### layouts/deep-dives/single.html
- Article template with:
  - Breadcrumb
  - Post header with ticker/verdict/performance badges
  - Performance tracking (price at publish -> current price, % change)
  - Markdown content
  - Post navigation

---

## CSS / Design System

**File**: `static/css/style.css` (1167 lines)

### CSS Variables (`:root`)

#### Colors
```css
--color-bg: #0d1117;            /* Dark background */
--color-surface: #161b22;       /* Card/panel background */
--color-border: #30363d;        /* Borders */
--color-text: #e6edf3;          /* Primary text */
--color-text-muted: #8b949e;    /* Secondary text */
--color-link: #58a6ff;          /* Links */
```

#### Signal Colors
```css
--color-yes: #238636;           /* Below-line signal (green) */
--color-yes-bg: rgba(35, 134, 54, 0.15);
--color-no: #8b949e;            /* Above-line signal (gray) */
--color-approaching: #d29922;   /* Approaching/warning (amber) */
--color-retreating: #8b949e;
```

#### Zone Colors (gradient from above to below)
```css
--zone-above: #30363d;
--zone-approaching: #9e6a03;
--zone-getting-close: #bb8009;
--zone-at-doorstep: #d29922;
--zone-below-line: #238636;
--zone-deep-value: #2ea043;
--zone-extreme-value: #3fb950;
```

#### Typography
```css
--font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
--font-mono: 'SF Mono', Consolas, 'Liberation Mono', Menlo, monospace;
```

#### Spacing
```css
--space-xs: 0.25rem;
--space-sm: 0.5rem;
--space-md: 1rem;
--space-lg: 1.5rem;
--space-xl: 2rem;
--space-2xl: 3rem;
```

#### Border Radius
```css
--radius-sm: 4px;
--radius-md: 8px;
--radius-lg: 12px;
```

### Additional Color Conventions (used in template inline styles)

```
#f85149     -- Negative returns, errors, red
#3fb950     -- Positive returns, green variant
#2563eb     -- Primary blue (charts, links in list)
#059669     -- Buffett quality green
#d97706     -- Yartseva amber
#7c3aed     -- Aristocrat purple
#0ea5e9     -- Wide moat blue
#06b6d4     -- Cannibal teal
#f87171     -- Diluter red
#8b5cf6     -- Insider purple
#10b981     -- FCF growing green
#a855f7     -- Gross margin purple (charts)
```

### Key Component Patterns

- **`.stock-card`**: Grid card for homepage/section lists. Gets zone-based border color via `.zone-below_line`, `.zone-deep_value`, `.zone-extreme_value`.
- **`.signal-box`**: Large YES/NO display on stock detail pages. Gets zone-based border/bg.
- **`.proximity-bar`**: Gradient bar showing position relative to 200WMA.
- **`.metric`**: Key metric card (buy threshold, RSI, etc.)
- **`.touches-table`**: Historical episodes table.
- **`.crossing-card`**: Blog post card for newly-below stocks (left green border).
- **`.recovered-card`**: Grid card for recovered stocks (left blue border).
- **`.health-chart-wrap`**: Chart container for business health dashboard.
- **`.stock-grid`**: `grid-template-columns: repeat(auto-fill, minmax(180px, 1fr))`
- **`.health-grid`**: `grid-template-columns: repeat(2, 1fr)` (1 on mobile)

### Responsive Breakpoints

- **600px**: Hero stats stack vertically, metrics grid to 1 column, stock grid to 2 columns, detail grid to 1 column, hide direction column and stock names in table
- **900px**: Hide mobile-only table columns (D/E, Shares, FCF%, RVOL, Accum.)

---

## GitHub Actions

### Weekly Stock Data Update and Deploy
**File**: `.github/workflows/build-deploy.yml`

**Schedule**: `0 6 * * 6` (6am UTC every Saturday)
**Also**: `workflow_dispatch` (manual trigger)

**Steps**:
1. Checkout repository (fetch-depth: 0)
2. Pull latest changes
3. Setup Python 3.11
4. Install Python deps: `pandas yfinance`
5. Run `cd below-the-line && python scripts/update_stocks.py`
   - Env vars: SKIP_BLOG, SKIP_EMAIL, ZOHO_EMAIL, ZEPTOMAIL_API_TOKEN, NETLIFY_API_TOKEN, NETLIFY_SITE_ID
   - On `workflow_dispatch`: SKIP_BLOG=true, SKIP_EMAIL=true
6. Update deep-dive performance data (continue-on-error: true)
7. Commit and push: stocks.json, bean_score files, blog posts, deep-dives
8. Setup Hugo (latest, extended)
9. Install Node dependencies
10. Build: `hugo --minify`
11. Deploy to Netlify via `nwtgck/actions-netlify@v3`

### Micro Cap Innovation Screener
**File**: `.github/workflows/microcap-screener.yml`

**Trigger**: Runs after Weekly Stock Data workflow completes (or manual)
**Steps**: Checkout, Python 3.11, install yfinance, run screener script, commit results

---

## Deployment

### Netlify Configuration (netlify.toml)
```toml
[build]
  command = "hugo --minify"
  publish = "public"

[build.environment]
  HUGO_VERSION = "0.139.0"
```

### Node Dependencies (package.json)
```json
{
  "dependencies": {
    "@netlify/blobs": "^8.0.0",      // For unsubscribe storage
    "@netlify/functions": "^3.0.0",   // Serverless functions
    "nodemailer": "^6.9.0"            // Email (may be legacy, ZeptoMail used now)
  }
}
```

### Netlify Features Used
- **Netlify Forms**: Email signup form (name: "notify") with honeypot spam protection
- **Netlify Blobs**: Stores unsubscribed email addresses (key = email)
- **Static hosting**: Serves Hugo `public/` directory

### Third-Party Services
- **Google Analytics**: G-KMCNBWBVVE
- **Google AdSense**: ca-pub-5600315940410875
- **ZeptoMail**: Email delivery (api.zeptomail.com)
- **Chart.js v4**: Client-side charts on stock detail pages
- **chartjs-plugin-annotation v3**: Reference lines on charts
- **Buy Me a Coffee**: Widget (data-id="drewc")

---

## Email System

### Subscription Flow
1. User submits email via Netlify form (name="notify") on homepage
2. Redirected to `/thanks/` page
3. Email stored by Netlify Forms

### Weekly Email Flow
1. `get_subscribers()` fetches all submissions from Netlify Forms API (paginated)
2. Checks Netlify Blobs "unsubscribes" store for opt-outs
3. Returns `emails - unsubscribed`
4. `send_weekly_email()` sends individually to each subscriber via ZeptoMail API
5. Each email has a personalized unsubscribe link: `https://mungbeans.io/.netlify/functions/unsubscribe?email=...`
6. Rate-limited: 1-second pause every 10 emails

### Email Content
- Dark-themed HTML email
- Table of newly-below stocks (symbol, name, % below, RSI)
- Table of newly-recovered stocks
- CTA button to full blog post
- Unsubscribe link in footer

---

## Key Patterns

### How to Add a New Content Section

1. Create `content/<section>/_index.md` with frontmatter (title, description)
2. Create `layouts/<section>/list.html` for the section index page
3. Create `layouts/<section>/single.html` for individual pages
4. Add nav link in `layouts/_default/baseof.html` and any standalone templates (index.html, stocks/list.html, stocks/single.html, blog templates, deep-dives templates)
5. Note: baseof.html nav must be updated separately from standalone templates since they duplicate the nav

### How to Add a New Data-Driven Section (like Stocks)

1. Add a Python script that outputs JSON to `assets/data/<name>.json`
2. Create `content/<section>/_content.gotmpl` content adapter
3. In the adapter, read with `resources.Get "data/<name>.json"` and call `$.AddPage` for each entry
4. Create templates for list and single views
5. Add the script to the GitHub Actions workflow
6. Add the JSON file to the git add step in the workflow

### How Stock Pages Are Generated

Stock pages are NOT markdown files. They are generated at Hugo build time by the content adapter `content/stocks/_content.gotmpl`. The adapter reads `assets/data/stocks.json` and creates virtual pages. Each page gets all its data via `.Params.*` (not `.Content`).

### Template Inheritance

Most templates are STANDALONE (have their own full HTML structure with `<!DOCTYPE html>`, `<head>`, `<body>`). Only the `_default/` templates use Hugo's baseof pattern. This means nav bar changes must be propagated to ALL templates manually. The nav appears in:
- `layouts/_default/baseof.html`
- `layouts/index.html`
- `layouts/stocks/list.html`
- `layouts/stocks/single.html`
- `layouts/blog/list.html`
- `layouts/blog/single.html`
- `layouts/deep-dives/list.html`
- `layouts/deep-dives/single.html`

### Weekly Blog Post Generation

The Python pipeline auto-generates markdown files in `content/blog/`. Each file has structured frontmatter with `newly_below` and `newly_recovered` arrays. The blog/single.html template renders crossing cards from this frontmatter data -- the markdown body is just an intro paragraph.

---

## Known Gotchas

### Hugo-Specific

1. **Content adapter pages have no `.Content`**: Stock pages are generated by `_content.gotmpl`. All data is in `.Params.*`, not `.Content`. The `_default/single.html` just renders `{{ .Content }}` which would be empty for stock pages (they use `stocks/single.html` instead).

2. **stocks.json is massive**: ~8M tokens. Hugo reads it at build time via `resources.Get`. It cannot be read with normal file tools. The `_content.gotmpl` iterates over every stock and creates a page.

3. **Standalone vs baseof templates**: The homepage, stock pages, and blog pages are all standalone HTML documents. They do NOT use baseof.html. Any changes to the header, footer, or analytics tags must be replicated across 8 template files.

4. **Zone CSS classes use underscores**: Zone values like `below_line` become CSS classes `.zone-below_line` (with underscore, not hyphen). The CSS must match.

5. **`math.Abs` in Hugo templates**: Used throughout for displaying distance below the line as a positive number. It is a Hugo built-in function.

### yfinance-Specific

6. **Rate limiting**: yfinance calls Yahoo Finance. Processing ~2060 stocks takes significant time. The pipeline adds 1-second pauses every 50 stocks.

7. **Field name inconsistency**: yfinance cashflow field names vary between versions (`Free Cash Flow` vs `FreeCashFlow`, `Operating Cash Flow` vs `OperatingCashFlow`). The code tries multiple names with fallbacks.

8. **Weekly candle dating**: yfinance labels weekly candles by Monday. The pipeline adjusts to show Friday close date: `(index + DateOffset(days=(4 - weekday) % 7))`.

9. **Share count data**: Can be unreliable for ADRs and micro-caps. Bean Score rejects stocks with `|FCF yield| > 100%` or `hist sigma > 20pp` as data quality gates.

10. **Timezone handling**: yfinance returns tz-aware timestamps. The code does `tz_localize(None)` in bean_score.py and handles mixed tz-aware/naive comparisons in update_stocks.py.

### Pipeline-Specific

11. **Previous week comparison**: The pipeline loads the previous `stocks.json` before overwriting it to detect crossings. If the file is missing (first run), blog generation is skipped.

12. **SKIP_BLOG and SKIP_EMAIL**: Set automatically on `workflow_dispatch` (manual) runs to avoid generating duplicate blog posts or sending test emails.

13. **companies.json is a separate file**: Company names, sectors, and IR URLs are maintained separately from the pipeline output. If a new ticker is added to `STOCK_UNIVERSE` but not to `companies.json`, it will appear with empty name/sector/IR URL.

14. **Bean Score imports**: The pipeline does `from bean_score import weekly_bean_score_snapshot` (relative import from the scripts/ directory), so bean_score.py must be in the same directory.

### Deployment-Specific

15. **Hugo version mismatch**: netlify.toml specifies `HUGO_VERSION = "0.139.0"`. The GitHub Actions workflow uses `hugo-version: 'latest'`. These could diverge.

16. **Netlify Forms require deploy**: The `data-netlify="true"` attribute on the signup form only works when deployed to Netlify. Local development won't process form submissions.

17. **Bean Score data duplication**: Bean Score JSON files exist in both `assets/data/` (for Hugo build) and `static/data/` (for direct web access). Updates go to `assets/data/` via the Python script.
