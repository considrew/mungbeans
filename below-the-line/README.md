# Below The Line

A simple stock screening tool that tracks when quality stocks drop below their 200-week moving average.

## Quick Start

### Prerequisites

- [Hugo](https://gohugo.io/installation/) (extended version)
- Python 3.9+
- [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) (free tier works)

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/below-the-line.git
   cd below-the-line
   ```

2. **Install Python dependencies**
   ```bash
   pip install pandas requests
   ```

3. **Set your API key**
   ```bash
   export ALPHA_VANTAGE_KEY="4RJ3Q220NSRGS26P"
   ```

4. **Update stock data** (optional - sample data included)
   ```bash
   python scripts/update_stocks.py
   ```

5. **Run Hugo dev server**
   ```bash
   hugo server -D
   ```

6. **Open** http://localhost:1313

## Project Structure

```
below-the-line/
├── .github/workflows/    # GitHub Actions for weekly builds
├── assets/data/          # Stock data JSON (generated)
├── content/
│   ├── stocks/           # Stock pages (generated from JSON)
│   ├── about.md
│   └── disclaimer.md
├── layouts/
│   ├── _default/         # Base templates
│   ├── stocks/           # Stock page templates
│   └── index.html        # Homepage
├── scripts/
│   └── update_stocks.py  # Data pipeline
├── static/css/           # Stylesheets
└── hugo.toml             # Hugo configuration
```

## Data Pipeline

The `scripts/update_stocks.py` script:

1. Fetches weekly price data from Alpha Vantage
2. Calculates 200-week moving average
3. Calculates 14-week RSI
4. Detects historical touches of the 200WMA
5. Outputs `assets/data/stocks.json`

Run it weekly (Saturday recommended) to get Friday close data.

### Rate Limits

Alpha Vantage free tier: 25 calls/day, 5 calls/minute

The script processes ~50 stocks with 12-second delays between calls. For more stocks:
- Run across multiple days (the script can be modified to resume)
- Upgrade to Alpha Vantage premium ($50/month)

## Deployment

### Option 1: Cloudflare Pages (Recommended)

1. Create a Cloudflare Pages project
2. Add repository secrets in GitHub:
   - `ALPHA_VANTAGE_KEY`
   - `CLOUDFLARE_API_TOKEN`
   - `CLOUDFLARE_ACCOUNT_ID`
3. Push to main branch
4. GitHub Actions will build and deploy weekly

### Option 2: GitHub Pages

1. Uncomment the GitHub Pages deployment step in `.github/workflows/build-deploy.yml`
2. Comment out the Cloudflare deployment step
3. Enable GitHub Pages in repository settings
4. Set source to "GitHub Actions"

### Option 3: Netlify

1. Uncomment the Netlify deployment step
2. Add `NETLIFY_AUTH_TOKEN` and `NETLIFY_SITE_ID` secrets

## Customization

### Adding Stocks

Edit the `STOCK_UNIVERSE` list in `scripts/update_stocks.py`:

```python
STOCK_UNIVERSE = [
    'AAPL', 'MSFT', 'GOOGL',  # Add your stocks here
]
```

### Changing the Domain

Edit `hugo.toml`:

```toml
baseURL = 'https://yourdomain.com/'
```

## Signals Explained

| Signal | Meaning |
|--------|---------|
| **YES** | Stock is below 200-week moving average |
| **NO** | Stock is above 200-week moving average |
| **↓ Approaching** | Getting closer to the line (week-over-week) |
| **↑ Moving away** | Getting further from the line |
| **RSI < 30** | Short-term oversold condition |

## Legal

This is an educational tool, not financial advice. See `/disclaimer/` for full details.

## License

MIT
