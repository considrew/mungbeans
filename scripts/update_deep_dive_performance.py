#!/usr/bin/env python3
"""
Below The Line - Deep Dive Performance Updater

Reads all deep-dive markdown files, fetches current prices via yfinance,
and updates the performance frontmatter fields.

Run weekly (alongside update_stocks.py) or on-demand via workflow_dispatch.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

import yfinance as yf

CONTENT_DIR = Path(__file__).parent.parent / "below-the-line" / "content" / "deep-dives"


def extract_frontmatter(text: str) -> tuple[str, str]:
    """Split a markdown file into frontmatter and body."""
    match = re.match(r"^---\n(.*?\n)---\n(.*)$", text, re.DOTALL)
    if not match:
        raise ValueError("No valid frontmatter found")
    return match.group(1), match.group(2)


def get_field(frontmatter: str, field: str) -> str | None:
    """Extract a single YAML field value (unquoted)."""
    match = re.search(rf'^{field}:\s*"?([^"\n]+)"?\s*$', frontmatter, re.MULTILINE)
    return match.group(1).strip() if match else None


def set_field(frontmatter: str, field: str, value: str) -> str:
    """Set a YAML field value (quoted)."""
    pattern = rf'^({field}:\s*).*$'
    return re.sub(pattern, rf'\1"{value}"', frontmatter, count=1, flags=re.MULTILINE)


def fetch_current_price(symbol: str) -> tuple[float, str] | None:
    """Fetch the most recent closing price for a ticker via yfinance."""
    try:
        hist = yf.Ticker(symbol).history(period="5d")
        if hist.empty:
            print(f"  WARNING: No data returned for {symbol}")
            return None
        price = float(hist["Close"].iloc[-1])
        date = hist.index[-1].strftime("%Y-%m-%d")
        return price, date
    except Exception as e:
        print(f"  ERROR fetching {symbol}: {e}")
        return None


def process_file(filepath: Path, price_cache: dict) -> dict | None:
    """
    Process a single deep-dive markdown file.

    Returns a summary dict for the report, or None if skipped.
    """
    text = filepath.read_text()
    try:
        frontmatter, body = extract_frontmatter(text)
    except ValueError:
        return None

    ticker = get_field(frontmatter, "ticker")
    if not ticker:
        return None  # Not a stock article (e.g. how-to, backtest)

    pub_price_str = get_field(frontmatter, "performance_price_at_publish")
    if not pub_price_str:
        return None

    pub_price = float(pub_price_str.replace("$", ""))

    # Fetch price (use cache to avoid duplicate API calls for shared tickers)
    if ticker not in price_cache:
        time.sleep(2)  # Rate limit buffer — runs after main pipeline's ~2000 calls
        price_cache[ticker] = fetch_current_price(ticker)
    result = price_cache[ticker]
    if result is None:
        return {"file": filepath.name, "ticker": ticker, "error": True}

    current_price, as_of = result
    ret = ((current_price - pub_price) / pub_price) * 100
    ret_str = f"{ret:+.1f}%"

    frontmatter = set_field(frontmatter, "performance_since", ret_str)
    frontmatter = set_field(frontmatter, "performance_price_current", f"${current_price:.2f}")
    frontmatter = set_field(frontmatter, "performance_as_of", as_of)

    summary = {
        "file": filepath.name,
        "ticker": ticker,
        "pub_price": pub_price,
        "current_price": current_price,
        "return": ret_str,
    }

    # Handle faceoff (dual-ticker) articles
    ticker_b = get_field(frontmatter, "ticker_b")
    if ticker_b:
        pub_price_b_str = get_field(frontmatter, "performance_price_at_publish_b")
        if pub_price_b_str:
            pub_price_b = float(pub_price_b_str.replace("$", ""))

            if ticker_b not in price_cache:
                time.sleep(2)
                price_cache[ticker_b] = fetch_current_price(ticker_b)
            result_b = price_cache[ticker_b]

            if result_b:
                current_price_b, _ = result_b
                ret_b = ((current_price_b - pub_price_b) / pub_price_b) * 100

                frontmatter = set_field(frontmatter, "performance_since_b", f"{ret_b:+.1f}%")
                frontmatter = set_field(frontmatter, "performance_price_current_b", f"${current_price_b:.2f}")

                summary["ticker_b"] = ticker_b
                summary["pub_price_b"] = pub_price_b
                summary["current_price_b"] = current_price_b
                summary["return_b"] = f"{ret_b:+.1f}%"

    # Write updated file
    filepath.write_text(f"---\n{frontmatter}---\n{body}")
    return summary


def main():
    if not CONTENT_DIR.exists():
        print(f"ERROR: Content directory not found: {CONTENT_DIR}")
        sys.exit(1)

    files = sorted(
        f for f in CONTENT_DIR.glob("*.md") if f.name != "_index.md"
    )

    print(f"Found {len(files)} articles in {CONTENT_DIR}")
    print("-" * 90)

    price_cache: dict[str, tuple[float, str] | None] = {}
    results = []
    errors = []

    for f in files:
        summary = process_file(f, price_cache)
        if summary is None:
            continue
        if summary.get("error"):
            errors.append(summary)
        else:
            results.append(summary)

    # Print summary table
    print(f"\n{'Article':<45} {'Ticker':<8} {'Pub':>10} {'Current':>10} {'Return':>10}")
    print("=" * 90)
    for r in results:
        print(f"{r['file']:<45} {r['ticker']:<8} ${r['pub_price']:>9.2f} ${r['current_price']:>9.2f} {r['return']:>10}")
        if "ticker_b" in r:
            print(f"{'':<45} {r['ticker_b']:<8} ${r['pub_price_b']:>9.2f} ${r['current_price_b']:>9.2f} {r['return_b']:>10}")
    print("=" * 90)

    if errors:
        print(f"\nWARNING: Skipped {len(errors)} tickers due to errors (likely rate-limited): {', '.join(e['ticker'] for e in errors)}")
        print("Continuing — performance data is non-critical and will update next run.")

    print(f"\nUpdated {len(results)} articles. Skipped {len(errors)}.")


if __name__ == "__main__":
    main()
