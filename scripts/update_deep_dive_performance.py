#!/usr/bin/env python3
"""
Below The Line - Deep Dive Performance Updater

Reads all deep-dive markdown files and updates performance frontmatter
using prices from the already-generated stocks.json — zero API calls.

Run weekly as part of the build-deploy workflow (after update_stocks.py).
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

# Paths relative to this script's location (repo_root/scripts/)
REPO_ROOT = Path(__file__).parent.parent
CONTENT_DIR = REPO_ROOT / "below-the-line" / "content" / "deep-dives"
STOCKS_JSON = REPO_ROOT / "below-the-line" / "assets" / "data" / "stocks.json"


def load_price_cache() -> dict[str, tuple[float, str]]:
    """
    Build a ticker -> (close_price, last_updated) map from stocks.json.

    This uses the data the main pipeline JUST generated — no API calls needed.
    """
    if not STOCKS_JSON.exists():
        print(f"ERROR: stocks.json not found at {STOCKS_JSON}")
        print("  This script must run AFTER update_stocks.py generates stocks.json.")
        sys.exit(1)

    with open(STOCKS_JSON) as f:
        data = json.load(f)

    as_of = data.get("generated_iso", datetime.now().strftime("%Y-%m-%d"))
    cache = {}
    for stock in data.get("stocks", []):
        symbol = stock.get("symbol")
        close = stock.get("close")
        if symbol and close is not None:
            cache[symbol] = (float(close), as_of)

    print(f"  Loaded {len(cache)} prices from stocks.json (as of {as_of})")
    return cache


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
    """Set a YAML field value (quoted) — only updates existing fields."""
    pattern = rf'^({field}:\s*).*$'
    return re.sub(pattern, rf'\1"{value}"', frontmatter, count=1, flags=re.MULTILINE)


def upsert_field(frontmatter: str, field: str, value: str) -> str:
    """Set a YAML field (quoted), inserting it at the end if it doesn't exist.

    set_field silently does nothing when the field is absent; upsert_field
    guarantees the field lands in the frontmatter on first write.
    """
    pattern = rf'^({field}:\s*).*$'
    if re.search(pattern, frontmatter, re.MULTILINE):
        return re.sub(pattern, rf'\1"{value}"', frontmatter, count=1, flags=re.MULTILINE)
    return frontmatter.rstrip('\n') + f'\n{field}: "{value}"\n'


# Suffixes for multi-ticker articles (primary has no suffix)
TICKER_SUFFIXES = ["b", "c", "d", "e"]


def process_file(filepath: Path, price_cache: dict) -> dict | None:
    """
    Process a single deep-dive markdown file.

    Handles single-ticker, faceoff (ticker + ticker_b), and multi-ticker
    (ticker + ticker_b through ticker_e) articles. Returns a summary dict
    for the report, or None if the file should be skipped entirely.
    """
    text = filepath.read_text()
    try:
        frontmatter, body = extract_frontmatter(text)
    except ValueError:
        return None

    ticker = get_field(frontmatter, "ticker")
    if not ticker:
        return None  # No-stock articles (framework posts, how-tos, backtests)

    pub_price_str = get_field(frontmatter, "performance_price_at_publish")
    if not pub_price_str:
        return None  # Publish price not yet seeded — skip

    pub_price = float(pub_price_str.replace("$", ""))

    result = price_cache.get(ticker)
    if result is None:
        print(f"  WARNING: {ticker} not found in stocks.json — skipping {filepath.name}")
        return {"file": filepath.name, "ticker": ticker, "error": True}

    current_price, as_of = result
    ret = ((current_price - pub_price) / pub_price) * 100
    ret_str = f"{ret:+.1f}%"

    # upsert so first-time fields get written even if not already in frontmatter
    frontmatter = upsert_field(frontmatter, "performance_since", ret_str)
    frontmatter = upsert_field(frontmatter, "performance_price_current", f"${current_price:.2f}")
    frontmatter = upsert_field(frontmatter, "performance_as_of", as_of)

    summary = {
        "file": filepath.name,
        "ticker": ticker,
        "pub_price": pub_price,
        "current_price": current_price,
        "return": ret_str,
    }

    # Handle additional tickers (ticker_b through ticker_e)
    for suffix in TICKER_SUFFIXES:
        ticker_x = get_field(frontmatter, f"ticker_{suffix}")
        if not ticker_x:
            break  # No more tickers for this article

        pub_price_x_str = get_field(frontmatter, f"performance_price_at_publish_{suffix}")
        if not pub_price_x_str:
            print(f"  NOTE: {ticker_x} (ticker_{suffix}) has no performance_price_at_publish_{suffix} — skipping")
            continue

        pub_price_x = float(pub_price_x_str.replace("$", ""))
        result_x = price_cache.get(ticker_x)

        if result_x:
            current_price_x, _ = result_x
            ret_x = ((current_price_x - pub_price_x) / pub_price_x) * 100
            frontmatter = upsert_field(frontmatter, f"performance_since_{suffix}", f"{ret_x:+.1f}%")
            frontmatter = upsert_field(frontmatter, f"performance_price_current_{suffix}", f"${current_price_x:.2f}")
            summary[f"ticker_{suffix}"] = ticker_x
            summary[f"pub_price_{suffix}"] = pub_price_x
            summary[f"current_price_{suffix}"] = current_price_x
            summary[f"return_{suffix}"] = f"{ret_x:+.1f}%"
        else:
            print(f"  WARNING: {ticker_x} (ticker_{suffix}) not found in stocks.json")

    # Write updated file
    filepath.write_text(f"---\n{frontmatter}---\n{body}")
    return summary


def main():
    print("=" * 90)
    print("Deep Dive Performance Updater (reads from stocks.json — zero API calls)")
    print("=" * 90)

    if not CONTENT_DIR.exists():
        print(f"ERROR: Content directory not found: {CONTENT_DIR}")
        sys.exit(1)

    # Load prices from the just-generated stocks.json
    price_cache = load_price_cache()

    files = sorted(
        f for f in CONTENT_DIR.glob("*.md") if f.name != "_index.md"
    )

    print(f"Found {len(files)} articles in {CONTENT_DIR}")
    print("-" * 90)

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
        print(f"\nWARNING: {len(errors)} tickers not in stocks.json: {', '.join(e['ticker'] for e in errors)}")
        print("These may be tickers not in STOCK_UNIVERSE — add them or skip.")

    print(f"\nUpdated {len(results)} articles. Skipped {len(errors)}.")


if __name__ == "__main__":
    main()
