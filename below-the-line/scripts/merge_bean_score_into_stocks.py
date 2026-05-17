#!/usr/bin/env python3
"""
Merge Bean Score display levels into existing stocks.json.

Run this after bean_score.py and bean_score_levels.py to add dislocation
price levels to stock pages without re-running the full update pipeline.

The quarterly FCF chart data now comes through bean_score_levels.py
(which reads it from bean_score_latest.json where it's persisted during
the main bean_score.py computation — no separate yfinance call needed).

Usage:
    python merge_bean_score_into_stocks.py
"""
import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / 'assets' / 'data'
STOCKS_FILE = DATA_DIR / 'stocks.json'
DISPLAY_FILE = DATA_DIR / 'bean_score_display.json'


def main():
    if not STOCKS_FILE.exists():
        print(f"ERROR: {STOCKS_FILE} not found")
        sys.exit(1)

    if not DISPLAY_FILE.exists():
        print(f"ERROR: {DISPLAY_FILE} not found. Run bean_score_levels.py first.")
        sys.exit(1)

    # Load both files
    with open(STOCKS_FILE) as f:
        stocks_data = json.load(f)

    with open(DISPLAY_FILE) as f:
        display_data = json.load(f)

    bean_stocks = display_data.get('stocks', {})
    print(f"Loaded {len(bean_stocks)} Bean Score display records")

    # Merge into each stock
    merged = 0
    with_quarterly = 0
    for stock in stocks_data.get('stocks', []):
        symbol = stock.get('symbol')
        if symbol and symbol in bean_stocks:
            stock['bean_score_data'] = bean_stocks[symbol]
            merged += 1
            if bean_stocks[symbol].get('quarterly_fcf'):
                with_quarterly += 1

    print(f"Merged Bean Score data into {merged} stocks ({with_quarterly} with quarterly chart)")

    # Write back
    with open(STOCKS_FILE, 'w') as f:
        json.dump(stocks_data, f, separators=(',', ':'))

    print(f"✓ Updated {STOCKS_FILE}")


if __name__ == '__main__':
    main()
