#!/usr/bin/env python3
"""
Bean Score Dislocation Levels Generator

Reads bean_score_latest.json and computes 5 stock price levels where the
Bean Score would hit specific σ thresholds. These serve as swing trade
reference points on stock pages.

The key insight: between earnings dates, TTM FCF is constant, so the Bean
Score is purely a function of stock price. We can pre-compute the prices
at which various σ levels would trigger.

Math:
  bean_score = (deviation - hist_dev_mean) / hist_dev_std
  deviation = current_fcf_yield - baseline_fcf_yield
  current_fcf_yield = (ttm_fcf / (price × shares)) × 100

  Solving for price at target bean_score Z:
    target_deviation = Z × hist_dev_std + hist_dev_mean
    target_fcf_yield = target_deviation + baseline_fcf_yield  (in %)
    price = ttm_fcf / (target_fcf_yield / 100 × shares)

Output: bean_score_display.json — per-stock dislocation levels + metadata
for integration into stocks.json and the Hugo stock page template.

Usage:
    python bean_score_levels.py [--shares-file SHARES_FILE]

    If --shares-file is not provided, uses yfinance to look up shares
    outstanding (slower but self-contained).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import numpy as np

DATA_DIR = Path(__file__).parent.parent / 'assets' / 'data'
LATEST_FILE = DATA_DIR / 'bean_score_latest.json'
HISTORY_FILE = DATA_DIR / 'bean_score_history.json'
STOCKS_FILE = DATA_DIR / 'stocks.json'
OUTPUT_FILE = DATA_DIR / 'bean_score_display.json'

# σ thresholds for the 5 dislocation price levels
# Positive = cheap (yield higher than usual), Negative = expensive (yield lower)
SIGMA_LEVELS = [
    {'sigma': 2.0,  'label': 'Deep Value',    'description': 'Extremely cheap — 2σ above baseline yield'},
    {'sigma': 1.0,  'label': 'Value',         'description': 'Meaningfully cheap — 1σ above baseline yield'},
    {'sigma': 0.0,  'label': 'Fair Value',    'description': 'At historical mean deviation from baseline'},
    {'sigma': -1.0, 'label': 'Expensive',     'description': 'Meaningfully expensive — 1σ below baseline yield'},
    {'sigma': -2.0, 'label': 'Deep Expensive', 'description': 'Extremely expensive — 2σ below baseline yield'},
]


def compute_price_at_sigma(
    sigma: float,
    ttm_fcf: float,
    shares: float,
    baseline_fcf_yield: float,
    hist_dev_mean: float,
    hist_dev_std: float,
) -> Optional[float]:
    """Compute the stock price where Bean Score would equal the given σ.

    Returns None if the math produces a nonsensical result (e.g., negative
    price, infinite price due to zero target yield, or target yield that
    doesn't match the sign of FCF).
    """
    # Target deviation from baseline (in percentage points)
    target_deviation = sigma * hist_dev_std + hist_dev_mean

    # Target FCF yield (in %)
    target_fcf_yield = target_deviation + baseline_fcf_yield

    # For positive FCF, target yield must be positive for a valid price
    # For negative FCF, target yield must be negative
    if ttm_fcf > 0 and target_fcf_yield <= 0:
        return None
    if ttm_fcf < 0 and target_fcf_yield >= 0:
        return None
    if target_fcf_yield == 0:
        return None

    # price = ttm_fcf / (target_fcf_yield/100 × shares)
    price = ttm_fcf / (target_fcf_yield / 100.0 * shares)

    # Sanity checks
    if price <= 0:
        return None
    if price > 1e6:  # $1M/share cap to filter nonsense
        return None

    return round(price, 2)


def get_shares_from_stocks_json() -> dict:
    """Extract shares outstanding per ticker from stocks.json.

    Uses market_cap / close as the derived shares count. This avoids
    needing a separate yfinance call.
    """
    shares_map = {}
    if not STOCKS_FILE.exists():
        return shares_map

    with open(STOCKS_FILE) as f:
        data = json.load(f)

    for stock in data.get('stocks', []):
        symbol = stock.get('symbol')
        market_cap = stock.get('market_cap')
        close = stock.get('close')
        if symbol and market_cap and close and close > 0:
            shares_map[symbol] = market_cap / close

    return shares_map


def generate_bean_score_display(verbose: bool = False) -> dict:
    """Generate display data for all stocks with valid Bean Score data.

    Returns dict keyed by ticker with:
      - levels: list of 5 dislocation price levels
      - current_score: current Bean Score
      - current_fcf_yield: current FCF yield %
      - baseline_fcf_yield: baseline from last earnings
      - ttm_fcf: trailing twelve month FCF
      - last_report_date: date of last quarterly report
      - hist_dev_std: historical σ (for context)
      - n_quarters: data depth indicator
      - sector: stock sector
    """
    if not LATEST_FILE.exists():
        print(f"ERROR: {LATEST_FILE} not found. Run bean_score.py first.")
        sys.exit(1)

    with open(LATEST_FILE) as f:
        latest = json.load(f)

    scores = latest.get('scores', {})
    shares_map = get_shares_from_stocks_json()

    # Count weekly history snapshots for tracking progress
    history_weeks = 0
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as f:
                history = json.load(f)
            history_weeks = len(history)
        except Exception:
            pass

    if verbose:
        print(f"Loaded {len(scores)} Bean Scores, {len(shares_map)} share counts, {history_weeks} history weeks")

    display_data = {}
    skipped_no_shares = 0
    skipped_negative_fcf = 0
    skipped_bad_levels = 0

    for ticker, score_data in scores.items():
        ttm_fcf = score_data.get('ttm_fcf', 0)

        # Only generate levels for positive-FCF companies
        # Negative FCF makes the dislocation concept less actionable
        if ttm_fcf <= 0:
            skipped_negative_fcf += 1
            continue

        # Get shares outstanding — prefer value from bean_score computation,
        # fall back to deriving from stocks.json (market_cap / close)
        shares = score_data.get('shares') or shares_map.get(ticker)
        if not shares or shares <= 0:
            skipped_no_shares += 1
            continue

        baseline_fcf_yield = score_data.get('baseline_fcf_yield', 0)
        hist_dev_mean = score_data.get('hist_dev_mean', 0)
        hist_dev_std = score_data.get('hist_dev_std', 0)

        if hist_dev_std <= 0:
            continue

        # Compute 5 price levels
        levels = []
        valid_levels = 0
        for level_def in SIGMA_LEVELS:
            sigma = level_def['sigma']
            price = compute_price_at_sigma(
                sigma=sigma,
                ttm_fcf=ttm_fcf,
                shares=shares,
                baseline_fcf_yield=baseline_fcf_yield,
                hist_dev_mean=hist_dev_mean,
                hist_dev_std=hist_dev_std,
            )
            levels.append({
                'sigma': sigma,
                'label': level_def['label'],
                'price': price,
            })
            if price is not None:
                valid_levels += 1

        # Need at least 3 valid levels to be useful
        if valid_levels < 3:
            skipped_bad_levels += 1
            continue

        entry = {
            'levels': levels,
            'current_score': score_data.get('bean_score'),
            'current_fcf_yield': score_data.get('current_fcf_yield'),
            'baseline_fcf_yield': baseline_fcf_yield,
            'ttm_fcf': ttm_fcf,
            'last_report_date': score_data.get('last_report_date'),
            'hist_dev_std': hist_dev_std,
            'hist_dev_mean': hist_dev_mean,
            'n_quarters': score_data.get('n_quarters'),
            'n_observations': score_data.get('n_observations'),
            'tracking_weeks': history_weeks,
            'sector': score_data.get('sector'),
            'computed_at': score_data.get('computed_at'),
        }

        # Pass through quarterly chart data if present (from bean_score.py)
        qc = score_data.get('quarterly_chart')
        if qc:
            entry['quarterly_fcf'] = {'quarters': qc}

        # Pass through earnings date data (from bean_score.py)
        ned = score_data.get('next_earnings_date')
        if ned:
            entry['next_earnings_date'] = ned
        eh = score_data.get('earnings_history')
        if eh:
            entry['earnings_history'] = eh

        display_data[ticker] = entry

    if verbose:
        print(f"Generated levels for {len(display_data)} stocks")
        print(f"Skipped: {skipped_negative_fcf} negative FCF, "
              f"{skipped_no_shares} no shares data, "
              f"{skipped_bad_levels} insufficient valid levels")

    return display_data


def main():
    verbose = '--verbose' in sys.argv or '-v' in sys.argv

    display_data = generate_bean_score_display(verbose=verbose)

    output = {
        'generated_at': __import__('datetime').datetime.utcnow().isoformat(),
        'n_stocks': len(display_data),
        'sigma_levels': SIGMA_LEVELS,
        'stocks': display_data,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"✓ Bean Score display data written to {OUTPUT_FILE}")
    print(f"  {len(display_data)} stocks with valid dislocation levels")


if __name__ == '__main__':
    main()
