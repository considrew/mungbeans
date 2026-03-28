#!/usr/bin/env python3
"""
Bean Score — Cash Yield Dislocation Metric

Measures how far a stock's FCF yield has deviated from its quarterly baseline,
normalized by the stock's own historical intra-quarter price behavior.

Core idea:
  - Each quarter, a new FCF value is reported → new baseline FCF yield
  - Between quarters, FCF yield changes are purely price-driven
  - We measure how far price has pushed FCF yield from the quarterly baseline
  - We normalize by the stock's own historical intra-quarter deviation σ
  - A Bean Score > 2σ means the market is overreacting relative to this
    stock's own typical behavior → candidate for dislocation

The score is self-calibrating:
  - A stable stock (low σ) triggers on small moves
  - A volatile stock (high σ) requires larger moves to trigger
  - No sector adjustments or arbitrary thresholds needed

Usage:
  # Single stock
  score = compute_bean_score('AAPL')

  # Batch (for pipeline integration)
  scores = compute_bean_scores_batch(['AAPL', 'MSFT', 'GOOGL'])

  # Full S&P 500 snapshot (for weekly persistence)
  snapshot = weekly_bean_score_snapshot(ticker_list)

Data persistence:
  - bean_score_history.json: append-only log of weekly snapshots
  - bean_score_latest.json: most recent scores for all tracked tickers
  - bean_score_alerts.json: log of threshold crossings (>2σ events)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf


class NumpyEncoder(json.JSONEncoder):
    """Handle numpy/pandas types in JSON serialization."""
    def default(self, obj):
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime, date)):
            return obj.isoformat()
        return super().default(obj)


# Where to persist Bean Score data
DATA_DIR = Path(__file__).parent.parent / 'assets' / 'data'
HISTORY_FILE = DATA_DIR / 'bean_score_history.json'
LATEST_FILE = DATA_DIR / 'bean_score_latest.json'
ALERTS_FILE = DATA_DIR / 'bean_score_alerts.json'


def _get_quarterly_fcf(ticker_obj) -> Optional[pd.Series]:
    """Extract quarterly Free Cash Flow series from a yfinance Ticker.

    Returns a tz-naive, date-sorted Series of quarterly FCF values.
    Tries multiple field names for compatibility across yfinance versions.
    """
    cf = ticker_obj.quarterly_cashflow
    if cf is None or len(cf) == 0:
        return None

    # Try known field names
    for field in ['Free Cash Flow', 'FreeCashFlow']:
        if field in cf.index:
            fcf = cf.loc[field].dropna().sort_index()
            fcf.index = fcf.index.tz_localize(None)
            return fcf

    # Fallback: compute from Operating Cash Flow - Capital Expenditure
    ocf_field = next((f for f in ['Operating Cash Flow', 'OperatingCashFlow',
                                   'Total Cash From Operating Activities']
                      if f in cf.index), None)
    capex_field = next((f for f in ['Capital Expenditure', 'CapitalExpenditure',
                                     'Capital Expenditures']
                        if f in cf.index), None)

    if ocf_field and capex_field:
        # Capital expenditure is typically negative in yfinance
        fcf = cf.loc[ocf_field] + cf.loc[capex_field]
        fcf = fcf.dropna().sort_index()
        fcf.index = fcf.index.tz_localize(None)
        return fcf

    return None


def _get_annual_fcf(ticker_obj) -> Optional[pd.Series]:
    """Extract annual Free Cash Flow series from a yfinance Ticker.

    Used to extend history beyond the ~7 quarters yfinance provides.
    Returns tz-naive, date-sorted Series.
    """
    cf = ticker_obj.cashflow
    if cf is None or len(cf) == 0:
        return None

    for field in ['Free Cash Flow', 'FreeCashFlow']:
        if field in cf.index:
            fcf = cf.loc[field].dropna().sort_index()
            fcf.index = fcf.index.tz_localize(None)
            return fcf

    ocf_field = next((f for f in ['Operating Cash Flow', 'OperatingCashFlow',
                                   'Total Cash From Operating Activities']
                      if f in cf.index), None)
    capex_field = next((f for f in ['Capital Expenditure', 'CapitalExpenditure',
                                     'Capital Expenditures']
                        if f in cf.index), None)

    if ocf_field and capex_field:
        fcf = cf.loc[ocf_field] + cf.loc[capex_field]
        fcf = fcf.dropna().sort_index()
        fcf.index = fcf.index.tz_localize(None)
        return fcf

    return None


def compute_bean_score(ticker: str, verbose: bool = False) -> Optional[dict]:
    """Compute the Bean Score for a single ticker.

    Returns a dict with:
      - ticker: str
      - bean_score: float (z-score, positive = unusually cheap)
      - current_fcf_yield: float (current TTM FCF / market cap, as %)
      - baseline_fcf_yield: float (yield at last quarterly report price, as %)
      - deviation_pp: float (current - baseline, in percentage points)
      - hist_dev_std: float (historical intra-quarter deviation σ, in pp)
      - hist_dev_mean: float (historical mean deviation, in pp)
      - ttm_fcf: float (trailing twelve months FCF, in dollars)
      - n_quarters: int (number of quarterly baselines used)
      - n_observations: int (number of weekly deviation observations)
      - last_report_date: str (date of most recent quarterly report)
      - percentile: float (where current yield sits in 3-year distribution)
      - velocity_4w: float (change in FCF yield over last 4 weeks, in pp)
      - velocity_13w: float (change in FCF yield over last 13 weeks, in pp)
      - sector: str
      - computed_at: str (ISO timestamp)

    Returns None if insufficient data.
    """
    try:
        tk = yf.Ticker(ticker)

        # Get quarterly FCF
        fcf_quarterly = _get_quarterly_fcf(tk)
        if fcf_quarterly is None or len(fcf_quarterly) < 4:
            if verbose:
                print(f"  {ticker}: insufficient quarterly FCF data")
            return None

        fcf_dates = fcf_quarterly.index.tolist()

        # Get weekly price history covering all FCF dates
        start_date = fcf_dates[0] - pd.Timedelta(days=30)
        hist = tk.history(start=start_date, interval='1wk')
        if hist is None or len(hist) < 20:
            if verbose:
                print(f"  {ticker}: insufficient price history")
            return None
        hist.index = hist.index.tz_localize(None)

        # Get shares outstanding
        info = tk.info
        shares = info.get('sharesOutstanding')
        if not shares:
            mc = info.get('marketCap')
            cp = info.get('currentPrice')
            if mc and cp and cp > 0:
                shares = mc / cp
            else:
                if verbose:
                    print(f"  {ticker}: cannot determine shares outstanding")
                return None

        # Build intra-quarter deviation distribution
        intra_quarter_deviations = []
        quarter_baselines = []

        for i in range(3, len(fcf_dates)):
            report_date = fcf_dates[i]
            ttm_fcf = fcf_quarterly.iloc[i-3:i+1].sum()

            # Find price at report date
            close_prices = hist['Close']
            mask = close_prices.index <= report_date
            if mask.sum() == 0:
                continue

            baseline_price = float(close_prices[mask].iloc[-1])
            baseline_mcap = baseline_price * shares
            if baseline_mcap <= 0:
                continue
            baseline_fcf_yield = ttm_fcf / baseline_mcap

            # Next report date boundary
            if i + 1 < len(fcf_dates):
                next_report = fcf_dates[i + 1]
            else:
                next_report = hist.index[-1]

            # Weekly prices during this inter-quarter period
            period_mask = (close_prices.index > report_date) & \
                          (close_prices.index <= next_report)
            period_prices = close_prices[period_mask]

            if len(period_prices) == 0:
                continue

            # FCF yield at each weekly price (FCF fixed at TTM value)
            period_yields = ttm_fcf / (period_prices * shares)
            deviations = (period_yields - baseline_fcf_yield) * 100  # pp

            intra_quarter_deviations.extend(deviations.tolist())

            quarter_baselines.append({
                'date': report_date,
                'ttm_fcf': float(ttm_fcf),
                'baseline_price': baseline_price,
                'baseline_yield': float(baseline_fcf_yield * 100),
            })

        if len(intra_quarter_deviations) < 8 or len(quarter_baselines) < 2:
            if verbose:
                print(f"  {ticker}: insufficient deviation data "
                      f"({len(intra_quarter_deviations)} obs, "
                      f"{len(quarter_baselines)} quarters)")
            return None

        # Historical deviation statistics
        dev_array = np.array(intra_quarter_deviations)
        dev_mean = float(dev_array.mean())
        dev_std = float(dev_array.std())

        # Current state
        latest = quarter_baselines[-1]
        current_price = float(hist['Close'].iloc[-1])
        current_mcap = current_price * shares
        current_fcf_yield = (latest['ttm_fcf'] / current_mcap) * 100
        current_deviation = current_fcf_yield - latest['baseline_yield']

        # Bean Score (z-score)
        if dev_std > 0:
            bean_score = (current_deviation - dev_mean) / dev_std
        else:
            bean_score = 0.0

        # Percentile: where does current yield sit in the 3-year distribution?
        all_yields = []
        for qb in quarter_baselines:
            all_yields.append(qb['baseline_yield'])
        # Reconstruct weekly yields from price history
        full_yields = (latest['ttm_fcf'] / (hist['Close'] * shares)) * 100
        percentile = float((full_yields < current_fcf_yield).mean() * 100)

        # Velocity: rate of FCF yield change (purely price-driven)
        velocity_4w = 0.0
        velocity_13w = 0.0
        if len(full_yields) >= 4:
            velocity_4w = float(current_fcf_yield - full_yields.iloc[-4] * 1) \
                if len(full_yields) >= 4 else 0.0
            # Recompute properly
            fy_4w_ago = float((latest['ttm_fcf'] /
                              (float(hist['Close'].iloc[-4]) * shares)) * 100)
            velocity_4w = current_fcf_yield - fy_4w_ago
        if len(full_yields) >= 13:
            fy_13w_ago = float((latest['ttm_fcf'] /
                               (float(hist['Close'].iloc[-13]) * shares)) * 100)
            velocity_13w = current_fcf_yield - fy_13w_ago

        # Data quality gate: reject scores built on nonsense data.
        # Stocks with |FCF yield| > 100% almost always reflect bad share-count
        # data (e.g. ADRs where yfinance reports the wrong share basis) or
        # micro-caps with negligible market cap.  Hist σ > 20pp is a secondary
        # signal of the same problem.  We hard-reject these to keep the
        # dashboard and alert log clean.
        MAX_ABS_YIELD = 100.0   # percent
        MAX_HIST_STD  = 20.0    # percentage points

        if abs(current_fcf_yield) > MAX_ABS_YIELD:
            if verbose:
                print(f"  {ticker}: rejected — |FCF yield| "
                      f"{abs(current_fcf_yield):.1f}% > {MAX_ABS_YIELD}%")
            return None

        if dev_std > MAX_HIST_STD:
            if verbose:
                print(f"  {ticker}: rejected — hist σ "
                      f"{dev_std:.2f}pp > {MAX_HIST_STD}pp")
            return None

        return {
            'ticker': ticker,
            'bean_score': round(bean_score, 3),
            'current_fcf_yield': round(current_fcf_yield, 3),
            'baseline_fcf_yield': round(latest['baseline_yield'], 3),
            'deviation_pp': round(current_deviation, 3),
            'hist_dev_mean': round(dev_mean, 4),
            'hist_dev_std': round(dev_std, 4),
            'ttm_fcf': latest['ttm_fcf'],
            'n_quarters': len(quarter_baselines),
            'n_observations': len(intra_quarter_deviations),
            'last_report_date': latest['date'].strftime('%Y-%m-%d'),
            'percentile': round(percentile, 1),
            'velocity_4w': round(velocity_4w, 4),
            'velocity_13w': round(velocity_13w, 4),
            'sector': info.get('sector', 'Unknown'),
            'computed_at': datetime.utcnow().isoformat(),
        }

    except Exception as e:
        if verbose:
            print(f"  {ticker}: error — {e}")
        return None


def compute_bean_scores_batch(tickers: list[str],
                              verbose: bool = False) -> list[dict]:
    """Compute Bean Scores for a list of tickers.

    Returns list of score dicts (skipping tickers with insufficient data).
    """
    results = []
    total = len(tickers)
    for i, ticker in enumerate(tickers):
        if verbose and (i + 1) % 25 == 0:
            print(f"  [{i+1}/{total}] Processing {ticker}...")
        score = compute_bean_score(ticker, verbose=verbose)
        if score is not None:
            results.append(score)
    return results


def weekly_bean_score_snapshot(tickers: list[str],
                               verbose: bool = False) -> dict:
    """Generate a full weekly snapshot and persist to disk.

    This is meant to be called from the weekly pipeline (update_stocks.py)
    after the main price data has been updated.

    Persists:
      - bean_score_latest.json: current scores for all tickers
      - bean_score_history.json: append-only weekly snapshots
      - bean_score_alerts.json: append-only log of >2σ events

    Returns the snapshot dict.
    """
    scores = compute_bean_scores_batch(tickers, verbose=verbose)

    snapshot_date = datetime.utcnow().strftime('%Y-%m-%d')
    snapshot = {
        'date': snapshot_date,
        'n_scored': len(scores),
        'n_attempted': len(tickers),
        'scores': {s['ticker']: s for s in scores},
    }

    # Persist latest
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(LATEST_FILE, 'w') as f:
        json.dump(snapshot, f, cls=NumpyEncoder, indent=2)

    # Append to history (one entry per week)
    history = []
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as f:
                history = json.load(f)
        except (json.JSONDecodeError, ValueError):
            history = []

    # Compact history entry: just ticker → bean_score mapping + date
    compact = {
        'date': snapshot_date,
        'scores': {s['ticker']: {
            'bean_score': s['bean_score'],
            'current_fcf_yield': s['current_fcf_yield'],
            'baseline_fcf_yield': s['baseline_fcf_yield'],
            'deviation_pp': s['deviation_pp'],
            'hist_dev_std': s['hist_dev_std'],
            'velocity_13w': s['velocity_13w'],
        } for s in scores}
    }
    history.append(compact)
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, cls=NumpyEncoder)

    # Log alerts (>2σ or <-2σ events)
    alerts = []
    if ALERTS_FILE.exists():
        try:
            with open(ALERTS_FILE) as f:
                alerts = json.load(f)
        except (json.JSONDecodeError, ValueError):
            alerts = []

    new_alerts = []
    for s in scores:
        if abs(s['bean_score']) >= 2.0:
            direction = 'CHEAP' if s['bean_score'] > 0 else 'EXPENSIVE'
            new_alerts.append({
                'date': snapshot_date,
                'ticker': s['ticker'],
                'bean_score': s['bean_score'],
                'direction': direction,
                'current_fcf_yield': s['current_fcf_yield'],
                'baseline_fcf_yield': s['baseline_fcf_yield'],
                'deviation_pp': s['deviation_pp'],
                'hist_dev_std': s['hist_dev_std'],
                'sector': s['sector'],
            })

    if new_alerts:
        alerts.extend(new_alerts)
        with open(ALERTS_FILE, 'w') as f:
            json.dump(alerts, f, cls=NumpyEncoder, indent=2)

    if verbose:
        print(f"\nBean Score snapshot: {len(scores)}/{len(tickers)} scored")
        print(f"  Alerts (|z| >= 2σ): {len(new_alerts)}")
        overreaction_cheap = [s for s in scores if s['bean_score'] >= 2.0]
        overreaction_expensive = [s for s in scores if s['bean_score'] <= -2.0]
        if overreaction_cheap:
            print(f"\n  OVERREACTION CHEAP ({len(overreaction_cheap)} stocks):")
            for s in sorted(overreaction_cheap,
                           key=lambda x: x['bean_score'], reverse=True)[:10]:
                print(f"    {s['ticker']:6s} | {s['bean_score']:+.2f}σ | "
                      f"FCF Y={s['current_fcf_yield']:.2f}% | "
                      f"base={s['baseline_fcf_yield']:.2f}% | "
                      f"σ={s['hist_dev_std']:.2f}pp | {s['sector']}")
        if overreaction_expensive:
            print(f"\n  OVERREACTION EXPENSIVE ({len(overreaction_expensive)} stocks):")
            for s in sorted(overreaction_expensive,
                           key=lambda x: x['bean_score'])[:10]:
                print(f"    {s['ticker']:6s} | {s['bean_score']:+.2f}σ | "
                      f"FCF Y={s['current_fcf_yield']:.2f}% | "
                      f"base={s['baseline_fcf_yield']:.2f}% | "
                      f"σ={s['hist_dev_std']:.2f}pp | {s['sector']}")

    return snapshot


def load_latest_scores() -> Optional[dict]:
    """Load the most recent Bean Score snapshot from disk."""
    if LATEST_FILE.exists():
        with open(LATEST_FILE) as f:
            return json.load(f)
    return None


def load_score_history() -> list[dict]:
    """Load the full Bean Score history from disk."""
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE) as f:
            return json.load(f)
    return []


def load_alerts() -> list[dict]:
    """Load the alert log from disk."""
    if ALERTS_FILE.exists():
        with open(ALERTS_FILE) as f:
            return json.load(f)
    return []


# ---------------------------------------------------------------------------
# CLI entrypoint for testing / manual runs
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        # Single ticker mode
        tickers = sys.argv[1:]
        for t in tickers:
            result = compute_bean_score(t.upper(), verbose=True)
            if result:
                print(f"\n{t.upper()} Bean Score: {result['bean_score']:+.2f}σ")
                print(json.dumps(result, indent=2, cls=NumpyEncoder))
            else:
                print(f"\n{t.upper()}: Could not compute Bean Score")
    else:
        print("Usage: python bean_score.py TICKER [TICKER ...]")
        print("       python bean_score.py AAPL MSFT ADBE")
        print("\nFor batch/pipeline usage, import and call:")
        print("  weekly_bean_score_snapshot(ticker_list)")
