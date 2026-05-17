#!/usr/bin/env python3
"""
Bean Score Dislocation Tracking — Historical Accuracy Analysis

Tracks what happens to stock prices AFTER Bean Score signals trigger.
This is the backtesting/validation layer for the Bean Score system.

How it works:
  1. Reads bean_score_history.json (weekly snapshots of scores)
  2. Detects "signal events" — when a stock crosses a σ threshold
  3. Tracks subsequent price changes at various horizons (1w, 4w, 13w, 26w)
  4. Computes win rates and average returns per signal type
  5. Outputs bean_score_tracking.json for display on stock pages

Signal Types:
  - "deep_value":  Bean Score crosses above +2σ (entered Deep Value)
  - "value":       Bean Score crosses above +1σ (entered Value territory)
  - "expensive":   Bean Score crosses below -1σ (entered Expensive territory)
  - "deep_expensive": Bean Score crosses below -2σ (entered Deep Expensive)

The key question we're answering:
  "When the Bean Score says a stock is cheap/expensive, does the price
   subsequently move in the predicted direction?"

For cheap signals (σ > 1): we expect price to RISE → positive returns
For expensive signals (σ < -1): we expect price to FALL → negative returns

Limitations:
  - Requires many weeks of history to produce meaningful stats
  - Early results will have wide confidence intervals
  - Does NOT account for earnings-driven changes (FCF resets invalidate
    the pre-existing score basis)

Usage:
    python bean_score_tracking.py [--verbose]

Output: assets/data/bean_score_tracking.json
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

DATA_DIR = Path(__file__).parent.parent / 'assets' / 'data'
HISTORY_FILE = DATA_DIR / 'bean_score_history.json'
STOCKS_FILE = DATA_DIR / 'stocks.json'
OUTPUT_FILE = DATA_DIR / 'bean_score_tracking.json'

# Signal thresholds — when does a score constitute a "signal"?
SIGNAL_THRESHOLDS = {
    'deep_value': {'direction': 'long', 'min_score': 2.0, 'label': 'Deep Value (>2σ)'},
    'value': {'direction': 'long', 'min_score': 1.0, 'max_score': 2.0, 'label': 'Value (1-2σ)'},
    'expensive': {'direction': 'short', 'max_score': -1.0, 'min_score': -2.0, 'label': 'Expensive (-1 to -2σ)'},
    'deep_expensive': {'direction': 'short', 'max_score': -2.0, 'label': 'Deep Expensive (<-2σ)'},
}

# Horizons to measure (in weeks)
HORIZONS = [1, 4, 13, 26]


def classify_signal(score: float) -> Optional[str]:
    """Classify a bean score into a signal type, or None if neutral."""
    if score >= 2.0:
        return 'deep_value'
    elif score >= 1.0:
        return 'value'
    elif score <= -2.0:
        return 'deep_expensive'
    elif score <= -1.0:
        return 'expensive'
    return None


def detect_signal_events(history: list) -> list:
    """Detect threshold crossings from weekly Bean Score history.

    A signal event is recorded when a stock crosses INTO a threshold zone
    (wasn't there the previous week, is there this week).

    Returns list of:
      {ticker, signal_type, entry_date, entry_score, entry_index}
    """
    events = []

    if len(history) < 2:
        return events

    for i in range(1, len(history)):
        prev_snapshot = history[i - 1]
        curr_snapshot = history[i]
        curr_date = curr_snapshot['date']
        prev_scores = prev_snapshot.get('scores', {})
        curr_scores = curr_snapshot.get('scores', {})

        for ticker, curr_data in curr_scores.items():
            curr_score = curr_data.get('bean_score')
            if curr_score is None:
                continue

            curr_signal = classify_signal(curr_score)
            if curr_signal is None:
                continue

            # Check if this is a NEW crossing (wasn't in this zone before)
            prev_data = prev_scores.get(ticker, {})
            prev_score = prev_data.get('bean_score')

            if prev_score is not None:
                prev_signal = classify_signal(prev_score)
                if prev_signal == curr_signal:
                    # Already in same zone — not a new event
                    continue

            events.append({
                'ticker': ticker,
                'signal_type': curr_signal,
                'entry_date': curr_date,
                'entry_score': curr_score,
                'entry_index': i,
            })

    return events


def get_price_at_snapshot(ticker: str, history: list, index: int) -> Optional[float]:
    """Get stock price from a history snapshot.

    Returns the close price if stored in the snapshot (added May 2026+),
    otherwise returns None for older snapshots that lack price data.
    """
    if index < 0 or index >= len(history):
        return None
    snapshot = history[index]
    ticker_data = snapshot.get('scores', {}).get(ticker, {})
    return ticker_data.get('price')


def compute_returns_from_scores(history: list, events: list) -> list:
    """For each signal event, compute subsequent bean score at EVERY available week.

    This gives us the full "response curve" — how does the score evolve
    week by week after a signal fires? This is critical because we don't
    know in advance what the optimal holding period is.

    The continuous week-by-week data lets us chart:
      - When mean reversion starts
      - How fast it progresses
      - Whether there's a "sweet spot" horizon
      - Whether the signal is early (lag before response)

    Returns events enriched with:
      - 'trajectory': list of {week, score, score_change, date} for every
        available subsequent week
      - 'outcomes': dict with fixed horizon checkpoints (1w, 4w, 13w, 26w)
    """
    enriched = []
    n_snapshots = len(history)

    for event in events:
        entry_idx = event['entry_index']
        ticker = event['ticker']
        outcomes = {}
        trajectory = []

        # Get entry price for return calculations
        entry_price = get_price_at_snapshot(ticker, history, entry_idx)
        event['entry_price'] = entry_price

        # Track every available week after signal
        max_weeks = min(n_snapshots - entry_idx - 1, 52)  # Cap at 1 year
        for week in range(1, max_weeks + 1):
            target_idx = entry_idx + week
            target_snapshot = history[target_idx]
            target_scores = target_snapshot.get('scores', {})
            target_data = target_scores.get(ticker, {})
            target_score = target_data.get('bean_score')

            if target_score is None:
                trajectory.append({'week': week, 'score': None, 'score_change': None, 'price_return_pct': None, 'date': target_snapshot['date']})
                continue

            score_change = target_score - event['entry_score']

            # Calculate actual price return if both prices available
            target_price = target_data.get('price')
            price_return = None
            if entry_price and target_price and entry_price > 0:
                price_return = round((target_price - entry_price) / entry_price * 100, 2)

            point = {
                'week': week,
                'score': round(target_score, 3),
                'score_change': round(score_change, 3),
                'price_return_pct': price_return,
                'date': target_snapshot['date'],
            }
            trajectory.append(point)

            # Also store at fixed horizons for quick lookup
            if week in HORIZONS:
                outcomes[f'{week}w'] = {
                    'score_at_horizon': round(target_score, 3),
                    'score_change': round(score_change, 3),
                    'price_return_pct': price_return,
                    'date': target_snapshot['date'],
                }

        # Fill in missing fixed horizons as None
        for h in HORIZONS:
            if f'{h}w' not in outcomes:
                outcomes[f'{h}w'] = None

        event_enriched = {**event, 'outcomes': outcomes, 'trajectory': trajectory}
        enriched.append(event_enriched)

    return enriched


def aggregate_results(enriched_events: list) -> dict:
    """Aggregate signal events into summary statistics + response curves.

    Returns per signal type:
      - total events
      - per fixed horizon: n_measured, mean_score_change, pct_mean_reverted
      - response_curve: week-by-week average score change (the key chart data)

    The response_curve answers: "On average, how does the Bean Score evolve
    each week after this signal type fires?" This reveals the TIME EFFECT —
    whether mean reversion is immediate or delayed, fast or gradual.
    """
    results = {}

    for signal_type, config in SIGNAL_THRESHOLDS.items():
        type_events = [e for e in enriched_events if e['signal_type'] == signal_type]

        if not type_events:
            results[signal_type] = {
                'label': config['label'],
                'direction': config['direction'],
                'total_events': 0,
                'horizons': {},
                'response_curve': [],
            }
            continue

        # Fixed horizon stats
        horizon_stats = {}
        for horizon in HORIZONS:
            key = f'{horizon}w'
            measured = [e for e in type_events if e['outcomes'].get(key) is not None]

            if not measured:
                horizon_stats[key] = {
                    'n_measured': 0,
                    'n_total': len(type_events),
                }
                continue

            score_changes = [e['outcomes'][key]['score_change'] for e in measured]

            if config['direction'] == 'long':
                reverted = [sc for sc in score_changes if sc < 0]
            else:
                reverted = [sc for sc in score_changes if sc > 0]

            mean_change = sum(score_changes) / len(score_changes)
            pct_reverted = len(reverted) / len(measured) * 100

            horizon_stats[key] = {
                'n_measured': len(measured),
                'n_total': len(type_events),
                'mean_score_change': round(mean_change, 3),
                'pct_mean_reverted': round(pct_reverted, 1),
                'median_score_change': round(sorted(score_changes)[len(score_changes) // 2], 3),
            }

        # Build response curve: average score change at each week
        # This is the key visualization data
        response_curve = []
        max_week = max(
            (len(e.get('trajectory', [])) for e in type_events),
            default=0
        )

        for week in range(1, max_week + 1):
            week_changes = []
            week_returns = []
            for event in type_events:
                traj = event.get('trajectory', [])
                if week <= len(traj):
                    point = traj[week - 1]
                    if point.get('score_change') is not None:
                        week_changes.append(point['score_change'])
                    if point.get('price_return_pct') is not None:
                        week_returns.append(point['price_return_pct'])

            if week_changes:
                mean_sc = sum(week_changes) / len(week_changes)
                # For long signals: negative score_change = price rose = "correct"
                # For short signals: positive score_change = price fell = "correct"
                if config['direction'] == 'long':
                    pct_correct = sum(1 for x in week_changes if x < 0) / len(week_changes) * 100
                else:
                    pct_correct = sum(1 for x in week_changes if x > 0) / len(week_changes) * 100

                point_data = {
                    'week': week,
                    'n_signals': len(week_changes),
                    'mean_score_change': round(mean_sc, 3),
                    'pct_correct_direction': round(pct_correct, 1),
                }

                # Add price return stats when available
                if week_returns:
                    mean_ret = sum(week_returns) / len(week_returns)
                    if config['direction'] == 'long':
                        win_rate = sum(1 for x in week_returns if x > 0) / len(week_returns) * 100
                    else:
                        win_rate = sum(1 for x in week_returns if x < 0) / len(week_returns) * 100
                    point_data['n_with_price'] = len(week_returns)
                    point_data['mean_price_return_pct'] = round(mean_ret, 2)
                    point_data['win_rate_pct'] = round(win_rate, 1)

                response_curve.append(point_data)

        results[signal_type] = {
            'label': config['label'],
            'direction': config['direction'],
            'total_events': len(type_events),
            'horizons': horizon_stats,
            'response_curve': response_curve,
        }

    return results


def per_stock_tracking(enriched_events: list) -> dict:
    """Group tracking data by stock for per-page display.

    Returns dict keyed by ticker with signal history + summary.
    """
    by_stock = defaultdict(list)
    for event in enriched_events:
        by_stock[event['ticker']].append(event)

    stock_tracking = {}
    for ticker, events in by_stock.items():
        signals = []
        for e in events:
            signals.append({
                'date': e['entry_date'],
                'type': e['signal_type'],
                'score': e['entry_score'],
                'outcomes': e['outcomes'],
            })

        stock_tracking[ticker] = {
            'signal_count': len(events),
            'signals': signals,
        }

    return stock_tracking


def main():
    verbose = '--verbose' in sys.argv or '-v' in sys.argv

    if not HISTORY_FILE.exists():
        print(f"ERROR: {HISTORY_FILE} not found. Need weekly history data.")
        sys.exit(1)

    with open(HISTORY_FILE) as f:
        history = json.load(f)

    if verbose:
        print(f"Loaded {len(history)} weekly snapshots")
        if history:
            print(f"  Date range: {history[0]['date']} to {history[-1]['date']}")
            print(f"  Stocks in latest: {len(history[-1].get('scores', {}))}")

    # Step 1: Detect signal events (threshold crossings)
    events = detect_signal_events(history)
    if verbose:
        print(f"\nDetected {len(events)} signal events:")
        by_type = defaultdict(int)
        for e in events:
            by_type[e['signal_type']] += 1
        for t, c in sorted(by_type.items()):
            print(f"  {t}: {c}")

    # Step 2: Compute outcomes at each horizon
    enriched = compute_returns_from_scores(history, events)

    # Step 3: Aggregate results
    aggregate = aggregate_results(enriched)

    # Step 4: Per-stock tracking
    stock_tracking = per_stock_tracking(enriched)

    # Step 5: Compute data sufficiency
    total_weeks = len(history)
    min_weeks_for_stats = 13  # Need at least 13 weeks for 13w horizon
    data_sufficient = total_weeks >= min_weeks_for_stats

    output = {
        'generated_at': datetime.utcnow().isoformat(),
        'data_range': {
            'first_date': history[0]['date'] if history else None,
            'last_date': history[-1]['date'] if history else None,
            'n_weeks': total_weeks,
            'sufficient_for_analysis': data_sufficient,
        },
        'signal_thresholds': SIGNAL_THRESHOLDS,
        'horizons_weeks': HORIZONS,
        'aggregate_results': aggregate,
        'per_stock': stock_tracking,
        'methodology': {
            'signal_detection': 'Threshold crossing — stock enters a new σ zone that it was not in the prior week',
            'outcome_metric': 'Bean Score change at horizon (mean reversion = signal was correct)',
            'interpretation': {
                'long_signals': 'Score > 1σ means cheap. If score subsequently drops, it means price rose toward fair value (signal was correct).',
                'short_signals': 'Score < -1σ means expensive. If score subsequently rises, it means price fell toward fair value (signal was correct).',
            },
            'limitations': [
                'Only tracks score change, not actual price return (until price history is added)',
                f'Currently only {total_weeks} weeks of data — need 13+ for meaningful statistics',
                'Earnings resets can invalidate the signal basis (FCF changes)',
                'Not a backtest — data collection started May 2026',
            ],
        },
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"✓ Bean Score tracking written to {OUTPUT_FILE}")
    print(f"  {len(events)} signal events across {len(stock_tracking)} stocks")
    print(f"  Data depth: {total_weeks} weeks ({'sufficient' if data_sufficient else 'INSUFFICIENT — need 13+ weeks'})")

    if not data_sufficient:
        print(f"\n  ⚠️  Only {total_weeks} weeks of history available.")
        print(f"     Statistical analysis will become meaningful after ~13 weeks.")
        print(f"     Continue running weekly updates to accumulate data.")


if __name__ == '__main__':
    main()
