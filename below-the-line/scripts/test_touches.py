#!/usr/bin/env python3
"""Quick test: compare old vs new find_historical_touches on GME."""

import pandas as pd
import numpy as np
import yfinance as yf


def fetch_and_prep(symbol: str) -> pd.DataFrame:
    """Fetch weekly data and compute 200WMA + pct_from_wma."""
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="max", interval="1wk")
    df = df.rename(columns={'Close': 'close'})
    df['adjusted_close'] = df['close']
    df['WMA_200'] = df['adjusted_close'].rolling(window=200, min_periods=50).mean()
    df['pct_from_wma'] = ((df['adjusted_close'] - df['WMA_200']) / df['WMA_200']) * 100
    return df.dropna(subset=['WMA_200'])


def old_find_historical_touches(df):
    """OLD version - one touch per cross-below event (the buggy one)."""
    df = df.copy()
    df['below'] = df['adjusted_close'] < df['WMA_200']
    df['cross_below'] = df['below'] & ~df['below'].shift(1).fillna(False)
    touches = []
    cross_dates = df[df['cross_below']].index.tolist()
    for cross_date in cross_dates:
        idx = df.index.get_loc(cross_date)
        subsequent = df.iloc[idx:]
        cross_above = subsequent[~subsequent['below']]
        if len(cross_above) > 0:
            end_idx = df.index.get_loc(cross_above.index[0])
            weeks = end_idx - idx
        else:
            weeks = len(subsequent[subsequent['below']])
        touches.append({'date': cross_date.strftime('%b %Y'), 'weeks': weeks})
    return touches


def new_find_historical_touches(df, recovery_weeks=2):
    """NEW version - merges oscillations using hysteresis."""
    df = df.copy()
    df['below'] = df['adjusted_close'] < df['WMA_200']
    touches = []
    i = 0
    n = len(df)
    while i < n:
        if not df.iloc[i]['below']:
            i += 1
            continue
        episode_start = i
        j = i + 1
        consecutive_above = 0
        episode_end = None
        while j < n:
            if not df.iloc[j]['below']:
                consecutive_above += 1
                if consecutive_above >= recovery_weeks:
                    episode_end = j - consecutive_above + 1
                    break
            else:
                consecutive_above = 0
            j += 1
        start_date = df.index[episode_start]
        if episode_end is not None:
            weeks = episode_end - episode_start
            min_pct = df.iloc[episode_start:episode_end]['pct_from_wma'].min()
            recovery_date = df.index[episode_end].strftime('%b %Y')
            touches.append({
                'date': start_date.strftime('%b %Y'),
                'recovery': recovery_date,
                'weeks': weeks,
                'max_depth': round(abs(min_pct), 1)
            })
            i = episode_end
        else:
            remaining = df.iloc[episode_start:]
            weeks = len(remaining)
            min_pct = remaining['pct_from_wma'].min()
            touches.append({
                'date': start_date.strftime('%b %Y'),
                'recovery': 'ongoing',
                'weeks': weeks,
                'max_depth': round(abs(min_pct), 1)
            })
            break
    return touches


if __name__ == '__main__':
    for symbol in ['GME', 'AAPL', 'MSFT']:
        print(f"\n{'='*50}")
        print(f"  {symbol}")
        print(f"{'='*50}")

        df = fetch_and_prep(symbol)

        old = old_find_historical_touches(df)
        new = new_find_historical_touches(df)

        print(f"\n  OLD method: {len(old)} touches")
        print(f"  NEW method: {len(new)} touches")

        print(f"\n  NEW touches detail:")
        for t in new:
            print(f"    {t['date']:>8}  |  {t['weeks']:>3} weeks  |  "
                  f"depth {t['max_depth']:>5}%  |  recovered {t.get('recovery','?')}")
