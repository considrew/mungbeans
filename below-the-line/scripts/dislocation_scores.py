"""
dislocation_scores.py — Bean Score-adjacent dislocation signals.

Each score follows the Bean Score pattern: deviation from the stock's OWN
baseline, normalized by its own variability where history is deep enough to
support a real sigma. Positive always means "cheaper / more dislocated than
this stock's norm."

True z-scores (long weekly price history):
  yield_dislocation_z   TTM dividend yield vs own trailing 10-yr distribution.
                        +2.0 = yield two sigmas above its own norm (classic
                        Weiss signal). Null for non-payers / short histories.
  drawdown_z            Current pct_from_wma vs the stock's own full-history
                        distribution of pct_from_wma. Separates a habitual
                        diver at -30% (unremarkable) from a steady compounder
                        at -8% (rare event).

Cross-sectional (computed across the whole universe in a post-pass):
  sector_relative_z     pct_from_wma vs sector median, scaled by the sector's
                        current dispersion. + = idiosyncratically cheaper than
                        its sector (not just a sector-wide selloff).
  insider_intensity_pct TTM insider buy dollars / market cap, percentile rank
                        among stocks with any insider buying. 95 = top 5%.

Baseline deltas (yfinance exposes only ~4 annual statement points, so these
are honest percentage-point deltas from the stock's own recent baseline — NOT
sigmas; do not present them as z-scores):
  buyback_accel_pp      YoY share-count change minus the 3-yr annualized rate.
                        Negative = repurchases accelerating vs own cadence.
  accrual_gap_trend_pp  (Net income − FCF) / revenue, latest year vs prior-year
                        mean. Positive = earnings increasingly outrunning cash
                        — the classic value-trap tell. Surfaced as
                        earnings_quality: improving | stable | deteriorating.
  fcf_yield_vs_hist_pp  Current FCF yield minus own recent annual mean.
                        + = cheaper on cash flow than its own recent norm.

dislocation_stack counts aligned cheapness signals (0-6). earnings_quality is
a gate, not a stack member: a deteriorating accrual gap sets quality_warning
instead of adding to the stack.

Known v1 caveats:
- Dividend yields are computed on the adjusted-price series, which slightly
  inflates older yields for long-history payers; the 10-yr window bounds this.
- Annual-statement deltas are coarse by construction (4 data points).
"""

import math
from typing import Optional

import pandas as pd

# Thresholds for the stack
Z_THRESHOLD = 1.5
BUYBACK_ACCEL_THRESHOLD = -2.0   # pp/yr faster than own cadence
INSIDER_PCTL_THRESHOLD = 90.0
FCF_VS_HIST_THRESHOLD = 2.0      # pp cheaper than own recent norm
ACCRUAL_DETERIORATING = 3.0      # pp of revenue
ACCRUAL_IMPROVING = -3.0

YIELD_WINDOW_WEEKS = 520         # 10 years
MIN_YIELD_WEEKS = 156            # 3 years of payment history


def _clean(vals):
    return [float(v) for v in vals
            if v is not None and not (isinstance(v, float) and math.isnan(v))]


def _zscore(current: float, history: list, min_n: int) -> Optional[float]:
    vals = _clean(history)
    if current is None or len(vals) < min_n:
        return None
    mu = sum(vals) / len(vals)
    var = sum((v - mu) ** 2 for v in vals) / len(vals)
    sd = math.sqrt(var)
    if sd < 1e-9:
        return None
    return round((float(current) - mu) / sd, 2)


def yield_dislocation_z(df: pd.DataFrame) -> Optional[float]:
    """TTM dividend yield vs the stock's own trailing 10-yr yield distribution."""
    if 'Dividends' not in df.columns:
        return None
    try:
        ttm = df['Dividends'].rolling(window=52, min_periods=40).sum()
        yld = (ttm / df['adjusted_close']) * 100.0
        yld = yld.dropna()
        yld = yld[yld > 0].tail(YIELD_WINDOW_WEEKS)
        if len(yld) < MIN_YIELD_WEEKS:
            return None
        current = float(yld.iloc[-1])
        if current <= 0:
            return None
        return _zscore(current, list(yld.iloc[:-1]), min_n=MIN_YIELD_WEEKS - 1)
    except Exception:
        return None


def drawdown_z(df: pd.DataFrame) -> Optional[float]:
    """Current pct_from_wma vs the stock's own full-history distribution.

    Sign-flipped so positive = unusually far BELOW its own norm.
    """
    try:
        series = df['pct_from_wma'].dropna()
        if len(series) < 104:
            return None
        current = float(series.iloc[-1])
        z = _zscore(current, list(series.iloc[:-1]), min_n=103)
        return None if z is None else round(-z, 2)
    except Exception:
        return None


def buyback_accel_pp(fundamentals: dict) -> Optional[float]:
    """YoY share-count change minus 3-yr annualized change. − = accelerating buybacks."""
    yoy = fundamentals.get('shares_change_yoy')
    three = fundamentals.get('shares_change_3yr')
    if yoy is None or three is None:
        return None
    try:
        annualized = (((1.0 + three / 100.0) ** (1.0 / 3.0)) - 1.0) * 100.0
        return round(yoy - annualized, 2)
    except (ValueError, ZeroDivisionError):
        return None


def accrual_gap_trend(health_chart: dict) -> tuple:
    """Returns (accrual_gap_trend_pp, earnings_quality).

    Gap = (net income − FCF) / revenue per fiscal year. Trend = latest gap vs
    mean of prior years. Positive trend = quality deteriorating.
    """
    try:
        years = health_chart.get('years') or []
        gaps = []
        for ni, fcf, rev in zip(health_chart.get('net_income') or [],
                                health_chart.get('fcf') or [],
                                health_chart.get('revenue') or []):
            if ni is None or fcf is None or rev is None or rev <= 0:
                gaps.append(None)
            else:
                gaps.append((ni - fcf) / rev * 100.0)
        valid = [(y, g) for y, g in zip(years, gaps) if g is not None]
        if len(valid) < 3:
            return None, 'insufficient_data'
        latest = valid[-1][1]
        prior = [g for _, g in valid[:-1]]
        trend = round(latest - (sum(prior) / len(prior)), 2)
        if trend >= ACCRUAL_DETERIORATING:
            quality = 'deteriorating'
        elif trend <= ACCRUAL_IMPROVING:
            quality = 'improving'
        else:
            quality = 'stable'
        return trend, quality
    except Exception:
        return None, 'insufficient_data'


def fcf_yield_vs_hist_pp(fundamentals: dict) -> Optional[float]:
    """Current FCF yield minus the stock's own recent annual mean (pp)."""
    current = fundamentals.get('fcf_yield')
    health = fundamentals.get('health_chart') or {}
    hist = _clean(health.get('fcf_yield') or [])
    if current is None or len(hist) < 3:
        return None
    return round(float(current) - (sum(hist) / len(hist)), 2)


def compute_stock_dislocation(df: pd.DataFrame, fundamentals: dict) -> dict:
    """Per-stock scores computable from one stock's own data.

    Cross-sectional fields (sector_relative_z, insider_intensity_pct, the
    stack) are filled in later by apply_cross_sectional_dislocation().
    """
    trend_pp, quality = accrual_gap_trend(fundamentals.get('health_chart') or {})
    return {
        'yield_dislocation_z': yield_dislocation_z(df),
        'drawdown_z': drawdown_z(df),
        'buyback_accel_pp': buyback_accel_pp(fundamentals),
        'accrual_gap_trend_pp': trend_pp,
        'earnings_quality': quality,
        'fcf_yield_vs_hist_pp': fcf_yield_vs_hist_pp(fundamentals),
        # Filled in post-pass:
        'sector_relative_z': None,
        'insider_intensity_pct': None,
        'dislocation_stack': 0,
        'stack_signals': [],
        'quality_warning': quality == 'deteriorating',
    }


def apply_cross_sectional_dislocation(all_stocks: list) -> None:
    """Post-pass over the full universe. Mutates stocks in place.

    Must run AFTER sector metadata has been merged onto each stock.
    """
    # ── Sector-relative dislocation ──
    by_sector = {}
    for s in all_stocks:
        sector = s.get('sector') or ''
        if sector and s.get('pct_from_wma') is not None:
            by_sector.setdefault(sector, []).append(float(s['pct_from_wma']))

    sector_stats = {}
    for sector, vals in by_sector.items():
        if len(vals) < 8:
            continue
        srt = sorted(vals)
        n = len(srt)
        median = srt[n // 2] if n % 2 else (srt[n // 2 - 1] + srt[n // 2]) / 2.0
        mu = sum(vals) / n
        sd = math.sqrt(sum((v - mu) ** 2 for v in vals) / n)
        if sd > 1e-9:
            sector_stats[sector] = (median, sd)

    # ── Insider intensity percentile ──
    candidates = []
    for s in all_stocks:
        total = s.get('insider_buy_total_12m') or 0
        mcap = s.get('market_cap')
        if total > 0 and mcap and mcap > 0:
            candidates.append((s['symbol'], total / mcap))
    candidates.sort(key=lambda t: t[1])
    n_cand = len(candidates)
    intensity_pct = {sym: round((rank + 1) / n_cand * 100.0, 1)
                     for rank, (sym, _) in enumerate(candidates)} if n_cand >= 10 else {}

    # ── Fill per-stock + build the stack ──
    for s in all_stocks:
        d = s.get('dislocation')
        if d is None:
            continue

        stats = sector_stats.get(s.get('sector') or '')
        if stats and s.get('pct_from_wma') is not None:
            median, sd = stats
            d['sector_relative_z'] = round(-(float(s['pct_from_wma']) - median) / sd, 2)

        d['insider_intensity_pct'] = intensity_pct.get(s['symbol'])

        signals = []
        if (d['yield_dislocation_z'] or 0) >= Z_THRESHOLD:
            signals.append('yield')
        if (d['drawdown_z'] or 0) >= Z_THRESHOLD:
            signals.append('drawdown')
        if (d['sector_relative_z'] or 0) >= Z_THRESHOLD:
            signals.append('sector')
        if (d['buyback_accel_pp'] is not None
                and d['buyback_accel_pp'] <= BUYBACK_ACCEL_THRESHOLD
                and (s.get('shares_change_yoy') or 0) < 0):
            signals.append('buyback')
        if (d['insider_intensity_pct'] or 0) >= INSIDER_PCTL_THRESHOLD:
            signals.append('insider')
        if (d['fcf_yield_vs_hist_pp'] or 0) >= FCF_VS_HIST_THRESHOLD:
            signals.append('value_vs_history')

        d['stack_signals'] = signals
        d['dislocation_stack'] = len(signals)
