#!/usr/bin/env python3
"""
signals -- the quantified read behind the tingle.

Each function here turns a piece of "the move is exhausting and turning" into a
number you can threshold, so the watcher fires on a confluence of real signals
rather than a hand-tuned score. Nothing here is proprietary or clever; these
are the standard mean-reversion reads, chosen because each measures a different
axis of the same intuition:

  VWAP sigma-extension  how far price is stretched from the session's
                        volume-weighted fair price, in standard deviations.
                        The spine -- reversion is, definitionally, a move back
                        toward VWAP.

  RSI(2) and RSI(14)    momentum exhaustion, fast and slow. RSI(2) is Connors'
                        index mean-reversion workhorse: >95 is stretched up,
                        <5 stretched down.

  ATR-normalized move   the move from the prior close measured in units of the
                        normal daily range, so a 2% day counts for more when
                        the tape is quiet (VIX 12) than when it is wild (VIX
                        30). This is what raw percent cannot see.

  Bollinger tag-reject  price pierced a 2-sigma band and closed back inside --
                        the reversal bar itself, not just an extended reading.

  Prior-day level tag   price tagged the prior session's high or low and was
                        rejected. Those levels are reversion magnets.

Index options are priced on SPX, but SPX has no share volume, so VWAP and the
band reads are computed on SPY (its tradeable proxy) and QQQ directly. The
direction is what carries over; the option is still struck on the real
underlying by the caller.

    python3 signals.py --selftest        # math only, no network
    python3 signals.py --symbol SPY      # live read
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DAILY_CACHE = HERE / '.daily_cache.json'
TIMEOUT = 20
UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'}

# The symbol whose bars we read for signals, per option underlying.
PROXY = {'SPX': 'SPY', 'SPY': 'SPY', 'QQQ': 'QQQ'}


# --- pure signal math: covered by --selftest ---------------------------------

def vwap(bars) -> float:
    """Session VWAP from (o,h,l,c,volume) bars, on the typical price."""
    num = den = 0.0
    for o, h, l, c, v in bars:
        tp = (h + l + c) / 3.0
        num += tp * v
        den += v
    return num / den if den else (bars[-1][3] if bars else 0.0)


def vwap_sigma(spot: float, bars) -> float:
    """Extension from VWAP in standard deviations of the session's own spread.

    The denominator is the dispersion of typical price around VWAP across the
    session, so '2 sigma' means stretched relative to how this particular day
    has been trading, not an absolute percent.
    """
    vw = vwap(bars)
    devs = [((h + l + c) / 3.0) - vw for _, h, l, c, _ in bars]
    sd = st.pstdev(devs) if len(devs) > 2 else 0.0
    return (spot - vw) / sd if sd else 0.0


def rsi(closes, n: int):
    """Wilder's RSI over `closes`. None if too few points."""
    if len(closes) <= n:
        return None
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    ag, al = gains / n, losses / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def atr_pct(daily, n: int = 14):
    """ATR(n) as a percent of price, from daily (o,h,l,c) bars.

    True range includes the overnight gap, which is the point -- an
    ATR-normalized move should be measured against ranges that include gaps,
    since gaps are exactly what this strategy trades.
    """
    if len(daily) < n + 1:
        return None
    trs = []
    for i in range(1, len(daily)):
        o, h, l, c = daily[i]
        pc = daily[i - 1][3]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[-n:]) / n
    last_close = daily[-1][3]
    return 100.0 * atr / last_close if last_close else None


def atr_extension(move_pct: float, atrp) -> float:
    """The move from prior close, in units of ATR%. 1.5 = a-day-and-a-half's
    worth of normal range spent in this one move."""
    if not atrp or atrp <= 0:
        return 0.0
    return abs(move_pct) / atrp


def bollinger_pctb(closes, n: int = 20, k: float = 2.0):
    """%b: 1.0 is the upper band, 0.0 the lower. None if too few points."""
    if len(closes) < n:
        return None
    window = closes[-n:]
    m = st.mean(window)
    sd = st.pstdev(window)
    if sd == 0:
        return 0.5
    upper, lower = m + k * sd, m - k * sd
    return (closes[-1] - lower) / (upper - lower)


def band_tag_reject(closes, n: int = 20, k: float = 2.0, lookback: int = 6):
    """Return +1 if price pierced the UPPER band recently and has closed back
    inside (a fade-up confirmation), -1 for the lower band (fade-down), else 0.

    This is the reversal bar, not merely an extended reading: the extension has
    to have happened AND started to unwind.
    """
    if len(closes) < n + 1:
        return 0
    now = bollinger_pctb(closes, n, k)
    if now is None:
        return 0
    recent = [bollinger_pctb(closes[:len(closes) - j], n, k) for j in range(1, lookback + 1)]
    recent = [x for x in recent if x is not None]
    if recent and max(recent) > 1.0 and now <= 1.0:
        return +1
    if recent and min(recent) < 0.0 and now >= 0.0:
        return -1
    return 0


def prior_day_tag(spot: float, session_high: float, session_low: float,
                  pdh: float, pdl: float, tol: float = 0.0007):
    """Return +1 if the session tagged the prior-day HIGH and price is back
    below it (fade-up), -1 for the prior-day LOW tagged and reclaimed
    (fade-down), else 0."""
    if pdh and session_high >= pdh * (1 - tol) and spot < pdh:
        return +1
    if pdl and session_low <= pdl * (1 + tol) and spot > pdl:
        return -1
    return 0


# --- confluence decision -----------------------------------------------------

def evaluate_signals(sig: dict, cfg: dict) -> dict:
    """Turn the raw signal reads into per-signal votes and a fire decision.

    A vote is +1 (confirms fading an UP move -> buy puts), -1 (confirms fading
    a DOWN move -> buy calls), or 0. The move's own direction sets which side
    we are even considering; a signal only counts if it confirms overextension
    in that direction. VWAP is the required spine; the rest are confirmations.
    """
    up = sig['move_pct'] > 0            # an up day is faded with puts
    want = +1 if up else -1
    votes = {}

    z = sig['vwap_sigma']
    votes['vwap'] = +1 if z >= cfg['vwap_sigma_gate'] else (-1 if z <= -cfg['vwap_sigma_gate'] else 0)

    r2 = sig.get('rsi2')
    if r2 is None:
        votes['rsi2'] = 0
    else:
        votes['rsi2'] = +1 if r2 >= cfg['rsi2_hi'] else (-1 if r2 <= cfg['rsi2_lo'] else 0)

    ext = sig.get('atr_ext', 0.0)
    votes['atr'] = want if ext >= cfg['atr_ext_min'] else 0

    votes['band'] = sig.get('band_tag', 0)
    votes['pdl'] = sig.get('pd_tag', 0)

    # count confirmations that agree with the fade side
    spine_ok = votes['vwap'] == want
    confirmations = [k for k, v in votes.items() if k != 'vwap' and v == want]
    enabled_conf = [k for k in cfg['enabled'] if k != 'vwap']
    confirmations = [k for k in confirmations if k in enabled_conf]

    fired = (('vwap' not in cfg['enabled']) or spine_ok) and \
            len(confirmations) >= cfg['min_confirm']

    return {
        'side': 'P' if up else 'C',
        'votes': votes,
        'spine_ok': spine_ok,
        'confirmations': confirmations,
        'n_confirm': len(confirmations),
        'fired': fired,
    }


# --- network -----------------------------------------------------------------

def _get(url: str, retries: int = 3) -> bytes:
    """GET with backoff. Yahoo's chart endpoints 429 intermittently under any
    polling; a couple of spaced retries clears it far more often than not."""
    last = None
    for i in range(retries):
        try:
            return urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=TIMEOUT).read()
        except urllib.error.HTTPError as e:
            last = e
            if e.code != 429 or i == retries - 1:
                raise
            time.sleep(3 * (i + 1))
    raise last


def fetch_intraday_bars(symbol: str):
    """Today's 5-minute (o,h,l,c,v) bars plus prev close, via Yahoo charts."""
    raw = json.loads(_get(f'https://query1.finance.yahoo.com/v8/finance/chart/'
                          f'{symbol}?interval=5m&range=1d&includePrePost=false'))
    res = raw['chart']['result']
    if not res:
        return None
    res = res[0]
    q = res['indicators']['quote'][0]
    prev = res['meta'].get('chartPreviousClose') or res['meta'].get('previousClose')
    bars = []
    for i in range(len(res['timestamp'])):
        o, h, l, c, v = (q['open'][i], q['high'][i], q['low'][i],
                         q['close'][i], q['volume'][i])
        if None in (o, h, l, c):
            continue
        bars.append((o, h, l, c, v or 0))
    if not bars or not prev:
        return None
    return prev, bars


def fetch_daily(symbol: str):
    """Prior-day high/low and ATR% from ~2 months of daily bars.

    Cached to disk per (symbol, date): these change once a day, so there is no
    reason to refetch them every poll and every reason not to.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    try:
        cache = json.loads(DAILY_CACHE.read_text())
    except Exception:
        cache = {}
    key = f'{symbol}:{today}'
    if key in cache:
        return cache[key]

    raw = json.loads(_get(f'https://query1.finance.yahoo.com/v8/finance/chart/'
                          f'{symbol}?interval=1d&range=2mo'))
    res = raw['chart']['result'][0]
    q = res['indicators']['quote'][0]
    daily = []
    for i in range(len(res['timestamp'])):
        o, h, l, c = q['open'][i], q['high'][i], q['low'][i], q['close'][i]
        if None in (o, h, l, c):
            continue
        daily.append((o, h, l, c))
    if len(daily) < 16:
        return None
    # last bar is today (partial); prior day is the one before it
    pdh, pdl = daily[-2][1], daily[-2][2]
    out = {'pdh': pdh, 'pdl': pdl, 'atr_pct': atr_pct(daily, 14)}
    cache[key] = out
    try:
        DAILY_CACHE.write_text(json.dumps(cache))
    except Exception:
        pass
    return out


def compute(option_symbol: str):
    """Full signal read for an option underlying, using its proxy's bars."""
    proxy = PROXY.get(option_symbol.upper(), option_symbol.upper())
    try:
        intr = fetch_intraday_bars(proxy)
    except Exception as e:
        return {'symbol': option_symbol, 'proxy': proxy, 'error': f'intraday fetch failed: {e}'}
    if not intr:
        return {'symbol': option_symbol, 'proxy': proxy, 'error': 'no intraday bars'}
    prev, bars = intr
    closes = [b[3] for b in bars]
    spot = closes[-1]
    session_high = max(b[1] for b in bars)
    session_low = min(b[2] for b in bars)
    move_pct = 100.0 * (spot - prev) / prev

    daily = None
    try:
        daily = fetch_daily(proxy)
    except Exception:
        pass
    atrp = daily['atr_pct'] if daily else None
    pdh = daily['pdh'] if daily else None
    pdl = daily['pdl'] if daily else None

    return {
        'symbol': option_symbol, 'proxy': proxy,
        'spot': round(spot, 2), 'prev_close': round(prev, 2),
        'move_pct': round(move_pct, 3),
        'vwap': round(vwap(bars), 2),
        'vwap_sigma': round(vwap_sigma(spot, bars), 2),
        'rsi2': round(rsi(closes, 2), 1) if rsi(closes, 2) is not None else None,
        'rsi14': round(rsi(closes, 14), 1) if rsi(closes, 14) is not None else None,
        'atr_pct': round(atrp, 2) if atrp else None,
        'atr_ext': round(atr_extension(move_pct, atrp), 2),
        'pctb': round(bollinger_pctb(closes), 2) if bollinger_pctb(closes) is not None else None,
        'band_tag': band_tag_reject(closes),
        'pd_tag': prior_day_tag(spot, session_high, session_low, pdh, pdl),
        'session_high': round(session_high, 2), 'session_low': round(session_low, 2),
        'error': None,
    }


# --- selftest ----------------------------------------------------------------

def selftest() -> int:
    # VWAP: flat volume, symmetric prices -> mean
    bars = [(10, 10, 10, 10, 100), (12, 12, 12, 12, 100)]
    assert abs(vwap(bars) - 11.0) < 1e-9

    # RSI: monotonic rise -> 100, monotonic fall -> 0
    assert rsi([1, 2, 3, 4, 5], 2) == 100.0
    assert rsi([5, 4, 3, 2, 1], 2) == 0.0
    mixed = rsi([1, 2, 1, 2, 1, 2, 1, 2], 2)
    assert 0 < mixed < 100

    # ATR%: constant 2-wide daily range on a 100 close -> ~2%
    daily = [(100, 101, 99, 100)] * 20
    assert abs(atr_pct(daily, 14) - 2.0) < 1e-6
    assert abs(atr_extension(3.0, 2.0) - 1.5) < 1e-9
    assert atr_extension(3.0, None) == 0.0

    # Bollinger %b: last point at the mean -> 0.5
    flat = [100.0] * 19 + [100.0]
    assert abs(bollinger_pctb(flat) - 0.5) < 1e-9
    # a flat base then a jump lands in the upper half of the band
    jumped = [100.0] * 19 + [104.0]
    assert bollinger_pctb(jumped) > 0.5

    # band tag-reject: spike above the band then back inside -> +1
    seq = [100.0] * 20 + [130.0, 101.0]
    assert band_tag_reject(seq) == +1

    # prior-day tag: tagged the high, now below it -> +1
    assert prior_day_tag(spot=99.9, session_high=100.05, session_low=98.0,
                         pdh=100.0, pdl=95.0) == +1
    assert prior_day_tag(spot=95.1, session_high=99.0, session_low=94.95,
                         pdh=100.0, pdl=95.0) == -1

    # confluence: an up move confirmed by vwap+rsi+atr must fire; a lone signal must not
    cfg = {'enabled': ['vwap', 'rsi2', 'atr', 'band', 'pdl'],
           'vwap_sigma_gate': 2.0, 'rsi2_hi': 95, 'rsi2_lo': 5,
           'atr_ext_min': 1.5, 'min_confirm': 2}
    hot = evaluate_signals({'move_pct': 1.8, 'vwap_sigma': 2.4, 'rsi2': 97,
                            'atr_ext': 1.7, 'band_tag': 1, 'pd_tag': 0}, cfg)
    assert hot['fired'] and hot['side'] == 'P', hot
    cold = evaluate_signals({'move_pct': 1.8, 'vwap_sigma': 2.4, 'rsi2': 60,
                             'atr_ext': 0.5, 'band_tag': 0, 'pd_tag': 0}, cfg)
    assert not cold['fired'], cold
    # spine failing (vwap not stretched) blocks the fire even with confirmations
    nospine = evaluate_signals({'move_pct': 1.8, 'vwap_sigma': 0.3, 'rsi2': 97,
                                'atr_ext': 1.7, 'band_tag': 1, 'pd_tag': 0}, cfg)
    assert not nospine['fired'], nospine

    print('selftest: all checks passed')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--symbol', default='SPY')
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--selftest', action='store_true')
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    res = compute(args.symbol)
    print(json.dumps(res, indent=2) if args.json else res)
    return 0


if __name__ == '__main__':
    sys.exit(main())
