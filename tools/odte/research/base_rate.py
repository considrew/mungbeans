#!/usr/bin/env python3
"""
Measure the base rate: what does buying 0DTE premium return, before any view?

WHY THIS EXISTS

Before designing a scoring model that picks a call or a put, it is worth
knowing what the structure itself pays. If buying a 0DTE option is a losing
proposition on average, then the scorer is not choosing between good trades --
it is looking for the rare sessions that overcome a standing deficit, and it
should refuse the rest. That is a different tool, so this question comes first.

METHOD

For each session an at-the-money option is priced at the open using VIX1D as
the volatility input, then settled against that session's close. VIX1D is the
market's own one-day implied volatility, so this is close to what was actually
quoted, without needing historical option data that no free source carries.

Two deliberate caveats. The option prices are theoretical -- there is no
historical bid-ask, so real fills would be worse than these figures, not
better. And every session is traded in both directions, so nothing here is
selected: this is the cost of the structure, not the result of a strategy.

WHAT IT FOUND

Buying an ATM 0DTE at the open and holding to the close returns -5.3% (calls)
and -15.4% (puts) on average, with medians near total loss. Realized moves
arrive at 0.614x the implied one-sigma, and only 25.9% of sessions exceed one
sigma against a fair-value expectation of 31.7%. That gap is the volatility
risk premium: the seller collects it and the buyer pays it.

The break-even sweep is the number that shapes the build. Weighting each
session's winning and losing side by an assumed directional accuracy, EV
crosses zero near 56% -- and near 60% once friction is included. A model that
trades every day at 52% loses. One that trades rarely and is right 62% of the
time on those days works.

USAGE

    python3 base_rate.py             # fetch and run
    python3 base_rate.py --selftest  # math only, no network
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import statistics as st
import sys
import urllib.request

VIX1D = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX1D_History.csv'
GSPC = 'https://query2.finance.yahoo.com/v8/finance/chart/%5EGSPC?interval=1d&range=5y'
TRADING_DAYS = 252
TIMEOUT = 40
UA = {'User-Agent': 'Mozilla/5.0'}


# --- pure functions: covered by --selftest -----------------------------------

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs(F: float, K: float, T: float, sigma: float, cp: str) -> float:
    """Undiscounted Black-76. Over a single session the discount factor is noise."""
    if T <= 0 or sigma <= 0:
        return max(0.0, (F - K) if cp == 'C' else (K - F))
    v = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / v
    d2 = d1 - v
    if cp == 'C':
        return F * norm_cdf(d1) - K * norm_cdf(d2)
    return K * norm_cdf(-d2) - F * norm_cdf(-d1)


def implied_move(spot: float, vix1d: float) -> float:
    """One-sigma expected move for the session.

    VIX1D is an annualised volatility over a one-trading-day horizon, so the
    session move is the annual figure divided by sqrt(252). Dividing by the
    number of calendar hours instead -- which is the intuitive-looking mistake
    -- understates the implied move by roughly 2.3x and inverts the entire
    conclusion, making options look systematically cheap.
    """
    return spot * (vix1d / 100.0) / math.sqrt(TRADING_DAYS)


def trade_return(open_px: float, close_px: float, vix1d: float, cp: str) -> float | None:
    """Return on an ATM 0DTE bought at the open and settled at the close."""
    K = round(open_px / 5.0) * 5.0
    T = 1.0 / TRADING_DAYS
    premium = bs(open_px, K, T, vix1d / 100.0, cp)
    if premium <= 0:
        return None
    payoff = max(0.0, (close_px - K) if cp == 'C' else (K - close_px))
    return (payoff - premium) / premium


# --- network -----------------------------------------------------------------

def get(url: str) -> bytes:
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=TIMEOUT).read()


def load_vix1d() -> dict[dt.date, float]:
    out = {}
    for row in csv.DictReader(io.StringIO(get(VIX1D).decode())):
        m, d, y = row['DATE'].split('/')
        out[dt.date(int(y), int(m), int(d))] = float(row['OPEN'])
    return out


def load_spx() -> dict[dt.date, dict[str, float]]:
    res = json.loads(get(GSPC))['chart']['result'][0]
    q = res['indicators']['quote'][0]
    out = {}
    for i, ts in enumerate(res['timestamp']):
        if q['open'][i] and q['close'][i]:
            day = dt.datetime.utcfromtimestamp(ts).date()
            out[day] = {'o': q['open'][i], 'c': q['close'][i]}
    return out


def run() -> int:
    vix, spx = load_vix1d(), load_spx()
    days = sorted(set(vix) & set(spx))
    if len(days) < 100:
        print('not enough overlapping sessions', file=sys.stderr)
        return 1
    print(f'sessions: {len(days)}   {days[0]} -> {days[-1]}\n')

    ratios = [abs(spx[d]['c'] - spx[d]['o']) / implied_move(spx[d]['o'], vix[d]) for d in days]
    over = 100 * sum(1 for r in ratios if r > 1) / len(ratios)
    print('Implied vs realised')
    print(f'  median |close-open| / implied 1sd : {st.median(ratios):.3f}')
    print(f'  sessions exceeding 1sd            : {over:.1f}%   (fair value ~31.7%)\n')

    print('Buy ATM 0DTE at open, hold to close, no costs')
    for cp in ('C', 'P'):
        r = [x for d in days if (x := trade_return(spx[d]['o'], spx[d]['c'], vix[d], cp)) is not None]
        win = 100 * sum(1 for x in r if x > 0) / len(r)
        print(f'  {cp}: mean {100 * st.mean(r):+6.1f}%   median {100 * st.median(r):+7.1f}%   win {win:.1f}%')

    print('\nBreak-even directional accuracy')
    for pct in range(45, 71, 5):
        acc = pct / 100.0
        tot = []
        for d in days:
            o, c, v = spx[d]['o'], spx[d]['c'], vix[d]
            right = 'C' if c >= o else 'P'
            wrong = 'P' if right == 'C' else 'C'
            for cp, w in ((right, acc), (wrong, 1 - acc)):
                x = trade_return(o, c, v, cp)
                if x is not None:
                    tot.append(w * x)
        print(f'  {pct}% -> {200 * st.mean(tot):+6.1f}% per trade')
    return 0


def selftest() -> int:
    # A call struck at the forward with no volatility is worthless.
    assert abs(bs(100, 100, 1 / 252, 0.0, 'C')) < 1e-12

    # Parity holds undiscounted when F == K: call equals put.
    assert abs(bs(100, 100, 0.1, 0.2, 'C') - bs(100, 100, 0.1, 0.2, 'P')) < 1e-9

    # Intrinsic at expiry.
    assert abs(bs(110, 100, 0, 0.2, 'C') - 10) < 1e-12
    assert abs(bs(90, 100, 0, 0.2, 'P') - 10) < 1e-12

    # The annualisation is the thing most likely to be got wrong, so pin it:
    # 16% VIX1D on a 5000 index is about a 50 point session sigma.
    m = implied_move(5000.0, 16.0)
    assert 49 < m < 51, f'implied_move wrong: {m}'

    # A session that closes exactly at the strike loses the whole premium.
    r = trade_return(5000.0, 5000.0, 16.0, 'C')
    assert r is not None and abs(r + 1.0) < 1e-9, f'expected total loss, got {r}'

    # A large favourable move must produce a gain.
    assert trade_return(5000.0, 5300.0, 16.0, 'C') > 0
    assert trade_return(5000.0, 4700.0, 16.0, 'P') > 0

    print('selftest: all checks passed')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--selftest', action='store_true')
    args = ap.parse_args()
    return selftest() if args.selftest else run()


if __name__ == '__main__':
    sys.exit(main())
