#!/usr/bin/env python3
"""
Validate that we can reprice a live option chain accurately enough to trust.

WHY THIS EXISTS

Every downstream number -- the target option price, the stop, the expected
value of holding another hour -- comes out of an option pricing model. If the
model cannot reproduce the prices actually quoted in the market right now, then
none of those numbers mean anything, and the tool is generating confident
fiction. So this runs first, and the rest of the build is conditional on it.

WHAT IT FOUND

Cboe publishes an implied volatility and a full set of greeks per contract, for
free, and they are not usable. At the same strike and expiry a call and a put
must imply the same volatility -- that is put-call parity, not a modelling
preference. Cboe's figures disagreed by as much as 0.05 vol points, which is
enormous on a contract trading at 0.10 vol.

The cause is the underlying reference. SPX options price off the forward, not
the cash index, and the index quote in this feed is unreliable anyway: it
printed a 7675 / 7748 bid-ask around a 7708 level. Repricing naively against
spot gave a mean absolute error of $7.01 against mid.

The fix is to stop trusting the feed's view of the underlying. The forward is
implied from the options themselves, by inverting put-call parity at the
strikes with the tightest quotes:

    F = K + e^(rT) * (call_mid - put_mid)

That came out 16.17 points above the quoted spot. Repricing off it dropped mean
error to $1.28, and solving our own volatilities from mid prices brought
call-vs-put IV agreement to a median of 0.0001 vol points -- an internally
arbitrage-free surface.

USAGE

    python3 validate_surface.py                 # SPX, nearest expiry
    python3 validate_surface.py --symbol SPY
    python3 validate_surface.py --selftest      # math only, no network
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import urllib.request
from datetime import datetime

CBOE = 'https://cdn.cboe.com/api/global/delayed_quotes/options/{}.json'
RATE = 0.04          # short-rate stand-in; the horizons here make it near-irrelevant
TIMEOUT = 30


# --- pricing: pure functions, covered by --selftest --------------------------

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def black76(F: float, K: float, T: float, r: float, sigma: float, cp: str) -> float:
    """Option price off the forward.

    Black-76 rather than Black-Scholes because we price against an implied
    forward, never against a spot we do not trust.
    """
    df = math.exp(-r * T)
    if T <= 0 or sigma <= 0:
        return df * max(0.0, (F - K) if cp == 'C' else (K - F))
    v = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / v
    d2 = d1 - v
    if cp == 'C':
        return df * (F * norm_cdf(d1) - K * norm_cdf(d2))
    return df * (K * norm_cdf(-d2) - F * norm_cdf(-d1))


def implied_vol(price: float, F: float, K: float, T: float, r: float, cp: str) -> float | None:
    """Volatility that reproduces `price`, by bisection.

    Bisection rather than Newton because vega collapses to nothing on 0DTE
    wings and Newton diverges there. Speed is irrelevant at this scale.
    """
    lo, hi = 1e-4, 5.0
    if black76(F, K, T, r, hi, cp) < price:
        return None
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        if black76(F, K, T, r, mid, cp) < price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def implied_forward(pairs: list[tuple[float, float, float, float]], T: float, r: float,
                    take: int = 5) -> float | None:
    """Forward implied from put-call parity, averaged over the tightest quotes.

    `pairs` is (total_spread, strike, call_mid, put_mid). The tightest strikes
    are the most informative: parity holds exactly in theory and approximately
    in a market, and the approximation is worst where the quotes are widest.
    """
    if not pairs:
        return None
    ranked = sorted(pairs)[:take]
    fwds = [K + math.exp(r * T) * (c - p) for _, K, c, p in ranked]
    return sum(fwds) / len(fwds)


OCC = re.compile(r'^(?P<root>[A-Z]+)(?P<exp>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$')


def parse_occ(sym: str, root: str = '') -> tuple[str, str, float]:
    """OCC contract symbol -> (yymmdd, call/put, strike).

    Parsed by pattern rather than by slicing at a fixed offset, because the
    root is not a fixed width. SPX carries two roots: `SPX` for the AM-settled
    standard monthlies and `SPXW` for the PM-settled weeklies -- and the
    weeklies are where every daily expiry lives. Slicing at a fixed offset
    silently drops the entire 0DTE surface and reports it as an empty chain,
    which is exactly the sort of quiet wrong answer this tool cannot afford.
    """
    m = OCC.match(sym)
    if not m:
        raise ValueError(f'unparseable OCC symbol: {sym}')
    return m.group('exp'), m.group('cp'), int(m.group('strike')) / 1000.0


# --- network -----------------------------------------------------------------

def fetch_chain(symbol: str) -> dict:
    key = '_SPX' if symbol.upper() == 'SPX' else symbol.upper()
    req = urllib.request.Request(CBOE.format(key), headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read())


def run(symbol: str, expiry_override: str | None = None) -> int:
    run._expiry = expiry_override
    data = fetch_chain(symbol)['data']
    root = 'SPX' if symbol.upper() == 'SPX' else symbol.upper()
    spot = data['current_price']
    contracts = data['options']

    stamps = [c['last_trade_time'] for c in contracts if c.get('last_trade_time')]
    if not stamps:
        print('no quote timestamps in feed', file=sys.stderr)
        return 1
    now = datetime.fromisoformat(max(stamps))

    def hours_to(exp: str) -> float:
        settle = datetime.strptime('20' + exp, '%Y%m%d').replace(hour=16)
        return (settle - now).total_seconds() / 3600.0

    # Skip expiries that have already settled. The feed keeps them in the chain
    # after the fact with stale two-sided quotes, and pricing against T <= 0
    # produces intrinsic values that look like a working answer.
    expiries = sorted({parse_occ(c['option'])[0] for c in contracts})
    live = [e for e in expiries if hours_to(e) > 0.25]
    if not live:
        print('no unexpired expiries in chain', file=sys.stderr)
        return 1
    expiry = wanted if (wanted := getattr(run, '_expiry', None)) in live else live[0]
    T = hours_to(expiry) / (365 * 24)

    book: dict[float, dict[str, dict]] = {}
    for c in contracts:
        exp, cp, K = parse_occ(c['option'])
        if exp != expiry or c['bid'] <= 0:
            continue
        book.setdefault(K, {})[cp] = c

    pairs = []
    for K, side in book.items():
        if 'C' not in side or 'P' not in side:
            continue
        call, put = side['C'], side['P']
        if call['volume'] < 200 or put['volume'] < 200 or abs(K - spot) > 150:
            continue
        cm = (call['bid'] + call['ask']) / 2
        pm = (put['bid'] + put['ask']) / 2
        spread = (call['ask'] - call['bid']) + (put['ask'] - put['bid'])
        pairs.append((spread, K, cm, pm))

    F = implied_forward(pairs, T, RATE)
    if F is None:
        print('could not imply a forward -- not enough two-sided liquid strikes', file=sys.stderr)
        return 1

    print(f'{root} expiry {expiry}   T = {T * 365 * 24:.1f}h')
    print(f'  quoted spot     {spot:.2f}')
    print(f'  implied forward {F:.2f}   basis {F - spot:+.2f} pts   ({len(pairs)} strike pairs)')

    errs, agree = [], []
    for K in sorted(book):
        side = book[K]
        for cp, c in side.items():
            if c['volume'] < 50 or c['iv'] <= 0.001 or c['bid'] < 0.05:
                continue
            mid = (c['bid'] + c['ask']) / 2
            errs.append(abs(black76(F, K, T, RATE, c['iv'], cp) - mid))
        if 'C' in side and 'P' in side and side['C']['volume'] > 100 and side['P']['volume'] > 100:
            cm = (side['C']['bid'] + side['C']['ask']) / 2
            pm = (side['P']['bid'] + side['P']['ask']) / 2
            iv_c = implied_vol(cm, F, K, T, RATE, 'C')
            iv_p = implied_vol(pm, F, K, T, RATE, 'P')
            if iv_c and iv_p:
                agree.append(abs(iv_c - iv_p))

    if errs:
        errs.sort()
        print(f'  repricing vs vendor IV: mean ${sum(errs) / len(errs):.3f}  '
              f'median ${errs[len(errs) // 2]:.3f}  (n={len(errs)})')
    if agree:
        agree_sorted = sorted(agree)
        print(f'  our call-vs-put IV:     mean {sum(agree) / len(agree):.4f}  '
              f'median {agree_sorted[len(agree_sorted) // 2]:.4f} vol pts  (n={len(agree)})')
        print('  -> surface is internally consistent' if agree_sorted[len(agree_sorted) // 2] < 0.005
              else '  -> WARNING: surface is not consistent, do not trade off it')
    return 0


def selftest() -> int:
    # Put-call parity must hold in the pricer itself.
    F, K, T, r, s = 100.0, 100.0, 0.25, 0.04, 0.20
    c = black76(F, K, T, r, s, 'C')
    p = black76(F, K, T, r, s, 'P')
    assert abs((c - p) - math.exp(-r * T) * (F - K)) < 1e-9, 'parity violated'

    # ATM call and put must be equal when F == K.
    assert abs(c - p) < 1e-9, 'ATM call != ATM put at F == K'

    # Round-tripping a price through implied_vol must recover the input vol.
    got = implied_vol(c, F, K, T, r, 'C')
    assert got is not None and abs(got - s) < 1e-6, f'iv round-trip failed: {got}'

    # Intrinsic at expiry.
    assert abs(black76(110, 100, 0, r, s, 'C') - 10) < 1e-9
    assert abs(black76(90, 100, 0, r, s, 'P') - 10) < 1e-9

    # The forward implier must recover a known forward from clean parity quotes.
    T2, r2 = 0.01, 0.04
    true_F = 5000.0
    synth = []
    for K_ in (4990.0, 5000.0, 5010.0):
        cm = black76(true_F, K_, T2, r2, 0.15, 'C')
        pm = black76(true_F, K_, T2, r2, 0.15, 'P')
        synth.append((0.5, K_, cm, pm))
    F_hat = implied_forward(synth, T2, r2)
    assert F_hat is not None and abs(F_hat - true_F) < 1e-6, f'forward implier off: {F_hat}'

    # OCC parsing.
    assert parse_occ('SPX260821C00200000') == ('260821', 'C', 200.0)
    # The weekly root is a different width -- this is the case that broke.
    assert parse_occ('SPXW260819P00700000') == ('260819', 'P', 700.0)
    assert parse_occ('QQQ260819C00600000') == ('260819', 'C', 600.0)

    print('selftest: all checks passed')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--symbol', default='SPX')
    ap.add_argument('--expiry', help='yymmdd; defaults to nearest unexpired')
    ap.add_argument('--selftest', action='store_true')
    args = ap.parse_args()
    return selftest() if args.selftest else run(args.symbol, args.expiry)


if __name__ == '__main__':
    sys.exit(main())
