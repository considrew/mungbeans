#!/usr/bin/env python3
"""
Options screener: monthly and LEAPS contracts on the liquid part of the universe.

WHAT "CHEAP" MEANS HERE

Four measures, all shown, because they answer different questions.

1. Parity gap. A call is cheap in the strongest sense when buying it and
   exercising costs less than buying the stock:

       ask + strike  <  price - dividends before expiry

   The ask matters, not the last trade and not the bid. Deep ITM LEAPS carry
   wide spreads, and a bid below parity with an ask above it is not a trade
   anyone can make; screening on the bid is how this idea usually turns into
   noise.

   The dividend term matters just as much. A call is a claim on the stock
   without its dividends, so for any payer the deep ITM calls sit below
   spot-parity as arithmetic rather than opportunity. Without subtracting them
   this screen returns the entire Dividend Aristocrat list every week.

   GENI pays nothing, which is why the Jan 2028 $5 call at $2.31 against $7.59
   was a real 3.7% discount rather than an artifact.

   This is not arbitrage. You forgo the dividends, you carry exercise and
   assignment mechanics, and you need the spread to stay open long enough to
   get out.

2. Time value per day. Premium above intrinsic divided by days remaining.
   Finds long-dated contracts where the runway is cheap even above parity.

3. IV against the stock's own realized volatility. The Bean Score logic applied
   to options: is this contract expensive relative to how this stock actually
   moves, rather than relative to other stocks.

4. Discount versus owning shares. The parity gap as a percentage of spot, so
   $0.28 on a $7.59 stock reads as 3.7% and sorts against everything else.

LIQUIDITY

Every contract shown has to be one you can leave. Contracts below the volume
and open-interest floors are dropped before any of the above is computed, and
the spread is displayed rather than hidden, because on a deep ITM LEAP the
spread is frequently larger than the edge.

USAGE

    python3 scripts/options_screener.py --discover      # find optionable names, cache the list
    python3 scripts/options_screener.py                 # screen them, write data/options.json
    python3 scripts/options_screener.py --symbols GENI  # one name, verbose
    python3 scripts/options_screener.py --selftest      # math only, no network
"""
from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
STOCKS = REPO / 'assets' / 'data' / 'stocks.json'
OUT = REPO / 'assets' / 'data' / 'options.json'
OPTIONABLE = REPO / 'scripts' / '.optionable_cache.json'

# --- screen parameters -------------------------------------------------------
MIN_OPEN_INTEREST = 100      # contracts you can leave
MIN_VOLUME = 10              # traded today, not just resting
MAX_SPREAD_PCT = 0.15        # ask-bid as a fraction of mid
MIN_DAYS = 21                # nothing about to expire
MAX_DAYS = 900               # ~2.5 years, covers LEAPS
TIMEOUT = 15
UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'}


# --- pure functions: no network, covered by --selftest -----------------------

def is_monthly(d: date) -> bool:
    """Standard expiration: the third Friday of its month.

    Weeklies are excluded deliberately. They carry most of the volume and none
    of the relevance here: a sub-parity deep ITM contract needs time on it for
    the discount to be worth the mechanics.
    """
    if d.weekday() != 4:
        return False
    return 15 <= d.day <= 21


def expected_dividends(price: float, annual_yield_pct: float | None, days: int) -> float:
    """Dividends expected before expiry.

    Approximated from the trailing yield rather than a projected ex-date
    calendar. Good enough to stop the screen firing on every payer, and stated
    as an approximation rather than dressed up as precision. A stock yielding
    3% with 400 days to expiry is assumed to pay ~3.3% of spot.
    """
    if not annual_yield_pct or annual_yield_pct <= 0 or price <= 0 or days <= 0:
        return 0.0
    return price * (annual_yield_pct / 100.0) * (days / 365.0)


def realized_vol(closes: list[float], window: int = 60) -> float | None:
    """Annualised realised volatility from daily closes."""
    px = [c for c in closes if c and c > 0][-(window + 1):]
    if len(px) < 20:
        return None
    rets = [math.log(px[i] / px[i - 1]) for i in range(1, len(px))]
    if len(rets) < 2:
        return None
    try:
        return statistics.stdev(rets) * math.sqrt(252)
    except statistics.StatisticsError:
        return None


def score_contract(c: dict, spot: float, div_yield: float | None, today: date) -> dict | None:
    """Everything the screen knows about one contract. None if it fails the gates."""
    try:
        strike = float(c['strike'])
        bid = float(c.get('bid') or 0)
        ask = float(c.get('ask') or 0)
        oi = int(c.get('openInterest') or 0)
        vol = int(c.get('volume') or 0)
        # UTC, not local. Yahoo stamps expirations at UTC midnight, so
        # date.fromtimestamp() shifts them back a day anywhere west of
        # Greenwich and every monthly then reads as a weekly.
        expiry = datetime.fromtimestamp(float(c['expiration']), tz=timezone.utc).date()
    except (KeyError, TypeError, ValueError):
        return None

    days = (expiry - today).days
    if not (MIN_DAYS <= days <= MAX_DAYS):
        return None
    if not is_monthly(expiry):
        return None
    if oi < MIN_OPEN_INTEREST or vol < MIN_VOLUME:
        return None
    if ask <= 0 or bid <= 0 or ask < bid:
        return None

    mid = (bid + ask) / 2
    spread = ask - bid
    spread_pct = spread / mid if mid > 0 else 1.0
    if spread_pct > MAX_SPREAD_PCT:
        return None

    is_call = (c.get('contract_type') or 'call') == 'call'
    intrinsic = max(0.0, spot - strike) if is_call else max(0.0, strike - spot)

    # Cost to enter is the ask. Everything below follows from that.
    time_value = ask - intrinsic
    divs = expected_dividends(spot, div_yield, days)

    # Negative parity_gap means buying and exercising beats buying the stock,
    # after giving up the dividends you would have collected.
    parity_gap = (ask + strike) - (spot - divs) if is_call else None
    discount_pct = (-parity_gap / spot * 100) if (parity_gap is not None and spot > 0) else None

    iv = c.get('impliedVolatility')
    try:
        iv = float(iv) if iv is not None else None
    except (TypeError, ValueError):
        iv = None

    return {
        'contract': c.get('contractSymbol'),
        'type': 'call' if is_call else 'put',
        'strike': round(strike, 2),
        'expiry': expiry.isoformat(),
        'days': days,
        'bid': round(bid, 2),
        'ask': round(ask, 2),
        'mid': round(mid, 2),
        'spread': round(spread, 2),
        'spread_pct': round(spread_pct * 100, 1),
        'volume': vol,
        'open_interest': oi,
        'intrinsic': round(intrinsic, 2),
        'time_value': round(time_value, 3),
        'time_value_per_day': round(time_value / days, 4) if days else None,
        'expected_dividends': round(divs, 3),
        'parity_gap': round(parity_gap, 3) if parity_gap is not None else None,
        'below_parity': bool(parity_gap is not None and parity_gap < 0),
        'discount_pct': round(discount_pct, 2) if discount_pct is not None else None,
        'iv': round(iv * 100, 1) if iv else None,
        'moneyness': round(spot / strike, 3) if strike else None,
    }


def annotate_iv(rows: list[dict], rv: float | None) -> None:
    """IV against the stock's own realised volatility, in place."""
    if not rv or rv <= 0:
        return
    for r in rows:
        if r.get('iv'):
            r['iv_vs_realized'] = round((r['iv'] / 100.0) / rv, 2)
            r['realized_vol'] = round(rv * 100, 1)


# --- network -----------------------------------------------------------------

def get(url: str):
    try:
        with urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=TIMEOUT) as r:
            return json.load(r)
    except Exception:
        return None


def fetch_chain(symbol: str, expiry_ts: int | None = None) -> dict | None:
    url = f'https://query2.finance.yahoo.com/v7/finance/options/{symbol.replace(".", "-")}'
    if expiry_ts:
        url += f'?date={expiry_ts}'
    d = get(url)
    res = ((d or {}).get('optionChain') or {}).get('result') or []
    return res[0] if res else None


def fetch_daily_closes(symbol: str) -> list[float]:
    d = get(f'https://query2.finance.yahoo.com/v8/finance/chart/'
            f'{symbol.replace(".", "-")}?interval=1d&range=1y')
    res = ((d or {}).get('chart') or {}).get('result') or []
    if not res:
        return []
    q = (res[0].get('indicators') or {}).get('quote') or [{}]
    return [c for c in (q[0].get('close') or []) if c]


# --- selftest ----------------------------------------------------------------

def selftest() -> int:
    fails = 0

    def check(name, got, want):
        nonlocal fails
        ok = (abs(got - want) < 1e-6) if isinstance(want, float) else got == want
        if not ok:
            fails += 1
        print(f"{'ok  ' if ok else 'FAIL'}  {name:<44} {got}{'' if ok else f'  want {want}'}")

    print('monthly expiration detection')
    check('2026-01-15 third Friday', is_monthly(date(2026, 1, 16)), True)
    check('2026-08-21 third Friday', is_monthly(date(2026, 8, 21)), True)
    check('2026-08-14 second Friday', is_monthly(date(2026, 8, 14)), False)
    check('2026-08-28 fourth Friday', is_monthly(date(2026, 8, 28)), False)
    check('2026-08-19 Wednesday', is_monthly(date(2026, 8, 19)), False)

    print('\ndividend adjustment')
    check('no yield', expected_dividends(100, None, 365), 0.0)
    check('3% over a year', round(expected_dividends(100, 3.0, 365), 4), 3.0)
    check('3% over half a year', round(expected_dividends(100, 3.0, 182), 4), 1.4959)

    print("\nGENI Jan 2028 $5 call — Drew's actual fill")
    geni = score_contract(
        {'strike': 5.0, 'bid': 2.25, 'ask': 2.31, 'openInterest': 1200, 'volume': 40,
         'expiration': datetime(2028, 1, 21, tzinfo=timezone.utc).timestamp(),
         'impliedVolatility': 0.55, 'contract_type': 'call',
         'contractSymbol': 'GENI280121C00005000'},
        spot=7.59, div_yield=None, today=date(2026, 8, 15))
    check('scored', geni is not None, True)
    if geni:
        check('intrinsic 7.59-5.00', geni['intrinsic'], 2.59)
        check('time value 2.31-2.59', geni['time_value'], -0.28)
        check('parity gap negative', geni['parity_gap'] < 0, True)
        check('below parity', geni['below_parity'], True)
        check('discount 0.28/7.59', geni['discount_pct'], 3.69)

    print('\ndividend payer that only looks cheap')
    payer = score_contract(
        {'strike': 40.0, 'bid': 20.10, 'ask': 20.30, 'openInterest': 900, 'volume': 25,
         'expiration': datetime(2027, 1, 15, tzinfo=timezone.utc).timestamp(),
         'impliedVolatility': 0.22, 'contract_type': 'call'},
        spot=60.50, div_yield=3.5, today=date(2026, 8, 15))
    check('scored', payer is not None, True)
    if payer:
        # ask+strike = 60.30 < spot 60.50, so it clears on a naive test.
        check('naive test would flag it', (20.30 + 40.0) < 60.50, True)
        check('dividends subtracted', payer['expected_dividends'] > 0.8, True)
        check('correctly NOT below parity', payer['below_parity'], False)

    print('\nliquidity and expiry gates')
    base = {'strike': 50.0, 'bid': 5.0, 'ask': 5.2, 'openInterest': 500, 'volume': 50,
            'expiration': datetime(2027, 1, 15, tzinfo=timezone.utc).timestamp(),
            'contract_type': 'call'}
    t = date(2026, 8, 15)
    check('baseline passes', score_contract(dict(base), 55, None, t) is not None, True)
    check('thin open interest dropped',
          score_contract({**base, 'openInterest': 5}, 55, None, t) is None, True)
    check('no volume dropped',
          score_contract({**base, 'volume': 0}, 55, None, t) is None, True)
    check('wide spread dropped',
          score_contract({**base, 'bid': 4.0, 'ask': 6.0}, 55, None, t) is None, True)
    check('weekly expiry dropped',
          score_contract({**base, 'expiration': datetime(2027, 1, 8, tzinfo=timezone.utc).timestamp()},
                         55, None, t) is None, True)
    check('too near expiry dropped',
          score_contract({**base, 'expiration': datetime(2026, 8, 21, tzinfo=timezone.utc).timestamp()},
                         55, None, t) is None, True)

    print('\nUTC expiry parsing (this broke on any US timezone)')
    ts = datetime(2028, 1, 21, tzinfo=timezone.utc).timestamp()
    check('parses to 2028-01-21 regardless of TZ',
          datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(), '2028-01-21')

    print('\nrealised volatility')
    flat = [100.0] * 90
    check('flat series has ~zero vol', (realized_vol(flat) or 0) < 1e-9, True)
    check('short series returns None', realized_vol([100, 101]) is None, True)

    print(f"\n{'PASS' if not fails else str(fails) + ' FAILURES'}")
    return fails


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--selftest', action='store_true')
    ap.add_argument('--discover', action='store_true')
    ap.add_argument('--symbols', nargs='*')
    ap.add_argument('--delay', type=float, default=0.4)
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    if args.selftest:
        sys.exit(selftest())

    doc = json.loads(STOCKS.read_text())
    stocks = {s['symbol']: s for s in doc['stocks']}
    today = date.today()

    if args.symbols:
        targets = [s.upper() for s in args.symbols]
    elif OPTIONABLE.exists() and not args.discover:
        targets = json.loads(OPTIONABLE.read_text())['symbols']
    else:
        targets = sorted(stocks)
    if args.limit:
        targets = targets[:args.limit]

    print(f'{len(targets)} symbols, {TIMEOUT}s timeout each\n')

    results, optionable = {}, []
    for i, sym in enumerate(targets, 1):
        print(f'  [{i}/{len(targets)}] {sym:<6}', end='', flush=True)
        head = fetch_chain(sym)
        time.sleep(args.delay)
        if not head or not head.get('expirationDates'):
            print(' no chain')
            continue

        spot = ((head.get('quote') or {}).get('regularMarketPrice')
                or stocks.get(sym, {}).get('close'))
        if not spot:
            print(' no price')
            continue

        div_yield = stocks.get(sym, {}).get('dividend_yield')

        # Monthlies and LEAPS only, so most listed expirations are skipped.
        wanted = []
        for ts in head['expirationDates']:
            ed = datetime.fromtimestamp(ts, tz=timezone.utc).date()
            if is_monthly(ed) and MIN_DAYS <= (ed - today).days <= MAX_DAYS:
                wanted.append(ts)
        rows = []
        for ts in wanted:
            chain = fetch_chain(sym, ts)
            time.sleep(args.delay)
            if not chain or not chain.get('options'):
                continue
            opt = chain['options'][0]
            for kind in ('calls', 'puts'):
                for c in opt.get(kind) or []:
                    c['contract_type'] = kind[:-1]
                    r = score_contract(c, spot, div_yield, today)
                    if r:
                        rows.append(r)

        if not rows:
            print(f' {len(wanted)} expiries, nothing passed the gates')
            continue

        closes = fetch_daily_closes(sym)
        time.sleep(args.delay)
        annotate_iv(rows, realized_vol(closes))

        rows.sort(key=lambda r: (r['discount_pct'] is None, -(r['discount_pct'] or 0)))
        results[sym] = {'spot': round(float(spot), 2), 'contracts': rows}
        optionable.append(sym)
        cheap = sum(1 for r in rows if r['below_parity'])
        print(f' {len(rows)} contracts, {cheap} below parity')

    if args.discover or not OPTIONABLE.exists():
        OPTIONABLE.write_text(json.dumps({'generated': today.isoformat(),
                                          'symbols': sorted(optionable)}, indent=2))
        print(f'\nCached {len(optionable)} optionable symbols')

    OUT.write_text(json.dumps({
        'generated': today.isoformat(),
        'criteria': {'min_open_interest': MIN_OPEN_INTEREST, 'min_volume': MIN_VOLUME,
                     'max_spread_pct': MAX_SPREAD_PCT, 'min_days': MIN_DAYS,
                     'max_days': MAX_DAYS, 'monthlies_only': True},
        'stocks': results,
    }, indent=2))

    total = sum(len(v['contracts']) for v in results.values())
    cheap = [(s, r) for s, v in results.items() for r in v['contracts'] if r['below_parity']]
    print(f'\n{len(results)} stocks, {total} contracts, {len(cheap)} below parity')
    for s, r in sorted(cheap, key=lambda x: -(x[1]['discount_pct'] or 0))[:15]:
        print(f"   {s:<6} {r['expiry']} ${r['strike']:<8g} {r['type']:<5} "
              f"ask {r['ask']:>7.2f}  {r['discount_pct']:>5.2f}% under shares  "
              f"OI {r['open_interest']:>6}  spread {r['spread_pct']:.1f}%")
    print(f'\nWrote {OUT}')


if __name__ == '__main__':
    main()
