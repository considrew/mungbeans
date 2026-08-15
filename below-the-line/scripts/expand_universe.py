#!/usr/bin/env python3
"""
Expand STOCK_UNIVERSE with the most liquid US-listed names it is missing.

The universe grew by hand and has gaps that are not systematic — SRAD was
absent while GENI, its direct competitor, had been tracked for months. Rather
than patch names in one at a time as articles surface them, this sweeps the
full exchange-listed set and takes whatever the universe happens to have
missed, ranked by size.

Screen:
  - listed on NASDAQ / NYSE / NYSE American / Cboe (no OTC, no test issues)
  - common stock only (no ETFs, no warrants, units, rights, preferred)
  - not already in STOCK_UNIVERSE
  - >= MIN_WEEKS of weekly price history, so the 200-week average is real
  - >= MIN_MARKET_CAP and >= MIN_ADV_SHARES average volume
  - ranked by market cap, top TARGET_COUNT taken

ALWAYS_INCLUDE names bypass the ranking but still must pass the data checks —
they are the ones an article has already referenced.

Usage:
    python3 scripts/expand_universe.py --dry-run     # candidate pool only, no Yahoo calls
    python3 scripts/expand_universe.py               # full run, writes universe_additions.json
    python3 scripts/expand_universe.py --apply       # also patches update_stocks.py

Yahoo rate-limits aggressively. A full run is ~2000 probes at 0.4s and takes
roughly 20 minutes. Resumable: re-running reuses .universe_probe_cache.json.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import random
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
UPDATE_STOCKS = REPO / 'scripts' / 'update_stocks.py'
CACHE = REPO / 'scripts' / '.universe_probe_cache.json'
OUT = REPO / 'scripts' / 'universe_additions.json'

TARGET_COUNT = 100
MIN_WEEKS = 200           # a full 200-week average, no partial signals
MIN_MARKET_CAP = 1_000_000_000
MIN_ADV_SHARES = 300_000  # thin names make the touch statistics meaningless

# Referenced in published work, so they belong in the screener regardless of rank.
ALWAYS_INCLUDE = ['SRAD']

UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'}

NASDAQ_LISTED = 'https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt'
OTHER_LISTED = 'https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt'

# Name fragments marking something other than an operating common stock.
# Blank-check vehicles are the big one: ~200 of them clear the symbol-format
# checks, have no operating history, and would all fail the screen anyway.
# Excluding them here saves the Yahoo calls on a rate-limited endpoint.
#   "acquisition" unqualified, because "Acquisition Corp", "Acquisition Corporation"
#   and "Acquisition Inc" all appear and no operating company uses the word.
#   "beneficial interest" is deliberately NOT excluded — it is standard REIT
#   charter language, and REITs are legitimate candidates.
NON_COMMON = re.compile(
    r'\b(warrants?|rights?|units?|preferred|depositary|debenture|notes?|'
    r'convertible|ETF|ETN|fund|index|when.issued|'
    r'acquisition|blank check|royalty trust|closed end|municipal)\b', re.I)

# NYSE-style suffixes: .A/.B/.C are share classes and belong. .R (rights),
# .U (units), .W/.WS (warrants) do not.
CLASS_SUFFIX_OK = ('A', 'B', 'C')


def fetch(url: str, timeout: int = 30) -> str:
    return urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=timeout).read().decode('utf-8', 'replace')


def current_universe() -> list[str]:
    src = UPDATE_STOCKS.read_text(encoding='utf-8')
    m = re.search(r'STOCK_UNIVERSE = \[(.*?)\n\]', src, re.S)
    if not m:
        sys.exit('Could not locate STOCK_UNIVERSE in update_stocks.py')
    return re.findall(r"'([A-Z\.\-]{1,6})'", m.group(1))


def candidate_pool(universe: set[str]) -> list[dict]:
    """Exchange-listed common stock not already tracked."""
    rows = []

    # nasdaqlisted.txt: Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot|ETF|NextShares
    for line in fetch(NASDAQ_LISTED).splitlines()[1:]:
        p = line.split('|')
        if len(p) < 8 or p[0] == '' or line.startswith('File Creation Time'):
            continue
        sym, name, test, etf = p[0].strip(), p[1], p[3].strip(), p[6].strip()
        if test == 'Y' or etf == 'Y':
            continue
        rows.append({'symbol': sym, 'name': name, 'exchange': 'NASDAQ'})

    # otherlisted.txt: ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot|Test Issue|NASDAQ Symbol
    for line in fetch(OTHER_LISTED).splitlines()[1:]:
        p = line.split('|')
        if len(p) < 8 or p[0] == '' or line.startswith('File Creation Time'):
            continue
        sym, name, exch, etf, test = p[0].strip(), p[1], p[2].strip(), p[4].strip(), p[6].strip()
        if test == 'Y' or etf == 'Y':
            continue
        exch_name = {'A': 'NYSE American', 'N': 'NYSE', 'P': 'NYSE Arca',
                     'Z': 'Cboe BZX', 'V': 'IEX'}.get(exch, exch)
        if exch in ('P', 'Z'):     # almost entirely ETFs and structured products
            continue
        rows.append({'symbol': sym, 'name': name, 'exchange': exch_name})

    out, seen = [], set()
    for r in rows:
        s = r['symbol']
        if s in seen or s in universe:
            continue
        # Class shares use a dot on Yahoo (BRK.B); five-letter NASDAQ symbols
        # ending in W/R/U/P are warrants, rights, units and preferred.
        if len(s) == 5 and s[-1] in 'WRUP':
            continue
        m = re.fullmatch(r'[A-Z]{1,5}(?:\.([A-Z]{1,2}))?', s)
        if not m:
            continue
        if m.group(1) and m.group(1) not in CLASS_SUFFIX_OK:
            continue
        if NON_COMMON.search(r['name']):
            continue
        seen.add(s)
        out.append(r)
    return sorted(out, key=lambda r: r['symbol'])


def probe(symbol: str) -> dict | None:
    """Weeks of history, market cap and average volume from one Yahoo call."""
    y = symbol.replace('.', '-')
    url = (f'https://query2.finance.yahoo.com/v8/finance/chart/{y}'
           f'?range=10y&interval=1wk')
    try:
        d = json.load(urllib.request.urlopen(
            urllib.request.Request(url, headers=UA), timeout=25))
        res = d['chart']['result'][0]
        meta = res['meta']
        closes = [c for c in res['indicators']['quote'][0]['close'] if c is not None]
        vols = [v for v in res['indicators']['quote'][0]['volume'] if v]
        if not closes:
            return None
        price = meta.get('regularMarketPrice') or closes[-1]
        shares = meta.get('sharesOutstanding')
        return {
            'symbol': symbol,
            'name': meta.get('longName') or meta.get('shortName') or '',
            'weeks': len(closes),
            'price': price,
            'market_cap': (price * shares) if (price and shares) else None,
            'adv_shares': (sum(vols[-52:]) / max(len(vols[-52:]), 1) / 5) if vols else 0,
        }
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true',
                    help='build the candidate pool and stop, no Yahoo calls')
    ap.add_argument('--apply', action='store_true',
                    help='patch STOCK_UNIVERSE in update_stocks.py')
    ap.add_argument('--limit', type=int, default=0,
                    help='probe only the first N candidates (testing)')
    ap.add_argument('--delay', type=float, default=0.4)
    args = ap.parse_args()

    universe = current_universe()
    uset = set(universe)
    print(f'Current universe: {len(universe)} tickers')

    pool = candidate_pool(uset)
    print(f'Exchange-listed common stock not already tracked: {len(pool)}')
    by_ex: dict[str, int] = {}
    for r in pool:
        by_ex[r['exchange']] = by_ex.get(r['exchange'], 0) + 1
    for k, v in sorted(by_ex.items(), key=lambda kv: -kv[1]):
        print(f'   {k:16s} {v:>5}')

    for s in ALWAYS_INCLUDE:
        print(f'   always-include {s}: '
              f'{"in pool" if any(r["symbol"] == s for r in pool) else "NOT IN POOL — check listing"}')

    if args.dry_run:
        print('\nDry run. No Yahoo calls made.')
        return

    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    targets = pool[:args.limit] if args.limit else pool
    print(f'\nProbing {len(targets)} symbols ({len(cache)} cached)...')

    results, checked = [], 0
    for i, r in enumerate(targets, 1):
        s = r['symbol']
        if s in cache:
            rec = cache[s]
        else:
            rec = probe(s)
            cache[s] = rec
            checked += 1
            time.sleep(args.delay + random.uniform(0, 0.2))
            if checked % 100 == 0:
                CACHE.write_text(json.dumps(cache))
                print(f'   {i}/{len(targets)}  ({checked} fetched)')
        if rec:
            rec['exchange'] = r['exchange']
            results.append(rec)

    CACHE.write_text(json.dumps(cache))

    def passes(rec: dict) -> bool:
        return (rec['weeks'] >= MIN_WEEKS
                and (rec['market_cap'] or 0) >= MIN_MARKET_CAP
                and rec['adv_shares'] >= MIN_ADV_SHARES)

    eligible = [r for r in results if passes(r)]
    eligible.sort(key=lambda r: -(r['market_cap'] or 0))

    forced = [r for r in results if r['symbol'] in ALWAYS_INCLUDE]
    for r in forced:
        why = []
        if r['weeks'] < MIN_WEEKS:
            why.append(f"only {r['weeks']}w history")
        if (r['market_cap'] or 0) < MIN_MARKET_CAP:
            why.append('below cap floor')
        if r['adv_shares'] < MIN_ADV_SHARES:
            why.append('below volume floor')
        if why:
            print(f"\n  note: {r['symbol']} forced in despite {', '.join(why)}")

    chosen, seen = [], set()
    for r in forced + eligible:
        if r['symbol'] in seen:
            continue
        seen.add(r['symbol'])
        chosen.append(r)
        if len(chosen) >= TARGET_COUNT + len(forced):
            break

    print(f'\nProbed OK: {len(results)}   passed screen: {len(eligible)}   selected: {len(chosen)}')
    print(f'\n{"symbol":<8}{"cap $B":>9}  {"ADV k":>8}  {"wks":>5}  name')
    for r in chosen:
        print(f'{r["symbol"]:<8}{(r["market_cap"] or 0)/1e9:>9.1f}  '
              f'{r["adv_shares"]/1e3:>8.0f}  {r["weeks"]:>5}  {r["name"][:44]}')

    OUT.write_text(json.dumps({
        'generated': time.strftime('%Y-%m-%d'),
        'criteria': {'min_weeks': MIN_WEEKS, 'min_market_cap': MIN_MARKET_CAP,
                     'min_adv_shares': MIN_ADV_SHARES, 'target': TARGET_COUNT},
        'universe_before': len(universe),
        'additions': [r['symbol'] for r in chosen],
        'detail': chosen,
    }, indent=2))
    print(f'\nWrote {OUT}')

    if args.apply:
        apply_additions([r['symbol'] for r in chosen], universe)
    else:
        print('Re-run with --apply to patch update_stocks.py.')


def apply_additions(new: list[str], universe: list[str]) -> None:
    """Rewrite STOCK_UNIVERSE as a sorted, deduped, 10-per-line block."""
    src = UPDATE_STOCKS.read_text(encoding='utf-8')
    m = re.search(r'(STOCK_UNIVERSE = \[)(.*?)(\n\])', src, re.S)
    merged = sorted(set(universe) | set(new))

    lines = []
    for i in range(0, len(merged), 10):
        lines.append('    ' + ', '.join(f"'{t}'" for t in merged[i:i + 10]) + ',')
    body = '\n' + '\n'.join(lines).rstrip(',')

    patched = src[:m.start(2)] + body + src[m.end(2):]
    UPDATE_STOCKS.write_text(patched, encoding='utf-8')

    # Re-read and verify rather than trusting the regex.
    check = re.search(r'STOCK_UNIVERSE = \[(.*?)\n\]',
                      UPDATE_STOCKS.read_text(encoding='utf-8'), re.S)
    got = re.findall(r"'([A-Z\.\-]{1,6})'", check.group(1))
    assert len(got) == len(merged), f'expected {len(merged)}, wrote {len(got)}'
    assert set(got) == set(merged), 'ticker set changed during write'
    missing = [t for t in universe if t not in set(got)]
    assert not missing, f'dropped existing tickers: {missing[:10]}'
    print(f'Patched update_stocks.py: {len(universe)} -> {len(got)} tickers, '
          f'{len(got) - len(universe)} added, none dropped.')


if __name__ == '__main__':
    main()
