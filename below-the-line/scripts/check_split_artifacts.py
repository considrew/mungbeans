#!/usr/bin/env python3
"""
Find crossings that are split artifacts rather than price moves.

Yahoo restates history for splits, but not instantly. A split a few days before
the Saturday run can arrive with the newest weekly bar in post-split dollars
while the preceding 200 bars are still pre-split. The 200-week line then sits
at the old scale, the price at the new one, and the stock reads as having
crashed through the line.

Monster split 2-for-1 on 11 Aug 2026. The 15 Aug run recorded close $46.82
against a line of $60.54 and published it as a new crossing at -22.7%. The
Friday before, it was 49% above.

Older splits are fine — NVDA, AVGO, LRCX and CMG all carry sane lines — so this
is latency at the edge, not a systemic failure. It bites in the days right
after a split, which is exactly when the false signal gets published.

Deliberately does NOT use yfinance: .splits pulls full history per symbol and
stalls indefinitely under throttling. This hits the same chart endpoint the
Netlify quote function uses, one call per symbol, with a hard timeout.

    python3 scripts/check_split_artifacts.py                    # this week's crossings
    python3 scripts/check_split_artifacts.py --symbols MNST     # just one
    python3 scripts/check_split_artifacts.py --strip            # drop artifacts from crossings.json
    python3 scripts/check_split_artifacts.py --all              # audit everything (slow)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA_DIRS = [REPO / 'assets' / 'data', REPO / 'static' / 'data']
CACHE = REPO / 'scripts' / '.split_check_cache.json'

LOOKBACK_DAYS = 45
TOLERANCE = 0.12        # how close line/price must be to the split ratio
TIMEOUT = 15            # hard per-request ceiling; no retries, no backoff

UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'}


def load(name: str):
    for d in DATA_DIRS:
        p = d / name
        if p.exists():
            return json.loads(p.read_text()), p
    sys.exit(f'{name} not found in {[str(d) for d in DATA_DIRS]}')


def recent_splits(symbol: str) -> list[tuple[str, float, int]]:
    """(date, ratio, age_days) for splits in the lookback window. [] on any failure."""
    url = (f'https://query2.finance.yahoo.com/v8/finance/chart/'
           f'{symbol.replace(".", "-")}?interval=1d&range=3mo&events=split')
    try:
        with urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=TIMEOUT) as r:
            data = json.load(r)
    except Exception as e:
        return [('ERROR', 0.0, -1)] if 'HTTP Error 429' in str(e) else []

    events = ((data.get('chart') or {}).get('result') or [{}])[0].get('events') or {}
    now = datetime.now(timezone.utc)
    out = []
    for ev in (events.get('splits') or {}).values():
        try:
            num, den = float(ev['numerator']), float(ev['denominator'])
            when = datetime.fromtimestamp(float(ev['date']), tz=timezone.utc)
        except Exception:
            continue
        if den == 0:
            continue
        age = (now - when).days
        if 0 <= age <= LOOKBACK_DAYS:
            out.append((when.date().isoformat(), num / den, age))
    return sorted(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--strip', action='store_true')
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--symbols', nargs='*', help='check only these')
    ap.add_argument('--delay', type=float, default=0.3)
    args = ap.parse_args()

    stocks_doc, stocks_path = load('stocks.json')
    stocks = {s['symbol']: s for s in stocks_doc['stocks']}
    print(f'stocks.json     {stocks_path.relative_to(REPO)}  '
          f'as of {stocks_doc.get("generated_readable")}')

    cross_doc, cross_path = load('crossings.json')
    newly = [x if isinstance(x, str) else x.get('symbol')
             for x in cross_doc.get('newly_below', [])]
    print(f'crossings.json  {cross_path.relative_to(REPO)}  '
          f'{cross_doc.get("date")}  {len(newly)} newly below\n')

    if args.symbols:
        targets = [s.upper() for s in args.symbols]
    elif args.all:
        targets = sorted(stocks)
    else:
        targets = sorted(newly)

    # Drop any ERROR entries an older build may have written, so a poisoned
    # cache re-probes instead of replaying a rate-limit failure forever.
    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    cache = {k: v for k, v in cache.items()
             if not (v and isinstance(v, list) and v and v[0] and v[0][0] == 'ERROR')}
    print(f'Checking {len(targets)} symbols, {TIMEOUT}s timeout each '
          f'(~{len(targets) * (args.delay + 0.4) / 60:.1f} min worst case)\n')

    artifacts, clean, throttled = [], [], []
    for i, sym in enumerate(targets, 1):
        print(f'  [{i}/{len(targets)}] {sym:<6}', end='', flush=True)

        if sym in cache:
            sp = [tuple(x) for x in cache[sym]]
            print(' (cached)', end='')
        else:
            sp = recent_splits(sym)
            cache[sym] = sp
            time.sleep(args.delay)
            if i % 25 == 0:
                CACHE.write_text(json.dumps(cache))

        if sp and sp[0][0] == 'ERROR':
            throttled.append(sym)
            print(' rate limited')
            continue
        if not sp:
            print(' no recent split')
            continue

        s = stocks.get(sym)
        if not s or not s.get('wma_200') or not s.get('close'):
            print(' split, but no stock record')
            continue

        date, ratio, age = sp[-1]
        implied = s['wma_200'] / s['close']
        unadjusted = abs(implied - ratio) / ratio < TOLERANCE
        print(f'  {ratio:g}-for-1 {date} ({age}d)  '
              f'{"ARTIFACT" if unadjusted else "line already restated"}')

        row = (f"{sym:<6} {ratio:g}-for-1 on {date} ({age}d ago)  "
               f"close {s['close']:>9.2f}  line {s['wma_200']:>9.2f}  "
               f"{s['pct_from_wma']:>7.1f}%")
        (artifacts if unadjusted else clean).append((sym, row, ratio))

    CACHE.write_text(json.dumps(cache))
    print()

    if throttled:
        print(f'Rate limited on {len(throttled)}: {", ".join(throttled[:15])}')
        print('Results cached; re-run to pick up where it stopped.\n')

    if clean:
        print(f'Split, line already restated ({len(clean)}) — no action:')
        for _, row, _ in clean:
            print('   ' + row)
        print()

    if not artifacts:
        if throttled and not clean:
            print('INCONCLUSIVE — every symbol was rate limited. Nothing was checked.')
            print('Wait a few minutes and re-run; confirmed results are reused.')
        else:
            print(f'No split artifacts found across '
                  f'{len(targets) - len(throttled)} symbols checked.')
            if throttled:
                print(f'{len(throttled)} unchecked (rate limited) — re-run to finish.')
        return

    print(f'SPLIT ARTIFACTS ({len(artifacts)}) — line is still pre-split:\n')
    for sym, row, ratio in artifacts:
        s = stocks[sym]
        corrected = s['wma_200'] / ratio
        real = (s['close'] / corrected - 1) * 100
        print('   ' + row)
        print(f"   {'':<6} line should be ~{corrected:.2f}, putting it {real:+.1f}% "
              f"({'below' if real < 0 else 'above'} the line)\n")

    in_crossings = [s for s, _, _ in artifacts if s in newly]
    if in_crossings:
        print(f'Published as new crossings: {", ".join(in_crossings)}')

    if args.strip and in_crossings:
        drop = set(in_crossings)
        kept = [x for x in cross_doc.get('newly_below', [])
                if (x if isinstance(x, str) else x.get('symbol')) not in drop]
        cross_doc['newly_below'] = kept
        cross_doc.setdefault('excluded_split_artifacts', []).extend(sorted(drop))
        for d in DATA_DIRS:
            p = d / 'crossings.json'
            if p.exists():
                p.write_text(json.dumps(cross_doc, indent=2))
                print(f'  rewrote {p.relative_to(REPO)}: {len(newly)} -> {len(kept)}')
    elif in_crossings:
        print('\nRe-run with --strip to remove them from crossings.json.')


if __name__ == '__main__':
    main()
