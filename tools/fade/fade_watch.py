#!/usr/bin/env python3
"""
fade_watch -- a watcher that pings you when a fade-the-extension setup ripens.

WHAT THIS IS, AND WHAT IT IS NOT

This is not a signal engine that tells you a trade is good. The edge here is
discretionary -- a read that the day's thesis is changing in real time -- and
no rule replaces that read. What a machine can do is the part you cannot do by
staring at the tape: watch two indices continuously, notice when the objective
conditions that tend to precede a reversion line up, and tap you on the
shoulder with a fully-priced ticket so you are reacting to a setup instead of
hunting for one.

So this polls SPX and QQQ during the session, scores how extended and how
exhausted the current move is, and when that score crosses a line it fires one
alert: the side to fade, the strike in your 1.5-2% OTM band, an expiry with a
few days of life so the contract survives the overnight hold, the entry
premium off a self-built option surface, and a scale-out ladder so the rare
monster gets banked in stages instead of round-tripping.

WHY FADE, WHY OTM, WHY OVERNIGHT

The research phase (see research/) established three things on ten years of
data. Buying at-the-money and holding to a same-day expiry is structurally
negative. Fading a large prior move with a cheap out-of-the-money option is
not: hold-to-close mean return crosses positive once the move being faded
exceeds ~2%, and the edge grows monotonically with the size of the move. And a
fixed profit target destroys it, because the payoff is fat-tailed -- the few
days that reverse hard are the whole return, so the exit has to let a runner
ride. This tool encodes exactly those three findings and nothing more
confident than them.

HONESTY ABOUT THE THRESHOLDS

The score weights and trigger level below are a starting point, not a
validated optimum -- the realtime intraday setup is not the same object as the
close-to-close backtest, and no free source carries the intraday option data
to backtest it properly. Treat the first weeks as paper testing. Every alert
is logged to alerts.jsonl precisely so the thresholds can be tuned against
what actually happened, rather than against a feeling about what happened.

USAGE

    python3 fade_watch.py --selftest         # math only, no network
    python3 fade_watch.py --once             # evaluate both symbols once, print
    python3 fade_watch.py --once --symbol QQQ
    python3 fade_watch.py --loop             # poll during the session, alert on ripen
    python3 fade_watch.py --once --json       # machine-readable evaluation

ALERTS

By default an alert prints to the terminal with a bell. To get it on your
phone, set one of:

    FADE_WATCH_NTFY=your-topic        # https://ntfy.sh push, zero setup
    FADE_WATCH_WEBHOOK=https://...    # POST the alert JSON anywhere (Discord, Slack)

Nothing here places an order. It hands you a ticket; you decide.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ALERT_LOG = HERE / 'alerts.jsonl'
STATE = HERE / '.fade_watch_state.json'

sys.path.insert(0, str(HERE))
import signals  # noqa: E402  (sibling module; path set above so it resolves from anywhere)

# --- configuration: everything tunable lives here ----------------------------

CONFIG = {
    'symbols': ['SPX', 'QQQ'],

    # The trigger is a CONFLUENCE of real signals, not a weighted score. It is
    # silent until VWAP-extension (the required spine) plus at least
    # `min_confirm` of the other enabled signals all point the same way. See
    # signals.py for what each one measures. This is deliberately mostly-off:
    # a fuzzy score that is always partly lit is exactly what we are replacing.
    'signals': {
        'enabled': ['vwap', 'rsi2', 'atr', 'band', 'pdl'],
        'vwap_sigma_gate': 2.0,    # price this many sigma off VWAP to arm the spine
        'rsi2_hi': 95,             # RSI(2) at/above -> stretched up (fade with puts)
        'rsi2_lo': 5,              # at/below -> stretched down (fade with calls)
        'atr_ext_min': 1.5,        # move >= this many ATRs to count
        'min_confirm': 2,          # confirmations required beyond the VWAP spine
    },

    # Contract selection.
    'otm_target_pct': 1.75,        # centre of your 1.5-2.0% OTM band
    'otm_band_pct': (1.4, 2.1),    # acceptable strike distance from spot
    'dte_min': 2,                  # must outlive the overnight hold
    'dte_max': 5,                  # a week at the outside
    'min_open_interest': 200,
    'max_spread_pct_of_mid': 0.20, # skip a strike you cannot leave cheaply

    # Scale-out ladder, expressed as fractions of the faded move that price
    # retraces. Each rung reprices the option at that reversion level, one
    # trading day of decay spent, and banks a slice. The last slice is a
    # runner with no cap -- that is where the fat tail is captured.
    'ladder': [
        {'retrace': 0.35, 'take': 0.34, 'label': 'bank on the open pop'},
        {'retrace': 0.60, 'take': 0.33, 'label': 'second bank'},
        {'retrace': 1.00, 'take': 0.33, 'label': 'runner -- trail, do not cap'},
    ],
    'exit_hold_days': 1.0,         # decay assumed spent by the time you exit

    'rate': 0.04,
    'poll_seconds': 120,
    'realert_cooldown_min': 45,    # do not re-fire the same side within this
}

TIMEOUT = 20
UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'}


# --- pricing: pure, self-tested; the vetted surface math from research/ -------

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def black76(F: float, K: float, T: float, r: float, sigma: float, cp: str) -> float:
    """Option price off the forward. We never trust a spot we did not imply."""
    df = math.exp(-r * T)
    if T <= 0 or sigma <= 0:
        return df * max(0.0, (F - K) if cp == 'C' else (K - F))
    v = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / v
    d2 = d1 - v
    if cp == 'C':
        return df * (F * norm_cdf(d1) - K * norm_cdf(d2))
    return df * (K * norm_cdf(-d2) - F * norm_cdf(-d1))


def greeks(F: float, K: float, T: float, r: float, sigma: float, cp: str) -> dict:
    """Delta, gamma, theta (per day), vega (per vol point) off the forward."""
    df = math.exp(-r * T)
    if T <= 0 or sigma <= 0:
        intrinsic = max(0.0, (F - K) if cp == 'C' else (K - F))
        return {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0, 'price': df * intrinsic}
    v = sigma * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / v
    d2 = d1 - v
    delta = df * (norm_cdf(d1) if cp == 'C' else norm_cdf(d1) - 1.0)
    gamma = df * norm_pdf(d1) / (F * v)
    vega = df * F * norm_pdf(d1) * math.sqrt(T) / 100.0
    theta = (-df * F * norm_pdf(d1) * sigma / (2.0 * math.sqrt(T))) / 365.0
    return {'delta': delta, 'gamma': gamma, 'theta': theta, 'vega': vega,
            'price': black76(F, K, T, r, sigma, cp)}


def implied_vol(price: float, F: float, K: float, T: float, r: float, cp: str) -> float | None:
    """Volatility reproducing `price`, by bisection (robust on thin wings)."""
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


def implied_forward(pairs, T: float, r: float, take: int = 5) -> float | None:
    """Forward from put-call parity over the tightest-quoted strikes."""
    if not pairs:
        return None
    ranked = sorted(pairs)[:take]
    return sum(K + math.exp(r * T) * (c - p) for _, K, c, p in ranked) / len(ranked)


import re
_OCC = re.compile(r'^(?P<root>[A-Z]+)(?P<exp>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$')


def parse_occ(sym: str):
    """OCC symbol -> (yymmdd, C/P, strike). Pattern-matched, never sliced:

    SPX weeklies -- which is where every short-dated expiry lives -- carry the
    SPXW root, a different width than SPX. Fixed-offset slicing silently drops
    the entire short-dated surface and reports an empty chain.
    """
    m = _OCC.match(sym)
    if not m:
        raise ValueError(f'unparseable OCC symbol: {sym}')
    return m.group('exp'), m.group('cp'), int(m.group('strike')) / 1000.0


# The setup decision now lives in signals.py (VWAP/RSI/ATR/Bollinger/prior-day
# confluence). fade_watch consumes signals.compute() + signals.evaluate_signals()
# and owns only the contract selection, pricing and ladder below.


# --- reversion ladder --------------------------------------------------------

def build_ladder(entry_prem, F, K, sigma, T_exit, r, cp, faded_move_abs, spot, cfg):
    """Reprice the option at each reversion rung and lay out the scale-out.

    faded_move_abs is the point size of the move being faded. A retrace of
    fraction f moves spot back toward the prior close by f * faded_move_abs, in
    the direction we are positioned. We reprice at that level with one day of
    decay spent, and report the gain per rung. The last rung is a runner: the
    price shown is a reference, not a cap.
    """
    rungs = []
    for r_ in cfg['ladder']:
        f = r_['retrace']
        move = f * faded_move_abs
        # positioned to profit as spot reverts: puts want spot down, calls up.
        spot_at = spot - move if cp == 'P' else spot + move
        F_at = F - move if cp == 'P' else F + move
        val = black76(F_at, K, T_exit, r, sigma, cp)
        gain = (val - entry_prem) / entry_prem if entry_prem > 0 else 0.0
        rungs.append({
            'retrace': f,
            'take_fraction': r_['take'],
            'label': r_['label'],
            'underlying_at': round(spot_at, 2),
            'option_price': round(val, 2),
            'gain_pct': round(100 * gain, 1),
        })
    return rungs


# --- network -----------------------------------------------------------------

def _get(url: str) -> bytes:
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=TIMEOUT).read()


def intraday_from_chain(data: dict):
    """Underlying session state read from the option chain's own header.

    The Cboe delayed feed carries the index/ETF alongside its options --
    current price, session open/high/low, prior close, and a clock -- so a
    single request gives both the tape and the chain, and it is not
    rate-limited the way the chart endpoints are. This is the primary source;
    there is deliberately no second dependency to fall over.

    Session high/low here are the feed's running extremes, which is exactly
    what the exhaustion component needs. minutes-into-session is derived from
    the feed's own ET clock rather than trusted from elsewhere.
    """
    spot = data.get('current_price')
    prev = data.get('prev_day_close')
    if not spot or not prev:
        return None
    hi = data.get('high') or spot
    lo = data.get('low') or spot
    stamp = data.get('last_trade_time')
    clock = datetime.fromisoformat(stamp) if stamp else datetime.utcnow()
    minutes_in = max((clock.hour * 60 + clock.minute) - (9 * 60 + 30), 0.0)
    return {'prev_close': prev, 'spot': spot, 'high': max(hi, spot), 'low': min(lo, spot),
            'minutes_in': float(minutes_in), 'clock': clock}


def fetch_chain(symbol: str):
    key = '_SPX' if symbol.upper() == 'SPX' else symbol.upper()
    raw = json.loads(_get(f'https://cdn.cboe.com/api/global/delayed_quotes/options/{key}.json'))
    return raw['data']


# --- contract selection ------------------------------------------------------

def select_contract(chain, symbol, cp, spot, cfg, market_now):
    """Pick the expiry and strike, and price it off our own surface.

    Returns a dict describing the ticket, or None with a reason if no contract
    in the window clears the liquidity floors.
    """
    root = 'SPX' if symbol.upper() == 'SPX' else symbol.upper()
    options = chain['options']

    def hours_to(exp):
        settle = datetime.strptime('20' + exp, '%Y%m%d').replace(hour=16)
        return (settle - market_now).total_seconds() / 3600.0

    # candidate expiries inside the DTE window (in calendar days, close enough)
    exps = sorted({parse_occ(c['option'])[0] for c in options})
    windowed = []
    for e in exps:
        days = hours_to(e) / 24.0
        if cfg['dte_min'] - 0.4 <= days <= cfg['dte_max'] + 0.4:
            windowed.append((days, e))
    if not windowed:
        return None, 'no expiry in the 2-5 DTE window'
    windowed.sort()
    _, expiry = windowed[0]
    T = hours_to(expiry) / (365 * 24)

    # build a small book at this expiry to imply the forward
    book = {}
    for c in options:
        exp, ocp, K = parse_occ(c['option'])
        if exp != expiry or c['bid'] <= 0:
            continue
        book.setdefault(K, {})[ocp] = c
    pairs = []
    for K, side in book.items():
        if 'C' in side and 'P' in side and abs(K - spot) < spot * 0.02:
            cc, pp = side['C'], side['P']
            if cc['volume'] and pp['volume']:
                pairs.append((cc['ask'] - cc['bid'] + pp['ask'] - pp['bid'], K,
                              (cc['bid'] + cc['ask']) / 2, (pp['bid'] + pp['ask']) / 2))
    F = implied_forward(pairs, T, cfg['rate']) or spot

    # target strike in the OTM band, on the fade side
    lo, hi = cfg['otm_band_pct']
    target = spot * (1 - cfg['otm_target_pct'] / 100) if cp == 'P' \
        else spot * (1 + cfg['otm_target_pct'] / 100)
    best = None
    for K, side in book.items():
        if cp not in side:
            continue
        dist_pct = 100 * (spot - K) / spot if cp == 'P' else 100 * (K - spot) / spot
        if not (lo <= dist_pct <= hi):
            continue
        c = side[cp]
        mid = (c['bid'] + c['ask']) / 2
        if mid <= 0:
            continue
        spread_pct = (c['ask'] - c['bid']) / mid
        if c['open_interest'] < cfg['min_open_interest'] or spread_pct > cfg['max_spread_pct_of_mid']:
            continue
        score = abs(K - target)
        if best is None or score < best[0]:
            best = (score, K, c, mid, dist_pct, spread_pct)
    if best is None:
        return None, 'no strike in the OTM band cleared liquidity/spread floors'

    _, K, c, mid, dist_pct, spread_pct = best
    iv = implied_vol(mid, F, K, T, cfg['rate'], cp) or (c['iv'] or 0.0)
    g = greeks(F, K, T, cfg['rate'], iv, cp)
    return {
        'expiry': expiry, 'dte': round(hours_to(expiry) / 24.0, 2),
        'strike': K, 'cp': cp, 'forward': round(F, 2),
        'entry_mid': round(mid, 2), 'entry_ask': c['ask'],
        'otm_pct': round(dist_pct, 2), 'spread_pct': round(100 * spread_pct, 1),
        'iv': round(iv, 4), 'open_interest': int(c['open_interest']),
        'delta': round(g['delta'], 3), 'gamma': round(g['gamma'], 5),
        'theta': round(g['theta'], 3), 'vega': round(g['vega'], 3),
        'T': T,
    }, None


# --- evaluation --------------------------------------------------------------

def evaluate(symbol: str, cfg: dict):
    """Full evaluation for one symbol. Returns a result dict (never raises for
    ordinary data gaps -- it reports them in 'error').

    The setup read (VWAP/RSI/ATR/Bollinger/prior-day confluence) comes from
    signals.py, on the symbol's tradeable proxy. Only if it fires do we touch
    the option chain, so a quiet tape costs one intraday request, not a chain.
    """
    sig = signals.compute(symbol)
    if sig.get('error'):
        return {'symbol': symbol, 'error': sig['error']}
    dec = signals.evaluate_signals(sig, cfg['signals'])

    result = {
        'symbol': symbol, 'proxy': sig['proxy'],
        'spot': sig['spot'], 'prev_close': sig['prev_close'], 'move_pct': sig['move_pct'],
        'side': 'PUT (fade up)' if dec['side'] == 'P' else 'CALL (fade down)',
        'cp': dec['side'],
        'read': {'vwap_sigma': sig['vwap_sigma'], 'rsi2': sig['rsi2'], 'rsi14': sig['rsi14'],
                 'atr_ext': sig['atr_ext'], 'pctb': sig['pctb'],
                 'band_tag': sig['band_tag'], 'pd_tag': sig['pd_tag']},
        'votes': dec['votes'], 'spine_ok': dec['spine_ok'],
        'confirmations': dec['confirmations'], 'n_confirm': dec['n_confirm'],
        'ripe': dec['fired'], 'error': None,
    }
    if not dec['fired']:
        return result

    # fired: now price the real option off the Cboe chain for the true underlying
    try:
        chain = fetch_chain(symbol)
    except Exception as e:
        result['error'] = f'chain fetch failed: {e}'
        return result
    intr = intraday_from_chain(chain)
    if not intr:
        result['error'] = 'no underlying data in chain header'
        return result
    spot, prev = intr['spot'], intr['prev_close']
    result['clock'] = intr['clock'].strftime('%Y-%m-%d %H:%M ET')

    contract, why = select_contract(chain, symbol, dec['side'], spot, cfg, intr['clock'])
    if contract is None:
        result['error'] = why
        return result

    faded_move_abs = abs(spot - prev)
    T_exit = max(contract['T'] - cfg['exit_hold_days'] / 252.0, 1e-5)
    ladder = build_ladder(contract['entry_mid'], contract['forward'], contract['strike'],
                          contract['iv'], T_exit, cfg['rate'], dec['side'], faded_move_abs, spot, cfg)
    result['contract'] = contract
    result['ladder'] = ladder
    return result


# --- alerting ----------------------------------------------------------------

def format_alert(res: dict) -> str:
    c = res['contract']
    rd = res['read']
    lines = [
        f"\a=== FADE SETUP RIPE: {res['symbol']} ===  ({res.get('clock', '')})",
        f"  {res['symbol']} {res['spot']}  ({res['move_pct']:+.2f}% from prior close {res['prev_close']})",
        f"  SIDE: {res['side']}",
        f"  signals fired: VWAP {rd['vwap_sigma']:+.2f}sd + " +
        " + ".join(res['confirmations']) + f"  ({res['n_confirm']} confirmations)",
        f"  read: RSI2 {rd['rsi2']}  RSI14 {rd['rsi14']}  ATR-ext {rd['atr_ext']}  "
        f"%b {rd['pctb']}  band {rd['band_tag']:+d}  pd {rd['pd_tag']:+d}",
        f"  CONTRACT: {res['symbol']} {c['expiry']} {c['strike']:.0f}{c['cp']}"
        f"  ({c['otm_pct']}% OTM, {c['dte']}d)",
        f"    entry ~{c['entry_mid']} (ask {c['entry_ask']})  IV {c['iv']}  "
        f"delta {c['delta']}  theta {c['theta']}/day  OI {c['open_interest']}  spread {c['spread_pct']}%",
        f"  SCALE-OUT LADDER (reversion of the {abs(res['spot']-res['prev_close']):.2f}pt move):",
    ]
    for r in res['ladder']:
        lines.append(f"    {int(r['retrace']*100):>3}% retrace -> underlying {r['underlying_at']}  "
                     f"option ~{r['option_price']}  ({r['gain_pct']:+.0f}%)  "
                     f"take {int(r['take_fraction']*100)}%  [{r['label']}]")
    lines.append("  Size for a total loss. This is a ticket, not an order.")
    return "\n".join(lines)


def deliver(res: dict):
    text = format_alert(res)
    print(text)
    topic = os.environ.get('FADE_WATCH_NTFY')
    if topic:
        try:
            urllib.request.urlopen(urllib.request.Request(
                f'https://ntfy.sh/{topic}', data=text.replace('\a', '').encode(),
                headers={'Title': f"Fade {res['symbol']} {res['cp']} ({res['n_confirm']}+VWAP)",
                         'Priority': 'high', 'Tags': 'chart_with_downwards_trend'}), timeout=TIMEOUT)
        except Exception as e:
            print(f'  (ntfy push failed: {e})', file=sys.stderr)
    hook = os.environ.get('FADE_WATCH_WEBHOOK')
    if hook:
        try:
            urllib.request.urlopen(urllib.request.Request(
                hook, data=json.dumps({'content': text.replace('\a', '')}).encode(),
                headers={'Content-Type': 'application/json'}), timeout=TIMEOUT)
        except Exception as e:
            print(f'  (webhook post failed: {e})', file=sys.stderr)
    with ALERT_LOG.open('a') as f:
        f.write(json.dumps({'ts': datetime.utcnow().isoformat(), **res}) + '\n')


# --- de-dupe across polls ----------------------------------------------------

def _load_state() -> dict:
    try:
        return json.loads(STATE.read_text())
    except Exception:
        return {}


def _save_state(s: dict):
    try:
        STATE.write_text(json.dumps(s))
    except Exception:
        pass


def should_alert(res: dict, cfg: dict) -> bool:
    """Fire once per ripening; re-fire only after the cooldown, or if another
    signal has joined the confluence since (a materially stronger setup)."""
    if not res.get('ripe') or res.get('error') or 'contract' not in res:
        return False
    st = _load_state()
    key = f"{res['symbol']}:{res['cp']}"
    prev = st.get(key)
    now = datetime.utcnow()
    if prev:
        last = datetime.fromisoformat(prev['ts'])
        cooled = (now - last).total_seconds() / 60.0 >= cfg['realert_cooldown_min']
        stronger = res['n_confirm'] > prev.get('n_confirm', 0)
        if not (cooled or stronger):
            return False
    st[key] = {'ts': now.isoformat(), 'n_confirm': res['n_confirm']}
    _save_state(st)
    return True


# --- loop --------------------------------------------------------------------

def run_loop(cfg: dict):
    print(f"fade_watch: polling {', '.join(cfg['symbols'])} every {cfg['poll_seconds']}s. "
          f"Ctrl-C to stop.", file=sys.stderr)
    while True:
        for sym in cfg['symbols']:
            res = evaluate(sym, cfg)
            tag = 'RIPE' if res.get('ripe') else 'quiet'
            if res.get('error'):
                note = res['error']
            else:
                rd = res.get('read', {})
                note = (f"move {res.get('move_pct')}%  VWAP {rd.get('vwap_sigma')}sd  "
                        f"RSI2 {rd.get('rsi2')}  confirms {res.get('n_confirm')}")
            print(f"  {sym}: {tag}  {note}", file=sys.stderr)
            if should_alert(res, cfg):
                deliver(res)
        time.sleep(cfg['poll_seconds'])


# --- selftest ----------------------------------------------------------------

def selftest() -> int:
    # pricing: parity, ATM equality, iv round-trip, intrinsic
    F, K, T, r, s = 100.0, 100.0, 0.02, 0.04, 0.20
    c = black76(F, K, T, r, s, 'C'); p = black76(F, K, T, r, s, 'P')
    assert abs((c - p) - math.exp(-r * T) * (F - K)) < 1e-9
    assert abs(c - p) < 1e-9
    got = implied_vol(c, F, K, T, r, 'C')
    assert got and abs(got - s) < 1e-6
    assert abs(black76(110, 100, 0, r, s, 'C') - 10) < 1e-9

    # greeks: OTM put delta is negative and small; theta is negative
    g = greeks(5000.0, 5100.0, 3 / 252, r, 0.15, 'P')
    assert -1 < g['delta'] < 0 and g['theta'] < 0 and g['gamma'] > 0

    # OCC parsing including the weekly root that broke the first pass
    assert parse_occ('SPXW260819P00700000') == ('260819', 'P', 700.0)
    assert parse_occ('QQQ260819C00600000') == ('260819', 'C', 600.0)

    # the setup decision lives in signals.py; confirm the wiring holds here
    hot = signals.evaluate_signals(
        {'move_pct': 1.8, 'vwap_sigma': 2.4, 'rsi2': 97, 'atr_ext': 1.7,
         'band_tag': 1, 'pd_tag': 0}, CONFIG['signals'])
    assert hot['fired'] and hot['side'] == 'P'
    calm = signals.evaluate_signals(
        {'move_pct': 0.3, 'vwap_sigma': 0.4, 'rsi2': 55, 'atr_ext': 0.3,
         'band_tag': 0, 'pd_tag': 0}, CONFIG['signals'])
    assert not calm['fired']

    # ladder: rungs are increasing in retrace and the gains are ordered for a put
    lad = build_ladder(1.0, 5000.0, 4912.0, 0.15, 2 / 252, r, 'P',
                       faded_move_abs=100.0, spot=5000.0, cfg=CONFIG)
    assert len(lad) == 3
    assert lad[0]['gain_pct'] < lad[-1]['gain_pct']       # deeper reversion pays more
    assert lad[-1]['underlying_at'] < 5000.0              # a put profits as spot falls

    print('selftest: all checks passed')
    return 0


# --- cli ---------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--once', action='store_true', help='evaluate once and print')
    ap.add_argument('--loop', action='store_true', help='poll during the session')
    ap.add_argument('--symbol', help='restrict to one symbol')
    ap.add_argument('--json', action='store_true', help='machine-readable output')
    ap.add_argument('--selftest', action='store_true')
    args = ap.parse_args()

    cfg = dict(CONFIG)
    if args.symbol:
        cfg['symbols'] = [args.symbol.upper()]

    if args.selftest:
        return selftest()
    if args.loop:
        run_loop(cfg)
        return 0

    # default: --once
    out = [evaluate(s, cfg) for s in cfg['symbols']]
    if args.json:
        print(json.dumps(out, indent=2))
        return 0
    for res in out:
        if res.get('error') and not res.get('ripe'):
            print(f"{res['symbol']}: ({res['error']})")
        elif res.get('ripe') and 'contract' in res:
            print(format_alert(res))
        else:
            rd = res.get('read', {})
            print(f"{res['symbol']} {res.get('spot')}  {res.get('move_pct')}%  (quiet)  "
                  f"VWAP {rd.get('vwap_sigma')}sd  RSI2 {rd.get('rsi2')}  "
                  f"ATR-ext {rd.get('atr_ext')}  %b {rd.get('pctb')}  "
                  f"confirms {res.get('n_confirm')}/{cfg['signals']['min_confirm']}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
