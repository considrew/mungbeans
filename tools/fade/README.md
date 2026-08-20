# fade — a watcher for fading intraday extension with cheap OTM optionality

Status: **live paper-testing tool.** It reads live market data and hands you a
priced ticket. It never places an order. Local/personal use; publishes nothing
to the site.

## The strategy, in one paragraph

When an index runs hard and the day's thesis starts to turn, buy a cheap
out-of-the-money option in the reversion direction — puts after an up-move,
calls after a down-move — a few days from expiry so it survives the overnight
hold, and scale out into the next day's swing. Most tickets expire worthless
for small money; the occasional hard reversion pays multiples. The edge is the
fat right tail, so the exit must never cap it.

## Why this shape (from the research phase)

`research/` holds the study that got us here, on ten years of data:

- Buying **at-the-money** and holding to a same-day (0DTE) expiry is
  structurally negative — you pay the volatility risk premium every day.
- **Fading** a large move with a **cheap OTM** option is not: hold-to-close
  mean return crosses positive once the faded move exceeds ~2%, and the edge
  grows monotonically with the size of the move.
- A **fixed profit target destroys it** — the payoff is fat-tailed, so a
  ladder that banks partials and rides a runner beats any single target, and a
  tight overnight stop bleeds it (the adverse extreme often precedes the
  reversion).

The 0DTE framing was a dead end; this tool is the 2–5 DTE overnight version
that the data actually supports.

## `fade_watch.py`

Standard-library Python, no dependencies. One well-documented file.

```
python3 fade_watch.py --selftest        # pricing + scoring self-checks, no network
python3 fade_watch.py --once            # evaluate SPX and QQQ now, print
python3 fade_watch.py --once --symbol QQQ
python3 fade_watch.py --once --json     # machine-readable
python3 fade_watch.py --loop            # poll during the session, alert on ripen
```

What it does each poll:

1. Computes a **signal read** on the symbol's tradeable proxy (SPY for SPX,
   QQQ direct) from 5-minute bars — see `signals.py`.
2. Decides on **confluence**, not a score: the trigger is silent until the
   VWAP σ-extension spine is armed **and** at least `min_confirm` of the other
   enabled signals point the same way. A quiet tape costs one intraday
   request, not a chain.
3. Only if it fires does it touch the Cboe chain — selecting the expiry
   (2–5 DTE) and strike (your 1.5–2% OTM band), pricing off a **self-built**
   surface (forward implied from put-call parity, IV from the mid, because the
   vendor greeks are internally inconsistent — see research), and building the
   scale-out ladder.
4. Fires one alert naming exactly which signals lit up (de-duped per side, with
   a cooldown; re-fires if another signal joins the confluence) and logs it to
   `alerts.jsonl`.

### The signals (`signals.py`)

Each is a standard, individually-computable read; the tingle is quantified as
their agreement. All thresholds live at the top of `fade_watch.py` under
`CONFIG['signals']`.

| Signal | Measures | Fires when |
|---|---|---|
| **VWAP σ-extension** (spine, required) | stretch from the session's volume-weighted fair price | \|z\| ≥ 2σ |
| **RSI(2)** | fast momentum exhaustion (Connors' index reversal) | ≥ 95 / ≤ 5 |
| **ATR-normalized move** | the move in units of normal daily range | ≥ 1.5 ATR |
| **Bollinger tag-reject** | pierced a 2σ band and closed back inside | cross back through 1.0 / 0.0 |
| **Prior-day level tag** | tagged the prior-day high/low and rejected | tag + reject |

Run a live read on its own: `python3 signals.py --symbol SPY --json`.

### Getting the ping on your phone

Set one environment variable before `--loop`:

```
export FADE_WATCH_NTFY=some-unguessable-topic   # https://ntfy.sh, install the app, subscribe to the topic
# or
export FADE_WATCH_WEBHOOK=https://discord.com/api/webhooks/...   # POSTs the alert
```

### A fired alert looks like

```
=== FADE SETUP RIPE: SPX ===
  SPX 7707.98  (+2.30% from prior close 7534.68)
  SIDE: PUT (fade up)
  signals fired: VWAP +2.41sd + rsi2 + atr + band  (3 confirmations)
  read: RSI2 96.8  RSI14 74.2  ATR-ext 1.9  %b 0.88  band +1  pd +0
  CONTRACT: SPX 260824 7575P  (1.73% OTM, 4.02d)
    entry ~3.45  IV 0.1304  delta -0.073  theta -1.82/day  OI 824  spread 2.9%
  SCALE-OUT LADDER:
     35% retrace -> underlying 7647  option ~5.83   (+69%)   take 34%  [bank on the open pop]
     60% retrace -> underlying 7604  option ~14.66  (+325%)  take 33%  [second bank]
    100% retrace -> underlying 7535  option ~44.99  (+1204%) take 33%  [runner -- do not cap]
```

## Honest limitations

- The score weights and trigger are a **starting point, not a validated
  optimum.** The realtime intraday setup is not the same object as the
  close-to-close backtest, and no free source carries intraday option history
  to backtest it properly. The first weeks are paper testing; `alerts.jsonl`
  is there so the thresholds get tuned against outcomes, not vibes.
- Option prices are from a model surface fitted to live mids. Real fills are
  worse than mid. Size every ticket for a total loss.
- The Cboe feed is delayed (~15 min). For a setup that is about being early,
  treat the alert as "go look now," not "the price you will get."

## Files

- `fade_watch.py` — the watcher.
- `research/` — the study behind the strategy (`base_rate.py`,
  `validate_surface.py`, `findings.html`, `README.md`).
- `alerts.jsonl` — created on first alert; the paper-testing ledger.
