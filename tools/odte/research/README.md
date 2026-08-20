# 0DTE / weekly options scoring — research phase

Status: **research only, pending approval. Nothing here trades or is wired into the site.**

Findings writeup: `findings.html` (also published as an artifact).

## What was established

1. **Buying 0DTE premium is structurally negative-EV.** Over 1,070 sessions
   (May 2022 - Aug 2026), an ATM 0DTE bought at the open and settled at the
   close returns -5.3% (calls) / -15.4% (puts) on average, median -80% / -100%.
   Realized moves come in at 0.614x the implied 1-sigma; only 25.9% of sessions
   exceed 1 sigma against a fair-value expectation of 31.7%.

2. **Exiting into a swing before expiry helps but does not create edge.**
   Across 1,442 trades on hourly paths with target/stop exits, every
   configuration is negative (-6.4% to -17.7% mean). It converts the median
   outcome from -80% to -30% -- it controls the loss, it does not make the gain.

3. **~56% directional accuracy is break-even before costs**, near 60% after.
   That is the bar any scoring model has to clear, and it only has to clear it
   on the sessions it chooses to trade. Hence: build a gate, not a signal.

4. **Vendor greeks are unusable.** Cboe's published per-contract IV disagrees
   between call and put at the same strike by up to 0.05 vol points. Implying
   the forward from put-call parity (it sat 16.17 pts above the quoted spot)
   and solving our own IVs from mids cuts mean repricing error from $7.01 to
   $1.28 and brings call/put IV agreement to a median of 0.0001 vol points.
   The engine must build its own surface.

5. **Execution cost does not decide the instrument.** Measured on the front
   unexpired expiry: SPX 1.8% of mid, SPY 1.7%, QQQ 2.6% (off-hours snapshot,
   so upper bounds). SPX earns the default slot on structure -- cash
   settlement, no early assignment, 60/40 tax treatment -- not on spread.

6. **SPX trades under two roots and the daily expiries are all `SPXW`.**
   Parsing OCC symbols by fixed offset works on monthlies and silently
   mangles every 0DTE contract; a first probe reported zero 0DTE contracts on
   a chain carrying thousands. Parse by pattern, and assert the chosen expiry
   is unexpired -- the feed keeps settled expiries with stale two-sided
   quotes, so pricing at T <= 0 returns intrinsic values that look valid.
   Both cases are pinned in `--selftest`.

## Data availability

Free and working: Cboe delayed chains (SPX/SPY/QQQ, bid/ask/IV/greeks/OI/volume),
Cboe index history (VIX1D 2022+, VIX9D 2011+, VIX, VVIX, SKEW), Yahoo charts
(1h ~3yr, 5m ~60d, 1m ~8d/request, daily decades).

Not available: historical 0DTE option quotes. DoltHub carries monthly expiries
only with no QQQ; Cboe's historical statistics return 403; FMP intraday is a
paid tier. Alpha Vantage HISTORICAL_OPTIONS may work if the account tier
includes it -- no key present in this environment.

**Therefore every backtest figure above is model-repriced**: real underlying
paths and real VIX1D, theoretical option prices, no historical bid-ask. Sound
for measuring structural drag. Not sound for validating fill quality.

## Scripts

- `validate_surface.py` -- pulls a live Cboe chain, implies the forward from
  parity, solves IVs from mids, reports repricing error and call/put agreement.
- `base_rate.py` -- the negative-EV study and the break-even accuracy curve.

Both are read-only and hit only free public endpoints.

## Note on the existing screener

`below-the-line/scripts/options_screener.py` reads Yahoo's option endpoint,
which now returns `Invalid Crumb`. That screener is likely broken in
production. Migrating it to the Cboe source would fix it -- separate from
this build.
