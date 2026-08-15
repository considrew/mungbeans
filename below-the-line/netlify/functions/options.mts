/**
 * Live option chain for one symbol, scored server-side.
 *
 * Mirrors scripts/options_screener.py. That script is the batch/base-rate path;
 * this is the on-page path. If the scoring rules change, change both — the
 * Python self-test is the reference implementation.
 *
 * WHY SERVER-SIDE: Yahoo's option endpoint blocks browser CORS, and the
 * arithmetic wants the ask rather than the last trade, which is easy to get
 * wrong in a template. Doing it here keeps one implementation and lets the CDN
 * absorb the traffic.
 *
 * DELAY: Yahoo option quotes are delayed, typically 15 minutes. This is fine
 * for measuring how a chain is priced and useless as an execution trigger. The
 * response carries `delayed: true` so the page can say so rather than implying
 * a live tape.
 *
 * GET /.netlify/functions/options?symbol=NVDA&expiries=4
 */

const SYMBOL_RE = /^[A-Z0-9.\-]{1,10}$/;

// Scope for the execution test. Widen once the numbers have been checked
// against a broker's chain on these names.
const ENABLED = new Set(["AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA"]);

const MIN_OPEN_INTEREST = 100;
const MIN_VOLUME = 10;
const MAX_SPREAD_PCT = 0.15;
const MIN_DAYS = 21;
const MAX_DAYS = 900;

const UA = { "User-Agent": "Mozilla/5.0 (mungbeans.io options)" };

/** Third Friday of its month. Weeklies are excluded on purpose. */
function isMonthly(d: Date): boolean {
  return d.getUTCDay() === 5 && d.getUTCDate() >= 15 && d.getUTCDate() <= 21;
}

/** UTC, always. Yahoo stamps expirations at UTC midnight. */
function toUTCDate(ts: number): Date {
  return new Date(ts * 1000);
}

function daysBetween(from: Date, to: Date): number {
  return Math.round((to.getTime() - from.getTime()) / 864e5);
}

/**
 * Dividends expected before expiry, approximated from the trailing yield.
 * Without this the screen calls every dividend payer's deep ITM calls cheap,
 * because a call is a claim on the stock without its dividends.
 */
function expectedDividends(price: number, annualYieldPct: number, days: number): number {
  if (!(annualYieldPct > 0) || !(price > 0) || !(days > 0)) return 0;
  return price * (annualYieldPct / 100) * (days / 365);
}

function scoreContract(c: any, spot: number, divYield: number, today: Date, isCall: boolean) {
  const strike = Number(c?.strike);
  const bid = Number(c?.bid ?? 0);
  const ask = Number(c?.ask ?? 0);
  const oi = Number(c?.openInterest ?? 0);
  const vol = Number(c?.volume ?? 0);
  const expTs = Number(c?.expiration);
  if (![strike, bid, ask, expTs].every((n) => isFinite(n))) return null;

  const expiry = toUTCDate(expTs);
  const days = daysBetween(today, expiry);
  if (days < MIN_DAYS || days > MAX_DAYS) return null;
  if (!isMonthly(expiry)) return null;
  if (oi < MIN_OPEN_INTEREST || vol < MIN_VOLUME) return null;
  if (ask <= 0 || bid <= 0 || ask < bid) return null;

  const mid = (bid + ask) / 2;
  const spreadPct = mid > 0 ? (ask - bid) / mid : 1;
  if (spreadPct > MAX_SPREAD_PCT) return null;

  const intrinsic = isCall ? Math.max(0, spot - strike) : Math.max(0, strike - spot);
  const timeValue = ask - intrinsic;                 // cost to enter is the ask
  const divs = expectedDividends(spot, divYield, days);

  // Negative means buying the call and exercising beats buying the stock,
  // after giving up the dividends you would have collected holding shares.
  const parityGap = isCall ? ask + strike - (spot - divs) : null;
  const discountPct = parityGap !== null && spot > 0 ? (-parityGap / spot) * 100 : null;

  const iv = Number(c?.impliedVolatility);

  return {
    contract: c?.contractSymbol ?? null,
    type: isCall ? "call" : "put",
    strike: +strike.toFixed(2),
    expiry: expiry.toISOString().slice(0, 10),
    days,
    bid: +bid.toFixed(2),
    ask: +ask.toFixed(2),
    spread: +(ask - bid).toFixed(2),
    spreadPct: +(spreadPct * 100).toFixed(1),
    volume: vol,
    openInterest: oi,
    intrinsic: +intrinsic.toFixed(2),
    timeValue: +timeValue.toFixed(3),
    timeValuePerDay: days ? +(timeValue / days).toFixed(4) : null,
    expectedDividends: +divs.toFixed(3),
    parityGap: parityGap === null ? null : +parityGap.toFixed(3),
    belowParity: parityGap !== null && parityGap < 0,
    discountPct: discountPct === null ? null : +discountPct.toFixed(2),
    iv: isFinite(iv) && iv > 0 ? +(iv * 100).toFixed(1) : null,
  };
}

async function fetchChain(symbol: string, date?: number) {
  const q = date ? `?date=${date}` : "";
  // query2 first; query1 as a fallback because they throttle independently.
  for (const host of ["query2", "query1"]) {
    try {
      const r = await fetch(
        `https://${host}.finance.yahoo.com/v7/finance/options/${encodeURIComponent(symbol)}${q}`,
        { headers: UA }
      );
      if (!r.ok) continue;
      const j = await r.json();
      const res = j?.optionChain?.result?.[0];
      if (res) return res;
    } catch {
      // try the other host
    }
  }
  return null;
}

export default async (req: Request) => {
  const url = new URL(req.url);
  const symbol = (url.searchParams.get("symbol") || "").toUpperCase().trim();
  const want = Math.min(Math.max(Number(url.searchParams.get("expiries") || 4), 1), 8);

  if (!SYMBOL_RE.test(symbol)) {
    return Response.json({ error: "invalid symbol" }, { status: 400 });
  }
  if (!ENABLED.has(symbol)) {
    return Response.json(
      { error: "not enabled", detail: "Options are limited to the test set while the numbers are being checked." },
      { status: 404, headers: { "Cache-Control": "public, max-age=3600" } }
    );
  }

  const head = await fetchChain(symbol);
  if (!head?.expirationDates?.length) {
    return Response.json({ error: "chain unavailable" }, {
      status: 502, headers: { "Cache-Control": "public, max-age=60" },
    });
  }

  const spot = Number(head?.quote?.regularMarketPrice);
  if (!isFinite(spot) || spot <= 0) {
    return Response.json({ error: "no price" }, { status: 502 });
  }

  // Yahoo reports this as a fraction on some symbols and a percent on others.
  const rawYield = Number(head?.quote?.trailingAnnualDividendYield ?? 0);
  const divYield = isFinite(rawYield) ? (rawYield < 1 ? rawYield * 100 : rawYield) : 0;

  const today = new Date();
  const monthlies = (head.expirationDates as number[]).filter((ts) => {
    const d = toUTCDate(ts);
    const n = daysBetween(today, d);
    return isMonthly(d) && n >= MIN_DAYS && n <= MAX_DAYS;
  });

  // Nearest few plus the furthest, so a LEAP is always in the sample rather
  // than falling off the end of the list.
  const picked = monthlies.slice(0, want - 1);
  if (monthlies.length > want - 1) picked.push(monthlies[monthlies.length - 1]);

  const chains = await Promise.all(picked.map((ts) => fetchChain(symbol, ts)));

  const contracts: any[] = [];
  for (const ch of chains) {
    const opt = ch?.options?.[0];
    if (!opt) continue;
    for (const c of opt.calls || []) {
      const r = scoreContract(c, spot, divYield, today, true);
      if (r) contracts.push(r);
    }
    for (const c of opt.puts || []) {
      const r = scoreContract(c, spot, divYield, today, false);
      if (r) contracts.push(r);
    }
  }

  contracts.sort((a, b) => (b.discountPct ?? -999) - (a.discountPct ?? -999));

  return Response.json(
    {
      symbol,
      spot,
      dividendYield: +divYield.toFixed(2),
      asOf: new Date().toISOString(),
      delayed: true,
      delayNote: "Yahoo option quotes are delayed, typically 15 minutes.",
      expiriesChecked: picked.length,
      criteria: {
        minOpenInterest: MIN_OPEN_INTEREST, minVolume: MIN_VOLUME,
        maxSpreadPct: MAX_SPREAD_PCT * 100, minDays: MIN_DAYS, maxDays: MAX_DAYS,
        monthliesOnly: true,
      },
      belowParityCount: contracts.filter((c) => c.belowParity).length,
      contracts,
    },
    {
      headers: {
        "Cache-Control": "public, max-age=30, s-maxage=60, stale-while-revalidate=300",
        "Access-Control-Allow-Origin": "*",
      },
    }
  );
};
