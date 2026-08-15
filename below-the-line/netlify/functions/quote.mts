/**
 * Live quote proxy for the mid-week price check.
 *
 * The 200-week line barely moves between Friday closes (one week is 1/200th
 * of the average), so a live price against the stored weekly line is accurate
 * all week. This function proxies Yahoo's chart endpoint server-side (no API
 * key, no CORS issue) and lets the CDN cache responses for 5 minutes so we
 * never hammer Yahoo regardless of traffic.
 *
 * SPLITS. The stored 200-week line is in pre-split terms until the next
 * Saturday pipeline run restates it, while the live quote is post-split
 * immediately. Comparing the two manufactures a crossing: Monster split 2-for-1
 * on 11 Aug 2026 and read as 25% below a line it was 49% above. So the caller
 * passes the date its stored levels came from, and this returns the cumulative
 * split factor since then. Anything derived from the stored levels has to be
 * divided by that factor before it is compared to `price`.
 *
 * GET /.netlify/functions/quote?symbol=MNST&since=2026-08-07
 * -> { symbol, price, previousClose, marketState, asOf,
 *      splitFactor, splits: [{ date, ratio, label }] }
 */

const SYMBOL_RE = /^[A-Z0-9.\-]{1,10}$/;
const SINCE_RE = /^\d{4}-\d{2}-\d{2}$/;

type SplitEvent = { date: string; ratio: number; label: string };

/** Splits strictly after `sinceMs`, plus their cumulative ratio. */
function splitsSince(result: any, sinceMs: number | null): {
  splits: SplitEvent[];
  factor: number;
} {
  const raw = result?.events?.splits;
  if (!raw || sinceMs === null) return { splits: [], factor: 1 };

  const splits: SplitEvent[] = [];
  let factor = 1;

  for (const key of Object.keys(raw)) {
    const ev = raw[key];
    const num = Number(ev?.numerator);
    const den = Number(ev?.denominator);
    const ts = Number(ev?.date);
    if (!isFinite(num) || !isFinite(den) || den === 0 || !isFinite(ts)) continue;

    const at = ts * 1000;
    if (at <= sinceMs) continue;

    const ratio = num / den;
    // Guard against absurd values rather than silently rescaling a price by
    // a bad number. A 1000:1 reverse split is real but rare enough to be worth
    // a manual look; better to show nothing than to show nonsense.
    if (!isFinite(ratio) || ratio <= 0 || ratio > 100 || ratio < 0.01) continue;

    splits.push({
      date: new Date(at).toISOString().slice(0, 10),
      ratio,
      label: ev?.splitRatio || `${num}-for-${den}`,
    });
    factor *= ratio;
  }

  splits.sort((a, b) => a.date.localeCompare(b.date));
  return { splits, factor };
}

export default async (req: Request) => {
  const url = new URL(req.url);
  const symbol = (url.searchParams.get("symbol") || "").toUpperCase().trim();
  const since = (url.searchParams.get("since") || "").trim();

  if (!SYMBOL_RE.test(symbol)) {
    return Response.json({ error: "invalid symbol" }, { status: 400 });
  }

  const sinceMs = SINCE_RE.test(since) ? Date.parse(since + "T00:00:00Z") : null;

  try {
    // 3mo range so a split is still visible if the pipeline has been stalled
    // for weeks; events=split costs nothing extra on the same call.
    const yahoo = await fetch(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}` +
        `?interval=1d&range=3mo&events=split`,
      { headers: { "User-Agent": "Mozilla/5.0 (mungbeans.io mid-week check)" } }
    );
    if (!yahoo.ok) {
      return Response.json({ error: "quote unavailable" }, {
        status: 502,
        headers: { "Cache-Control": "public, max-age=60" },
      });
    }

    const data = await yahoo.json();
    const result = data?.chart?.result?.[0];
    const meta = result?.meta;
    const price = meta?.regularMarketPrice;

    if (typeof price !== "number" || !isFinite(price) || price <= 0) {
      return Response.json({ error: "quote unavailable" }, {
        status: 502,
        headers: { "Cache-Control": "public, max-age=60" },
      });
    }

    const { splits, factor } = splitsSince(result, sinceMs);

    return Response.json(
      {
        symbol,
        price,
        previousClose: meta?.chartPreviousClose ?? null,
        marketState: meta?.marketState ?? null,
        asOf: new Date().toISOString(),
        // 1 when nothing happened. Divide any stored pre-split level by this
        // before comparing it to `price`.
        splitFactor: factor,
        splits,
      },
      {
        headers: {
          // CDN caches per-symbol for 5 minutes; browsers for 1.
          "Cache-Control": "public, max-age=60, s-maxage=300, stale-while-revalidate=600",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );
  } catch {
    return Response.json({ error: "quote unavailable" }, {
      status: 502,
      headers: { "Cache-Control": "public, max-age=60" },
    });
  }
};
