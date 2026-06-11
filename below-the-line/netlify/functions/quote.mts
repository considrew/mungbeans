/**
 * Live quote proxy for the mid-week price check.
 *
 * The 200-week line barely moves between Friday closes (one week is 1/200th
 * of the average), so a live price against the stored weekly line is accurate
 * all week. This function proxies Yahoo's chart endpoint server-side (no API
 * key, no CORS issue) and lets the CDN cache responses for 5 minutes so we
 * never hammer Yahoo regardless of traffic.
 *
 * GET /.netlify/functions/quote?symbol=NKE
 * -> { symbol, price, previousClose, marketState, asOf }
 */

const SYMBOL_RE = /^[A-Z0-9.\-]{1,10}$/;

export default async (req: Request) => {
  const url = new URL(req.url);
  const symbol = (url.searchParams.get("symbol") || "").toUpperCase().trim();

  if (!SYMBOL_RE.test(symbol)) {
    return Response.json({ error: "invalid symbol" }, { status: 400 });
  }

  try {
    const yahoo = await fetch(
      `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=1d`,
      { headers: { "User-Agent": "Mozilla/5.0 (mungbeans.io mid-week check)" } }
    );
    if (!yahoo.ok) {
      return Response.json({ error: "quote unavailable" }, {
        status: 502,
        headers: { "Cache-Control": "public, max-age=60" },
      });
    }

    const data = await yahoo.json();
    const meta = data?.chart?.result?.[0]?.meta;
    const price = meta?.regularMarketPrice;

    if (typeof price !== "number" || !isFinite(price) || price <= 0) {
      return Response.json({ error: "quote unavailable" }, {
        status: 502,
        headers: { "Cache-Control": "public, max-age=60" },
      });
    }

    return Response.json(
      {
        symbol,
        price,
        previousClose: meta?.chartPreviousClose ?? null,
        marketState: meta?.marketState ?? null,
        asOf: new Date().toISOString(),
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
