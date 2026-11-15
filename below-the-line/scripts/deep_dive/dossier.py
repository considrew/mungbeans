#!/usr/bin/env python3
"""Build the numbers dossier for one ticker.

Everything quantitative the deep dive states about the company's reported
financials should trace to this file or to a source Claude cites. The publish
price in particular comes from here and nowhere else: validate.py checks the
page against `publish.price`.

Usage:
    python dossier.py TICKER
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from common import STOCKS_JSON, dossier_path, load_json, write_json
from select_candidates import recent_news

INFO_KEYS = [
    "longName", "exchange", "fullExchangeName", "sector", "industry", "country", "website",
    "longBusinessSummary", "fullTimeEmployees", "marketCap", "enterpriseValue",
    "sharesOutstanding", "impliedSharesOutstanding", "floatShares",
    "sharesShort", "shortPercentOfFloat", "heldPercentInsiders", "heldPercentInstitutions",
    "totalCash", "totalDebt", "totalRevenue", "revenueGrowth", "grossMargins",
    "operatingMargins", "ebitdaMargins", "profitMargins", "ebitda", "freeCashflow",
    "operatingCashflow", "trailingEps", "forwardEps", "trailingPE", "forwardPE",
    "priceToSalesTrailing12Months", "enterpriseToRevenue", "enterpriseToEbitda",
    "beta", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "fiftyDayAverage", "twoHundredDayAverage",
    "targetLowPrice", "targetMeanPrice", "targetMedianPrice", "targetHighPrice",
    "numberOfAnalystOpinions", "recommendationKey", "dividendYield",
    "lastFiscalYearEnd", "mostRecentQuarter", "companyOfficers",
]


def frame(df: pd.DataFrame | None, cols: int) -> dict:
    """Statement DataFrame -> {period: {line item: value}}, newest first."""
    if df is None or df.empty:
        return {}
    out = {}
    for col in list(df.columns)[:cols]:
        label = col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col)
        series = df[col].dropna()
        out[label] = {str(k): float(v) for k, v in series.items()}
    return out


def safe(fn, default=None):
    try:
        return fn()
    except Exception as e:  # yfinance endpoints fail independently; keep the rest
        return default if default is not None else {"_error": str(e)}


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    sym = sys.argv[1].upper()
    t = yf.Ticker(sym)

    hist = t.history(period="2y", interval="1d", auto_adjust=False)
    if hist.empty:
        print(f"ERROR: no price history for {sym}", file=sys.stderr)
        return 1
    close = hist["Close"]
    last_date = close.index[-1]
    last = float(close.iloc[-1])
    yr = close.iloc[-252:]

    def ret(n):
        return round((last / float(close.iloc[-1 - n]) - 1) * 100, 2) if len(close) > n else None

    ytd_base = close[close.index.year < last_date.year]
    info = safe(lambda: t.info, {})
    officers = [
        {k: o.get(k) for k in ("name", "title", "age", "yearBorn", "totalPay")}
        for o in (info.get("companyOfficers") or [])[:8]
    ]

    stocks = load_json(STOCKS_JSON) or {"stocks": []}
    site = next((s for s in stocks["stocks"] if s.get("symbol") == sym), {})
    site = {k: v for k, v in site.items() if k not in ("growth_chart", "touch_chart")}

    dossier = {
        "ticker": sym,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "publish": {
            # The price the article is graded against. Last completed session.
            "price": round(last, 2),
            "price_date": last_date.strftime("%Y-%m-%d"),
        },
        "price": {
            "last_close": round(last, 2),
            "week_52_low": round(float(yr.min()), 2),
            "week_52_high": round(float(yr.max()), 2),
            "return_5d_pct": ret(5),
            "return_1m_pct": ret(21),
            "return_3m_pct": ret(63),
            "return_1y_pct": ret(252),
            "return_ytd_pct": round((last / float(ytd_base.iloc[-1]) - 1) * 100, 2) if len(ytd_base) else None,
            "last_10_sessions": [
                {"date": d.strftime("%Y-%m-%d"), "close": round(float(c), 2), "volume": int(v)}
                for d, c, v in zip(hist.index[-10:], close.iloc[-10:], hist["Volume"].iloc[-10:])
            ],
        },
        "info": {k: info.get(k) for k in INFO_KEYS if k != "companyOfficers"},
        "officers": officers,
        "share_count_note": (
            "sharesOutstanding is BASIC. The framework requires fully diluted per-share "
            "math with components stated (options, RSUs, warrants, convertibles, deal "
            "consideration). Get the components from the latest 10-Q/10-K and state them."
        ),
        "quarterly": {
            "income": safe(lambda: frame(t.quarterly_income_stmt, 8)),
            "balance_sheet": safe(lambda: frame(t.quarterly_balance_sheet, 5)),
            "cash_flow": safe(lambda: frame(t.quarterly_cashflow, 8)),
        },
        "annual": {
            "income": safe(lambda: frame(t.income_stmt, 4)),
            "balance_sheet": safe(lambda: frame(t.balance_sheet, 4)),
            "cash_flow": safe(lambda: frame(t.cashflow, 4)),
        },
        "earnings_dates": safe(lambda: [
            {"date": str(i), **{str(k): (None if pd.isna(v) else v) for k, v in r.items()}}
            for i, r in t.earnings_dates.head(8).iterrows()
        ], []),
        "analyst_price_targets": safe(lambda: dict(t.analyst_price_targets), {}),
        "insider_transactions": safe(lambda: t.insider_transactions.head(20).to_dict("records"), []),
        "institutional_holders": safe(lambda: t.institutional_holders.head(10).to_dict("records"), []),
        "news_last_30d": recent_news(sym, days=30),
        "mungbeans_site_data": site,
    }

    path = dossier_path(sym)
    write_json(path, dossier)
    print(f"Dossier written: {path}")
    print(f"Publish price: ${dossier['publish']['price']} ({dossier['publish']['price_date']})")
    print(f"52w range: {dossier['price']['week_52_low']} - {dossier['price']['week_52_high']}")
    print(f"Quarters of income data: {len(dossier['quarterly']['income'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
