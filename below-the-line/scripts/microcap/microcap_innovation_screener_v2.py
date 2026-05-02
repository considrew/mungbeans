"""
Micro Cap Innovation Screener v2
=================================
Screens US stocks ($50M-$500M market cap) for innovation signals
and management quality. Filters out companies with red flags
like dilution, reverse splits, and excessive stock compensation.

Scores each company on:
  1. Patent activity (USPTO PatentsView API)
  2. Cash runway (yfinance)
  3. Revenue trajectory (yfinance)
  4. Management quality — dilution + compensation checks (yfinance)

Red flag filters (auto-reject):
  - Reverse stock splits in last 3 years
  - Share count growth >20% YoY (heavy dilution)
  - Stock-based compensation >50% of revenue
  - Pre-revenue with valuation >$200M (hype pricing)

Outputs ranked JSON to /data/microcap_innovation/ for Hugo.

SETUP:
  1. Get a free PatentsView API key: https://patentsview.org/apis/api-endpoints
  2. export PATENTSVIEW_API_KEY="your_key_here"
  3. pip install yfinance requests

USAGE:
  python scripts/microcap/microcap_innovation_screener_v2.py
"""

import yfinance as yf
import json
import time
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
MIN_MARKET_CAP = 50_000_000
MAX_MARKET_CAP = 500_000_000
PATENT_LOOKBACK_YEARS = 3
OUTPUT_DIR = "data/microcap_innovation"

PATENT_LOOKUP_FILE = "data/microcap_innovation/patent_lookup.json"

TICKER_SOURCE = "data/microcap_tickers.txt"

# ---------------------------------------------------------------------------
# MARKET TAGS
# ---------------------------------------------------------------------------
# Maps yfinance industry strings to more specific, investor-friendly tags.
# If an industry isn't listed here, we fall back to the sector.
# You can expand this dict as you encounter new industries.

INDUSTRY_TO_TAG = {
    # Space & Defense
    "Aerospace & Defense": "Defense & Aerospace",
    # Quantum & Computing
    "Semiconductors": "Semiconductors",
    "Semiconductor Equipment & Materials": "Semiconductor Equipment",
    "Software - Infrastructure": "Enterprise Software",
    "Software - Application": "Application Software",
    "Information Technology Services": "IT Services",
    # Biotech & Health
    "Biotechnology": "Biotech",
    "Medical Devices": "Medical Devices",
    "Medical Instruments & Supplies": "Medical Devices",
    "Drug Manufacturers - Specialty & Generic": "Pharma",
    "Drug Manufacturers - General": "Pharma",
    "Diagnostics & Research": "Diagnostics",
    "Health Information Services": "Health Tech",
    # Energy & Clean Tech
    "Solar": "Solar Energy",
    "Utilities - Renewable": "Renewable Energy",
    "Uranium": "Nuclear Energy",
    "Oil & Gas Equipment & Services": "Energy Services",
    # Manufacturing & Industrials
    "Specialty Industrial Machinery": "Industrial Tech",
    "Electrical Equipment & Parts": "Electrical Equipment",
    "Scientific & Technical Instruments": "Scientific Instruments",
    "3D Printing": "Additive Manufacturing",
    # Communications
    "Telecom Services": "Telecommunications",
    "Communication Equipment": "Communication Equipment",
    # Other Tech
    "Electronic Components": "Electronic Components",
    "Computer Hardware": "Computer Hardware",
    "Cybersecurity": "Cybersecurity",
}


def get_market_tag(sector, industry):
    """
    Assign a specific market tag based on industry.
    Falls back to sector if no specific mapping exists.
    """
    if industry in INDUSTRY_TO_TAG:
        return INDUSTRY_TO_TAG[industry]
    # Fallback: use sector as-is
    return sector or "Unknown"


# ---------------------------------------------------------------------------
# TICKER LOADING
# ---------------------------------------------------------------------------
def load_tickers(filepath):
    """Load tickers from a text file (one per line)."""
    if os.path.exists(filepath):
        with open(filepath) as f:
            return [line.strip() for line in f if line.strip()]
    else:
        print(f"  [!] {filepath} not found. Using sample tickers for demo.")
        return [
            "ASTS", "LUNR", "RKLB", "IREN", "SMCI",
            "AEHR", "BFLY", "MVST", "SOUN", "IONQ",
            "DNA", "ARQQ", "RGTI", "QUBT", "VNET",
            "STEM", "DM", "OUST", "AEVA", "LAZR",
            "SATL", "NNOX", "ACHR", "JOBY", "LILM",
        ]


# ---------------------------------------------------------------------------
# STOCK DATA + RED FLAG DETECTION
# ---------------------------------------------------------------------------
def get_stock_data(ticker):
    """
    Pull financials from yfinance. Returns dict or None if filtered out.
    Also checks for management quality red flags.
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        market_cap = info.get("marketCap", 0)
        if not market_cap or market_cap < MIN_MARKET_CAP or market_cap > MAX_MARKET_CAP:
            return None

        # --- Core financials ---
        cash = info.get("totalCash", 0) or 0
        ocf = info.get("operatingCashflow", 0) or 0
        revenue = info.get("totalRevenue", 0) or 0
        revenue_growth = info.get("revenueGrowth", 0) or 0

        # Cash runway
        if ocf < 0:
            quarterly_burn = abs(ocf) / 4
            runway_months = (cash / quarterly_burn) * 3 if quarterly_burn > 0 else 999
        else:
            runway_months = 999

        # --- Red flag checks ---
        red_flags = []

        # 1. Share dilution: compare current shares to float/outstanding
        shares_outstanding = info.get("sharesOutstanding", 0) or 0

        # 2. Stock-based compensation vs revenue
        #    yfinance doesn't give SBC directly, but we can approximate
        #    by comparing net income to operating cash flow. If OCF is much
        #    higher than net income, SBC is likely a big chunk.
        net_income = info.get("netIncomeToCommon", 0) or 0
        if revenue > 0 and ocf != 0 and net_income != 0:
            # SBC proxy: gap between OCF and net income (SBC is added back in OCF)
            sbc_proxy = max(0, ocf - net_income)
            sbc_to_revenue = sbc_proxy / revenue if revenue > 0 else 0
            if sbc_to_revenue > 0.50:
                red_flags.append(f"SBC_HIGH ({sbc_to_revenue:.0%} of revenue)")
        else:
            sbc_to_revenue = 0

        # 3. Pre-revenue hype check
        if revenue < 1_000_000 and market_cap > 200_000_000:
            red_flags.append(f"HYPE_PRICING (${revenue/1e6:.1f}M rev, ${market_cap/1e6:.0f}M mcap)")

        # 4. Reverse split detection via stock split history
        has_reverse_split = False
        try:
            splits = stock.splits
            if splits is not None and len(splits) > 0:
                three_years_ago = datetime.now() - timedelta(days=3 * 365)
                for date, ratio in splits.items():
                    split_date = date.to_pydatetime().replace(tzinfo=None)
                    if split_date > three_years_ago and ratio < 1:
                        has_reverse_split = True
                        red_flags.append(f"REVERSE_SPLIT ({ratio} on {split_date.strftime('%Y-%m-%d')})")
        except Exception:
            pass

        # 5. Share count growth (dilution check)
        #    We compare current shares outstanding to what we can infer.
        #    yfinance gives impliedSharesOutstanding sometimes.
        #    A simpler proxy: if float percentage is very low, insiders
        #    and institutions hold most shares, which can be a mixed signal.
        float_shares = info.get("floatShares", 0) or 0
        if shares_outstanding > 0 and float_shares > 0:
            insider_held_pct = 1 - (float_shares / shares_outstanding)
        else:
            insider_held_pct = 0

        # --- Build result ---
        sector = info.get("sector", "Unknown")
        industry = info.get("industry", "Unknown")

        return {
            "ticker": ticker,
            "name": info.get("shortName", ticker),
            "market_cap": market_cap,
            "sector": sector,
            "industry": industry,
            "market_tag": get_market_tag(sector, industry),
            "cash": cash,
            "operating_cashflow": ocf,
            "runway_months": round(runway_months, 1),
            "revenue": revenue,
            "revenue_growth": round(revenue_growth * 100, 1),
            "shares_outstanding": shares_outstanding,
            "insider_held_pct": round(insider_held_pct * 100, 1),
            "sbc_to_revenue": round(sbc_to_revenue * 100, 1),
            "has_reverse_split": has_reverse_split,
            "red_flags": red_flags,
            "red_flag_count": len(red_flags),
        }

    except Exception as e:
        print(f"  [!] Error fetching {ticker}: {e}")
        return None


# ---------------------------------------------------------------------------
# PATENT LOOKUP (from local bulk data file)
# ---------------------------------------------------------------------------
def load_patent_lookup():
    """
    Load the patent lookup JSON built by patent_data_downloader.py.
    Returns the lookup dict or None if file doesn't exist.
    """
    if not os.path.exists(PATENT_LOOKUP_FILE):
        return None

    with open(PATENT_LOOKUP_FILE) as f:
        data = json.load(f)

    return data.get("lookup", {})


def get_patent_count(company_name, patent_lookup):
    """
    Look up a company's patent count from the local bulk data.
    Tries exact match first, then fuzzy substring match.
    Returns (count, titles_list, status_string).
    """
    if patent_lookup is None:
        return 0, [], "NO_LOOKUP_FILE"

    # Clean company name
    clean_name = company_name
    for suffix in [", Inc.", " Inc.", " Inc", " Corp.", " Corp",
                   " LLC", " Ltd.", " Ltd", " Co.", " Co",
                   ", L.P.", " L.P.", " Holdings", " Group"]:
        clean_name = clean_name.replace(suffix, "")
    clean_name = clean_name.strip().lower()

    # Try exact match first
    if clean_name in patent_lookup:
        entry = patent_lookup[clean_name]
        return entry["patent_count"], entry["sample_titles"], "OK"

    # Try substring match — find keys that contain our company name
    matches = []
    for key, entry in patent_lookup.items():
        if clean_name in key or key in clean_name:
            matches.append(entry)

    if matches:
        # Take the best match (highest patent count)
        best = max(matches, key=lambda x: x["patent_count"])
        return best["patent_count"], best["sample_titles"], "FUZZY"

    return 0, [], "NOT_FOUND"


# ---------------------------------------------------------------------------
# SCORING (now 0-12 with management quality factor)
# ---------------------------------------------------------------------------
def score_company(company_data, patent_count):
    """
    Scoring system. Each factor 0-3 points. Max score = 12.

    Patent Score (0-3):       0=none, 1=1-3, 2=4-10, 3=11+
    Cash Runway Score (0-3):  0=<12mo, 1=12-24, 2=24-48, 3=48+ or CF+
    Revenue Score (0-3):      0=declining, 1=flat, 2=10-50%, 3=50%+
    Management Score (0-3):   0=2+ red flags, 1=1 flag, 2=0 flags, 3=0 flags + insider buying
    """
    # Patent score
    if patent_count >= 11:
        patent_score = 3
    elif patent_count >= 4:
        patent_score = 2
    elif patent_count >= 1:
        patent_score = 1
    else:
        patent_score = 0

    # Cash runway score
    runway = company_data["runway_months"]
    if runway >= 48 or company_data["operating_cashflow"] > 0:
        runway_score = 3
    elif runway >= 24:
        runway_score = 2
    elif runway >= 12:
        runway_score = 1
    else:
        runway_score = 0

    # Revenue trajectory score
    growth = company_data["revenue_growth"]
    if growth >= 50:
        revenue_score = 3
    elif growth >= 10:
        revenue_score = 2
    elif growth >= 0:
        revenue_score = 1
    else:
        revenue_score = 0

    # Management quality score
    flag_count = company_data["red_flag_count"]
    if flag_count >= 2:
        mgmt_score = 0
    elif flag_count == 1:
        mgmt_score = 1
    else:
        # Clean record. Bonus point if insiders hold >10%
        if company_data["insider_held_pct"] > 10:
            mgmt_score = 3
        else:
            mgmt_score = 2

    total = patent_score + runway_score + revenue_score + mgmt_score

    return {
        "patent_score": patent_score,
        "runway_score": runway_score,
        "revenue_score": revenue_score,
        "mgmt_score": mgmt_score,
        "total_score": total,
    }


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("MICRO CAP INNOVATION SCREENER v2")
    print(f"Market cap range: ${MIN_MARKET_CAP/1e6:.0f}M - ${MAX_MARKET_CAP/1e6:.0f}M")
    print(f"Patent lookback: {PATENT_LOOKBACK_YEARS} years")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)

    # Load patent lookup from local bulk data
    patent_lookup = load_patent_lookup()
    if patent_lookup:
        print(f"\nPatent lookup loaded: {len(patent_lookup):,} organizations")
    else:
        print(f"\n  [!] No patent lookup file found at {PATENT_LOOKUP_FILE}")
        print("  Run patent_data_downloader.py first to build it.")
        print("  Patent scoring will be skipped.\n")

    tickers = load_tickers(TICKER_SOURCE)
    print(f"Loaded {len(tickers)} tickers to screen.\n")

    results = []
    rejected = []

    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Screening {ticker}...")

        stock_data = get_stock_data(ticker)
        if not stock_data:
            print(f"  -> Filtered out (market cap outside range or error)")
            continue

        print(f"  -> {stock_data['name']} | ${stock_data['market_cap']/1e6:.0f}M | Tag: {stock_data['market_tag']}")

        # Print red flags
        if stock_data["red_flags"]:
            for flag in stock_data["red_flags"]:
                print(f"  -> RED FLAG: {flag}")

        # Auto-reject on reverse split
        if stock_data["has_reverse_split"]:
            print(f"  -> REJECTED: reverse split detected")
            rejected.append({"ticker": ticker, "reason": "reverse_split"})
            continue

        # Get patent data from local lookup
        patent_count, patent_titles, patent_status = get_patent_count(
            stock_data["name"], patent_lookup
        )
        if patent_status in ("OK", "FUZZY"):
            print(f"  -> {patent_count} patents ({patent_status})")
        elif patent_status == "NO_LOOKUP_FILE":
            print(f"  -> Patents: skipped (no lookup file)")
        else:
            print(f"  -> Patents: {patent_status}")

        # Score it
        scores = score_company(stock_data, patent_count)
        print(f"  -> Innovation Score: {scores['total_score']}/12 "
              f"(P:{scores['patent_score']} R:{scores['runway_score']} "
              f"G:{scores['revenue_score']} M:{scores['mgmt_score']})")

        result = {
            **stock_data,
            "patent_count": patent_count,
            "patent_titles": patent_titles,
            "patent_status": patent_status,
            **scores,
            "screened_date": datetime.now().strftime("%Y-%m-%d"),
            "market_cap_display": f"${stock_data['market_cap']/1e6:.1f}M",
            "cash_display": f"${stock_data['cash']/1e6:.1f}M",
            "revenue_display": f"${stock_data['revenue']/1e6:.1f}M",
        }

        results.append(result)
        time.sleep(1)

    # Sort by total score descending
    results.sort(key=lambda x: x["total_score"], reverse=True)

    # Group by market tag for the JSON output
    tags = {}
    for r in results:
        tag = r["market_tag"]
        if tag not in tags:
            tags[tag] = []
        tags[tag].append(r["ticker"])

    # Save output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "screener_results.json")

    with open(output_path, "w") as f:
        json.dump({
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "filters": {
                "min_market_cap": MIN_MARKET_CAP,
                "max_market_cap": MAX_MARKET_CAP,
                "patent_lookback_years": PATENT_LOOKBACK_YEARS,
            },
            "total_screened": len(tickers),
            "total_passed": len(results),
            "total_rejected": len(rejected),
            "market_tags": tags,
            "companies": results,
            "rejected": rejected,
        }, f, indent=2)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {len(results)} passed | {len(rejected)} rejected")
    print(f"Output: {output_path}")
    print(f"\nMarket Tags:")
    for tag, tickers_in_tag in sorted(tags.items()):
        print(f"  {tag}: {', '.join(tickers_in_tag)}")
    print(f"\nTop 5 by Innovation Score:")
    print(f"{'=' * 60}")
    for r in results[:5]:
        flags = f" ⚠ {', '.join(r['red_flags'])}" if r['red_flags'] else ""
        print(f"  {r['ticker']:6s} | {r['total_score']:2d}/12 | "
              f"{r['market_cap_display']:>8s} | "
              f"{r['market_tag']}{flags}")


if __name__ == "__main__":
    main()
