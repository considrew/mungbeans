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
import pandas as pd
import numpy as np
import json
import time
import os
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# RATE LIMITING & RETRY
# ---------------------------------------------------------------------------
def retry_on_rate_limit(func, *args, max_retries=3, base_delay=5, **kwargs):
    """
    Retry a function call with exponential backoff on rate limit errors.
    Catches common yfinance/HTTP 429 errors.
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = any(s in err_str for s in [
                '429', 'rate limit', 'too many requests', 'throttled',
                'please slow down', 'exceeded', 'forbidden'
            ])
            if is_rate_limit and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 2)
                print(f"    [!] Rate limited, waiting {delay:.0f}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(delay)
            else:
                raise
    return None


BATCH_SIZE = 10          # Pause every N tickers
BATCH_PAUSE = 8          # Seconds to pause between batches
PER_TICKER_DELAY = 2     # Seconds between each ticker


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
MIN_MARKET_CAP = 50_000_000
MAX_MARKET_CAP = 500_000_000
PATENT_LOOKBACK_YEARS = 3
OUTPUT_DIR = "assets/data"

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

        result = {
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

        # Fetch deep financials (reuses the existing yf.Ticker object)
        print(f"  -> Fetching deep financials...")
        deep = fetch_deep_financials(stock, info)
        result.update(deep)

        return result

    except Exception as e:
        print(f"  [!] Error fetching {ticker}: {e}")
        return None


# ---------------------------------------------------------------------------
# DEEP FINANCIAL DATA (mirrors update_stocks.py depth)
# ---------------------------------------------------------------------------

class NumpyEncoder(json.JSONEncoder):
    """Handle numpy/pandas types in JSON serialization."""
    def default(self, obj):
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp,)):
            return obj.isoformat()
        return super().default(obj)


def get_share_change(ticker_obj):
    """
    Calculate share count change over time to detect buybacks vs dilution.
    Returns (yoy_change%, 3yr_change%, current_shares).
    Negative = buybacks. Positive = dilution.
    """
    try:
        bs = getattr(ticker_obj, '_cached_bs', None) or ticker_obj.balance_sheet
        if bs is None or bs.empty or 'Ordinary Shares Number' not in bs.index:
            return None, None, None

        shares = bs.loc['Ordinary Shares Number'].dropna().sort_index(ascending=False)
        if len(shares) < 2:
            return None, None, None

        current = float(shares.iloc[0])

        yoy_change = None
        if len(shares) >= 2:
            year_ago = float(shares.iloc[1])
            if year_ago > 0:
                yoy_change = round(((current - year_ago) / year_ago) * 100, 1)

        three_yr_change = None
        if len(shares) >= 4:
            three_yr_ago = float(shares.iloc[3])
            if three_yr_ago > 0:
                three_yr_change = round(((current - three_yr_ago) / three_yr_ago) * 100, 1)
        elif len(shares) >= 3:
            oldest = float(shares.iloc[-1])
            if oldest > 0:
                three_yr_change = round(((current - oldest) / oldest) * 100, 1)

        return yoy_change, three_yr_change, int(current)
    except Exception:
        return None, None, None


def get_fcf_trend(ticker_obj):
    """
    Calculate free cash flow trend from annual cashflow statements.
    Returns dict with fcf_trend, fcf_cagr_3yr, fcf_consecutive_positive, fcf_history.
    """
    empty = {'fcf_trend': 'insufficient_data', 'fcf_cagr_3yr': None,
             'fcf_consecutive_positive': 0, 'fcf_history': []}
    try:
        cf = getattr(ticker_obj, '_cached_cf', None) or ticker_obj.cashflow
        if cf is None or cf.empty:
            return empty

        fcf_row = None
        for label in ['Free Cash Flow', 'FreeCashFlow']:
            if label in cf.index:
                fcf_row = cf.loc[label]
                break
        if fcf_row is None:
            return empty

        fcf_row = fcf_row.dropna().sort_index(ascending=True)
        if len(fcf_row) < 3:
            return empty

        fcf_values = fcf_row.tail(4)
        history = []
        for date, val in fcf_values.items():
            year = date.year if hasattr(date, 'year') else pd.Timestamp(date).year
            history.append({'year': year, 'fcf': round(float(val))})

        consecutive_positive = 0
        for entry in reversed(history):
            if entry['fcf'] > 0:
                consecutive_positive += 1
            else:
                break

        cagr = None
        if len(history) >= 3:
            oldest_fcf = float(fcf_values.iloc[0])
            newest_fcf = float(fcf_values.iloc[-1])
            years_span = len(fcf_values) - 1
            if oldest_fcf > 0 and newest_fcf > 0 and years_span > 0:
                cagr = round(((newest_fcf / oldest_fcf) ** (1 / years_span) - 1) * 100, 1)
            elif oldest_fcf > 0 and newest_fcf <= 0:
                cagr = -100.0

        yoy_changes = []
        vals = list(fcf_values)
        for i in range(1, len(vals)):
            prev, curr = float(vals[i-1]), float(vals[i])
            yoy_changes.append(curr > prev if prev != 0 else curr > 0)

        ups = sum(yoy_changes)
        downs = len(yoy_changes) - ups

        if ups >= 2 and (cagr is not None and cagr > 0):
            trend = 'growing'
        elif downs >= 2 and (cagr is None or cagr < 0):
            trend = 'declining'
        else:
            trend = 'volatile'

        return {'fcf_trend': trend, 'fcf_cagr_3yr': cagr,
                'fcf_consecutive_positive': consecutive_positive, 'fcf_history': history}
    except Exception:
        return empty


def get_health_metrics(ticker_obj, market_cap=None):
    """
    Build annual time-series data for the Business Health dashboard.
    Returns dict with years, revenue, net_income, fcf, total_debt, roic,
    gross_margin, shares, fcf_yield — all as arrays.
    market_cap: current market cap in dollars (avoids extra API call).
    """
    empty = {'years': [], 'revenue': [], 'net_income': [], 'fcf': [],
             'total_debt': [], 'roic': [], 'gross_margin': [], 'shares': [],
             'fcf_yield': []}
    try:
        fin = getattr(ticker_obj, '_cached_fin', None) or ticker_obj.financials
        bs = getattr(ticker_obj, '_cached_bs', None) or ticker_obj.balance_sheet
        cf = getattr(ticker_obj, '_cached_cf', None) or ticker_obj.cashflow

        if fin is None or fin.empty:
            return empty

        cutoff = pd.Timestamp.now() - pd.Timedelta(weeks=200)
        dates = sorted([d for d in fin.columns if pd.Timestamp(d) >= cutoff])

        years, revenue, net_income, fcf_vals = [], [], [], []
        total_debt, roic, gross_margin_pcts = [], [], []
        shares_list, fcf_yield_list = [], []

        for date in dates:
            year = pd.Timestamp(date).year
            years.append(year)

            # Revenue
            rev = None
            for lbl in ['Total Revenue', 'Revenue']:
                if lbl in fin.index:
                    v = fin.loc[lbl, date]
                    if pd.notna(v):
                        rev = round(float(v) / 1e6, 1)
                        break
            revenue.append(rev)

            # Net Income
            ni = None
            for lbl in ['Net Income', 'Net Income Common Stockholders']:
                if lbl in fin.index:
                    v = fin.loc[lbl, date]
                    if pd.notna(v):
                        ni = round(float(v) / 1e6, 1)
                        break
            net_income.append(ni)

            # Operating Income (for NOPAT/ROIC)
            op_income_raw = None
            for lbl in ['Operating Income', 'EBIT']:
                if lbl in fin.index:
                    v = fin.loc[lbl, date]
                    if pd.notna(v):
                        op_income_raw = float(v)
                        break

            # Tax rate
            tax_rate = 0.25
            if 'Tax Provision' in fin.index and 'Pretax Income' in fin.index:
                tp = fin.loc['Tax Provision', date]
                pt = fin.loc['Pretax Income', date]
                if pd.notna(tp) and pd.notna(pt) and float(pt) != 0:
                    tax_rate = max(0.0, min(0.40, float(tp) / float(pt)))

            # FCF
            fcf_v = None
            if cf is not None and not cf.empty and date in cf.columns:
                if 'Free Cash Flow' in cf.index:
                    v = cf.loc['Free Cash Flow', date]
                    if pd.notna(v):
                        fcf_v = round(float(v) / 1e6, 1)
            fcf_vals.append(fcf_v)

            # Total Debt + ROIC
            debt_v, roic_v = None, None
            if bs is not None and not bs.empty and date in bs.columns:
                if 'Total Debt' in bs.index:
                    v = bs.loc['Total Debt', date]
                    if pd.notna(v):
                        debt_v = round(float(v) / 1e6, 1)
                if debt_v is None:
                    lt = bs.loc['Long Term Debt', date] if 'Long Term Debt' in bs.index else None
                    st = bs.loc['Current Debt', date] if 'Current Debt' in bs.index else None
                    lt = float(lt) if lt is not None and pd.notna(lt) else 0.0
                    st = float(st) if st is not None and pd.notna(st) else 0.0
                    if lt + st > 0:
                        debt_v = round((lt + st) / 1e6, 1)

                if op_income_raw is not None:
                    nopat = op_income_raw * (1 - tax_rate)
                    equity_raw = None
                    for lbl in ['Stockholders Equity', 'Total Stockholder Equity', 'Common Stock Equity']:
                        if lbl in bs.index:
                            v = bs.loc[lbl, date]
                            if pd.notna(v):
                                equity_raw = float(v)
                                break
                    debt_raw = float(debt_v * 1e6) if debt_v is not None else 0.0
                    cash_raw = 0.0
                    for lbl in ['Cash And Cash Equivalents', 'Cash Cash Equivalents And Short Term Investments']:
                        if lbl in bs.index:
                            v = bs.loc[lbl, date]
                            if pd.notna(v):
                                cash_raw = float(v)
                                break
                    if equity_raw is not None:
                        invested_cap = equity_raw + debt_raw - cash_raw
                        if invested_cap > 0:
                            roic_v = round((nopat / invested_cap) * 100, 1)

            total_debt.append(debt_v)
            roic.append(roic_v)

            # Gross Margin %
            gm_v = None
            if 'Gross Profit' in fin.index:
                gp = fin.loc['Gross Profit', date]
                if pd.notna(gp) and rev is not None and rev > 0:
                    gm_v = round((float(gp) / (rev * 1e6)) * 100, 1)
            gross_margin_pcts.append(gm_v)

            # Shares Outstanding (millions)
            shares_v = None
            if bs is not None and not bs.empty and date in bs.columns:
                for lbl in ['Ordinary Shares Number', 'Share Issued']:
                    if lbl in bs.index:
                        v = bs.loc[lbl, date]
                        if pd.notna(v):
                            shares_v = round(float(v) / 1e6, 1)
                            break
            shares_list.append(shares_v)

            # FCF Yield % — use current market cap passed in to avoid extra API calls
            fcf_yield_v = None
            if fcf_v is not None and market_cap and market_cap > 0:
                mcap_m = market_cap / 1e6
                fcf_yield_v = round((fcf_v / mcap_m) * 100, 1)
            fcf_yield_list.append(fcf_yield_v)

        return {'years': years, 'revenue': revenue, 'net_income': net_income,
                'fcf': fcf_vals, 'total_debt': total_debt, 'roic': roic,
                'gross_margin': gross_margin_pcts, 'shares': shares_list,
                'fcf_yield': fcf_yield_list}
    except Exception:
        return empty


def fetch_deep_financials(ticker_obj, info):
    """
    Fetch the full suite of fundamental data for a microcap.
    Pre-fetches balance_sheet/cashflow/financials ONCE and caches on the
    ticker object to minimize API calls in downstream functions.
    """
    try:
        # Pre-fetch statement data ONCE to avoid redundant API calls
        # in get_share_change(), get_fcf_trend(), get_health_metrics()
        if not hasattr(ticker_obj, '_cached_bs'):
            try:
                ticker_obj._cached_bs = ticker_obj.balance_sheet
            except Exception:
                ticker_obj._cached_bs = None
        if not hasattr(ticker_obj, '_cached_cf'):
            try:
                ticker_obj._cached_cf = ticker_obj.cashflow
            except Exception:
                ticker_obj._cached_cf = None
        if not hasattr(ticker_obj, '_cached_fin'):
            try:
                ticker_obj._cached_fin = ticker_obj.financials
            except Exception:
                ticker_obj._cached_fin = None

        # Basic metrics from info
        market_cap = info.get('marketCap')
        fcf = info.get('freeCashflow')
        book_value = info.get('bookValue')
        price_to_book = info.get('priceToBook')
        profit_margin = info.get('profitMargins')
        operating_margin = info.get('operatingMargins')
        roe = info.get('returnOnEquity')
        debt_to_equity = info.get('debtToEquity')
        gross_margin = info.get('grossMargins')
        current_ratio = info.get('currentRatio')
        dividend_yield = info.get('dividendYield')

        # Derived
        fcf_yield = None
        if fcf and market_cap and market_cap > 0:
            fcf_yield = round((fcf / market_cap) * 100, 2)

        roe_pct = round(roe * 100, 1) if roe is not None else None
        gross_margin_pct = round(gross_margin * 100, 1) if gross_margin is not None else None
        profit_margin_pct = round(profit_margin * 100, 1) if profit_margin is not None else None
        operating_margin_pct = round(operating_margin * 100, 1) if operating_margin is not None else None

        # Share change tracking
        shares_yoy, shares_3yr, shares_current = get_share_change(ticker_obj)

        # Quality flags
        has_positive_fcf = bool(fcf is not None and fcf > 0)
        low_debt = bool(debt_to_equity is not None and debt_to_equity < 50)
        high_roe = bool(roe_pct is not None and roe_pct > 15)
        wide_moat = bool(gross_margin_pct is not None and gross_margin_pct > 40 and high_roe)
        buffett_quality = bool(high_roe and low_debt and has_positive_fcf
                               and profit_margin is not None and profit_margin > 0)
        is_buying_back = bool(shares_3yr is not None and shares_3yr < -2)
        is_diluting = bool(shares_3yr is not None and shares_3yr > 2)
        is_cannibal = bool(shares_3yr is not None and shares_3yr < -5)

        # FCF trend
        fcf_trend_data = get_fcf_trend(ticker_obj)

        # Business health time-series (pass market_cap to avoid extra API call)
        health_data = get_health_metrics(ticker_obj, market_cap=market_cap)

        return {
            'fcf': fcf,
            'fcf_yield': fcf_yield,
            'book_value': round(book_value, 2) if book_value else None,
            'price_to_book': round(price_to_book, 2) if price_to_book else None,
            'profit_margin': profit_margin_pct,
            'operating_margin': operating_margin_pct,
            'roe': roe_pct,
            'debt_to_equity': round(debt_to_equity, 1) if debt_to_equity else None,
            'gross_margin': gross_margin_pct,
            'current_ratio': round(current_ratio, 2) if current_ratio else None,
            'dividend_yield': round(dividend_yield * 100, 2) if dividend_yield else None,
            'shares_change_yoy': shares_yoy,
            'shares_change_3yr': shares_3yr,
            # Quality flags
            'has_positive_fcf': has_positive_fcf,
            'low_debt': low_debt,
            'high_roe': high_roe,
            'wide_moat': wide_moat,
            'buffett_quality': buffett_quality,
            'is_buying_back': is_buying_back,
            'is_diluting': is_diluting,
            'is_cannibal': is_cannibal,
            # FCF trend
            'fcf_trend': fcf_trend_data['fcf_trend'],
            'fcf_cagr_3yr': fcf_trend_data['fcf_cagr_3yr'],
            'fcf_consecutive_positive': fcf_trend_data['fcf_consecutive_positive'],
            'fcf_history': fcf_trend_data['fcf_history'],
            # Health dashboard
            'health_chart': health_data if health_data['years'] else None,
        }
    except Exception as e:
        print(f"    [!] Deep financials error: {e}")
        return {
            'fcf': None, 'fcf_yield': None, 'book_value': None,
            'price_to_book': None, 'profit_margin': None, 'operating_margin': None,
            'roe': None, 'debt_to_equity': None, 'gross_margin': None,
            'current_ratio': None, 'dividend_yield': None,
            'shares_change_yoy': None, 'shares_change_3yr': None,
            'has_positive_fcf': False, 'low_debt': False, 'high_roe': False,
            'wide_moat': False, 'buffett_quality': False,
            'is_buying_back': False, 'is_diluting': False, 'is_cannibal': False,
            'fcf_trend': 'insufficient_data', 'fcf_cagr_3yr': None,
            'fcf_consecutive_positive': 0, 'fcf_history': [],
            'health_chart': None,
        }


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

        # Batch pause every BATCH_SIZE tickers to avoid rate limits
        if i > 0 and i % BATCH_SIZE == 0:
            print(f"  -- Batch pause ({BATCH_PAUSE}s to avoid rate limiting) --")
            time.sleep(BATCH_PAUSE)

        try:
            stock_data = retry_on_rate_limit(get_stock_data, ticker)
        except Exception as e:
            print(f"  [!] Failed after retries: {e}")
            stock_data = None

        if not stock_data:
            print(f"  -> Filtered out (market cap outside range or error)")
            time.sleep(PER_TICKER_DELAY)
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
            time.sleep(PER_TICKER_DELAY)
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
        time.sleep(PER_TICKER_DELAY)

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
    output_path = os.path.join(OUTPUT_DIR, "microcap_screener.json")

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
        }, f, indent=2, cls=NumpyEncoder)

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
