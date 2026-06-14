"""
Transcript Fetcher for Microcap Qualitative Analysis
=====================================================
Fetches earnings call transcripts and SEC filing MD&A sections
for microcap companies. Two-tier sourcing:

  1. Financial Modeling Prep (FMP) API — actual earnings call transcripts
     with Q&A sections. Best signal for management quality assessment.
  2. SEC EDGAR — 10-K / 10-Q filings, extracting the MD&A section.
     Universal coverage, free, no API key needed.

All fetched content is cached locally to avoid re-fetching on subsequent runs.

SETUP:
  export FMP_API_KEY="your_key_here"        # financialmodelingprep.com
  pip install requests beautifulsoup4

USAGE:
  from transcript_fetcher import fetch_transcripts
  transcripts = fetch_transcripts("KULR")   # returns list of transcript dicts
"""

import json
import os
import re
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
CACHE_DIR = REPO_ROOT / "data" / "microcap_transcripts"
CIK_CACHE_FILE = CACHE_DIR / "_cik_lookup.json"

# SEC EDGAR requires a User-Agent with contact info
SEC_USER_AGENT = "mungbeans.io research@mungbeans.io"

# Rate limiting
FMP_DELAY = 0.5         # seconds between FMP API calls
EDGAR_DELAY = 0.15      # SEC asks for max 10 requests/sec
MAX_TRANSCRIPTS = 4     # most recent quarters to fetch per ticker

# FMP API base
FMP_BASE = "https://financialmodelingprep.com/api"


# ---------------------------------------------------------------------------
# CACHE LAYER
# ---------------------------------------------------------------------------
def _cache_path(ticker, source, period):
    """Build cache file path: data/microcap_transcripts/KULR/2026-Q1_fmp.json"""
    ticker_dir = CACHE_DIR / ticker.upper()
    ticker_dir.mkdir(parents=True, exist_ok=True)
    safe_period = period.replace("/", "-").replace(" ", "_")
    return ticker_dir / f"{safe_period}_{source}.json"


def _read_cache(ticker, source, period):
    """Read cached transcript. Returns dict or None."""
    path = _cache_path(ticker, source, period)
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            # Cache entries older than 90 days are stale (re-fetch)
            cached_at = data.get("cached_at", "")
            if cached_at:
                cached_date = datetime.fromisoformat(cached_at)
                if (datetime.now() - cached_date).days > 90:
                    return None
            return data
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _write_cache(ticker, source, period, data):
    """Write transcript data to cache."""
    data["cached_at"] = datetime.now().isoformat()
    path = _cache_path(ticker, source, period)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# FMP API — EARNINGS CALL TRANSCRIPTS
# ---------------------------------------------------------------------------
def _fmp_request(endpoint, params=None):
    """Make an FMP API request. Returns parsed JSON or None."""
    api_key = os.environ.get("FMP_API_KEY", "")
    if not api_key:
        return None

    if params is None:
        params = {}
    params["apikey"] = api_key

    url = f"{FMP_BASE}{endpoint}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "mungbeans.io/1.0"
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
        print(f"    [FMP] Request failed: {e}")
        return None


def fetch_fmp_transcripts(ticker, max_transcripts=MAX_TRANSCRIPTS):
    """
    Fetch earnings call transcripts from Financial Modeling Prep.
    Returns list of transcript dicts, most recent first.

    Each dict: {
        source: "fmp",
        ticker: str,
        period: "2026-Q1",
        date: "2026-01-28",
        content: str (full transcript text),
        year: int,
        quarter: int,
    }
    """
    if not os.environ.get("FMP_API_KEY"):
        return []

    transcripts = []

    # FMP v4 endpoint: list available transcripts
    available = _fmp_request(f"/v4/earning_call_transcript", {"symbol": ticker})
    if not available:
        # Try v3 endpoint with recent quarters
        available = []
        now = datetime.now()
        for q_offset in range(max_transcripts + 2):
            quarter = ((now.month - 1) // 3) + 1 - q_offset
            year = now.year
            while quarter <= 0:
                quarter += 4
                year -= 1
            available.append({"year": year, "quarter": quarter})

    if isinstance(available, list):
        # Sort by most recent first
        available = sorted(available,
                           key=lambda x: (x.get("year", 0), x.get("quarter", 0)),
                           reverse=True)

    fetched = 0
    for entry in available[:max_transcripts + 2]:
        if fetched >= max_transcripts:
            break

        year = entry.get("year")
        quarter = entry.get("quarter")
        if not year or not quarter:
            continue

        period = f"{year}-Q{quarter}"

        # Check cache first
        cached = _read_cache(ticker, "fmp", period)
        if cached:
            transcripts.append(cached)
            fetched += 1
            continue

        # Fetch from API
        time.sleep(FMP_DELAY)
        data = _fmp_request(f"/v3/earning_call_transcript/{ticker}",
                            {"year": year, "quarter": quarter})

        if data and isinstance(data, list) and len(data) > 0:
            transcript_text = data[0].get("content", "")
            if transcript_text and len(transcript_text) > 200:
                result = {
                    "source": "fmp",
                    "ticker": ticker.upper(),
                    "period": period,
                    "date": data[0].get("date", ""),
                    "content": transcript_text,
                    "year": year,
                    "quarter": quarter,
                    "char_count": len(transcript_text),
                }
                _write_cache(ticker, "fmp", period, result)
                transcripts.append(result)
                fetched += 1
                print(f"    [FMP] Fetched {period} ({len(transcript_text):,} chars)")

    return transcripts


# ---------------------------------------------------------------------------
# SEC EDGAR — 10-K / 10-Q MD&A EXTRACTION
# ---------------------------------------------------------------------------
def _edgar_request(url):
    """Make a request to SEC EDGAR with required User-Agent."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": SEC_USER_AGENT,
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            content_type = resp.headers.get("Content-Type", "")
            if "json" in content_type:
                return json.loads(raw)
            return raw.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"    [EDGAR] Request failed for {url}: {e}")
        return None


def _load_cik_lookup():
    """Load or build the ticker -> CIK mapping from SEC."""
    if CIK_CACHE_FILE.exists():
        try:
            with open(CIK_CACHE_FILE) as f:
                data = json.load(f)
            # Refresh if older than 30 days
            if data.get("fetched_at"):
                fetched = datetime.fromisoformat(data["fetched_at"])
                if (datetime.now() - fetched).days < 30:
                    return data.get("lookup", {})
        except (json.JSONDecodeError, ValueError):
            pass

    print("    [EDGAR] Fetching company tickers lookup...")
    url = "https://www.sec.gov/files/company_tickers.json"
    raw = _edgar_request(url)
    if not raw or not isinstance(raw, dict):
        return {}

    lookup = {}
    for entry in raw.values():
        ticker = entry.get("ticker", "").upper()
        cik = entry.get("cik_str")
        if ticker and cik:
            lookup[ticker] = str(cik)

    # Cache it
    CIK_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CIK_CACHE_FILE, "w") as f:
        json.dump({"fetched_at": datetime.now().isoformat(), "lookup": lookup}, f)

    print(f"    [EDGAR] Cached {len(lookup):,} ticker-CIK mappings")
    return lookup


def _get_cik(ticker):
    """Get CIK number for a ticker."""
    lookup = _load_cik_lookup()
    return lookup.get(ticker.upper())


def _extract_mda_section(html_text):
    """
    Extract the Management's Discussion and Analysis section from a
    10-K or 10-Q HTML filing.

    Modern iXBRL filings use anchor IDs and HTML entities. This function
    handles both legacy plain-text and modern iXBRL formats.

    Strategy:
      1. Try anchor-based extraction (modern filings use IDs like
         #ITEM7MANAGEMENTSDISCUSSION...)
      2. Fall back to regex on extracted text

    Returns extracted text or None.
    """
    if not html_text:
        return None

    # ------------------------------------------------------------------
    # STEP 1: Try anchor-based extraction for modern iXBRL filings
    # ------------------------------------------------------------------
    # Look for anchor IDs that mark the MD&A section
    anchor_patterns = [
        r'(?i)id\s*=\s*["\']?(ITEM7MANAGEMENT[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item7management[^"\'>\s]*)',
        r'(?i)name\s*=\s*["\']?(ITEM7MANAGEMENT[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item_7_management[^"\'>\s]*)',
        # 10-Q uses Item 2 for MD&A
        r'(?i)id\s*=\s*["\']?(ITEM2MANAGEMENT[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item2management[^"\'>\s]*)',
    ]

    end_anchor_patterns = [
        r'(?i)id\s*=\s*["\']?(ITEM7A[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(ITEM8[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item7a[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item8[^"\'>\s]*)',
        # 10-Q end markers
        r'(?i)id\s*=\s*["\']?(ITEM3[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(ITEM4[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item3[^"\'>\s]*)',
        r'(?i)id\s*=\s*["\']?(item4[^"\'>\s]*)',
    ]

    anchor_start = None
    for pattern in anchor_patterns:
        match = re.search(pattern, html_text)
        if match:
            anchor_start = match.start()
            break

    if anchor_start is not None:
        # Find the end anchor
        anchor_end = None
        search_from = anchor_start + 100
        for pattern in end_anchor_patterns:
            match = re.search(pattern, html_text[search_from:])
            if match:
                candidate = search_from + match.start()
                if anchor_end is None or candidate < anchor_end:
                    anchor_end = candidate

        if anchor_end is None:
            anchor_end = min(anchor_start + 500000, len(html_text))

        html_chunk = html_text[anchor_start:anchor_end]

        # Extract text from this HTML chunk
        if HAS_BS4:
            soup = BeautifulSoup(html_chunk, "html.parser")
            text = soup.get_text(separator="\n")
        else:
            text = re.sub(r'<[^>]+>', ' ', html_chunk)
            text = re.sub(r'&nbsp;', ' ', text)
            text = re.sub(r'&#\d+;', ' ', text)
            text = re.sub(r'&\w+;', ' ', text)

        # Strip leading anchor ID remnant (e.g. 'id="ITEM7MANAGE..."')
        text = re.sub(r'^id="[^"]*">\s*', '', text)

        # Clean up
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = text.strip()

        if len(text) >= 1000:
            # Cap at 60K chars
            if len(text) > 60000:
                text = text[:60000] + "\n\n[... truncated for analysis ...]"
            return text

    # ------------------------------------------------------------------
    # STEP 2: Fall back to text-based extraction
    # ------------------------------------------------------------------
    if HAS_BS4:
        soup = BeautifulSoup(html_text, "html.parser")
        text = soup.get_text(separator="\n")
    else:
        text = re.sub(r'<[^>]+>', ' ', html_text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&#\d+;', ' ', text)
        text = re.sub(r'&\w+;', ' ', text)
        text = re.sub(r'\s+', ' ', text)

    # Normalize quotes/apostrophes for matching
    # Unicode right single quote (U+2019), left single quote (U+2018)
    text_normalized = text.replace(''', "'").replace(''', "'")
    text_normalized = text_normalized.replace('"', '"').replace('"', '"')

    # MD&A header patterns (match on normalized text, use positions on original)
    mda_start_patterns = [
        r"(?i)item\s*7[\.\s]*[\-—–]?\s*management'?s?\s*discussion\s*and\s*analysis",
        r"(?i)management'?s?\s*discussion\s*and\s*analysis\s*of\s*(?:financial|results)",
        r"(?i)item\s*2[\.\s]*[\-—–]?\s*management'?s?\s*discussion\s*and\s*analysis",
    ]

    mda_end_patterns = [
        r"(?i)item\s*7\s*a[\.\s]*[\-—–]?\s*quantitative\s*and\s*qualitative",
        r"(?i)item\s*8[\.\s]*[\-—–]?\s*financial\s*statements",
        r"(?i)item\s*3[\.\s]*[\-—–]?\s*quantitative\s*and\s*qualitative",
        r"(?i)item\s*4[\.\s]*[\-—–]?\s*controls\s*and\s*procedures",
    ]

    # Find the LAST occurrence of MD&A header (skip table of contents entries)
    mda_start = None
    for pattern in mda_start_patterns:
        for match in re.finditer(pattern, text_normalized):
            # Skip if this looks like a table of contents entry (short context)
            context_after = text_normalized[match.end():match.end()+200]
            # TOC entries typically have page numbers right after
            if re.match(r'\s*\.{2,}\s*\d+', context_after):
                continue
            if re.match(r'\s+\d+\s*$', context_after[:30]):
                continue
            mda_start = match.start()

    if mda_start is None:
        return None

    mda_end = None
    search_text = text_normalized[mda_start + 100:]
    for pattern in mda_end_patterns:
        match = re.search(pattern, search_text)
        if match:
            candidate = mda_start + 100 + match.start()
            if mda_end is None or candidate < mda_end:
                mda_end = candidate

    if mda_end is None:
        mda_end = min(mda_start + 80000, len(text))

    mda_text = text[mda_start:mda_end].strip()

    # Clean up
    mda_text = re.sub(r'\n{3,}', '\n\n', mda_text)
    mda_text = re.sub(r' {2,}', ' ', mda_text)

    if len(mda_text) < 1000:
        return None

    if len(mda_text) > 60000:
        mda_text = mda_text[:60000] + "\n\n[... truncated for analysis ...]"

    return mda_text


def fetch_edgar_filings(ticker, filing_types=("10-K", "10-Q"),
                        max_filings=MAX_TRANSCRIPTS):
    """
    Fetch recent SEC filings and extract MD&A sections.
    Returns list of transcript-like dicts.

    Each dict: {
        source: "edgar",
        ticker: str,
        period: "10-K_2025",
        date: "2025-12-31",
        content: str (MD&A text),
        filing_type: "10-K" or "10-Q",
        filing_url: str,
        char_count: int,
    }
    """
    cik = _get_cik(ticker)
    if not cik:
        print(f"    [EDGAR] No CIK found for {ticker}")
        return []

    # Pad CIK to 10 digits
    cik_padded = cik.zfill(10)

    # Get submissions (filing list)
    time.sleep(EDGAR_DELAY)
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    subs = _edgar_request(submissions_url)
    if not subs or not isinstance(subs, dict):
        return []

    recent = subs.get("filings", {}).get("recent", {})
    if not recent:
        return []

    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    results = []
    # Only look at the most recent 10 filings of each type
    # to avoid fetching decades of old filings
    cutoff_date = (datetime.now() - timedelta(days=3*365)).strftime("%Y-%m-%d")

    for form_type in filing_types:
        type_count = 0
        for i, form in enumerate(forms):
            if len(results) >= max_filings:
                break
            if form != form_type:
                continue
            # Skip filings older than 3 years
            filing_date_str = dates[i] if i < len(dates) else ""
            if filing_date_str and filing_date_str < cutoff_date:
                continue
            type_count += 1
            if type_count > 4:  # Max 4 filings per type
                break

            filing_date = dates[i] if i < len(dates) else ""
            accession = accessions[i] if i < len(accessions) else ""
            primary_doc = primary_docs[i] if i < len(primary_docs) else ""

            if not accession or not primary_doc:
                continue

            # Build period identifier
            year = filing_date[:4] if filing_date else "unknown"
            period = f"{form_type}_{year}"
            if form_type == "10-Q":
                month = filing_date[5:7] if len(filing_date) >= 7 else ""
                q_map = {"01": "Q4", "02": "Q4", "03": "Q1", "04": "Q1",
                         "05": "Q2", "06": "Q2", "07": "Q2", "08": "Q3",
                         "09": "Q3", "10": "Q3", "11": "Q4", "12": "Q4"}
                quarter = q_map.get(month, "")
                period = f"{form_type}_{year}-{quarter}" if quarter else f"{form_type}_{year}-{month}"

            # Check cache
            cached = _read_cache(ticker, "edgar", period)
            if cached:
                results.append(cached)
                continue

            # Fetch the filing document
            accession_clean = accession.replace("-", "")
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{primary_doc}"

            time.sleep(EDGAR_DELAY)
            print(f"    [EDGAR] Fetching {form_type} ({filing_date})...")
            html = _edgar_request(doc_url)

            if not html or not isinstance(html, str):
                continue

            # Extract MD&A
            mda_text = _extract_mda_section(html)
            if not mda_text:
                print(f"    [EDGAR] Could not extract MD&A from {form_type} ({filing_date})")
                continue

            result = {
                "source": "edgar",
                "ticker": ticker.upper(),
                "period": period,
                "date": filing_date,
                "content": mda_text,
                "filing_type": form_type,
                "filing_url": doc_url,
                "char_count": len(mda_text),
            }
            _write_cache(ticker, "edgar", period, result)
            results.append(result)
            print(f"    [EDGAR] Extracted MD&A: {len(mda_text):,} chars")

    return results


# ---------------------------------------------------------------------------
# MAIN ORCHESTRATOR
# ---------------------------------------------------------------------------
def fetch_transcripts(ticker, prefer_calls=True, max_total=MAX_TRANSCRIPTS):
    """
    Fetch transcripts for a ticker. Primary: FMP earnings calls.
    Fallback: SEC EDGAR 10-K/10-Q MD&A sections.

    Args:
        ticker: Stock ticker symbol
        prefer_calls: If True, try FMP first. If enough found, skip EDGAR.
        max_total: Maximum total transcripts to return

    Returns:
        List of transcript dicts sorted by date (most recent first).
        Each dict has: source, ticker, period, date, content, char_count
    """
    ticker = ticker.upper()
    all_transcripts = []

    # Tier 1: FMP earnings call transcripts
    if prefer_calls and os.environ.get("FMP_API_KEY"):
        print(f"  [Transcripts] Fetching FMP earnings calls for {ticker}...")
        fmp_results = fetch_fmp_transcripts(ticker, max_transcripts=max_total)
        all_transcripts.extend(fmp_results)
        if fmp_results:
            print(f"  [Transcripts] Got {len(fmp_results)} FMP transcript(s)")

    # Tier 2: SEC EDGAR (if FMP didn't yield enough)
    remaining = max_total - len(all_transcripts)
    if remaining > 0:
        print(f"  [Transcripts] Fetching EDGAR filings for {ticker}...")
        edgar_results = fetch_edgar_filings(ticker, max_filings=remaining)
        all_transcripts.extend(edgar_results)
        if edgar_results:
            print(f"  [Transcripts] Got {len(edgar_results)} EDGAR filing(s)")

    if not all_transcripts:
        print(f"  [Transcripts] No transcripts found for {ticker}")
        return []

    # Sort by date, most recent first
    all_transcripts.sort(key=lambda x: x.get("date", ""), reverse=True)

    # Cap at max_total
    all_transcripts = all_transcripts[:max_total]

    total_chars = sum(t.get("char_count", 0) for t in all_transcripts)
    sources = set(t["source"] for t in all_transcripts)
    print(f"  [Transcripts] Total: {len(all_transcripts)} transcript(s), "
          f"{total_chars:,} chars from {', '.join(sources)}")

    return all_transcripts


def fetch_transcripts_batch(tickers, max_per_ticker=MAX_TRANSCRIPTS):
    """
    Fetch transcripts for multiple tickers with rate limiting.
    Returns dict: {ticker: [transcript_dicts]}
    """
    results = {}
    for i, ticker in enumerate(tickers):
        print(f"\n[{i+1}/{len(tickers)}] Fetching transcripts for {ticker}...")
        results[ticker] = fetch_transcripts(ticker, max_total=max_per_ticker)

        # Batch pause every 5 tickers
        if (i + 1) % 5 == 0 and i + 1 < len(tickers):
            print(f"  -- Batch pause (3s) --")
            time.sleep(3)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python transcript_fetcher.py TICKER [TICKER2 ...]")
        print("\nEnvironment variables:")
        print("  FMP_API_KEY  — Financial Modeling Prep API key (optional)")
        print(f"\nCache directory: {CACHE_DIR}")
        sys.exit(1)

    tickers = [t.upper() for t in sys.argv[1:]]

    for ticker in tickers:
        transcripts = fetch_transcripts(ticker)
        if transcripts:
            print(f"\n{'='*60}")
            print(f"RESULTS FOR {ticker}: {len(transcripts)} transcript(s)")
            print(f"{'='*60}")
            for t in transcripts:
                print(f"  {t['period']} | {t['source']} | {t['date']} | "
                      f"{t['char_count']:,} chars")
                # Show first 200 chars as preview
                preview = t["content"][:200].replace("\n", " ")
                print(f"  Preview: {preview}...")
                print()
        else:
            print(f"\nNo transcripts found for {ticker}")
