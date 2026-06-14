"""
Management Analyzer — Qualitative AI Analysis for Microcap Screener
====================================================================
Uses Google Gemini Flash API to analyze earnings call transcripts and
SEC filings, scoring management across five dimensions:

  1. Visionary Conviction (0-5)  — Does the CEO have a clear, ambitious thesis?
  2. Capital Allocation (0-5)    — R&D discipline, dilution, insider ownership
  3. TAM Analysis (0-5)         — Stated vs realistic vs missed opportunities
  4. Execution Evidence (0-5)   — Are they hitting milestones? Improving metrics?
  5. Red Flag Inverse (0-5)     — 5 = clean, 0 = multiple red flags

Total Conviction Score: 0-25

SETUP:
  export GOOGLE_AI_API_KEY="your_key_here"
  pip install google-genai

USAGE:
  from management_analyzer import analyze_company, batch_analyze
  result = analyze_company("KULR", transcripts, company_context)
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

try:
    from google import genai
    from google.genai import types as genai_types
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
OUTPUT_DIR = REPO_ROOT / "assets" / "data"
PROFILES_FILE = OUTPUT_DIR / "management_profiles.json"
SCORES_FILE = OUTPUT_DIR / "management_scores.json"

# Gemini models — Flash for cost-efficient batch runs (free tier),
# Pro for deeper analysis on high-scoring companies
MODEL_FAST = "gemini-2.0-flash"
MODEL_DEEP = "gemini-2.5-pro-preview-06-05"

# Rate limiting
API_DELAY = 1.0          # seconds between API calls
MAX_INPUT_CHARS = 80000  # max transcript chars to send (cost control)


# ---------------------------------------------------------------------------
# ANALYSIS PROMPT
# ---------------------------------------------------------------------------
ANALYSIS_SYSTEM_PROMPT = """You are an expert microcap stock analyst specializing in identifying future 10x+ companies at the sub-$500M market cap stage. You've studied how companies like Rocket Lab (RKLB), AST SpaceMobile (ASTS), and Tesla (TSLA) looked and sounded when they were small.

Your job is to analyze management quality from earnings call transcripts and SEC filings. You are looking for visionaries — founders and CEOs who have a specific, testable thesis about where the world is going and are building something massive. You are equally vigilant about detecting promotional hype, excuse-making, and empire-building disguised as innovation.

You must be brutally honest. Most microcaps fail. Your analysis helps investors separate the 1-in-50 future category leader from the 49 that will stagnate or go to zero."""

ANALYSIS_USER_PROMPT = """Analyze the following management communications for {ticker} ({name}).

COMPANY CONTEXT:
- Sector: {sector}
- Market Cap: ${market_cap_m:.0f}M
- Revenue: ${revenue_m:.1f}M
- Revenue Growth: {revenue_growth}%
- Cash: ${cash_m:.1f}M
- Operating Cash Flow: ${ocf_m:.1f}M
- Cash Runway: {runway} months
- Insider Held: {insider_pct}%
- Red Flags from Quant Screen: {quant_red_flags}

TRANSCRIPTS/FILINGS ({n_transcripts} document(s), {total_chars:,} chars):

{transcript_text}

---

Score this company's management across these five dimensions. For each, provide a score (0-5) and a concise explanation (2-3 sentences max).

SCORING RUBRICS:

1. VISIONARY CONVICTION (0-5):
   0 = No discernible strategy, reactive management
   1 = Generic growth talk, no specific thesis
   2 = Has a market thesis but it's vague or derivative
   3 = Clear thesis with specific market opportunity identified
   4 = Compelling, specific thesis with evidence of deep market understanding
   5 = Exceptional — a founder-driven thesis that reframes how to think about the market (early Musk, early Beck at RKLB)

   Key signals: Specificity of market claims, consistency across quarters, original thinking vs. buzzword repetition, whether they can articulate WHY they win (not just WHAT they do)

2. CAPITAL ALLOCATION (0-5):
   0 = Reckless spending, heavy dilution, empire building
   1 = Poor capital discipline, SBC heavy, unfocused spending
   2 = Average — some discipline but questionable priorities
   3 = Good discipline — R&D focused, controlled burn rate
   4 = Strong — strategic spending, meaningful insider ownership, shareholder alignment
   5 = Exceptional — every dollar deployed strategically, insiders buying on open market, clean cap table

   Key signals: R&D as % of revenue trend, dilution trajectory, cash burn acceleration/deceleration, acquisition rationale, insider buying patterns

3. TAM ANALYSIS (0-5):
   0 = No TAM discussion or wildly unrealistic claims
   1 = Generic large TAM claim with no supporting logic
   2 = Stated TAM with some rationale but unclear path to capture
   3 = Well-articulated TAM with realistic penetration assumptions
   4 = Sophisticated TAM analysis showing multiple vectors of growth
   5 = TAM is both large AND they've identified underappreciated adjacent markets

   For this dimension, provide THREE assessments:
   - Stated TAM: What does management claim?
   - Realistic TAM: Based on current products/capabilities, what can they actually serve?
   - Missed TAM: What adjacent markets are they NOT targeting that they could/should be?

4. EXECUTION EVIDENCE (0-5):
   0 = Consistent misses, no tangible progress
   1 = Mostly talk, few concrete milestones
   2 = Some execution but inconsistent
   3 = Solid execution — meeting or beating stated milestones
   4 = Strong execution — ahead of plan with expanding customer traction
   5 = Exceptional — rapid milestone achievement with compounding advantages

   Key signals: Do they reference specific customer wins, contract values, milestone dates? Are they ahead or behind their own stated timelines? Revenue growth trajectory vs. promises.

5. RED FLAGS (0-5, INVERSE — higher is better):
   5 = Clean — no concerning signals
   4 = Minor concerns (1 flag)
   3 = Moderate concerns (2 flags)
   2 = Significant concerns (3+ flags)
   1 = Major concerns — pattern of problematic behavior
   0 = Critical — active deception or severe governance issues

   Red flags to check: buzzword density without substance, blame externalization ("macro headwinds" every quarter), guidance manipulation (sandbagging then "beating"), executive turnover (especially CFO), related-party transactions, promotional language disproportionate to results, avoiding analyst questions, excessive "one-time charges", reverse mergers or unusual corporate structure

Respond in EXACTLY this JSON format (no markdown code fences, just raw JSON):
{{
  "ticker": "{ticker}",
  "vision_score": <0-5>,
  "vision_summary": "<2-3 sentences>",
  "capital_score": <0-5>,
  "capital_summary": "<2-3 sentences>",
  "tam_score": <0-5>,
  "tam_stated": "<1-2 sentences on what management claims>",
  "tam_realistic": "<1-2 sentences on what they can actually serve>",
  "tam_missed": ["<adjacent market 1>", "<adjacent market 2>"],
  "tam_gap_assessment": "<1 sentence: how honest/realistic is their TAM framing?>",
  "execution_score": <0-5>,
  "execution_summary": "<2-3 sentences>",
  "red_flag_score": <0-5>,
  "red_flags_found": ["<flag 1>", "<flag 2>"],
  "green_flags_found": ["<flag 1>", "<flag 2>"],
  "conviction_score": <0-25 total>,
  "management_style": "<one of: visionary_founder, steady_operator, promotional_hype, defensive_manager, turnaround_artist, empire_builder>",
  "one_line_verdict": "<single sentence: would you back this management team with your own money?>",
  "key_quotes": [
    {{"quote": "<exact or near-exact quote from transcript>", "context": "<why this matters>", "sentiment": "positive"}},
    {{"quote": "<exact or near-exact quote>", "context": "<why this matters>", "sentiment": "concerning"}}
  ],
  "comparables": "<which known company does this remind you of at the same stage, and why? 1-2 sentences>"
}}"""


# ---------------------------------------------------------------------------
# GEMINI API INTERFACE
# ---------------------------------------------------------------------------
_genai_client = None

def _get_client():
    """Lazily initialize the Gemini client."""
    global _genai_client
    if _genai_client is None:
        api_key = os.environ.get("GOOGLE_AI_API_KEY", "")
        if not api_key:
            return None
        _genai_client = genai.Client(api_key=api_key)
    return _genai_client

def _call_gemini(system_prompt, user_prompt, model=MODEL_FAST):
    """
    Call Gemini API and return the response text.
    Returns None on failure.
    """
    if not HAS_GENAI:
        print("    [!] google-genai package not installed. pip install google-genai")
        return None

    client = _get_client()
    if not client:
        print("    [!] GOOGLE_AI_API_KEY not set")
        return None

    config = genai_types.GenerateContentConfig(
        system_instruction=system_prompt,
        max_output_tokens=2000,
        temperature=0.3,
    )

    try:
        response = client.models.generate_content(
            model=model,
            contents=user_prompt,
            config=config,
        )
        return response.text
    except Exception as e:
        error_str = str(e)
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
            print("    [!] Rate limited — waiting 60s...")
            time.sleep(60)
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=user_prompt,
                    config=config,
                )
                return response.text
            except Exception as e2:
                print(f"    [!] Retry failed: {e2}")
                return None
        else:
            print(f"    [!] Gemini API error: {e}")
            return None


def _parse_analysis(response_text):
    """
    Parse Gemini's JSON response into a structured dict.
    Handles common JSON formatting issues.
    """
    if not response_text:
        return None

    # Strip markdown code fences if present
    text = response_text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    # Remove 'json' language tag if present
    if text.startswith("json"):
        text = text[4:].strip()

    try:
        result = json.loads(text)

        # Validate required fields
        required = ["vision_score", "capital_score", "tam_score",
                     "execution_score", "red_flag_score", "conviction_score"]
        for field in required:
            if field not in result:
                print(f"    [!] Missing required field: {field}")
                return None

        # Clamp scores to valid ranges
        for field in required:
            if field == "conviction_score":
                result[field] = max(0, min(25, int(result[field])))
            else:
                result[field] = max(0, min(5, int(result[field])))

        # Verify conviction_score is the sum
        expected_total = (result["vision_score"] + result["capital_score"] +
                          result["tam_score"] + result["execution_score"] +
                          result["red_flag_score"])
        if result["conviction_score"] != expected_total:
            result["conviction_score"] = expected_total

        return result

    except json.JSONDecodeError as e:
        print(f"    [!] Failed to parse Claude response as JSON: {e}")
        print(f"    Response preview: {text[:200]}...")
        return None


# ---------------------------------------------------------------------------
# ANALYSIS ORCHESTRATOR
# ---------------------------------------------------------------------------
def analyze_company(ticker, transcripts, company_context, deep=False):
    """
    Analyze a single company's management quality.

    Args:
        ticker: Stock ticker
        transcripts: List of transcript dicts from transcript_fetcher
        company_context: Dict with company financials from screener
        deep: If True, use Sonnet for deeper analysis (more expensive)

    Returns:
        Analysis dict with scores and commentary, or None on failure.
    """
    if not transcripts:
        print(f"    [!] No transcripts for {ticker}, skipping analysis")
        return None

    # Combine transcripts, most recent first, respecting char limit
    combined_text = ""
    transcripts_used = 0
    for t in transcripts:
        header = f"\n{'='*40}\n{t['source'].upper()} — {t['period']} ({t['date']})\n{'='*40}\n"
        content = t["content"]

        # Check if adding this would exceed limit
        if len(combined_text) + len(header) + len(content) > MAX_INPUT_CHARS:
            # Truncate this transcript to fit
            remaining = MAX_INPUT_CHARS - len(combined_text) - len(header) - 100
            if remaining > 2000:
                content = content[:remaining] + "\n\n[... truncated ...]"
            else:
                break

        combined_text += header + content
        transcripts_used += 1

    if not combined_text or len(combined_text) < 500:
        print(f"    [!] Insufficient transcript content for {ticker}")
        return None

    # Build the prompt
    ctx = company_context or {}
    market_cap = ctx.get("market_cap", 0) or 0
    revenue = ctx.get("revenue", 0) or 0
    cash = ctx.get("cash", 0) or 0
    ocf = ctx.get("operating_cashflow", 0) or 0
    red_flags = ctx.get("red_flags", [])

    prompt = ANALYSIS_USER_PROMPT.format(
        ticker=ticker,
        name=ctx.get("name", ticker),
        sector=ctx.get("sector", "Unknown"),
        market_cap_m=market_cap / 1e6,
        revenue_m=revenue / 1e6,
        revenue_growth=ctx.get("revenue_growth", 0),
        cash_m=cash / 1e6,
        ocf_m=ocf / 1e6,
        runway=ctx.get("runway_months", "N/A"),
        insider_pct=ctx.get("insider_held_pct", 0),
        quant_red_flags=", ".join(red_flags) if red_flags else "None",
        n_transcripts=transcripts_used,
        total_chars=len(combined_text),
        transcript_text=combined_text,
    )

    # Choose model based on depth flag
    model = MODEL_DEEP if deep else MODEL_FAST

    print(f"    [AI] Analyzing {ticker} with {model} "
          f"({transcripts_used} transcript(s), {len(combined_text):,} chars)...")

    time.sleep(API_DELAY)
    response = _call_gemini(ANALYSIS_SYSTEM_PROMPT, prompt, model=model)
    result = _parse_analysis(response)

    if result:
        # Add metadata
        result["ticker"] = ticker
        result["name"] = ctx.get("name", ticker)
        result["sector"] = ctx.get("sector", "Unknown")
        result["market_cap"] = market_cap
        result["transcripts_analyzed"] = transcripts_used
        result["transcript_sources"] = list(set(t["source"] for t in transcripts[:transcripts_used]))
        result["transcript_periods"] = [t["period"] for t in transcripts[:transcripts_used]]
        result["model_used"] = model
        result["analyzed_at"] = datetime.now().isoformat()
        print(f"    [AI] Conviction Score: {result['conviction_score']}/25 "
              f"(V:{result['vision_score']} C:{result['capital_score']} "
              f"T:{result['tam_score']} E:{result['execution_score']} "
              f"R:{result['red_flag_score']})")
    else:
        print(f"    [AI] Analysis failed for {ticker}")

    return result


def batch_analyze(tickers_with_context, transcripts_by_ticker,
                  deep_threshold=18):
    """
    Analyze a batch of companies.

    Args:
        tickers_with_context: Dict {ticker: company_context_dict}
        transcripts_by_ticker: Dict {ticker: [transcript_dicts]}
        deep_threshold: Conviction score threshold for re-analysis with Sonnet

    Returns:
        Dict of analysis results: {ticker: analysis_dict}
    """
    results = {}
    tickers = list(tickers_with_context.keys())

    print(f"\n{'='*60}")
    print(f"MANAGEMENT ANALYSIS — {len(tickers)} companies")
    print(f"{'='*60}\n")

    # Pass 1: Fast analysis with Haiku
    for i, ticker in enumerate(tickers):
        transcripts = transcripts_by_ticker.get(ticker, [])
        context = tickers_with_context[ticker]

        print(f"\n[{i+1}/{len(tickers)}] Analyzing {ticker} ({context.get('name', '')})...")

        if not transcripts:
            print(f"  -> Skipped (no transcripts)")
            continue

        result = analyze_company(ticker, transcripts, context, deep=False)
        if result:
            results[ticker] = result

        # Rate limiting
        if (i + 1) % 5 == 0 and i + 1 < len(tickers):
            print(f"\n  -- Batch pause (3s) --")
            time.sleep(3)

    # Pass 2: Deep re-analysis for high scorers (optional)
    high_scorers = [t for t, r in results.items()
                    if r.get("conviction_score", 0) >= deep_threshold]

    if high_scorers:
        print(f"\n{'='*60}")
        print(f"DEEP ANALYSIS — {len(high_scorers)} high-conviction companies")
        print(f"(Re-analyzing with {MODEL_DEEP})")
        print(f"{'='*60}\n")

        for ticker in high_scorers:
            transcripts = transcripts_by_ticker.get(ticker, [])
            context = tickers_with_context[ticker]

            print(f"  Deep-diving {ticker}...")
            deep_result = analyze_company(ticker, transcripts, context, deep=True)
            if deep_result:
                deep_result["deep_analysis"] = True
                results[ticker] = deep_result

            time.sleep(API_DELAY)

    return results


# ---------------------------------------------------------------------------
# PERSISTENCE
# ---------------------------------------------------------------------------
def save_results(results):
    """
    Save analysis results to two files:
    1. management_profiles.json — full analysis for profile pages
    2. management_scores.json — slim scores for screener table integration
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Full profiles
    profiles = {
        "generated": datetime.now().strftime("%Y-%m-%d"),
        "generated_iso": datetime.now().isoformat(),
        "n_analyzed": len(results),
        "model_primary": MODEL_FAST,
        "model_deep": MODEL_DEEP,
        "profiles": results,
    }
    with open(PROFILES_FILE, "w") as f:
        json.dump(profiles, f, indent=2)
    print(f"\nProfiles saved: {PROFILES_FILE}")

    # Slim scores for screener integration
    scores = {}
    for ticker, r in results.items():
        scores[ticker] = {
            "conviction_score": r.get("conviction_score", 0),
            "vision_score": r.get("vision_score", 0),
            "capital_score": r.get("capital_score", 0),
            "tam_score": r.get("tam_score", 0),
            "execution_score": r.get("execution_score", 0),
            "red_flag_score": r.get("red_flag_score", 0),
            "management_style": r.get("management_style", "unknown"),
            "one_line_verdict": r.get("one_line_verdict", ""),
            "analyzed_at": r.get("analyzed_at", ""),
        }

    scores_output = {
        "generated": datetime.now().strftime("%Y-%m-%d"),
        "n_scored": len(scores),
        "scores": scores,
    }
    with open(SCORES_FILE, "w") as f:
        json.dump(scores_output, f, indent=2)
    print(f"Scores saved: {SCORES_FILE}")


def load_existing_profiles():
    """Load previously saved profiles for incremental updates."""
    if PROFILES_FILE.exists():
        try:
            with open(PROFILES_FILE) as f:
                data = json.load(f)
            return data.get("profiles", {})
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


def load_existing_scores():
    """Load previously saved scores."""
    if SCORES_FILE.exists():
        try:
            with open(SCORES_FILE) as f:
                data = json.load(f)
            return data.get("scores", {})
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


# ---------------------------------------------------------------------------
# FULL PIPELINE
# ---------------------------------------------------------------------------
def run_full_pipeline(tickers, screener_data=None, force_reanalyze=False,
                      deep_threshold=18):
    """
    Run the complete qualitative analysis pipeline:
    1. Fetch transcripts for all tickers
    2. Analyze each company with Claude
    3. Re-analyze high scorers with deeper model
    4. Save results

    Args:
        tickers: List of ticker symbols
        screener_data: Dict from microcap_screener.json {companies: [...]}
        force_reanalyze: If True, re-analyze even if cached results exist
        deep_threshold: Score threshold for deep re-analysis (0-25)

    Returns:
        Dict of analysis results
    """
    from transcript_fetcher import fetch_transcripts

    # Build context lookup from screener data
    context_lookup = {}
    if screener_data:
        for company in screener_data.get("companies", []):
            context_lookup[company["ticker"]] = company

    # Load existing results for incremental updates
    existing = {} if force_reanalyze else load_existing_profiles()

    # Filter to tickers that need analysis
    tickers_to_analyze = []
    for ticker in tickers:
        if ticker in existing and not force_reanalyze:
            # Check if analysis is stale (>30 days old)
            analyzed_at = existing[ticker].get("analyzed_at", "")
            if analyzed_at:
                try:
                    analyzed_date = datetime.fromisoformat(analyzed_at)
                    if (datetime.now() - analyzed_date).days < 30:
                        print(f"  [Skip] {ticker} — analyzed {analyzed_at[:10]}")
                        continue
                except ValueError:
                    pass
        tickers_to_analyze.append(ticker)

    if not tickers_to_analyze:
        print("All tickers already analyzed (within 30 days). Use force_reanalyze=True to refresh.")
        return existing

    print(f"\nFetching transcripts for {len(tickers_to_analyze)} tickers...")

    # Fetch transcripts
    transcripts_by_ticker = {}
    for i, ticker in enumerate(tickers_to_analyze):
        print(f"\n[{i+1}/{len(tickers_to_analyze)}] Fetching transcripts for {ticker}...")
        transcripts_by_ticker[ticker] = fetch_transcripts(ticker)

        if (i + 1) % 5 == 0:
            time.sleep(2)

    # Filter to tickers that actually have transcripts
    tickers_with_transcripts = {
        t: context_lookup.get(t, {"name": t, "sector": "Unknown"})
        for t in tickers_to_analyze
        if transcripts_by_ticker.get(t)
    }

    if not tickers_with_transcripts:
        print("\nNo transcripts found for any tickers. Aborting analysis.")
        return existing

    print(f"\n{len(tickers_with_transcripts)}/{len(tickers_to_analyze)} "
          f"tickers have transcripts available.")

    # Run analysis
    new_results = batch_analyze(tickers_with_transcripts, transcripts_by_ticker,
                                deep_threshold=deep_threshold)

    # Merge with existing
    all_results = {**existing, **new_results}

    # Save
    save_results(all_results)

    # Print summary
    print(f"\n{'='*60}")
    print(f"ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"  New analyses: {len(new_results)}")
    print(f"  Total profiles: {len(all_results)}")

    if new_results:
        sorted_results = sorted(new_results.items(),
                                key=lambda x: x[1].get("conviction_score", 0),
                                reverse=True)
        print(f"\n  Top scorers:")
        for ticker, r in sorted_results[:10]:
            style = r.get("management_style", "?")
            print(f"    {ticker:6s} | {r['conviction_score']:2d}/25 | "
                  f"V:{r['vision_score']} C:{r['capital_score']} "
                  f"T:{r['tam_score']} E:{r['execution_score']} "
                  f"R:{r['red_flag_score']} | {style}")
            verdict = r.get("one_line_verdict", "")
            if verdict:
                print(f"           \"{verdict}\"")

    return all_results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python management_analyzer.py TICKER [TICKER2 ...]")
        print("       python management_analyzer.py --all")
        print("       python management_analyzer.py --all --force")
        print("\nEnvironment variables:")
        print("  GOOGLE_AI_API_KEY  — Required for Gemini API")
        print("  FMP_API_KEY        — Optional for earnings call transcripts")
        print(f"\nOutput:")
        print(f"  Profiles: {PROFILES_FILE}")
        print(f"  Scores:   {SCORES_FILE}")
        sys.exit(1)

    from transcript_fetcher import fetch_transcripts

    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    if "--all" in sys.argv:
        # Load tickers from the screener ticker list
        ticker_file = REPO_ROOT / "data" / "microcap_tickers.txt"
        if ticker_file.exists():
            with open(ticker_file) as f:
                tickers = [line.strip() for line in f if line.strip()]
        else:
            print(f"Ticker file not found: {ticker_file}")
            sys.exit(1)

        # Load screener data for context
        screener_file = OUTPUT_DIR / "microcap_screener.json"
        screener_data = None
        if screener_file.exists():
            with open(screener_file) as f:
                screener_data = json.load(f)

        run_full_pipeline(tickers, screener_data=screener_data,
                          force_reanalyze=force)
    else:
        # Analyze specific tickers
        tickers = [t.upper() for t in args]

        for ticker in tickers:
            print(f"\n{'='*60}")
            print(f"Analyzing {ticker}")
            print(f"{'='*60}")

            transcripts = fetch_transcripts(ticker)
            if not transcripts:
                print(f"No transcripts found for {ticker}")
                continue

            result = analyze_company(ticker, transcripts,
                                     {"name": ticker, "sector": "Unknown"},
                                     deep=True)
            if result:
                print(f"\n--- RESULT ---")
                print(json.dumps(result, indent=2))
