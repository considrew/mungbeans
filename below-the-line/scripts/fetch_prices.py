#!/usr/bin/env python3
"""
The Book - price fetcher (Layer B, generated)

Collects the ticker set that The Book needs to value itself, then pulls daily
adjusted-close history from Yahoo Finance and writes one JSON file per ticker
to data/prices/{TICKER}.json.

Ticker set =
  - every `ticker` (stock) / underlying (call/put) in data/positions/*.yml
  - every `call.ticker` in content/deep-dives/*.md front matter (best effort)
  - SPY (always — the benchmark)

This is PUBLIC MARKET DATA ONLY. No brokerage, no credentials, no account
access (see build brief Rev 2 §E / §15). It is safe to run on a schedule.

Refresh mode (brief §E) is a config flag, not a rewrite:
  - A (default): run this on a weekly cron + workflow_dispatch; commit the JSON.
  - B: run it by hand before each deploy.
Either way book.json.as_of records when prices were last refreshed.

Usage:
  python scripts/fetch_prices.py
  REFRESH_MODE=B python scripts/fetch_prices.py
"""
from __future__ import annotations

import json
import os
import random
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import yaml

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

# ---- Paths -----------------------------------------------------------------
ROOT = Path(__file__).parent.parent          # below-the-line/
DATA_DIR = ROOT / "data"
POSITIONS_DIR = DATA_DIR / "positions"
PRICES_DIR = DATA_DIR / "prices"
DEEP_DIVES_DIR = ROOT / "content" / "deep-dives"
CONFIG_FILE = DATA_DIR / "portfolio_config.yml"

BENCHMARK = "SPY"


# ---- Retry helper (mirrors update_stocks.py) -------------------------------
def retry_on_rate_limit(func, *args, max_retries=3, base_delay=5, **kwargs):
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            err = str(e).lower()
            is_rate_limit = any(s in err for s in ["429", "rate limit", "too many requests", "throttled"])
            if is_rate_limit and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 2)
                print(f"  [!] Rate limited, waiting {delay:.0f}s ({attempt + 1}/{max_retries})...")
                time.sleep(delay)
            else:
                raise
    return None


# ---- Config ----------------------------------------------------------------
def load_config() -> dict:
    cfg = {"refresh_mode": os.environ.get("REFRESH_MODE", "A"), "inception": None}
    if CONFIG_FILE.exists():
        try:
            disk = yaml.safe_load(CONFIG_FILE.read_text()) or {}
            cfg.update({k: v for k, v in disk.items() if v is not None})
        except Exception as e:  # noqa: BLE001
            print(f"  [!] Could not read {CONFIG_FILE.name}: {e}")
    # env always wins for the mode toggle
    cfg["refresh_mode"] = os.environ.get("REFRESH_MODE", cfg.get("refresh_mode", "A"))
    return cfg


# ---- Ticker discovery ------------------------------------------------------
def load_positions() -> list[dict]:
    out = []
    if not POSITIONS_DIR.exists():
        return out
    for f in sorted(POSITIONS_DIR.glob("*.yml")) + sorted(POSITIONS_DIR.glob("*.yaml")):
        try:
            doc = yaml.safe_load(f.read_text())
            if isinstance(doc, dict):
                doc.setdefault("id", f.stem)
                out.append(doc)
        except Exception as e:  # noqa: BLE001
            print(f"  [!] Skipping unparseable {f.name}: {e}")
    return out


_FRONT_MATTER = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def scan_article_calls() -> list[dict]:
    """Best-effort: pull `call:` blocks from deep-dive front matter."""
    calls = []
    if not DEEP_DIVES_DIR.exists():
        return calls
    for f in DEEP_DIVES_DIR.glob("*.md"):
        try:
            m = _FRONT_MATTER.match(f.read_text())
            if not m:
                continue
            fm = yaml.safe_load(m.group(1)) or {}
            call = fm.get("call")
            if isinstance(call, dict) and call.get("ticker"):
                calls.append({
                    "ticker": str(call["ticker"]).upper(),
                    "date": str(call.get("date")) if call.get("date") else None,
                    "article": "/" + f.relative_to(ROOT / "content").with_suffix("").as_posix() + "/",
                })
        except Exception:  # noqa: BLE001
            continue
    return calls


def first_event_date(positions: list[dict]) -> Optional[str]:
    dates = []
    for p in positions:
        for ev in p.get("events", []) or []:
            d = ev.get("date")
            if d:
                dates.append(str(d))
    return min(dates) if dates else None


def collect_tickers(positions: list[dict], calls: list[dict]) -> set[str]:
    tickers: set[str] = set()
    for p in positions:
        if str(p.get("status", "")).lower() == "draft":
            continue  # publish gate: drafts aren't valued
        t = p.get("ticker")
        if t:
            tickers.add(str(t).upper())
    for c in calls:
        tickers.add(c["ticker"])
    tickers.add(BENCHMARK)
    return tickers


# ---- Fetch -----------------------------------------------------------------
def fetch_one(ticker: str, start: str) -> Optional[dict]:
    if yf is None:
        raise RuntimeError("yfinance not installed; run `pip install yfinance`")

    def _do():
        return yf.Ticker(ticker).history(start=start, interval="1d", auto_adjust=True)

    df = retry_on_rate_limit(_do)
    if df is None or df.empty:
        print(f"  ✗ {ticker}: no data")
        return None
    closes = df["Close"].dropna()
    if closes.empty:
        print(f"  ✗ {ticker}: no closes")
        return None
    dates = [d.strftime("%Y-%m-%d") for d in closes.index]
    vals = [round(float(v), 4) for v in closes.values]
    print(f"  ✓ {ticker}: {len(vals)} days ({dates[0]} → {dates[-1]})")
    return {
        "ticker": ticker,
        "source": "yfinance",
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "start": start,
        "dates": dates,
        "close": vals,
    }


def main() -> int:
    cfg = load_config()
    positions = load_positions()
    calls = scan_article_calls()

    inception = cfg.get("inception") or first_event_date(positions)
    if not inception:
        print("No events found in data/positions/*.yml and no inception in config. Nothing to fetch.")
        # Still refresh SPY from a sane default so the benchmark exists.
        inception = date.today().replace(month=1, day=1).isoformat()

    tickers = sorted(collect_tickers(positions, calls))
    print(f"Refresh mode: {cfg['refresh_mode']}  |  inception: {inception}")
    print(f"Fetching {len(tickers)} tickers: {', '.join(tickers)}\n")

    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    ok, failed = 0, []
    for i, t in enumerate(tickers):
        data = None
        try:
            data = fetch_one(t, inception)
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ {t}: {e}")
        if data:
            (PRICES_DIR / f"{t}.json").write_text(json.dumps(data, indent=2))
            ok += 1
        else:
            failed.append(t)
        if (i + 1) % 25 == 0:
            time.sleep(1)  # be polite to Yahoo

    print(f"\nWrote {ok}/{len(tickers)} price files to {PRICES_DIR.relative_to(ROOT)}/")
    if failed:
        print(f"Failed: {', '.join(failed)}")
        if BENCHMARK in failed:
            print("ERROR: benchmark (SPY) failed to fetch — the book cannot be benchmarked.")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
