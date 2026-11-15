#!/usr/bin/env python3
"""Build the weekly deep-dive shortlist.

Ranks the tracked universe by how likely a deep dive on each stock is to get
read this week, using what actually drives search and click traffic: the size
of the week's move, how unusual the volume was, how recognisable the company
is, how much news it generated, and whether it just crossed the 200-week line
(the site's own hook). Stocks that already have a deep dive are excluded, so
every week is a new name.

The output is a shortlist, not a decision. Claude makes the final pick from it
after reading the news, and validate.py refuses any ticker that is not on it.

Usage:
    python select_candidates.py [--top 10] [--min-cap 500e6]
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

from common import CANDIDATES, STOCKS_JSON, covered_tickers, load_json, write_json

CHUNK = 200           # tickers per yfinance batch
NEWS_POOL = 30        # how many price/volume leaders get a news lookup
MOVE_CAP = 35.0       # % weekly move at which the move component saturates
MIN_PRICE = 3.0

# Weights of the viewership proxy. They sum to 1.
W_MOVE, W_NEWS, W_LINE, W_RVOL, W_SIZE = 0.38, 0.24, 0.16, 0.12, 0.10


def weekly_stats(symbols: list[str]) -> dict[str, dict]:
    """Last-five-session return, relative volume and biggest single day."""
    out: dict[str, dict] = {}
    for i in range(0, len(symbols), CHUNK):
        chunk = symbols[i:i + CHUNK]
        try:
            df = yf.download(chunk, period="3mo", interval="1d", progress=False,
                             auto_adjust=True, group_by="ticker", threads=True)
        except Exception as e:  # network hiccup: skip the chunk, keep going
            print(f"  WARNING: batch {i // CHUNK} failed: {e}", file=sys.stderr)
            continue
        for sym in chunk:
            try:
                d = df[sym].dropna(subset=["Close"]) if len(chunk) > 1 else df.dropna(subset=["Close"])
                if len(d) < 25:
                    continue
                close, vol = d["Close"], d["Volume"]
                last, prev = float(close.iloc[-1]), float(close.iloc[-6])
                if prev <= 0:
                    continue
                daily = close.pct_change().iloc[-5:] * 100
                base_vol = float(vol.iloc[-25:-5].mean())
                out[sym] = {
                    "close": round(last, 2),
                    "close_date": d.index[-1].strftime("%Y-%m-%d"),
                    "prev_week_close": round(prev, 2),
                    "week_return_pct": round((last / prev - 1) * 100, 2),
                    "max_day_move_pct": round(float(daily.abs().max()), 2),
                    "rvol": round(float(vol.iloc[-5:].mean()) / base_vol, 2) if base_vol > 0 else None,
                }
            except Exception:
                continue
        time.sleep(1)
    return out


def recent_news(symbol: str, days: int = 8) -> list[dict]:
    """Headlines from the last `days` days, newest first."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    items = []
    try:
        raw = yf.Ticker(symbol).news or []
    except Exception:
        return items
    for n in raw:
        c = n.get("content") or n
        title = c.get("title")
        when = c.get("pubDate") or c.get("displayTime")
        if not title or not when:
            continue
        try:
            ts = datetime.fromisoformat(str(when).replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts < cutoff:
            continue
        items.append({
            "title": title,
            "publisher": (c.get("provider") or {}).get("displayName"),
            "date": ts.strftime("%Y-%m-%d"),
            "url": ((c.get("canonicalUrl") or {}).get("url")
                    or (c.get("clickThroughUrl") or {}).get("url")),
        })
    return sorted(items, key=lambda x: x["date"], reverse=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--min-cap", type=float, default=500e6)
    args = ap.parse_args()

    data = load_json(STOCKS_JSON)
    if not data:
        print(f"ERROR: {STOCKS_JSON} missing", file=sys.stderr)
        return 1

    covered = covered_tickers()
    universe = {}
    for s in data["stocks"]:
        sym = s.get("symbol")
        cap = s.get("market_cap") or 0
        if not sym or sym in covered or cap < args.min_cap or not s.get("name"):
            continue
        if not sym.isalpha():        # share classes and OTC suffixes: thin coverage
            continue
        universe[sym] = s
    print(f"Universe: {len(universe)} eligible ({len(covered)} already covered, excluded)")

    stats = weekly_stats(sorted(universe))
    print(f"Priced: {len(stats)}")
    if len(stats) < len(universe) * 0.5:
        print("ERROR: fewer than half the universe priced; refusing to rank on partial data",
              file=sys.stderr)
        return 1

    rows = []
    for sym, st in stats.items():
        if st["close"] < MIN_PRICE:
            continue
        s = universe[sym]
        cap = s["market_cap"]
        wma = s.get("wma_200")
        move = min(abs(st["week_return_pct"]), MOVE_CAP) / MOVE_CAP
        # A one-day gap reads as an event even when the week nets out smaller.
        move = max(move, min(st["max_day_move_pct"], MOVE_CAP) / MOVE_CAP * 0.85)
        rvol = min(max((st["rvol"] or 1) - 1, 0), 4) / 4
        size = min(max(math.log10(cap) - 9, 0), 3) / 3      # $1B -> 0, $1T -> 1
        crossed = bool(wma and st["prev_week_close"] >= wma > st["close"])
        near = bool(wma and abs(st["close"] / wma - 1) <= 0.05)
        line = 1.0 if crossed else (0.4 if near else 0.0)
        rows.append({
            "ticker": sym,
            "name": s["name"],
            "sector": s.get("sector"),
            "market_cap": cap,
            **st,
            "wma_200": wma,
            "pct_from_wma": round((st["close"] / wma - 1) * 100, 1) if wma else None,
            "crossed_below_200wma_this_week": crossed,
            "_parts": {"move": move, "rvol": rvol, "size": size, "line": line},
        })

    # News is one request per ticker, so only the price/volume leaders get one.
    def prelim(r):
        p = r["_parts"]
        return W_MOVE * p["move"] + W_RVOL * p["rvol"] + W_SIZE * p["size"] + W_LINE * p["line"]

    rows.sort(key=prelim, reverse=True)
    pool = rows[:NEWS_POOL]
    for r in pool:
        r["headlines"] = recent_news(r["ticker"])
        r["_parts"]["news"] = min(len(r["headlines"]), 10) / 10
        p = r["_parts"]
        r["viewership_score"] = round(100 * (prelim(r) + W_NEWS * p["news"]), 1)
        r["score_parts"] = {k: round(v, 2) for k, v in p.items()}
        time.sleep(0.3)

    pool.sort(key=lambda r: r["viewership_score"], reverse=True)
    shortlist = pool[:args.top]
    for rank, r in enumerate(shortlist, 1):
        r["rank"] = rank
        r.pop("_parts", None)

    if not shortlist:
        print("ERROR: empty shortlist", file=sys.stderr)
        return 1

    write_json(CANDIDATES, {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "price_date": shortlist[0]["close_date"],
        "weights": {"move": W_MOVE, "news": W_NEWS, "size": W_SIZE, "rvol": W_RVOL, "line": W_LINE},
        "excluded_already_covered": sorted(covered),
        "candidates": shortlist,
    })
    print(f"\nShortlist written to {CANDIDATES}")
    for r in shortlist:
        print(f"  {r['rank']:>2}. {r['ticker']:<6} score {r['viewership_score']:>5}  "
              f"week {r['week_return_pct']:+6.1f}%  rvol {r['rvol']}  "
              f"news {len(r['headlines']):>2}  {r['name'][:40]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
