#!/usr/bin/env python3
"""
The Book - portfolio builder (Layer B, generated)

Joins authored reasoning (Layer A: data/positions/*.yml + data/cash.yml) with
generated numbers (Layer B: data/prices/{TICKER}.json) into the two render
artifacts the site consumes:

    data/book.json        -> The Book chart, holdings dropdown, headline stats
    data/scoreboard.json  -> Track Record table + Home teaser

Core principles (build brief §3, Rev 2 §D):
  - Reasoning is authored; numbers are generated. Never hand-edit these outputs.
  - Stocks are valued from yfinance prices. Options are valued from the manual
    `current_mark` (per-share premium), stepwise — NOT from daily option pricing.
  - Cash comes from data/cash.yml.
  - The endpoints are real (current prices + authored cash); the in-between path
    is a price-reconstructed approximation, and is labelled as such.
  - Publish gate: a position with an empty thesis is `draft` and is excluded.
  - A return is never emitted without its same-window SPY benchmark.

Integrity checks fail loud: a missing benchmark is fatal; a stale option mark or
an unresolved ticker is surfaced as a warning in the JSON and on stderr.

Usage:
  python scripts/build_portfolio.py
"""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
POSITIONS_DIR = DATA_DIR / "positions"
PRICES_DIR = DATA_DIR / "prices"
CASH_FILE = DATA_DIR / "cash.yml"
CONFIG_FILE = DATA_DIR / "portfolio_config.yml"
DEEP_DIVES_DIR = ROOT / "content" / "deep-dives"

BOOK_OUT = DATA_DIR / "book.json"
SCOREBOARD_OUT = DATA_DIR / "scoreboard.json"
BENCHMARK = "SPY"

BUY_ACTIONS = {"open", "add"}
SELL_ACTIONS = {"trim", "close"}


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #
def load_config() -> dict:
    cfg = {"refresh_mode": "A", "inception": None}
    if CONFIG_FILE.exists():
        try:
            disk = yaml.safe_load(CONFIG_FILE.read_text()) or {}
            cfg.update({k: v for k, v in disk.items() if v is not None})
        except Exception as e:  # noqa: BLE001
            print(f"  [!] config: {e}", file=sys.stderr)
    return cfg


def load_positions() -> list[dict]:
    out = []
    if POSITIONS_DIR.exists():
        for f in sorted(POSITIONS_DIR.glob("*.yml")) + sorted(POSITIONS_DIR.glob("*.yaml")):
            doc = yaml.safe_load(f.read_text())
            if isinstance(doc, dict):
                doc.setdefault("id", f.stem)
                out.append(doc)
    return out


def load_cash() -> list[dict]:
    if not CASH_FILE.exists():
        return []
    doc = yaml.safe_load(CASH_FILE.read_text()) or {}
    bals = doc.get("balances") or []
    bals = [b for b in bals if b.get("date") and b.get("amount") is not None]
    bals.sort(key=lambda b: str(b["date"]))
    return bals


def load_prices() -> dict[str, pd.Series]:
    series: dict[str, pd.Series] = {}
    if not PRICES_DIR.exists():
        return series
    for f in PRICES_DIR.glob("*.json"):
        try:
            d = json.loads(f.read_text())
            idx = pd.to_datetime(d["dates"])
            s = pd.Series(d["close"], index=idx).sort_index()
            series[d.get("ticker", f.stem).upper()] = s
        except Exception as e:  # noqa: BLE001
            print(f"  [!] price file {f.name}: {e}", file=sys.stderr)
    return series


def price_fetched_at(prices: dict) -> Optional[str]:
    latest = None
    if PRICES_DIR.exists():
        for f in PRICES_DIR.glob("*.json"):
            try:
                fa = json.loads(f.read_text()).get("fetched_at")
                if fa and (latest is None or fa > latest):
                    latest = fa
            except Exception:  # noqa: BLE001
                pass
    return latest


def asof(series: Optional[pd.Series], d) -> Optional[float]:
    """Last close at or before date d (nearest prior trading day)."""
    if series is None or series.empty:
        return None
    v = series.asof(pd.Timestamp(d))
    return None if (v is None or pd.isna(v)) else float(v)


# --------------------------------------------------------------------------- #
# Per-position analysis
# --------------------------------------------------------------------------- #
def thesis_blank(p: dict) -> bool:
    return not str(p.get("thesis") or "").strip()


def is_option(p: dict) -> bool:
    return str(p.get("asset_type", "")).lower() in ("call", "put")


def event_qty(ev: dict) -> Optional[float]:
    q = ev.get("contracts") if ev.get("contracts") is not None else ev.get("shares")
    return float(q) if q is not None else None


def committed_usd(p: dict, ev: dict) -> Optional[float]:
    q, price = event_qty(ev), ev.get("price")
    if q is None or price is None:
        return None
    mult = 100 if is_option(p) else 1  # options: premium-at-risk, not notional
    return round(q * mult * float(price), 2)


def net_qty_at(p: dict, d) -> float:
    n = 0.0
    for ev in p.get("events", []) or []:
        if str(ev.get("date")) > str(d):
            continue
        q = event_qty(ev)
        if q is None:
            continue
        if ev.get("action") in BUY_ACTIONS:
            n += q
        elif ev.get("action") in SELL_ACTIONS:
            n -= q
    return n


def option_mark_at(p: dict, d) -> Optional[float]:
    """Stepwise per-share premium: last event price up to d; current_mark once
    we're at/after the final event."""
    evs = [e for e in (p.get("events") or []) if e.get("price") is not None]
    if not evs:
        return p.get("current_mark")
    prior = [e for e in evs if str(e.get("date")) <= str(d)]
    last_event_date = max(str(e["date"]) for e in evs)
    if str(d) >= last_event_date and p.get("current_mark") is not None:
        return float(p["current_mark"])
    if prior:
        return float(prior[-1]["price"])
    return None


def position_value_at(p: dict, d, prices: dict) -> Optional[float]:
    n = net_qty_at(p, d)
    if n == 0:
        return 0.0
    if is_option(p):
        mark = option_mark_at(p, d)
        return None if mark is None else round(n * 100 * mark, 2)
    px = asof(prices.get(str(p.get("ticker", "")).upper()), d)
    return None if px is None else round(n * px, 2)


def analyze_position(p: dict, prices: dict, as_of) -> dict:
    """Returns realized/unrealized economics + entry/exit facts for one position."""
    events = sorted((p.get("events") or []), key=lambda e: str(e.get("date")))
    mult = 100 if is_option(p) else 1

    total_units = 0.0          # shares or contracts currently held
    total_cost = 0.0           # cost basis of held units ($)
    realized_pnl = 0.0
    realized_cost = 0.0        # cost basis of units that were sold ($)
    entry_date = None
    entry_price = None
    exit_date = None
    exit_price = None

    for ev in events:
        q = event_qty(ev)
        price = ev.get("price")
        act = ev.get("action")
        if act in BUY_ACTIONS and q and price is not None:
            if entry_date is None:
                entry_date, entry_price = str(ev["date"]), float(price)
            total_cost += q * mult * float(price)
            total_units += q
        elif act in SELL_ACTIONS and q and price is not None:
            avg = (total_cost / total_units) if total_units else 0.0
            sold_cost = q * avg
            proceeds = q * mult * float(price)
            realized_pnl += proceeds - sold_cost
            realized_cost += sold_cost
            total_cost -= sold_cost
            total_units -= q
            exit_date, exit_price = str(ev["date"]), float(price)

    net_units = round(total_units, 6)
    is_closed = (str(p.get("status")) == "closed") or (net_units <= 0 and any(e.get("action") == "close" for e in events))

    # current valuation
    if is_option(p):
        mark = option_mark_at(p, as_of)
        cur_value = (net_units * 100 * mark) if (net_units > 0 and mark is not None) else 0.0
        price_ok = True  # options aren't priced from yfinance
        marked = p.get("current_mark") is not None
    else:
        px = asof(prices.get(str(p.get("ticker", "")).upper()), as_of)
        cur_value = (net_units * px) if (net_units > 0 and px is not None) else 0.0
        price_ok = px is not None
        marked = True  # n/a for stocks

    remaining_cost = max(total_cost, 0.0)
    unrealized = cur_value - remaining_cost if net_units > 0 else 0.0
    unrealized_pct = (unrealized / remaining_cost * 100) if remaining_cost > 0 else None

    return {
        "entry_date": entry_date,
        "entry_price": entry_price,
        "exit_date": exit_date,
        "exit_price": exit_price,
        "net_units": net_units,
        "is_closed": is_closed,
        "current_value": round(cur_value, 2),
        "remaining_cost": round(remaining_cost, 2),
        "unrealized_usd": round(unrealized, 2),
        "unrealized_pct": round(unrealized_pct, 1) if unrealized_pct is not None else None,
        "realized_pnl": round(realized_pnl, 2),
        "realized_cost": round(realized_cost, 2),
        "price_ok": price_ok,
        "marked": marked,
    }


def spy_return(prices: dict, start, end) -> Optional[float]:
    spy = prices.get(BENCHMARK)
    a, b = asof(spy, start), asof(spy, end)
    if a is None or b is None or a == 0:
        return None
    return round((b / a - 1) * 100, 1)


# --------------------------------------------------------------------------- #
# Equity curve (event-derived cash + reconstructed holdings)
# --------------------------------------------------------------------------- #
def build_cash_timeline(positions: list[dict], balances: list[dict]):
    """Merged, date-ordered list of cash re-anchors (authored balances) and
    flows (buys -, sells +). Returns a cash_at(d) function or None if no cash."""
    if not balances:
        return None
    timeline = []
    for b in balances:
        timeline.append((str(b["date"]), 0, float(b["amount"])))  # 0 = anchor (sorts first)
    for p in positions:
        mult = 100 if is_option(p) else 1
        for ev in p.get("events", []) or []:
            q, price, act = event_qty(ev), ev.get("price"), ev.get("action")
            if q is None or price is None:
                continue
            flow = -q * mult * float(price) if act in BUY_ACTIONS else (q * mult * float(price) if act in SELL_ACTIONS else 0)
            if flow:
                timeline.append((str(ev["date"]), 1, flow))  # 1 = flow
    timeline.sort(key=lambda x: (x[0], x[1]))

    def cash_at(d):
        running = None
        ds = str(d)
        for date_s, kind, val in timeline:
            if date_s > ds:
                break
            running = val if kind == 0 else (running or 0) + val
        return running

    return cash_at


def build_series(positions: list[dict], prices: dict, cash_at, inception, as_of):
    grid = pd.date_range(start=pd.Timestamp(inception), end=pd.Timestamp(as_of), freq="W-FRI")
    if len(grid) == 0 or grid[-1] < pd.Timestamp(as_of):
        grid = grid.append(pd.DatetimeIndex([pd.Timestamp(as_of)]))

    dates, port, spy = [], [], []
    spy_series = prices.get(BENCHMARK)
    base_port = None
    base_spy = asof(spy_series, grid[0])

    for d in grid:
        holdings = 0.0
        for p in positions:
            v = position_value_at(p, d, prices)
            holdings += v if v is not None else 0.0
        c = cash_at(d) if cash_at else 0.0
        total = holdings + (c if c is not None else 0.0)
        if base_port is None and total > 0:
            base_port = total
        dates.append(d.strftime("%Y-%m-%d"))
        port.append(total)
        spy.append(asof(spy_series, d))

    base_port = base_port or 1.0
    port_norm = [round(v / base_port * 100, 2) for v in port]
    if base_spy:
        spy_norm = [round((v / base_spy * 100), 2) if v else None for v in spy]
    else:
        spy_norm = [None] * len(dates)
    return {"dates": dates, "portfolio": port_norm, "spy": spy_norm}


def max_drawdown(norm: list[float]) -> float:
    peak = -math.inf
    mdd = 0.0
    for v in norm:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, (v / peak - 1) * 100)
    return round(mdd, 1)


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def main() -> int:
    cfg = load_config()
    all_positions = load_positions()
    balances = load_cash()
    prices = load_prices()

    warnings: list[str] = []
    fatal: list[str] = []

    # publish gate
    published, drafts = [], []
    for p in all_positions:
        (drafts if thesis_blank(p) else published).append(p)
    for p in drafts:
        print(f"  · draft (no thesis), excluded: {p.get('id')}")

    # inception / as_of
    ev_dates = [str(ev["date"]) for p in published for ev in (p.get("events") or []) if ev.get("date")]
    inception = cfg.get("inception") or (min(ev_dates) if ev_dates else None)
    as_of = price_fetched_at(prices) or date.today().isoformat()
    if not inception:
        print("No published positions with events. Writing empty artifacts.")
        inception = as_of

    # benchmark must exist and span the window (fail loud)
    if BENCHMARK not in prices or prices[BENCHMARK].empty:
        fatal.append("benchmark SPY price file missing — the book cannot be benchmarked")
    else:
        spy = prices[BENCHMARK]
        if spy.index.min() > pd.Timestamp(inception):
            warnings.append(f"SPY history starts {spy.index.min().date()}, after inception {inception}")

    # per-position economics
    analyses = {p["id"]: analyze_position(p, prices, as_of) for p in published}

    # integrity: unresolved tickers / unmarked options
    for p in published:
        a = analyses[p["id"]]
        if not a["price_ok"]:
            warnings.append(f"{p['id']} ({p.get('ticker')}): no price data — value shown as 0")
        if is_option(p) and not a["marked"]:
            warnings.append(f"{p['id']} ({p.get('ticker')}): option has no current_mark — using last premium")

    # equity curve
    cash_at = build_cash_timeline(published, balances)
    series = build_series(published, prices, cash_at, inception, as_of)
    mdd = max_drawdown(series["portfolio"])
    headline_return = round(series["portfolio"][-1] - 100, 1) if series["portfolio"] else 0.0
    spy_vals = [v for v in series["spy"] if v is not None]
    spy_return_pct = round(spy_vals[-1] - 100, 1) if spy_vals else None
    if spy_return_pct is None:
        fatal.append("benchmark series empty over window — refusing to emit unbenchmarked returns")

    current_cash = float(balances[-1]["amount"]) if balances else None

    # markers (one per event)
    markers = []
    for p in published:
        for ev in p.get("events", []) or []:
            if not ev.get("date"):
                continue
            markers.append({
                "position_id": p["id"],
                "date": str(ev["date"]),
                "action": ev.get("action"),
                "ticker": str(p.get("ticker", "")).upper(),
                "committed_usd": committed_usd(p, ev),
                "price": ev.get("price"),
                "note": (str(ev.get("note")).strip() or None) if ev.get("note") else None,
            })
    markers.sort(key=lambda m: m["date"])

    # open positions
    open_positions = []
    for p in published:
        a = analyses[p["id"]]
        if a["is_closed"] or a["net_units"] <= 0:
            continue
        entry = a["entry_date"]
        op = {
            "position_id": p["id"],
            "ticker": str(p.get("ticker", "")).upper(),
            "asset_type": p.get("asset_type"),
            "market_value": a["current_value"],
            "unrealized_pct": a["unrealized_pct"],
            "vs_spy_since_entry_pct": (
                round((a["unrealized_pct"] or 0) - (spy_return(prices, entry, as_of) or 0), 1)
                if a["unrealized_pct"] is not None and spy_return(prices, entry, as_of) is not None else None
            ),
            "entry_date": entry,
            "verdict": p.get("verdict"),
            "price_ok": a["price_ok"],
            "marked": a["marked"],
        }
        if is_option(p):
            op["contracts"] = a["net_units"]
            op["strike"] = p.get("strike")
            op["expiry"] = p.get("expiry")
            op["current_mark"] = p.get("current_mark")
        else:
            op["shares"] = a["net_units"]
        open_positions.append(op)

    book = {
        "inception": inception,
        "as_of": as_of,
        "refresh_mode": cfg.get("refresh_mode", "A"),
        "cash": round(current_cash, 2) if current_cash is not None else None,
        "headline": {
            "return_pct": headline_return,
            "spy_return_pct": spy_return_pct,
            "max_drawdown_pct": mdd,
            "open_positions": len(open_positions),
        },
        "series": series,
        "markers": markers,
        "open_positions": open_positions,
        "approximation_note": (
            "Endpoints are real (current prices and authored cash); the in-between path is "
            "reconstructed from authored events priced against public market data. Options are "
            "valued from manually entered marks, stepwise."
        ),
        "integrity": {"ok": len(fatal) == 0, "warnings": warnings, "errors": fatal},
    }

    # ----- scoreboard ----- #
    rows = []
    for p in published:
        a = analyses[p["id"]]
        start = a["entry_date"]
        end = a["exit_date"] if a["is_closed"] else as_of
        if a["is_closed"]:
            ret_pct = round(a["realized_pnl"] / a["realized_cost"] * 100, 1) if a["realized_cost"] else None
            ret_usd = a["realized_pnl"]
        else:
            ret_pct = a["unrealized_pct"]
            ret_usd = a["unrealized_usd"]
        spy_pct = spy_return(prices, start, end)
        if spy_pct is None and not is_option(p):
            warnings.append(f"{p['id']}: no SPY benchmark over window {start}→{end}")
        rows.append({
            "type": "position",
            "position_id": p["id"],
            "ticker": str(p.get("ticker", "")).upper(),
            "asset_type": p.get("asset_type"),
            "verdict": p.get("verdict"),
            "status": "closed" if a["is_closed"] else "open",
            "entry_date": start,
            "exit_date": a["exit_date"] if a["is_closed"] else None,
            "entry_price": a["entry_price"],
            "exit_price": a["exit_price"] if a["is_closed"] else None,
            "return_pct": ret_pct,
            "return_usd": ret_usd,
            "spy_return_pct": spy_pct,
            "alpha_pct": round(ret_pct - spy_pct, 1) if (ret_pct is not None and spy_pct is not None) else None,
            "article": p.get("article"),
            "real_money": True,
        })

    # best-effort thesis-only article calls
    rows += build_article_call_rows(prices, as_of, {r["position_id"] for r in rows})

    rows.sort(key=lambda r: (r.get("entry_date") or r.get("call_date") or ""), reverse=True)

    resolved = [r for r in rows if r["status"] == "closed" and r.get("return_pct") is not None]
    hits = sum(1 for r in resolved if r["return_pct"] > 0)
    hit_rate = round(hits / len(resolved) * 100, 0) if resolved else None

    scoreboard = {
        "as_of": as_of,
        "aggregate": {
            "calls": len(rows),
            "return_pct": headline_return,
            "spy_return_pct": spy_return_pct,
            "max_drawdown_pct": mdd,
            "hit_rate_pct": hit_rate,
            "hit_rate_n": len(resolved),
            "hit_rate_caveat": "Hit rate over a small sample in a particular market regime; "
                               "bull-market beta is not skill. Read it alongside alpha and drawdown.",
        },
        "rows": rows,
        "integrity": {"ok": len(fatal) == 0, "warnings": warnings, "errors": fatal},
    }

    # ----- write / report ----- #
    if fatal:
        for e in fatal:
            print(f"  ✗ FATAL: {e}", file=sys.stderr)
        print("Refusing to write artifacts due to fatal integrity errors.", file=sys.stderr)
        return 1

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BOOK_OUT.write_text(json.dumps(book, indent=2))
    SCOREBOARD_OUT.write_text(json.dumps(scoreboard, indent=2))

    for w in warnings:
        print(f"  [!] {w}", file=sys.stderr)
    print(f"\nWrote {BOOK_OUT.relative_to(ROOT)} and {SCOREBOARD_OUT.relative_to(ROOT)}")
    print(f"  inception {inception} → as_of {as_of}  |  refresh mode {cfg.get('refresh_mode')}")
    print(f"  return {headline_return}%  vs SPY {spy_return_pct}%  |  max DD {mdd}%  |  open {len(open_positions)}  |  rows {len(rows)}")
    return 0


def build_article_call_rows(prices: dict, as_of, seen_position_ids: set) -> list[dict]:
    """Scan deep-dive front matter for tracked thesis-only calls (real_money:false)."""
    import re
    rows = []
    if not DEEP_DIVES_DIR.exists():
        return rows
    fm_re = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
    for f in DEEP_DIVES_DIR.glob("*.md"):
        try:
            m = fm_re.match(f.read_text())
            if not m:
                continue
            fm = yaml.safe_load(m.group(1)) or {}
            call = fm.get("call")
            if not (isinstance(call, dict) and call.get("ticker") and call.get("date")):
                continue
            if call.get("position_id") in seen_position_ids:
                continue  # already represented as a real position
            ticker = str(call["ticker"]).upper()
            ref = call.get("entry_reference")
            cur = asof(prices.get(ticker), as_of)
            ret = round((cur / float(ref) - 1) * 100, 1) if (cur and ref) else None
            spy_pct = spy_return(prices, str(call["date"]), as_of)
            rows.append({
                "type": "call",
                "ticker": ticker,
                "verdict": fm.get("verdict"),
                "status": "open",
                "call_date": str(call["date"]),
                "entry_date": str(call["date"]),
                "entry_reference": ref,
                "current": round(cur, 2) if cur else None,
                "return_pct": ret,
                "return_usd": None,
                "spy_return_pct": spy_pct,
                "alpha_pct": round(ret - spy_pct, 1) if (ret is not None and spy_pct is not None) else None,
                "article": "/" + f.relative_to(ROOT / "content").with_suffix("").as_posix() + "/",
                "real_money": bool(call.get("real_money", False)),
            })
        except Exception:  # noqa: BLE001
            continue
    return rows


if __name__ == "__main__":
    sys.exit(main())
