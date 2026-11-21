#!/usr/bin/env python3
"""AI Exposure & Toll Gate Ranking Engine (v0.2)

Implements docs/ai-exposure-methodology.md:
  - AI Bust Exposure (0-100): Pillars A (Depreciation Bomb, 25%),
    B (Leverage & Funding, 25%), C (Counterparty/Circularity, 20%, overlay),
    D (Revenue Dependence, 15%, overlay), E (Market-Implied AI Beta, 15%).
  - AI Toll Gate (0-100, curated-only): Pillars T1 (Data Moat, 45%, overlay),
    T2 (Capital Lightness, 20%), T3 (Efficiency Capture, 10%),
    T4 (Cash Quality, 25%) minus Disintermediation Penalty (0 to -20).
  - Quadrant: toll_collector | levered_landlord | bystander | bag_holder | unlinked.

Reads:
  below-the-line/assets/data/stocks.json
  below-the-line/data/ai_overlay.yml
  below-the-line/assets/data/ai_exposure.json (optional prior run for hysteresis)

Writes:
  below-the-line/assets/data/ai_exposure.json
  below-the-line/static/data/ai_exposure.json
  Merges slim 5-field summary into below-the-line/assets/data/stocks.json

Usage:
  python scripts/ai_exposure.py [--no-merge] [--verbose]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import yaml

REPO = Path(__file__).resolve().parent.parent
STOCKS_FILE = REPO / "assets" / "data" / "stocks.json"
OVERLAY_FILE = REPO / "data" / "ai_overlay.yml"
OUT_ASSETS = REPO / "assets" / "data" / "ai_exposure.json"
OUT_STATIC = REPO / "static" / "data" / "ai_exposure.json"

# Equal-weight Tier-1 AI basket for Pillar E regression (stock i excluded when estimating i)
AI_BASKET = [
    "NVDA", "AVGO", "AMD", "MU", "MRVL", "SMCI", "ANET", "DELL", "HPE",
    "VRT", "VST", "CEG", "NBIS", "IREN", "APLD", "ORCL",
]

# Bust exposure nominal weights
BUST_W = {"A": 0.25, "B": 0.25, "C": 0.20, "D": 0.15, "E": 0.15}
# Toll gate nominal weights
TOLL_W = {"T1": 0.45, "T2": 0.20, "T3": 0.10, "T4": 0.25}

# Gate & quadrant thresholds (v0.2, calibrated on Nov 2026 universe to exclude unrelated high-beta noise)
BETA_ENTER_T = 2.5
BETA_ENTER_SHRUNK = 0.40
BETA_EXIT_T = 1.75
BUST_HIGH_CUTOFF = 50.0
TOLL_HIGH_CUTOFF = 60.0
TOLL_MOAT_MIN_RAW = 6  # out of 10
STALE_DAYS = 180


def _is_num(v: Any) -> bool:
    return v is not None and isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))


def _sector_group(sector: str | None) -> str:
    s = (sector or "").lower()
    if "reit" in s or "real estate" in s:
        return "reit"
    if "utilit" in s:
        return "utility"
    if "financial" in s or "bank" in s or "insurance" in s or "capital markets" in s or "asset management" in s:
        return "financial"
    return "general"


def _ym_months(start_ym: str, n: int) -> list[str]:
    """Generate n YYYY-MM strings starting from start_ym."""
    try:
        y, m = int(start_ym[:4]), int(start_ym[5:7])
    except Exception:
        return []
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def build_monthly_returns(stocks: list[dict]) -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    """Build {symbol: {YYYY-MM: monthly_return}} and SPY {YYYY-MM: monthly_return} from growth_chart."""
    stock_rets: dict[str, dict[str, float]] = {}
    spy_rets: dict[str, float] = {}

    for st in stocks:
        gc = st.get("growth_chart")
        if not gc or not gc.get("start") or not gc.get("s") or not gc.get("b"):
            continue
        s_vals = gc["s"]
        b_vals = gc["b"]
        n = min(len(s_vals), len(b_vals))
        if n < 19:
            continue
        months = _ym_months(gc["start"], n)
        if not months:
            continue

        # Take trailing 37 points (36 monthly returns)
        start_idx = max(0, n - 37)
        r_map: dict[str, float] = {}
        for i in range(start_idx + 1, n):
            p0, p1 = s_vals[i - 1], s_vals[i]
            if _is_num(p0) and _is_num(p1) and p0 > 0:
                r_map[months[i]] = (float(p1) / float(p0)) - 1.0
            b0, b1 = b_vals[i - 1], b_vals[i]
            if months[i] not in spy_rets and _is_num(b0) and _is_num(b1) and b0 > 0:
                spy_rets[months[i]] = (float(b1) / float(b0)) - 1.0

        if len(r_map) >= 18:
            stock_rets[st["symbol"]] = r_map

    return stock_rets, spy_rets


def compute_ai_betas(stocks: list[dict]) -> dict[str, dict[str, Optional[float]]]:
    """Estimate Pillar E: r_i = a + b_mkt * r_SPY + b_AI * EW_AI(-i), with smooth t-stat shrinkage."""
    stock_rets, spy_rets = build_monthly_returns(stocks)
    basket_avail = [t for t in AI_BASKET if t in stock_rets]

    # Collect common recent months (up to 36)
    all_months = sorted(spy_rets.keys())[-36:]
    results: dict[str, dict[str, Optional[float]]] = {}

    for st in stocks:
        sym = st["symbol"]
        rm = stock_rets.get(sym)
        if not rm:
            results[sym] = {"ai_beta": None, "ai_beta_t": None, "ai_beta_shrunk": None}
            continue

        # Build aligned rows excluding sym from AI basket
        y_list, x_spy, x_ai = [], [], []
        for mo in all_months:
            if mo not in rm or mo not in spy_rets:
                continue
            b_vals = [stock_rets[t][mo] for t in basket_avail if t != sym and mo in stock_rets[t]]
            if len(b_vals) < 6:
                continue
            y_list.append(rm[mo])
            x_spy.append(spy_rets[mo])
            x_ai.append(sum(b_vals) / len(b_vals))

        n = len(y_list)
        if n < 18:
            results[sym] = {"ai_beta": None, "ai_beta_t": None, "ai_beta_shrunk": None}
            continue

        y = np.array(y_list, dtype=float)
        X = np.column_stack([np.ones(n), np.array(x_spy, dtype=float), np.array(x_ai, dtype=float)])
        try:
            coef, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
            resid = y - X @ coef
            dof = n - 3
            if dof <= 0:
                raise ValueError("insufficient dof")
            s2 = float(np.sum(resid ** 2)) / dof
            xtx_inv = np.linalg.pinv(X.T @ X)
            se_ai = math.sqrt(max(1e-12, s2 * float(xtx_inv[2, 2])))
            beta_ai = float(coef[2])
            t_stat = beta_ai / se_ai
            shrunk = beta_ai * (t_stat ** 2) / (t_stat ** 2 + 4.0)
            results[sym] = {
                "ai_beta": round(beta_ai, 3),
                "ai_beta_t": round(t_stat, 2),
                "ai_beta_shrunk": round(shrunk, 3),
            }
        except Exception:
            results[sym] = {"ai_beta": None, "ai_beta_t": None, "ai_beta_shrunk": None}

    return results


def winsorized_percentiles(values: dict[str, Optional[float]], higher_is_worse: bool = True) -> dict[str, Optional[float]]:
    """Winsorize at 5th/95th percentiles, then compute percentile rank (0-100) across non-null entries."""
    valid = [(k, float(v)) for k, v in values.items() if _is_num(v)]
    out: dict[str, Optional[float]] = {k: None for k in values}
    if len(valid) < 3:
        return out

    arr = np.array([v for _, v in valid], dtype=float)
    lo, hi = float(np.percentile(arr, 5)), float(np.percentile(arr, 95))
    clipped = [(k, min(hi, max(lo, v))) for k, v in valid]
    c_arr = np.array([v for _, v in clipped], dtype=float)
    n = len(c_arr)

    for k, v in clipped:
        # Mid-rank percentile (0-100)
        lt = int(np.sum(c_arr < v))
        eq = int(np.sum(c_arr == v))
        pct = ((lt + 0.5 * eq) / n) * 100.0
        if not higher_is_worse:
            pct = 100.0 - pct
        out[k] = round(max(0.0, min(100.0, pct)), 1)
    return out


def percentile_with_sector_split(
    raw_map: dict[str, Optional[float]],
    sec_map: dict[str, str],
    higher_is_worse: bool = True,
    sector_relative_groups: set[str] | None = None,
) -> dict[str, Optional[float]]:
    """Rank general universe together; rank specified sector groups (utility, reit) within sector."""
    sector_relative_groups = sector_relative_groups or set()
    # Full-universe rank first
    out = winsorized_percentiles(raw_map, higher_is_worse=higher_is_worse)
    for grp in sector_relative_groups:
        sub = {k: v for k, v in raw_map.items() if sec_map.get(k) == grp}
        non_null = sum(1 for v in sub.values() if _is_num(v))
        if non_null >= 5:
            sub_pct = winsorized_percentiles(sub, higher_is_worse=higher_is_worse)
            out.update(sub_pct)
    return out


def extract_raw_metrics(st: dict, ov: dict, beta_info: dict, sec_grp: str) -> dict[str, Optional[float]]:
    """Extract raw quantitative metrics for a single stock from ai_fundamentals + stocks.json fallbacks."""
    af = st.get("ai_fundamentals") or {}
    hc = st.get("health_chart") or {}

    # Latest revenue fallback from health_chart (in $M -> $)
    rev = af.get("revenue") or st.get("revenue")
    if not _is_num(rev):
        hc_rev = [x for x in (hc.get("revenue") or []) if _is_num(x) and x > 0]
        if hc_rev:
            rev = float(hc_rev[-1]) * 1e6

    # Latest total_debt & debt_growth_yoy fallback from health_chart (in $M -> $)
    total_debt = af.get("total_debt")
    debt_growth_yoy = af.get("debt_growth_yoy")
    hc_debt = [x for x in (hc.get("total_debt") or []) if _is_num(x)]
    if not _is_num(total_debt) and hc_debt:
        total_debt = float(hc_debt[-1]) * 1e6
    if not _is_num(debt_growth_yoy) and len(hc_debt) >= 2 and hc_debt[-2] > 0:
        debt_growth_yoy = ((float(hc_debt[-1]) - float(hc_debt[-2])) / float(hc_debt[-2])) * 100.0

    capex = af.get("capex")
    da = af.get("da")
    ebit = af.get("ebit")
    ebitda = af.get("ebitda")
    ni_fy = af.get("net_income")
    ocf = af.get("ocf")
    cash = af.get("total_cash")
    int_exp = af.get("interest_expense")
    fcf = st.get("fcf")
    mcap = st.get("market_cap")

    # REITs: approximate FFO = Net Income + D&A in place of EBITDA
    denom_a = ebitda
    if sec_grp == "reit" and _is_num(ni_fy) and _is_num(da):
        denom_a = float(ni_fy) + float(da)

    # Pillar A raw metrics
    capex_to_da = (float(capex) / float(da)) if (_is_num(capex) and _is_num(da) and da > 0) else None
    depreciation_overhang = None
    dep_overhang_max_flag = False
    if _is_num(capex) and _is_num(da) and _is_num(denom_a):
        if denom_a > 0:
            depreciation_overhang = ((float(capex) - float(da)) / float(denom_a)) * 100.0
        elif capex_to_da is not None and capex_to_da > 1.5:
            dep_overhang_max_flag = True

    depreciation_overhang_ebit = None
    if _is_num(capex) and _is_num(da) and _is_num(ebit) and ebit > 0:
        depreciation_overhang_ebit = ((float(capex) - float(da)) / float(ebit)) * 100.0

    capex_intensity = (float(capex) / float(rev) * 100.0) if (_is_num(capex) and _is_num(rev) and rev > 0) else None
    capex_growth_vs_rev = None
    if _is_num(af.get("capex_3yr_cagr")) and _is_num(af.get("rev_3yr_cagr")):
        capex_growth_vs_rev = float(af["capex_3yr_cagr"]) - float(af["rev_3yr_cagr"])

    # Pillar B raw metrics
    capex_funding_gap = None
    if _is_num(capex) and capex > 0 and _is_num(ocf):
        capex_funding_gap = max(0.0, float(capex) - float(ocf)) / float(capex) * 100.0
    elif _is_num(fcf) and _is_num(rev) and rev > 0:
        # Graceful proxy before ai_fundamentals is populated on next Saturday run:
        # FCF deficit as % of revenue (0 when FCF >= 0)
        capex_funding_gap = max(0.0, -float(fcf)) / float(rev) * 100.0

    net_debt_to_ebitda = None
    if _is_num(total_debt) and _is_num(cash) and _is_num(ebitda) and ebitda > 0:
        net_debt_to_ebitda = (float(total_debt) - float(cash)) / float(ebitda)
    elif _is_num(st.get("debt_to_equity")):
        # Fallback leverage metric when EBITDA/cash not yet populated
        net_debt_to_ebitda = float(st["debt_to_equity"]) / 100.0

    interest_coverage_inv = None
    if _is_num(int_exp) and _is_num(ebit) and ebit > 0:
        interest_coverage_inv = float(int_exp) / float(ebit)

    shares_change_3yr = float(st["shares_change_3yr"]) if _is_num(st.get("shares_change_3yr")) else None

    off_bs = ov.get("off_bs_obligations_usd")
    off_bs_to_mcap = (float(off_bs) / float(mcap) * 100.0) if (_is_num(off_bs) and _is_num(mcap) and mcap > 0) else None

    # Toll Gate quantitative raw metrics (T2, T3, T4)
    gross_margin = float(st["gross_margin"]) if _is_num(st.get("gross_margin")) else None
    op_margin_delta_3y = float(af["op_margin_delta_3y"]) if _is_num(af.get("op_margin_delta_3y")) else None
    sga_ratio_delta_3y = float(af["sga_ratio_delta_3y"]) if _is_num(af.get("sga_ratio_delta_3y")) else None

    fcf_margin = (float(fcf) / float(rev) * 100.0) if (_is_num(fcf) and _is_num(rev) and rev > 0) else None
    fcf_consec = float(st["fcf_consecutive_positive"]) if _is_num(st.get("fcf_consecutive_positive")) else None
    net_cash_to_mcap = None
    if _is_num(cash) and _is_num(total_debt) and _is_num(mcap) and mcap > 0:
        net_cash_to_mcap = (float(cash) - float(total_debt)) / float(mcap) * 100.0

    return {
        "capex_to_da": round(capex_to_da, 2) if capex_to_da is not None else None,
        "depreciation_overhang": round(depreciation_overhang, 1) if depreciation_overhang is not None else None,
        "depreciation_overhang_ebit": round(depreciation_overhang_ebit, 1) if depreciation_overhang_ebit is not None else None,
        "dep_overhang_max_flag": dep_overhang_max_flag,
        "capex_intensity": round(capex_intensity, 1) if capex_intensity is not None else None,
        "capex_growth_vs_rev": round(capex_growth_vs_rev, 1) if capex_growth_vs_rev is not None else None,
        "capex_funding_gap": round(capex_funding_gap, 1) if capex_funding_gap is not None else None,
        "net_debt_to_ebitda": round(net_debt_to_ebitda, 2) if net_debt_to_ebitda is not None else None,
        "debt_growth_yoy": round(debt_growth_yoy, 1) if _is_num(debt_growth_yoy) else None,
        "interest_coverage_inv": round(interest_coverage_inv, 3) if interest_coverage_inv is not None else None,
        "shares_change_3yr": round(shares_change_3yr, 1) if shares_change_3yr is not None else None,
        "off_bs_to_mcap": round(off_bs_to_mcap, 1) if off_bs_to_mcap is not None else None,
        "ai_beta": beta_info.get("ai_beta"),
        "ai_beta_t": beta_info.get("ai_beta_t"),
        "ai_beta_shrunk": beta_info.get("ai_beta_shrunk"),
        "gross_margin": round(gross_margin, 1) if gross_margin is not None else None,
        "op_margin_delta_3y": round(op_margin_delta_3y, 1) if op_margin_delta_3y is not None else None,
        "sga_ratio_delta_3y": round(sga_ratio_delta_3y, 1) if sga_ratio_delta_3y is not None else None,
        "fcf_margin": round(fcf_margin, 1) if fcf_margin is not None else None,
        "fcf_consec": fcf_consec,
        "net_cash_to_mcap": round(net_cash_to_mcap, 1) if net_cash_to_mcap is not None else None,
        "has_full_af": bool(af),
    }


def _mean_non_null(vals: list[Optional[float]]) -> Optional[float]:
    clean = [v for v in vals if _is_num(v)]
    return round(sum(clean) / len(clean), 1) if clean else None


def _is_stale(reviewed_val: Any, today: date) -> bool:
    if not reviewed_val:
        return True
    try:
        if isinstance(reviewed_val, date):
            rev_d = reviewed_val
        else:
            rev_d = datetime.strptime(str(reviewed_val)[:10], "%Y-%m-%d").date()
        return (today - rev_d).days > STALE_DAYS
    except Exception:
        return True


def _confidence_label(weight_share: float) -> str:
    if weight_share >= 0.85:
        return "high"
    if weight_share >= 0.60:
        return "medium"
    return "insufficient_data"


def _lower_confidence(c1: Optional[str], c2: Optional[str]) -> Optional[str]:
    rank = {"insufficient_data": 0, "medium": 1, "high": 2}
    vals = [c for c in (c1, c2) if c in rank]
    if not vals:
        return None
    return min(vals, key=lambda c: rank[c])


def compute_all(stocks: list[dict], overlay: dict[str, dict], prior_records: dict[str, dict]) -> dict:
    today = date.today()
    betas = compute_ai_betas(stocks)
    sec_map = {st["symbol"]: _sector_group(st.get("sector")) for st in stocks}

    raw_by_sym: dict[str, dict[str, Any]] = {}
    for st in stocks:
        sym = st["symbol"]
        raw_by_sym[sym] = extract_raw_metrics(st, overlay.get(sym) or {}, betas.get(sym) or {}, sec_map[sym])

    # Cross-sectional percentiles (universe-wide; utilities & REITs ranked within sector for A & B)
    def col(name: str) -> dict[str, Optional[float]]:
        return {sym: raw_by_sym[sym].get(name) for sym in raw_by_sym}

    sr_ab = {"utility", "reit"}
    p_capex_da = percentile_with_sector_split(col("capex_to_da"), sec_map, True, sr_ab)
    p_dep_over = percentile_with_sector_split(col("depreciation_overhang"), sec_map, True, sr_ab)
    p_capex_int = percentile_with_sector_split(col("capex_intensity"), sec_map, True, sr_ab)
    p_capex_gvr = percentile_with_sector_split(col("capex_growth_vs_rev"), sec_map, True, sr_ab)

    p_fund_gap = percentile_with_sector_split(col("capex_funding_gap"), sec_map, True, sr_ab)
    p_nd_ebitda = percentile_with_sector_split(col("net_debt_to_ebitda"), sec_map, True, sr_ab)
    p_debt_yoy = percentile_with_sector_split(col("debt_growth_yoy"), sec_map, True, sr_ab)
    p_int_cov = percentile_with_sector_split(col("interest_coverage_inv"), sec_map, True, sr_ab)
    p_shares = percentile_with_sector_split(col("shares_change_3yr"), sec_map, True, sr_ab)
    p_off_bs = winsorized_percentiles(col("off_bs_to_mcap"), True)

    p_ai_beta = winsorized_percentiles(col("ai_beta_shrunk"), True)

    # Toll Gate quant percentiles (higher = better)
    p_capex_light = winsorized_percentiles(col("capex_intensity"), False)
    p_gross_margin = winsorized_percentiles(col("gross_margin"), True)
    p_dep_light = winsorized_percentiles(col("depreciation_overhang"), False)
    p_op_delta = winsorized_percentiles(col("op_margin_delta_3y"), True)
    p_sga_delta = winsorized_percentiles(col("sga_ratio_delta_3y"), True)
    p_fcf_margin = winsorized_percentiles(col("fcf_margin"), True)
    p_fcf_consec = winsorized_percentiles(col("fcf_consec"), True)
    p_net_cash = winsorized_percentiles(col("net_cash_to_mcap"), True)

    records: dict[str, dict] = {}
    ranked: list[dict] = []
    quad_counts = {"toll_collector": 0, "levered_landlord": 0, "bystander": 0, "bag_holder": 0, "unlinked": 0}

    tier_default_d = {1: 80.0, 2: 55.0, 3: 45.0, 4: 35.0}

    for st in stocks:
        sym = st["symbol"]
        raw = raw_by_sym[sym]
        ov = overlay.get(sym) or {}
        sec_grp = sec_map[sym]
        stale = _is_stale(ov.get("reviewed"), today) if ov else False
        ov_decay = 0.5 if stale else 1.0

        # --- Pillar A: Depreciation Bomb ---
        if sec_grp == "financial":
            pillar_a = None
        else:
            dep_p = 100.0 if raw.get("dep_overhang_max_flag") else p_dep_over.get(sym)
            pillar_a = _mean_non_null([p_capex_da.get(sym), dep_p, p_capex_int.get(sym), p_capex_gvr.get(sym)])

        # --- Pillar B: Leverage & Funding ---
        if sec_grp == "financial":
            pillar_b = None
        else:
            pillar_b = _mean_non_null([
                p_fund_gap.get(sym), p_nd_ebitda.get(sym), p_debt_yoy.get(sym),
                p_int_cov.get(sym), p_shares.get(sym), p_off_bs.get(sym),
            ])

        # --- Pillar C: Counterparty / Circularity (overlay-only) ---
        pillar_c = None
        cp = ov.get("counterparty")
        if isinstance(cp, dict):
            cp_vals = [cp.get(k) for k in (
                "c1_backlog_concentration", "c2_vendor_financing",
                "c3_equity_for_orders", "c4_take_or_pay_payer", "c5_collateral_fragility",
            ) if _is_num(cp.get(k))]
            if cp_vals:
                pillar_c = round((sum(cp_vals) / (2.0 * len(cp_vals))) * 100.0, 1)

        # --- Pillar D: Revenue Dependence (overlay) ---
        pillar_d = None
        ai_tier = ov.get("ai_tier")
        ai_rev_share = ov.get("ai_revenue_share")
        if _is_num(ai_rev_share):
            pillar_d = round(max(0.0, min(100.0, float(ai_rev_share))), 1)
        elif ai_tier in tier_default_d:
            pillar_d = tier_default_d[ai_tier]

        # --- Pillar E: Market-Implied AI Beta ---
        pillar_e = p_ai_beta.get(sym)

        # --- AI-Relevance Gate (with entry/exit hysteresis) ---
        prior_rel = bool((prior_records.get(sym) or {}).get("ai_relevant"))
        prior_reason = (prior_records.get(sym) or {}).get("ai_gate_reason")
        b_t = raw.get("ai_beta_t")
        b_sh = raw.get("ai_beta_shrunk")

        ai_relevant = False
        ai_gate_reason = "none"
        if ai_tier in (1, 2, 3):
            ai_relevant = True
            ai_gate_reason = "overlay_tier"
        elif _is_num(b_t) and _is_num(b_sh):
            if b_t >= BETA_ENTER_T and b_sh >= BETA_ENTER_SHRUNK:
                ai_relevant = True
                ai_gate_reason = "ai_beta"
            elif prior_rel and prior_reason == "ai_beta" and b_t >= BETA_EXIT_T and b_sh > 0:
                ai_relevant = True
                ai_gate_reason = "ai_beta"

        # --- Composite Bust Exposure ---
        bust_pillars = {"A": pillar_a, "B": pillar_b, "C": pillar_c, "D": pillar_d, "E": pillar_e}
        if sec_grp == "financial":
            eff_w = {"C": 0.40 * ov_decay, "D": 0.30 * ov_decay, "E": 0.30}
        else:
            eff_w = {
                "A": BUST_W["A"],
                "B": BUST_W["B"],
                "C": BUST_W["C"] * ov_decay,
                "D": BUST_W["D"] * ov_decay,
                "E": BUST_W["E"],
            }
            # Fallback: if D missing on a gated ticker, E absorbs D's weight
            if pillar_d is None and pillar_e is not None:
                eff_w["E"] += BUST_W["D"]
                eff_w["D"] = 0.0

        avail_w = sum(w for k, w in eff_w.items() if bust_pillars[k] is not None)
        bust_conf = _confidence_label(avail_w)
        # Allow medium confidence when ai_fundamentals isn't populated yet (B+C+D+E = 75%!)
        ai_bust_score = None
        if ai_relevant and avail_w >= 0.60:
            weighted_sum = sum(bust_pillars[k] * w for k, w in eff_w.items() if bust_pillars[k] is not None)
            ai_bust_score = round(weighted_sum / avail_w, 1)

        # --- Toll Gate (curated-only: T1 must exist in overlay) ---
        tg = ov.get("toll_gate")
        t1_raw = None
        pillar_t1 = None
        if isinstance(tg, dict):
            t1_vals = [tg.get(k) for k in (
                "t1a_exclusivity", "t1b_freshness", "t1c_authority",
                "t1d_ai_monetization", "t1e_embeddedness",
            ) if _is_num(tg.get(k))]
            if t1_vals:
                t1_raw = sum(t1_vals)
                pillar_t1 = round((t1_raw / (2.0 * len(t1_vals))) * 100.0, 1)

        pillar_t2 = _mean_non_null([p_capex_light.get(sym), p_gross_margin.get(sym), p_dep_light.get(sym)])
        pillar_t3 = _mean_non_null([p_op_delta.get(sym), p_sga_delta.get(sym)])
        pillar_t4 = _mean_non_null([p_fcf_margin.get(sym), p_fcf_consec.get(sym), p_net_cash.get(sym)])
        penalty = float(ov.get("disintermediation_penalty") or 0.0) if pillar_t1 is not None else 0.0

        toll_pillars = {
            "T1": pillar_t1, "T2": pillar_t2, "T3": pillar_t3, "T4": pillar_t4,
            "penalty": round(penalty, 1), "t1_raw": t1_raw,
        }
        ai_toll_score = None
        toll_conf = None
        if pillar_t1 is not None:
            tw = {
                "T1": TOLL_W["T1"] * ov_decay,
                "T2": TOLL_W["T2"],
                "T3": TOLL_W["T3"],
                "T4": TOLL_W["T4"],
            }
            t_avail = sum(w for k, w in tw.items() if toll_pillars[k] is not None)
            toll_conf = _confidence_label(t_avail)
            if t_avail >= 0.60:
                t_comp = sum(toll_pillars[k] * w for k, w in tw.items() if toll_pillars[k] is not None) / t_avail
                ai_toll_score = round(max(0.0, t_comp - penalty), 1)

        # --- Quadrant assignment ---
        passes_toll_moat = (
            ai_toll_score is not None
            and ai_toll_score >= TOLL_HIGH_CUTOFF
            and t1_raw is not None
            and t1_raw >= TOLL_MOAT_MIN_RAW
        )
        high_bust = ai_relevant and ai_bust_score is not None and ai_bust_score >= BUST_HIGH_CUTOFF

        if passes_toll_moat:
            quadrant = "levered_landlord" if high_bust else "toll_collector"
        elif ai_relevant and ai_bust_score is not None:
            quadrant = "bag_holder" if high_bust else "bystander"
        elif ai_tier == 4 and pillar_e is not None:
            # Explicit Tier-4 curated narrative watch (e.g., PLTR) shown as bystander if low bust
            quadrant = "bystander"
            ai_relevant = True
            ai_gate_reason = "overlay_tier"
            if avail_w >= 0.60:
                weighted_sum = sum(bust_pillars[k] * w for k, w in eff_w.items() if bust_pillars[k] is not None)
                ai_bust_score = round(weighted_sum / avail_w, 1)
                if ai_bust_score >= BUST_HIGH_CUTOFF:
                    quadrant = "bag_holder"
        else:
            quadrant = "unlinked"

        quad_counts[quadrant] += 1
        combined_conf = _lower_confidence(bust_conf if ai_bust_score is not None else None, toll_conf)

        rec = {
            "symbol": sym,
            "name": st.get("name", sym),
            "sector": st.get("sector"),
            "close": st.get("close"),
            "pct_from_wma": st.get("pct_from_wma"),
            "below_line": bool(st.get("below_line")),
            "fcf_yield": st.get("fcf_yield"),
            "market_cap": st.get("market_cap"),
            "ai_bust_score": ai_bust_score,
            "ai_bust_pillars": bust_pillars,
            "ai_bust_confidence": bust_conf if ai_relevant else "unlinked",
            "ai_toll_score": ai_toll_score,
            "ai_toll_pillars": toll_pillars,
            "ai_toll_confidence": toll_conf or "not_curated",
            "ai_confidence": combined_conf,
            "ai_quadrant": quadrant,
            "ai_relevant": ai_relevant,
            "ai_gate_reason": ai_gate_reason,
            "ai_tier": ai_tier,
            "ai_beta": raw.get("ai_beta"),
            "ai_beta_t": raw.get("ai_beta_t"),
            "ai_beta_shrunk": raw.get("ai_beta_shrunk"),
            "depreciation_overhang": raw.get("depreciation_overhang"),
            "depreciation_overhang_ebit": raw.get("depreciation_overhang_ebit"),
            "capex_to_da": raw.get("capex_to_da"),
            "capex_funding_gap": raw.get("capex_funding_gap"),
            "useful_life_extended": bool(ov.get("useful_life_extended", False)),
            "search_summary": ov.get("search_summary"),
            "overlay": {
                "curated": bool(ov),
                "sources": ov.get("sources") or [],
                "reviewed": str(ov.get("reviewed")) if ov.get("reviewed") else None,
                "stale": stale,
            },
        }
        records[sym] = rec
        if quadrant != "unlinked":
            ranked.append(rec)

    # Sort ranked: Toll Collectors / Levered Landlords / Bag Holders / Bystanders, then by max score
    quad_order = {"bag_holder": 0, "levered_landlord": 1, "toll_collector": 2, "bystander": 3}
    ranked.sort(
        key=lambda r: (
            0 if r["overlay"]["curated"] else 1,
            quad_order.get(r["ai_quadrant"], 9),
            -(max(r["ai_bust_score"] or 0.0, r["ai_toll_score"] or 0.0)),
        )
    )

    return {
        "generated_iso": today.isoformat(),
        "generated_readable": today.strftime("%B %d, %Y"),
        "methodology_version": "0.2",
        "counts": {
            "universe_total": len(stocks),
            "curated_total": sum(1 for r in records.values() if r["overlay"]["curated"]),
            "ranked_total": len(ranked),
            **quad_counts,
        },
        "ranked": ranked,
        "stocks": {sym: rec for sym, rec in records.items() if rec["ai_quadrant"] != "unlinked" or rec["overlay"]["curated"]},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-merge", action="store_true", help="Do not merge slim 5-field summary into stocks.json")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if not STOCKS_FILE.exists():
        print(f"ERROR: {STOCKS_FILE} not found", file=sys.stderr)
        return 1

    stocks_payload = json.loads(STOCKS_FILE.read_text(encoding="utf-8"))
    stocks = stocks_payload.get("stocks", [])
    overlay = yaml.safe_load(OVERLAY_FILE.read_text(encoding="utf-8")) if OVERLAY_FILE.exists() else {}
    overlay = {k.upper(): v for k, v in (overlay or {}).items() if isinstance(v, dict)}

    prior_records = {}
    if OUT_ASSETS.exists():
        try:
            prior_records = json.loads(OUT_ASSETS.read_text(encoding="utf-8")).get("stocks", {})
        except Exception:
            prior_records = {}

    result = compute_all(stocks, overlay, prior_records)
    # Match stocks.json date so site freshness stamps agree
    if stocks_payload.get("generated_iso"):
        result["generated_iso"] = stocks_payload["generated_iso"]
    if stocks_payload.get("generated_readable"):
        result["generated_readable"] = stocks_payload["generated_readable"]

    OUT_ASSETS.parent.mkdir(parents=True, exist_ok=True)
    OUT_STATIC.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(result, separators=(",", ":"))
    OUT_ASSETS.write_text(serialized, encoding="utf-8")
    OUT_STATIC.write_text(serialized, encoding="utf-8")

    if not args.no_merge:
        by_sym = result["stocks"]
        merged = 0
        for st in stocks:
            rec = by_sym.get(st["symbol"])
            if rec:
                st["ai_quadrant"] = rec["ai_quadrant"]
                st["ai_bust_score"] = rec["ai_bust_score"]
                st["ai_toll_score"] = rec["ai_toll_score"]
                st["depreciation_overhang"] = rec["depreciation_overhang"]
                st["ai_confidence"] = rec["ai_confidence"]
                merged += 1
            else:
                st["ai_quadrant"] = "unlinked"
                st["ai_bust_score"] = None
                st["ai_toll_score"] = None
                st["depreciation_overhang"] = None
                st["ai_confidence"] = None
        STOCKS_FILE.write_text(json.dumps(stocks_payload, separators=(",", ":")), encoding="utf-8")
        print(f"Merged slim 5-field AI exposure summary into stocks.json ({merged} gated/curated stocks)")

    c = result["counts"]
    print(
        f"AI Exposure v0.2 complete: {c['ranked_total']} ranked ({c['curated_total']} curated) — "
        f"Toll Collectors: {c['toll_collector']}, Levered Landlords: {c['levered_landlord']}, "
        f"Bag Holders: {c['bag_holder']}, Bystanders: {c['bystander']}, Unlinked: {c['unlinked']}"
    )

    if args.verbose:
        print("\nTop 15 Bag Holders / Levered Landlords (by Bust Score):")
        by_bust = sorted([r for r in result["ranked"] if r["ai_bust_score"] is not None], key=lambda r: r["ai_bust_score"], reverse=True)
        for r in by_bust[:15]:
            print(f"  {r['symbol']:6s} {r['ai_quadrant']:17s} bust={r['ai_bust_score']:5.1f} toll={str(r['ai_toll_score']):>5s} beta={str(r['ai_beta']):>6s} (t={str(r['ai_beta_t']):>5s}) gate={r['ai_gate_reason']}")
        print("\nTop 15 Toll Collectors / Levered Landlords (by Toll Score):")
        by_toll = sorted([r for r in result["ranked"] if r["ai_toll_score"] is not None], key=lambda r: r["ai_toll_score"], reverse=True)
        for r in by_toll[:15]:
            print(f"  {r['symbol']:6s} {r['ai_quadrant']:17s} toll={r['ai_toll_score']:5.1f} bust={str(r['ai_bust_score']):>5s} T1={r['ai_toll_pillars'].get('t1_raw')}/10")

    return 0


if __name__ == "__main__":
    sys.exit(main())
