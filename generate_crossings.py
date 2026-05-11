#!/usr/bin/env python3
"""One-time script to generate crossings.json from git history."""
import json
import subprocess

prev_data = json.loads(
    subprocess.check_output(["git", "show", "4c44ce1:below-the-line/assets/data/stocks.json"])
)
curr_data = json.load(open("below-the-line/assets/data/stocks.json"))

prev = {s["symbol"]: s for s in prev_data["stocks"]}
nb, nr = [], []

for s in curr_data["stocks"]:
    p = prev.get(s["symbol"])
    if not p:
        continue
    if s.get("below_line") and not p.get("below_line"):
        nb.append({
            "symbol": s["symbol"],
            "name": s.get("name", ""),
            "pct_from_wma": s.get("pct_from_wma", 0),
            "rsi_14": s.get("rsi_14", 0),
        })
    elif not s.get("below_line") and p.get("below_line"):
        nr.append({
            "symbol": s["symbol"],
            "name": s.get("name", ""),
            "pct_from_wma": s.get("pct_from_wma", 0),
        })

nb.sort(key=lambda x: x["pct_from_wma"])
nr.sort(key=lambda x: x["pct_from_wma"])

crossings = {
    "date": "2026-05-10",
    "date_display": "May 10, 2026",
    "blog_slug": "2026-05-10-weekly-signal-report",
    "newly_below": nb,
    "newly_recovered": nr,
}

with open("below-the-line/assets/data/crossings.json", "w") as f:
    json.dump(crossings, f, indent=2)

print(f"{len(nb)} below, {len(nr)} recovered")
