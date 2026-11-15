#!/usr/bin/env python3
"""Hand a finished deep dive to the publish workflow.

Run by the scheduled Claude session once the writer and editor passes are done.
Validates, files the run record (meta, dossier, shortlist) under
docs/deep-dive-runs/<TICKER>/ so the workflow can re-validate it and so there is
a permanent record of what the piece was written from, and commits both.

It does not push. Pushing the session's claude/* branch is what triggers
.github/workflows/weekly-deep-dive.yml, which gates, merges to main and deploys.

Usage:
    python submit.py [--skip-shortlist]
"""
from __future__ import annotations

import shutil
import subprocess
import sys

from common import CANDIDATES, META, REPO, RUNS, WORK, dossier_path, load_json, page_path

HERE = "below-the-line/scripts/deep_dive"


def main() -> int:
    hold = WORK / "HOLD"
    if hold.exists():
        print("HELD by the editor. Nothing submitted.\n")
        print(hold.read_text())
        return 1

    flags = [a for a in sys.argv[1:] if a == "--skip-shortlist"]
    gate = subprocess.run([sys.executable, f"{HERE}/validate.py", "--check-scope", *flags], cwd=REPO)
    if gate.returncode != 0:
        print("\nValidation failed. Nothing submitted.")
        return 1

    meta = load_json(META)
    ticker = meta["ticker"].upper()
    dossier = load_json(dossier_path(ticker))
    run = RUNS / ticker
    run.mkdir(parents=True, exist_ok=True)
    for src in (META, CANDIDATES, dossier_path(ticker)):
        if src.exists():
            shutil.copy2(src, run / src.name)

    article = page_path(ticker).relative_to(REPO)
    msg = f"Weekly deep dive: {ticker} ({dossier['publish']['price_date']})"
    for cmd in (["git", "add", str(article), str(run.relative_to(REPO))],
                ["git", "commit", "-q", "-m", msg]):
        res = subprocess.run(cmd, cwd=REPO)
        if res.returncode != 0:
            print(f"git failed: {' '.join(cmd)}")
            return 1

    print(f"\nCommitted: {msg}")
    print("Now push this branch. The push triggers the publish workflow.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
