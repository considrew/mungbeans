# Weekly deep dive: scheduled task

This is the standing instruction for the weekly scheduled Claude session. The
scheduled task's own prompt just points here, so the process can be changed by
editing this file on `main`.

The owner has authorised this run in advance: pick the stock, write the piece,
and push it for publication without asking for confirmation. There is nobody to
answer a question, so do not ask one. If something blocks you, stop, publish
nothing, and say plainly what blocked you in your final message.

## 0. Setup

Work in the `considrew/mungbeans` repository, from the repo root, starting from
the latest `main`:

    git fetch origin main && git checkout -B "$(git branch --show-current)" origin/main
    pip install -q pandas yfinance

Stay on the `claude/*` branch this session was given. Never push to `main`; the
publish workflow does that after its own checks.

If `below-the-line/scripts/deep_dive/submit.py` does not exist on `main`, the
pipeline has not been merged yet: stop and say so.

## 1. Shortlist

    python below-the-line/scripts/deep_dive/select_candidates.py

Takes about three minutes. It writes `.deep-dive/candidates.json`. If it exits
non-zero, stop.

## 2. Write

Read `below-the-line/scripts/deep_dive/prompts/writer.md` and follow it exactly,
start to finish. It covers the pick, the dossier, the research, the page,
`.deep-dive/meta.json`, and the validator loop.

## 3. Edit, in a fresh context

Spawn one subagent (general-purpose) with this prompt and wait for it:

> Read below-the-line/scripts/deep_dive/prompts/editor.md in the repository at
> the current working directory and follow it exactly, start to finish.

The editor must come to the draft cold, which is why it is a separate agent. Do
not do the editor pass yourself, and do not summarise the draft for it.

## 4. Submit

    python below-the-line/scripts/deep_dive/submit.py

This re-runs the validator with the scope check, files the run under
`docs/deep-dive-runs/<TICKER>/`, and commits. If it reports a hold or a
validation failure, fix what it names and run it again; if the editor held the
piece, stop there and report the reason.

Then push the branch:

    git push -u origin "$(git branch --show-current)"

The push triggers the "Weekly Deep Dive" GitHub workflow, which validates
again, merges the piece to `main` and deploys the site. Do not open a pull
request.

## 5. Confirm

Check that the workflow run for your push succeeded (GitHub Actions tools, the
"Weekly Deep Dive" workflow). It takes roughly five minutes. If it failed, read
the failing step's log, fix the cause if it is in the article or meta.json,
re-run `submit.py`, and push again. Two attempts at most.

Finish with a short report: ticker, title, verdict, price at publish, the live
URL (`https://mungbeans.io/deep-dives/<ticker lowercase>/`), the three other
candidates you considered and why you passed on them, and anything the editor
changed that the owner should look at when reviewing.
