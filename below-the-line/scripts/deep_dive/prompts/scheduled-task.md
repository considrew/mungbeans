# Weekly deep dive: scheduled task

This is the standing instruction for the weekly scheduled Claude session. The
scheduled task's own prompt just points here, so the process can be changed by
editing this file on `main`.

The owner has authorised this run in advance: pick the stock, write the piece,
and push it for publication without asking for confirmation. There is nobody to
answer a question, so do not ask one. If something blocks you, stop, publish
nothing, and say plainly what blocked you in your final message.

## 0. Setup

Work in the `considrew/mungbeans` repository, from the repo root. The
repository should be attached to this session with push access. Check that
first, before spending any effort:

    git push --dry-run origin HEAD:refs/heads/claude/deep-dive-access-check

If that is refused (the git proxy reports the repository is not in the
session's authorized set), stop now and report: "the scheduled task has no
repository attached; add considrew/mungbeans to it in the Claude scheduled
tasks settings." Do not write a piece that cannot be published.

Work on a `claude/*` branch cut from the latest `main`. If the session was
given one, use it. If you are on `main`, create one:

    git fetch origin main
    BR=$(git branch --show-current); case "$BR" in claude/*) ;; *) BR="claude/weekly-deep-dive-$(date -u +%Y-%m-%d)";; esac
    git checkout -B "$BR" origin/main
    python -m pip install -q pandas yfinance

Use `python -m pip`, so the packages land in the interpreter that runs the
scripts. Never push to `main`; the publish workflow does that after its own
checks.

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

If you have GitHub tools that can read Actions runs, check that the "Weekly
Deep Dive" run for your push succeeded. It takes roughly five minutes. If it
failed, read the failing step's log, fix the cause if it is in the article or
meta.json, re-run `submit.py`, and push again. Two attempts at most. If you
have no such tools, say in your report that the publish run was not confirmed;
a failed publish opens a GitHub issue on its own.

Finish with a short report: ticker, title, verdict, price at publish, the live
URL (`https://mungbeans.io/deep-dives/<ticker lowercase>/`), the three other
candidates you considered and why you passed on them, and anything the editor
changed that the owner should look at when reviewing.
