---
title: "The Bean Score, Audited: Six Weeks of Real Signal Data"
date: 2026-08-08
draft: true
tags: ["deep-dive", "bean-score", "methodology"]
description: "The Bean Score's accuracy tracker was supposed to accumulate weekly. A cron bug wrote seven junk daily snapshots in June, then a fix overcorrected it to monthly. Both are repaired. Here's what the clean data shows so far, what the tracker will report at thirteen weeks, and how the negative-FCF expansion changed coverage."
verdict: "methodology"
---

<!-- QUEUED: after the GENI review. Use the mungbeans-deep-dive skill.
Outline agreed with the author:

1. Own the data problem in the open, in the site's voice: the June cron bug
   (daily snapshots for a week), the overcorrection (monthly), the prune to
   Saturday-only snapshots, and the restored weekly cadence. The record of
   reasoning includes the plumbing failures.
2. What exists now: six clean weekly snapshots, 1,888 stocks covered
   (including the negative-FCF expansion from June), dislocation score
   family (yield, drawdown, sector-relative, buyback, insider, earnings
   quality).
3. Early reads, clearly labeled as early: score distribution, the +2sigma
   cohort's forward behavior so far, at least one miss discussed at the
   same length as any hit.
4. What the tracker reports at 13 weeks and the pre-committed standard:
   win rates by sigma threshold vs SPY over the same window, published
   whether flattering or not.
-->
