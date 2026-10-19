---
name: incident-writeup
description: Write incident resolution records in plain English — "What happened / What we did / Outcome" timeline style. Use when writing or revising incident resolution text for GitHub issues, incident.json resolution_note fields, or incident reports.
---

# incident-writeup — plain-English resolution records

Companion to `incident-handle`. Invoke when documenting how an incident was
resolved — in a GitHub issue, an `incident.json` `resolution_note`, or any
ops record a non-specialist might read later.

## Structure (required)

Three parts, in this order. For longer resolutions use the bullet/numbered
form; for short ones a 2–4 sentence version of the same shape is fine.

```
**What happened:**
- <the finding, in one line — what the check flagged and why it looks wrong>
- <the investigation — what we checked and what we found>
- <(if the first fix was wrong) — say so plainly: "First fix (too weak): ...">

**What we did:**
1. <action, in order>
2. <action>
3. <action>

**Outcome:** <end state, residual risk, and anything left open>
```

## Vocabulary rules

- Prefer everyday words: "the vendor still serves this bar" not "vendor-
  consistent"; "marked it as harmless" not "suppressed"; "re-downloaded" or
  "backfilled" not "re-fetched"; "removed" not "purged" (unless the purge is
  the load-bearing semantic, then say "removed (purged)").
- Name the vendor once as plain `Yahoo` or `the vendor (Yahoo)`.
- Keep exact identifiers a reader must look up — command/action names
  (`delete_daily_rows`, `on_request_load`, `metadata_load`), config keys
  (`ingestion.exclude_rows`, `acknowledged_delisted`), file names, task
  codes (INC-T6), table names (`daily_close`, `weekly_close`). Never
  paraphrase these.
- Translate jargon once, inline, on first use — e.g. "a ghost bar the
  vendor keeps for a dead stock", "the weekly rollup tables".
- Be honest about uncertainty: "we couldn't prove it was safe" /
  "cause unknown — removed but logged, so it can be restored".
- Keep numbers concrete: row counts, dates, windows.

## Tone

- Short sentences. A reader who doesn't know this codebase should follow
  the story: flagged → checked → found → fixed → verified.
- Admit course-corrections ("the first fix was too weak") — they are the
  most instructive part.
- End with what the reader should believe is true now ("WBD data ends at
  2026-10-02, the last real bar") plus anything still open or reversible.

## Reference example (approved style — WBD 2026-10-05 ghost bar)

**What happened:**
- The health check flagged a WBD bar on 2026-10-05: Open price = 0, tiny
  volume (40 shares). That looks broken.
- We checked the vendor (Yahoo). It serves the same broken-looking bar —
  it's the last day WBD traded before delisting. So the data isn't corrupt;
  it's a "ghost" bar the vendor keeps for a dead stock.
- First fix (too weak): we wrote it off as harmless in the suppression file.

**What we did:**
1. Deleted the row via `delete_daily_rows` (new command from INC-T6).
2. Added `(WBD, 2026-10-05)` to `ingestion.exclude_rows` in config — the
   loader now drops this pair on every load path, forever.
3. Rebuilt weekly tables (INC-T7) — the bad bar had already leaked into
   `weekly_close` and `weekly_indicators`; the rebuild removed it.

**Outcome:** WBD data ends at 2026-10-02 (last real bar). The 10-05 bar is
treated as "unknown, not trusted" — removed but logged, so it can be
restored if it's ever proven real. WBD was later confirmed delisted and
its history removed via `exclude_tickers`.

## Where it applies

- GitHub issue Resolution sections (the durable incident record).
- `incident.json` `resolution_note` fields in the local store — keep them
  to the same shape (the short form is fine there).
- Report.md / TICKET.md closing notes.
