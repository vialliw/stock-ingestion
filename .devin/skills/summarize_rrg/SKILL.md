# summarize_rrg

Summarize all dated weekly RRG analysis reports in `docs/rrg/` into a
single `| Date | Weekly summary |` markdown table saved to
`docs/rrg/weekly_summary.md`. Use when the user asks for a summary
table / digest / roll-up of the weekly RRG reports, or after new dated
reports are added and the roll-up needs regenerating.

Repo: `B:\projects\stock-ingestion`
Companion skill: `analyze_rrg` (produces the per-week reports this skill
summarizes).

## Input contract

- `docs/rrg/*_YYYY-MM-DD_analysis_report.md` — dated reports only.
  Undated `*_analysis_reportYYYYMMDD.md` files are SKIPPED (they are
  same-date duplicates of the dated flagship report).
- Each report is expected to carry the `analyze_rrg` enrichment contract:
  a `> Weekly read:` (or `> Read:`) line under §1 Quadrant Census and a
  `**Net call:**` line under §7. Reports missing either are flagged by
  the script — read that report in full before writing its row.

## Output contract

`docs/rrg/weekly_summary.md`:

- Title: `# RRG Weekly Summary Table — ETF Dashboard vs SPY`
- One-line header: report count, date range, lookback.
- Arc recap: 1–2 lines naming each regime phase in order plus the
  inflection dates (e.g. "energy/commodity May–Jun → semis/growth
  Jul–Aug → metals inflection 08-21 → healthcare/defensive Sep–Nov").
- Table: `| Date | Weekly summary |`, rows ascending by date.

## Procedure

1. **Digest** — extract per-week source material:
   ```
   python .devin/skills/summarize_rrg/summarize_rrg.py --digest
   ```
   Prints per-date JSON: `weekly_read`, `net_call`, `census`, plus a
   `complete` flag. Incomplete rows are listed on stderr.
2. **Draft** — emit the skeleton table:
   ```
   python .devin/skills/summarize_rrg/summarize_rrg.py --out docs/rrg/weekly_summary.md
   ```
3. **Enrich (REQUIRED)** — rewrite every row in place: 2–3 sentences
   covering (a) the week's regime character / breadth, (b) the key
   movers or signals, (c) the call. Keep ticker names and velocities
   verbatim. Fill the arc-recap line from the digest's cross-week
   continuity (regime handoffs, inflection weeks, streak gains/losses).
   Do NOT ship the raw draft — the weekly-read + net-call concat is
   source material, not the summary.
4. **Log** — append the run to `devin_blackboard/EVENTS.log` and update
   `STATE.md` per AGENTS.md.

## Rules / gotchas

- Regenerate the WHOLE table when new weeks land — never append rows to
  a stale arc recap.
- `weekly_summary.md` (and any other non-`*_analysis_report.md` file in
  the directory) is an output, not an input; the glob excludes it.
- Rows flagged incomplete mean the source report predates the enrichment
  contract — read the full report and summarize manually.
- Pure analysis task — no `src/`/`tests/` changes; the orchestrator may
  run it directly (no B/C/D cycle needed).
