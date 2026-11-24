# summarize_rra

Summarize all dated weekly RRA analysis reports in `docs/rra/` into a
single `| Date | Weekly summary |` markdown table saved to
`docs/rra/weekly_summary.md`. Use when the user asks for a summary
table / digest / roll-up of the weekly RRA reports, or after new dated
reports are added and the roll-up needs regenerating.

Repo: `B:\projects\stock-ingestion`
Companion skill: `analyze_rra` (produces the per-week reports this skill
summarizes).

## Input contract

- `docs/rra/*_YYYY-MM-DD_analysis_report.md` — dated reports only.
  Undated `*_analysis_reportYYYYMMDD.md` files are SKIPPED (they are
  same-date duplicates of the dated flagship report).
- Each report is expected to carry the `analyze_rra` enrichment contract:
  a `> Weekly read:` (or `> Read:`) line under §1 Quadrant Census and a
  `**Net call:**` line under §7. Reports missing either are flagged by
  the script — read that report in full before writing its row.

## Output contract

`docs/rra/weekly_summary.md`:

- Title: `# RRA Weekly Summary Table — ETF Dashboard vs SPY`
- One-line header: report count, date range, lookback.
- Arc recap: 1–2 lines naming each regime phase in order plus the
  inflection dates (e.g. "energy/commodity May–Jun → semis/growth
  Jul–Aug → metals inflection 08-21 → healthcare/defensive Sep–Nov").
- Table: `| Date | Weekly summary |`, rows ascending by date.

## Procedure

1. **Digest** — extract per-week source material:
   ```
   python .devin/skills/summarize_rra/summarize_rra.py --digest
   ```
   Prints per-date JSON: `weekly_read`, `net_call`, `census`, plus a
   `complete` flag. Incomplete rows are listed on stderr.
2. **Draft** — emit the skeleton table:
   ```
   python .devin/skills/summarize_rra/summarize_rra.py --out docs/rra/weekly_summary.md
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

## Index mode — report_index.md (REQUIRED, deterministic)

`report_index.md` is a generated index over the same dated-report set.
It is **not** enriched — it must always reflect the directory exactly:

```
python .devin/skills/summarize_rra/summarize_rra.py --index docs/rra/report_index.md
# --index, --out and --digest are independent outputs and may be combined:
python .devin/skills/summarize_rra/summarize_rra.py \
    --out docs/rra/weekly_summary.md --index docs/rra/report_index.md
```

- Output: `# RRA Report Index — ETF Dashboard vs SPY` + a
  `| Date | Report | Lead | Impr | Weak | Lag |` table (report name is a
  markdown link; quadrant counts come from each report's census line).
- Link targets are the PUBLISHED Pages pages — dated reports ship as
  styled `.html`, so each link is absolute:
  `https://vialliw.github.io/Relative-Rotation-Analysis-Dashboard/report/<same-stem>.html`
  (link text stays the `.md` filename). Only dated `*_analysis_report.md`
  rows are linked this way.
- The weekly pipeline (`scripts/rrg_weekly_pipeline.py --generate`) runs
  this step **unconditionally** after the summary check, so additions
  and deletions are reflected on every run. Never edit the file by hand.
- `report_index.md` bypasses the publish enrichment gate (no
  `> Read:`/`Net call:` markers needed) — it is always shippable.

## Rules / gotchas

- Regenerate the WHOLE table when new weeks land — never append rows to
  a stale arc recap.
- `weekly_summary.md` (and any other non-`*_analysis_report.md` file in
  the directory) is an output, not an input; the glob excludes it.
- Rows flagged incomplete mean the source report predates the enrichment
  contract — read the full report and summarize manually.
- Pure analysis task — no `src/`/`tests/` changes; the orchestrator may
  run it directly (no B/C/D cycle needed).
