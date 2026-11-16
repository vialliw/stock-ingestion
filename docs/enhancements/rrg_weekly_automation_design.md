# RRG Weekly Report Automation — Design (steps 2–5, semi-auto)

Status: approved by user 2026-11-16 (enrichment = semi-auto, timing = Fri 22:15, implement via agents)

## Goal

Automate the deterministic halves of the weekly RRG report workflow that
ships `docs/rrg/*.md` to `vialliw/Relative-Rotation-Analysis-Dashboard:report/`:

- **Step 2 (generate JSON)** — automated
- **Step 3 (analyze → report)** — draft automated; narrative enrichment manual
- **Step 4 (summarize)** — draft automated; enriched rows manual
- **Step 5 (publish)** — automated, gated on enrichment markers

User enriches drafts in a Devin session whenever convenient (the
`analyze_rrg` / `summarize_rrg` skills already encode the contract); the
publish phase ships only files that pass the enrichment gate.

## Trigger

New scheduled task `StockIngestion-RRGWeekly`:

- Triggers: **Fri 22:15** (buffer after the 21:30 `StockIngestion-DailyLoad`,
  which ends with `_weekly_bridge → run_weekly_refresh()` rebuilding
  `weekly_close`) plus **Sat + Sun 09:00** (cheap no-op publish attempts —
  catches weekend enrichment without waiting a week).
- `StartWhenAvailable` = true — a missed Friday run fires on next boot;
  `as_of` resolution still lands on the correct last-Friday bar.
- Principal: user / Interactive / Limited, `pythonw.exe` interpreter,
  WorkingDirectory = repo root (same conventions as StockIngestion-DailyLoad).

Every trigger runs the SAME command (`--all`); idempotency makes weekend
runs no-ops when nothing changed.

## Pipeline — `scripts/rrg_weekly_pipeline.py`

Subcommands: `--generate`, `--publish`, `--all` (default), `--dry-run`
(for tests/manual inspection — prints intended actions, exits 0).

### Phase A — generate (`--generate`)

1. `as_of` = last Friday (today if Friday, else prior Friday).
2. Call `generate_rrg_dashboard(config, "ETFs", as_of=as_of,
   json_only=True, output_dir="docs/rrg")` — snaps to the last valid
   weekly bar ≤ as_of; returns `json_path`. Holiday/failed-load weeks
   resolve to the previous week's bar → same file → harmless overwrite.
3. Resolve `report_path` = prompt stem with `prompt → report`.
   - If the report file exists AND contains `> Weekly read:` → already
     enriched → leave untouched.
   - Else if absent → run `.devin/skills/analyze_rrg/analyze_rrg.py
     <json> --out <report>` → unenriched draft flagged `needs_enrichment`.
4. `docs/rrg/weekly_summary.md`: create via `summarize_rrg.py --out`
   ONLY if the file does not exist — never overwrite (the enrich pass
   owns it; overwriting would clobber enriched rows).
5. Write `docs/rrg/.rrg_pipeline_state.json`:
   `{as_of, resolved_date, prompt_json, report_md, report_enriched,
   summary_has_date, generated_at}` — the state file is what a later
   Devin session reads to know "enrich X, refresh summary, then publish".

### Phase B — enrichment (MANUAL, unchanged)

User opens a Devin session and asks to run the `analyze_rrg` /
`summarize_rrg` skills — enrich `> Read:` / `> Weekly read:` /
`**Net call:**` per contract, add the new summary row + arc line.

### Phase C — publish (`--publish`)

1. **Enrichment gate** — a dated report is publishable only if it
   contains both `> Weekly read:` (or `> Read:`) and `**Net call:**`;
   `weekly_summary.md` is publishable only if it contains the resolved
   date of each dated report it claims to cover. Unenriched artifacts
   are skipped (never shipped as drafts).
2. Preflight `git ls-remote https://github.com/vialliw/Relative-Rotation-Analysis-Dashboard`
   — fail loudly (log + toast) if auth/connectivity is broken.
3. Clone/pull the remote into `.tmp/rrg-dash` (inside workspace),
   **mirror-sync** `docs/rrg/*.md` → `report/` (add/update changed,
   delete remote-only `.md` files — remote `report/` should equal local
   set; `.json` files are NOT synced).
4. If diff → commit authored `devin-ai-integration[bot]
   <158243242+devin-ai-integration[bot]@users.noreply.github.com>` →
   push. Remote `auto_index.yaml` regenerates `REPORT_INDEX.md`.
5. Cleanup temp clone; log to `devin_blackboard/EVENTS.log`; toast via
   `scripts/notify_done.ps1` only when something was actually published.

### Scheduling — `scripts/register_rrg_weekly.ps1`

Registers the task above via `Register-ScheduledTask` /
`Set-ScheduledTask`, preferring `pythonw.exe`, matching
`register_daily_task.ps1` conventions (documented trigger caveat: use
the multi-trigger form, not `/sc DAILY`).

## Failure handling

| Failure | Behavior |
|---|---|
| Friday load failed / PC off | `as_of` snaps to prior Friday → same artifacts → publish no-ops; next good week self-heals |
| Holiday-shortened week | snaps to Thursday bar; filename date = actual bar date |
| Phase A crash | logged, exit≠0, state file untouched → retried next trigger |
| Enrichment never done | publish gate blocks drafts indefinitely — no deadline, reports just wait |
| Push auth/network failure | logged + toast; remote untouched; retried next trigger |
| `.tmp/rrg-dash` dirty/leftover | wiped and re-cloned each publish |

## Decisions locked

- Enrichment engine: semi-auto (no headless `devin -p`, no LLM API key).
- Timing: Fri 22:15 primary (+ Sat/Sun 09:00 publish retries).
- md-only sync; commit author = devin-ai-integration[bot].
- No `src/` changes; all new code lives in `scripts/` + `tests/`.
