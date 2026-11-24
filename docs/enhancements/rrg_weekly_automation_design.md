# RRG Weekly Report Automation — Design (steps 2–5)

Status: approved by user 2026-11-16 (timing = Fri 22:15, implement via
agents); **enrichment engine updated to auto via headless local
`devin -p` 2026-11-23** (was semi-auto); **dated reports publish as
styled `.html` (RRG-HTML-T1, 2026-11-24)** — `.md` stays the internal
source of truth.

## Goal

Automate the deterministic halves of the weekly RRG report workflow that
ships `docs/rra/` artifacts to
`vialliw/Relative-Rotation-Analysis-Dashboard:report/` — dated
`*_analysis_report*.md` publish as rendered `.html`,
`weekly_summary.md`/`report_index.md` publish as `.md`:

- **Step 2 (generate JSON)** — automated
- **Step 3 (analyze → report)** — draft automated; narrative enrichment
  automated via headless `devin -p` (Phase B′ below); manual Devin
  session remains a supported fallback
- **Step 4 (summarize)** — draft automated; enriched rows automated via
  the same headless session
- **Step 5 (publish)** — automated, gated on enrichment markers

A headless local Devin session enriches drafts shortly after generation
(the `analyze_rra` / `summarize_rra` skills already encode the
contract); the publish phase ships only files that pass the enrichment
gate, so a failed or skipped enrichment run just delays shipping — it
can never ship a draft.

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

Companion task `StockIngestion-RRGEnrich` (Phase B′, see below):

- Triggers: **Fri 22:45** (30 min after the 22:15 RRGWeekly trigger —
  generation has finished and the state file is in place) plus
  **Sat + Sun 08:30** (30 min *before* the 09:00 publish retries, so an
  enrichment run lands its markers in time for the same-morning publish
  instead of waiting a week).
- `StartWhenAvailable`, user / Interactive / Limited, `pythonw.exe`,
  WorkingDirectory = repo root — same conventions as RRGWeekly.
  The wrapper exits cheaply without spawning once the markers verify, so
  repeat triggers are near-no-ops.

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

### Phase B′ — enrichment (AUTO, headless `devin -p`)

`scripts/rrg_enrich_headless.py` (stdlib-only wrapper) runs on the
`StockIngestion-RRGEnrich` triggers:

1. Reads `docs/rra/.rrg_pipeline_state.json`. No-ops (exit 0) when the
   report is enriched by ANY evidence — the `report_enriched` flag OR
   the report file's on-disk `> Weekly read:`/`**Net call:**` markers
   (the flag can lag a manual enrichment until the next `--generate`;
   the markers are what the publish gate actually reads) — **and**
   `weekly_summary.md` already covers `resolved_date`.
2. Otherwise takes `devin_blackboard/locks/rrg_enrich.lock`
   (`O_CREAT | O_EXCL`, stale >10 min recovered per `poll_tasks.py`
   conventions — a held lock exits without spawning) and spawns ONE
   headless local session: `devin.exe -p "<ENRICHMENT PROMPT>"` via the
   bundled CLI at `%LOCALAPPDATA%\Programs\Devin\resources\app\extensions\windsurf\devin\bin\devin.exe`
   (fallback `devin` on PATH), cwd = repo root, killed after 30 min.
3. The prompt embeds `{json_path}`/`{report_path}`/`{resolved_date}`
   from the state file and instructs the session to: (a) run
   `analyze_rra.py` only if the report draft is missing; (b) enrich
   `> Read:`/`> Weekly read:` + `**Net call:**` per the `analyze_rra`
   contract (tables verbatim); (c) add the new `weekly_summary.md` row +
   arc recap per `summarize_rra` and regenerate `report_index.md` via
   `--index`; (d) edit only `docs/rra/*.md` + `EVENTS.log`; (e) stop
   once both markers verify.
4. Logs `[ENRICH-T1]` spawn/pid/exit/marker-verification lines to
   `EVENTS.log`. **Non-blocking**: a non-zero exit, spawn failure, or
   timeout only logs a warning — the wrapper always exits 0 and the
   publish gate keeps holding drafts.

Manual enrichment in an interactive Devin session remains a supported
fallback (the skills contract is unchanged).

### Phase C — publish (`--publish`)

Artifact architecture: **`.md` = internal source** (draft → enrich in
`docs/rra/`); **`.html` = publish artifact** for dated reports only —
`weekly_summary.md` and `report_index.md` stay `.md` and publish
(Jekyll renders them on Pages; no name collisions once the report `.md`
are deleted remotely).

1. **Enrichment gate (on `.md`, unchanged)** — a dated report is
   publishable only if it contains both `> Weekly read:` (or `> Read:`)
   and `**Net call:**`; `weekly_summary.md` is publishable only if it
   contains the resolved date of each dated report it claims to cover.
   Unenriched artifacts are skipped (never shipped as drafts).
2. **Render** — every gated `*_analysis_report*.md` is rendered to a
   same-stem `.html` in `docs/rra/` via
   `.devin/skills/analyze_rra/rra_html.py` (mistune + post-processing:
   sortable numeric-aware tables, green/red quadrant-crossing and tilt
   rows, colored census-grid cells — approved reference
   `.tmp/rra_report_2026-11-20.html`). The `.html` ships INSTEAD of the
   `.md`.
3. Preflight `git ls-remote https://github.com/vialliw/Relative-Rotation-Analysis-Dashboard`
   — fail loudly (log + toast) if auth/connectivity is broken.
4. Clone/pull the remote into `.tmp/rrg-dash` (inside workspace),
   **mirror-sync** the publish set → `report/`: add/update shipped
   files; deletions are decided per remote file:

   | Remote file | Condition | Action |
   |---|---|---|
   | `.md` in publish set (`weekly_summary.md`, `report_index.md`) | — | kept (synced) |
   | `.md` | same-stem `.html` in publish set | **deleted — superseded** (how dated reports migrate `.md` → `.html`; never gated by `--prune`) |
   | `.md` | same-named local `.md` exists | **kept** — a local `.md` that FAILED the gate still protects its remote `.md` |
   | `.md` | remote-only (no local `.md`) | dated name → **held unless `--prune`**; non-dated → deleted |
   | `.html` | in publish set | kept (synced) |
   | `.html` | same-stem local `.md` exists | **kept — parity protection**: a gate-failed local `.md` protects its remote `.html` exactly as it protects its remote `.md`; a regressed/removed draft must never 404 the last good published report |
   | `.html` | remote-only (no same-stem local `.md`) | dated name → **held unless `--prune`**; non-dated → deleted |

   **`--prune` circuit breaker:** deleting a *dated* report artifact
   (`*_YYYY-MM-DD_analysis_report.(md|html)`) that has no local source
   requires the explicit `--prune` flag on `--publish`. Without it such
   files are HELD — printed `held <name> (<reason>; --prune to delete)`,
   recorded via `log_event`, and counted in the publish summary/toast
   (`N remote deleted, M held`). Superseded `.md` and non-dated
   remote-only files are NOT gated. `.json` files are NOT synced and
   never deleted.

   **Recovery path:** the remote is a git repo — any wrong deletion or
   bad publish is recovered via `git revert` of the publish commit on
   `vialliw/Relative-Rotation-Analysis-Dashboard` (remote history is the
   rollback; held files simply wait for a `--prune` run or a restored
   local source).
5. If diff → commit authored `devin-ai-integration[bot]
   <158243242+devin-ai-integration[bot]@users.noreply.github.com>` →
   push. Remote `auto_index.yaml` regenerates `REPORT_INDEX.md`.
6. Cleanup temp clone; log to `devin_blackboard/EVENTS.log`; toast via
   `scripts/notify_done.ps1` only when something was actually published.

### Scheduling — `scripts/register_rrg_weekly.ps1` + `scripts/register_rrg_enrich.ps1`

Registers the tasks above via `Register-ScheduledTask` /
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
| Enrichment session fails / times out / `devin` missing | `[ENRICH-T1]` WARNING in `EVENTS.log`, wrapper still exits 0 — publish gate holds the draft; retried on the next RRGEnrich trigger |
| Two enrich runs at once | `locks/rrg_enrich.lock` (`O_CREAT|O_EXCL`) — second run exits without spawning; stale lock (>10 min) auto-recovered |
| Push auth/network failure | logged + toast; remote untouched; retried next trigger |
| `.tmp/rrg-dash` dirty/leftover | wiped and re-cloned each publish |

## Decisions locked

- Timing: Fri 22:15 primary (+ Sat/Sun 09:00 publish retries); enrichment on Fri 22:45 + Sat/Sun 08:30.
- Dated reports publish as rendered `.html` (styled/sortable via `rra_html.py`); `weekly_summary.md` + `report_index.md` publish as `.md`; `.json` never synced; commit author = devin-ai-integration[bot].
- `report_index.md` links dated reports to their absolute Pages `.html` URLs (`https://vialliw.github.io/Relative-Rotation-Analysis-Dashboard/report/<stem>.html`).
- Publish deletion parity + circuit breaker: a gate-failed local `.md` protects its remote `.md` AND same-stem `.html`; dated remote-only artifacts are HELD unless `--prune`; remote recovery is `git revert` on the dashboard repo.
- No `src/` changes; all new code lives in `scripts/` + `tests/` + `.devin/skills/`.
