# Plan — Deliberate duckdb 2.0 Upgrade

Status: **proposed** (2026-11-22). Current pinned state: `duckdb>=1.5,<2` in
`requirements.txt`, system site-packages has 1.5.6, Flight Server healthy.

## Why now

duckdb 2.0 keeps arriving *accidentally*: it leaked into user site-packages
twice already (11-04 with `adbc_driver_duckdb`, again 11-18), and each time a
Flight Server restart absorbed it and broke the pipeline — all `do_get` reads
dead, `daily_load` crashing mid-write at `register()`. Two emergency
remediations (11-08, 11-22) only restored 1.5.6; the pin does nothing while
user site can shadow system site. Upgrading deliberately — after making the
code dual-compatible — removes this recurring outage vector permanently.

## Verified breaking changes (duckdb 2.0.0, probed live 2026-11-22)

The 11-08 eval already measured the surface: **362/2054 tests fail under 2.0,
collapsing to 3 root causes.** Confirmed details:

### B1 — Arrow result API removed (3 sites)

`conn.execute(q)` results no longer expose `fetch_arrow_table()` or `.arrow()`;
they expose `to_arrow_table()` / `to_arrow_reader()`. A `DuckDBPyRelation`
(`conn.sql(q)`) still has `.arrow()` → `RecordBatchReader` on **both** 1.5.6
and 2.0.0 (verified live on each).

| Site | Current call |
|---|---|
| `src/db.py:417` (`execute_query` — the `do_get` path) | `conn.execute(query).fetch_arrow_table()` |
| `src/ingestion.py:2109` (`snapshot()` inside DML `create_as`) | `cur.execute(sql).fetch_arrow_table()` |
| `src/db_challenger.py:803` (challenger `execute_query`) | `conn.execute(query).arrow()` + reader→table normalize |

**Fix (dual-compatible):** `conn.sql(query).arrow()` + the existing
`isinstance(result, pa.RecordBatchReader) → read_all()` normalization.
Identical result on 1.5.6 and 2.0 — no version sniffing needed.

### B2 — `register()` rejects connections with pending results (10 sites)

duckdb 2.0 raises `ResourceInUse` on `register()` when the connection holds an
unfetched result. Every write-path `execute()` leaves a pending result behind,
so the *next* call's `register()` crashes — this is what killed the 11-06
`daily_load` at `db.py:447`.

Sites (`.register(` count = 10): `src/db.py` ×3 (staging_df / staging_metadata /
staging_fear_greed on `self.conn`), `src/db_challenger.py` ×3,
`src/ingestion.py` ×2 (dml_stage, dml_put_stage on `cur`),
`src/signal_analysis.py` ×1, `src/signal_pipeline.py` ×1.

**Fix:** run each register→execute→unregister unit on a **dedicated cursor**
(`cur = conn.cursor()` — a fresh cursor has no pending results), and consume
results where `unregister()` proves to trip the same rule. Cursor use also
improves isolation on the shared write connection. Executor to verify
empirically in the 2.0 venv whether `unregister()` is affected.

### B3 — One `TypeError` — site unknown

The 11-08 eval recorded 1 TypeError-rooted failure group. Phase 0 repro will
locate it.

## Phase 0 — Reproduce (read-only, no repo changes)

1. `python -m venv %TEMP%\duckdb2env` (or `pip install --target`), install
   `duckdb==2.0.x` (latest 2.x patch — check `pip index versions duckdb`) plus
   the repo's other requirements.
2. Run the full suite under it; collect the failure list.
3. Confirm the 3 root causes; locate the TypeError site; test whether
   `unregister()` also enforces the pending-result rule.
4. Output: updated site list appended to this plan / EVENTS.log.

## Phase 1 — Dual-compat code changes (Executor B via blackboard)

- Apply the B1 arrow fix at the 3 sites (relation-`.arrow()` pattern).
- Apply the B2 cursor-per-write-unit fix at the 10 register sites.
- Fix the B3 TypeError.
- **Acceptance:** the full suite must pass under **both** `duckdb==1.5.6` and
  `duckdb==2.0.x` (two env runs — dual-compat is the gate, not just 2.0);
  `python scripts/check_loops.py --src-dir src` exit 0; R1A-T1 RawClose
  durability tests are the canary for unchanged MERGE semantics; pandas 3.x
  `str`-dtype object-normalization workaround stays either way.

## Phase 2 — Env + deploy (Orchestrator ops)

1. `requirements.txt`: `duckdb>=1.5,<2` → `duckdb>=2,<3`.
2. Snapshot the DB file before the first 2.0 write (storage stays old-format
   on existing files — verified 11-08 — so this is belt-and-braces rollback).
3. `pip install duckdb==2.0.x` into **system** site-packages (never `--user`).
4. Restart `Flight Server`; smoke in order: `do_get` SELECT → `data_health` →
   `on_request_load` one ticker/one day (exercises register+MERGE) →
   `weekly_refresh` → `dml_client` do_put probe → `fear_greed_refresh`.
5. `python scripts/incident_watch.py` — no new findings expected.
6. Timing: away from the 21:30 `daily_load` (Mon–Fri), Fri 22:15 weekly
   pipeline, and 22:30 watcher windows.

## Phase 3 — Hardening (closes the recurrence vector)

- Re-register the `Flight Server` scheduled task as
  `pythonw.exe -s scripts\run_flight_server.py` — `-s` disables user
  site-packages, so a future `--user` install can never shadow the pinned dep
  again. Apply the same to other scheduled pipeline tasks if cheap.
- Optional defect task (already flagged): the watcher has no `do_get` read-path
  coverage — this outage ran ~26h silently both times.
- Re-check `adbc_driver_duckdb` compat afterwards (upgrading to 2.0 likely
  *satisfies* it — it was what dragged 2.0 in).

## Rollback

`pip install duckdb==1.5.6` + restart Flight Server. Safe because 2.0 keeps
the old storage version on existing DB files (verified 11-08); if that
guarantee ever fails, restore the Phase-2 snapshot. Dual-compat Phase-1 code
needs no revert.

## Proposed task registration

| Task | Role | Scope |
|---|---|---|
| D2-T0 | B | Phase-0 venv repro → failure list (output: EVENTS.log report) |
| D2-T1 | B | B1+B2+B3 code fixes; suite green under 1.5.6 AND 2.0.x |
| D2-T2 | B | `requirements.txt` pin flip |
| — | Orchestrator | Phase-2 deploy (env, backup, restart, smoke, watcher) |
| — | D | Critic gate on each task's AC incl. the two-env test runs |
