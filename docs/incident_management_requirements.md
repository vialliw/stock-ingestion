# Incident Management — Raise-Ticket Requirements

Scope: monitoring of the **daily download job** (`daily_load` / `daily_load_once.py`,
run by the `StockIngestion-FlightServer` scheduled-task chain). This document
specifies what the external incident-management system must check, how findings
map to incident severity, and when/how tickets are raised and we are notified.

Status: requirements draft, based on live `data_health` output of 2026-10-12.

---

## 1. Data sources

The monitor consumes two signals per daily run, in order:

| # | Source | Interface | Signal |
|---|--------|-----------|--------|
| S1 | Daily job result | `daily_load` Flight action / `daily_load_once.py` exit code | `{status: SUCCESS\|PARTIAL, failed: {...}}`; exit 0/1 |
| S2 | Post-job health check | `python scripts\data_health_report.py --json` (invoked via `scripts\data_health.bat`) | composite health JSON; exit 0 = OK, 1 = WARN / CHECK_FAILED / server unreachable |

**Hard requirement:** parse the `--json` output, never the formatted text view.
`data_health` is report-only and never writes to the DB, so polling it is safe.

### 1.1 Timing semantics (must implement, or we get false pages)

- `latest_trading_day` = last **ended** NYSE session (close + 2h grace).
- If `pending_session` is present, that session is still open / inside the grace
  window — it must **not** count as missing data.
- Run the health check **after** the daily job completes (task-chained), plus a
  once-morning sweep is acceptable. Do not alert on today's session until its
  grace window has passed.

### 1.2 Health JSON fields consumed

```
health_status                       # OK | WARN | CHECK_FAILED  (top level)
daily_close.health_status           # section status; mode: full|…
daily_close.as_of, latest_trading_day, pending_session
daily_close.checks.staleness        # stale_count, stale_tickers[{ticker,last_date}]
daily_close.checks.coverage         # missing_count, configured_count, missing_tickers[]
daily_close.checks.integrity        # violation_count, reason_counts{}, violations[{ticker,date,reason}]
daily_close.checks.duplicates       # duplicate_count, duplicates[]
daily_close.checks.gaps             # tickers_with_gaps, total_missing_days,
                                    #   per_ticker[{ticker,missing_count,missing_days[]}], window_start/end
fear_greed.health_status            # OK|WARN|CHECK_FAILED|DISABLED (+staleness/integrity/series)
weekly.health_status                # OK|WARN|CHECK_FAILED (+staleness/parity/stale_tickers/integrity)
company_metadata.health_status      # OK|WARN|CHECK_FAILED (+coverage/null_fields)
```

---

## 2. Severity levels

| Level | Name | Meaning | Notify |
|-------|------|---------|--------|
| SEV1 | Critical | Monitoring is blind or the pipeline is down | Immediate toast + ticket |
| SEV2 | High | Latest-session data wrong/missing at scale, or data corruption | Toast + ticket, same day |
| SEV3 | Medium | Localised data quality issue, likely self-healing next run | Ticket + daily digest |
| SEV4 | Info | Expected/benign or purely informational | Log only, no ticket |

### 2.1 Classification rules (evaluate in order; first match wins)

**SEV1 — raise immediately**
- `data_health_report.py` exits 1 **and** no JSON payload (Flight server
  unreachable → the daily job itself cannot run).
- `daily_close.health_status == CHECK_FAILED` or any section `CHECK_FAILED`.
- Daily job (S1) crashes / exits non-zero without reaching the load stage
  (e.g. DuckDB lock exhaustion).

**SEV2 — ticket same day**
- S1 job status `PARTIAL` with `failed` count ≥ 10 tickers.
- `staleness.stale_count ≥ 25` (≈2% of the 1214 configured tickers) — a broad
  ingestion failure, not per-ticker noise.
- `coverage.missing_count > 0` for tickers that previously had data (silent
  ticker loss). New-ticker absences right after a config change are SEV3.
- `duplicates.duplicate_count > 0` — violates the (Ticker, Date) PK contract;
  indicates an upsert-path defect, always actionable.
- `integrity` violations dated **within the trailing 30-session window** after
  suppressing the known-benign list (§3) — recent corrupt rows mean the loader
  accepted bad data.
- `gaps.total_missing_days ≥ 30` in one run, or any single ticker missing ≥ 10
  sessions in the window (broad or persistent gap, not a feed blip).
- Same incident (any level) unacknowledged across ≥ 3 consecutive checks →
  escalate one level.

**SEV3 — ticket, digest notification**
- `stale_count` 1–24, or `missing_count` 1–10, or small per-ticker gaps
  (< 10 sessions/ticker, < 30 total) — typically feed gaps, halts, or a missed
  run; verify next-run self-recovery.
- S1 `PARTIAL` with `failed` < 10 (normal for delisted/late-IPO names).
- `fear_greed` or `weekly` section WARN (staleness/parity/integrity).
- `company_metadata` coverage WARN.
- Suspected delisting/corporate action: ticker stale ≥ 5 sessions with no
  integrity error — ticket says "verify ticker status" rather than "re-fetch".

**SEV4 — log only**
- Integrity violations matching the known-benign list (§3).
- `company_metadata` null-field rows (informational by design).
- `pending_session` notes; `DISABLED` optional sections.

### 2.2 Dedup, lifecycle, escalation

- **Incident key** = stable hash of (check, sorted affected tickers/dates).
  An identical finding across runs updates `last_seen` on the open ticket —
  it must NOT open a duplicate.
- Lifecycle: `open → acknowledged → resolved`. Auto-resolve when the next
  check reports the incident key absent; log resolution.
- Escalation: unacknowledged SEV3 ≥ 3 checks → SEV2; unacknowledged SEV2
  ≥ 3 checks → repeat toast (re-page), keep SEV2.

---

## 3. Known-benign suppressions (required)

These rows are real market values, not corruption — suppress before
classifying `integrity` findings:

| Ticker | Dates | Reason |
|--------|-------|--------|
| CL=F | 2020-04-20, 2020-04-21 | Negative WTI crude settle (real) |
| ^IRX | 2020-03-18 .. 2020-03-30 | Zero 13-week T-bill yield (real) |

Suppression list lives in incident-manager config, not in the health check —
the check must keep reporting them so new unexpected dates still surface.

Delisted tickers: the loader already applies delisting protection
(persistent-empty tickers are kept, not deleted). A ticker stale for many
sessions is *expected* post-delisting — the monitor should carry an
acknowledged-delisted list so confirmed cases downgrade SEV3 → SEV4.

---

## 4. Ticket content

Each raised ticket must carry:

- `incident_key`, `severity`, `first_seen`, `last_seen`, `state`
- `source`: `daily_load` or `data_health`, plus run timestamp (`as_of`)
- `summary`: human-readable (e.g. "5 tickers stale vs latest session 2026-10-09")
- `evidence`: the offending JSON fragment (verbatim)
- `suggested_action` per check type:
  - stale / gaps / coverage → `daily_load {"tickers": [...]}` or
    `on_request_load` for the missing window (MERGE-upsert, idempotent)
  - duplicates → investigate upsert path (should be impossible)
  - recent-date integrity → targeted `on_request_load` re-fetch for that
    ticker/date
  - suspected delisting → verify ticker validity, then add to acknowledged list
  - server unreachable → restart `StockIngestion-FlightServer` per ops runbook

## 5. Notification channel

- SEV1/SEV2: Windows toast via `scripts/notify_done.ps1` (registered AUMID —
  a plain "PowerShell" AppID is silently dropped on Win11; never hand-roll).
- SEV3: append to a daily digest toast; individual toast optional.
- All levels: append one JSONL record per event to the incident store
  (e.g. `logs/incidents.jsonl`) — this is the audit trail for follow-up.

---

## 6. Worked example — live run, 2026-10-12

Actual output (`health_status = WARN`, exit 1; `latest_trading_day`
2026-10-09, `pending_session` 2026-10-12 → correctly not flagged):

| Finding | Classification | Ticket |
|---------|----------------|--------|
| DBRG stale since 2026-09-29 (8 sessions) | SEV3 — suspected delisting/halt, "verify ticker status" | open |
| QRVO, WBD missing 2026-10-06..09 (4 days each) | SEV3 — per-ticker gap | open |
| LESL, TCBI missing 2026-10-09 (1 day each) | SEV3 — likely feed blip, confirm next run | open |
| WBD 2026-10-05 null/nonpositive OHLC | **SEV2** — recent-date integrity violation | open |
| CL=F / ^IRX historical rows | SEV4 — suppressed (known-benign) | none |
| coverage 0/1214, duplicates 0 | clean | none |
| fear_greed / weekly / metadata all OK | clean | none |

Net: 1 × SEV2 (WBD corrupt row — re-fetch 2026-10-05), 3 × SEV3 tickets,
digest notification. If the same keys persist through tomorrow's check
unacknowledged, escalation counters advance per §2.2.

---

## 7. Storage layout (DRAFT — awaiting review)

Decision recorded 2026-10-16: one **centralized incident store** shared by all
Devin projects; each project still *creates* incidents via a project-local
`incidents\` path that is a **symlink into its central sub-tree**. The watcher
never needs to know the physical location.

### 7.1 Central root — DECIDED 2026-10-16: Option A (local)

`C:\Intel\incidents\` — same drive as the DuckDB file and logs. No network
dependency, no fsutil changes; the project link uses a junction
(`mklink /J`), which cannot target UNC anyway.

~~Option B — NAS `Z:\vault-nas\Vault\devin-incidents\`~~ — deferred. Revisit
only if incidents must be browsed from other machines; would need
`mklink /D` + `fsutil behavior set SymlinkEvaluation L2R:1 R2R:1` and a
local spool fallback for NAS outages. The inner tree is root-agnostic, so
migration is a link re-point, not a redesign.

Inner tree under `C:\Intel\incidents\`:

```
C:\Intel\incidents\
  _index\                      # optional cross-project roll-up (phase 2)
  stock-ingestion\
    _project.json              # {"name","repo_path","source":"data_health","tz":"America/New_York"}
    _config\
      suppressions.json        # known-benign rows + acknowledged delisted tickers
    _last_check.json           # heartbeat: {ts, exit_code, health_status, new/updated/resolved counts}
    _reports\                  # raw health JSON, one file per run (audit trail)
      2026-10-16.json
    events.jsonl               # append-only per-project feed (no shared file -> no locking)
    open\                      # ACTIVE incidents — EMPTY DIRECTORY = HEALTHY
      20261012_sev2_integrity_WBD_2026-10-05\
        incident.json          # {incident_key, severity, check, first_seen, last_seen,
                               #  unacked_checks, severity_history[], state, suggested_action}
        report.json            # verbatim evidence fragment from the health JSON
        TICKET.md              # human summary + copy-paste remediation
      20261012_sev3_stale_DBRG\...
      20261012_sev3_gap_QRVO\...
    acknowledged\              # operator moved it here; watcher still updates last_seen
    resolved\                  # watcher auto-moves when key absent from latest report
```

### 7.2 Project-local view (what lives in this repo)

```
stock-ingestion\
  incidents  ->  C:\Intel\incidents\stock-ingestion   # mklink /J (junction)
```

- Creation stays project-local: watcher writes `incidents\open\<key>\…`,
  which lands in the central store transparently.
- Other projects repeat the pattern: `other-repo\incidents` →
  `C:\Intel\incidents\<other-repo>`.
- **OneDrive caveat (verify before commit):** the junction itself sits inside
  a OneDrive-synced folder. If OneDrive follows it and starts ingesting the
  incident tree, exclude `incidents` from sync or move the link outside the
  synced root.

### 7.3 Semantics

- **Dedup:** folder name IS the incident key —
  `YYYYMMDD_<sev>_<check>_<detail>` (e.g. `..._stale_DBRG`,
  `..._integrity_WBD_2026-10-05`; multi-ticker keys get `..._stale_multi5`).
  A repeat finding updates `last_seen`/`unacked_checks` in `incident.json` —
  never a second folder.
- **Escalation:** `unacked_checks` bumps each run while in `open\`; ≥3 →
  severity bump, folder renamed, entry appended to `severity_history`.
- **Auto-resolve:** key absent in latest report → folder moved `open\` →
  `resolved\`, `state` updated, `events.jsonl` line appended.
- **No news is good news:** `open\` empty + `_last_check.json` fresh = healthy.
  A stale `_last_check.json` means the *watcher* is down — the handler checks
  heartbeat freshness first.
- **Notify:** toast only when a *new* SEV1/SEV2 folder is created.

### 7.4 Open items from 2026-10-16 discussion — status

| Question | Resolution |
|----------|-----------|
| Store path | **DECIDED: `C:\Intel\incidents\`** (junction link) |
| Handler mode | On-demand agent works `open\` (default, confirm) |
| Watcher timing | Fixed ~18:30 ET after daily load (confirm) |
| Auto-resolve | Yes — folder move on key-absent (confirm) |
