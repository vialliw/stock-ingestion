# Incident Health Watch — Operations Guide

Automated monitoring for the daily download job. A scheduled watcher probes the
Flight server's `data_health` action after each daily load, classifies findings
into severity levels, and materializes incident folders for follow-up.

**No news is good news:** `incidents\open\` empty + a fresh `_last_check.json`
heartbeat = healthy. Nothing is produced on a clean run except the heartbeat
and a `_reports\` snapshot.

Requirements/spec: `docs/incident_management_requirements.md`
(§2 severity rules, §3 suppressions, §4 ticket fields, §7 storage layout).

---

## Components

| Piece | Path | What it does |
|-------|------|--------------|
| Watcher | `scripts/incident_watch.py` | Fetches health JSON → classifies → writes incident folders |
| Registrar | `scripts/register_incident_watch.ps1` | Provisions the store + junction; registers the scheduled task |
| Scheduled task | `StockIngestion-HealthWatch` | Daily 18:30 ET (23:30 machine-local), after the 16:14 ET daily load |
| Incident store | `C:\Intel\incidents\stock-ingestion\` | Central root for all Devin projects |
| Repo view | `incidents\` (junction → store) | Project-local creation path; gitignored |
| Toast | `scripts/notify_done.ps1` | Fires only on NEW SEV1/SEV2 |

## Severity levels (summary)

| Level | Trigger examples | Notify |
|-------|------------------|--------|
| SEV1 | Flight server unreachable, section CHECK_FAILED | Toast immediately |
| SEV2 | Broad staleness (≥25), coverage loss, duplicates>0, recent-date integrity/raw_close violations, large gaps | Toast + ticket |
| SEV3 | Few stale tickers, small gaps, suspected delisting, section WARN, historical raw_close issues | Ticket (digest only) |
| SEV4 | Known-benign rows, acknowledged delisted tickers, informational | Log only |

Unacknowledged ≥3 consecutive checks → escalates one level (folder renamed,
history recorded, re-toast). Full rules: requirements doc §2.1.

## Folder layout

```
incidents\                        (junction → C:\Intel\incidents\stock-ingestion)
  _project.json                   # project descriptor
  _config\suppressions.json       # known-benign rows + acknowledged_delisted
  _last_check.json                # heartbeat — handler checks this FIRST
  _reports\YYYY-MM-DD.json        # raw health snapshot per run
  events.jsonl                    # append-only transition log
  open\                           # active incidents (empty = healthy)
    20261016_sev2_integrity_WBD_2026-10-05\
      incident.json               # key, severity, first/last_seen, unacked_checks,
                                  #   severity_history, state, suggested_action
      report.json                 # verbatim evidence fragment
      TICKET.md                   # human summary + copy-paste remediation
  acknowledged\                   # operator-seen; watcher still updates
  resolved\                       # auto-moved when finding clears
```

## Working incidents (handler workflow)

1. Look in `incidents\open\`. Empty + fresh `_last_check.json` → nothing to do.
   Stale `_last_check.json` → the watcher itself is down (check the task).
2. Each `TICKET.md` carries a `suggested_action`, e.g.:
   - stale/gap/coverage → `daily_load {"tickers":[...]}` or `on_request_load`
     for the window (idempotent MERGE-upsert)
   - recent-date integrity → targeted `on_request_load` re-fetch
   - recent raw_close → `on_request_load` for the affected ticker(s)
   - suspected delisting → verify ticker, then add to
     `_config/suppressions.json` `acknowledged_delisted`
   - server unreachable → restart `StockIngestion-FlightServer`
3. Move a folder `open\` → `acknowledged\` to stop escalation counting;
   the watcher still bumps `last_seen` while the finding persists.
4. Auto-resolve: when the finding disappears from the health report the
   watcher moves the folder to `resolved\` and logs the transition.

## Manual commands

```powershell
# Live check now (exit 0 even on WARN — folders carry the signal)
python scripts\incident_watch.py

# Offline self-test — synthetic fixture into a temp dir, no server needed
python scripts\incident_watch.py --self-test

# Options: --incidents-root <path>  --gap-days N
```

```powershell
# Provision store + junction without registering
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -ProvisionOnly

# Register under your own account (visible in Task Scheduler GUI; runs only while logged on)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -UserPrincipal

# Register as SYSTEM (elevated, hidden from non-elevated GUI)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1

# Remove the task (store + junction stay)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -Unregister
```

Note: machine policy is `AllSigned` — `-ExecutionPolicy Bypass` is required.

## Caveats

- **Runs only while logged on** under `-UserPrincipal` (Interactive token);
  `StartWhenAvailable` catches up at next logon after a missed slot.
- **RawClose semantics:** the watcher classifies one finding per affected
  ticker aggregate.  Finite zero/negative `RawClose` values (e.g. `^IRX`,
  `CL=F` Apr-2020) are valid and never raise tickets.  The known 6,507
  historical NULLs for delisted WBD/QRVO/LESL are suppressed as
  `acknowledged_delisted`.
- **RawClose truncation fallback:** invalid rows the bounded `per_ticker`
  list cannot attribute become ONE incident under the stable key
  `raw_close/count_unattributed` (the row count lives in evidence, not the
  key — a drifting count updates the same ticket).  Severity is SEV2
  whenever recency cannot be ruled out — `latest_invalid_date` inside the
  trailing window, missing (pre-HRAW-T2 payload), or unparseable — and
  SEV3 only when it provably falls outside the window.
- **DST drift:** during the ~5 wk/yr UK–US DST-mismatch window the task fires
  19:30 ET instead of 18:30 — still safely after the 16:14 ET load.
- **OneDrive:** the `incidents` junction sits inside the synced repo; the
  store itself lives outside OneDrive. If sync ever ingests the tree,
  exclude `incidents` in OneDrive settings.
- **`git add -A`:** `incidents/` is gitignored — live incident state must
  never be committed.
- Suppression list additions go in `_config/suppressions.json`, not in code —
  the health check keeps reporting everything so new anomalies still surface.
