---
name: incident-handle
description: Work incident folders from the StockIngestion-HealthWatch store — triage, delisting vs fetch-failure vs corruption triage tree, remediation via Flight actions or suppressions, verify auto-resolve
triggers:
  - user
  - model
allowed-tools:
  - read
  - grep
  - glob
  - exec
  - edit
---

# incident-handle — working the incident queue

Companion to the `stock-ingestion-ops` runbook. Invoke when `incidents/open/`
(repo junction → `C:\Intel\incidents\stock-ingestion\open`) is non-empty or
`_last_check.json` is stale.

Store: `C:\Intel\incidents\stock-ingestion\` · Spec:
`docs/incident_management_requirements.md` · Ops guide: `docs/INCIDENT_WATCH.md`

## Step 0 — heartbeat first, always

Read `_last_check.json` before anything else. Stale `ts` = the watcher itself
is down → check `StockIngestion-HealthWatch` (runs as user, Interactive — only
while logged on) before trusting "open/ is empty".

## Step 1 — triage each folder

Read `incident.json` (severity, check, evidence_summary, suggested_action,
unacked_checks) and `report.json` (verbatim offending JSON). Group folders by
check before acting — one remediation often clears several.

## Step 2 — root cause (decision tree, learned 2026-10-17)

Probe DB state via Flight SQL, then upstream yfinance on the affected window:

```python
import pyarrow.flight as fl, json
c = fl.FlightClient("grpc://127.0.0.1:8815")
tbl = c.do_get(fl.Ticket(json.dumps({"sql": "<SQL>"}).encode())).read_all()
```

| Signal | Upstream `yf.download` shows… | Verdict | Action |
|--------|------------------------------|---------|--------|
| stale/gap | nothing after DB last_date | **Delisted/corporate action** | `acknowledged_delisted` (see below) |
| stale/gap | data exists for missing days | **Fetch miss** | Re-fetch (below) |
| coverage `missing_tickers` | upstream serves nothing at all | **Invalid/mis-configured symbol** | Check `git log src/config.yaml` for a recent add; correct or remove the symbol (user decision), never suppress |
| coverage + metadata warn | upstream healthy | **Newly configured, never backfilled** | `on_request_load` from the data floor + `metadata_load` (below) |
| integrity row | same "bad" values upstream | **Real weird bar** (halted final day, negative WTI) | `benign_integrity` entry |
| integrity row | different values upstream | **Corrupt row** | Re-fetch ticker/date |
| flat O=H=L=C, Vol=0 at a delisted ticker's end | upstream stopped earlier | **Vendor terminal placeholder** | Quarantine (below) — NOT benign |
| zero/neg Open with tiny Vol at delist end | upstream still serves it | **Suspect terminal bar — cause unresolved** | Quarantine (below) |

**Config-vs-data distinction:** a ticker absent from `daily_close` entirely
shows up under **coverage** (`missing_tickers`), not gaps — gap-days only
count tickers with at least one stored row. An absent ticker with an
upstream-empty probe is almost always a bad symbol, not a data problem
(real case 2026-10-17: user added `DXY`; Yahoo serves nothing — the dollar
index is `DX-Y.NYB`; fixed by replacing with `UUP`). A config change needs
a **`Flight Server` task restart** — the server holds config in memory;
the watcher will keep flagging the old symbol until then.

Real 2026-10-17 precedent: DBRG/QRVO/WBD confirmed delisted this way.
**Correction to earlier handling:** WBD 2026-10-05 `Open=0, Vol=40` is NOT
established benign — vendor-consistent ≠ valid. Both it and the phantom
QRVO 2026-10-05 flat bar were quarantined (deleted + exclude_rows) after an
external review found they'd propagated to weekly_close/weekly_indicators.
Three confirmations per ticker is normal (no upstream rows, `daily_load`
"Empty dataset" error, hard stop in DB).

## Step 3 — remediate

**Re-fetch genuine misses** (idempotent MERGE-upsert, safe to over-fetch):

```python
# targeted window — FAST, preferred for known date ranges
c.do_action(fl.Action("on_request_load", json.dumps(
    {"tickers": ["ITEQ"], "start_date": "2026-10-16",
     "end_date": "2026-10-17"}).encode()))   # end-EXCLUSIVE
```

`daily_load {"tickers": [...], "force": true}` works too but ignores the list
for scoping and ran a full 1214-ticker catch-up (~6 min, 4842 rows) on
2026-10-17 — harmless but slow; prefer `on_request_load` for targeted windows.

**Newly configured ticker checklist** (coverage/metadata incidents):
`on_request_load` with `start_date` at the data floor (~2010-01-04 after the
2010 backfill) → `metadata_load {"tickers": [...]}` → restart `Flight Server`
if the config edit postdates the last server start → re-run the watcher.

**Suppress confirmed delisted / benign rows** — edit
`incidents/_config/suppressions.json` (operational data, safe to edit):

```json
{"benign_integrity": [{"ticker": "CL=F", "date": "2020-04-20"}, ...],
 "acknowledged_delisted": ["DBRG", "QRVO", "WBD"]}
```

`acknowledged_delisted` suppresses stale/gap findings at EVERY level incl.
aggregates (INC-T5). `benign_integrity` suppresses specific (ticker, date)
integrity rows — CL=F 2020-04-20/21 and ^IRX 2020-03-18..30 are built-in.
`quarantined_rows` is audit-only (documents rows removed via
`delete_daily_rows`); the enforcement lives in `src/config.yaml`.

**Quarantine artifact/placeholder rows** (INC-T6/T7, enforced):

```python
# remove the stored row(s)
c.do_action(fl.Action("delete_daily_rows", json.dumps(
    {"rows": [{"ticker": "QRVO", "date": "2026-10-05"}]}).encode()))
# then rebuild rollups — weekly rebuilds delete orphan weeks (INC-T7)
c.do_action(fl.Action("weekly_refresh", b"{}"))
```

Then add the pair to `ingestion.exclude_rows` in `src/config.yaml` (loader
drops matching (Ticker,Date) rows from every load path + legacy do_put) —
REQUIRED when upstream still serves the bad bar, else it returns on the next
backfill. Deletion alone is durable only for withdrawn upstream bars.
Requires a Flight server running code ≥ INC-T6 (restart `Flight Server` task
if the action is unknown).

**Never** "fix" a delisted ticker by re-fetching — upstream returns empty
forever; suppression is the resolution.

## Step 4 — verify

```powershell
python scripts\incident_watch.py
```

Cleared findings auto-resolve (`open/` → `resolved/` + `events.jsonl` line).
Reconcile: `suppressed_detail` in stdout/heartbeat should account for every
known-benign/delisted item. `open/` empty + fresh heartbeat = done.

Move a folder `open/` → `acknowledged/` only for findings you've seen but
can't fix yet — it stops the ≥3-check escalation, watcher keeps updating
`last_seen`. Don't hand-move to `resolved/` — let the watcher do it.

## Step 5 — false positive? File a defect, don't bury it

If a folder was created by a watcher bug (not a data problem), the fix is a
code task on `scripts/incident_watch.py` via the blackboard (INC-T5 pattern:
registered, executor B, challenger, critic) — NOT a suppression entry. Tell
the orchestrator; do not hand-edit `src/`/`scripts/` from a handler session.

## Gotchas

- **Config changes are not live until `Flight Server` restarts** — the
  server holds `config.yaml` in memory; a symbol add/remove/fix invisible to
  the watcher until then. Restart via the `Flight Server` scheduled task.
- When a user action resolves an incident (e.g. a config fix), add a
  `resolution_note` field to the resolved folder's `incident.json` so the
  record explains *why* it cleared, not just that it did.
- SEV3 findings escalate to SEV2 (rename + re-page) on the 3rd consecutive
  check — resolve or acknowledge before then to avoid the page.
- `pending_session` is never a finding — today's session inside the +2h grace
  is expected.
- Toast fires on NEW SEV1/SEV2 and escalations only; suppressed items never
  toast.
- Report-level `health_status` stays WARN while delisted tickers remain in
  `src/config.yaml` — that's expected; suppression is at incident level.
  Removing delisted tickers from config is a separate user decision (data
  is preserved by delisting protection).
- `daily_load` returns PARTIAL for delisted names — normal, not a failure.
- Probe scripts go in `%TEMP%`, never the repo; no real `C:\Intel` writes in
  tests.
