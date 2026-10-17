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
is down → check `StockInestion-HealthWatch` (runs as user, Interactive — only
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
| integrity row | same "bad" values upstream | **Real weird bar** (halted final day, negative WTI) | `benign_integrity` entry |
| integrity row | different values upstream | **Corrupt row** | Re-fetch ticker/date |

Real 2026-10-17 precedent: DBRG/QRVO/WBD were all confirmed delisted this way;
WBD 2026-10-05 `Open=0, Vol=40` was the real final bar → benign, not corrupt.
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

**Suppress confirmed delisted / benign rows** — edit
`incidents/_config/suppressions.json` (operational data, safe to edit):

```json
{"benign_integrity": [{"ticker": "CL=F", "date": "2020-04-20"}, ...],
 "acknowledged_delisted": ["DBRG", "QRVO", "WBD"]}
```

`acknowledged_delisted` suppresses stale/gap findings at EVERY level incl.
aggregates (INC-T5). `benign_integrity` suppresses specific (ticker, date)
integrity rows — CL=F 2020-04-20/21 and ^IRX 2020-03-18..30 are built-in.

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
