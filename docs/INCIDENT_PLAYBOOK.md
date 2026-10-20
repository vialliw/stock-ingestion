# Incident Playbook — Detect → Triage → Fix → Verify → Record

End-to-end playbook for working the StockIngestion-HealthWatch incident
queue. This is the human-facing version of the agent skill
`.devin/skills/incident-handle`, which covers the whole lifecycle in one
place — **handling** the queue (triage, root-cause, remediate, verify) and
**recording** each resolution in plain English (resolution_note + closed
GitHub issue). Same procedure, two presentations.

It does **not** cover the watcher machinery itself. The automated
detection layer (`scripts/incident_watch.py`, the `StockIngestion-
HealthWatch` scheduled task, severities, suppression file schema, folder
lifecycle) is documented separately in `docs/INCIDENT_WATCH.md` and stays
deployed as a scheduled task — the playbook starts where the watcher
stops: a non-empty `open\` folder or a stale heartbeat.

Store: `C:\Intel\incidents\stock-ingestion\` (repo junction `incidents\`)
· Ops guide: `docs/INCIDENT_WATCH.md` · Spec:
`docs/incident_management_requirements.md`

```
        ┌─────────────┐
        │  Heartbeat  │  _last_check.json fresh?
        └──────┬──────┘
               │
        ┌──────▼──────┐      ┌──────────────┐
        │   Triage    │─────▶│  Root cause  │  DB probe + upstream probe
        └──────┬──────┘      └──────┬───────┘
               │                    │
        ┌──────▼────────────────────▼──────┐
        │           Remediate              │  re-fetch / suppress /
        │                                  │  quarantine / config fix
        └──────┬───────────────────────────┘
               │
        ┌──────▼──────┐      ┌───────────────┐
        │   Verify    │─────▶│    Record     │  resolution_note +
        │  (re-run    │      │               │  GitHub issue (closed)
        │   watcher)  │      │               │
        └─────────────┘      └───────────────┘
```

## Phase 1 — Detect

1. Read `incidents\_last_check.json` **first**. Fields: `ts`, `probe`,
   `health_status`, `new`, `updated`, `resolved`, `suppressed`.
2. If `ts` is stale (older than ~26h), the watcher itself is down — check
   the `StockIngestion-HealthWatch` scheduled task (runs as user,
   Interactive — only while logged on). An empty `open\` is meaningless
   without a fresh heartbeat.
3. List `open\` — each folder is one live finding. `acknowledged\` = seen,
   parked. `resolved\` = history.

## Phase 2 — Triage

For each open folder read `incident.json` (severity, check,
`evidence_summary`, `suggested_action`, `unacked_checks`) and
`report.json` (verbatim offending JSON). **Group by `check`** — one fix
often clears several folders.

## Phase 3 — Root cause

Probe the DB via Flight SQL, then upstream yfinance on the same window:

```python
import pyarrow.flight as fl, json
c = fl.FlightClient("grpc://127.0.0.1:8815")
tbl = c.do_get(fl.Ticket(json.dumps({"sql": "<SQL>"}).encode())).read_all()
```

| Signal | Upstream shows… | Verdict | Fix |
|--------|-----------------|---------|-----|
| stale/gap | nothing after DB last_date | Delisted / corporate action | `acknowledged_delisted` |
| stale/gap | data exists for the missing days | Fetch miss | `on_request_load` |
| coverage / metadata warn | upstream empty entirely | Bad symbol in config | Fix `src/config.yaml` (user decision) — never suppress |
| coverage + metadata warn | upstream healthy | New ticker, never backfilled | `on_request_load` from data floor + `metadata_load` |
| integrity row | same "bad" values upstream | Real weird bar (halt day, negative WTI) | `benign_integrity` entry |
| integrity row | different values upstream | Corrupt row | Re-fetch ticker/date |
| flat bar / zero-Open at a delisted ticker's end | stopped earlier / still serves it | Vendor terminal artifact | Quarantine (below) — **not** benign |
| `raw_close` invalid | upstream serves the window | Fixable gap | `on_request_load` repopulates RawClose |
| `raw_close` invalid | upstream gone | Pre-v9 migration row | `do_put` `RawClose=Close` — only when exact (no corp actions in window) |

**Config-vs-data:** a ticker with zero stored rows flags under
**coverage**, not gaps. Absent + upstream-empty ≈ bad symbol. Config
changes are not live until the **Flight Server task restarts** (config is
held in memory).

## Phase 4 — Remediate

**Re-fetch a miss** (idempotent upsert — safe to over-fetch):

```python
c.do_action(fl.Action("on_request_load", json.dumps(
    {"tickers": ["LESL"], "start_date": "2026-10-19",
     "end_date": "2026-10-20"}).encode()))   # end-EXCLUSIVE
```

Prefer `on_request_load` for known windows — `daily_load {"tickers":…}`
ignores scoping and runs a full catch-up (~6 min).

**New ticker checklist:** `on_request_load` from ~2010-01-04 →
`metadata_load {"tickers":[…]}` → restart Flight Server → re-run watcher.

**Suppress (delisted / real weird bars):** edit
`incidents\_config\suppressions.json` — `acknowledged_delisted` (ticker
list; suppresses stale+gap at every level incl. aggregates),
`benign_integrity` ({ticker,date} pairs). `quarantined_rows` is
audit-only documentation.

**Quarantine a vendor artifact:**

```python
c.do_action(fl.Action("delete_daily_rows", json.dumps(
    {"rows": [{"ticker": "QRVO", "date": "2026-10-05"}]}).encode()))
c.do_action(fl.Action("weekly_refresh", b"{}"))  # purges orphan rollups
```

Then add the pair to `ingestion.exclude_rows` in `src/config.yaml` —
required while upstream still serves the bad bar, else it returns on the
next backfill. Needs Flight server code ≥ INC-T6.

**Never** re-fetch a delisted ticker "to fix it" — upstream stays empty
forever; suppression is the resolution.

## Phase 5 — Verify

```powershell
python scripts\incident_watch.py
```

Cleared findings auto-resolve (`open\` → `resolved\` + `events.jsonl`
line). Done = `open\` empty + fresh heartbeat + `suppressed_detail`
accounts for every known-benign item.

- `acknowledged\` = seen but can't fix yet (stops the 3-check SEV3→SEV2
  escalation; watcher keeps updating `last_seen`).
- **Never** hand-move to `resolved\` — the watcher owns that move.
- **Never** bury a watcher bug under a suppression — file a blackboard
  defect task on `scripts\incident_watch.py` instead (INC-T5 pattern).

## Phase 6 — Record (plain-English writeup)

Every resolved folder gets a `resolution_note` in `incident.json`, and
each incident gets a **closed GitHub issue** in `vialliw/stock-ingestion`
(labels `incident` + `sevN` + `resolved`; title `[INC YYYY-MM-DD]`).
GitHub can't backdate — the body carries the true timeline table.

Write the Resolution section in plain English, three parts:

```
**What happened:**
- <what the check flagged, one line>
- <what we checked and found>
- <"First fix (too weak): …" — admit course-corrections>

**What we did:**
1. <action>
2. <action>

**Outcome:** <what's true now, residual risk, anything reversible>
```

Vocabulary: everyday words first ("the vendor still serves this bar",
"marked harmless", "re-downloaded"), exact identifiers verbatim
(`delete_daily_rows`, `ingestion.exclude_rows`, `acknowledged_delisted`,
INC-T6, `daily_close`), jargon translated once inline, concrete numbers
and dates, honest uncertainty ("unknown — removed but logged, restorable").

Reference example — GitHub issues #1–#15, especially #1 (the WBD ghost
bar); the same style rules live in `incident-handle` Step 6.

## Recurring patterns (learned)

- **Settle-window misses** — a same-day session flagged ~2.5h after close
  is usually Yahoo still settling; the trailing lookback or a targeted
  `on_request_load` resolves it. Recurring → move the nightly trigger
  later rather than re-handling.
- **`pending_session`** is never a finding — inside the +2h grace, expected.
- **Report-level WARN is fine** while delisted/known items exist —
  suppression operates at incident level; `open\` empty is what matters.
- **`exclude_tickers` purges history** when upstream confirms delisting —
  deliberate semantics, not "preserve and skip".
- Probe scripts go in `%TEMP%`, never the repo; no real `C:\Intel` writes
  in tests.
