---
name: stock-ingestion-ops
description: Operational runbook for the stock-ingestion Flight service — restart, ad-hoc/backfill loads, health-check semantics, DML client, exports
argument-hint: "[operation]"
triggers:
  - user
  - model
allowed-tools:
  - read
  - grep
  - glob
  - exec
---

# stock-ingestion Operations Runbook

Repo: `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`
Service: Arrow Flight on `127.0.0.1:8815`, canonical DB
`C:\Intel\parquet\stock_ingestion.duckdb`, config `src/config.yaml`,
schema v8. Server runs as scheduled task **FlightServer**.

Quick client:

```python
import pyarrow.flight as fl
c = fl.FlightClient("grpc://localhost:8815")
tbl = c.do_get(fl.Ticket(b"<SQL>".encode())).read_all()   # reads
resp = list(c.do_action(fl.Action("<action>", b'<json>'))) # writes/ops
```

## Restart the service (needed after config.yaml OR src/ changes)

The server loads config once at startup — new tickers/settings are invisible
to a running process.

```powershell
Stop-ScheduledTask -TaskName "FlightServer"; Start-Sleep 3
Start-ScheduledTask  -TaskName "FlightServer"; Start-Sleep 5
```

Then verify port + config picked up:

```sql
SELECT count(DISTINCT Ticker) FROM daily_close   -- sanity
```

If an in-flight action dies with `Stream removed`: the write may have
committed — read-verify before retrying (a stop/start race can leave a
second instance holding the DB lock; check `logs/flight_server*.log`).

## Ad-hoc / ranged loads

| Goal | Action | Params |
|---|---|---|
| Catch up all tickers | `daily_load` | `{}` — runs split-check + lookback-4 fetch + weekly refresh |
| Specific tickers | `daily_load` | `{"tickers": [...]}` |
| Historical range | `on_request_load` | `{"tickers":[...], "start_date":"YYYY-MM-DD","end_date":"YYYY-MM-DD"}` — end-exclusive; MERGE-upsert idempotent; detects new tickers → backfills from `get_aligned_start_date()` (currently 2016-09-01) |
| Force daily run in console | — | `python scripts/daily_load_once.py` |

`daily_load` returns `{"status": "SUCCESS|PARTIAL", "failed": {...}}` —
PARTIAL is normal for late-IPO tickers on old windows and delisted names
(persistent-empty tickers get delisting-protected, not deleted).

## Health check semantics (the Sep-30/Oct-1 gotcha)

`data_health` reports `latest_trading_day` = last **ended** session —
NYSE close + 2h grace (`db.py` `_expected_trading_dates`). A session still
inside its grace window appears as `pending_session`, not latest. So
"health shows yesterday" ≠ "today's data missing" — check `MAX(Date)`
directly and the `pending_session` field. Known benign integrity hits:
negative WTI (2020-04-20/21) and zero ^IRX (Mar 2020) — real market values.

Scripts: `scripts/data_health_report.py`, `scripts/data_health_verify.py`.

## Client DML (pivot namespace, F26)

`scripts/dml_client.py` is the vendored reference client. Server-side:
`dml_admin` action — `register_namespace | create_table (structured spec →
real PK) | create_as | create_view | insert_select (append|replace,
replace = atomic DELETE+INSERT txn) | drop_table | list_tables | pin/unpin`.
Generalized `do_put` descriptor: `{"table","mode":create_or_replace|append|
replace}`. Every op audited in `client_dml_audit` incl. rejections.
Guardrails: allowlisted namespaces, single-statement SELECT via
`extract_statements`, autoinstall/autoload killed at DB open, denylist on
file-read/generator/`duckdb_*` functions, `_write_lock` + timeout +
`interrupt()`. `pivot` is a **reserved word** — quote it in source SQL
(`"pivot"."t"`). Full API: `docs/DATA_SERVICE_API.md`,
`docs/client_dml_sql_study.md`.

## Bulk export pattern

```python
tbl = c.do_get(fl.Ticket(b"SELECT ... ORDER BY Date, Ticker")).read_all()
tbl.to_pandas().to_parquet(path, compression="zstd", index=False)
```

One `do_get` streams multi-million-row tables fine. Verify after write:
row count, ticker count, range, sort order, exclusion leakage.

## Split handling

`_check_and_apply_splits` runs before each daily fetch — splits dated ≥ a
ticker's earliest stored row are "unprocessed" → trigger a full-history
re-download *before* delete (data stays consistent), then recorded in
`splits_processed`. Extending the floor (e.g. the 2016-09-01 backfill) can
surface in-window splits — scan with one batched `yf.download(actions=True)`
on the gap window and trigger a targeted load for just those tickers.

## Quality gates before closing any task

`python -m pytest tests/` · `python scripts/check_loops.py --src-dir src`
(unjustified `for` loops fail — `# justified: <reason>` within 6 lines) ·
`python scripts/poll_tasks.py --verify <id>` · `README.md` ≡
`docs/README.md` byte-identical · then
`powershell -NoProfile -File scripts/notify_done.ps1 -Message "..."`.
