# Phase 2 Development Plan

**Status:** DRAFT — awaiting PM approval
**Date:** 2026-09-07
**Baseline:** Phase 1 complete, 122/122 tests pass, 0 loop checker violations

---

## PM Decisions (confirmed)

| Item | Decision |
|---|---|
| R2 Sync | Sync the single DuckDB file directly (no Parquet export). Standalone Flight action, auto-trigger after successful daily_load, also manual trigger. |
| APScheduler | Separate process (`src/scheduler_main.py`). |
| do_put / IngestBatch | Include in Phase 2. Table name is `DAILY_CLOSE` (not `daily_quotes`). |
| Intraday timeframes | **Removed entirely** — not deferred, not Phase 3. Schema stays daily-only. |
| Parquet Report Output | **Deferred to Phase 3.** User will provide SQL. Report format will be xlsx (not Parquet). |
| Query Timeout | Default 120 seconds. No row limit (production: ~1200 tickers, 6-year initial load, abundant rows). |
| mTLS Authentication | Deferred to Phase 3. |
| Health check | Uses table name `DAILY_CLOSE`. |
| Table rename | `daily_quotes` → `DAILY_CLOSE` throughout entire codebase (schema migration, Cycle 0). |

---

## Scope

### Cycle 0 — Table Rename + Hardening (baseline migration)

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T0 | Rename `daily_quotes` → `DAILY_CLOSE` throughout codebase | executor | src/db.py, src/db_challenger.py, src/ingestion.py, src/ingestion_challenger.py, src/flight_server.py, src/flight_server_challenger.py, src/store.py, src/store_challenger.py, tests/*.py |
| F7-T0b | SQL CAST(Date AS VARCHAR) in generate_top_20_dollar_volume — eliminate Date stringify loops | executor | src/db.py, src/db_challenger.py |
| F7-T0c | batch_record_splits method in DatabaseManager — eliminate record_split loops | executor | src/db.py, src/db_challenger.py, src/ingestion.py, src/ingestion_challenger.py |
| F7-T0d | check_loops.py --strict mode implementation | executor | scripts/check_loops.py |
| F7-T0e | Critic: verify rename + hardening + full suite + loop checker | critic | tests/ (update assertions) |

**Cycle 0 goals:**

**Table rename (F7-T0):**
- Rename `daily_quotes` table to `DAILY_CLOSE` in DuckDB schema creation
- Update ALL SQL queries across `src/db.py`, `src/db_challenger.py`, `src/ingestion.py`, `src/ingestion_challenger.py`, `src/flight_server.py`, `src/flight_server_challenger.py`
- Update `src/store.py`, `src/store_challenger.py` if they reference `daily_quotes`
- Update ALL test files that reference `daily_quotes` in SQL strings or assertions
- Update tech spec `docs/tech_spec/gemini_tech_spec.1.0.2.md` — replace `daily_quotes` with `DAILY_CLOSE`
- Schema migration: `ALTER TABLE daily_quotes RENAME TO DAILY_CLOSE` for existing DBs, or recreate if fresh
- Bump `SCHEMA_VERSION` from 1 to 2
- Add migration logic in `DatabaseManager.__init__`: check `schema_version`, if 1 and `daily_quotes` exists, rename to `DAILY_CLOSE`

**SQL CAST (F7-T0b):**
- Replace `for item in result: item["Date"] = str(item["Date"])` with SQL `CAST(Date AS VARCHAR)` in the `generate_top_20_dollar_volume` query
- Date comes back as string from DuckDB — no Python loop needed

**Batch INSERT splits (F7-T0c):**
- Add `DatabaseManager.batch_record_splits(splits: List[Tuple[str, str, float]])` — single SQL `INSERT INTO splits_processed VALUES (?, ?, ?), (?, ?, ?), ...` with `ON CONFLICT DO NOTHING`
- Replace the `for split_date_str, factor in unprocessed:` loops in both ingestion variants

**check_loops --strict (F7-T0d):**
- Implement `--strict` flag that flags `# justified:` comments with empty reasons (e.g. `# justified:` with no text after the colon)

**All changes must pass loop checker with 0 violations and full test suite.**

---

### Cycle 1 — Structured JSON Logging + Log File Configuration

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T1 | Implement structured JSON logging + configure log_file path | executor | src/config.py, src/main.py, src/config.yaml |
| F7-T1b | Challenger: independent JSON logging variant | challenger | src/config_challenger.py (if needed) |
| F7-T1c | Critic: verify log format + log file path | critic | tests/test_logging.py |

**Cycle 1 goals:**
- Add `log_format` config option: `"text"` (default) or `"json"`
- When `json`, use `python-json-logger` (`pythonjsonlogger.jsonlogger.JsonFormatter`) — all existing `logger.*` calls work unchanged
- `main.py` configures `logging.FileHandler` writing to `cfg.logging.log_file` (currently broken — `basicConfig` doesn't use the configured path)
- Also configure console handler for visible output
- Log file parent directory created if absent (like `duckdb_dir`)
- Structured JSON log line example: `{"asctime": "2026-09-07...", "levelname": "INFO", "name": "src.ingestion", "message": "..."}`
- New dependency: `python-json-logger` (published 2023, stable)

---

### Cycle 2 — Health Check Action + Ingestion Runs Audit Table

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T2 | Implement health_check Flight action + ingestion_runs audit table | executor | src/flight_server.py, src/db.py, src/ingestion.py |
| F7-T2b | Challenger: independent health_check + audit variant | challenger | src/flight_server_challenger.py, src/db_challenger.py, src/ingestion_challenger.py |
| F7-T2c | Critic: verify health_check + audit table | critic | tests/test_health_check.py, tests/test_audit.py |

**Cycle 2 goals:**
- New `ingestion_runs` table: `run_id` (auto-increment), `run_type`, `start_time`, `end_time`, `duration_ms`, `tickers_succeeded` (JSON), `tickers_failed` (JSON), `errors` (JSON), `rows_upserted`, `status`
- `IngestionEngine` writes one audit row after each `run_initial_load` / `run_daily_load` / `run_on_request_load` (single SQL INSERT, no loop)
- New Flight action `health_check` — returns JSON: `{"status": "healthy", "db_connected": true, "db_path": "...", "ticker_count": N, "last_run": "...", "uptime_seconds": ...}`
- Health check: DB file exists, `SELECT 1` works, row count in `DAILY_CLOSE`, latest `ingestion_runs` row
- No network calls (no yfinance), fast (<100ms)
- New Flight action `get_run_history` — returns recent `ingestion_runs` rows as Arrow table

---

### Cycle 3 — Query Timeout

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T3 | Implement query timeout in do_get (120s default, no row limit) | executor | src/config.py, src/flight_server.py, src/config.yaml |
| F7-T3b | Challenger: independent variant | challenger | src/flight_server_challenger.py, src/config_challenger.py |
| F7-T3c | Critic: verify timeout | critic | tests/test_query_timeout.py |

**Cycle 3 goals:**
- Add `query_timeout_seconds` (default 120) to `server:` config section
- **No row limit** — production has ~1200 tickers, 6-year initial load, abundant rows
- `do_get` enforces timeout via `threading.Timer` — returns `FlightServerError` with descriptive message if exceeded
- No `LIMIT` injection — queries run as-is
- Config: `server.query_timeout_seconds: 120`

---

### Cycle 4 — R2 Sync (DuckDB File Direct)

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T4 | Implement R2 sync — upload DuckDB file directly + auto after daily_load + manual Flight action | executor | src/r2_sync.py, src/config.py, src/config.yaml, src/flight_server.py, src/ingestion.py |
| F7-T4b | Challenger: independent R2 sync variant | challenger | src/r2_sync_challenger.py, src/flight_server_challenger.py, src/ingestion_challenger.py |
| F7-T4c | Critic: verify R2 sync (mocked) | critic | tests/test_r2_sync.py |

**Cycle 4 goals:**
- New module `src/r2_sync.py` with class `R2SyncManager`
- Uses `boto3` S3-compatible client with R2 credentials from env vars (`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT_URL`)
- **Syncs the single DuckDB file directly** — `boto3.client('s3', endpoint_url=...).upload_file(cfg.db_path, bucket, key)`
- No Parquet export — the DuckDB file is the canonical store
- Config: new `r2:` section with `bucket_name`, `prefix`, `enabled` (default `false`)
- **Auto-sync:** after `run_daily_load` succeeds (status SUCCESS or PARTIAL), if `r2.enabled` is true, automatically sync the DuckDB file to R2
- **Manual sync:** new Flight action `sync_to_r2` — operator can trigger explicitly
- Sync only happens after successful processing (per AGENTS.md: "Do not write to R2 during intermediate steps")
- R2 credentials never committed — read from environment / GitHub repo secrets
- New dependency: `boto3` (stable, widely used)

---

### Cycle 5 — APScheduler (Separate Process)

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T5 | Implement standalone scheduler process (src/scheduler_main.py) | executor | src/scheduler.py, src/scheduler_main.py, src/config.py, src/config.yaml |
| F7-T5b | Challenger: independent scheduler variant | challenger | src/scheduler_challenger.py |
| F7-T5c | Critic: verify scheduler (mocked) | critic | tests/test_scheduler.py |

**Cycle 5 goals:**
- New module `src/scheduler.py` with class `PipelineScheduler`
- Uses `APScheduler` `BackgroundScheduler` to run `daily_load` at configurable time (default 16:30 ET)
- **Standalone process:** `src/scheduler_main.py` — separate entrypoint, does NOT start Flight server
- Scheduler checks NYSE market calendar before running (reuse `pandas_market_calendars`)
- Config: new `scheduler:` section with `schedule_time` (default "16:30"), `timezone` (default "America/New_York"), `enabled` (default false)
- Graceful shutdown: SIGINT/SIGTERM stops scheduler cleanly
- The scheduler process opens its own DuckDB connection (read-write) — must coordinate with Flight server's connection
- **DuckDB multi-process note:** DuckDB does not allow two read-write connections from different processes. The scheduler process will use `read_only=False` but must complete quickly. The Flight server's write connection is only active during ingestion actions, so the scheduler can acquire the write lock between Flight requests. If contention occurs, the scheduler will retry with backoff.
- New dependency: `APScheduler` (stable, widely used)

---

### Cycle 6 — do_put / IngestBatch

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T6 | Implement do_put in Flight server — accept external batch data into DAILY_CLOSE | executor | src/flight_server.py, src/db.py |
| F7-T6b | Challenger: independent do_put variant | challenger | src/flight_server_challenger.py, src/db_challenger.py |
| F7-T6c | Critic: verify do_put | critic | tests/test_do_put.py |

**Cycle 6 goals:**
- Implement `do_put` in `MarketDataFlightServer`
- Accept Arrow record batches with `DAILY_CLOSE` schema (7 columns: Date, Ticker, Open, High, Low, Close, Volume)
- Validate schema matches exactly — reject mismatched schemas with `FlightServerError`
- Reject data for tickers in `exclude_tickers` — filter + log warning
- Upsert via existing `DatabaseManager.upsert_dataframe`
- `do_put` reads all batches from the stream, concatenates, upserts in one operation
- No per-row loop — use `pyarrow.Table.from_batches()` + `to_pandas()` + `upsert_dataframe()`

---

### Cycle 7 — Integration + Phase 2 Hardening

| ID | Title | Owner | Output Files |
|---|---|---|---|
| F7-T7 | Integration tests for all Phase 2 features | executor | tests/test_phase2_integration.py |
| F7-T7b | Critic: full regression + Phase 2 acceptance | critic | tests/ (update as needed) |

**Cycle 7 goals:**
- End-to-end test: scheduler triggers daily_load → audit row written → R2 sync runs → health_check reflects last run
- Full regression: all Phase 1 tests still pass (122+, updated for DAILY_CLOSE rename)
- Loop checker: 0 violations on all src/ files including new modules
- Pre-commit hook: active and blocking unjustified loops in new code

---

## Deferred to Phase 3

| Feature | Reason |
|---|---|
| Parquet/xlsx report output | User will provide SQL; report format as xlsx. Deferred per PM. |
| mTLS authentication | Flight server is localhost-only for now. |
| `ingestion_runs` audit table | No longer deferred — included in Phase 2 Cycle 2. |

## Removed (not deferred, not Phase 3)

| Feature | Reason |
|---|---|
| Intraday timeframes | Removed entirely per PM. Schema stays daily-only. `interval="1d"` is permanent. |

---

## New Dependencies

| Package | Version | Purpose | Published |
|---|---|---|---|
| `python-json-logger` | stable (2023+) | JSON structured logging | Yes, 7+ days |
| `boto3` | stable | R2 S3-compatible client (upload DuckDB file) | Yes, 7+ days |
| `APScheduler` | stable | Automated scheduling | Yes, 7+ days |

All dependencies must be added via `pip install` (not manual requirements.txt edit).

---

## Config Changes

```yaml
# New/changed sections in config.yaml for Phase 2:

logging:
  log_file: "C:\\Intel\\parquet\\stock_ingestion.log"
  log_format: "text"  # "text" or "json" (new)

server:
  flight_host: "127.0.0.1"
  flight_port: 8815
  query_timeout_seconds: 120  # new, default 120, no row limit

# reports section unchanged — Parquet/xlsx deferred to Phase 3

r2:  # new section
  enabled: false
  bucket_name: ""
  prefix: "stock-ingestion/"

scheduler:  # new section
  enabled: false
  schedule_time: "16:30"
  timezone: "America/New_York"
```

---

## DAG Dependency Graph

```
Cycle 0 (Table Rename + Hardening)
  F7-T0 (rename) -> F7-T0b (CAST) -> F7-T0c (batch INSERT) -> F7-T0d (--strict) -> F7-T0e (critic)
    |
    v
Cycle 1 (JSON Logging)
  F7-T1 + F7-T1b (parallel) -> F7-T1c
    |
    v
Cycle 2 (Health Check + Audit)
  F7-T2 + F7-T2b (parallel) -> F7-T2c
    |
    v
Cycle 3 (Query Timeout)
  F7-T3 + F7-T3b (parallel) -> F7-T3c
    |
    v
Cycle 4 (R2 Sync)
  F7-T4 + F7-T4b (parallel) -> F7-T4c
    |
    v
Cycle 5 (Scheduler)
  F7-T5 + F7-T5b (parallel) -> F7-T5c
    |
    v
Cycle 6 (do_put)
  F7-T6 + F7-T6b (parallel) -> F7-T6c
    |
    v
Cycle 7 (Integration)
  F7-T7 -> F7-T7b
```

---

## Risks

1. **Table rename breaks everything** (Cycle 0): `daily_quotes` → `DAILY_CLOSE` touches every SQL query, every test, the tech spec. Mitigation: executor does a comprehensive grep for `daily_quotes` across all files, renames systematically, runs full test suite. Schema migration for existing DB: `ALTER TABLE daily_quotes RENAME TO DAILY_CLOSE`.

2. **DuckDB multi-process contention** (Cycle 5): Scheduler process and Flight server process both need write access to the same DuckDB file. Mitigation: scheduler acquires write lock briefly, retries with backoff if locked.

3. **R2 credentials in CI** (Cycle 4): Tests must mock `boto3` — never use real R2 credentials in tests. Mitigation: all R2 tests use `unittest.mock.patch`.

4. **Schema migration** (Cycle 2): Adding `ingestion_runs` table requires schema version bump. Mitigation: `DatabaseManager.__init__` already has `schema_version` table — add migration logic.

5. **APScheduler timezone handling** (Cycle 5): Market holidays vs scheduler triggers. Mitigation: reuse existing `pandas_market_calendars` NYSE check inside the scheduled job.

---

## Acceptance Criteria (Phase 2 complete)

1. All Phase 1 tests still pass (updated for DAILY_CLOSE rename)
2. All Phase 2 tests pass (new tests for each cycle)
3. Loop checker: 0 violations on all src/ files
4. Pre-commit hook: active
5. Table renamed: `DAILY_CLOSE` everywhere, no `daily_quotes` references remain
6. R2 sync: works with mocked boto3, auto-syncs DuckDB file after daily_load when enabled, manual `sync_to_r2` action works
7. Scheduler: runs as separate process, triggers daily_load at configured time
8. Health check: returns service status in <100ms, references DAILY_CLOSE
9. Audit table: records every ingestion run
10. JSON logging: configurable, writes to configured log file path
11. Query timeout: 120s default, enforced in do_get, no row limit
12. do_put: accepts external batch data into DAILY_CLOSE with schema validation
13. Blackboard: STATE.md + EVENTS.log updated for each cycle
14. No secrets committed
