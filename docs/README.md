# stock-ingestion

A quantitative stock-data ingestion service that centralizes daily equity data from `yfinance` into a local DuckDB store, exposes programmatic reads through Apache Arrow Flight, and synchronizes the completed database artifact to Cloudflare R2.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the Flight Server](#running-the-flight-server)
  - [Running the Flight Server as a Windows Scheduled Task](#running-the-flight-server-as-a-windows-scheduled-task)
  - [Running the Scheduler (Standalone)](#running-the-scheduler-standalone)
  - [Scheduling the Daily Load on Windows](#scheduling-the-daily-load-on-windows)
  - [Querying via Arrow Flight (do_get)](#querying-via-arrow-flight-do_get)
  - [Triggering Ingestion Actions (do_action)](#triggering-ingestion-actions-do_action)
  - [Uploading Data via Arrow Flight (do_put)](#uploading-data-via-arrow-flight-do_put)
  - [R2 Synchronization](#r2-synchronization)
  - [Interactive Report App (Streamlit)](#interactive-report-app-streamlit)
  - [Market Breadth Dashboard](#market-breadth-dashboard)
  - [RRG Dashboard](#rrg-dashboard)
- [Database Schema](#database-schema)
- [Testing](#testing)
- [Loop Checker](#loop-checker)
- [Multi-Agent Workflow](#multi-agent-workflow)
- [Known Non-Blocking Concerns](#known-non-blocking-concerns)

---

## Overview

`stock-ingestion` is a production-grade data pipeline for quantitative stock analysis. It:

1. **Downloads** daily OHLCV/close data from `yfinance` for a configurable universe of tickers.
2. **Stores** canonical daily data in a local DuckDB file with upsert semantics and split-adjustment tracking.
3. **Serves** read-only SQL queries to concurrent consumers via Apache Arrow Flight.
4. **Syncs** the completed DuckDB file to Cloudflare R2 as a durable artifact.
5. **Schedules** automatic daily loads via APScheduler in a separate process.

The project was built using a coordinated multi-agent workflow (executor → challenger → critic) with durable Git/blackboard state, ensuring every feature passed structured acceptance criteria before release.

---

## Architecture

```
                    yfinance
                       |
                       v
              IngestionEngine
                       |
                       v
                 DuckDB (local file)
                   /        \
                  v          v
    Arrow Flight Server    R2 Sync
    (read-only queries)    (DuckDB file upload)
                  ^
                  |
            Consumers
    (pandas, polars, etc.)

    APScheduler (separate process)
         |
         v
    IngestionEngine.run_daily_load()
```

### Key design decisions

- **DuckDB as canonical store** — no Parquet export; the DuckDB file is the single source of truth.
- **R2 sync only after completion** — intermediate states are never uploaded.
- **Read-only Flight endpoint** — `do_get` accepts SELECT/WITH queries only; no DML or DDL.
- **Query timeout** — configurable, default 120 seconds, no row limit.
- **Scheduler as separate process** — does not start the Flight server; opens its own DuckDB connection with lock retry/backoff.
- **SQL > vectorized > loop** — enforced by an AST-based loop checker and pre-commit hook.

---

## Features

### Data Ingestion

- **Initial load** — fetches `initial_load_years` years of daily OHLCV data for all configured tickers (default 6; this repo's `config.yaml` sets 10).
- **Daily load** — fetches the latest trading day's data. Two modes:
  - **Trading-day gated (default)** — skips weekends and NYSE market holidays. Used by the APScheduler daemon and GitHub Actions CI.
  - **Catch-up (`force=True`)** — runs even on non-trading days so a machine that was off on a trading day downloads the missed data on the next day it is on. Uses the configured `daily_lookback_days` window (default 4). Used by the local Windows scheduled task.
- **On-request load** — fetches a custom date range for specific tickers.
- **Automatic retry** — after the first batch download, tickers that came back empty (e.g. yfinance rate-limiting reporting valid tickers as "possibly delisted; no price data found") are retried once in a follow-up download after a ~15s cool-down, single-threaded for gentler pacing. Recovered tickers move from `tickers_failed` to `tickers_succeeded` and their rows are merged into the result.
- **Split adjustment** — tracks processed splits in a dedicated table to avoid double-adjustment. Re-downloads happen **before** any delete, and only tickers that returned data are replaced — a failed or empty re-download preserves existing history and leaves the split unprocessed for retry.
- **Ticker exclusion** — an `exclude_tickers` list filters out unwanted tickers during ingestion and `do_put`.
- **Purge safeguard** — an excluded ticker that returns empty data is only purged after a cheap independent confirmation (a small single-ticker `yf.download` over a recent window) also finds no data. If the check returns rows or errors unexpectedly, the delisting is "unconfirmed" — the ticker stays in `tickers_failed` and its stored history is preserved (a rate-limit throttle can never wipe real data).

### Arrow Flight Server

- **`do_get`** — read-only SQL queries returning Arrow record batches. SELECT/WITH statements only. Configurable query timeout (default 120s, no row limit).
- **`do_put`** — accepts external Arrow record batches matching the `daily_close` schema (7 columns), validates schema, filters excluded tickers, and upserts in a single vectorized operation.
- **`do_action`** — ten administrative actions:
  - `initial_load` — run the initial historical load. Depth = `ingestion.initial_load_years` (default 6); an explicit `years` key in the action payload overrides it. `years` must be an integer in `[1, 30]` (bool/non-int rejected); invalid values raise `ValueError` in the engine and surface as a `FlightServerError` — never a silent empty download.
  - `daily_load` — run today's incremental load.
  - `on_request_load` — load a custom date range for specified tickers.
  - `metadata_load` — backfill `company_metadata` from `yf.Ticker(t).info` (see [Company Metadata](#company-metadata)). Payload: `tickers` (list of strings, optional — defaults to all configured tickers), `refresh` (bool, default `false` — `true` re-fetches even present tickers), `delay_s` (number ≥ 0, default 0.5 — pacing between `.info` calls), `max_tickers` (integer ≥ 1, optional cap). Returns `SUCCESS` (no failures), `PARTIAL` (some `tickers_failed`), or `ABORTED` (circuit breaker tripped).
  - `weekly_refresh` — rebuild `weekly_close` and `weekly_indicators` from `daily_close` (see [Weekly Layer](#weekly-layer)). No payload required. Returns `SUCCESS` with row counts, or `FAILED` with an `error` field — the daily tables are unaffected.
  - `daily_report` — generate a top-20 dollar-volume report as JSON.
  - `health_check` — return server uptime, DB status, ticker count, row count, schema version.
  - `data_health` — run the full `daily_close` data-health report (see [Data Health Checks](#data-health-checks)).
  - `get_run_history` — return the ingestion audit log as JSON.
  - `sync_to_r2` — manually trigger an R2 sync of the DuckDB file.

### R2 Synchronization

- Direct DuckDB file upload to Cloudflare R2 via `boto3` S3-compatible API.
- Credentials read from environment variables only — never committed.
- Auto-sync after `daily_load` succeeds (SUCCESS or PARTIAL) when `r2.enabled` is true.
- Manual sync via the `sync_to_r2` Flight action.

### Scheduler

- APScheduler `BackgroundScheduler` triggers `daily_load` at a configurable time (default 16:30 ET).
- Checks the NYSE market calendar before running — skips market holidays.
- Opens its own read-write DuckDB connection; retries with exponential backoff if the file is locked.
- Graceful shutdown on SIGINT/SIGTERM.
- Runs as a standalone process (`scheduler_main.py`) — does not start the Flight server.

### Local Scheduled Task (Windows)

- `scripts/register_daily_task.ps1` registers a Windows Task Scheduler job via `schtasks.exe` (idempotent, `/f` replaces).
- `scripts/register_flight_service.ps1` registers a second, service-like task (`StockIngestion-FlightServer`) that starts the Flight server at system boot (AtStartup trigger, restart-on-failure, no time limit).
- Reads `scheduler.schedule_time` + `scheduler.timezone` from `config.yaml` and converts to **machine-local time** (handles DST; warns on invalid/ambiguous local times).
- Invokes `scripts/daily_load_once.py` — a one-shot runner that runs one daily-load cycle with `force=True` (catch-up mode, 4-day lookback) then exits. When the Flight service is running it routes the load through the service's `daily_load` action so the service stays the single DuckDB writer; otherwise it opens DuckDB directly.
- Exit codes are Task Scheduler-visible: `1` on Flight server error/timeout, DuckDB lock exhaustion, or uncaught exception (full traceback to the configured log file), `0` otherwise.
- See [Scheduling the Daily Load on Windows](#scheduling-the-daily-load-on-windows).

### Logging

- Configurable format: `text` (default) or `json` (via `python-json-logger`).
- Dual output: console (stderr) + file at a configurable path.
- All modules use standard `logging.getLogger(__name__)`.
- Every load run (`initial_load`, `daily_load`, `on_request_load`) ends with a `WARNING`-level summary naming failed tickers when any exist — e.g. `Daily load finished with 6 failed tickers: [...]` — so PARTIAL outcomes are visible in the log without querying `errors_json`.

### Audit Trail

- `ingestion_runs` table records every ingestion run with:
  - run type, start/completion timestamps, status, rows upserted, tickers succeeded/failed, errors, duration.
- Queryable via the `get_run_history` Flight action.

### Data Health Checks

`DatabaseManager.check_daily_close_health` runs SQL-driven, **report-only** health checks against `daily_close` — findings are never repaired and never change a load's status:

- **Staleness** — tickers whose `MAX(Date)` is older than the latest *ended* NYSE trading day (via `pandas_market_calendars`; the reference date/calendar is injectable for tests).
- **Coverage** — configured tickers absent from `daily_close` (excluding `exclude_tickers`).
- **Integrity** — rows with NULL/non-positive OHLC, `High < Low`, or `Volume < 0`.
- **Duplicates** — `(Ticker, Date)` pairs with count > 1 (defensive; the primary key normally prevents them).
- **Gaps** (full mode only) — per-ticker missing NYSE sessions inside a trailing `gap_days`-session window (default 30), via a SQL anti-join against the generated calendar.

A session counts as expected only once it has **ended** — i.e. `market_close + grace_minutes <= now` (`grace_minutes` default 120, covering the scheduled ~16:14 ET load completing). Sessions whose close+grace is still in the future are excluded from both the staleness reference and the gap window, so pre-open/early-session calls no longer flag today as missing. `now` is injectable (naive values are treated as UTC); when `as_of` is given without `now`, the whole `as_of` day counts as elapsed.

The report dict carries `health_status` in `{OK, WARN, CHECK_FAILED}` plus per-check counts and bounded per-ticker lists (first 50, with totals). `pending_session` names the NYSE session excluded for not yet having ended (`null` when every session counts — e.g. a post-close call or an injected `trading_days`). `CHECK_FAILED` means the check itself could not run — it never masks or crashes a completed load.

- **Full report on demand** — the `data_health` Flight action returns the full report including gap detection; the payload may carry `gap_days` (integer ≥ 1).
- **Embedded in loads** — completed `daily_load` and `initial_load` results include a `health` key with the light report (staleness + coverage + integrity + duplicates, no gaps). `SKIPPED` results and `on_request_load` carry no health block. The server logs a `WARNING` whenever `health_status != OK`.
- **`daily_load_once.py`** — logs a `WARNING` naming the health status when a Flight-routed load reports `health_status != OK`; the exit code is unchanged.

### Company Metadata

`company_metadata` stores per-ticker reference data fetched from `yf.Ticker(t).info` — `company_name` (from `longName`), `sector`, `industry`, and `quote_type` (from `quoteType`), keyed by `ticker`. It is populated by `IngestionEngine.run_metadata_load`, reachable only through the Flight `metadata_load` action so the server stays the single DuckDB writer.

Rate-limit safety (a port of the legacy `load_company_metadata_to_duckdb3b.py` job, hardened):

- **Skip-existing by default** — without `refresh`, only tickers missing from `company_metadata` are fetched (a SQL anti-join against that table — *not* `daily_close`, so a ticker with prices but no metadata is still fetched).
- **Pacing** — `delay_s` seconds are slept between `.info` calls (default 0.5). yfinance has no batch API for `.info`, so each ticker is a separate call.
- **Circuit breaker** — `max_consecutive_failures` (default 15) fetch exceptions in a row aborts the run early with `aborted=True` and a `reason`; unprocessed tickers stay missing for a later retry.
- **No NULL-row poisoning** — a fetch *exception* writes no row (unlike the legacy job, which wrote a NULL-field row that permanently poisoned skip-existing under rate-limiting). A *successful* but empty/sparse `.info` still writes a NULL-field row, matching the legacy behaviour for genuinely unknown symbols.
- **Incremental flush** — rows upsert in `batch_size` chunks (default 50) during the loop plus a final flush, so an aborted run keeps the progress it already wrote.
- **`max_tickers`** caps the fetch list — useful for smoke tests.

Bridging: completed `initial_load` and `daily_load` runs (SUCCESS and PARTIAL only — never `SKIPPED`, never `on_request_load`) end with a best-effort metadata top-up: configured tickers missing from `company_metadata` are fetched and the result is merged under a `metadata` key on the load result. Any bridge failure is logged as a warning and reported as `{"status": "FAILED", ...}` — it can never fail a completed load.

On-demand CLI (Flight-only — never opens DuckDB directly):

```bash
python scripts/load_company_metadata.py                # missing tickers only
python scripts/load_company_metadata.py --refresh      # re-fetch all configured
python scripts/load_company_metadata.py --tickers AAPL,MSFT --refresh
python scripts/load_company_metadata.py --limit 5 --delay 1.0   # smoke test
```

`scripts/load_company_metadata.bat` is the double-clickable Windows wrapper (flags pass through, e.g. `load_company_metadata.bat --refresh`).

### Weekly Layer

`weekly_close` and `weekly_indicators` persist the legacy weekly aggregation + indicator stack (ported verbatim from `libs/vw_pipe_data_duckdb.py`: `aggregate_to_weekly` and `update_weekly_indicators_01`) directly inside `stock_ingestion.duckdb`.

- **Friday dating** — every weekly bar is keyed by the *nominal Friday* of its ISO week: `(date_trunc('week', Date)::DATE + 4)`. `date_trunc('week', ...)` yields the Monday; `+4` lands on Friday. Holiday weeks whose sessions end earlier still carry the Friday key, and the current partial week is always present once any daily row exists.
- **`weekly_close`** — per `(week, ticker)`: `arg_min(Open, Date)` / `arg_max(Close, Date)` for first-open/last-close, `max(High)`, `min(Low)`, `sum(Volume)` (`Volume` is DOUBLE to match `daily_close`; legacy used BIGINT — same values). Rebuilt via `INSERT ... ON CONFLICT (Date, Ticker) DO UPDATE`, so re-runs are idempotent.
- **`weekly_indicators`** — the `_01` column set under the base name: Slow Stochastic (`Slow_K`, `Slow_D`, `Stoch_Crossover`, `Slow_D_80_20_Crossover`), Bollinger Bands (`BB_Upper`/`BB_Central`/`BB_Lower`, `BB_Reversal_Signal`), PPO (`PPO`, `PPO_Signal`, `PPO_Hist_Crossover`, `PPO_Zero_Crossover`), Force Index (`Force_Index`, `Force_Zero_Crossover`) and log-close EMAs (`Log_Close_EMA18`, `Log_Close_EMA40`). Rebuilt via `INSERT OR REPLACE` over a single CTE chain reading `weekly_close`.
- **Refresh** — `IngestionEngine.run_weekly_refresh()` calls `rebuild_weekly_close()` then `rebuild_weekly_indicators()` (order matters: indicators read `weekly_close`) and returns `{status, weekly_close_rows, weekly_indicators_rows, duration_ms}`. All work is pure SQL.
- **Bridging** — completed `initial_load`, `daily_load`, and `on_request_load` runs (SUCCESS/PARTIAL; never `SKIPPED`) end with a best-effort weekly refresh merged under a `weekly` key on the load result, exactly like the `metadata` bridge. Any failure is logged as a warning and reported as `{"status": "FAILED", ...}` — it can never fail a completed load.
- **On demand** — the `weekly_refresh` Flight action (empty payload) or the CLI:

```bash
python scripts/refresh_weekly.py
```

`scripts/refresh_weekly.bat` is the double-clickable Windows wrapper.

---

## Project Structure

```
stock-ingestion/
├── src/
│   ├── __init__.py
│   ├── config.py              # Pydantic v2 config models + YAML loader
│   ├── config.yaml            # Default configuration file
│   ├── db.py                  # DatabaseManager — DuckDB schema, migrations, queries
│   ├── ingestion.py           # IngestionEngine — yfinance download + split adjustment
│   ├── flight_server.py       # MarketDataFlightServer — Arrow Flight do_get/do_put/do_action
│   ├── r2_sync.py             # R2SyncManager — Cloudflare R2 DuckDB file upload
│   ├── scheduler.py           # PipelineScheduler — APScheduler daily load
│   ├── scheduler_main.py      # Standalone scheduler entrypoint
│   ├── main.py                # Flight server entrypoint
│   ├── store.py               # CSV store (legacy, Phase 0)
│   ├── config_challenger.py   # Challenger variant of config (review reference)
│   ├── db_challenger.py       # Challenger variant of DatabaseManager (review reference)
│   ├── flight_server_challenger.py  # Challenger Flight server variant (review reference)
│   ├── ingestion_challenger.py # Challenger ingestion variant (review reference)
│   └── store_challenger.py    # Challenger store variant (review reference)
├── tests/
│   ├── test_breadth_dashboard.py  # Market breadth dashboard (SQL/EMA/indicators/figure/CLI)
│   ├── test_config.py          # initial_load_years + market_breadth_years config fields
│   ├── test_config_breaking.py
│   ├── test_db.py
│   ├── test_delisted_lifecycle.py
│   ├── test_do_put.py
│   ├── test_flight_server.py
│   ├── test_health_check.py
│   ├── test_ingestion.py
│   ├── test_integration.py
│   ├── test_logging.py
│   ├── test_lookback_days.py
│   ├── test_metadata.py
│   ├── test_phase2_integration.py
│   ├── test_poll_tasks.py
│   ├── test_query_timeout.py
│   ├── test_r2_sync.py
│   ├── test_report_app.py     # Streamlit report app (SQL builders, fetch fallback, xlsx, RRG tab)
│   ├── test_rrg_dashboard.py  # RRG dashboard (presets/SQL/RRG math/figure/LLM payload/CLI)
│   ├── test_scheduler.py
│   ├── test_store_csv.py
│   └── test_weekly.py          # Weekly layer: Friday keys, OHLCV agg, indicators, bridge
├── scripts/
│   ├── breadth_dashboard.bat   # Double-click wrapper for breadth_dashboard.py
│   ├── breadth_dashboard.py    # Market breadth HTML dashboard (port of legacy breadth_builder02.py, Flight-first)
│   ├── check_loops.py          # AST-based loop justification checker
│   ├── ci_daily_load.py        # GitHub Actions one-shot daily load (R2 download/upload)
│   ├── daily_load_once.py      # Windows Task one-shot runner (Flight-first routing, catch-up mode, exit codes)
│   ├── data_health.bat         # Double-click wrapper for data_health_report.py
│   ├── load_company_metadata.bat  # Double-click wrapper for load_company_metadata.py
│   ├── data_health_report.py   # On-demand daily_close health report via the data_health Flight action
│   ├── data_health_verify.bat  # Double-click wrapper for data_health_verify.py
│   ├── data_health_verify.py   # Probes flagged gaps vs yfinance: download_gap vs halt_or_suspension
│   ├── load_company_metadata.py   # company_metadata backfill via the metadata_load Flight action
│   ├── poll_tasks.py           # Multi-agent task lifecycle manager
│   ├── pre_commit_check_loops.py  # Pre-commit hook wrapper
│   ├── register_daily_task.ps1 # schtasks registration (timezone conversion, idempotent)
│   ├── refresh_weekly.bat      # Double-click wrapper for refresh_weekly.py
│   ├── refresh_weekly.py       # weekly_close + weekly_indicators rebuild via the weekly_refresh Flight action
│   ├── register_flight_service.ps1  # AtStartup service task for the Flight server (restart-on-failure)
│   ├── report_app.py           # Streamlit report app (2 daily + 3 weekly RVOL/regime tabs + Market Breadth + RRG)
│   ├── report_top25_active.py  # Top-25 active xlsx report — "Avg Trade Money ($M)" column (Flight-first, read-only DuckDB fallback)
│   ├── rrg_dashboard.bat       # Double-click wrapper for rrg_dashboard.py
│   ├── rrg_dashboard.py        # RRG HTML+JSON dashboards (port of legacy plot_rrg_chart_gemini10b.py, Flight-first)
│   ├── run_flight_server.py    # cwd-safe wrapper launching src.main.main() for the service task
│   └── workflow_phase2.py      # Dynamic Workflow orchestration script
├── .github/workflows/
│   ├── daily_load.yml          # CI daily load (cron, trading days)
│   └── verify_secrets.yml      # R2 secrets sanity check
├── devin_blackboard/
│   ├── STATE.md                # Canonical project state
│   ├── EVENTS.log              # Append-only event log
│   ├── TASKS.json              # Task definitions and status
│   └── locks/                  # Task claim locks
├── docs/
│   ├── dev_plan/               # Phase 1 and Phase 2 development plans
│   ├── tech_spec/              # Technical specifications
│   ├── workflow_idea/          # Multi-agent workflow documentation and role prompts
│   └── DATA_SERVICE_API.md     # Consumer-facing data-service contract (schema + Flight API)
├── AGENTS.md                   # Multi-agent blackboard protocol rules
├── requirements.txt
└── README.md
```

---

## Installation

### Prerequisites

- Python 3.10+
- pip

### Steps

```bash
git clone https://github.com/vialliw/stock-ingestion.git
cd stock-ingestion
pip install -r requirements.txt
```

### Dependencies

| Package | Purpose |
|---------|---------|
| `duckdb` | Local columnar database |
| `pyarrow` | Arrow Flight server + record batches |
| `yfinance` | Equity data download |
| `pandas` | Data manipulation |
| `pandas-market-calendars` | NYSE market calendar |
| `scipy` | RRG dashboard spline trails |
| `pydantic` | Configuration validation |
| `pyyaml` | YAML config loading |
| `boto3` | Cloudflare R2 S3-compatible sync |
| `python-json-logger` | JSON log format |
| `APScheduler` | Daily load scheduling |
| `openpyxl` | Excel (xlsx) report output |
| `streamlit` | Interactive report app |
| `plotly` | Market breadth dashboard charts |

---

## Configuration

All configuration is in `src/config.yaml`, loaded by `PipelineConfig.from_yaml()` with Pydantic v2 validation.

```yaml
database:
  duckdb_dir: "C:\Intel\parquet"
  duckdb_filename: "stock_ingestion.duckdb"

ingestion:
  tickers:
    - "AAPL"
    - "MSFT"
    - "NVDA"
  exclude_tickers:
    - "AVB"
  daily_lookback_days: 4    # trailing calendar-day refresh window for daily_load
  initial_load_years: 10    # historical depth for initial_load (default 6, valid range 1-30)

server:
  flight_host: "127.0.0.1"
  flight_port: 8815
  query_timeout_seconds: 120

logging:
  log_file: "C:\Intel\parquet\stock_ingestion.log"
  log_format: "text"  # or "json"

reports:
  report_dir: "C:\Intel\parquet"
  market_breadth_years: 3   # trailing-years window for the market-breadth dashboard (0 = all data)

r2:
  enabled: false
  bucket_name: ""
  prefix: "stock-ingestion/"

scheduler:
  enabled: false
  schedule_time: "16:30"
  timezone: "America/New_York"
```

### Environment Variables (R2)

R2 credentials are read from environment variables only — never stored in config files or code:

| Variable | Description |
|----------|-------------|
| `R2_ACCESS_KEY_ID` | R2 access key |
| `R2_SECRET_ACCESS_KEY` | R2 secret key |
| `R2_ENDPOINT_URL` | R2 S3-compatible endpoint URL |

---

## Usage

### Running the Flight Server

```bash
python -m src.main
```

This loads `src/config.yaml`, configures logging, opens the DuckDB database, and starts the Arrow Flight daemon on the configured host/port. SIGINT/SIGTERM triggers a safe WAL checkpoint and clean exit.

### Running the Flight Server as a Windows Scheduled Task

To keep the Flight server running like a service on a local Windows machine, register the `StockIngestion-FlightServer` task (run from an **elevated** prompt — the task runs as `SYSTEM`):

```powershell
# Register (idempotent — re-running replaces the existing task)
powershell -ExecutionPolicy Bypass -File scripts\register_flight_service.ps1

# Remove the task
powershell -ExecutionPolicy Bypass -File scripts\register_flight_service.ps1 -Unregister
```

The registration uses the PowerShell `ScheduledTasks` cmdlets with:

- **AtStartup trigger** — the server starts at boot, before any user logs on.
- **Restart on failure** — 3 restart attempts, 1 minute apart.
- **No execution time limit** — `ExecutionTimeLimit` is zero, so the daemon is never killed for running too long.
- **`MultipleInstances = IgnoreNew`** — a duplicate launch request is dropped, so two server processes can never fight over the DuckDB write lock.
- **Absolute `python.exe`** — the interpreter is resolved at registration time (`Get-Command python`), so the task does not depend on `PATH`.
- **`scripts\run_flight_server.py`** — a cwd-safe wrapper that anchors the repo root on `sys.path`, makes it the working directory, then calls `src.main.main()`.

The task runs as `SYSTEM` whether or not a user is logged on. Note that **user-scoped environment variables are not visible to `SYSTEM`** — if `r2.enabled` is true, set `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and `R2_ENDPOINT_URL` at machine scope.

The service is **headless-safe**: the task runs with no console attached (`sys.stdout`/`sys.stderr` are `None`), so `scripts\run_flight_server.py` redirects any missing stdio stream to the null device before startup — guarding every library that touches stdio (the observed crash was the `yf.download` progress-bar finalizer calling `sys.stderr.flush()` *after* a download completed). Both ingestion engines additionally call `yf.download(..., progress=False)`, so the tqdm progress bar never engages.

> **Single writer:** while this service task is registered, do **not** also run `python -m src.scheduler_main` (the standalone APScheduler daemon). The Flight server holds the DuckDB write lock for its whole lifetime, and `scheduler_main.py` would fight it for that lock. The daily scheduled task below already routes its load through the running service.

When the service is up, `scripts/daily_load_once.py` sends `do_action("daily_load", {"force": true})` to `grpc://{server.flight_host}:{server.flight_port}` instead of opening DuckDB itself — the service performs the catch-up load and stays the single writer. If the service is unreachable (short connect probe fails), the runner logs the fallback and opens DuckDB directly as before.

### Running the Scheduler (Standalone)

```bash
python -m src.scheduler_main
```

This starts the APScheduler process. It does **not** start the Flight server. The scheduler triggers `daily_load` at the configured time (default 16:30 ET), checks the NYSE calendar, and opens its own DuckDB connection with lock retry/backoff.

### Scheduling the Daily Load on Windows

For a local machine that is not on every day, register a Windows scheduled task instead of running the daemon:

```powershell
# Register (reads schedule_time + timezone from src/config.yaml)
powershell -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1

# Custom time (bypasses timezone conversion — treated as local time)
powershell -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1 -Time 16:30

# Remove the task
powershell -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1 -Unregister
```

The registration script converts `scheduler.schedule_time` from `scheduler.timezone` (e.g. `16:14 America/New_York`) to machine-local time, accounting for DST. The task invokes `scripts/daily_load_once.py`, which:

- **Routes through the Flight service first** — when `StockIngestion-FlightServer` (above) is running, the runner calls `do_action("daily_load", {"force": true})` on `grpc://{server.flight_host}:{server.flight_port}` so the service stays the single DuckDB writer. `SUCCESS`/`PARTIAL` → exit `0`; a server error or call timeout → exit `1`. Only when the service is unreachable does it open DuckDB directly.
- Runs one daily-load cycle with **`force=True`** — it does **not** skip on non-trading days, so a machine that was off on a trading day catches up on the next day it is on (e.g. PC off Friday → Saturday run downloads Friday's data). The window is `ingestion.daily_lookback_days` (default 4 calendar days).
- On the direct path, retries the DuckDB open with backoff; exits `1` if the lock cannot be acquired or an unexpected exception occurs (full traceback in the configured log file) so Task Scheduler records the failure.
- Logs to `logging.log_file` in the configured text/JSON format.

This differs from the GitHub Actions path (`scripts/ci_daily_load.py`), which still skips on market-closed days — CI runs on a weekday cron schedule, so catching up is unnecessary there.

### Migrating from the legacy Pipe01 job

This project replaces the legacy `projects\jobs\Pipe01.get.data.via.DuckDB.py` daily-ingestion job. The equivalent of `python projects\jobs\Pipe01.get.data.via.DuckDB.py <db>` is the `StockIngestion-DailyLoad` scheduled task (or `python scripts/daily_load_once.py` manually), which writes `daily_close` into `stock_ingestion.duckdb` through the Flight service's `daily_load` action.

- **Coverage parity verified** — after the 10-year `initial_load`, every configured ticker's `daily_close` in `stock_ingestion.duckdb` has `MIN(Date)`/`MAX(Date)` coverage at least equal to the legacy file's; no backfill was needed.
- **Legacy file frozen** — `pipe01.get.data.via.duckdb.duckdb` is no longer updated. Scripts that read it keep working on stale data; `weekly_close`/`weekly_indicators_*` consumers should move to a later migration stage.
- **Deliberate deviation** — the legacy job ran `CHECKPOINT`/`VACUUM` after each load; this project relies on DuckDB's automatic per-commit checkpointing. Run `VACUUM` manually while the service is stopped if the file ever needs compacting.

### Querying via Arrow Flight (do_get)

> **For external consumers:** [`docs/DATA_SERVICE_API.md`](docs/DATA_SERVICE_API.md) is the complete consumer-facing contract — endpoint, query protocol, full schema reference, action payloads, price-basis caveats, and recipes. Other projects integrating with this service should start there.

Connect to the Flight server and send a JSON ticket containing a `sql` field:

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://127.0.0.1:8815")
ticket = flight.Ticket(b'{"sql": "SELECT * FROM daily_close WHERE Ticker = \'AAPL\' LIMIT 10"}')
reader = client.do_get(ticket)
table = reader.read_all()
print(table.to_pandas())
```

**Rules:**
- Only `SELECT` and `WITH` (CTE) statements are accepted.
- Query timeout defaults to 120 seconds (configurable via `server.query_timeout_seconds`).
- No `LIMIT` is injected — the caller controls result size.
- Timeout raises `FlightServerError` with a descriptive message.

### Triggering Ingestion Actions (do_action)

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://127.0.0.1:8815")

# Initial historical load — omit "years" to use ingestion.initial_load_years;
# an explicit "years" must be an integer in [1, 30]
action = flight.Action("initial_load", b'{"years": 6}')
results = list(client.do_action(action))
print(json.loads(results[0].body.to_pybytes()))

# Daily incremental load
action = flight.Action("daily_load", b'{}')
results = list(client.do_action(action))
print(json.loads(results[0].body.to_pybytes()))

# On-request load for specific tickers
action = flight.Action("on_request_load", b'{"tickers": ["AAPL"], "start_date": "2024-01-01", "end_date": "2024-06-01"}')
results = list(client.do_action(action))

# Health check
action = flight.Action("health_check", b'{}')
results = list(client.do_action(action))

# Full daily_close data-health report (incl. gap detection);
# "gap_days" is optional — defaults to 30 trailing NYSE sessions
action = flight.Action("data_health", b'{"gap_days": 30}')
results = list(client.do_action(action))

# Company-metadata backfill — missing tickers only by default;
# all payload keys are optional (tickers/refresh/delay_s/max_tickers)
action = flight.Action("metadata_load", b'{"refresh": false, "delay_s": 0.5}')
results = list(client.do_action(action))

# Weekly layer rebuild (weekly_close + weekly_indicators); empty payload
action = flight.Action("weekly_refresh", b'{}')
results = list(client.do_action(action))

# Get ingestion run history
action = flight.Action("get_run_history", b'{}')
results = list(client.do_action(action))

# Manual R2 sync
action = flight.Action("sync_to_r2", b'{}')
results = list(client.do_action(action))

# Daily report (top-20 dollar volume)
action = flight.Action("daily_report", b'{"date": "2024-09-06"}')
results = list(client.do_action(action))
```

### Uploading Data via Arrow Flight (do_put)

Upload Arrow record batches matching the `daily_close` schema:

```python
import pyarrow as pa
import pyarrow.flight as flight

schema = pa.schema([
    pa.field("Date", pa.date32()),
    pa.field("Ticker", pa.string()),
    pa.field("Open", pa.float64()),
    pa.field("High", pa.float64()),
    pa.field("Low", pa.float64()),
    pa.field("Close", pa.float64()),
    pa.field("Volume", pa.float64()),
])

data = pa.table({
    "Date": pa.array([18600], type=pa.date32()),
    "Ticker": pa.array(["AAPL"]),
    "Open": pa.array([170.0], type=pa.float64()),
    "High": pa.array([175.0], type=pa.float64()),
    "Low": pa.array([169.0], type=pa.float64()),
    "Close": pa.array([174.0], type=pa.float64()),
    "Volume": pa.array([50000000.0], type=pa.float64()),
}, schema=schema)

client = flight.FlightClient("grpc://127.0.0.1:8815")
descriptor = flight.FlightDescriptor.for_path("daily_close")
writer, _ = client.do_put(descriptor, schema)
writer.write_table(data)
writer.close()
```

**Rules:**
- Schema must match exactly (7 columns, correct types) — mismatch raises `FlightServerError`.
- Tickers in `exclude_tickers` are filtered out with a warning log.
- Data is upserted in a single vectorized operation via `DatabaseManager.upsert_dataframe`.
- No per-row loops — uses `reader.read_all()` + `to_pandas()` + `upsert_dataframe()`.

### R2 Synchronization

R2 sync uploads the DuckDB file directly to Cloudflare R2:

- **Automatic** — after `daily_load` succeeds (SUCCESS or PARTIAL), if `r2.enabled` is true.
- **Manual** — via the `sync_to_r2` Flight action.
- **Credentials** — read from `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT_URL` environment variables.

Set environment variables before starting the server:

```bash
export R2_ACCESS_KEY_ID="your-access-key"
export R2_SECRET_ACCESS_KEY="your-secret-key"
export R2_ENDPOINT_URL="https://<account-id>.r2.cloudflarestorage.com"
```

Enable in config:

```yaml
r2:
  enabled: true
  bucket_name: "your-bucket"
  prefix: "stock-ingestion/"
```

### Interactive Report App (Streamlit)

`scripts/report_app.py` is a Streamlit app that renders the RVOL edge reports from `docs/report_idea/` plus the market-breadth and RRG dashboards as seven interactive tabs — the three weekly reports first, then the two daily ones, then Market Breadth, then Relative Rotation (RRG):

- **Weekly — Combined Regimes** — the combined weekly report (`report_weekly_trends.md`): all four non-Normal weekly volume regimes among the top-K most-liquid names, ordered by regime priority (Strong Acceleration, Acceleration, Strong Deceleration, Deceleration — matching the spec's recommended reading order) then `rvol` descending.
- **Weekly — Acceleration** — weekly acceleration candidates only (`report_weekly_trend_accelerating.md`), ordered by `rvol` descending.
- **Weekly — Deceleration** — weekly deceleration candidates only (`report_weekly_trend_decelerating.md`), ordered by `rvol` ascending (quietest names first).
- **Daily — Top 25 by Dollar Volume** — top-N tickers ranked by latest-day dollar volume, enriched with RVOL, relative dollar volume, pct change, a `volume_regime` classification (Strong Acceleration / Acceleration / Normal / Deceleration / Strong Deceleration), and share of total average dollar volume.
- **Daily — Acceleration Candidates** — keeps only `rvol >= threshold` names among the top-K most-liquid tickers by latest dollar volume, ordered by `rvol` descending.
- **Market Breadth** — the 5-panel breadth dashboard ported verbatim (SQL / indicator math / figure / guide) from the legacy `breadth_builder02.py`: SPY candlestick + 50-day EMA, % of stocks above their 50-/200-day SMA, net advances & volume ratio, and new highs vs. new lows over a 252-day lookback. Rendered via `st.plotly_chart`; a "Years of history" widget (default `reports.market_breadth_years`, `0` = all data) applies a post-query trailing-years cutoff.
- **Relative Rotation (RRG)** — the animated Relative Rotation Graph ported verbatim (presets / `weekly_close` query / RS-Ratio & RS-Momentum math / spline trails / quadrant annotations) from the legacy `plot_rrg_chart_gemini10b.py`. A preset selectbox picks one of six ticker universes; period, trail-length, lookback-years and benchmark widgets default to the preset's values. Rendered via `st.plotly_chart`, with a latest-positions stats table and the LLM analysis-prompt JSON in an expander plus a download button. Requires the persisted `weekly_close` table.

The weekly tabs aggregate `daily_close` into weekly bars (`DATE_TRUNC('week', Date)`, first-Open/last-Close via `ARRAY_AGG`, summed volume and dollar volume, `LAG`-based weekly pct change) and average over the last N **weeks**. Note the weekly deceleration defaults differ from the daily ones: `strong_decel=0.50` / `decel=0.70` (daily uses 0.6 / 0.8).

**Guides:** a top-level "About / How to read these reports" expander under the app title renders the generic sections of `report_basic.md` verbatim (purpose, recommended usage, methodology — the session-specific data tables are excluded), and each of the six report tabs renders its own source spec's interpretation guide verbatim in an "Interpretation guide" expander (the RRG tab instead shows its LLM analysis-prompt JSON in an expander).

Launch it with:

```bash
streamlit run scripts/report_app.py
```

Each tab exposes interactive parameters that feed into the generated SQL (numeric values only): lookback periods/weeks (default 30), top-N limit (daily top-25 tab, default 25), the four RVOL regime thresholds (daily defaults 2.5 / 1.8 / 0.8 / 0.6; weekly decel defaults 0.50 / 0.70), the liquidity top-K for the candidates/weekly tabs (default 50), and a `volume_regime` multiselect filter on the daily top-25 tab. Results render as a dataframe plus a bar chart of the key metric (`latest_dollar_volume` / `rvol`) — the chart is drawn via `st.altair_chart` with `sort=None` on the Ticker axis so the bars follow the query's `ORDER BY` (the table's default order), falling back to `st.vega_lite_chart` if Altair is unavailable — with a download button that produces an in-memory `.xlsx` (auto-sized columns via openpyxl) and a "Refresh data" button that clears the cached query result.

**Data access:** the app queries the running Flight server first (`do_get` on `grpc://{server.flight_host}:{server.flight_port}`, 5-second availability probe, 120-second call bound) — **the Flight server must be running for the primary path**, and it is required whenever another process holds the DuckDB write lock. If the server is unreachable or the query fails, the app falls back to a direct *read-only* DuckDB connection (`duckdb.connect(db_path, read_only=True)`); if both fail, the tab shows an error.

### Market Breadth Dashboard

`scripts/breadth_dashboard.py` is the standalone (non-Streamlit) generator for the same 5-panel market breadth dashboard shown in the app's "Market Breadth" tab — a Flight-sourced port of the legacy `breadth_builder02.py`. It runs the two queries (universe breadth metrics excluding SPY, plus SPY OHLC) through the same Flight-first / read-only-DuckDB-fallback fetch path as the report app, applies the trailing-years cutoff in pandas *after* the query (the SMA_200 / 252-day lookback windows need pre-cutoff history), and writes a self-contained interactive HTML file.

```bash
python scripts/breadth_dashboard.py        # uses reports.market_breadth_years (default 3)
python scripts/breadth_dashboard.py 5      # explicit trailing-years override
python scripts/breadth_dashboard.py 0      # all history
```

Or double-click `scripts/breadth_dashboard.bat` (optional years argument passes through).

Output: **`Stock_Ingestion_Market_Breadth.html`** written to `reports.report_dir` (`C:\Intel\parquet` in the shipped config) — the filename is fixed, not derived from the database name. The file embeds plotly.js plus the full interpretation guide.

### RRG Dashboard

`scripts/rrg_dashboard.py` is the standalone (non-Streamlit) generator for the animated Relative Rotation Graph shown in the app's "Relative Rotation (RRG)" tab — a Flight-sourced port of the legacy `plot_rrg_chart_gemini10b.py`. It queries the persisted `weekly_close` table through the same Flight-first / read-only-DuckDB-fallback fetch path as the report app, computes the legacy RS-Ratio / RS-Momentum series verbatim, and writes a self-contained animated HTML file (spline trails + diamond heads, benchmark bars, stats and guide tables, per-date frames with a slider and play/pause, quadrant annotations) plus an LLM analysis-prompt JSON.

Six presets mirror the legacy `__main__` configurations verbatim (all use `period=14`, `trail=3`):

| Preset | Universe | Benchmark | Lookback | Output prefix |
|--------|----------|-----------|----------|---------------|
| `ETFs` | 85 thematic/sector ETFs | `SPY` | 1 year | `Relative_Rotation_Dashboard_ETFs` |
| `sector1` | 17 sector ETFs | `SPY` | 3 years | `Relative_Rotation_Dashboard_sector1` |
| `sector2` | 17 sector ETFs | `SGOV` | 3 years | `Relative_Rotation_Dashboard_sector2` |
| `sector3` | 7 broad ETFs | `SPY` | 3 years | `Relative_Rotation_Dashboard_sector3` |
| `Semi` | 35 semiconductor names | `SMH` | 3 years | `Relative_Rotation_Dashboard_Semi` |
| `AI` | 41 AI-theme names | `XLK` | 3 years | `Relative_Rotation_Dashboard_AI` |

```bash
python scripts/rrg_dashboard.py                         # all six presets
python scripts/rrg_dashboard.py --dashboard sector1     # one preset
python scripts/rrg_dashboard.py --dashboard Semi --trail 5 --years 1 --benchmark SMH
```

Or double-click `scripts/rrg_dashboard.bat` (flags pass through). Per-preset failures are logged and the batch continues; the exit code is `1` only if every selected preset fails.

Output per preset: **`<prefix>.html`** and **`<prefix>_analysis_prompt.json`** written to `reports.report_dir` (`C:\Intel\parquet` in the shipped config). The HTML loads plotly.js from a CDN (`include_plotlyjs='cdn'`); the JSON is the legacy `gen_ai_prompt` payload (per-ticker quadrant, velocity, persistence metrics for LLM analysis). Requires `weekly_close` (see [Weekly Layer](#weekly-layer)) and `scipy` (spline trails — lazy import).

---

## Database Schema

The DuckDB database contains seven tables:

### `daily_close` (canonical OHLCV data)

| Column | Type | Description |
|--------|------|-------------|
| `Date` | DATE | Trading date (primary key) |
| `Ticker` | VARCHAR | Ticker symbol (primary key) |
| `Open` | DOUBLE | Opening price |
| `High` | DOUBLE | High price |
| `Low` | DOUBLE | Low price |
| `Close` | DOUBLE | Closing price |
| `Volume` | DOUBLE | Trading volume |

Primary key: `(Date, Ticker)` — upsert semantics on conflict.

### `splits_processed` (split adjustment tracking)

| Column | Type | Description |
|--------|------|-------------|
| `Ticker` | VARCHAR | Ticker symbol (primary key) |
| `SplitDate` | DATE | Date of the split (primary key) |
| `SplitFactor` | DOUBLE | Split ratio |
| `ProcessedAt` | TIMESTAMP | When the split was processed |

### `ingestion_runs` (audit trail)

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | BIGINT | Auto-incremented run ID (primary key) |
| `run_type` | VARCHAR | `initial_load`, `daily_load`, or `on_request_load` |
| `started_at` | TIMESTAMP | Run start time |
| `completed_at` | TIMESTAMP | Run completion time |
| `status` | VARCHAR | `SUCCESS`, `PARTIAL`, `SKIPPED`, or `UNKNOWN` |
| `rows_upserted` | INTEGER | Total rows upserted |
| `tickers_succeeded` | INTEGER | Tickers successfully loaded |
| `tickers_failed` | INTEGER | Tickers that failed |
| `errors_json` | VARCHAR | JSON-encoded error details |
| `duration_ms` | INTEGER | Run duration in milliseconds |

### `company_metadata` (per-ticker reference data)

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | VARCHAR | Ticker symbol (primary key) |
| `company_name` | VARCHAR | Company name (from `yf.Ticker.info["longName"]`) |
| `sector` | VARCHAR | Sector (nullable) |
| `industry` | VARCHAR | Industry (nullable) |
| `quote_type` | VARCHAR | Quote type, e.g. `EQUITY`/`ETF` (from `quoteType`) |

Populated by the `metadata_load` Flight action (see [Company Metadata](#company-metadata)). Upsert semantics on `ticker`; a successful-but-empty `.info` writes a NULL-field row so the ticker stops counting as missing, while a fetch exception writes no row so it stays retriable.

### `weekly_close` (Friday-dated weekly OHLCV)

| Column | Type | Description |
|--------|------|-------------|
| `Date` | DATE | Nominal Friday of the ISO week — `(date_trunc('week', Date)::DATE + 4)` (primary key) |
| `Ticker` | VARCHAR | Ticker symbol (primary key) |
| `Open` | DOUBLE | First open of the week (`arg_min(Open, Date)`) |
| `High` | DOUBLE | Week high (`max(High)`) |
| `Low` | DOUBLE | Week low (`min(Low)`) |
| `Close` | DOUBLE | Last close of the week (`arg_max(Close, Date)`) |
| `Volume` | DOUBLE | Summed weekly volume (`sum(Volume)`) |

Rebuilt from `daily_close` by `rebuild_weekly_close()` (`INSERT ... ON CONFLICT DO UPDATE` — idempotent). See [Weekly Layer](#weekly-layer).

### `weekly_indicators` (weekly technical indicators)

| Column | Type | Description |
|--------|------|-------------|
| `Date` | DATE | Nominal Friday of the ISO week (primary key) |
| `Ticker` | VARCHAR | Ticker symbol (primary key) |
| `Slow_K` | DOUBLE | Slow stochastic %K (3-week mean of 14-week fast %K) |
| `Slow_D` | DOUBLE | Slow stochastic %D (3-week mean of Slow_K) |
| `Stoch_Crossover` | INTEGER | Slow_K/Slow_D crossover: +1 up, -1 down, 0 none |
| `Slow_D_80_20_Crossover` | INTEGER | Stochastic crossover gated on 80/20 threshold crossings |
| `BB_Upper` | DOUBLE | Bollinger upper band (20-week mean + 2σ) |
| `BB_Central` | DOUBLE | Bollinger central band (20-week mean) |
| `BB_Lower` | DOUBLE | Bollinger lower band (20-week mean - 2σ) |
| `BB_Reversal_Signal` | INTEGER | Close re-entering the bands: +1 from below, -1 from above |
| `PPO` | DOUBLE | Percentage Price Oscillator (12/26-week) |
| `PPO_Signal` | DOUBLE | 9-week signal line of PPO |
| `PPO_Hist_Crossover` | INTEGER | PPO/signal crossover: +1 up, -1 down |
| `PPO_Zero_Crossover` | INTEGER | PPO zero-line crossing: +1 up, -1 down |
| `Force_Index` | DOUBLE | Force Index — `(Close - prev Close) * Volume` |
| `Force_Zero_Crossover` | INTEGER | Force Index zero-line crossing: +1 up, -1 down |
| `Log_Close_EMA18` | DOUBLE | `LN(Close / EMA18)` (18-week mean) |
| `Log_Close_EMA40` | DOUBLE | `LN(Close / EMA40)` (40-week mean) |

Rebuilt from `weekly_close` by `rebuild_weekly_indicators()` (`INSERT OR REPLACE` over the legacy `update_weekly_indicators_01` CTE chain — the `_01` column set under the base name).

### `schema_version` (migration tracking)

| Column | Type | Description |
|--------|------|-------------|
| `version` | INTEGER | Schema version number |
| `applied_at` | TIMESTAMP | When the migration was applied |

Current schema version: 5. The database auto-migrates from v1 (legacy `daily_quotes` table) to v2 (rename to `daily_close`) to v3 (add `ingestion_runs` table) to v4 (add `company_metadata` table) to v5 (add `weekly_close` and `weekly_indicators` tables).

---

## Testing

```bash
# Full test suite
python -m pytest tests/ -q

# Specific test module
python -m pytest tests/test_flight_server.py -q

# With verbose output
python -m pytest tests/ -v
```

The test suite includes 787 tests covering:

| Test File | Coverage |
|-----------|----------|
| `test_breadth_dashboard.py` | Market breadth dashboard: verbatim SQL, EMA seeding, indicators, figure, CLI |
| `test_config.py` | `initial_load_years` + `market_breadth_years` config fields (both impls) |
| `test_config_breaking.py` | Config validation, edge cases, breaking changes |
| `test_db.py` | DatabaseManager: schema, migrations, upsert, queries |
| `test_delisted_lifecycle.py` | Delisted ticker handling |
| `test_do_put.py` | do_put schema validation, exclude filtering, upsert |
| `test_flight_server.py` | do_get SELECT validation, do_action routing |
| `test_health_check.py` | health_check + get_run_history actions |
| `test_ingestion.py` | IngestionEngine: initial/daily/on-request loads |
| `test_integration.py` | End-to-end Flight server integration |
| `test_logging.py` | JSON/text logging, log file path |
| `test_lookback_days.py` | daily_lookback_days config, load windows, force/catch-up flag |
| `test_metadata.py` | company_metadata load: skip-existing, refresh, pacing, circuit breaker, batch flush |
| `test_phase2_integration.py` | Phase 2 end-to-end: scheduler → load → audit → R2 → health |
| `test_poll_tasks.py` | Task lifecycle manager (claim/done/blocked/stale) |
| `test_query_timeout.py` | Query timeout enforcement, no LIMIT injection |
| `test_r2_sync.py` | R2 sync (mocked boto3), auto-sync, manual sync |
| `test_report_app.py` | Streamlit report app: SQL builders, fetch fallback, xlsx, Market Breadth + RRG tabs |
| `test_rrg_dashboard.py` | RRG dashboard: verbatim presets, weekly_close SQL, RRG math, figure, LLM payload, CLI |
| `test_scheduler.py` | PipelineScheduler (mocked APScheduler) |
| `test_store_csv.py` | CSV store (legacy) |
| `test_weekly.py` | Weekly layer: Friday-dated weekly_close, weekly_indicators, refresh ordering, bridge |

All external services (yfinance, boto3, APScheduler) are mocked in tests — no real network calls.

---

## Loop Checker

The project enforces a **SQL > vectorized > loop** performance hierarchy using an AST-based loop checker.

```bash
# Normal mode (warns on unjustified loops)
python scripts/check_loops.py --src-dir src

# Strict mode (exits non-zero on unjustified loops)
python scripts/check_loops.py --src-dir src --strict
```

Any `for` loop in `src/*.py` must have a `# justified: <reason>` comment on the `for` line or within 6 lines above it. Exempt patterns: `range()`, `enumerate()`, `zip()`, `.items()`, `.values()`, `.keys()`, constant list/tuple literals.

A pre-commit hook (`.git/hooks/pre-commit`) runs the checker and blocks commits with unjustified loops.

---

## Multi-Agent Workflow

This project was built using a coordinated multi-agent workflow with durable Git/blackboard state. Agents B/C/D are spawned by the orchestrator as **local subagents** via the Devin CLI's `run_subagent` tool — no Devin Cloud session, no org quota, no cost beyond the local session's model usage. (Devin Cloud MCP sessions via `devin_session_create` are an optional alternative when org quota is available — see `docs/workflow_idea/devin_platform_setup.md`.)

### Roles

| Role | Agent | Spawned via | Responsibility | Edits |
|------|-------|-------------|---------------|-------|
| A | Orchestrator (PM) | Main Devin CLI session | Plans cycles, spawns subagents, verifies completion | Blackboard only |
| B | Executor | `run_subagent` + `subagent_general` | Implements features in `src/` | `src/`, `STATE.md` |
| C | Challenger | `run_subagent` + `subagent_general` | Reviews executor output, reports concerns | None (review only) |
| D | Critic | `run_subagent` + `subagent_general` | Verifies acceptance criteria, final gate | `tests/` |

`subagent_general` is write-capable (required — every role must claim its task and update the blackboard). `subagent_explore` is a cheap read-only profile used only for watchers. Role prompts live in `docs/workflow_idea/agent_<role>_prompt.md` — the single source of truth; `.devin/agents/{executor,challenger,critic}.md` profiles and Devin Cloud playbooks are thin pointers to them, so subagents are spawned with `run_subagent(profile="executor", ...)`.

### Workflow

```
B (executor) → C (challenger) → D (critic)
```

1. Orchestrator writes task definitions to `TASKS.json`.
2. Orchestrator spawns Agent B via `run_subagent` (`subagent_general`); B claims its task with `poll_tasks.py --claim`, implements the feature, verifies with `--verify`, and marks it done with `--done --evidence`.
3. Orchestrator spawns Agent C; C reviews B's output and reports PASS/FAIL/CONCERNS via `--done`/`--blocked`.
4. Orchestrator spawns Agent D; D verifies all acceptance criteria with concrete evidence and marks done or blocked.
5. All task lifecycle changes are durable in Git (auto-committed and pushed via `poll_tasks.py`).

### Blackboard Files

| File | Purpose |
|------|---------|
| `devin_blackboard/STATE.md` | Canonical project state |
| `devin_blackboard/EVENTS.log` | Append-only event log |
| `devin_blackboard/TASKS.json` | Task definitions, status, acceptance criteria |
| `devin_blackboard/locks/` | Atomic task claim locks |

### Task Lifecycle

```bash
# Claim a task
python scripts/poll_tasks.py --claim <task_id> <agent>

# Verify acceptance criteria
python scripts/poll_tasks.py --verify <task_id>

# Mark done with evidence
python scripts/poll_tasks.py --done <task_id> <agent> --evidence "<summary>"

# Mark blocked with reason
python scripts/poll_tasks.py --blocked <task_id> <agent> --reason "<reason>"

# Check for ready tasks
python scripts/poll_tasks.py B   # or C, D
```

All lifecycle commands automatically commit and push to `origin main`.

---

## Known Non-Blocking Concerns

These issues were identified during review but are non-blocking. They may be addressed in a future hardening cycle:

1. **Pydantic boolean coercion** — `query_timeout_seconds` may accept booleans (Pydantic coerces `true` to `1`).
2. **Prefix-only SQL validation** — `do_get` validates only the SQL prefix; trailing statements after a semicolon may execute.
3. **Daemon worker continuation** — the query worker thread may continue running after `threading.Timer` signals a timeout.
4. **Audit status `UNKNOWN`** — some ingestion result dictionaries omit a `status` key, causing the audit row to record `UNKNOWN`.
5. **DuckDB API deprecation** — `fetch_arrow_table()` may be deprecated in DuckDB 1.5.5.
6. **R2 exception handling** — `sync_to_r2` does not wrap credential/file/bucket exceptions in `FlightServerError`.
7. **R2 PARTIAL zero-row sync** — auto-sync triggers on `PARTIAL` even when `total_rows == 0`.
8. **Cross-platform key building** — `_build_key` uses `os.path.basename` which may not split Windows backslash paths on Linux.
9. **Scheduler broad exception retry** — `_create_db_manager` retries on any `Exception`, not only DuckDB lock errors.
10. **Scheduler shutdown** — `shutdown(wait=False)` may not await long-running jobs before process exit.
11. **Scheduler timezone validation** — invalid timezone strings crash on `scheduler.start()` instead of being caught earlier.
12. **do_put read_all** — uses `reader.read_all()` instead of `pyarrow.Table.from_batches()` (functionally equivalent).
13. **health_check column swap** — `get_health_info()` swaps `schema_version` and `last_run` columns, causing JSON serialization issues in some cases.

---

## License

See repository for license details.
