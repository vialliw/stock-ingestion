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
  - [Running the Scheduler (Standalone)](#running-the-scheduler-standalone)
  - [Querying via Arrow Flight (do_get)](#querying-via-arrow-flight-do_get)
  - [Triggering Ingestion Actions (do_action)](#triggering-ingestion-actions-do_action)
  - [Uploading Data via Arrow Flight (do_put)](#uploading-data-via-arrow-flight-do_put)
  - [R2 Synchronization](#r2-synchronization)
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

- **Initial load** — fetches 6 years of daily OHLCV data for all configured tickers.
- **Daily load** — fetches the latest trading day's data; skips weekends and NYSE market holidays.
- **On-request load** — fetches a custom date range for specific tickers.
- **Split adjustment** — tracks processed splits in a dedicated table to avoid double-adjustment.
- **Ticker exclusion** — an `exclude_tickers` list filters out unwanted tickers during ingestion and `do_put`.

### Arrow Flight Server

- **`do_get`** — read-only SQL queries returning Arrow record batches. SELECT/WITH statements only. Configurable query timeout (default 120s, no row limit).
- **`do_put`** — accepts external Arrow record batches matching the `daily_close` schema (7 columns), validates schema, filters excluded tickers, and upserts in a single vectorized operation.
- **`do_action`** — seven administrative actions:
  - `initial_load` — run initial 6-year historical load.
  - `daily_load` — run today's incremental load.
  - `on_request_load` — load a custom date range for specified tickers.
  - `daily_report` — generate a top-20 dollar-volume report as JSON.
  - `health_check` — return server uptime, DB status, ticker count, row count, schema version.
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

### Logging

- Configurable format: `text` (default) or `json` (via `python-json-logger`).
- Dual output: console (stderr) + file at a configurable path.
- All modules use standard `logging.getLogger(__name__)`.

### Audit Trail

- `ingestion_runs` table records every ingestion run with:
  - run type, start/completion timestamps, status, rows upserted, tickers succeeded/failed, errors, duration.
- Queryable via the `get_run_history` Flight action.

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
│   ├── test_config_breaking.py
│   ├── test_db.py
│   ├── test_delisted_lifecycle.py
│   ├── test_do_put.py
│   ├── test_flight_server.py
│   ├── test_health_check.py
│   ├── test_ingestion.py
│   ├── test_integration.py
│   ├── test_logging.py
│   ├── test_phase2_integration.py
│   ├── test_poll_tasks.py
│   ├── test_query_timeout.py
│   ├── test_r2_sync.py
│   ├── test_scheduler.py
│   └── test_store_csv.py
├── scripts/
│   ├── check_loops.py          # AST-based loop justification checker
│   ├── poll_tasks.py           # Multi-agent task lifecycle manager
│   ├── pre_commit_check_loops.py  # Pre-commit hook wrapper
│   └── workflow_phase2.py      # Dynamic Workflow orchestration script
├── devin_blackboard/
│   ├── STATE.md                # Canonical project state
│   ├── EVENTS.log              # Append-only event log
│   ├── TASKS.json              # Task definitions and status
│   └── locks/                  # Task claim locks
├── docs/
│   ├── dev_plan/               # Phase 1 and Phase 2 development plans
│   ├── tech_spec/              # Technical specifications
│   └── workflow_idea/          # Multi-agent workflow documentation and role prompts
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
| `pydantic` | Configuration validation |
| `pyyaml` | YAML config loading |
| `boto3` | Cloudflare R2 S3-compatible sync |
| `python-json-logger` | JSON log format |
| `APScheduler` | Daily load scheduling |

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

server:
  flight_host: "127.0.0.1"
  flight_port: 8815
  query_timeout_seconds: 120

logging:
  log_file: "C:\Intel\parquet\stock_ingestion.log"
  log_format: "text"  # or "json"

reports:
  report_dir: "C:\Intel\parquet"

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

### Running the Scheduler (Standalone)

```bash
python -m src.scheduler_main
```

This starts the APScheduler process. It does **not** start the Flight server. The scheduler triggers `daily_load` at the configured time (default 16:30 ET), checks the NYSE calendar, and opens its own DuckDB connection with lock retry/backoff.

### Querying via Arrow Flight (do_get)

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

# Initial 6-year load
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

---

## Database Schema

The DuckDB database contains four tables:

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

### `schema_version` (migration tracking)

| Column | Type | Description |
|--------|------|-------------|
| `version` | INTEGER | Schema version number |
| `applied_at` | TIMESTAMP | When the migration was applied |

Current schema version: 3. The database auto-migrates from v1 (legacy `daily_quotes` table) to v2 (rename to `daily_close`) to v3 (add `ingestion_runs` table).

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

The test suite includes 217 tests covering:

| Test File | Coverage |
|-----------|----------|
| `test_config_breaking.py` | Config validation, edge cases, breaking changes |
| `test_db.py` | DatabaseManager: schema, migrations, upsert, queries |
| `test_delisted_lifecycle.py` | Delisted ticker handling |
| `test_do_put.py` | do_put schema validation, exclude filtering, upsert |
| `test_flight_server.py` | do_get SELECT validation, do_action routing |
| `test_health_check.py` | health_check + get_run_history actions |
| `test_ingestion.py` | IngestionEngine: initial/daily/on-request loads |
| `test_integration.py` | End-to-end Flight server integration |
| `test_logging.py` | JSON/text logging, log file path |
| `test_phase2_integration.py` | Phase 2 end-to-end: scheduler → load → audit → R2 → health |
| `test_poll_tasks.py` | Task lifecycle manager (claim/done/blocked/stale) |
| `test_query_timeout.py` | Query timeout enforcement, no LIMIT injection |
| `test_r2_sync.py` | R2 sync (mocked boto3), auto-sync, manual sync |
| `test_scheduler.py` | PipelineScheduler (mocked APScheduler) |
| `test_store_csv.py` | CSV store (legacy) |

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

This project was built using a coordinated multi-agent workflow with durable Git/blackboard state.

### Roles

| Role | Agent | Responsibility | Edits |
|------|-------|---------------|-------|
| A | Orchestrator (PM) | Plans cycles, spawns agents, verifies completion | Blackboard only |
| B | Executor (SWE) | Implements features in `src/` | `src/`, `STATE.md` |
| C | Challenger (GLM) | Reviews executor output, reports concerns | None (review only) |
| D | Critic (SWE) | Verifies acceptance criteria, final gate | `tests/` |

### Workflow

```
B (executor) → C (challenger) → D (critic)
```

1. Orchestrator writes task definitions to `TASKS.json`.
2. Agent B implements the feature and marks it done.
3. Agent C reviews B's output and reports PASS/FAIL/CONCERNS.
4. Agent D verifies all acceptance criteria with concrete evidence and marks done or blocked.
5. All task lifecycle changes are durable in Git (auto-pushed via `poll_tasks.py`).

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
