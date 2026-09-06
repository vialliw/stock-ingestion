# System Architecture Specification: Centralized Quantitative Data Ingestion Pipeline (yfinance + DuckDB + Apache Arrow Flight)

**Document Version:** 1.0.2

**Status:** Technical Specification — Phase 1

**Architecture Pattern:** Local IPC Server via PyArrow Flight

---

## Changelog

### 1.0.0 → 1.0.1

| Change | Reason |
|---|---|
| Table schema revised to 7 columns: `Date, Ticker, Open, High, Low, Close, Volume` | PM-defined canonical schema |
| `adj_close` column removed | Not in revised schema |
| `dollar_volume` generated column removed from table | Computed inline in report query |
| `updated_at` timestamp column removed | Not in revised schema |
| Primary key order changed to `(Date, Ticker)` | Per revised schema |
| Column names capitalized | Per revised schema |

### 1.0.1 revision

| Change | Reason |
|---|---|
| `log_file` parameter added to configuration | PM requirement: configurable log file location |
| New ticker detection added to `daily_load` | PM requirement: backfill new config tickers from aligned start date |
| Config model restructured to nested Pydantic models matching YAML | Fix: flat model did not match nested YAML structure |
| YAML config loader added (`PipelineConfig.from_yaml`) | Fix: `config.yaml` was never loaded |
| `do_get` now uses a read-only DuckDB connection | Fix: prevent write operations via read endpoint |
| Manual stock split adjustment restored with `splits_processed` tracking table | Fix: `auto_adjust=True` only adjusts newly fetched data; historical data already in DuckDB before a split date must be retroactively adjusted. Tracking table prevents double-adjustment. |
| `do_put` / `IngestBatch` removed from API | Deferred to Phase 2 |
| `timeframe` config parameter removed | Schema is daily-only; intraday deferred |
| `threading.Lock` added around all write operations | Fix: Arrow Flight serves concurrent requests |
| `APScheduler` scheduling deferred to Phase 2 | Daily load is triggered manually via Flight action for Phase 1 |
| R2 sync deferred to Phase 2 | Core function works locally first |
| API field names standardized to `start_date` / `end_date` | Fix: API table said `start`, code used `start_date` |
| `daily_load` action added to API table | Fix: was implemented but missing from API docs |
| Config defaults aligned (`tickers=[]`, `exclude_tickers=[]`) | Fix: schema table and code disagreed |
| `is_us_market_open_today()` standalone function removed | Fix: duplicated inline check in `run_daily_load` |
| `report_dir` added to configuration | PM requirement: report path must be configurable |
| Trailing 4-day refresh always runs after empty-DB initial load | PM correction: system may be offline for days |
| Initial load SQL changed from `INSERT ON CONFLICT` to `MERGE INTO` | Fix: spec text and code now agree on single upsert pattern |

### 1.0.1 → 1.0.2

| Change | Reason |
|---|---|
| `Volume` column type changed from `BIGINT` to `DOUBLE` | Review: yfinance returns float volumes after split adjustments; BIGINT would truncate |
| yfinance `end` date adjusted to `end_date + 1 day` | Review fix: yfinance `end` parameter is exclusive; passing today would miss today's bar |
| Batch download via `yf.download()` replaces per-ticker `stock.history()` | Review: batch call is significantly faster for multi-ticker universes |
| Per-ticker loop retained only for split detection (`stock.splits`) | Splits are per-ticker metadata; no batch API exists |
| `do_get` SELECT-only validation added | Review: read-only connection prevents writes, but SELECT-only guard also blocks expensive non-SELECT statements |
| Action results now include `rows_upserted`, `tickers_succeeded`, `tickers_failed`, `duration_ms` | Review: clients need richer status for monitoring |
| SQL queries truncated to 200 chars in logs | Review: avoid logging large/sensitive payloads |
| Redundant `idx_quotes_date_ticker` index removed | Review: DuckDB already indexes the primary key |
| `schema_version` table added | Review: prepare for future schema migrations |
| Soft-fail WARNING log added for non-excluded empty tickers | Review: operators need visibility into prolonged data outages |
| Pydantic validators added: tickers uppercased/deduplicated, port range 1–65535 | Review: config validation hardening |
| Log messages standardized to `RUNTIME EVENT:` prefix | Review: inconsistent prefixes between purge and split paths |
| Known Limitations section added | Review: document yfinance data quality, single-node, etc. |
| Architecture diagram annotated: Phase 1 uses external triggering | Review: clarify that no internal scheduler runs in Phase 1 |

### 1.0.2 revision (same version)

| Change | Reason |
|---|---|
| Split handling changed from SQL UPDATE re-adjustment to DELETE + re-download | PM decision: DELETE all ticker rows and re-download from aligned start date with `auto_adjust=True` is simpler and less error-prone than SQL UPDATE math |
| `adjust_for_split()` SQL UPDATE method removed | Replaced by `delete_ticker_data()` + batch re-download with `auto_adjust=True` |
| `splits_processed` table retained for idempotency | Still needed to avoid re-downloading every run |
| Split adjustment concurrency note removed | No longer relevant — DELETE + re-download is atomic under write lock |

---

## 1. System Overview

This specification details the design for a centralized quantitative market data ingestion pipeline. The system ingests daily equity pricing data from `yfinance`, stores historical state in a single local DuckDB database file, and serves concurrent local downstream clients (N read-only worker processes) via an Apache Arrow Flight IPC server.

By wrapping DuckDB inside a single PyArrow Flight service process, the system bypasses DuckDB's single-process file-locking constraints while maintaining zero-copy data transfer speeds for analytics tasks.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      Flight Data Daemon (FlightServer)                           │
│                                                                                  │
│   ┌────────────────────────┐      ┌─────────────────────┐     ┌──────────────┐   │
│   │ Scheduler (Phase 2)    │      │  DuckDB Engine      │     │ Flight RPC   │   │
│   │ - US Market Calendar   │─────▶│  (RW Lock + RO Pool)│◀────│ Endpoint     │   │
│   └────────────────────────┘      └──────────┬──────────┘     └──────▲───────┘   │
└──────────────────────────────────────────────┼───────────────────────┼───────────┘
                                               │                       │
                                               ▼                       │ (gRPC IPC)
                                    ┌──────────────────────┐           │
                                    │  market_data.duckdb  │           │
                                    └──────────────────────┘           │
           ┌───────────────────────────────────────────────────────────┴───────────┐
           │                                                                       │
┌──────────┴───────────────┐                             ┌─────────────────────────┴────────┐
│ yfinance Ingest Engine   │                             │ RO Client Processes (1..N)       │
│ (Internal / On-Request)  │                             │ (Dashboards / Backtest Workers)  │
└──────────────────────────┘                             └──────────────────────────────────┘
```

> **Phase 1 note:** The Scheduler block is shown for architectural completeness but is NOT active in Phase 1. Daily loads are triggered externally (OS cron, systemd timer, or manual Flight action). The `pandas_market_calendars` NYSE check still runs inside `daily_load` to skip non-trading days.

---

## 2. Scope

### Phase 1 (this specification)

- Configuration via `config.yaml` with Pydantic validation, nested schema, and field validators
- DuckDB local storage with the 7-column `daily_quotes` schema + `schema_version` + `splits_processed` tables
- yfinance batch ingestion: initial load, daily rolling load, on-request load
- New ticker detection and aligned backfill in daily load
- Delisted ticker purge with soft-fail logging for non-excluded empty tickers
- Stock split detection and historical data re-adjustment with idempotent tracking
- Apache Arrow Flight read service (`do_get`) with SELECT-only validation and read-only DuckDB connections
- Flight actions for ingestion triggers and report generation (`do_action`) with rich status results
- Top 20 dollar volume daily report
- Graceful shutdown with DuckDB checkpoint
- Configurable log file and report directory locations
- Thread-safe write operations via `threading.Lock`

### Deferred (Phase 2+)

| Feature | Reason |
|---|---|
| R2 sync | Core function works locally first; R2 is a sync target after processing completes |
| APScheduler automated scheduling | Daily load is triggered manually or via OS-level cron for Phase 1 |
| `do_put` / `IngestBatch` | External batch ingestion not needed for Phase 1 |
| Intraday timeframes | Schema is daily-only; `interval="1d"` is hardcoded |
| `ingestion_runs` audit table | Useful for observability but not core function |
| Parquet report output | JSON is sufficient for Phase 1 |
| Query timeout / max row limit | Add when query patterns are understood |
| mTLS authentication | Flight server is localhost-only for Phase 1 |
| Structured JSON logging | Plain text logging is sufficient for Phase 1 |
| Health check action | Add in observability phase |

---

## 3. Configuration

Configuration management uses Pydantic for schema validation. The Pydantic model is nested to match the YAML structure exactly. A `from_yaml` classmethod loads `config.yaml` and falls back to defaults if the file is absent. Field validators enforce data hygiene.

### 3.1 Configuration Schema

| Section | Parameter | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `database` | `duckdb_dir` | `str` | `"./data"` | Directory containing the DuckDB database file. Created if absent. |
| `database` | `duckdb_filename` | `str` | `"market_data.duckdb"` | DuckDB physical file name. |
| `ingestion` | `tickers` | `List[str]` | `[]` | Primary list of universe tickers. Automatically uppercased and deduplicated. |
| `ingestion` | `exclude_tickers` | `List[str]` | `[]` | Blacklist tickers. Automatically uppercased and deduplicated. |
| `server` | `flight_host` | `str` | `"127.0.0.1"` | Bind IP for Arrow Flight server. |
| `server` | `flight_port` | `int` | `8815` | Listening port for Arrow Flight service. Must be 1–65535. |
| `logging` | `log_file` | `str` | `"./logs/pipeline.log"` | Path to the log file. Parent directory is created if absent. |
| `reports` | `report_dir` | `str` | `"./data/reports"` | Directory for generated report JSON files. Created if absent. |

### 3.2 Example `config.yaml`

```yaml
database:
  duckdb_dir: "./data"
  duckdb_filename: "market_data.duckdb"

ingestion:
  tickers:
    - "AAPL"
    - "MSFT"
    - "NVDA"
  exclude_tickers:
    - "OTC_DELISTED_XYZ"

server:
  flight_host: "127.0.0.1"
  flight_port: 8815

logging:
  log_file: "./logs/pipeline.log"

reports:
  report_dir: "./data/reports"
```

### 3.3 Config Loading Behavior

- On startup, `PipelineConfig.from_yaml("src/config.yaml")` is called.
- If `src/config.yaml` does not exist, all Pydantic defaults are used.
- If `src/config.yaml` exists, its nested keys are mapped directly to the nested Pydantic sub-models.
- Access pattern in code: `config.database.duckdb_dir`, `config.ingestion.tickers`, `config.server.flight_host`, `config.logging.log_file`.

### 3.4 Field Validators

| Field | Validator | Behavior |
|---|---|---|
| `ingestion.tickers` | `field_validator` | Uppercase each ticker, remove duplicates, preserve order. |
| `ingestion.exclude_tickers` | `field_validator` | Uppercase each ticker, remove duplicates, preserve order. |
| `server.flight_port` | `field_validator` | Must be in range 1–65535. Raises `ValueError` if out of range. |

---

## 4. Data Storage Schema (DuckDB)

### 4.1 Table Definition

| column_name | column_type | null | key | default | extra |
|---|---|---|---|---|---|
| Date | DATE | NO | PRI | | |
| Ticker | VARCHAR | NO | PRI | | |
| Open | DOUBLE | YES | | | |
| High | DOUBLE | YES | | | |
| Low | DOUBLE | YES | | | |
| Close | DOUBLE | YES | | | |
| Volume | DOUBLE | YES | | | |

Primary key: `(Date, Ticker)`. DuckDB automatically creates an index for the primary key — no additional index is needed.

> **Volume type note:** `DOUBLE` instead of `BIGINT` because yfinance returns float volumes after split/dividend adjustments. `BIGINT` would silently truncate fractional shares.

```sql
CREATE TABLE IF NOT EXISTS daily_quotes (
    Date DATE NOT NULL,
    Ticker VARCHAR NOT NULL,
    Open DOUBLE,
    High DOUBLE,
    Low DOUBLE,
    Close DOUBLE,
    Volume DOUBLE,
    PRIMARY KEY (Date, Ticker)
);
```

### 4.2 Schema Versioning

A `schema_version` table tracks the current schema version for future migrations:

```sql
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO schema_version (version)
SELECT 1
WHERE NOT EXISTS (SELECT 1 FROM schema_version WHERE version = 1);
```

On startup, `DatabaseManager._init_schema()` creates all Phase 1 tables and records schema version 1. Future schema changes will increment the version and run migration logic.

### 4.3 `splits_processed` Table

```sql
CREATE TABLE IF NOT EXISTS splits_processed (
    Ticker VARCHAR NOT NULL,
    SplitDate DATE NOT NULL,
    SplitFactor DOUBLE NOT NULL,
    ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Ticker, SplitDate)
);
```

### 4.4 Upsert Pattern (used by all ingestion operations)

All ingestion operations (initial, daily, on-request) use the same `MERGE INTO` upsert via `DatabaseManager.upsert_dataframe()`. The dataframe is registered as a DuckDB view named `staging_df`, then merged:

```sql
MERGE INTO daily_quotes AS target
USING staging_df AS source
ON target.Date = source.Date AND target.Ticker = source.Ticker
WHEN MATCHED THEN UPDATE SET
    Open = source.Open,
    High = source.High,
    Low = source.Low,
    Close = source.Close,
    Volume = source.Volume
WHEN NOT MATCHED THEN INSERT
    (Date, Ticker, Open, High, Low, Close, Volume)
VALUES
    (source.Date, source.Ticker, source.Open, source.High, source.Low, source.Close, source.Volume);
```

---

## 5. Ingestion Operations

### 5.1 yfinance End-Date Semantics

yfinance's `end` parameter is **exclusive**. If today is 2026-09-04 and you pass `end="2026-09-04"`, the response excludes September 4th. To include the end date, pass `end = end_date + 1 day`.

All ingestion operations apply this adjustment before calling `yf.download()`:

```python
from datetime import timedelta
end_dt_inclusive = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
```

### 5.2 Batch Download Strategy

Pricing data is fetched via `yf.download()` (batch) rather than per-ticker `stock.history()`. This reduces HTTP round-trips significantly for multi-ticker universes.

```python
df = yf.download(
    tickers=tickers,
    start=start_date,
    end=end_dt_inclusive,   # +1 day because yfinance end is exclusive
    interval="1d",
    auto_adjust=True,
    group_by='ticker',
    threads=True
)
```

The per-ticker loop is retained **only for split detection** (`yf.Ticker(t).splits`), which has no batch API.

### 5.3 Initial Load (`initial_load`)

- **Behavior:** Downloads 6 years of historical daily data for all configured tickers via batch `yf.download()` and upserts into `daily_quotes`.
- **Execution:** Triggered manually via the Flight `do_action` endpoint (`action_type: "initial_load"`). Intended for first-time database population or when the database is empty.
- **yfinance parameters:** `interval="1d"`, `auto_adjust=True`, `group_by='ticker'`.

### 5.4 Daily Rolling Load (`daily_load`)

- **Behavior:** Downloads the trailing 4 calendar days for all configured tickers to account for settlement windows, exchange data adjustments, and late-reported bar updates.
- **New Ticker Detection (backfill before refresh):** Before the trailing-4-day refresh, the daily load checks for tickers in `config.ingestion.tickers` that do not yet exist in the database. If new tickers are found:
  1. Query the aligned start date: `SELECT MIN(Date) FROM daily_quotes` — the earliest date across all existing tickers.
  2. Download historical data for the new tickers from the aligned start date to today.
  3. Upsert via the standard `MERGE INTO` pattern.
  4. Log: `INFO - New ticker(s) detected: [TSLA, AMD]. Backfilling from aligned start date 2020-09-06.`
  5. If the database is empty (no existing tickers), fall back to `initial_load` with 6 years. The trailing 4-day refresh still runs afterwards to ensure the most recent data is present (the system may have been offline for days).
- **Market Calendar Check:** Before any download, the daily load checks the NYSE calendar. If the market is closed (holiday/weekend), the load is skipped entirely.
- **yfinance parameters:** `interval="1d"`, `auto_adjust=True`, `group_by='ticker'`.

### 5.5 On-Request Load (`on_request_load`)

- **Behavior:** Accepts a custom payload defining `tickers: List[str]`, `start_date: str`, and `end_date: str`. Downloads and upserts via the standard `MERGE INTO` pattern.
- **yfinance parameters:** `interval="1d"`, `auto_adjust=True`, `group_by='ticker'`.

### 5.6 Stock Split Detection & Re-download

#### Problem

`auto_adjust=True` only adjusts OHLC data **within a single yfinance API response**. It does NOT retroactively adjust data already stored in DuckDB from previous loads. Without handling, a split creates a discontinuity:

```
Day 1:   initial_load stores day 1–40 (pre-split OHLC)
Day 40:  XYZ splits 2:1
Day 41:  daily_load fetches day 37–40 with auto_adjust=True
         → yfinance returns day 37–40 already adjusted (halved OHLC, doubled Volume)
         → MERGE updates day 37–40 in DB
         → day 1–36 in DB remain pre-split → INCONSISTENT
```

#### Solution: DELETE + Re-download

When a new split is detected for a ticker:

1. **Delete** all existing rows for that ticker from `daily_quotes`.
2. **Re-download** the full history from the aligned start date (earliest `Date` across all tickers in the database) to today, using `auto_adjust=True`.
3. **Upsert** the re-downloaded data.

This is simpler and less error-prone than SQL UPDATE-based re-adjustment:
- No SQL math (no factor direction errors, no comparison operator bugs, no overlap concerns).
- yfinance's `auto_adjust=True` handles all splits correctly in a single pass.
- Multiple compounding splits are handled automatically by yfinance.
- The only cost is re-downloading ~1500 rows (6 years daily) for the affected ticker, which is trivial.

#### Idempotency via `splits_processed` table

A tracking table prevents re-downloading every run (see §4.3 for schema). Each detected split is recorded as `(Ticker, SplitDate, SplitFactor)`. On subsequent runs, already-processed splits are skipped.

#### Detection logic

On every `fetch_and_clean` call, for each ticker:

1. Fetch `yf.Ticker(t).splits` (all historical splits from yfinance).
2. For each split, check if `(ticker, split_date)` exists in `splits_processed`.
3. If any unprocessed split is found for the ticker:
   a. Record all unprocessed splits in `splits_processed`.
   b. Delete all rows: `DELETE FROM daily_quotes WHERE Ticker = ?`
   c. Re-download from aligned start date to today with `auto_adjust=True`.
   d. Upsert the re-downloaded data.
   e. Log: `WARNING - RUNTIME EVENT: Stock split detected for '{ticker}'. Deleted and re-downloaded from {aligned_start} to today.`
4. If all splits are already processed, skip.

This handles all edge cases:
- **Normal case:** split detected on the next daily load after it occurs.
- **Daemon downtime:** all missed splits are detected on the next fetch; a single DELETE + re-download fixes everything.
- **Multiple splits:** yfinance's `auto_adjust=True` handles all compounding splits in one download.
- **Re-runs:** the tracking table ensures no unnecessary re-downloads.

---

## 6. Runtime Events

### 6.1 Stock Split Detection & Re-download

During all ingestion calls (`fetch_and_clean`), for each ticker:

1. Fetch `yf.Ticker(t).splits` — returns all historical split events.
2. For each split not yet in `splits_processed`:
   - Record the split in `splits_processed` to prevent re-processing.
   - Delete all rows for the ticker:
     ```sql
     DELETE FROM daily_quotes WHERE Ticker = :ticker;
     ```
   - Re-download full history from the aligned start date with `auto_adjust=True`.
   - Upsert the re-downloaded data.
   - Log: `WARNING - RUNTIME EVENT: Stock split detected for '{ticker}'. Deleted and re-downloaded from {aligned_start} to today.`

### 6.2 Delisted Ticker Detection & Purge

During all ingestion calls (`fetch_and_clean`):

1. If `yfinance` returns an empty dataframe for a ticker AND that ticker is listed in `config.ingestion.exclude_tickers`:
   - Execute: `DELETE FROM daily_quotes WHERE Ticker = ?`
   - Log: `CRITICAL - RUNTIME EVENT: Delisted ticker data completely purged for '{ticker}'.`
2. If a ticker returns empty data but is NOT in `exclude_tickers`:
   - Log: `WARNING - RUNTIME EVENT: Ticker '{ticker}' returned empty dataset. Not in exclude list — skipped (no purge). This may indicate a temporary data outage.`
   - This avoids accidental deletion of tickers experiencing temporary data outages while giving operators visibility.

---

## 7. Apache Arrow Flight Protocol

Clients communicate with the Flight daemon using JSON-encoded Flight descriptors.

### 7.1 RPC Functions

```
Flight Server Endpoints
├── do_get(ticket)        --> Stream PyArrow Record Batches (Read Queries, read-only DuckDB connection, SELECT-only)
└── do_action(action)     --> Execute Administrative Routines (Ingestion Triggers, Report)
```

`do_put` is not implemented in Phase 1.

### 7.2 API Schema

| Function | Type | Input Descriptor / Ticket JSON | Output Payload |
| --- | --- | --- | --- |
| `QueryData` | `do_get` | `{"sql": "SELECT * FROM daily_quotes WHERE Ticker='AAPL'"}` | PyArrow RecordBatch Stream |
| `TriggerInitLoad` | `do_action` | `Action: "initial_load", Body: {"years": 6}` | ActionResult (see §7.5) |
| `TriggerDailyLoad` | `do_action` | `Action: "daily_load", Body: {}` | ActionResult (see §7.5) |
| `TriggerOnDemand` | `do_action` | `Action: "on_request_load", Body: {"tickers": ["NVDA"], "start_date": "2026-01-01", "end_date": "2026-09-01"}` | ActionResult (see §7.5) |
| `GenerateReport` | `do_action` | `Action: "daily_report", Body: {"date": "2026-09-04"}` | ActionResult (see §7.5) |

### 7.3 Read-Only Enforcement

`do_get` uses two layers of protection:

1. **Read-only DuckDB connection:** `duckdb.connect(db_path, read_only=True)` — DuckDB itself rejects any write operation.
2. **SELECT-only validation:** The SQL string is stripped of leading whitespace and checked to ensure it starts with `SELECT` (case-insensitive). Non-SELECT statements are rejected before reaching DuckDB. This prevents expensive non-SELECT operations (e.g., recursive CTEs, `PRAGMA`, `COPY`) even if they don't modify data.

```python
def _validate_select_only(sql: str) -> None:
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise flight.FlightServerError(
            "Only SELECT and WITH (CTE) statements are allowed via do_get"
        )
```

The connection is closed after the query completes.

### 7.4 Thread Safety

All write operations (`upsert_dataframe`, `purge_ticker`, `delete_ticker_data`, `record_split`, schema initialization, `CHECKPOINT`) are serialized via a `threading.Lock` held by `DatabaseManager`. Read operations use independent read-only connections and do not acquire the lock, allowing concurrent reads from multiple Flight clients.

### 7.5 Action Result Format

All `do_action` responses return JSON with a standard structure:

**Success:**
```json
{
  "status": "SUCCESS",
  "rows_upserted": 1250,
  "tickers_succeeded": ["AAPL", "MSFT", "NVDA"],
  "tickers_failed": [],
  "duration_ms": 3400
}
```

**Partial failure:**
```json
{
  "status": "PARTIAL",
  "rows_upserted": 800,
  "tickers_succeeded": ["AAPL", "MSFT"],
  "tickers_failed": ["NVDA"],
  "errors": {"NVDA": "HTTP 429: Too Many Requests"},
  "duration_ms": 2100
}
```

**Report action:**
```json
{
  "status": "SUCCESS",
  "report": [...],
  "duration_ms": 15
}
```

---

## 8. Reliability & Shutdown

### 8.1 Graceful Shutdown with WAL Checkpoint

To protect DuckDB against Write-Ahead Log (`.wal`) corruption during abrupt host termination (`SIGINT`, `SIGTERM`):

- **Signal Handling:** Catch `SIGINT` and `SIGTERM` signals.
- **Checkpointing Protocol:** On shutdown, acquire the write lock, issue `CHECKPOINT`, and close the read-write connection cleanly.

```python
def shutdown_handler(signum, frame):
    logger.warning(f"Shutdown signal {signum} trapped. Commencing emergency WAL checkpoint...")
    db_manager.close()
    logger.info("Database closed safely. Exiting daemon process.")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)
```

### 8.2 Market Calendar Check

The daily load uses `pandas_market_calendars` with the `NYSE` calendar to determine whether the current day is a trading day. If the market is closed, the daily load is skipped. This check is performed inline in `run_daily_load()`.

---

## 9. Top 20 Dollar Volume Daily Report

The system generates daily ranking reports aggregating dollar volume (`Close * Volume`). `dollar_volume` is computed inline in the query — it is not a stored column.

### 9.1 Report Query

```sql
SELECT
    Ticker,
    Date,
    Close,
    Volume,
    (Close * Volume) AS dollar_volume
FROM daily_quotes
WHERE Date = (SELECT MAX(Date) FROM daily_quotes)
ORDER BY dollar_volume DESC
LIMIT 20;
```

When a specific `target_date` is provided:

```sql
SELECT
    Ticker,
    Date,
    Close,
    Volume,
    (Close * Volume) AS dollar_volume
FROM daily_quotes
WHERE Date = ?
ORDER BY dollar_volume DESC
LIMIT 20;
```

### 9.2 Output Format

Reports are stored as JSON files in `{config.reports.report_dir}/dollar_volume_YYYYMMDD.json` and returned in the Flight action result payload. The report directory is created if it does not exist.

---

## 10. Reference Implementation

Below is the complete Phase 1 reference implementation.

```python
import os
import sys
import json
import signal
import logging
import threading
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta, timezone, date

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import yfinance as yf
import yaml
import pandas_market_calendars as mcal
from pydantic import BaseModel, Field, field_validator

# =====================================================================
# 1. CONFIGURATION
# =====================================================================

class DatabaseConfig(BaseModel):
    duckdb_dir: str = "./data"
    duckdb_filename: str = "market_data.duckdb"


class IngestionConfig(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    exclude_tickers: List[str] = Field(default_factory=list)

    @field_validator("tickers", "exclude_tickers")
    @classmethod
    def normalize_tickers(cls, v: List[str]) -> List[str]:
        """Uppercase, deduplicate, preserve order."""
        seen = set()
        result = []
        for t in v:
            upper = t.strip().upper()
            if upper and upper not in seen:
                seen.add(upper)
                result.append(upper)
        return result


class ServerConfig(BaseModel):
    flight_host: str = "127.0.0.1"
    flight_port: int = 8815

    @field_validator("flight_port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError(f"flight_port must be 1-65535, got {v}")
        return v


class LoggingConfig(BaseModel):
    log_file: str = "./logs/pipeline.log"


class ReportsConfig(BaseModel):
    report_dir: str = "./data/reports"


class PipelineConfig(BaseModel):
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    reports: ReportsConfig = Field(default_factory=ReportsConfig)

    @property
    def db_path(self) -> str:
        os.makedirs(self.database.duckdb_dir, exist_ok=True)
        return os.path.join(self.database.duckdb_dir, self.database.duckdb_filename)

    @classmethod
    def from_yaml(cls, path: str = "src/config.yaml") -> "PipelineConfig":
        if not os.path.exists(path):
            return cls()
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return cls(**data)


config = PipelineConfig.from_yaml()

# =====================================================================
# 2. LOGGING
# =====================================================================

_log_file = config.logging.log_file
_log_dir = os.path.dirname(_log_file)
if _log_dir:
    os.makedirs(_log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_file, mode="a")
    ]
)
logger = logging.getLogger("MarketDataPipeline")

def _truncate_sql(sql: str, max_len: int = 200) -> str:
    """Truncate SQL for logging to avoid large/sensitive log entries."""
    sql = sql.strip()
    if len(sql) <= max_len:
        return sql
    return sql[:max_len] + "...[truncated]"

# =====================================================================
# 3. DUCKDB MANAGER (thread-safe writes, concurrent reads)
# =====================================================================

SCHEMA_VERSION = 1

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._write_lock = threading.Lock()
        self.conn = duckdb.connect(self.db_path, read_only=False)
        self._init_schema()

    def _init_schema(self):
        with self._write_lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_quotes (
                    Date DATE NOT NULL,
                    Ticker VARCHAR NOT NULL,
                    Open DOUBLE,
                    High DOUBLE,
                    Low DOUBLE,
                    Close DOUBLE,
                    Volume DOUBLE,
                    PRIMARY KEY (Date, Ticker)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS splits_processed (
                    Ticker VARCHAR NOT NULL,
                    SplitDate DATE NOT NULL,
                    SplitFactor DOUBLE NOT NULL,
                    ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (Ticker, SplitDate)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            self.conn.execute("""
                INSERT INTO schema_version (version)
                SELECT ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM schema_version WHERE version = ?
                );
            """, [SCHEMA_VERSION, SCHEMA_VERSION])

    def _read_only_conn(self):
        """Open a short-lived read-only connection for concurrent read queries."""
        return duckdb.connect(self.db_path, read_only=True)

    def execute_query(self, query: str) -> pa.Table:
        conn = self._read_only_conn()
        try:
            return conn.execute(query).arrow()
        finally:
            conn.close()

    def upsert_dataframe(self, df: pd.DataFrame) -> int:
        """Upsert dataframe and return number of rows affected."""
        if df.empty:
            return 0
        with self._write_lock:
            self.conn.register("staging_df", df)
            self.conn.execute("""
                MERGE INTO daily_quotes AS target
                USING staging_df AS source
                ON target.Date = source.Date AND target.Ticker = source.Ticker
                WHEN MATCHED THEN UPDATE SET
                    Open = source.Open,
                    High = source.High,
                    Low = source.Low,
                    Close = source.Close,
                    Volume = source.Volume
                WHEN NOT MATCHED THEN INSERT
                    (Date, Ticker, Open, High, Low, Close, Volume)
                VALUES
                    (source.Date, source.Ticker, source.Open, source.High,
                     source.Low, source.Close, source.Volume);
            """)
            self.conn.unregister("staging_df")
            return len(df)

    def purge_ticker(self, ticker: str):
        with self._write_lock:
            self.conn.execute("DELETE FROM daily_quotes WHERE Ticker = ?", [ticker])
        logger.critical(f"RUNTIME EVENT: Delisted ticker data completely purged for '{ticker}'.")

    def is_split_processed(self, ticker: str, split_date: str) -> bool:
        conn = self._read_only_conn()
        try:
            result = conn.execute(
                "SELECT 1 FROM splits_processed WHERE Ticker = ? AND SplitDate = ?",
                [ticker, split_date]
            ).fetchone()
            return result is not None
        finally:
            conn.close()

    def record_split(self, ticker: str, split_date: str, split_factor: float):
        """Record a split in splits_processed for idempotency."""
        with self._write_lock:
            self.conn.execute("""
                INSERT INTO splits_processed (Ticker, SplitDate, SplitFactor)
                VALUES (?, ?, ?)
                ON CONFLICT (Ticker, SplitDate) DO NOTHING;
            """, [ticker, split_date, split_factor])

    def delete_ticker_data(self, ticker: str):
        """Delete all rows for a ticker (used before re-download after split)."""
        with self._write_lock:
            self.conn.execute("DELETE FROM daily_quotes WHERE Ticker = ?", [ticker])

    def get_existing_tickers(self) -> Set[str]:
        conn = self._read_only_conn()
        try:
            result = conn.execute("SELECT DISTINCT Ticker FROM daily_quotes").fetchall()
            return {row[0] for row in result}
        finally:
            conn.close()

    def get_aligned_start_date(self) -> Optional[str]:
        """Return the earliest Date across all tickers as ISO string, or None."""
        conn = self._read_only_conn()
        try:
            result = conn.execute("SELECT MIN(Date) FROM daily_quotes").fetchone()
            if result and result[0]:
                return str(result[0])
            return None
        finally:
            conn.close()

    def detect_new_tickers(self, configured_tickers: List[str]) -> List[str]:
        existing = self.get_existing_tickers()
        return [t for t in configured_tickers if t not in existing]

    def generate_top_20_dollar_volume(self, target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self._read_only_conn()
        try:
            if target_date:
                rel = conn.execute(
                    """SELECT Ticker, Date, Close, Volume,
                              (Close * Volume) AS dollar_volume
                       FROM daily_quotes
                       WHERE Date = ?
                       ORDER BY dollar_volume DESC
                       LIMIT 20;""",
                    [target_date]
                )
            else:
                rel = conn.execute(
                    """SELECT Ticker, Date, Close, Volume,
                              (Close * Volume) AS dollar_volume
                       FROM daily_quotes
                       WHERE Date = (SELECT MAX(Date) FROM daily_quotes)
                       ORDER BY dollar_volume DESC
                       LIMIT 20;"""
                )
            cols = [desc[0] for desc in rel.description]
            rows = rel.fetchall()
            result = []
            for row in rows:
                item = dict(zip(cols, row))
                item['Date'] = str(item['Date'])
                result.append(item)
            return result
        finally:
            conn.close()

    def close(self):
        with self._write_lock:
            logger.info("Executing DuckDB CHECKPOINT before closing handle...")
            try:
                self.conn.execute("CHECKPOINT;")
                self.conn.close()
                logger.info("DuckDB storage gracefully detached.")
            except Exception as e:
                logger.error(f"Error executing CHECKPOINT on close: {e}")

# =====================================================================
# 4. YFINANCE INGESTION ENGINE
# =====================================================================

def _validate_select_only(sql: str) -> None:
    """Reject non-SELECT/non-WITH statements for do_get."""
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise ValueError("Only SELECT and WITH (CTE) statements are allowed via do_get")


class IngestionEngine:
    def __init__(self, db_mgr: DatabaseManager, cfg: PipelineConfig):
        self.db = db_mgr
        self.cfg = cfg

    def _check_and_apply_splits(self, tickers: List[str]):
        """Per-ticker split detection. If any unprocessed split is found,
        delete all ticker data and re-download from aligned start date.
        No batch API exists for splits."""
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                splits = stock.splits
                if splits is None or splits.empty:
                    continue

                # Find any unprocessed splits
                unprocessed = []
                for split_dt, factor in splits.sort_index().items():
                    split_date_str = split_dt.strftime('%Y-%m-%d')
                    if not self.db.is_split_processed(t, split_date_str):
                        unprocessed.append((split_date_str, float(factor)))

                if not unprocessed:
                    continue

                # Record all unprocessed splits for idempotency
                for split_date_str, factor in unprocessed:
                    self.db.record_split(t, split_date_str, factor)

                # Delete all existing data for this ticker
                self.db.delete_ticker_data(t)

                # Re-download from aligned start date with auto_adjust=True
                aligned_start = self.db.get_aligned_start_date()
                if aligned_start is None:
                    # DB is now empty after delete; use 6-year default
                    end_dt = datetime.now()
                    start_dt = end_dt - timedelta(days=6 * 365)
                    aligned_start = start_dt.strftime('%Y-%m-%d')

                today = datetime.now().strftime('%Y-%m-%d')
                logger.warning(
                    f"RUNTIME EVENT: Stock split detected for '{t}'. "
                    f"Deleted and re-downloading from {aligned_start} to {today}."
                )
                df_redownload = self._batch_download([t], aligned_start, today)
                df_norm, _, _, _ = self._normalize_batch_df(df_redownload, [t])
                self.db.upsert_dataframe(df_norm)
                logger.info(f"Re-download complete for '{t}'.")

            except Exception as e:
                logger.error(f"Failed checking splits for ticker '{t}': {e}")

    def _batch_download(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Batch download via yf.download(). end_date is inclusive — adjusted +1 day internally."""
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        df = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_dt.strftime('%Y-%m-%d'),
            interval="1d",
            auto_adjust=True,
            group_by='ticker',
            threads=True
        )
        return df

    def _normalize_batch_df(
        self, df: pd.DataFrame, tickers: List[str]
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Normalize batch yf.download output to flat schema. Returns (df, succeeded, failed, errors)."""
        records = []
        succeeded = []
        failed = []
        errors = {}

        for t in tickers:
            try:
                if df.columns.nlevels == 2:
                    # Multi-ticker: columns are (ticker, field)
                    ticker_df = df[t].copy()
                else:
                    # Single ticker: columns are flat
                    ticker_df = df.copy()

                ticker_df = ticker_df.dropna(subset=['Open', 'High', 'Low', 'Close'])
                if ticker_df.empty:
                    if t in self.cfg.ingestion.exclude_tickers:
                        logger.warning(
                            f"RUNTIME EVENT: Ticker '{t}' returned empty dataset. "
                            f"Matches exclusion criteria."
                        )
                        self.db.purge_ticker(t)
                    else:
                        logger.warning(
                            f"RUNTIME EVENT: Ticker '{t}' returned empty dataset. "
                            f"Not in exclude list — skipped (no purge). "
                            f"This may indicate a temporary data outage."
                        )
                    failed.append(t)
                    errors[t] = "Empty dataset"
                    continue

                ticker_df = ticker_df.reset_index()
                ticker_df['Ticker'] = t
                ticker_df['Date'] = pd.to_datetime(ticker_df['Date']).dt.date
                records.append(
                    ticker_df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
                )
                succeeded.append(t)
            except Exception as e:
                logger.error(f"Failed processing ingestion for ticker '{t}': {e}")
                failed.append(t)
                errors[t] = str(e)
                if t in self.cfg.ingestion.exclude_tickers:
                    self.db.purge_ticker(t)

        if records:
            return pd.concat(records, ignore_index=True), succeeded, failed, errors
        return pd.DataFrame(), succeeded, failed, errors

    def fetch_and_clean(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Fetch pricing data (batch) + check splits (per-ticker).
        Returns (df, succeeded, failed, errors)."""
        if not tickers:
            return pd.DataFrame(), [], [], {}

        # --- Split detection (per-ticker, no batch API) ---
        self._check_and_apply_splits(tickers)

        # --- Batch pricing download ---
        raw_df = self._batch_download(tickers, start_date, end_date)
        if raw_df.empty:
            # All tickers returned nothing
            failed = []
            errors = {}
            for t in tickers:
                failed.append(t)
                errors[t] = "Empty dataset from yf.download"
                if t in self.cfg.ingestion.exclude_tickers:
                    self.db.purge_ticker(t)
            return pd.DataFrame(), [], failed, errors

        return self._normalize_batch_df(raw_df, tickers)

    def run_initial_load(self, years: int = 6) -> Dict[str, Any]:
        start_time = datetime.now()
        logger.info(f"Initiating initial historical load ({years} years)...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=years * 365)
        df, succeeded, failed, errors = self.fetch_and_clean(
            self.cfg.ingestion.tickers,
            start_dt.strftime('%Y-%m-%d'),
            end_dt.strftime('%Y-%m-%d')
        )
        rows = self.db.upsert_dataframe(df)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.info(f"Initial load completed. Rows upserted: {rows}.")
        return {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }

    def run_daily_load(self) -> Dict[str, Any]:
        start_time = datetime.now()
        nyse = mcal.get_calendar('NYSE')
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        sched = nyse.schedule(start_date=today_str, end_date=today_str)

        if sched.empty:
            logger.info("US Market is closed today (Holiday/Weekend). Daily load execution skipped.")
            return {"status": "SKIPPED", "reason": "Market closed", "duration_ms": 0}

        all_succeeded = []
        all_failed = []
        all_errors = {}
        total_rows = 0

        # --- New ticker detection: backfill from aligned start date ---
        new_tickers = self.db.detect_new_tickers(self.cfg.ingestion.tickers)
        if new_tickers:
            aligned_start = self.db.get_aligned_start_date()
            if aligned_start:
                logger.info(
                    f"RUNTIME EVENT: New ticker(s) detected: {new_tickers}. "
                    f"Backfilling from aligned start date {aligned_start}."
                )
                backfill_end = datetime.now().strftime('%Y-%m-%d')
                df_new, succ, fail, err = self.fetch_and_clean(
                    new_tickers, aligned_start, backfill_end
                )
                rows = self.db.upsert_dataframe(df_new)
                total_rows += rows
                all_succeeded.extend(succ)
                all_failed.extend(fail)
                all_errors.update(err)
                logger.info(f"Backfill complete for {new_tickers}. Rows: {rows}.")
            else:
                logger.info(
                    f"RUNTIME EVENT: New ticker(s) detected: {new_tickers}. "
                    f"Database is empty, performing initial load (6 years)."
                )
                result = self.run_initial_load(years=6)
                total_rows += result.get("rows_upserted", 0)
                all_succeeded.extend(result.get("tickers_succeeded", []))
                all_failed.extend(result.get("tickers_failed", []))
                all_errors.update(result.get("errors", {}))
                # Fall through to trailing 4-day refresh

        # --- Trailing 4-day refresh for all tickers ---
        logger.info("US Market active. Executing daily load (trailing 4 calendar days)...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=4)
        df, succ, fail, err = self.fetch_and_clean(
            self.cfg.ingestion.tickers,
            start_dt.strftime('%Y-%m-%d'),
            end_dt.strftime('%Y-%m-%d')
        )
        rows = self.db.upsert_dataframe(df)
        total_rows += rows
        all_succeeded.extend(succ)
        all_failed.extend(fail)
        all_errors.update(err)

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.info(f"Daily load complete. Total rows upserted: {total_rows}.")
        return {
            "rows_upserted": total_rows,
            "tickers_succeeded": list(set(all_succeeded)),
            "tickers_failed": list(set(all_failed)),
            "errors": all_errors if all_failed else {},
            "duration_ms": duration_ms,
        }

    def run_on_request_load(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Dict[str, Any]:
        start_time = datetime.now()
        logger.info(f"Executing on-request load for tickers {tickers} [{start_date} to {end_date}]...")
        df, succeeded, failed, errors = self.fetch_and_clean(tickers, start_date, end_date)
        rows = self.db.upsert_dataframe(df)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.info(f"On-request load complete. Rows upserted: {rows}.")
        return {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }

# =====================================================================
# 5. APACHE ARROW FLIGHT DAEMON
# =====================================================================

class MarketDataFlightServer(flight.FlightServerBase):
    def __init__(self, location: str, db_mgr: DatabaseManager,
                 ingest_engine: IngestionEngine, cfg: PipelineConfig):
        super().__init__(location)
        self.db = db_mgr
        self.engine = ingest_engine
        self.cfg = cfg

    def do_get(self, context, ticket):
        try:
            req = json.loads(ticket.ticket.decode('utf-8'))
            sql_query = req.get("sql")
            if not sql_query:
                raise flight.FlightServerError("Missing 'sql' field in ticket")
            _validate_select_only(sql_query)
            logger.info(f"Serving Arrow Flight query: {_truncate_sql(sql_query)}")
            arrow_table = self.db.execute_query(sql_query)
            return flight.RecordBatchStream(arrow_table)
        except flight.FlightServerError:
            raise
        except ValueError as e:
            logger.warning(f"Rejected do_get query: {e}")
            raise flight.FlightServerError(str(e))
        except Exception as e:
            logger.error(f"Failed to execute Flight do_get query: {e}")
            raise flight.FlightServerError(f"Query Execution Error: {str(e)}")

    def do_action(self, context, action):
        action_type = action.type
        payload = json.loads(action.body.to_pybytes().decode('utf-8')) if action.body else {}

        if action_type == "initial_load":
            years = payload.get("years", 6)
            result = self.engine.run_initial_load(years=years)
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return flight.Result(json.dumps({"status": status, **result}).encode('utf-8'))

        elif action_type == "daily_load":
            result = self.engine.run_daily_load()
            if result.get("status") == "SKIPPED":
                return flight.Result(json.dumps(result).encode('utf-8'))
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return flight.Result(json.dumps({"status": status, **result}).encode('utf-8'))

        elif action_type == "on_request_load":
            result = self.engine.run_on_request_load(
                tickers=payload["tickers"],
                start_date=payload["start_date"],
                end_date=payload["end_date"]
            )
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return flight.Result(json.dumps({"status": status, **result}).encode('utf-8'))

        elif action_type == "daily_report":
            start_time = datetime.now()
            target_date = payload.get("date")
            report_data = self.db.generate_top_20_dollar_volume(target_date)
            out_dir = self.cfg.reports.report_dir
            os.makedirs(out_dir, exist_ok=True)
            file_name = f"dollar_volume_{target_date or datetime.now().strftime('%Y%m%d')}.json"
            with open(os.path.join(out_dir, file_name), "w") as f:
                json.dump(report_data, f, indent=2)
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            return flight.Result(
                json.dumps({
                    "status": "SUCCESS",
                    "report": report_data,
                    "duration_ms": duration_ms
                }).encode('utf-8')
            )

        else:
            raise flight.FlightServerError(f"Unknown action: {action_type}")

# =====================================================================
# 6. MAIN ENTRY POINT
# =====================================================================

def main():
    db_manager = DatabaseManager(config.db_path)
    ingest_engine = IngestionEngine(db_manager, config)

    def shutdown_handler(signum, frame):
        logger.warning(f"Shutdown signal {signum} trapped. Commencing emergency WAL checkpoint...")
        db_manager.close()
        logger.info("Database closed safely. Exiting daemon process.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    location = f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    server = MarketDataFlightServer(location, db_manager, ingest_engine, config)
    logger.info(f"Apache Arrow Flight Daemon operational on {location}")
    server.serve()

if __name__ == "__main__":
    main()
```

---

## 11. Client Usage Examples

### 11.1 Concurrent Read Query from RO Client

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://127.0.0.1:8815")

ticket_payload = json.dumps({
    "sql": "SELECT * FROM daily_quotes WHERE Ticker = 'AAPL' ORDER BY Date DESC"
})
ticket = flight.Ticket(ticket_payload.encode('utf-8'))

reader = client.do_get(ticket)
arrow_table = reader.read_all()
df = arrow_table.to_pandas()
print(df.head())
```

### 11.2 Trigger Daily Load

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://127.0.0.1:8815")
action = flight.Action("daily_load", b"")
result = list(client.do_action(action))
print("Daily load response:", result[0].body.to_pybytes().decode('utf-8'))
```

### 11.3 Trigger On-Demand Load

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://127.0.0.1:8815")

action_payload = json.dumps({
    "tickers": ["TSLA", "AMD"],
    "start_date": "2026-01-01",
    "end_date": "2026-09-01"
}).encode('utf-8')

action = flight.Action("on_request_load", action_payload)
result = list(client.do_action(action))
print("On-demand load response:", result[0].body.to_pybytes().decode('utf-8'))
```

### 11.4 Generate Daily Report

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://127.0.0.1:8815")

action_payload = json.dumps({"date": "2026-09-04"}).encode('utf-8')
action = flight.Action("daily_report", action_payload)
result = list(client.do_action(action))
print("Report:", result[0].body.to_pybytes().decode('utf-8'))
```

---

## 12. Dependencies

```
duckdb>=1.0
pyarrow>=14.0
yfinance>=0.2
pandas>=2.0
pandas-market-calendars>=4.0
pydantic>=2.0
pyyaml>=6.0
```

---

## 13. Known Limitations

| Limitation | Impact | Mitigation / Phase |
|---|---|---|
| yfinance data quality | yfinance may return stale or missing bars; no SLA | Log warnings on empty data; monitor `tickers_failed` in action results |
| No corporate actions beyond splits | Dividends, mergers, name changes not explicitly handled | `auto_adjust=True` handles dividend adjustment in fetched data; historical corporate actions are Phase 2 |
| Single-node only | No clustering or replication; DuckDB file is local | By design for Phase 1; R2 sync in Phase 2 provides off-host backup |
| No authentication on Flight server | Any localhost process can read data or trigger ingestion | Acceptable for Phase 1 localhost; mTLS in Phase 2 |
| No query timeout | A malicious or expensive query can block a read connection | Add query timeout in Phase 2 when query patterns are understood |
| No intraday data | Schema and ingestion are daily-only | By design; intraday deferred to Phase 2+ |
