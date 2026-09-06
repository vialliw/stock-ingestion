# System Architecture Specification: Centralized Quantitative Data Ingestion Pipeline (yfinance + DuckDB + Apache Arrow Flight)

**Document Version:** 1.0.1

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
| Manual stock split adjustment removed | Fix: `auto_adjust=True` already handles splits; manual logic caused double-adjustment |
| `do_put` / `IngestBatch` removed from API | Deferred to Phase 2 |
| `timeframe` config parameter removed | Schema is daily-only; intraday deferred |
| `threading.Lock` added around all write operations | Fix: Arrow Flight serves concurrent requests |
| `APScheduler` scheduling deferred to Phase 2 | Daily load is triggered manually via Flight action for Phase 1 |
| R2 sync deferred to Phase 2 | Core function is local ingestion + read service |
| API field names standardized to `start_date` / `end_date` | Fix: API table said `start`, code used `start_date` |
| `daily_load` action added to API table | Fix: was implemented but missing from API docs |
| Config defaults aligned (`tickers=[]`, `exclude_tickers=[]`) | Fix: schema table and code disagreed |
| `is_us_market_open_today()` standalone function removed | Fix: duplicated inline check in `run_daily_load` |
| Report path standardized to `./data/reports/` | Fix: spec said `/data/reports/`, code used relative |
| `return` added after empty-DB initial load in `daily_load` | Fix: redundant trailing 4-day refresh after full 6-year load |
| Initial load SQL changed from `INSERT ON CONFLICT` to `MERGE INTO` | Fix: spec text and code now agree on single upsert pattern |

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

---

## 2. Scope

### Phase 1 (this specification)

- Configuration via `config.yaml` with Pydantic validation and nested schema
- DuckDB local storage with the 7-column `daily_quotes` schema
- yfinance ingestion: initial load, daily rolling load, on-request load
- New ticker detection and aligned backfill in daily load
- Delisted ticker purge
- Apache Arrow Flight read service (`do_get`) using read-only DuckDB connections
- Flight actions for ingestion triggers and report generation (`do_action`)
- Top 20 dollar volume daily report
- Graceful shutdown with DuckDB checkpoint
- Configurable log file location
- Thread-safe write operations via `threading.Lock`

### Deferred (Phase 2+)

| Feature | Reason |
|---|---|
| R2 sync | Core function works locally first; R2 is a sync target after processing completes |
| APScheduler automated scheduling | Daily load is triggered manually or via OS-level cron for Phase 1 |
| `do_put` / `IngestBatch` | External batch ingestion not needed for Phase 1 |
| Intraday timeframes | Schema is daily-only; `interval="1d"` is hardcoded |
| Manual stock split adjustment | Handled by yfinance `auto_adjust=True`; no manual SQL adjustment needed |

---

## 3. Configuration

Configuration management uses Pydantic for schema validation. The Pydantic model is nested to match the YAML structure exactly. A `from_yaml` classmethod loads `config.yaml` and falls back to defaults if the file is absent.

### 3.1 Configuration Schema

| Section | Parameter | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `database` | `duckdb_dir` | `str` | `"./data"` | Directory containing the DuckDB database file. Created if absent. |
| `database` | `duckdb_filename` | `str` | `"market_data.duckdb"` | DuckDB physical file name. |
| `ingestion` | `tickers` | `List[str]` | `[]` | Primary list of universe tickers to track. |
| `ingestion` | `exclude_tickers` | `List[str]` | `[]` | Blacklist tickers. Delisted tickers matching this list trigger purge workflows. |
| `server` | `flight_host` | `str` | `"127.0.0.1"` | Bind IP for Arrow Flight server. |
| `server` | `flight_port` | `int` | `8815` | Listening port for Arrow Flight service. |
| `logging` | `log_file` | `str` | `"./logs/pipeline.log"` | Path to the log file. Parent directory is created if absent. |

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
```

### 3.3 Config Loading Behavior

- On startup, `PipelineConfig.from_yaml("config.yaml")` is called.
- If `config.yaml` does not exist, all Pydantic defaults are used.
- If `config.yaml` exists, its nested keys are mapped directly to the nested Pydantic sub-models.
- Access pattern in code: `config.database.duckdb_dir`, `config.ingestion.tickers`, `config.server.flight_host`, `config.logging.log_file`.

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
| Volume | BIGINT | YES | | | |

Primary key: `(Date, Ticker)`.

```sql
CREATE TABLE IF NOT EXISTS daily_quotes (
    Date DATE NOT NULL,
    Ticker VARCHAR NOT NULL,
    Open DOUBLE,
    High DOUBLE,
    Low DOUBLE,
    Close DOUBLE,
    Volume BIGINT,
    PRIMARY KEY (Date, Ticker)
);

CREATE INDEX IF NOT EXISTS idx_quotes_date_ticker ON daily_quotes(Date, Ticker);
```

### 4.2 Upsert Pattern (used by all ingestion operations)

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

### 5.1 Initial Load (`initial_load`)

- **Behavior:** Downloads 6 years of historical daily data for all configured tickers and upserts into `daily_quotes`.
- **Execution:** Triggered manually via the Flight `do_action` endpoint (`action_type: "initial_load"`). Intended for first-time database population or when the database is empty.
- **yfinance parameters:** `interval="1d"`, `auto_adjust=True`.

### 5.2 Daily Rolling Load (`daily_load`)

- **Behavior:** Downloads the trailing 4 calendar days for all configured tickers to account for settlement windows, exchange data adjustments, and late-reported bar updates.
- **New Ticker Detection (backfill before refresh):** Before the trailing-4-day refresh, the daily load checks for tickers in `config.ingestion.tickers` that do not yet exist in the database. If new tickers are found:
  1. Query the aligned start date: `SELECT MIN(Date) FROM daily_quotes` — the earliest date across all existing tickers.
  2. Download historical data for the new tickers from the aligned start date to today.
  3. Upsert via the standard `MERGE INTO` pattern.
  4. Log: `INFO - New ticker(s) detected: [TSLA, AMD]. Backfilling from aligned start date 2020-09-06.`
  5. If the database is empty (no existing tickers), fall back to `initial_load` with 6 years and **return** — the trailing 4-day refresh is skipped because the full history was just loaded.
- **Market Calendar Check:** Before any download, the daily load checks the NYSE calendar. If the market is closed (holiday/weekend), the load is skipped entirely.
- **yfinance parameters:** `interval="1d"`, `auto_adjust=True`.

### 5.3 On-Request Load (`on_request_load`)

- **Behavior:** Accepts a custom payload defining `tickers: List[str]`, `start_date: str`, and `end_date: str`. Downloads and upserts via the standard `MERGE INTO` pattern.
- **yfinance parameters:** `interval="1d"`, `auto_adjust=True`.

### 5.4 Split Adjustment Note

yfinance's `auto_adjust=True` parameter automatically adjusts OHLC data for stock splits and dividends. No manual SQL-based split adjustment is performed. This is a deliberate simplification: the stored `Close` is the split-and-dividend-adjusted close.

---

## 6. Runtime Events

### 6.1 Delisted Ticker Detection & Purge

During all ingestion calls (`fetch_and_clean`):

1. If `yfinance` returns an empty dataframe for a ticker AND that ticker is listed in `config.ingestion.exclude_tickers`:
   - Execute: `DELETE FROM daily_quotes WHERE Ticker = ?`
   - Log: `CRITICAL - RUNTIME EVENT: Delisted ticker data completely purged for '{ticker}'.`
2. If a ticker returns empty data but is NOT in `exclude_tickers`, it is silently skipped (no purge, no error). This avoids accidental deletion of tickers experiencing temporary data outages.

---

## 7. Apache Arrow Flight Protocol

Clients communicate with the Flight daemon using JSON-encoded Flight descriptors.

### 7.1 RPC Functions

```
Flight Server Endpoints
├── do_get(ticket)        --> Stream PyArrow Record Batches (Read Queries, read-only DuckDB connection)
└── do_action(action)     --> Execute Administrative Routines (Ingestion Triggers, Report)
```

`do_put` is not implemented in Phase 1.

### 7.2 API Schema

| Function | Type | Input Descriptor / Ticket JSON | Output Payload |
| --- | --- | --- | --- |
| `QueryData` | `do_get` | `{"sql": "SELECT * FROM daily_quotes WHERE Ticker='AAPL'"}` | PyArrow RecordBatch Stream |
| `TriggerInitLoad` | `do_action` | `Action: "initial_load", Body: {"years": 6}` | ActionResult (`{"status": "SUCCESS"}`) |
| `TriggerDailyLoad` | `do_action` | `Action: "daily_load", Body: {}` | ActionResult (`{"status": "SUCCESS"}`) |
| `TriggerOnDemand` | `do_action` | `Action: "on_request_load", Body: {"tickers": ["NVDA"], "start_date": "2026-01-01", "end_date": "2026-09-01"}` | ActionResult (`{"status": "SUCCESS"}`) |
| `GenerateReport` | `do_action` | `Action: "daily_report", Body: {"date": "2026-09-04"}` | ActionResult (`{"status": "SUCCESS", "report": [...]}`) |

### 7.3 Read-Only Enforcement

`do_get` opens a short-lived read-only DuckDB connection (`duckdb.connect(db_path, read_only=True)`) per request. This ensures that clients cannot execute `DROP`, `DELETE`, `INSERT`, or `UPDATE` statements through the read endpoint, even if the SQL string is crafted maliciously. The connection is closed after the query completes.

### 7.4 Thread Safety

All write operations (`upsert_dataframe`, `purge_ticker`, schema initialization, `CHECKPOINT`) are serialized via a `threading.Lock` held by `DatabaseManager`. Read operations use independent read-only connections and do not acquire the lock, allowing concurrent reads from multiple Flight clients.

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

Reports are stored as JSON files in `./data/reports/dollar_volume_YYYYMMDD.json` and returned in the Flight action result payload.

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
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta, timezone

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import yfinance as yf
import yaml
import pandas_market_calendars as mcal
from pydantic import BaseModel, Field

# =====================================================================
# 1. CONFIGURATION
# =====================================================================

class DatabaseConfig(BaseModel):
    duckdb_dir: str = "./data"
    duckdb_filename: str = "market_data.duckdb"


class IngestionConfig(BaseModel):
    tickers: List[str] = Field(default_factory=list)
    exclude_tickers: List[str] = Field(default_factory=list)


class ServerConfig(BaseModel):
    flight_host: str = "127.0.0.1"
    flight_port: int = 8815


class LoggingConfig(BaseModel):
    log_file: str = "./logs/pipeline.log"


class PipelineConfig(BaseModel):
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @property
    def db_path(self) -> str:
        os.makedirs(self.database.duckdb_dir, exist_ok=True)
        return os.path.join(self.database.duckdb_dir, self.database.duckdb_filename)

    @classmethod
    def from_yaml(cls, path: str = "config.yaml") -> "PipelineConfig":
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

# =====================================================================
# 3. DUCKDB MANAGER (thread-safe writes, concurrent reads)
# =====================================================================

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
                    Volume BIGINT,
                    PRIMARY KEY (Date, Ticker)
                );
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_quotes_date_ticker
                ON daily_quotes(Date, Ticker);
            """)

    def _read_only_conn(self):
        """Open a short-lived read-only connection for concurrent read queries."""
        return duckdb.connect(self.db_path, read_only=True)

    def execute_query(self, query: str) -> pa.Table:
        conn = self._read_only_conn()
        try:
            return conn.execute(query).arrow()
        finally:
            conn.close()

    def upsert_dataframe(self, df: pd.DataFrame):
        if df.empty:
            return
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

    def purge_ticker(self, ticker: str):
        with self._write_lock:
            self.conn.execute("DELETE FROM daily_quotes WHERE Ticker = ?", [ticker])
        logger.critical(f"RUNTIME EVENT: Delisted ticker data completely purged for '{ticker}'.")

    def get_existing_tickers(self) -> Set[str]:
        conn = self._read_only_conn()
        try:
            result = conn.execute("SELECT DISTINCT Ticker FROM daily_quotes").fetchall()
            return {row[0] for row in result}
        finally:
            conn.close()

    def get_aligned_start_date(self) -> Optional[str]:
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

class IngestionEngine:
    def __init__(self, db_mgr: DatabaseManager, cfg: PipelineConfig):
        self.db = db_mgr
        self.cfg = cfg

    def fetch_and_clean(self, tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        # Per-ticker loop is required because yfinance .history() is per-ticker.
        # There is no batch API that returns a single DataFrame for multiple
        # tickers with the auto_adjust and interval parameters needed here.
        records = []
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                df = stock.history(
                    start=start_date,
                    end=end_date,
                    interval="1d",
                    auto_adjust=True
                )

                if df.empty:
                    if t in self.cfg.ingestion.exclude_tickers:
                        logger.warning(
                            f"Ticker '{t}' returned empty dataset. "
                            f"Matches exclusion criteria."
                        )
                        self.db.purge_ticker(t)
                    continue

                df = df.reset_index()
                df['Ticker'] = t
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                records.append(
                    df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
                )
            except Exception as e:
                logger.error(f"Failed processing ingestion for ticker '{t}': {e}")
                if t in self.cfg.ingestion.exclude_tickers:
                    self.db.purge_ticker(t)

        if records:
            return pd.concat(records, ignore_index=True)
        return pd.DataFrame()

    def run_initial_load(self, years: int = 6):
        logger.info(f"Initiating initial historical load ({years} years)...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=years * 365)
        df = self.fetch_and_clean(
            self.cfg.ingestion.tickers,
            start_dt.strftime('%Y-%m-%d'),
            end_dt.strftime('%Y-%m-%d')
        )
        self.db.upsert_dataframe(df)
        logger.info("Initial load operation completed.")

    def run_daily_load(self):
        nyse = mcal.get_calendar('NYSE')
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        sched = nyse.schedule(start_date=today_str, end_date=today_str)

        if sched.empty:
            logger.info("US Market is closed today (Holiday/Weekend). Daily load execution skipped.")
            return

        # --- New ticker detection: backfill from aligned start date ---
        new_tickers = self.db.detect_new_tickers(self.cfg.ingestion.tickers)
        if new_tickers:
            aligned_start = self.db.get_aligned_start_date()
            if aligned_start:
                logger.info(
                    f"New ticker(s) detected: {new_tickers}. "
                    f"Backfilling from aligned start date {aligned_start}."
                )
                backfill_end = datetime.now().strftime('%Y-%m-%d')
                df_new = self.fetch_and_clean(new_tickers, aligned_start, backfill_end)
                self.db.upsert_dataframe(df_new)
                logger.info(f"Backfill complete for {new_tickers}.")
            else:
                logger.info(
                    f"New ticker(s) detected: {new_tickers}. "
                    f"Database is empty, performing initial load (6 years)."
                )
                self.run_initial_load(years=6)
                return  # Full history loaded; skip trailing 4-day refresh

        # --- Trailing 4-day refresh for all tickers ---
        logger.info("US Market active. Executing daily load (trailing 4 calendar days)...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=4)
        df = self.fetch_and_clean(
            self.cfg.ingestion.tickers,
            start_dt.strftime('%Y-%m-%d'),
            end_dt.strftime('%Y-%m-%d')
        )
        self.db.upsert_dataframe(df)
        logger.info("Daily load update complete.")

    def run_on_request_load(self, tickers: List[str], start_date: str, end_date: str):
        logger.info(f"Executing on-request load for tickers {tickers} [{start_date} to {end_date}]...")
        df = self.fetch_and_clean(tickers, start_date, end_date)
        self.db.upsert_dataframe(df)
        logger.info("On-request load update complete.")

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
            logger.info(f"Serving Arrow Flight query: {sql_query}")
            arrow_table = self.db.execute_query(sql_query)
            return flight.RecordBatchStream(arrow_table)
        except flight.FlightServerError:
            raise
        except Exception as e:
            logger.error(f"Failed to execute Flight do_get query: {e}")
            raise flight.FlightServerError(f"Query Execution Error: {str(e)}")

    def do_action(self, context, action):
        action_type = action.type
        payload = json.loads(action.body.to_pybytes().decode('utf-8')) if action.body else {}

        if action_type == "initial_load":
            years = payload.get("years", 6)
            self.engine.run_initial_load(years=years)
            return flight.Result(json.dumps({"status": "SUCCESS"}).encode('utf-8'))

        elif action_type == "daily_load":
            self.engine.run_daily_load()
            return flight.Result(json.dumps({"status": "SUCCESS"}).encode('utf-8'))

        elif action_type == "on_request_load":
            self.engine.run_on_request_load(
                tickers=payload["tickers"],
                start_date=payload["start_date"],
                end_date=payload["end_date"]
            )
            return flight.Result(json.dumps({"status": "SUCCESS"}).encode('utf-8'))

        elif action_type == "daily_report":
            target_date = payload.get("date")
            report_data = self.db.generate_top_20_dollar_volume(target_date)
            out_dir = os.path.join(self.cfg.database.duckdb_dir, "reports")
            os.makedirs(out_dir, exist_ok=True)
            file_name = f"dollar_volume_{target_date or datetime.now().strftime('%Y%m%d')}.json"
            with open(os.path.join(out_dir, file_name), "w") as f:
                json.dump(report_data, f, indent=2)
            return flight.Result(
                json.dumps({"status": "SUCCESS", "report": report_data}).encode('utf-8')
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
