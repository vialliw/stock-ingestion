# System Architecture Specification: Centralized Quantitative Data Ingestion Pipeline (yfinance + DuckDB + Apache Arrow Flight)

**Document Version:** 1.0.0

**Status:** Draft / Technical Specification

**Architecture Pattern:** Microservice / Local IPC Server via PyArrow Flight

---

## 1. System Overview

This specification details the design for a centralized quantitative market data ingestion pipeline. The system ingests daily equity pricing data from `yfinance`, stores historical state in a single local DuckDB database file, and serves concurrent local downstream clients ($n$ Read-Only worker processes) via an Apache Arrow Flight IPC server.

By wrapping DuckDB inside a single PyArrow Flight service process, the system bypasses DuckDB’s single-process file-locking constraints (`IOException`) while maintaining zero-copy data transfer speeds for analytics tasks.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      Flight Data Daemon (FlightServer)                           │
│                                                                                  │
│   ┌────────────────────────┐      ┌─────────────────────┐     ┌──────────────┐   │
│   │ Scheduler (APScheduler)│      │  DuckDB Engine      │     │ Flight RPC   │   │
│   │ - US Market Calendar   │─────▶│  (Holds RW Lock)    │◀────│ Endpoint     │   │
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

## 2. Component Specifications

### 2.1 Configuration Module (`config.py` / `config.yaml`)

Configuration management uses Pydantic for schema validation and default assignment.

#### 2.1.1 Configuration Schema

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `tickers` | `List[str]` | `[]` | Primary list of universe tickers to track. |
| `exclude_tickers` | `List[str]` | `[]` | Blacklist tickers. Delisted tickers matching this list trigger purge workflows. |
| `adj_close` | `bool` | `true` | Standardizes unadjusted vs. adjusted price downloads from `yfinance`. |
| `timeframe` | `str` | `"1d"` | Default price bar resolution (`1d`, `1h`, `1m`). |
| `duckdb_dir` | `str` | `"./data"` | Target directory containing the DuckDB database file. |
| `duckdb_filename` | `str` | `"market_data.duckdb"` | DuckDB physical file name. |
| `flight_host` | `str` | `"127.0.0.1"` | Bind IP for Arrow Flight server. |
| `flight_port` | `int` | `8815` | Listening port for Arrow Flight service. |

#### 2.1.2 Specification (`config.yaml`)

```yaml
database:
  duckdb_dir: "./data"
  duckdb_filename: "market_data.duckdb"

ingestion:
  adj_close: true
  timeframe: "1d"
  tickers:
    - "AAPL"
    - "MSFT"
    - "NVDA"
  exclude_tickers:
    - "OTC_DELISTED_XYZ"

server:
  flight_host: "127.0.0.1"
  flight_port: 8815

```

---

### 2.2 Core Ingestion Engine & Data Operations

#### 2.2.1 Data Storage Schema (DuckDB)

```sql
CREATE TABLE IF NOT EXISTS daily_quotes (
    ticker VARCHAR NOT NULL,
    date DATE NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    dollar_volume DOUBLE GENERATED ALWAYS AS (close * volume),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_quotes_ticker_date ON daily_quotes(ticker, date);

```

#### 2.2.2 Initial Load (`initial_load`)

* **Behavior:** Appends 6 years of historical daily data for configured universe tickers.
* **Execution Constraint:** Executed once at database initialization or for newly added universe tickers.
* **Transaction Pattern:**
```sql
BEGIN TRANSACTION;
INSERT INTO daily_quotes (ticker, date, open, high, low, close, adj_close, volume)
SELECT ticker, date, open, high, low, close, adj_close, volume 
FROM staging_initial
ON CONFLICT (ticker, date) DO NOTHING;
COMMIT;

```



#### 2.2.3 Daily Rolling Load (`daily_load`)

* **Behavior:** Downloads the trailing **4 calendar days** to account for settlement windows, exchange data adjustments, and late-reported bar updates.
* **Deduplication / Upsert Pattern:**
```sql
MERGE INTO daily_quotes AS target
USING staging_daily AS source
ON target.ticker = source.ticker AND target.date = source.date
WHEN MATCHED THEN UPDATE SET
    open = source.open,
    high = source.high,
    low = source.low,
    close = source.close,
    adj_close = source.adj_close,
    volume = source.volume,
    updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT 
    (ticker, date, open, high, low, close, adj_close, volume)
VALUES 
    (source.ticker, source.date, source.open, source.high, source.low, source.close, source.adj_close, source.volume);

```



#### 2.2.4 On-Request Load (`on_request_load`)

* **Behavior:** Accepts a custom payload defining `tickers: List[str]`, `start_date: str`, and `end_date: str`. Applies the same `MERGE INTO` upsert logic as the daily load process.

---

### 2.3 Runtime Lifecycle Events

#### 2.3.1 Delisted Ticker Detection & Purge

During daily and on-request ingestion calls:

1. `yfinance` response metadata is evaluated for empty data frames, `404 Not Found`, or specific quote warnings.
2. If a ticker fails to return data and exists within `config.exclude_tickers`:
* Execute deletion procedure:
```sql
DELETE FROM daily_quotes WHERE ticker = 'TARGET_TICKER';

```


* Write event to system log: `CRITICAL - Delisted ticker detected and purged: TARGET_TICKER`.



#### 2.3.2 Stock Split Detection & Re-adjustment

1. Query `yf.Ticker(ticker).splits` for events occurring within the download window.
2. If a split event is identified:
* Fetch historical split ratio factor $S = \text{New Shares} / \text{Old Shares}$.
* Recalculate historical pricing metrics prior to the split date:
```sql
UPDATE daily_quotes
SET open = open / :split_factor,
    high = high / :split_factor,
    low = low / :split_factor,
    close = close / :split_factor,
    volume = CAST(volume * :split_factor AS BIGINT)
WHERE ticker = :ticker AND date < :split_date;

```


* Write event to log: `WARNING - Stock split detected for {ticker}: factor {split_factor} on {split_date}. Historical records adjusted.`



---

### 2.4 Apache Arrow Flight Protocol Specification

Clients communicate with the Flight daemon using JSON-encoded Flight descriptors (`Tickets` for reads, `Command Descriptors` for writes/actions).

#### 2.4.1 RPC Functions Overview

```
Flight Server Endpoints
├── do_get(ticket)        --> Stream PyArrow Record Batches (Read Queries)
├── do_put(descriptor)    --> Send PyArrow Record Batches (Ingestion)
└── do_action(action)     --> Execute Administrative Routines (Init, On-Demand, Report)

```

#### 2.4.2 API Schema

| Function | Type | Input Descriptor / Ticket JSON | Output Payload |
| --- | --- | --- | --- |
| `QueryData` | `do_get` | `{"sql": "SELECT * FROM daily_quotes WHERE ticker='AAPL'"}` | PyArrow RecordBatch Stream |
| `IngestBatch` | `do_put` | `Path Descriptor: "daily_quotes_staging"` | FlightPutResult (Status code) |
| `TriggerInitLoad` | `do_action` | `Action: "initial_load", Body: {"years": 6}` | ActionResult (`{"status": "SUCCESS"}`) |
| `TriggerOnDemand` | `do_action` | `Action: "on_request_load", Body: {"tickers": ["NVDA"], "start": "2026-01-01"}` | ActionResult (`{"rows_ingested": 120}`) |
| `GenerateReport` | `do_action` | `Action: "daily_report", Body: {"date": "2026-09-04"}` | ActionResult (JSON Top 20 Dollar Volume) |

---

### 2.5 Reliability, Scheduling, and Shutdown

#### 2.5.1 Automated Recovery & WAL Merging

To protect DuckDB against Write-Ahead Log (`.wal`) corruption during abrupt host termination (`SIGINT`, `SIGTERM`, power loss):

* **Signal Handling:** Catch `SIGINT` and `SIGTERM` signals using Python's `signal` framework.
* **Checkpointing Protocol:** On shutdown, halt incoming Flight tasks, flush the active transaction memory buffer, issue a DuckDB WAL merge, and close the disk handle cleanly:
```python
def graceful_shutdown(signum, frame):
    logger.info("Termination signal received. Initiating DuckDB checkpoint...")
    try:
        db_connection.execute("CHECKPOINT;")
        db_connection.close()
        logger.info("DuckDB transaction log cleanly merged. Storage safe.")
    except Exception as e:
        logger.error(f"Error during clean shutdown checkpoint: {e}")
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

```



#### 2.5.2 Market Calendar Scheduling

* Integrated using `pandas_market_calendars` and `APScheduler`.
* Targets the `NYSE` market calendar.
* Schedules the `daily_load` pipeline to run 30 minutes after US market close (e.g., 16:30 EST) **only on valid exchange trading days**.

```python
import pandas_market_calendars as mcal
from datetime import datetime, timezone

def is_us_market_open_today() -> bool:
    nyse = mcal.get_calendar('NYSE')
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    schedule = nyse.schedule(start_date=today, end_date=today)
    return not schedule.empty

```

---

## 3. Top 20 Dollar Volume Daily Report Specification

The system generates daily ranking reports aggregating dollar volume ($\text{Close Price} \times \text{Volume}$).

### 3.1 Report Query Logic

```sql
SELECT 
    ticker,
    date,
    close,
    volume,
    (close * volume) AS dollar_volume
FROM daily_quotes
WHERE date = (SELECT MAX(date) FROM daily_quotes)
ORDER BY dollar_volume DESC
LIMIT 20;

```

### 3.2 Output Format

Reports are automatically stored in standard structured JSON inside `/data/reports/dollar_volume_YYYYMMDD.json` and broadcast to system logs.

---

## 4. End-to-End Implementation

Below is a single, complete reference implementation containing the server daemon, database manager, task runner, and Arrow Flight service.

```python
import os
import sys
import time
import json
import signal
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal
from pydantic import BaseModel, Field

# =====================================================================
# 1. LOGGING & CONFIGURATION MODULE
# =====================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a")
    ]
)
logger = logging.getLogger("MarketDataPipeline")


class PipelineConfig(BaseModel):
    tickers: List[str] = Field(default_factory=lambda: ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"])
    exclude_tickers: List[str] = Field(default_factory=lambda: ["OTC_DELISTED_XYZ"])
    adj_close: bool = True
    timeframe: str = "1d"
    duckdb_dir: str = "./data"
    duckdb_filename: str = "market_data.duckdb"
    flight_host: str = "127.0.0.1"
    flight_port: int = 8815

    @property
    def db_path(self) -> str:
        os.makedirs(self.duckdb_dir, exist_ok=True)
        return os.path.join(self.duckdb_dir, self.duckdb_filename)


config = PipelineConfig()

# =====================================================================
# 2. DUCKDB MANAGER WITH WAL SAFEGUARDING
# =====================================================================

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path, read_only=False)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_quotes (
                ticker VARCHAR NOT NULL,
                date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, date)
            );
        """)

    def execute_query(self, query: str) -> pa.Table:
        return self.conn.execute(query).arrow()

    def upsert_dataframe(self, df: pd.DataFrame):
        if df.empty:
            return
        
        # Register in-memory Pandas DF as a DuckDB view
        self.conn.register("staging_df", df)
        self.conn.execute("""
            MERGE INTO daily_quotes AS target
            USING staging_df AS source
            ON target.ticker = source.ticker AND target.date = source.date
            WHEN MATCHED THEN UPDATE SET
                open = source.open,
                high = source.high,
                low = source.low,
                close = source.close,
                adj_close = source.adj_close,
                volume = source.volume,
                updated_at = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT 
                (ticker, date, open, high, low, close, adj_close, volume)
            VALUES 
                (source.ticker, source.date, source.open, source.high, source.low, source.close, source.adj_close, source.volume);
        """)
        self.conn.unregister("staging_df")

    def purge_ticker(self, ticker: str):
        self.conn.execute("DELETE FROM daily_quotes WHERE ticker = ?", [ticker])
        logger.critical(f"RUNTIME EVENT: Delisted ticker data completely purged for '{ticker}'.")

    def adjust_for_split(self, ticker: str, split_date: str, split_factor: float):
        self.conn.execute("""
            UPDATE daily_quotes
            SET open = open / ?,
                high = high / ?,
                low = low / ?,
                close = close / ?,
                volume = CAST(volume * ? AS BIGINT)
            WHERE ticker = ? AND date < ?;
        """, [split_factor, split_factor, split_factor, split_factor, split_factor, ticker, split_date])
        logger.warning(
            f"RUNTIME EVENT: Stock split processed for ticker '{ticker}'. "
            f"Ratio: {split_factor}, Split Date: {split_date}. Historical records re-adjusted."
        )

    def generate_top_20_dollar_volume(self, target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        date_filter = "date = ?" if target_date else "date = (SELECT MAX(date) FROM daily_quotes)"
        params = [target_date] if target_date else []
        
        query = f"""
            SELECT 
                ticker, 
                date, 
                close, 
                volume, 
                (close * volume) AS dollar_volume 
            FROM daily_quotes 
            WHERE {date_filter}
            ORDER BY dollar_volume DESC 
            LIMIT 20;
        """
        rel = self.conn.execute(query, params)
        cols = [desc[0] for desc in rel.description]
        rows = rel.fetchall()
        
        result = []
        for row in rows:
            item = dict(zip(cols, row))
            item['date'] = str(item['date'])
            result.append(item)
        return result

    def close(self):
        logger.info("Executing DuckDB CHECKPOINT before closing handle...")
        try:
            self.conn.execute("CHECKPOINT;")
            self.conn.close()
            logger.info("DuckDB storage gracefully detached.")
        except Exception as e:
            logger.error(f"Error executing CHECKPOINT on close: {e}")

# =====================================================================
# 3. YFINANCE DATA INGESTION ENGINE
# =====================================================================

class IngestionEngine:
    def __init__(self, db_mgr: DatabaseManager, cfg: PipelineConfig):
        self.db = db_mgr
        self.cfg = cfg

    def fetch_and_clean(self, tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        records = []
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                
                # Check split events in window
                splits = stock.splits
                if not splits.empty:
                    splits_in_window = splits[(splits.index >= start_date) & (splits.index <= end_date)]
                    for split_dt, factor in splits_in_window.items():
                        if factor != 0:
                            self.db.adjust_for_split(t, split_dt.strftime('%Y-%m-%d'), float(factor))

                # Download pricing data
                df = stock.history(start=start_date, end=end_date, auto_adjust=False)
                
                if df.empty:
                    if t in self.cfg.exclude_tickers:
                        logger.warning(f"Ticker '{t}' returned empty dataset. Matches exclusion criteria.")
                        self.db.purge_ticker(t)
                    continue

                df = df.reset_index()
                df['ticker'] = t
                
                # Column normalization
                df.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Adj Close': 'adj_close',
                    'Volume': 'volume'
                }, inplace=True)

                if 'adj_close' not in df.columns:
                    df['adj_close'] = df['close']

                df['date'] = pd.to_datetime(df['date']).dt.date
                records.append(df[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']])
            except Exception as e:
                logger.error(f"Failed processing ingestion for ticker '{t}': {e}")
                if t in self.cfg.exclude_tickers:
                    self.db.purge_ticker(t)

        if records:
            return pd.concat(records, ignore_index=True)
        return pd.DataFrame()

    def run_initial_load(self, years: int = 6):
        logger.info(f"Initiating initial historical load ({years} years)...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=years * 365)
        df = self.fetch_and_clean(
            self.cfg.tickers, 
            start_dt.strftime('%Y-%m-%d'), 
            end_dt.strftime('%Y-%m-%d')
        )
        self.db.upsert_dataframe(df)
        logger.info("Initial load operation completed.")

    def run_daily_load(self):
        # Verify US Trading Calendar
        nyse = mcal.get_calendar('NYSE')
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        sched = nyse.schedule(start_date=today_str, end_date=today_str)
        
        if sched.empty:
            logger.info("US Market is closed today (Holiday/Weekend). Daily load execution skipped.")
            return

        logger.info("US Market active. Executing daily load (trailing 4 calendar days)...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=4)
        
        df = self.fetch_and_clean(
            self.cfg.tickers, 
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
# 4. APACHE ARROW FLIGHT DAEMON
# =====================================================================

class MarketDataFlightServer(flight.FlightServerBase):
    def __init__(self, location: str, db_mgr: DatabaseManager, ingest_engine: IngestionEngine):
        super().__init__(location)
        self.db = db_mgr
        self.engine = ingest_engine

    def do_get(self, context, ticket):
        try:
            req = json.loads(ticket.ticket.decode('utf-8'))
            sql_query = req.get("sql")
            logger.info(f"Serving Arrow Flight query: {sql_query}")
            arrow_table = self.db.execute_query(sql_query)
            return flight.RecordBatchStream(arrow_table)
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
            
            # Save report JSON file to disk
            out_dir = os.path.join(config.duckdb_dir, "reports")
            os.makedirs(out_dir, exist_ok=True)
            file_name = f"dollar_volume_{target_date or datetime.now().strftime('%Y%m%d')}.json"
            
            with open(os.path.join(out_dir, file_name), "w") as f:
                json.dump(report_data, f, indent=2)

            return flight.Result(json.dumps({"status": "SUCCESS", "report": report_data}).encode('utf-8'))

        else:
            raise flight.FlightServerError(f"Unknown action: {action_type}")

# =====================================================================
# 5. MAIN ENTRY POINT WITH SIGNAL TRAPPING
# =====================================================================

def main():
    db_manager = DatabaseManager(config.db_path)
    ingest_engine = IngestionEngine(db_manager, config)

    # Signal trapping for clean WAL checkpointing
    def shutdown_handler(signum, frame):
        logger.warning(f"Shutdown signal {signum} trapped. Commencing emergency WAL checkpoint...")
        db_manager.close()
        logger.info("Database closed safely. Exiting daemon process.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    location = f"grpc://{config.flight_host}:{config.flight_port}"
    server = MarketDataFlightServer(location, db_manager, ingest_engine)
    logger.info(f"Apache Arrow Flight Daemon operational on {location}")
    
    server.serve()

if __name__ == "__main__":
    main()

```

---

## 5. Verification & Client Usage Scenarios

Downstream client processes (`RO` workers) submit zero-copy Arrow Flight requests over the network layer.

### 5.1 Execute Concurrent Read Query from RO Client

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://127.0.0.1:8815")

# Fetch AAPL daily records zero-copy
ticket_payload = json.dumps({"sql": "SELECT * FROM daily_quotes WHERE ticker = 'AAPL' ORDER BY date DESC"})
ticket = flight.Ticket(ticket_payload.encode('utf-8'))

reader = client.do_get(ticket)
arrow_table = reader.read_all()

# Convert instantly to pandas DataFrame without lock conflicts
df = arrow_table.to_pandas()
print(df.head())

```

### 5.2 Trigger On-Demand Ingestion Action

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