# Data Service API — Consumer Guide

**Audience:** other projects that consume the `stock-ingestion` data
service (e.g. `fundamental_stock_ranking` P3.1 market-data ingestion).
This document is the complete contract: endpoint, query protocol,
schema, semantics, and caveats. You do not need the rest of this repo —
only `pyarrow` and this page.

**Version:** schema v6 / service live as of 2026-09-20.

---

## 1. Endpoint

| Setting | Value |
|---|---|
| Protocol | Apache Arrow Flight over gRPC |
| Address | `grpc://127.0.0.1:8815` (`server.flight_host` / `server.flight_port` in `src/config.yaml`) |
| Service | Windows scheduled task **"Flight Server"** (auto-starts at boot) |
| Backing store | `C:\Intel\parquet\stock_ingestion.duckdb` (DuckDB) |
| Query timeout | 120 s per `do_get` (configurable server-side; no row limit) |

Liveness probe:

```python
import pyarrow.flight as flight
client = flight.FlightClient("grpc://127.0.0.1:8815")
client.wait_for_available(timeout=5)   # raises if the service is down
```

---

## 2. Querying — `do_get`

The ticket is a **JSON object with one `sql` key**. Only `SELECT` /
`WITH` statements are accepted — DDL and DML are rejected
(`FlightServerError`). Results stream back as Arrow record batches.

```python
import json
import pyarrow.flight as flight

def query(sql: str, timeout: float = 120.0):
    client = flight.FlightClient("grpc://127.0.0.1:8815")
    client.wait_for_available(timeout=5)
    ticket = flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))
    reader = client.do_get(
        ticket, options=flight.FlightCallOptions(timeout=timeout)
    )
    return reader.read_all().to_pandas()      # or .to_polars()

df = query("""
    SELECT Date, Ticker, Close
    FROM weekly_close
    WHERE Ticker IN ('XLK','XLF') AND Date >= '2025-01-01'
    ORDER BY Date
""")
```

### Type caveat — DATE columns

`to_pandas()` delivers `DATE` as **object-dtype `datetime.date`
values**, not `datetime64`. If you pivot or call `.date()`/`.dt.*`,
normalize first:

```python
df["Date"] = pd.to_datetime(df["Date"])
```

### Guidelines

- Build `IN (...)` lists as a quoted join (`'A','B'`) — never Python
  `tuple()` repr (a single element produces `('X',)`); validate ticker
  strings (`^[A-Z0-9.\-]+$`).
- Prefer pushing filters/aggregations into SQL — DuckDB executes them
  far faster than client-side pandas.

---

## 3. Schema

### 3.1 `daily_close` — canonical daily OHLCV (PRIMARY source)

| Column | Type | Notes |
|---|---|---|
| `Date` | DATE | NYSE trading day |
| `Ticker` | VARCHAR | uppercase symbol |
| `Open` `High` `Low` `Close` | DOUBLE | **yfinance `auto_adjust=True`** |
| `Volume` | DOUBLE | raw shares (unadjusted) |

PK `(Date, Ticker)`. ~10-year history (config `initial_load_years`).

> **⚠ Price-basis caveat (read before valuation work):** OHLC are
> **total-return adjusted** — yfinance `auto_adjust=True` bakes in
> *both splits and dividends* (the legacy `Adj Close` factor applied to
> OHLC). This is the right basis for returns/signals, but it is **not
> a raw historical close** — do **not** multiply it by a
> point-in-time share count for market cap. If your use case needs raw
> prices, flag it to the pipeline owner before building on `Close`.

### 3.2 `weekly_close` — weekly OHLCV derived from `daily_close`

Same columns/types/PK as `daily_close`, but `Date` is the **nominal
Friday** of the ISO week (`date_trunc('week', d) + 4`):

- Holiday-shortened weeks still carry the Friday key.
- The **current partial week is present** once any daily row exists —
  treat the latest row as a live bar until the week's Friday load.
- Aggregation: first `Open` / `max(High)` / `min(Low)` / last `Close` /
  `sum(Volume)`.

### 3.3 `weekly_indicators` — technical indicators on `weekly_close`

PK `(Date, Ticker)`, `Date` on the same Friday basis as
`weekly_close`. Columns:

| Group | Columns |
|---|---|
| Slow Stochastic | `Slow_K`, `Slow_D`, `Stoch_Crossover` (INT: +1/−1/0), `Slow_D_80_20_Crossover` |
| Bollinger | `BB_Upper`, `BB_Central`, `BB_Lower`, `BB_Reversal_Signal` (INT) |
| PPO | `PPO`, `PPO_Signal`, `PPO_Hist_Crossover` (INT), `PPO_Zero_Crossover` (INT) |
| Force Index | `Force_Index`, `Force_Zero_Crossover` (INT) |
| Log EMAs | `Log_Close_EMA18`, `Log_Close_EMA40` |

Early rows have NULLs during window warmup (13/19/25/39-week windows)
— legacy-identical behavior; filter with `IS NOT NULL` as needed.

### 3.4 `company_metadata` — per-ticker reference data

| Column | Type | Source (`yf.Ticker.info`) |
|---|---|---|
| `ticker` | VARCHAR PK | — |
| `company_name` | VARCHAR | `longName` |
| `sector` | VARCHAR | `sector` |
| `industry` | VARCHAR | `industry` |
| `quote_type` | VARCHAR | `quoteType` (`EQUITY`, `ETF`, …) |

NULL fields are meaningful (e.g. ETFs have no sector/industry on
Yahoo). Coverage grows over time — the daily load auto-backfills
missing tickers.

### 3.5 `fear_greed_history` — CNN Fear & Greed index

One row per calendar date, PK `date` (DATE). The composite 0–100
index plus each of CNN's 9 sub-indicators' raw metric value and
textual rating; `ingested_at` (TIMESTAMPTZ) is stamped on every write.

| Column | Type | Notes |
|---|---|---|
| `fg_score` | DOUBLE | composite Fear & Greed score (0–100) |
| `fg_rating` | VARCHAR | `extreme fear` / `fear` / `neutral` / `greed` / `extreme greed` |
| `stock_price_strength` `stock_price_breadth` `market_momentum_sp500` `market_momentum_sp125` `market_volatility_vix` `market_volatility_vix_50` `put_call_options` `junk_bond_demand` `safe_haven_demand` | DOUBLE | the **raw** metric (VIX level, put/call ratio, SPX price, breadth oscillator…) — **not** a 0–100 score |
| `<key>_rating` (one per key above) | VARCHAR | per-series rating label |

Backfills from `fear_greed.start_date` (default `2014-01-01`) via the
`fear_greed_refresh` action; every completed OHLCV load also refreshes
it best-effort (the `fear_greed` sub-block on the load result).
Absent sub-indicator series land as NULL columns — never an error.

### 3.6 Audit/internal tables (available but not a stable contract)

`ingestion_runs` (run log), `splits_processed` (split-repair
bookkeeping), `schema_version` (migration markers — current `6`).

---

## 4. Actions — `do_action`

`do_action(flight.Action("<name>", b"<json payload>"))` returns JSON
result rows. Payloads are optional JSON objects unless noted.

| Action | Payload | Returns | Purpose |
|---|---|---|---|
| `health_check` | — | `{status, db_connected, db_path, ticker_count, daily_close_count, schema_version, uptime_seconds, last_run}` | Service/DB liveness |
| `data_health` | `{gap_days?: int≥1}` | `{status, health:{health_status: OK\|WARN\|CHECK_FAILED, staleness, coverage, integrity, duplicates, gaps, pending_session}}` | Full `daily_close` data-quality report |
| `daily_load` | `{force?: bool}` | `{status: SUCCESS\|PARTIAL\|SKIPPED, tickers_*, rows_upserted, health, metadata, weekly}` | Today's incremental load (write) |
| `initial_load` | `{years?: int 1–30}` | like `daily_load` | Full-history load (write) |
| `on_request_load` | `{tickers: [str], start_date, end_date}` — **required** | like `daily_load` | Custom-range backfill (write) |
| `metadata_load` | `{tickers?: [str], refresh?: bool, delay_s?: num≥0, max_tickers?: int≥1}` | `{status: SUCCESS\|PARTIAL\|ABORTED, ...}` | `company_metadata` backfill (write, yfinance-paced) |
| `weekly_refresh` | — (empty) | `{status: SUCCESS\|FAILED, weekly_close_rows, weekly_indicators_rows, duration_ms}` | Rebuild both weekly tables (write, pure SQL ~17 s) |
| `fear_greed_refresh` | `{start_date?: "YYYY-MM-DD"}` | `{status: SUCCESS\|FAILED, rows_upserted, latest_date, fg_score, fg_rating, alert_triggered, alert_detail, missing_series, duration_ms}` | CNN Fear & Greed fetch + `fear_greed_history` upsert (write) |
| `daily_report` | `{date?: "YYYY-MM-DD"}` | `{status, report}` | Top-20 dollar-volume report (also writes a file server-side) |
| `get_run_history` | — | `{status, run_history: [...]}` | `ingestion_runs` audit log |
| `sync_to_r2` | — | R2 sync result | Push DB artifact to Cloudflare R2 |

Notes:

- Load results embed `health`, `metadata`, `weekly`, and `fear_greed`
  sub-blocks — every completed OHLCV load already refreshes the
  derived layers.
- Invalid payload fields raise `FlightServerError` (fail-fast, never
  silently coerced).

## 5. Uploads — `do_put`

Accepts an Arrow stream matching the `daily_close` schema exactly
(7 columns, PK `(Date,Ticker)` semantics); upserts in one vectorized
operation; `exclude_tickers` are filtered out. Use only if you are a
trusted producer — ordinary consumers should not write.

## 6. Rules for consumers

1. **Read via Flight only.** The server is the single long-lived
   writer. Never open `stock_ingestion.duckdb` in write mode from
   another process.
2. **Read-only fallback:** if the server is down you may open the file
   with `duckdb.connect(path, read_only=True)` — only when no writer
   holds the lock.
3. **SQL strings are passed through verbatim** — there is no
   parameter-binding protocol on the ticket; construct values safely
   (see §2 guidelines).
4. **Partial week caveat** for signal correctness: `weekly_close`'s
   latest row is a forming bar; `weekly_indicators` reflects it too.

## 7. Common recipes

```sql
-- Latest close per ticker
SELECT Ticker, Close, Date FROM daily_close
QUALIFY ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) = 1;

-- Weekly bars + indicators + sector, joined on the Friday key
SELECT w.Date, w.Ticker, w.Close, i.Slow_K, i.Slow_D,
       i.Slow_D_80_20_Crossover, m.sector
FROM weekly_close w
JOIN weekly_indicators i USING (Date, Ticker)
LEFT JOIN company_metadata m ON m.ticker = w.Ticker
WHERE w.Date >= '2026-01-01'
ORDER BY w.Date, w.Ticker;

-- Freshness check without the full data_health action
SELECT MAX(Date) AS latest_bar, COUNT(DISTINCT Ticker) AS tickers
FROM daily_close;
```
