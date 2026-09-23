# Data Service API — Consumer Guide

**Audience:** other projects that consume the `stock-ingestion` data
service (e.g. `fundamental_stock_ranking` P3.1 market-data ingestion).
This document is the complete contract: endpoint, query protocol,
schema, semantics, and caveats. You do not need the rest of this repo —
only `pyarrow` and this page.

**Version:** schema v7 / service live as of 2026-09-22.

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

> **Non-equity series:** `daily_close` also holds non-equity series —
> `^TNX`, `CL=F` and `^IRX` (macro series added for the Correlation
> dashboard). `Close` is a yield percent for the `^` indices and
> USD/bbl for `CL=F` — not a share price. Calendars are ragged
> (`CL=F` prints on equity-market holidays; partial-day index rows
> can be absent) and non-positive closes exist (`CL=F` Apr-2020
> negative, `^IRX` ~0% in Mar-2020) — inner-align on `Date` when
> combining tickers and do not assume `Close > 0`.

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
`Log_Close_EMA18`/`Log_Close_EMA40` are additionally NULL wherever
`Close/EMA <= 0` (non-positive closes like the `CL=F` Apr-2020
negative print, or a negative EMA window) — the guarded `LN` writes
NULL instead of failing the rebuild.

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

Backfills from `fear_greed.start_date` (default `2020-08-01`) via the
`fear_greed_refresh` action; every completed OHLCV load also refreshes
it best-effort (the `fear_greed` sub-block on the load result).
Absent sub-indicator series land as NULL columns — never an error.
CNN retains only ~6 years of history: a `start_date` behind the
advancing retention floor makes the dated `graphdata/{start_date}`
endpoint return HTTP 500, so on `requests.HTTPError` the load retries
once against the undated `graphdata` URL (rolling ~1 year) and reports
`start_date_fallback: true` + the actual URL in `fetched_url`.
Timeouts/connection errors do not fall back (same host), and a failing
retry surfaces as `FAILED`.

### 3.6 `signal_records` — ingested trading signals

One row per signal row ingested from `*_active_signal_combined.xlsx`
files (written by the `signal_pipeline` action — see §4). NUMERIC
types are verbatim from the legacy `signals.duckdb` schema.

| Column | Type | Notes |
|---|---|---|
| `Signal_File` `Model` `Ticker` | VARCHAR | source file name, model name, symbol |
| `Trading_Date` | DATE | signal date |
| `Open` `High` `Low` `Close` `Hist_WinRate` `ATR` `best_k` `Target` `Current_Price` `Stop_Price` `Target_Percent` `Stop_Percent` `Reward_Risk_Ratio` `Centroid_Dist` | NUMERIC(12,2) | NULL `Close` rows are repaired from `daily_close` during the pipeline run |
| `Cluster` | NUMERIC(1,0) | cluster id 0–5 (see `cluster_metadata`) |
| `Shares_to_buy` `Max_Drawdown` | NUMERIC(5,0) | position sizing / worst historical drawdown |
| `Drift_Warning` | BOOLEAN | feature-drift flag |

PK `(Ticker, Trading_Date, Cluster, Model, Signal_File)` — re-ingesting
the same file is a no-op. A dedupe step keeps only the latest
`Signal_File` per `(Trading_Date, Model, Ticker)`, and a prune step
deletes rows failing the configured thresholds.

### 3.7 `signal_performance` — forward-return outcome per signal

PK `(Ticker, Trading_Date, Cluster, Model, Signal_File)` — one row per
evaluated signal, computed from `daily_close` prices strictly after
`Trading_Date`:

| Column | Type | Notes |
|---|---|---|
| `Hit_Target_Date` / `Hit_Target_T` | DATE / INTEGER | first forward day with `Close >= Target` |
| `Hit_Stop_Date` / `Hit_Stop_T` | DATE / INTEGER | first forward day with `Close <= Stop_Price` |
| `Time_Exit_Close` | NUMERIC(12,2) | window-end close when neither hit (and `total_forward_days >= max_holding_days`) |
| `T` | INTEGER | `LEAST` of the hit Ts when both occur, else the single hit T, else `max_holding_days` |

Signals without enough forward days (no hit AND
`total_forward_days < max_holding_days`) are skipped entirely.
`signals.overwrite_existing` selects `INSERT OR REPLACE` vs
`INSERT ... ON CONFLICT DO NOTHING` on re-runs.

### 3.8 `cluster_metadata` — cluster id lookup

`Cluster` INTEGER PK, `Description` VARCHAR; 6 rows (0–5), refreshed
verbatim on every `signal_pipeline` run.

### 3.9 `signals_<name>` — per-set signal-analysis schemas

The `signal_analysis` action creates one isolated schema per configured
`signal_analysis.sets` entry (`signals_version_p`,
`signals_version_q`, …). Every object carries the `aly_` prefix and
lives only inside its set's schema:

- `aly_signal_records_raw` — 24-column raw ingest table; PK
  `(Trading_Date, Model, Ticker, Signal_File)`; dropped & recreated on
  every run.
- Four views built from the raw table:
  `aly_v_signals_baseline_all_signals`,
  `aly_v_signals_strict_eod_price_above_open`,
  `aly_v_signals_intraday_high_rr`,
  `aly_v_signals_atr_target_multiple_filter`.
- `aly_signal_performance` — 15-column forward-outcome table; unlike
  `main.signal_performance` (§3.7) it includes `Time_Exit_Date`,
  `Next_Day_Open`, `Open_Gap_Pct`, `Execution_Status`, and **stop wins
  same-day target/stop ties** (`Hit_Target_T` only when
  `Raw_Target_T < Raw_Stop_T`).

These are runtime-created schemas — no `SCHEMA_VERSION` migration (stays
`7`). Reports land under
`reports.report_dir/signal_analysis/<name>/`.

### 3.10 Audit/internal tables (available but not a stable contract)

`ingestion_runs` (run log), `splits_processed` (split-repair
bookkeeping), `schema_version` (migration markers — current `7`).

---

## 4. Actions — `do_action`

`do_action(flight.Action("<name>", b"<json payload>"))` returns JSON
result rows. Payloads are optional JSON objects unless noted.

| Action | Payload | Returns | Purpose |
|---|---|---|---|
| `health_check` | — | `{status, db_connected, db_path, ticker_count, daily_close_count, schema_version, uptime_seconds, last_run}` | Service/DB liveness |
| `data_health` | `{gap_days?: int≥1}` | `{status, health:{health_status: OK\|WARN\|CHECK_FAILED, mode, as_of, latest_trading_day, pending_session, checks:{staleness, coverage, integrity, duplicates, gaps}, daily_close:{…own status…}, fear_greed:{health_status: OK\|WARN\|CHECK_FAILED\|DISABLED, …}, weekly:{health_status, …}, company_metadata:{health_status, …}}}` | Full composite data-quality report (see note below) |
| `daily_load` | `{force?: bool}` | `{status: SUCCESS\|PARTIAL\|SKIPPED, tickers_*, rows_upserted, health, metadata, weekly}` | Today's incremental load (write) |
| `initial_load` | `{years?: int 1–30}` | like `daily_load` | Full-history load (write) |
| `on_request_load` | `{tickers: [str], start_date, end_date}` — **required** | like `daily_load` | Custom-range backfill (write) |
| `metadata_load` | `{tickers?: [str], refresh?: bool, delay_s?: num≥0, max_tickers?: int≥1}` | `{status: SUCCESS\|PARTIAL\|ABORTED, ...}` | `company_metadata` backfill (write, yfinance-paced) |
| `weekly_refresh` | — (empty) | `{status: SUCCESS\|FAILED, weekly_close_rows, weekly_indicators_rows, duration_ms}` | Rebuild both weekly tables (write, pure SQL ~17 s) |
| `fear_greed_refresh` | `{start_date?: "YYYY-MM-DD"}` | `{status: SUCCESS\|FAILED, rows_upserted, latest_date, fg_score, fg_rating, alert_triggered, alert_detail, missing_series, fetched_url, start_date_fallback, duration_ms}` | CNN Fear & Greed fetch + `fear_greed_history` upsert (write); HTTPError on the dated URL retries once on the undated URL (~6y retention floor — see §3.5) |
| `signal_pipeline` | `{folder_path?: str, backdate_days?: int≥1, max_holding_days?: int≥1, overwrite_existing?: bool, min_reward_risk?: num, min_target_pct?: num, exclude_clusters?: [int], exclude_models?: [str], filter_below_open?: bool}` — all optional, each overrides the `signals:` config | `{status: SUCCESS\|FAILED, steps: {ingest, dedupe, repair, prune, performance, repair_sweep, null_verification, cluster_metadata}, duration_ms}` | Six-step signal pipeline server-side (write): ingest xlsx signals → dedupe → repair NULL closes from `daily_close` → prune (thresholds active; pruned rows returned in `steps.prune.pruned_records`, no log file) → `signal_performance` compute → silent repair sweep + NULL verification → `cluster_metadata` refresh |
| `signal_analysis` | `{set?: str, backdate_days?: int≥1, max_holding_days?: int≥1, mc_iterations?: int≥1}` — all optional; `set` must match a configured `signal_analysis.sets` name (omit = all sets), numbers override the `signal_analysis:` config | `{status: SUCCESS\|PARTIAL\|FAILED, sets: {<name>: {status, schema: signals_<name>, report_dir, steps: {ingest, dedupe, repair, views, performance, phase5}}}, duration_ms}` | Five-phase `signal_input2` analyzer server-side (write): ingest all sheets of `*_active_signal_combined.xlsx` into `signals_<name>.aly_signal_records_raw` → dedupe + NULL/NaN `Close` repair from `daily_close` → four `aly_v_*` views → 15-col `aly_signal_performance` + per-view `scorecard` + `edge_validation` metrics (expectancy, profit factor, fill rate, SPY alpha, MC) under `steps.performance` → phase-5 analytics matrix + xlsx/heatmap reports |
| `daily_report` | `{date?: "YYYY-MM-DD"}` | `{status, report}` | Top-20 dollar-volume report (also writes a file server-side) |
| `dml_admin` | `{op: <enum>, ...}` — see §4.1 | `{status: SUCCESS\|FAILED\|REJECTED, op, target, rows_written?, error?, ...}` | Client-issued DML confined to `client_dml.allowed_namespaces` (the `pivot` contract): create/replace/append/drop registered tables and views, pin/unpin, TTL sweep. Every op is audited in `client_dml_audit` — including REJECTED ones. |
| `get_run_history` | — | `{status, run_history: [...]}` | `ingestion_runs` audit log |
| `sync_to_r2` | — | R2 sync result | Push DB artifact to Cloudflare R2 |

### 4.1 Client-issued DML — the `pivot` namespace (`dml_admin`)

Trusted consumers can store derived tables **in the same DuckDB
catalog** under a server-owned schema (`pivot` for the Bollinger pivot
project) instead of maintaining a separate local database. Everything
is validated, confined and audited server-side — no arbitrary DDL or
mutation SQL ever reaches the engine.

**Naming.** Every object is addressed as `{"table": "pivot.<name>"}`
or `{"table": "<name>", "namespace": "pivot"}` (both given ⇒ they must
agree). `namespace` and `<name>` must match `^[a-z][a-z0-9_]*$`. The
server composes the qualified name itself — clients can never spell
`main.daily_close` or any other core object. Only namespaces listed in
`client_dml.allowed_namespaces` (default: `pivot`) are writable, and
they must first be `register_namespace`d.

**Result envelope.** Every op returns
`{status: SUCCESS|FAILED|REJECTED, op, target, ...}`.
`REJECTED` means validation refused the operation before touching the
catalog (bad name, non-allowlisted namespace, unregistered table,
non-SELECT source SQL, unknown op); `FAILED` means a validated op hit a
DuckDB error at execution (e.g. a PK violation). Both are normal JSON
results — and both are written to `client_dml_audit`.

**Operations** (`payload.op`, closed enum):

| op | Extra payload keys | Behaviour |
|---|---|---|
| `register_namespace` | `namespace` | Create/confirm the schema; REJECTED unless allowlisted. |
| `create_table` | `table`, `columns` | Structured spec → `CREATE TABLE` with a real `PRIMARY KEY`. `columns` is a list of `{name, type, not_null?, pk?}`; `type` must be one of `DATE`, `INTEGER`, `BIGINT`, `DOUBLE`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP`, `UTINYINT`, `SMALLINT`. Duplicate-key inserts fail loudly. REJECTED if the object already exists. |
| `create_as` | `table`, `sql` | `SELECT`-only source → snapshot to Arrow → `CREATE OR REPLACE TABLE` under the write lock (the source read runs *outside* the lock but inside `dml_timeout_seconds` — an over-budget scan is interrupted and fails the op). REJECTED on PK-bearing targets (CTAS cannot preserve constraints — use `insert_select`). |
| `create_view` | `table`, `sql` | `SELECT`-only source → `CREATE OR REPLACE VIEW`. |
| `insert_select` | `table`, `mode`, `sql` | `mode: append` inserts; `mode: replace` runs `DELETE FROM` + `INSERT INTO ... SELECT` in **one transaction** — a failed source leaves the old rows intact. PK constraints stay active. |
| `drop_table` | `table` | Drops a registered table or view (`DROP TABLE`/`DROP VIEW` chosen by catalog kind). REJECTED for unregistered objects. |
| `list_tables` | `namespace?` | Returns the registry entries (optionally filtered to one allowlisted namespace). |
| `pin` / `unpin` | `table` | Set/clear `pinned` on a registered object — TTL sweep eligibility. |
| `dml_sweep` | `ttl_days?` | Drops registered objects with `pinned=false` whose `last_write` is older than the TTL (default `client_dml.ttl_days`, 90 days). Pinned and fresh objects are never touched. |

Optional `client_tag` on any op is recorded in the audit row.

**Source-SQL rules** (`create_as`, `create_view`, `insert_select`):
must begin with `SELECT` or `WITH`; must survive a regex denylist
covering four families —

- **mutation / session / catalog verbs:** `COPY`, `ATTACH`, `DETACH`,
  `LOAD`, `INSTALL`, `SET`, `CALL`, `PREPARE`, `EXECUTE`, `SECRET`,
  `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `MERGE`;
- **file / remote readers:** every `read_*`, `parquet_*`, `sqlite_*`,
  `st_read*`, `postgres_*`, `mysql_*`, `odbc_*` name, plus `glob`,
  `sniff_csv`/`csv_sniffer`, `delta_scan`, `iceberg_scan`,
  `query`/`query_table` and the `json_*serialize*_sql` plan-execution
  helpers;
- **introspection / side effects:** every `pragma*` and `duckdb_*`
  name, `current_setting`, `which_secret`, `setvariable`/`getvariable`,
  `checkpoint`/`force_checkpoint`, `nextval`/`setval`/`currval`,
  `setseed`, `copy_database`, `pg_sleep`/`sleep_ms`;
- **file/URL literals:** a quoted token in table position
  (`FROM '<path>'`, `JOIN 'https://…'`, `FROM t, "a/b.csv"`), any
  `'scheme://…'` literal, absolute/relative/UNC/home path literals, and
  literals ending in a known data-file extension (`'.csv'`,
  `'.parquet'`, `'.duckdb'`, …) are rejected wherever they appear —
  this kills the DuckDB replacement-scan shorthand that reads files or
  autoloads httpfs for remote URLs.

The statement must also classify via `duckdb.extract_statements` as
exactly one SELECT-type statement (so `WITH x AS (DELETE ...) SELECT`
and multi-statement payloads are rejected too). A client transaction
verb can never cross the wire — `replace` is the atomic-rebuild
primitive.

Two honest caveats. First, the denylist is deliberately
over-broad: a string *value* that merely resembles a file
(`WHERE note = 'report.csv'`) or an identifier that collides with a
denied family (a column named `query`) is rejected — quote values
accordingly. Second, it cannot *prove* total closure: file readers
introduced by future DuckDB versions/extensions outside these
families could slip through (verified on 1.5.5 that platform settings
cannot be scoped per-cursor — `enable_external_access` is GLOBAL-only
and a one-way switch — so the denylist is the enforcement layer).
It is defense-in-depth on a **trusted-client** write surface, not a
sandbox for hostile SQL.

**Lifecycle & housekeeping.** `main.table_registry` records every
client object (`full_name`, `owner`, `has_pk`, `pinned`, `created_at`,
`last_write`, `rows_est`) and is the source of truth for
insert/replace/drop eligibility; `main.client_dml_audit` logs every op
with `client_tag`, source-SQL SHA-256, input bytes, rows written,
status, error and duration. `dml_timeout_seconds` bounds each op
(default: the shared `query_timeout_seconds`); `max_namespace_mb` is an
optional advisory per-namespace size cap (off by default). After each
write the server asserts the object's catalog location via
`duckdb_tables()`.

`scripts/dml_client.py` vendors a thin `DmlClient` wrapper for all of
the above — copy it into a downstream project.

Notes:

- Load results embed `health`, `metadata`, `weekly`, and `fear_greed`
  sub-blocks — every completed OHLCV load already refreshes the
  derived layers.
- `health` is a **composite, additive** report (F17-T1): all existing
  `daily_close` keys stay at the top level of `health` (unchanged for
  pre-F17 consumers), and three sub-reports are added under
  `health.fear_greed`, `health.weekly`, and `health.company_metadata`,
  each with its own `health_status` and evidence fields; a
  `health.daily_close` key mirrors the daily report with its own
  status. The top-level `health.health_status` is the worst across the
  four sections (`OK < WARN < CHECK_FAILED`; a `DISABLED` section —
  e.g. `fear_greed.enabled: false` — is excluded from the rollup). A
  section that fails reports `CHECK_FAILED` + `error` in that section
  only; the report never raises and never changes a load's status.
  - `fear_greed`: presence (empty → WARN), `MAX(date)` staleness vs
    the latest ended NYSE session (`stale`, `latest_date`,
    `expected_date`, `days_behind`, `pending_session`), integrity
    (`fg_score` outside [0,100] / `fg_rating` outside the valid
    vocabulary / NULL `fg_score` counts + bounded samples), and
    fully-NULL sub-indicator series → `missing_series`.
  - `weekly`: `MAX(weekly_close.Date)` vs the expected nominal-Friday
    key (`latest_week`/`expected_week`), `weekly_close` vs
    `weekly_indicators` row-count parity + missing `(Date,Ticker)`
    keys (bounded `missing_keys`), `stale_tickers` absent from
    `daily_close`, NULL OHLC / `Volume < 0` counts.
  - `company_metadata`: configured-minus-excluded tickers missing from
    the table → WARN (`missing_tickers`, bounded); all-NULL field rows
    are informational (`null_field_rows`), never a finding.
  - The same composite is embedded under `health` on completed
    `initial_load`/`daily_load` results (light mode — no gap section);
    `SKIPPED` results and `on_request_load` carry no `health` block.
- Invalid payload fields raise `FlightServerError` (fail-fast, never
  silently coerced).

## 5. Uploads — `do_put`

`do_put` has **two paths**, selected by the Flight descriptor:

1. **Legacy `daily_close` upsert** — no descriptor, a PATH descriptor,
   or any CMD descriptor that is not a JSON object carrying `"table"`.
   The Arrow stream must match the `daily_close` schema exactly
   (7 columns, PK `(Date,Ticker)` semantics); rows are upserted in one
   vectorized operation and `exclude_tickers` are filtered out. This is
   the historical contract and is unchanged.
2. **Generalized client-DML write** — a CMD descriptor whose body is a
   JSON object `{"table": "pivot.<name>", "mode":
   "create_or_replace|append|replace"}` (optional `namespace`,
   `client_tag`). The stream is written into an allowlisted namespace
   with the same confinement, validation and auditing as `dml_admin`
   (§4.1):
   - `create_or_replace` — materializes the stream into a fresh table
     (refused on PK-bearing targets — constraints would be lost);
   - `append` — inserts into a registered table; PK violations and
     schema mismatches are rejected;
   - `replace` — atomic `DELETE` + `INSERT` in one transaction on a
     registered table.

   A rejected/failed write raises `FlightServerError` on the client;
   the `do_put:<mode>` audit row is still recorded server-side.

Use only if you are a trusted producer — ordinary consumers should not
write.

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
