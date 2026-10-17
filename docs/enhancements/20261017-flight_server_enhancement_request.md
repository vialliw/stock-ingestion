# Enhancement request — stock-ingestion Flight service

**To:** stock-ingestion service owner/PM | **From:** quant_strategy_building3 | **Date:** 17 October 2026
**Context:** Requirements v6 (ChatGPT-approved) needs two data capabilities the service does not yet provide. Both are write-path additions to the existing daily load — no changes to the read contract, no new consumer privileges requested.

## Request 1 — Per-bar raw close / adjustment factor (priority: high)

**Why:** v6 §5.1 requires fills, share counts and per-share commissions on **raw executable prices**; `daily_close` stores only `auto_adjust=True` total-return prices. Without a raw basis, backtest commissions and share counts are distorted around dividends/splits.

**What:** one of (either satisfies the need — your choice):

- **Option A:** add `raw_close DOUBLE` column to `daily_close`, populated from the same yfinance pull with `auto_adjust=False` (raw Close field). Adjustment factor then derivable as `adj/raw` per bar.
- **Option B:** a side table `price_adjustments(date, ticker, factor DOUBLE)` where `factor = adj_close/raw_close` — avoids touching the main table's 7-column contract and `do_put` schema.

**Scope:** full history 2010→present (one repair backfill), then populated on every daily load.

**Already mitigated (interim):** we pulled raw closes ourselves into a project-local DB (`data\quant.duckdb.raw_close`, 1,216 tickers, 4.3M rows) — so this is **not blocking**. Service-side support would retire the duplicate store and keep one source of truth. Note: yfinance "raw" closes are already split-normalized; in-trade split events are already covered by your `splits_processed` table — no work needed there.

## Request 2 — Earnings schedule store with snapshot archival (priority: high)

**Why:** v6 §5.2 requires excluding signals with earnings in the holding window *as the schedule was knowable on the signal date*. Realized dates alone can't prove knowability — promotion-quality evidence requires **prospectively archived schedule snapshots**, i.e. what the calendar said *on each day*, retained with fetch timestamps. This must start accumulating now.

**What:**

- Table `earnings_dates`:
  ```
  ticker VARCHAR, report_date DATE,
  status VARCHAR,        -- 'scheduled' | 'realized' (temporal status)
  verified BOOLEAN,      -- report actually occurred (verification status — kept separate per review)
  source VARCHAR,        -- 'yfinance' | other
  first_seen DATE,       -- first snapshot day this (ticker, report_date) appeared
  last_seen DATE,
  fetched_at TIMESTAMPTZ,
  PRIMARY KEY (ticker, report_date)
  ```
- Table `earnings_snapshots` (or equivalent retention): daily capture of each ticker's forward schedule — `(ticker, snapshot_date, report_date, source)` — so "what was knowable on day X" is answerable later. Append-only.
- New action `earnings_refresh` (or fold into `daily_load`): pulls `get_earnings_dates(limit=100)` per ticker, upserts, appends the day's snapshot; paced like `metadata_load`.
- Health section: flag tickers with suspiciously few quarters (missing-quarter/duplicate/gap/freshness checks — e.g. QRVO and LESL currently return a single row from Yahoo).

**Source:** yfinance covers realized dates back to ~2002 for long-listed names (verified, audit attached in our docs). A paid schedule-history feed is **not** required per external review — but the snapshot archival cannot be backfilled, so the sooner this starts the sooner Stage-3-qualifying evidence exists.

## Not requested (handled our side)

- `is_tradable` flag — derivable via `company_metadata.quote_type='EQUITY'`.
- A `quant` write namespace — our research artifacts stay in the project-local DB.
- Pre-market prices — v6 uses prior-close eligibility only.

## Acceptance criteria

1. `raw_close` or `factor` present for every `daily_close` row 2010→present; spot-check vs a fresh `auto_adjust=False` pull within rounding.
2. `earnings_dates` populated for all EQUITY tickers; snapshot table accumulating daily; anomalies flagged in `data_health`.
3. No change to the `do_get` single-SELECT contract or `do_put` schema for `daily_close` (unless Option A chosen — then document the new column).
