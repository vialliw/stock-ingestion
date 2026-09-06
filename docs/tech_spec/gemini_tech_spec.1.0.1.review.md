**Overall assessment:** This is a solid, well-structured Phase 1 technical specification. The architecture (single Flight daemon wrapping DuckDB for concurrent RO clients + controlled writes) correctly addresses DuckDB’s single-writer limitations while delivering zero-copy Arrow transfers. The schema is clean, the upsert/MERGE pattern is consistent, split handling is thoughtfully designed with idempotency, and the reference implementation is largely complete and usable as a starting point.

Below is a prioritized review with concrete comments for development planning.

### Strengths
- Clear separation of concerns (Config → DatabaseManager → IngestionEngine → FlightServer).
- Thread-safety via a single write lock + independent read-only connections is the right model.
- New-ticker detection + aligned backfill + “always do trailing 4-day refresh even after empty-DB initial load” are good operational details.
- Split handling (auto_adjust for the fetch window + retroactive adjust of historical rows + `splits_processed` tracking) correctly solves the classic yfinance discontinuity problem.
- Graceful shutdown with CHECKPOINT is present.
- Config is properly nested Pydantic + YAML loader with sensible defaults.
- Report computes `dollar_volume` on the fly (no stored column) and is configurable.

### Critical / High-Priority Issues to Address Before or Early in Development

1. **Split adjustment timing and race with concurrent reads**
   - `adjust_for_split` runs inside `fetch_and_clean` under the write lock (good), but it updates historical rows while clients may be reading. Document or enforce that heavy historical adjustments should preferably happen outside peak query windows, or consider a short exclusive mode / notification.
   - The SQL uses `Date < :split_date`. Confirm yfinance split dates are the first post-split trading day (they usually are). Test with real split examples (e.g., recent NVDA, TSLA, or older AAPL splits).

2. **yfinance `history` end-date semantics**
   - `end=end_date` is exclusive in yfinance. The trailing-4-day and on-request paths will miss the most recent day if you pass “today”. Always pass `end = tomorrow` (or use `period` / adjust by +1 day) and document it.

3. **Empty / delisted handling is asymmetric**
   - Empty DataFrame + ticker in `exclude_tickers` → purge. Empty but not in exclude → silent skip. This is intentional, but add a configurable “soft-fail” log level or counter so operators notice prolonged data outages for non-excluded tickers.
   - Consider also purging (or flagging) tickers that have been empty for N consecutive days even if not in the exclude list.

4. **No schema migration / versioning**
   - `CREATE TABLE IF NOT EXISTS` is fine for green-field, but once the DB exists you will need ALTER paths when the schema evolves (Phase 2). Add a simple `schema_version` table or pragma and a migration function early.

5. **Flight `do_get` SQL injection surface**
   - Clients can send arbitrary SQL. Even with a read-only connection this can still do expensive scans, recursive CTEs, or (in future DuckDB versions) other side effects. At minimum:
     - Enforce a simple allow-list of statements (SELECT only).
     - Or parse/validate with `sqlglot` / DuckDB’s own parser and reject non-SELECT.
     - Add a query timeout and max row limit.

6. **Missing error propagation / status detail in actions**
   - All actions currently return only `{"status": "SUCCESS"}`. Capture exceptions, partial failures (some tickers succeeded, others failed), number of rows upserted, and return richer status. Clients need this for monitoring.

7. **Logging of sensitive / large payloads**
   - Full SQL is logged on every `do_get`. For production, truncate or hash long queries and add structured logging (JSON) with request IDs.

8. **Batch download yfinance**
   - Always call yfinance in batch instead of per ticker for performance.

### Medium-Priority / Design Improvements

- **Config validation**
  - Add Pydantic validators: non-empty host/port range, tickers upper-cased / deduplicated, report_dir / log_file parent dirs writable, etc.
  - Support environment-variable overrides (common for container deployment).

- **Idempotency and partial failures in ingestion**
  - `fetch_and_clean` continues on per-ticker exceptions (good). Aggregate successes/failures and return a summary from the action.
  - Consider a small “ingestion_runs” audit table (run_id, type, start/end, tickers_requested, rows_upserted, status).

- **Report generation**
  - The query is correct. Consider also writing a Parquet/Arrow version alongside the JSON for downstream consumers, and add a “latest” symlink or fixed filename.

### Lower-Priority / Phase-2 Considerations (already deferred, but plan for them)
- `do_put` / external batch ingest: the MERGE path already exists, so adding it later is straightforward.
- Multi-timeframe: the schema is deliberately daily-only; keep it that way until needed.
- Authentication / mTLS for the Flight server (currently open on localhost only — fine for Phase 1).

### Code-Level Observations (quick wins)
- In `adjust_for_split` the log message says “RUNTIME EVENT” while the purge path uses “CRITICAL - RUNTIME EVENT”. Standardize.
- `get_aligned_start_date` returns `str`; keep it as `date` objects internally where possible to avoid format bugs.
- The index `idx_quotes_date_ticker` is redundant with the PRIMARY KEY; DuckDB already indexes the PK. Delete the index.
- `run_daily_load` falls through to the trailing refresh after an empty-DB initial load — confirmed and correct per the changelog.
- Client examples are clear and match the server.

### Recommended Development Sequence
1. **Scaffold & unit tests first**
   - Extract pure functions (split adjustment math, new-ticker detection, report SQL) and test them with synthetic data and real historical split cases.
   - Mock yfinance heavily.

2. **Core DB + ingestion (no Flight yet)**
   - DatabaseManager + IngestionEngine with a CLI entrypoint that can run `initial_load` / `daily_load` / `on_request_load`.
   - Verify MERGE, split tracking, purge, and the 4-day + new-ticker paths against a real (small) universe.

3. **Flight layer**
   - Wire `do_get` (with the SELECT-only guard) and the four actions.
   - Add richer action results and basic metrics (rows affected, duration).

4. **Hardening**
   - Signal handling, logging, config validation, query safeguards, retry around yfinance.
   - Integration test: start server → trigger loads → concurrent RO clients → generate report → graceful shutdown.

5. **Observability & ops**
   - Structured logs, simple health-check action, optional Prometheus metrics endpoint later.
   - Document the exact “first-time bootstrap” procedure and the recommended cron/systemd unit for Phase 1 daily loads.

6. **Phase-2 hooks**
   - Leave clear extension points (post-load callback, config flags for R2, etc.).

### Minor Doc / Spec Clean-ups
- The architecture diagram still shows “Scheduler (Phase 2)” — fine, but note that Phase 1 relies on external triggering.
- Confirm whether `Volume` should stay BIGINT or become DOUBLE (yfinance sometimes returns floats after adjustments).
- Add a short “Known Limitations” section (yfinance data quality, no corporate actions beyond splits, single-node only, etc.).

This spec is already in good enough shape to start coding. The highest-leverage next steps are (1) fixing the yfinance end-date exclusivity, (2) adding SELECT-only validation + richer action status, and (3) building a solid test suite around the split and new-ticker logic. Once those are solid, the rest of Phase 1 should be relatively straightforward. 

