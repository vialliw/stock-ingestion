# Development Plan — Phase 1 Implementation

**Reference:** Review "Recommended Development Sequence" (§gemini_tech_spec.1.0.1.review.md) mapped to multi-agent workflow (§procedure_multi_agents01.md).

**Spec under implementation:** `docs/tech_spec/gemini_tech_spec.1.0.2.md`

**Workflow:** Each cycle = Plan (A) → Execute B/C in parallel → Critic (D) → Fix loop. The Orchestrator (main session) coordinates, resolves conflicts, and approves completion based on evidence.

---

## Cycle mapping overview

| Cycle | Review step | Feature | Parallel duel? | Critic focus |
|---|---|---|---|---|
| 1 | Step 1 (partial) | Config + scaffold | Yes (config module) | Validators, YAML loading, defaults |
| 2 | Step 2 (partial) | DatabaseManager | Yes (DB module) | Upsert, read-only, thread safety, ticker detection |
| 3 | Step 2 (partial) | IngestionEngine | Yes (ingestion module) | Mocked yfinance, split handling, end-date fix, new-ticker backfill |
| 4 | Step 3 | Flight server | Yes (flight module) | SELECT validation, action routing, result format |
| 5 | Step 4 | Integration + hardening | No (assembly + test) | End-to-end: server → load → read → report → shutdown |

Steps 5 (observability) and 6 (Phase-2 hooks) from the review are deferred — they are not core function.

---

## Cycle 1: Config + Scaffold

**Goal:** Project scaffold, dependency manifest, configuration module with Pydantic validation and YAML loader.

### DAG

| ID | Title | Role | Model | Depends | Output files | Acceptance criteria |
|---|---|---|---|---|---|---|
| F1-T1 | Scaffold: update requirements.txt, verify src/config.yaml exists, ensure src/ and tests/ packages | executor | swe | — | `/requirements.txt`, `/src/__init__.py`, `/tests/__init__.py` | requirements.txt contains all 7 deps from spec §12 (duckdb, pyarrow, yfinance, pandas, pandas-market-calendars, pydantic, pyyaml). `/src/config.yaml` already exists (user-defined). `pip install -r requirements.txt` exits 0. |
| F1-T2 | Implement config module: Pydantic nested models, validators, from_yaml loader | executor | swe | F1-T1 | `/src/config.py` | All 5 sub-configs (Database, Ingestion, Server, Logging, Reports). Validators: tickers uppercased+deduplicated, port 1-65535. `from_yaml("src/config.yaml")` loads correctly with user's actual paths. `from_yaml("nonexistent")` returns defaults. `python -c "from src.config import PipelineConfig; c = PipelineConfig.from_yaml('src/config.yaml'); assert c.database.duckdb_dir == 'C:\\Intel\\parquet'; assert c.ingestion.tickers == ['AAPL','AVB','MSFT','NVDA']"` exits 0. |
| F1-T3 | Challenger: independent config module variant | challenger | glm | F1-T1 | `/src/config_challenger.py` | Same public API as F1-T2 (`PipelineConfig.from_yaml`, nested access). Structurally different implementation (e.g. manual YAML parsing vs Pydantic, or different validator approach). Same acceptance test passes. |
| F1-T4 | Critic: test config validation, YAML loading, defaults, edge cases | critic | swe | F1-T2, F1-T3 | `/tests/test_config.py` | Parametrized over both implementations. Tests: (1) YAML loads correctly; (2) missing file → defaults; (3) tickers uppercased; (4) tickers deduplicated; (5) port out of range raises; (6) empty tickers list → []; (7) nested access works (config.database.duckdb_dir etc). `pytest tests/test_config.py -v` exits 0. |

### Cycle execution

```
F1-T1 (scaffold)
  │
  ├── F1-T2 (executor: config.py)     ┐
  │                                   ├── F1-T4 (critic: test_config.py)
  └── F1-T3 (challenger: config_challenger.py) ┘
```

---

## Cycle 2: DatabaseManager

**Goal:** DuckDB manager with schema init, upsert, read-only connections, thread safety, split tracking, and ticker detection helpers.

### DAG

| ID | Title | Role | Model | Depends | Output files | Acceptance criteria |
|---|---|---|---|---|---|---|
| F2-T1 | Implement DatabaseManager: schema init, upsert_dataframe, read-only conn, write lock, purge, delete_ticker_data, record_split, is_split_processed, get_existing_tickers, get_aligned_start_date, detect_new_tickers, generate_top_20_dollar_volume, close | executor | swe | F1-T4 | `/src/db.py` | All methods from spec §10 reference implementation. Uses threading.Lock for writes. Read-only conn for reads. Creates daily_quotes (Volume DOUBLE), splits_processed, schema_version tables. `upsert_dataframe` returns row count. `python -c "from src.db import DatabaseManager"` exits 0. |
| F2-T2 | Challenger: independent DatabaseManager variant | challenger | glm | F1-T4 | `/src/db_challenger.py` | Same public API. Structurally different (e.g. connection pool vs single conn, different lock strategy, different SQL parameter binding). Same acceptance tests pass. |
| F2-T3 | Critic: test DatabaseManager with temp DuckDB files | critic | swe | F2-T1, F2-T2 | `/tests/test_db.py` | Parametrized over both. Tests with tmp_path-isolated DuckDB: (1) schema creates 3 tables; (2) upsert inserts new rows; (3) upsert updates existing rows (MERGE); (4) upsert empty df → 0 rows, no error; (5) read-only conn rejects DELETE; (6) detect_new_tickers returns correct diff; (7) get_aligned_start_date returns MIN(Date); (8) generate_top_20 returns sorted by dollar_volume DESC limit 20; (9) record_split + is_split_processed idempotency; (10) delete_ticker_data removes all rows for ticker. `pytest tests/test_db.py -v` exits 0. |

### Cycle execution

```
F1-T4 (config tests pass)
  │
  ├── F2-T1 (executor: db.py)         ┐
  │                                   ├── F2-T3 (critic: test_db.py)
  └── F2-T2 (challenger: db_challenger.py) ┘
```

---

## Cycle 3: IngestionEngine

**Goal:** yfinance batch ingestion with split detection (DELETE + re-download), new-ticker backfill, end-date exclusivity fix, and rich action results.

### DAG

| ID | Title | Role | Model | Depends | Output files | Acceptance criteria |
|---|---|---|---|---|---|---|
| F3-T1 | Implement IngestionEngine: _batch_download (end+1 fix), _check_and_apply_splits (DELETE + re-download), _normalize_batch_df, fetch_and_clean, run_initial_load, run_daily_load, run_on_request_load | executor | swe | F2-T3 | `/src/ingestion.py` | All methods from spec §10. Batch yf.download() for pricing, per-ticker for splits. End-date +1 day adjustment. Split handling: detect unprocessed splits → record in splits_processed → delete ticker data → re-download from aligned start → upsert. New ticker detection in daily_load. All run_* methods return dict with rows_upserted, tickers_succeeded, tickers_failed, errors, duration_ms. `python -c "from src.ingestion import IngestionEngine"` exits 0. |
| F3-T2 | Challenger: independent IngestionEngine variant | challenger | glm | F2-T3 | `/src/ingestion_challenger.py` | Same public API. Structurally different (e.g. different download retry strategy, different normalization pipeline, different split detection order). Same acceptance tests pass. |
| F3-T3 | Critic: test IngestionEngine with mocked yfinance | critic | swe | F3-T1, F3-T2 | `/tests/test_ingestion.py` | Parametrized over both. Mock yf.download and yf.Ticker.splits. Tests: (1) _batch_download adds +1 day to end_date; (2) fetch_and_clean returns (df, succeeded, failed, errors) tuple; (3) split detection: unprocessed split → delete + re-download; (4) already-processed split → skip; (5) multiple splits → single re-download; (6) empty ticker in exclude_list → purge; (7) empty ticker NOT in exclude_list → warning log, no purge; (8) run_daily_load: new ticker detection + backfill from aligned start; (9) run_daily_load: empty DB → initial_load + trailing refresh; (10) run_daily_load: market closed → SKIPPED. `pytest tests/test_ingestion.py -v` exits 0. |

### Cycle execution

```
F2-T3 (db tests pass)
  │
  ├── F3-T1 (executor: ingestion.py)      ┐
  │                                       ├── F3-T3 (critic: test_ingestion.py)
  └── F3-T2 (challenger: ingestion_challenger.py) ┘
```

---

## Cycle 4: Flight Server

**Goal:** Apache Arrow Flight daemon with SELECT-only do_get validation and 4 do_action endpoints with rich results.

### DAG

| ID | Title | Role | Model | Depends | Output files | Acceptance criteria |
|---|---|---|---|---|---|---|
| F4-T1 | Implement MarketDataFlightServer: do_get (SELECT-only validation, read-only conn, SQL truncation in logs), do_action (initial_load, daily_load, on_request_load, daily_report), rich action results | executor | swe | F3-T3 | `/src/flight_server.py` | do_get: rejects non-SELECT/non-WITH with FlightServerError; executes SELECT via read-only conn; truncates SQL in logs to 200 chars. do_action: routes 4 action types; returns JSON with status (SUCCESS/PARTIAL/SKIPPED), rows_upserted, tickers_succeeded, tickers_failed, errors, duration_ms; daily_report writes JSON to config.reports.report_dir. `python -c "from src.flight_server import MarketDataFlightServer"` exits 0. |
| F4-T2 | Challenger: independent FlightServer variant | challenger | glm | F3-T3 | `/src/flight_server_challenger.py` | Same public API. Structurally different (e.g. different validation approach, different action dispatch pattern, different error handling structure). Same acceptance tests pass. |
| F4-T3 | Critic: test Flight server SELECT validation and action routing | critic | swe | F4-T1, F4-T2 | `/tests/test_flight_server.py` | Parametrized over both. Tests: (1) _validate_select_only accepts "SELECT * FROM..."; (2) accepts "WITH cte AS (...) SELECT..."; (3) rejects "DELETE FROM..."; (4) rejects "DROP TABLE..."; (5) rejects "INSERT INTO..."; (6) rejects empty string; (7) _truncate_sql truncates at 200 chars; (8) do_action routing: initial_load, daily_load, on_request_load, daily_report all return correct status structure. Uses mocked DatabaseManager and IngestionEngine. `pytest tests/test_flight_server.py -v` exits 0. |

### Cycle execution

```
F3-T3 (ingestion tests pass)
  │
  ├── F4-T1 (executor: flight_server.py)      ┐
  │                                            ├── F4-T3 (critic: test_flight_server.py)
  └── F4-T2 (challenger: flight_server_challenger.py) ┘
```

---

## Cycle 5: Integration + Hardening

**Goal:** Assemble all modules into a working daemon, add signal handling, and run end-to-end integration tests. No parallel duel — this cycle is assembly + critic verification.

### DAG

| ID | Title | Role | Model | Depends | Output files | Acceptance criteria |
|---|---|---|---|---|---|---|
| F5-T1 | Implement main entrypoint: assemble config → DatabaseManager → IngestionEngine → FlightServer, signal handling (SIGINT/SIGTERM → CHECKPOINT → close), server.serve() | executor | swe | F4-T3 | `/src/main.py` | main() creates all components from config. Signal handler calls db_manager.close() (CHECKPOINT + close). Location string from config.server.flight_host:flight_port. `python -c "from src.main import main"` exits 0. |
| F5-T2 | Critic: integration test — start server, trigger loads, concurrent reads, report, graceful shutdown | critic | swe | F5-T1 | `/tests/test_integration.py` | Tests: (1) start server in background thread → connect FlightClient → do_get SELECT returns data after initial_load with mocked yfinance; (2) do_action daily_load returns SUCCESS/PARTIAL with rows_upserted > 0; (3) do_action daily_report returns report array with 20 items; (4) concurrent do_get from 3 threads → all succeed; (5) SIGINT → server shuts down with CHECKPOINT log. Uses mocked yfinance, temp DB, temp config. `pytest tests/test_integration.py -v` exits 0. |
| F5-T3 | Critic: select production modules and remove challenger variants | critic | swe | F5-T2 | `/src/__init__.py` (update) | After integration tests pass on both implementations, the Critic selects the better implementation for each module (based on test evidence, code clarity, performance). Update src/__init__.py to export from production modules (config.py, db.py, ingestion.py, flight_server.py, main.py). Challenger variants (config_challenger.py, db_challenger.py, ingestion_challenger.py, flight_server_challenger.py) remain in repo for reference but are not imported by main.py. Document selection rationale in EVENTS.log. |

### Cycle execution

```
F4-T3 (flight tests pass)
  │
  ├── F5-T1 (executor: main.py)
  │       │
  │       ▼
  │   F5-T2 (critic: test_integration.py)
  │       │
  │       ▼
  └── F5-T3 (critic: select production modules, update __init__.py)
```

---

## Post-cycle: Cleanup

After Cycle 5 passes:

1. Run full test suite: `python -m pytest tests/ -v`
2. Verify all challenger variants are not imported by production code
3. Update STATE.md: mark Phase 1 complete, update file tree
4. Update EVENTS.log with final evidence
5. Commit and push

---

## Module-to-file mapping

| Module | Production file | Challenger file | Test file |
|---|---|---|---|
| Config | `/src/config.py` | `/src/config_challenger.py` | `/tests/test_config.py` |
| DatabaseManager | `/src/db.py` | `/src/db_challenger.py` | `/tests/test_db.py` |
| IngestionEngine | `/src/ingestion.py` | `/src/ingestion_challenger.py` | `/tests/test_ingestion.py` |
| FlightServer | `/src/flight_server.py` | `/src/flight_server_challenger.py` | `/tests/test_flight_server.py` |
| Main | `/src/main.py` | — (no duel) | `/tests/test_integration.py` |

---

## Estimated task count

| Cycle | Tasks | Parallel duel? | Estimated commits |
|---|---|---|---|
| 1 | 4 (T1 scaffold + T2/T3 duel + T4 critic) | Yes | 4-5 |
| 2 | 3 (T1/T2 duel + T3 critic) | Yes | 3-4 |
| 3 | 3 (T1/T2 duel + T3 critic) | Yes | 3-4 |
| 4 | 3 (T1/T2 duel + T3 critic) | Yes | 3-4 |
| 5 | 3 (T1 assembly + T2/T3 critic) | No | 3 |
| **Total** | **16 tasks** | | **16-20 commits** |

---

## Key constraints for all cycles

1. **Blackboard protocol:** Every agent reads STATE.md + EVENTS.log first, updates them last.
2. **Output files only:** Agents edit only their task's output_files + blackboard files.
3. **No secrets in code:** R2 credentials come from GitHub secrets, never in source.
4. **Performance hierarchy:** SQL > vectorized > loops. Any loop must be justified.
5. **Python 3.10+:** Source in /src/, tests in /tests/.
6. **Test command:** `python -m pytest tests/`
7. **Completion requires evidence:** Critic must paste pytest output into EVENTS.log.
8. **Challenger divergence:** Challenger implementations must be structurally different from executor's, not cosmetic copies.
9. **Mock yfinance:** All ingestion tests must mock yf.download and yf.Ticker — no real API calls in tests.
10. **Temp DB files:** All DB tests must use tmp_path-isolated DuckDB files — no tests touch ./data/market_data.duckdb.

---

## What happens next

1. **PM reviews this plan** — approve, modify, or reject.
2. On approval, the Orchestrator spawns **Planner A** to write the Cycle 1 DAG into `devin_blackboard/TASKS.json`.
3. Orchestrator spawns **Executor B** and **Challenger C** in parallel as background subagents.
4. After both complete, Orchestrator spawns **Critic D** (foreground).
5. If Critic files BUG tasks, loop B/C on fixes. If clean, advance to Cycle 2.
6. Repeat for Cycles 2-5.
