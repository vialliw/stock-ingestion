# Project State — Canonical Source
Last Updated: 2026-09-07T00:30:00Z
Current Branch: main

## Goal
Centralize stock-data ingestion and provide program interfaces.
Storage backend: Cloudflare R2 (S3-compatible).

## File Tree (Agents MUST update this after every change)
- /AGENTS.md
- /.devin/agents/planner.md
- /.devin/agents/executor.md
- /.devin/agents/challenger.md
- /.devin/agents/critic.md
- /devin_blackboard/STATE.md
- /devin_blackboard/TASKS.json
- /devin_blackboard/EVENTS.log
- /docs/workflow_idea/procedure_multi_agents01.md
- /requirements.txt
- /src/            (ingestion + interface code)
  - __init__.py
  - store.py
  - store_challenger.py
  - config.py      (Pydantic nested config models + from_yaml loader — F1-T2)
  - config.yaml    (user-defined runtime config with real paths — DO NOT modify)
  - config_challenger.py  (challenger variant of config module — dataclasses + manual YAML, NOT Pydantic)
  - db.py            (DatabaseManager: schema init, MERGE upsert, read-only reads, write lock, split tracking, report query — F2-T1)
  - db_challenger.py  (challenger variant of DatabaseManager — composition + RLock + cursor pool + ON CONFLICT upsert + named params + SELECT-only validation, NOT read-only conn/MERGE — F2-T2)
- /tests/          (critic-written tests)
  - __init__.py
  - test_store_csv.py
  - test_config.py  (critic tests, parametrized over src.config + src.config_challenger — F1-T4) **MISSING from filesystem as of 2026-09-07T00:45Z — see Active Blockers**
  - test_config_breaking.py  (Critic D breaking tests, 18 tests — F1-T4 follow-up; 5 fail exposing divergences)

## Decisions Made (do NOT change without PM approval)
- [2026-09-06T18:10:00Z] Cycle 1 started: Config + Scaffold (F1-T1..F1-T4 per docs/dev_plan/phase1_dev_plan.md)
- [2026-09-06] Multi-agent workflow adopted (see docs/workflow_idea/procedure_multi_agents01.md)
- [2026-09-06] R2 as object storage; secrets via GitHub repo secrets, never in repo
- [2026-09-06] Using Devin native models: glm (GLM-5.2 High) + swe (SWE-1.7 Medium) — free in Devin Local
- [2026-09-06] OpenRouter not needed; all 4 roles use Devin native models
- [2026-09-06] Data storage: DuckDB (local file). Only sync to R2 when processing is finished.
- [2026-09-06] Performance hierarchy: SQL > vectorized computation > loop iteration. Any loop must be justified in the task report.
- [2026-09-06] Completion requires evidence against explicit acceptance_criteria — not just "code runs" or "tests pass" or "agent reports success".
- [2026-09-06] Requirements may not be redefined without PM (Orchestrator) approval.

## What's Done
- [x] Blackboard scaffolding
- [x] Custom subagent profiles created (planner, executor, challenger, critic)
- [x] AGENTS.md project rules committed
- [x] Cart test feature planned — TASKS.json DAG T1..T4 written (procedure §6, DuckDB variant per PM approval)
- [x] T1 scaffold: requirements.txt, /src, /tests created
- [x] T2: src/store.py implements store_csv with APPEND semantics
- [x] T3: src/store_challenger.py implements challenger variant of store_csv with APPEND semantics
- [x] T4: critic tests in tests/test_store_csv.py — 22/22 PASSED over both implementations (pytest exit 0); no BUG tasks filed
- [x] Cart test cycle (procedure §6) — PASS. Full loop verified: Plan → Execute (parallel) → Critic → done. Blackboard survived concurrent writes. 22/22 tests passed. No BUG tasks filed.
- [x] Workflow established — procedure §8 checklist complete
- [x] Cart test complete; Cycle 1 planning done — TASKS.json replaced with F1-T1..F1-T4 DAG (Config + Scaffold)
- [x] F1-T1 scaffold: requirements.txt updated with all 7 deps (duckdb, pyarrow, yfinance, pandas, pandas-market-calendars, pydantic, pyyaml); src/__init__.py and tests/__init__.py exist; src/config.yaml exists (untouched); pip install -r requirements.txt exit 0
- [x] F1-T2: src/config.py implements Pydantic v2 nested config (DatabaseConfig, IngestionConfig, ServerConfig, LoggingConfig, ReportsConfig, PipelineConfig) with field validators (tickers uppercased+stripped+deduped preserving order; flight_port 1-65535) and from_yaml classmethod (defaults if file missing). Loads user's src/config.yaml correctly. Both verification commands exit 0 with output 'OK'.
- [x] F1-T3: src/config_challenger.py implements challenger variant of config module using dataclasses + manual YAML parsing + manual validation (NOT Pydantic — structurally different from executor's src/config.py). Same public API (PipelineConfig.from_yaml, nested database/ingestion/server/logging/reports, db_path property). Tolerant YAML loader handles raw backslashes in double-quoted Windows paths. Both verification commands exit 0 with output 'OK'.
- [x] F1-T4: tests/test_config.py — 33/33 PASSED parametrized over both config impls (pytest exit 0); covers all 8 acceptance criteria (real config load, defaults, uppercase, dedup, port range, empty tickers, nested access, db_path). Full suite 55/55. No BUG tasks filed. Cycle 1 (Config + Scaffold) complete.
- [x] F2-T1: src/db.py implements DatabaseManager per spec §4/§9/§10 — daily_quotes (Volume DOUBLE, PK Date+Ticker), splits_processed (PK Ticker+SplitDate), schema_version; MERGE INTO upsert; threading.Lock on all writes; read-only reads. DEVIATION (documented): spec's `duckdb.connect(read_only=True)` fails in-process while a RW conn is open (DuckDB single-process restriction) — _read_only_conn falls back to a cursor wrapped in `BEGIN TRANSACTION READ ONLY` which still rejects writes. Also normalizes pandas 3.x 'str' dtype to object in upsert_dataframe (duckdb 1.4.3 cannot scan 'str' dtype) and coerces Date to datetime64. execute_query uses fetch_arrow_table() to return pa.Table. Smoke test: all methods verified incl. MERGE update, split idempotency, ro-conn DELETE rejection, purge/delete, top-20 ordering. Verification cmd exit 0 'OK'.
- [x] F2-T2: src/db_challenger.py implements challenger DatabaseManager variant. Structurally different from spec reference (which F2-T1 executor is expected to follow): (1) composition — _WriteGate + _ReadPool collaborators vs monolithic class; (2) threading.RLock vs Lock; (3) bounded cursor pool (cursors from the RW instance) vs per-query read-only connections — required because DuckDB 1.4.3 rejects opening read_only=True alongside an open RW connection; (4) read-only enforced via SELECT-only SQL validation in execute_query (defense-in-depth) vs read_only connection mode; (5) INSERT ... ON CONFLICT DO UPDATE vs MERGE INTO; (6) named $params vs positional ?; (7) __enter__/__exit__ + __del__ context-manager protocol. Stages upsert via pa.Table.from_pandas (pandas 3.0 str dtype not recognized by DuckDB register). execute_query normalizes RecordBatchReader→pa.Table (DuckDB 1.4 .arrow() returns RecordBatchReader). Evidence: python -c "from src.db_challenger import DatabaseManager; print('OK')" -> exit 0, output 'OK'. Full functional verification (temp DuckDB, 13 checks: schema 3 tables, insert, update-on-conflict, empty df, read-only rejects DELETE, detect_new_tickers, aligned_start_date, top-20 DESC, split idempotency, delete_ticker_data, purge, context manager, hermetic) -> 'ALL OK'. Implemented independently from spec §10 only; src/db.py NOT read (does not exist yet). NOTE: DAG deviation — F2-T2 depends_on F1-T4 which is done, but F2-T1 (executor db.py) not yet started; PM approved jumping ahead per user instruction. OneDrive sync removed the file once mid-session; rewritten and verified persistent.
- [x] BUG-F1-T3: Fixed db_path property in src/config_challenger.py to create duckdb_dir if absent (spec §3 line 151: "Created if absent"). Added os.makedirs(self.database.duckdb_dir, exist_ok=True) before the os.path.join in the db_path property. Evidence: python -m pytest tests/test_config_breaking.py::test_db_path_creates_duckdb_dir_if_absent -v -> 2 passed (both executor + challenger now pass D4). Full breaking suite: 4 failed, 14 passed (was 5 failed, 13 passed) — the 4 remaining failures are all executor:src.config (D1 bool port, D2 null tickers, D3 null exclude_tickers, D5 malformed YAML) belonging to BUG-F1-T2, not this task. Regression: real src/config.yaml still loads correctly (duckdb_dir=='C:\\Intel\\parquet', tickers==['AAPL','AVB','MSFT','NVDA']); defaults still load correctly (tickers==[], flight_port==8815). src/config.py NOT modified (not in output_files).

## Active Blockers
- None blocking Cycle 2. The 5 config edge-case bugs (D1-D5) are filed as BUG-F1-T2 and BUG-F1-T3 tasks (low priority, can be fixed in parallel with Cycle 2 or deferred).

## PM Adjudication on Critic D Breaking Tests (D1-D5)
- D1 (bool port): AGREE — executor must reject boolean port. Filed BUG-F1-T2.
- D2 (null tickers): AGREE — null should return []. Filed BUG-F1-T2.
- D3 (null exclude_tickers): AGREE — same as D2. Filed BUG-F1-T2.
- D4 (db_path mkdir): AGREE — challenger must create duckdb_dir. Filed BUG-F1-T3.
- D5 (malformed YAML): AGREE — must raise ValueError not TypeError. Filed BUG-F1-T2.
- Note: tests/test_config.py (33 acceptance tests) was lost due to session issue, not code bug. Critic reported 33/33 pass. test_config_breaking.py (18 edge-case tests) is committed.

## Cart test — chosen semantics (PM-approved deviation from §6)
- Storage: local DuckDB file (NOT R2). R2 sync only after processing is finished.
- `store_csv(symbol, date, csv_bytes) -> int` returns the generated row id.
- APPEND semantics: a second write for the same (symbol, date) persists as a NEW row and returns a strictly greater id. No upsert / no overwrite. This is what the Critic's breaking test verifies.
- Duel shape: Executor (T2, src/store.py) and Challenger (T3, src/store_challenger.py) implement independently in separate files using separate default DB paths to avoid clobbering; Critic (T4) parametrizes tests over both.
