# Project State — Canonical Source
Last Updated: 2026-09-07T00:15:00Z
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
- /tests/          (critic-written tests)
  - __init__.py
  - test_store_csv.py
  - test_config.py  (critic tests, parametrized over src.config + src.config_challenger — F1-T4)

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

## Active Blockers
- None

## Cart test — chosen semantics (PM-approved deviation from §6)
- Storage: local DuckDB file (NOT R2). R2 sync only after processing is finished.
- `store_csv(symbol, date, csv_bytes) -> int` returns the generated row id.
- APPEND semantics: a second write for the same (symbol, date) persists as a NEW row and returns a strictly greater id. No upsert / no overwrite. This is what the Critic's breaking test verifies.
- Duel shape: Executor (T2, src/store.py) and Challenger (T3, src/store_challenger.py) implement independently in separate files using separate default DB paths to avoid clobbering; Critic (T4) parametrizes tests over both.
