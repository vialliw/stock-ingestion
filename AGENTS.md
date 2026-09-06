# Project Rules — stock_ingestion (Multi-Agent Blackboard Protocol)

## Blackboard protocol (MANDATORY for all agents)

1. MANDATORY FIRST STEP: Read /devin_blackboard/STATE.md and /devin_blackboard/EVENTS.log.
2. MANDATORY LAST STEP: Append your action to /devin_blackboard/EVENTS.log and update /devin_blackboard/STATE.md.
3. You may ONLY edit files listed in your task's output_files (plus the blackboard files themselves).
4. Do not chat with other agents. Communicate only via file commits and EVENTS.log.
5. Never commit secrets. R2 credentials come from GitHub repo secrets, not the repo.
6. Do not redefine requirements without PM (Orchestrator) approval. If a task seems wrong, file a blocker in STATE.md and stop.

## Orchestrator / PM role (main Devin session)

The main Devin session is the Orchestrator (PM). It is NOT a coding agent. Its responsibilities:
- Spawns subagents (planner, executor, challenger, critic) and coordinates the run loop.
- Reviews implementation and QA evidence from all agents before marking a feature complete.
- Resolves disagreements between agents (e.g. B vs C conflict) and determines next action.
- Is the ONLY role that may approve requirement changes.

## Completion decision rule (MANDATORY)

Never consider a feature complete merely because:
1. The code runs.
2. Unit tests pass.
3. Output columns are populated.
4. The coding agent reports success.

Completion requires evidence against the explicit acceptance_criteria defined in TASKS.json. The Critic (Agent D) must verify each criterion with concrete evidence (test output, query results, diffs) before a task may move to status=done.

## Project context

- Goal: Centralize stock-data ingestion and provide program interfaces.
- Storage backend: Cloudflare R2 (S3-compatible). Secrets: R2_ACCESS_KEY_ID, R2_ENDPOINT_URL, R2_SECRET_ACCESS_KEY (GitHub repo secrets).
- Multi-agent workflow: see docs/workflow_idea/procedure_multi_agents01.md
- Agent roles: planner (glm), executor (swe), challenger (glm), critic (swe)
- Blackboard: /devin_blackboard/STATE.md, TASKS.json, EVENTS.log

## Project decisions (do NOT change without PM approval)

- **Data storage:** DuckDB (local file). Only sync to R2 when processing is finished. Do not write to R2 during intermediate steps.
- **Performance hierarchy:** SQL > vectorized computation > loop iteration.
  - The Coding Subagent (executor/challenger) MUST use SQL/DuckDB for data transformation where feasible.
  - Then Pandas/NumPy/Polars vectorized operations.
  - Only use explicit Python loops when neither SQL nor vectorized approaches are practical.
  - Any loop MUST be justified in the task report (EVENTS.log entry or commit message).

## Coding conventions

- Python 3.10+
- Tests in /tests/, source in /src/
- Run tests with: python -m pytest tests/
