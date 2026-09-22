# Project Rules — stock_ingestion (Multi-Agent Blackboard Protocol)

## Blackboard protocol (MANDATORY for all agents)

1. MANDATORY FIRST STEP: Read /devin_blackboard/STATE.md and /devin_blackboard/EVENTS.log.
2. MANDATORY LAST STEP: Append your action to /devin_blackboard/EVENTS.log and update /devin_blackboard/STATE.md.
3. You may ONLY edit files listed in your task's output_files (plus the blackboard files themselves).
4. Do not chat with other agents. Communicate only via file commits and EVENTS.log.
5. Never commit secrets. R2 credentials come from GitHub repo secrets, not the repo.
6. Do not redefine requirements without PM (Orchestrator) approval. If a task seems wrong, file a blocker in STATE.md and stop.

## Orchestrator / PM role (main Devin session)

The main Devin CLI session is the Orchestrator (PM). It is NOT a coding agent. Its responsibilities:
- Spawns agents B/C/D as **local subagents** via `run_subagent` (profile `subagent_general` — write-capable; `subagent_explore` is read-only and only for watchers) and coordinates the run loop. Local subagents share this workspace — no Devin Cloud session, no org quota needed. Devin Cloud MCP sessions (`devin_session_create`) are an OPTIONAL alternative when org quota is available.
- Reviews implementation and QA evidence from all agents before marking a feature complete.
- Resolves disagreements between agents (e.g. B vs C conflict) and determines next action.
- Is the ONLY role that may approve requirement changes.

## Role boundaries and escalation (MANDATORY)

The Orchestrator is a coordination and blackboard-management role. It MUST NOT directly edit, modify, or generate code in `src/` or `tests/` files, or execute implementation steps on behalf of an agent.

### What the Orchestrator MAY edit

- `devin_blackboard/` files: `STATE.md`, `TASKS.json`, `EVENTS.log`, and lock files.
- Documentation files such as `AGENTS.md` and `README.md`.
- Task-management scripts such as `scripts/poll_tasks.py` and `scripts/add_cycle*_tasks.py`.

### What the Orchestrator MUST NOT edit

- Source code under `src/`.
- Test files under `tests/`.
- Workflow implementation details that belong to Agent B (executor), Agent C (challenger), or Agent D (critic).

### Agent spawn failures

If any required subagent (B, C, or D) cannot be spawned — whether local `run_subagent` fails or the optional Devin Cloud `devin_session_create` path errors (quota, billing, or any other error) — the Orchestrator MUST:

1. STOP the current task.
2. Record the blocker in `devin_blackboard/EVENTS.log` and `STATE.md`.
3. Ask the user for direction. Do NOT implement the task in the orchestrator session.

The only exception is an explicit user override containing the exact phrase **"orchestrator may code"**. Without that override, the Orchestrator must not proceed with implementation.

### Explicit override protocol

If the user wants the Orchestrator to implement directly, the user must say one of the following:

- "orchestrator may code"
- "orchestrator may implement"
- "main devin may code"
- "override role boundaries"

Without one of these exact phrases, the Orchestrator must continue coordinating through the subagent workflow.

## Completion decision rule (MANDATORY)

Never consider a feature complete merely because:
1. The code runs.
2. Unit tests pass.
3. Output columns are populated.
4. The coding agent reports success.

Completion requires evidence against the explicit acceptance_criteria defined in TASKS.json. The Critic (Agent D) must verify each criterion with concrete evidence (test output, query results, diffs) before a task may move to status=done.

## Completion notification (MANDATORY for Orchestrator)

When all tasks in a request/cycle are `done` (or a plan is ready for user review), the Orchestrator MUST notify the user via Windows toast:

    powershell -NoProfile -File scripts/notify_done.ps1 -Message "<what finished>"

`scripts/notify_done.ps1` uses the registered Windows PowerShell AUMID — a plain `"PowerShell"` AppID is silently dropped on Windows 11 (no error, no popup). Do not hand-roll the toast inline; use the script.

## Project context

- Goal: Centralize stock-data ingestion and provide program interfaces.
- Storage backend: Cloudflare R2 (S3-compatible). Secrets: R2_ACCESS_KEY_ID, R2_ENDPOINT_URL, R2_SECRET_ACCESS_KEY (GitHub repo secrets).
- Multi-agent workflow: see docs/workflow_idea/procedure_multi_agents01.md
- Agent roles: orchestrator (A, main session), executor (B), challenger (C), critic (D) — spawned as local subagents via `run_subagent` with `subagent_general` (same model as orchestrator); `subagent_explore` (cheap, read-only) for watchers. Role separation comes from the prompt files in docs/workflow_idea/.
- Task lifecycle: `python scripts/poll_tasks.py --claim <id> <role>` → edit output_files only → `--verify <id>` → `--done <id> <role> --evidence "..."` (or `--blocked` + STATE.md blocker) → append EVENTS.log.
- Blackboard: /devin_blackboard/STATE.md, TASKS.json, EVENTS.log

## Project decisions (do NOT change without PM approval)

- **Data storage:** DuckDB (local file). Only sync to R2 when processing is finished. Do not write to R2 during intermediate steps.
- **Performance hierarchy:** SQL > vectorized computation > loop iteration.
  - The Coding Subagent (executor/challenger) MUST use SQL/DuckDB for data transformation where feasible.
  - Then Pandas/NumPy/Polars vectorized operations.
  - Only use explicit Python loops when neither SQL nor vectorized approaches are practical.
  - Any loop MUST be justified with a `# justified: <reason>` comment on the for-line or within 6 lines above it.
  - **Enforcement (automated):**
    - `scripts/check_loops.py` — AST-based checker that scans `src/*.py` for unjustified `for` loops. Run manually: `python scripts/check_loops.py --src-dir src`
    - `.git/hooks/pre-commit` — Git pre-commit hook that runs the checker and **blocks** commits with unjustified loops.
    - **Critic mandatory checklist** — the Critic (Agent D) MUST run `python scripts/check_loops.py --src-dir src` and verify exit code 0 before marking any task as `done`. This is added to every task's acceptance criteria.
  - Exempt patterns (not flagged): `range()`, `enumerate()`, `zip()`, `.items()`, `.values()`, `.keys()`, constant list/tuple literals.
  - To justify a loop: add `# justified: <reason>` comment on the for-line or within 6 lines above it.

## Coding conventions

- Python 3.10+
- Tests in /tests/, source in /src/
- Run tests with: python -m pytest tests/
