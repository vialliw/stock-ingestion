# Multi-Agent Workflow — Comprehensive Documentation

> **Project:** stock_ingestion — Centralize stock-data ingestion and expose program interfaces.
> **Repository:** `vialliw/stock-ingestion`
> **Local path:** `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`
> **Status:** Phase 1 complete. Phase 2 in progress.
> **Document version:** 4.0 (2026-09-12) — local `run_subagent` spawning is the DEFAULT; Devin Cloud MCP sessions are an optional alternative

---

## Table of Contents

1. [Overview and Core Ideas](#1-overview-and-core-ideas)
2. [Prerequisites](#2-prerequisites)
3. [The Blackboard (Shared State)](#3-the-blackboard-shared-state)
4. [Agent Roles and Session Architecture](#4-agent-roles-and-session-architecture)
5. [The Orchestration Loop](#5-the-orchestration-loop)
6. [Agent Execution Protocol (B/C/D)](#6-agent-execution-protocol-bcd)
7. [Verification and Completion Rules](#7-verification-and-completion-rules)
8. [Lock File Protocol](#8-lock-file-protocol)
9. [Dependency Rules](#9-dependency-rules)
10. [Communication Rules](#10-communication-rules)
11. [Context Slicing (Cost Control)](#11-context-slicing-cost-control)
12. [Phase 2 Cycle Plan](#12-phase-2-cycle-plan)
13. [Problems Encountered and Resolutions](#13-problems-encountered-and-resolutions)
14. [Lessons Learned](#14-lessons-learned)
15. [Open Questions for Third-Party Review](#15-open-questions-for-third-party-review)
16. [File Inventory](#16-file-inventory)
17. [Appendix: AGENTS.md (Project Rules)](#17-appendix-agentsmd-project-rules)
18. [Appendix: polling_workflow_protocol.md](#18-appendix-polling_workflow_protocolmd)
19. [Appendix: agent_a_prompt.md (Orchestrator)](#19-appendix-agent_a_promptmd-orchestrator)
20. [Appendix: agent_b_prompt.md (Executor)](#20-appendix-agent_b_promptmd-executor)
21. [Appendix: agent_c_prompt.md (Challenger)](#21-appendix-agent_c_promptmd-challenger)
22. [Appendix: agent_d_prompt.md (Critic)](#22-appendix-agent_d_promptmd-critic)

---

## 1. Overview and Core Ideas

### Why multi-agent?

A single agent writing and reviewing its own code has a blind spot: it cannot effectively challenge its own assumptions. The multi-agent approach introduces an **attack/defense loop** where independent agents implement, challenge, and verify work separately.

### Core ideas

| Problem | Solution |
|---|---|
| No shared ground truth — Agent A's work is invisible to Agent B | **Blackboard**: `STATE.md` + `TASKS.json` + `EVENTS.log` committed to git |
| Heterogeneous capabilities conflict | **Role specialization**: Orchestrator-A / Executor-B / Challenger-C / Critic-D |
| Communication protocol drift | **Strict JSON contract** for tasks; no free-form agent-to-agent chat |
| Need the attack/defense loop | **Generator → Challenger → Critic** iteration per cycle |
| Cost & context explosion | **Context slicing**: each agent sees only its task + relevant files + recent events |
| Need it free | **Local `run_subagent` spawning**: no Devin Cloud session, no org quota, no cost beyond the local session's model usage |

### Agent spawning and model selection

Agents B/C/D are spawned by the orchestrator as **local subagents** via the Devin CLI's built-in `run_subagent` tool:

| Profile | Access | Model | Used by |
|---|---|---|---|
| `subagent_general` | Read + write | Same model as the orchestrator session | Executor (B), Challenger (C), Critic (D) |
| `subagent_explore` | Read-only | Cheap (SWE-1.6) | Optional watcher / read-only probes |

Role differentiation comes from **system prompts** (the agent prompt files), not the model. The same model can serve different roles because the prompt defines the behavior. All three role agents need `subagent_general` — each must claim its task (`--claim` writes the lock file, TASKS.json, and EVENTS.log) and append evidence to the blackboard.

Devin Cloud MCP sessions (`devin_session_create`) remain an **optional alternative** for when org ACU quota is available — see §4.4 and `devin_platform_setup.md`.

---

## 2. Prerequisites

1. Repo exists on GitHub: `vialliw/stock-ingestion`
2. Local clone at `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`
3. R2 secrets stored as **GitHub repo secrets** (never committed):
   - `R2_ACCESS_KEY_ID`
   - `R2_ENDPOINT_URL`
   - `R2_SECRET_ACCESS_KEY`
4. Devin CLI authenticated: `devin auth login`
5. One Devin CLI session = the Orchestrator (Agent A). Agents B/C/D are spawned on demand as local subagents via `run_subagent` — no separate sessions, no Devin Cloud quota needed.
   - (Optional cloud alternative) Devin MCP server configured in `.devin/config.local.json` for `devin_session_create` — only if org ACU quota is available. See `devin_platform_setup.md`.

---

## 3. The Blackboard (Shared State)

Every agent reads and writes these files. They are committed to git — git history is the audit trail.

### 3.1 `devin_blackboard/STATE.md`

Canonical source of truth: project goal, file tree, decisions, what's done, active blockers.

### 3.2 `devin_blackboard/TASKS.json`

DAG of tasks. Schema (every task MUST have all fields):

```json
{
  "id": "F7-T2",
  "title": "Executor: health_check Flight action + ingestion_runs audit table",
  "role": "executor",
  "model": "swe",
  "status": "todo",
  "depends_on": ["F7-T1c"],
  "input_files": ["/src/flight_server.py", "/src/db.py"],
  "output_files": ["/src/flight_server.py", "/src/db.py"],
  "acceptance_criteria": "(1) ... (2) ... (3) ...",
  "assigned_to": "B"
}
```

- **Status values:** `todo` → `in_progress` → `done` | `blocked` → (back to `todo`)
- **assigned_to:** `"A"` (orchestrator), `"B"` (executor), `"C"` (challenger), `"D"` (critic), or `null`
- **depends_on:** list of task IDs that must be `done` before this task can be claimed

### 3.3 `devin_blackboard/EVENTS.log`

Append-only. One line per agent action:

```
[2026-09-07T20:15:00Z] [EXECUTOR] completed F7-T0 -> src/db.py, src/ingestion.py. 122/122 tests pass.
```

### 3.4 `devin_blackboard/locks/<task_id>.lock`

Per-task lock files preventing duplicate execution. See [§8](#8-lock-file-protocol).

### 3.5 `AGENTS.md` (repo root)

Project rules that every Devin session auto-loads. Contains the blackboard protocol rules, project context, performance hierarchy (SQL > vectorized > loop), and coding conventions.

---

## 4. Agent Roles and Subagent Architecture

### 4.1 Subagent topology (DEFAULT — local `run_subagent`)

One main Devin CLI session is the Orchestrator (Agent A). Agents B/C/D are spawned **on demand** as local subagents via the built-in `run_subagent` tool. Each `run_subagent` call is synchronous: the subagent executes its Poll-Execute-Done pass and returns a report. Agents run sequentially per cycle: B → C → D.

| Agent | Role | Spawned via | Prompt file | Responsibility |
|---|---|---|---|---|
| A | Orchestrator (PM) | Main Devin CLI session | `docs/workflow_idea/agent_a_prompt.md` | Plans cycles, writes tasks, spawns subagents, adjudicates, approves completion |
| B | Executor | `run_subagent` + `subagent_general` | `docs/workflow_idea/agent_b_prompt.md` | Implements features, fixes bugs, writes code in `src/` |
| C | Challenger | `run_subagent` + `subagent_general` | `docs/workflow_idea/agent_c_prompt.md` | Independent review, edge-case analysis, false-positive/negative detection |
| D | Critic | `run_subagent` + `subagent_general` | `docs/workflow_idea/agent_d_prompt.md` | Tests, verifies acceptance criteria, regression checks, loop checker enforcement |

Benefits of the local-subagent default:

- **No Devin Cloud needed** — no `devin_session_create`, no service-user API key.
- **No org quota** — cloud `out_of_quota` errors cannot block the workflow.
- **No extra cost** — subagent work is billed as the local session's own model usage.
- **Shared workspace** — subagents edit files and commit in the same working directory; `poll_tasks.py` lifecycle commits are immediately visible to the orchestrator.

### 4.2 Why local subagents (history)

The workflow went through three spawning mechanisms:

1. **Internal subagents (abandoned):** early orchestrator-spawned subagents locked to the orchestrator's model and shared its context — model mismatch and no context slicing.
2. **Four separate sessions / Devin Cloud MCP sessions (superseded):** each agent in its own session solved model/context separation, but required manual user relay (tabs) or org ACU quota (MCP `devin_session_create`). Cloud quota exhaustion (`out_of_quota`) blocked all B/C/D spawns — see §13.8.
3. **Local `run_subagent` (current default):** `subagent_general` is write-capable and `subagent_explore` is a cheap read-only profile. Role separation now comes from the prompt files rather than per-session model selection — the model-lock-in tradeoff was accepted because quota-free local execution outweighs it. Context slicing is preserved: each subagent's prompt tells it to read only its task, input_files, and the last lines of EVENTS.log.

### 4.3 Optional: read-only watcher

In the normal B → C → D flow no watcher is needed — each `run_subagent` call returns the subagent's report directly. If the orchestrator must wait on external state (e.g. a crashed agent left a task `in_progress`, or a manual user step is pending), it can spawn a `subagent_explore` read-only watcher that polls TASKS.json and reports status changes. `subagent_explore` cannot write, so it can never claim a task or edit the blackboard.

### 4.4 Optional: Devin Cloud MCP sessions (alternative)

When org ACU quota is available, Agents B/C/D MAY instead be spawned as remote Devin Cloud sessions via `devin_session_create` (playbook IDs in `devin_platform_setup.md`), waited on via `devin_session_gather`, and messaged via `devin_session_interact`. The blackboard protocol and `poll_tasks.py` subcommands are identical — only the spawn/wait/message mechanism differs. If `devin_session_create` fails with `out_of_quota`, use the local `run_subagent` default.

---

## 5. The Orchestration Loop

### 5.1 Flow diagram

```
Orchestrator (main session)
        │
        ▼
┌────────────────┐
│ run_subagent   │     profile=subagent_general
│ (Agent B)      │     prompt = agent_b_prompt.md + "poll, claim READY task,
└───────┬────────┘      execute, --verify, --done with evidence"
        │ synchronous return: B's report + blackboard updated
        ▼
┌────────────────┐
│ Orchestrator   │     reads TASKS.json: done → next role; blocked → Step 4
│ checks state   │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ run_subagent   │     profile=subagent_general
│ (Agent C)      │     (challenger reviews B's work)
└───────┬────────┘
        │ synchronous return
        ▼
┌────────────────┐
│ run_subagent   │     profile=subagent_general
│ (Agent D)      │     (critic verifies acceptance criteria)
└───────┬────────┘
        │ synchronous return
        ▼
┌────────────────┐
│ Orchestrator   │     writes next cycle tasks to TASKS.json
│ writes next    │     spawns Agent B again for the new cycle
│ cycle          │
└────────────────┘
```

### 5.2 Orchestrator steps

**Step 1 — Initial state check:**
```powershell
git pull
python scripts/poll_tasks.py B
python scripts/poll_tasks.py C
python scripts/poll_tasks.py D
```

**Step 2 — Write next cycle tasks** (when current cycle is complete):
- Read `docs/dev_plan/phase2_dev_plan.md` for next cycle spec
- Write 3 task objects to TASKS.json (executor, challenger, critic)
- Append to EVENTS.log, update STATE.md, commit
- Spawn Agent B via `run_subagent` (subagent_general)

**Step 3 — Spawn the next role subagent:**
- Call `run_subagent` with `profile="subagent_general"`; prompt = the role's prompt file body + the poll/claim/execute instruction
- The call blocks until the subagent finishes and returns its report
- Re-read TASKS.json (same workspace — no pull needed): done → spawn next role (B → C → D); blocked → Step 4
- Optional: `subagent_explore` read-only watcher if waiting on external state

**Step 4 — Handle blockers** (when any task is "blocked"):
- Read STATE.md "Active Blockers" + EVENTS.log evidence
- Investigate, adjudicate, document decision in EVENTS.log
- Set blocked task back to "todo" or create new task
- Re-spawn the appropriate role subagent

---

## 6. Agent Execution Protocol (B/C/D)

### 6.1 Core principle: poll-once-then-execute-or-stop

Agents are spawned as local subagents by the orchestrator. They do NOT run background polling. They do NOT sleep in loops. They:

1. **Poll once** when spawned (or after finishing a task)
2. If a task is READY: **claim → execute → verify → mark done → poll once more**
3. If no task is READY: **STOP and report** — the report is returned to the orchestrator

### 6.2 Detailed protocol

```
STEP 1: Sync and poll
        git pull
        python scripts/poll_tasks.py <B|C|D>

STEP 2: Read output:
        - "READY:" = claim and execute now
        - "WAITING:" = dependencies not done, stop and report
        - "No tasks" = nothing to do, stop and report

STEP 3: If READY task exists:
        a. CLAIM (atomic lock + status=in_progress + EVENTS.log + commit, one command):
           python scripts/poll_tasks.py --claim <task_id> <role>
        b. Read input_files, AGENTS.md, STATE.md, EVENTS.log (last 20 lines)
        c. EXECUTE the task (write to output_files ONLY)
        d. VERIFY all structured acceptance criteria:
           python scripts/poll_tasks.py --verify <task_id>
        e. If pass:
           python scripts/poll_tasks.py --done <task_id> <role> --evidence "<summary + evidence>"
           - Sets status="done", appends evidence to EVENTS.log, deletes lock, commits
           - Update STATE.md
           - Report to the orchestrator in your final message: "<task_id> complete."
           - Go back to STEP 1 (poll once more)
        f. If fail:
           python scripts/poll_tasks.py --blocked <task_id> <role> --reason "<failure>"
           - Sets status="blocked", appends failure to EVENTS.log, deletes lock, commits
           - Update STATE.md "Active Blockers"
           - Report to the orchestrator: "<task_id> BLOCKED: <reason>"
           - STOP — your report is returned to the orchestrator

STEP 4: If no READY task:
        - Report to the orchestrator: "No tasks ready for <role>."
        - STOP — do NOT poll again; the orchestrator re-spawns you when needed
```

### 6.3 Canonical polling script

All agents use `scripts/poll_tasks.py` — never self-written polling scripts:

```powershell
python scripts/poll_tasks.py B    # Agent B
python scripts/poll_tasks.py C    # Agent C
python scripts/poll_tasks.py D    # Agent D
```

Lifecycle subcommands (mandatory — never hand-edit TASKS.json or lock files):

```powershell
python scripts/poll_tasks.py --claim <task_id> <role>                          # atomic claim
python scripts/poll_tasks.py --verify <task_id>                                # run all criteria, JSON evidence map
python scripts/poll_tasks.py --done <task_id> <role> --evidence "<text>"       # verify + done + lock cleanup + commit
python scripts/poll_tasks.py --blocked <task_id> <role> --reason "<text>"      # blocked + lock cleanup + commit
python scripts/poll_tasks.py --clean-stale                                     # recover stale locks (>10 min)
```

Output format:
```
READY: F7-T2 assigned_to=B deps=['F7-T1c'] deps_ok=True
  title: Executor: health_check Flight action + ...
  input_files: [...]
  output_files: [...]
  acceptance_criteria: ...

WAITING: F7-T2b assigned_to=C deps=['F7-T2'] deps_ok=False

No tasks assigned_to=D with status=todo.
```

---

## 7. Verification and Completion Rules

### 7.1 Mandatory checks (ALL agents before marking done)

```powershell
python scripts/check_loops.py --src-dir src          # must exit 0
python scripts/check_loops.py --src-dir src --strict  # must exit 0
python -m pytest tests/ -q                            # must exit 0
```

### 7.2 Completion decision rule (from AGENTS.md)

A feature is **NOT** complete merely because:
1. The code runs.
2. Unit tests pass.
3. Output columns are populated.
4. The coding agent reports success.

Completion requires **evidence against the explicit `acceptance_criteria`** defined in TASKS.json. The Critic (Agent D) must verify each criterion with concrete evidence (test output, query results, diffs) before a task may move to `status=done`.

### 7.3 Performance hierarchy (from AGENTS.md)

```
SQL > vectorized computation > loop iteration
```

- The Executor MUST use SQL/DuckDB for data transformation where feasible.
- Then Pandas/NumPy/Polars vectorized operations.
- Only use explicit Python loops when neither SQL nor vectorization is practical.
- Any loop MUST be justified with a `# justified: <reason>` comment.
- Enforcement: `scripts/check_loops.py` + pre-commit hook + Critic checklist.

---

## 8. Lock File Protocol

Lock files prevent two sessions from working on the same task simultaneously.

- **Location:** `devin_blackboard/locks/<task_id>.lock`
- **Contents:** `{"agent": "B", "claimed_at": "2026-09-07T20:00:00Z"}`
- **Created:** before setting `status="in_progress"`
- **Deleted:** after setting `status="done"` (or `"blocked"`)
- **Stale lock:** if lock file exists AND task status is NOT `"in_progress"` for >10 minutes, any agent can delete the stale lock and claim the task

---

## 9. Dependency Rules

- A task with `depends_on: ["F7-T0"]` will NOT be picked up until F7-T0 has `status: "done"`
- The polling script checks all dependencies before reporting a task as READY
- If a dependency is `"blocked"`, the dependent task stays `"todo"` (not picked up)
- Dependency chain per cycle: `F7-T<N>` (executor) → `F7-T<N>b` (challenger) → `F7-T<N>c` (critic)
- Cross-cycle: each cycle's executor depends on the previous cycle's critic task

---

## 10. Communication Rules

1. **No agent-to-agent chat.** All communication is through blackboard files.
2. **EVENTS.log is append-only.** Never edit or delete previous entries.
3. **STATE.md is updateable.** Each agent updates their own section.
4. **TASKS.json is updateable.** Agents only update `status` for their own tasks.
5. **Git commits are the durability layer.** Every blackboard change is committed immediately.
6. **Never commit secrets.** R2 credentials come from environment variables / GitHub secrets.
7. **Pull before commit.** Always `git pull` before committing to avoid conflicts.
8. **No background polling by agents B/C/D.** They poll once when spawned, then execute or stop.
9. **No foreground polling by orchestrator.** `run_subagent` calls are synchronous — the orchestrator waits on the returned report, not a polling loop. Optional read-only watchers use `subagent_explore`.
10. **Orchestrator is the ONLY role** that may approve requirement changes.

---

## 11. Context Slicing (Cost Control)

No agent ever sees the whole project. Each agent session receives ONLY:
- Its role prompt (from `docs/workflow_idea/agent_<role>_prompt.md`)
- The specific task object from `TASKS.json` (found via `poll_tasks.py`)
- The files listed in that task's `input_files` / `output_files`
- `AGENTS.md` (project rules — auto-loaded)
- Last 20 lines of `EVENTS.log` (not full history)
- `STATE.md` (current state only)

Agents do not share conversation history with each other — the blackboard files are their only shared state.

---

## 12. Phase 2 Cycle Plan

Source: `docs/dev_plan/phase2_dev_plan.md`

| Cycle | Feature | Status | Tasks | Notes |
|---|---|---|---|---|
| 0 | Table rename + hardening | DONE | F7-T0, F7-T0e | `daily_quotes` → `daily_close`, SQL CAST, batch splits, `--strict` checker |
| 1 | JSON logging + log file path | DONE | F7-T1, F7-T1b, F7-T1c | `python-json-logger`, FileHandler, configurable format |
| 2 | Health check + audit table | READY | F7-T2, F7-T2b, F7-T2c | `ingestion_runs` table, `health_check` + `get_run_history` Flight actions |
| 3 | Query timeout | TODO | F7-T3, F7-T3b, F7-T3c | 120s default, no row limit, `threading.Timer` |
| 4 | R2 sync (DuckDB direct) | TODO | F7-T4, F7-T4b, F7-T4c | `boto3`, auto after daily_load, manual Flight action |
| 5 | APScheduler (separate process) | TODO | F7-T5, F7-T5b, F7-T5c | `src/scheduler_main.py`, SIGINT/SIGTERM, market calendar |
| 6 | do_put / IngestBatch | TODO | F7-T6, F7-T6b, F7-T6c | Arrow batch ingest, schema validation, exclude filter |
| 7 | Integration + hardening | TODO | F7-T7, F7-T7b | End-to-end tests, full regression |

### Task assignment pattern per cycle

```
F7-T<N>     (executor)    assigned_to="B"  depends_on=[<previous cycle critic>]
F7-T<N>b    (challenger)  assigned_to="C"  depends_on=["F7-T<N>"]
F7-T<N>c    (critic)      assigned_to="D"  depends_on=["F7-T<N>", "F7-T<N>b"]
```

---

## 13. Problems Encountered and Resolutions

### 13.1 Orchestrator internal subagent used wrong model

**Problem:** The orchestrator spawned an internal subagent to implement F7-T0. The subagent tool does not expose model selection, so the subagent used GLM-5.2 High (the orchestrator's model) instead of SWE-1.7 Medium (the intended executor model).

**Resolution:** Switched to four independent Devin sessions, each with its own model. The orchestrator no longer spawns coding subagents — it only spawns a read-only background watcher.

**Impact:** F7-T0 was completed by GLM-5.2 High instead of SWE-1.7 Medium. The work was verified correct (122/122 tests, loop checker clean, real DB migrated). Going forward, all executor work is done by Agent B's session with SWE-1.7 Medium.

### 13.2 Agent B's temp polling script was hardcoded for wrong role

**Problem:** Agent B created a temp polling script at `C:\Users\user\AppData\Local\Temp\poll_tasks.py` that was hardcoded for `assigned_to == 'C'` (Challenger), not `'B'` (Executor). Agent B reported "no tasks ready" because it was checking Agent C's tasks.

**Root cause:** The agent wrote its own polling script instead of using the canonical one. The hardcoded role meant it never saw its own tasks.

**Resolution:**
1. Created a parameterized canonical script: `scripts/poll_tasks.py <role>`
2. Updated all agent prompts to mandate: "NEVER write your own polling script — always use `python scripts/poll_tasks.py <role>`"
3. Added explicit rule in agent prompts and protocol documentation

### 13.3 Orchestrator foreground polling blocked user messages

**Problem:** The orchestrator ran `Start-Sleep 60` in the foreground to poll every 60 seconds. While sleeping, the user's messages queued up and could not be processed until the sleep completed. This made the orchestrator appear unresponsive.

**Root cause:** Foreground `Start-Sleep` blocks the entire session. There is no way to interrupt it cleanly from the user's side.

**Resolution:** The orchestrator now spawns a **background watcher subagent** that polls every 60 seconds. The watcher returns immediately when it detects a task status change. The orchestrator's main session stays free to respond to user messages at all times.

### 13.4 Agent B's "background polling" stalled without claiming tasks

**Problem:** Agent B reported "polling via scripts/poll_tasks.py B every 30s in the background (shell 47ffce)." However, after 4+ minutes, F7-T2 remained unclaimed — no lock file, no status change, no EVENTS.log update.

**Root cause:** A background shell can only execute commands and print output. It cannot make decisions, claim tasks, edit files, or commit. The agent itself needs to be in the session to interpret the polling output and take action.

**Resolution:**
1. Updated agent prompts to explicitly forbid background polling shells
2. Agents now poll **once** when instructed, then either execute (if READY) or stop (if not)
3. After completing a task, the agent polls once more for the next task. If none ready, it stops and reports.

### 13.5 No notification when agents finish tasks

**Problem:** Agents complete tasks and update TASKS.json/STATE.md/EVENTS.log, but the orchestrator only discovers this by polling. If the orchestrator isn't polling (or is blocked sleeping), it doesn't know the task is done.

**Root cause:** The blackboard protocol has no push notification mechanism — it's pull-only.

**Resolution:** The background watcher subagent detects state changes and notifies the orchestrator immediately. The orchestrator no longer needs to poll repeatedly — it waits for the watcher's notification.

### 13.6 Timestamp anomaly in EVENTS.log

**Problem:** During the initial cart test (Phase 0), T2's timestamp in EVENTS.log was out of order relative to T1. This is cosmetic — git log provides the reliable ordering.

**Resolution:** Use git log for audit ordering. EVENTS.log timestamps are informational, not authoritative.

### 13.7 Weak duel in cart test

**Problem:** During the initial cart test, the executor and challenger produced near-identical implementations. The challenger's ≥50% performance improvement rule may have incentivized convergence rather than divergence.

**Resolution:** Real divergence is expected on more complex tasks. The challenger role has been refined to focus on edge cases, false positives/negatives, and design gaps rather than requiring a competing implementation.

### 13.8 Devin Cloud quota exhaustion blocked all agent spawns

**Problem:** With the MCP-based approach (Tier 1), the orchestrator spawned Agents B/C/D as remote Devin Cloud sessions via `devin_session_create`. During the F7-T9b cycle the org returned HTTP 403 `out_of_quota` — no child sessions could be created, and the entire workflow stalled until the user granted an explicit "orchestrator may code" override (F7-T10 was implemented by the orchestrator under that override).

**Root cause:** `devin_session_create` consumes org-level ACU quota. When the quota is exhausted, the cloud spawning path is a hard dependency failure — there is no graceful degradation.

**Resolution:** Switched the DEFAULT spawning mechanism to the Devin CLI's built-in `run_subagent` tool. Local subagents (`subagent_general` write-capable, `subagent_explore` read-only) run inside the orchestrator's session: no cloud session, no org quota, no API key, no cost beyond local model usage. The role prompt files were rewritten to be executed verbatim by a local subagent (claim via `poll_tasks.py --claim`, edit `output_files` only, `--verify`, `--done` with evidence, append to EVENTS.log). `devin_session_create` remains documented as an optional cloud alternative in `devin_platform_setup.md` for when quota is available.

---

## 14. Lessons Learned

### 14.1 Local subagents are the pragmatic default

Three spawning mechanisms were tried: internal subagents (model lock-in, shared context — abandoned), four separate sessions / Devin Cloud MCP sessions (correct model+context separation but needed manual relay or org quota), and local `run_subagent` (current default). Local subagents accept the model-lock-in tradeoff — `subagent_general` uses the orchestrator's model — because role separation comes from the prompt files, context slicing is preserved by instructing each subagent to read only its task inputs, and quota-free execution eliminates the `out_of_quota` hard failure that stalled the workflow. Cloud sessions remain a documented alternative when quota returns.

### 14.2 Background shells cannot make decisions

A background shell (`exec` with `timeout: 0`) can run commands and produce output, but it cannot interpret that output, make decisions, edit files, or commit. Any workflow that requires the agent to "poll in the background and act on results" must have the agent in the session, not a background shell.

### 14.3 Foreground sleep blocks user interaction

Any `Start-Sleep` or long-running foreground command in the orchestrator session blocks user messages. The orchestrator must always keep its main session free. Background subagents are the solution.

### 14.4 Canonical scripts prevent configuration drift

When agents write their own polling scripts, they introduce bugs (wrong role, wrong path, wrong logic). A single canonical script with role as a CLI argument eliminates this class of errors.

### 14.5 Explicit "stop and report" prevents infinite loops

Without an explicit "stop and report" instruction, agents may loop indefinitely polling for tasks that don't exist yet. The protocol now explicitly says: if no READY task, STOP and wait for user instruction.

### 14.6 The orchestrator is the transport layer (local subagents)

With `run_subagent`, the orchestrator spawns each role directly and receives its report synchronously — the user no longer relays "please poll" messages between sessions. Agents still cannot chat with each other directly; the blackboard remains the only shared state, and each subagent's returned report is ephemeral — durable evidence must land in EVENTS.log via `--done`/`--blocked`.

---

## 15. Open Questions for Third-Party Review

1. **User as transport layer:** ~~Currently, the user manually tells Agent B/C/D when to poll.~~ **RESOLVED (v4.0):** the orchestrator now spawns Agents B/C/D directly as local subagents via `run_subagent` — no user relay needed. Remaining question: is there value in a file-based "ping" mechanism for long-running async work (e.g., orchestrator writes to `devin_blackboard/PING_B.md`)?

2. **Background watcher reliability:** The background watcher subagent polls every 60s. Is this the right interval? Too fast wastes resources; too slow delays cycle progression. Should it be configurable?

3. **Challenger value:** In practice, the challenger (Agent C) has produced useful findings (loop checker gaps, SQL optimization suggestions) but has not blocked any task. Is the challenger role earning its cost? Should it be merged with the critic?

4. **Sequential vs parallel execution:** Currently, executor → challenger → critic is strictly sequential. Could challenger and critic run in parallel (both review the executor's work independently)? This would speed up cycles but lose the dependency chain.

5. **Lock file race condition:** Two agents could check for a lock file simultaneously, both find it absent, and both create it. The current protocol doesn't use atomic file creation. Should we use `os.open(O_CREAT | O_EXCL)` for atomic lock acquisition?

6. **Git conflict handling:** Multiple agents committing to the same repo can cause merge conflicts. The protocol says "git pull before commit" but doesn't specify conflict resolution. Should the orchestrator be the merge authority?

7. **Task granularity:** Are the current task sizes appropriate? F7-T0 was large (rename + CAST + batch splits + strict checker in one task). Should we split future tasks more finely?

8. **Acceptance criteria verification:** The critic pastes evidence to EVENTS.log, but there's no automated check that ALL criteria were verified. Should we add a checklist mechanism to TASKS.json?

9. **Model selection:** Is SWE-1.7 Medium the right model for the executor, or would High thinking level produce better code? Is GLM-5.2 High appropriate for the challenger, or would a different model provide more diverse perspective?

10. **State recovery:** If an agent crashes mid-task, the lock file remains and the task is stuck in "in_progress". The 10-minute stale-lock rule handles this, but should there be a more explicit recovery procedure?

### Devin platform features for workflow improvement

11. **Devin Playbook:** Could Devin Playbook be used to codify the agent prompts and workflow protocol as reusable, version-controlled playbooks? Would playbooks help standardize the poll-execute-done lifecycle across sessions? How do playbooks interact with the blackboard files — can a playbook reference `TASKS.json` and `poll_tasks.py` directly?

12. **Devin Schedules:** Could Devin Schedules replace the manual "user tells Agent B to poll" step? For example, could we schedule Agent B's session to automatically poll `TASKS.json` every 5 minutes during active development hours? Would scheduled sessions have the same ability to claim tasks, edit files, and commit as interactive sessions? How do schedules handle the "stop and report" protocol — does a scheduled session wait for user input or proceed autonomously?

13. **Devin MCP (Model Context Protocol):** Could MCP servers expose the blackboard files (`TASKS.json`, `STATE.md`, `EVENTS.log`) as structured resources instead of flat files? Would an MCP server that wraps the blackboard with a proper API (e.g., `get_next_task(role)`, `claim_task(task_id)`, `report_done(task_id, evidence)`) eliminate the polling script and lock file protocol entirely? Could MCP handle push notifications for state changes, replacing the background watcher subagent?

14. **Devin Session Management:** Could Devin Session Management features (session persistence, session linking, session handoff) reduce the need for the user to manually coordinate between Agent B/C/D sessions? Can sessions be linked so that when Agent B completes a task, Agent C's session is automatically activated? Can session context be preserved across polling cycles so agents don't need to re-read `AGENTS.md` and `STATE.md` every time?

15. **Devin Dynamic Workflows:** Could Devin Dynamic Workflows encode the cycle progression (executor → challenger → critic → next cycle) as a declarative workflow rather than imperative polling? Would a dynamic workflow automatically route tasks to the correct agent based on `assigned_to` without manual user nudging? Can dynamic workflows handle the dependency chain (`depends_on`) and blocker resolution automatically? How would dynamic workflows interact with the acceptance criteria verification — can they gate task completion on critic approval?

---

## 16. File Inventory

### Workflow documentation

| File | Purpose |
|---|---|
| `docs/workflow_idea/procedure_multi_agents01.md` | This document — comprehensive workflow documentation |
| `docs/workflow_idea/polling_workflow_protocol.md` | Protocol v3.0 — event-driven coordination |
| `docs/workflow_idea/agent_a_prompt.md` | Orchestrator session prompt |
| `docs/workflow_idea/agent_b_prompt.md` | Executor session prompt |
| `docs/workflow_idea/agent_c_prompt.md` | Challenger session prompt |
| `docs/workflow_idea/agent_d_prompt.md` | Critic session prompt |

### Blackboard files

| File | Purpose |
|---|---|
| `devin_blackboard/STATE.md` | Canonical project state |
| `devin_blackboard/TASKS.json` | Task DAG with status and assignment |
| `devin_blackboard/EVENTS.log` | Append-only action log |
| `devin_blackboard/locks/` | Per-task lock files |

### Tooling

| File | Purpose |
|---|---|
| `scripts/poll_tasks.py` | Canonical polling script (role as CLI argument) |
| `scripts/check_loops.py` | AST-based loop checker with `--strict` mode |
| `scripts/pre_commit_check_loops.py` | Pre-commit hook wrapper |
| `.git/hooks/pre-commit` | Git pre-commit hook enforcing loop checker |

### Project rules

| File | Purpose |
|---|---|
| `AGENTS.md` | Project rules — blackboard protocol, performance hierarchy, coding conventions |

### Development plans

| File | Purpose |
|---|---|
| `docs/dev_plan/phase1_dev_plan.md` | Phase 1 plan (complete) |
| `docs/dev_plan/phase2_dev_plan.md` | Phase 2 plan (in progress) |
| `docs/tech_spec/gemini_tech_spec.1.0.2.md` | Technical specification |

---

## 17. Appendix: AGENTS.md (Project Rules)

> Source file: `AGENTS.md` (repo root). Auto-loaded by every Devin session. Updated 2026-09-12 for local `run_subagent` spawning.

~~~markdown
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
~~~

---

## 18. Appendix: polling_workflow_protocol.md

> Source file: `docs/workflow_idea/polling_workflow_protocol.md`

~~~markdown
# Multi-Agent Workflow Protocol — Local Subagent Coordination

**Status:** ACTIVE
**Date:** 2026-09-12
**Version:** 4.0 — local `run_subagent` spawning (no Devin Cloud required)

---

## Overview

The Orchestrator (the main Devin CLI session, Agent A) coordinates Agents B/C/D through the blackboard files in `devin_blackboard/`. **Agents B/C/D are spawned as LOCAL subagents** via the Devin CLI's built-in `run_subagent` tool — no Devin Cloud session, no org quota, no cost beyond the local session's own model usage. Spawning is synchronous: each `run_subagent` call returns the subagent's report when it finishes.

## Agents

| Agent | Role | Spawned via | Responsibility |
|---|---|---|---|
| A | Orchestrator (PM) | Main Devin CLI session | Plans, writes tasks, spawns role subagents, reviews evidence, resolves blockers |
| B | Executor | `run_subagent` + `subagent_general` | Implements features, fixes bugs, writes code in src/ |
| C | Challenger | `run_subagent` + `subagent_general` | Independent review, alternative implementations, false-positive/negative analysis |
| D | Critic | `run_subagent` + `subagent_general` | Tests, verifies acceptance criteria, regression checks, loop checker enforcement |
| — | Watcher (optional) | `run_subagent` + `subagent_explore` | Read-only observation of TASKS.json state; cannot claim or write |

Agents B/C/D all need the write-capable `subagent_general` profile because each must claim its task (`--claim` writes TASKS.json, EVENTS.log, and a lock file) and append evidence to the blackboard. `subagent_explore` is read-only and cheap (SWE-1.6) — suitable only for watching.

## Blackboard Files

| File | Purpose | Who writes |
|---|---|---|
| `devin_blackboard/TASKS.json` | Task definitions with status + assignment | Orchestrator creates; Agents update status only |
| `devin_blackboard/STATE.md` | Current project state, what's done, blockers | All agents update their section |
| `devin_blackboard/EVENTS.log` | Append-only log of all actions | All agents append |
| `devin_blackboard/locks/<task_id>.lock` | Per-task lock files | Agent that claims the task |

---

## Task Lifecycle

```
todo  -->  in_progress  -->  done
                       \->  blocked
```

1. **todo**: Orchestrator writes task to TASKS.json with `status: "todo"`, `assigned_to: "B"` (or "C", "D")
2. **in_progress**: Agent sees task assigned to them with status=todo, creates lock file, sets status="in_progress"
3. **done**: Agent completes, sets status="done", appends to EVENTS.log, updates STATE.md, deletes lock file
4. **blocked**: Agent hits a blocker, sets status="blocked", files blocker in STATE.md

---

## Agent Protocol (B/C/D) — Poll-Execute-Done (single pass)

**CRITICAL:** Agents do NOT run background polling shells. A background shell can only print — it cannot claim tasks, edit files, or make decisions. The agent must be IN the session to execute.

**CRITICAL:** Agents MUST use the canonical polling script `scripts/poll_tasks.py`. Agents MUST NOT write their own polling scripts.

### When instructed to poll (by user or after finishing a task):

```
STEP 1: Sync and poll
        git pull
        python scripts/poll_tasks.py <B|C|D>

STEP 2: Read output:
        - "READY:" = claim and execute now
        - "WAITING:" = dependencies not done, stop and report
        - "No tasks" = nothing to do, stop and report

STEP 3: If READY task exists:
        a. Check lock file (skip if < 10 min old, delete if stale)
        b. Create lock file
        c. Set status = "in_progress" in TASKS.json
        d. Append to EVENTS.log: "[<ROLE>] claimed <task_id>"
        e. git commit
        f. Read input_files, AGENTS.md, STATE.md, EVENTS.log
        g. EXECUTE the task (write to output_files ONLY)
        h. VERIFY (loop checker + pytest must exit 0)
        i. If pass: set status="done", append evidence to EVENTS.log, update STATE.md, delete lock, commit
           -> Report to user: "<task_id> complete. Polling for next task."
           -> Go back to STEP 1 (poll once more for next task)
        j. If fail: set status="blocked", append failure to EVENTS.log, update STATE.md, delete lock, commit
           -> Report to user: "<task_id> BLOCKED: <reason>. Awaiting orchestrator."
           -> STOP — wait for user instruction

STEP 4: If no READY task:
        - Report to user: "No tasks ready for <role>. Waiting for orchestrator."
        - STOP — wait for user to tell you to poll again
        - Do NOT run background polling. Do NOT sleep in a loop.
```

---

## Orchestrator Protocol (Session A) — Event-Driven with Background Watcher

**CRITICAL:** Do NOT run foreground polling loops (`Start-Sleep`). They block user messages. Use a background watcher subagent instead.

### Step 1: Initial state check
```
git pull
python scripts/poll_tasks.py B
python scripts/poll_tasks.py C
python scripts/poll_tasks.py D
```
Assess:
- All cycle tasks "done" → Step 2 (write next cycle)
- Any task "blocked" → Step 4 (handle blocker)
- Tasks "todo" or "in_progress" → Step 3 (spawn watcher)

### Step 2: Write next cycle tasks
a. Read `docs/dev_plan/phase2_dev_plan.md` for next cycle spec
b. Write 3 task objects to TASKS.json (executor, challenger, critic)
c. Append to EVENTS.log, update STATE.md, commit
d. Tell user: "Cycle N complete. Cycle N+1 tasks written. Please tell Agent B to poll."
e. Go to Step 3

### Step 3: Spawn background watcher
Spawn a background subagent (`run_subagent` with `is_background=true`):

```
Watch TASKS.json for state changes.
cd to C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion.
Every 60 seconds: git pull, check TASKS.json for tasks assigned to B/C/D.
Compare current statuses to previous poll.
Return immediately when ANY task changes status (todo→in_progress, in_progress→done, in_progress→blocked).
Report: which task(s) changed, old status, new status, new EVENTS.log entries.
Do NOT edit files. Do NOT claim tasks. Only observe and report.
```

When watcher notifies:
- Task → "done": check if cycle complete → Step 2 or spawn new watcher
- Task → "blocked": → Step 4
- Task → "in_progress": note it, spawn new watcher

**Main session stays free.** Respond to user messages immediately.

### Step 4: Handle blockers
a. Read STATE.md "Active Blockers" + EVENTS.log evidence
b. Investigate, adjudicate, document decision
c. Update TASKS.json (set back to "todo" or create new task)
d. Append to EVENTS.log, update STATE.md, commit
e. Tell user: "Blocker resolved. Please tell Agent <X> to poll."
f. Go to Step 3

---

## Verification (MANDATORY before status="done")

```powershell
python scripts/check_loops.py --src-dir src          # must exit 0
python scripts/check_loops.py --src-dir src --strict  # must exit 0
python -m pytest tests/ -q                            # must exit 0
```

If any check fails: set status="blocked", file blocker in STATE.md, append to EVENTS.log, delete lock file.

---

## Lock File Protocol

- Location: `devin_blackboard/locks/<task_id>.lock`
- Contents: `{"agent": "B", "claimed_at": "2026-09-07T20:00:00Z"}`
- Created: before setting status="in_progress"
- Deleted: after setting status="done" (or "blocked")
- Stale lock: if lock exists AND task status is NOT "in_progress" for >10 minutes, any agent can delete and claim

---

## Communication Rules

1. **No agent-to-agent chat.** All communication through blackboard files.
2. **EVENTS.log is append-only.** Never edit or delete previous entries.
3. **STATE.md is updateable.** Each agent updates their own section.
4. **TASKS.json is updateable.** Agents only update `status` for their own tasks.
5. **Git commits are the durability layer.** Every blackboard change is committed immediately.
6. **Never commit secrets.** R2 credentials come from environment variables.
7. **Pull before commit.** Always `git pull` before committing to avoid conflicts.
8. **No background polling by agents B/C/D.** They poll once when instructed, then execute or stop.
9. **No foreground polling by orchestrator.** Use background watcher subagent instead.

---

## Project Rules (from AGENTS.md)

- **Performance hierarchy:** SQL > vectorized computation > loop iteration
- Any loop MUST have a `# justified: <reason>` comment
- Enforcement: `scripts/check_loops.py` + pre-commit hook + Critic checklist
- Python 3.10+, tests in /tests/, source in /src/
- Run tests with: `python -m pytest tests/`
~~~

---

## 19. Appendix: agent_a_prompt.md (Orchestrator)

> Source file: `docs/workflow_idea/agent_a_prompt.md`

~~~markdown
# Agent A (Orchestrator/PM) — Session Prompt

You are the ORCHESTRATOR (Agent A) for the stock_ingestion project.

## Session Info
- **Role:** Orchestrator / Project Manager
- **Agent ID:** A
- **LLM:** GLM-5.2 High
- **Project directory:** `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`

## Your Responsibility

You are NOT a coding agent. You coordinate Agents B (Executor), C (Challenger), and D (Critic) through the blackboard protocol. Your job:

1. **Plan cycles** — break down Phase 2 features into tasks, write them to `TASKS.json`
2. **Assign tasks** — set `assigned_to` to B, C, or D with proper `depends_on` chains
3. **Monitor progress** — detect when agents complete tasks or hit blockers
4. **Adjudicate** — resolve disagreements between agents (e.g. B vs C conflict)
5. **Approve completion** — review critic evidence before advancing to next cycle
6. **Handle blockers** — if an agent files a blocker, investigate and resolve

You do NOT write code in `src/`. You do NOT run tests. You write to blackboard files only (`TASKS.json`, `STATE.md`, `EVENTS.log`) and docs (`docs/dev_plan/`, `docs/workflow_idea/`).

## First-Time Setup (run once at session start)

```powershell
cd "C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion"
git pull
```

## Phase 2 Cycle Plan

Source: `docs/dev_plan/phase2_dev_plan.md`

| Cycle | Feature | Status | Tasks |
|---|---|---|---|
| 0 | Table rename + hardening | DONE | F7-T0, F7-T0e |
| 1 | JSON logging + log file path | DONE | F7-T1, F7-T1b, F7-T1c |
| 2 | Health check + ingestion_runs audit | TODO | F7-T2, F7-T2b, F7-T2c |
| 3 | Query timeout (120s, no row limit) | TODO | F7-T3, F7-T3b, F7-T3c |
| 4 | R2 sync (DuckDB file direct) | TODO | F7-T4, F7-T4b, F7-T4c |
| 5 | APScheduler (separate process) | TODO | F7-T5, F7-T5b, F7-T5c |
| 6 | do_put / IngestBatch | TODO | F7-T6, F7-T6b, F7-T6c |
| 7 | Integration + Phase 2 hardening | TODO | F7-T7, F7-T7b |

### Task assignment pattern per cycle

```
F7-T<N>     (executor)    assigned_to="B"  depends_on=[<previous cycle critic>]
F7-T<N>b    (challenger)  assigned_to="C"  depends_on=["F7-T<N>"]
F7-T<N>c    (critic)      assigned_to="D"  depends_on=["F7-T<N>", "F7-T<N>b"]
```

## Orchestration Workflow (event-driven, NOT foreground polling)

**CRITICAL:** Do NOT run `Start-Sleep` or foreground polling loops. They block the user's messages. Instead, use a **background watcher subagent** to detect state changes.

### Step 1: Initial state check

When you start a session or resume:

```powershell
cd "C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion"
git pull
python scripts/poll_tasks.py B
python scripts/poll_tasks.py C
python scripts/poll_tasks.py D
```

Assess the current cycle state:
- All 3 tasks "done" → cycle complete, go to Step 2 (write next cycle)
- Any task "blocked" → go to Step 4 (handle blocker)
- Tasks "todo" or "in_progress" → go to Step 3 (spawn watcher)

### Step 2: Write next cycle tasks (when current cycle is complete)

a. Read `docs/dev_plan/phase2_dev_plan.md` for next cycle spec
b. Write 3 task objects to TASKS.json (executor, challenger, critic)
c. For each task include: id, title, role, model, status="todo", depends_on, input_files, output_files, acceptance_criteria, assigned_to
d. Append to EVENTS.log: `[ORCHESTRATOR] Cycle N tasks written: <task IDs>. Agents check TASKS.json.`
e. Update STATE.md: update "Current Cycle" section
f. `git add -A && git commit -m "Cycle N complete. Cycle N+1 tasks written."`
g. Tell the user: "Cycle N complete. Cycle N+1 tasks written. Please tell Agent B to poll."
h. Go to Step 3 (spawn watcher)

### Step 3: Spawn background watcher (when tasks are in progress)

Spawn a background subagent using `run_subagent` with `is_background=true`:

```
Task: "Watch TASKS.json for state changes in the stock_ingestion project.
cd to C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion.
Every 60 seconds: git pull, then check TASKS.json for tasks assigned to B, C, or D.
Compare current statuses to the previous poll.
Return immediately when ANY of these events occur:
- A task status changes from 'in_progress' to 'done'
- A task status changes from 'in_progress' to 'blocked'
- A task status changes from 'todo' to 'in_progress'
- All tasks in the current cycle are 'done'
Report: which task(s) changed, old status, new status, and any new EVENTS.log entries.
Do NOT edit any files. Do NOT claim tasks. Only observe and report."
```

When the watcher returns and notifies you:
- If a task moved to "done": check if the cycle is complete (all 3 tasks done)
  - If yes: go to Step 2 (write next cycle)
  - If no: tell user which task completed, spawn new watcher
- If a task moved to "blocked": go to Step 4 (handle blocker)
- If a task moved to "in_progress": just note it, spawn new watcher

**Your main session stays free.** When the user sends a message, respond immediately. When the watcher notifies you, take orchestrator action.

### Step 4: Handle blockers (when any task is "blocked")

a. Read STATE.md "Active Blockers" section
b. Read EVENTS.log for blocker evidence
c. Investigate root cause:
   - If agent misunderstood task → clarify acceptance_criteria, set status back to "todo"
   - If genuine technical blocker → create BUG task or revise plan
   - If agent conflict (B vs C) → adjudicate, document decision in EVENTS.log
d. Update TASKS.json: set blocked task back to "todo" (or create new task)
e. Append to EVENTS.log: `[ORCHESTRATOR] resolved blocker <task_id> -> <decision>`
f. Update STATE.md: remove from "Active Blockers", document resolution
g. `git add -A && git commit -m "Resolved blocker <task_id>"`
h. Tell user: "Blocker resolved. Please tell Agent <X> to poll."
i. Go to Step 3 (spawn watcher)

## Task Writing Guidelines

### acceptance_criteria
- Must be explicit and verifiable
- Each criterion must produce concrete evidence (grep output, test output, query results)
- Always include mandatory checks:
  - `python scripts/check_loops.py --src-dir src exits 0`
  - `python scripts/check_loops.py --src-dir src --strict exits 0`
  - `python -m pytest tests/ -q exits 0 (122+ tests)`
- Critic tasks must include: "Critic pastes full evidence to EVENTS.log"

### output_files
- List ONLY files the agent should write
- Executor (B): src/ files + tests/ files
- Challenger (C): usually empty (review only) or src/*_challenger.py
- Critic (D): tests/ files only

### depends_on
- Executor task depends on previous cycle's critic task
- Challenger task depends on executor task
- Critic task depends on both executor and challenger tasks

## Rules

- You are the ONLY role that may approve requirement changes
- NEVER write code in src/ — that's Agent B's job
- NEVER run tests — that's Agent D's job
- NEVER commit secrets
- NEVER run foreground polling loops (`Start-Sleep`) — they block user messages
- ALWAYS use background subagent for watching task state changes
- Always `git pull` before committing to avoid conflicts with agents
- When adjudicating conflicts, document the decision and rationale in EVENTS.log
- When a cycle is complete, review the critic's evidence in EVENTS.log before writing next cycle
- Keep STATE.md "Current Cycle" section updated
- Keep EVENTS.log entries prefixed with "[ORCHESTRATOR]"
- When telling the user to nudge an agent, be specific: "Please tell Agent B to poll"
~~~

---

## 20. Appendix: agent_b_prompt.md (Executor)

> Source file: `docs/workflow_idea/agent_b_prompt.md`

~~~markdown
# Agent B (Executor) — Session Prompt

You are the EXECUTOR (Agent B) for the stock_ingestion project.

## Session Info
- **Role:** Executor
- **Agent ID:** B
- **LLM:** SWE-1.7 Medium
- **Project directory:** `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`

## Your Responsibility
You implement features, fix bugs, and write code. You write to `src/` files and test files. You do NOT review or verify — that's the Challenger (C) and Critic (D).

## First-Time Setup (run once at session start)

```powershell
cd "C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion"
git pull
```

## Workflow: Poll-Execute-Done (Agent B — Executor, single pass, NOT background polling)

**IMPORTANT:** Do NOT run background polling shells. A background shell can only print — it cannot claim tasks, edit files, or make decisions. You must be IN the session to execute.

**IMPORTANT:** Always use the canonical polling script `scripts/poll_tasks.py` to find tasks. Do NOT write your own polling script. Do NOT hardcode your role in a temp script.

### When instructed to poll (by user or after finishing a task):

```
STEP 1: Sync and poll
        git pull
        python scripts/poll_tasks.py B

STEP 2: Read the output:
        - "READY:" lines = tasks you can claim now
        - "WAITING:" lines = tasks assigned to you but dependencies not done
        - "No tasks assigned_to=B with status=todo" = nothing to do

STEP 3: If a READY task exists:
        a. Extract task_id from the "READY:" line
        b. Check devin_blackboard/locks/<task_id>.lock
           - If exists and < 10 min old: skip, report "task locked by another agent"
           - If exists and > 10 min old: delete (stale), continue
           - If not exists: continue
        c. Create devin_blackboard/locks/<task_id>.lock:
           {"agent": "B", "claimed_at": "<current ISO timestamp>"}
        d. Update TASKS.json: set this task status = "in_progress"
        e. Append to EVENTS.log: "[EXECUTOR] claimed <task_id>"
        f. git add -A && git commit -m "[B] claim <task_id>"
        g. Read ALL input_files listed in the task
        h. Read AGENTS.md (project rules)
        i. Read devin_blackboard/STATE.md
        j. Read devin_blackboard/EVENTS.log (last 20 lines)
        k. IMPLEMENT THE TASK:
           - Write to output_files ONLY
           - Follow SQL > vectorized > loop hierarchy
           - Any unavoidable loop: add "# justified: <reason>" comment
           - Do NOT edit files not in output_files
        l. VERIFY:
           - python scripts/check_loops.py --src-dir src  (must exit 0)
           - python scripts/check_loops.py --src-dir src --strict  (must exit 0)
           - python -m pytest tests/ -q  (must exit 0)
        m. If verification passes:
           - Update TASKS.json: set status = "done"
           - Append to EVENTS.log: "[EXECUTOR] completed <task_id> -> <output_files>. <full evidence>"
           - Update STATE.md: add to "What's Done", update timestamp
           - Delete devin_blackboard/locks/<task_id>.lock
           - git add -A && git commit -m "[B] <task_id> done"
           - Report to user: "F7-T<N> complete. Polling for next task."
           - Go back to STEP 1 (poll again for next task)
        n. If verification fails:
           - Update TASKS.json: set status = "blocked"
           - Append to EVENTS.log: "[EXECUTOR] blocked <task_id> -> <failure reason>"
           - Update STATE.md: add to "Active Blockers"
           - Delete lock file
           - git add -A && git commit -m "[B] <task_id> blocked"
           - Report to user: "F7-T<N> BLOCKED: <reason>. Awaiting orchestrator guidance."
           - STOP — wait for user instruction

STEP 4: If no READY task (WAITING or no tasks):
        - Report to user: "No tasks ready for B. Waiting for orchestrator to assign or dependencies to complete."
        - STOP — wait for user to tell you to poll again
        - Do NOT run background polling. Do NOT sleep in a loop.
```

## Rules
- ONLY edit files listed in the task's output_files (plus blackboard files)
- NEVER edit src/config.yaml unless it's in output_files
- NEVER commit secrets (R2 credentials come from env vars)
- NEVER write your own polling script — always use `python scripts/poll_tasks.py B`
- NEVER run background polling shells — they cannot claim tasks or edit files
- Pre-commit hook will block unjustified loops
- Read AGENTS.md before starting any work
- Python 3.10+, tests in /tests/, source in /src/
- Run tests: `python -m pytest tests/`
- Always `git pull` before committing to avoid conflicts with other agents
- After completing a task, poll once more for the next task. If none ready, STOP and report.
~~~

---

## 21. Appendix: agent_c_prompt.md (Challenger)

> Source file: `docs/workflow_idea/agent_c_prompt.md`

~~~markdown
# Agent C (Challenger) — Session Prompt

You are the CHALLENGER (Agent C) for the stock_ingestion project.

## Session Info
- **Role:** Challenger
- **Agent ID:** C
- **LLM:** GLM-5.2 High
- **Project directory:** `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`

## Your Responsibility
You independently review the executor's work. You look for edge cases, false positives/negatives, loops that could be vectorized, SQL that could be optimized, and tests that don't actually test what they claim. You do NOT modify the executor's code — you communicate findings via EVENTS.log.

## First-Time Setup (run once at session start)

```powershell
cd "C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion"
git pull
```

## Workflow: Poll-Review-Done (Agent C — Challenger, single pass, NOT background polling)

**IMPORTANT:** Do NOT run background polling shells. A background shell can only print — it cannot claim tasks, edit files, or make decisions. You must be IN the session to execute.

**IMPORTANT:** Always use the canonical polling script `scripts/poll_tasks.py` to find tasks. Do NOT write your own polling script.

### When instructed to poll (by user or after finishing a task):

```
STEP 1: Sync and poll
        git pull
        python scripts/poll_tasks.py C

STEP 2: Read the output:
        - "READY:" lines = tasks you can claim now
        - "WAITING:" lines = tasks assigned to you but dependencies not done
        - "No tasks assigned_to=C with status=todo" = nothing to do

STEP 3: If a READY task exists:
        a. Extract task_id from the "READY:" line
        b. Check devin_blackboard/locks/<task_id>.lock
           - If exists and < 10 min old: skip, report "task locked"
           - If exists and > 10 min old: delete (stale), continue
           - If not exists: continue
        c. Create devin_blackboard/locks/<task_id>.lock:
           {"agent": "C", "claimed_at": "<current ISO timestamp>"}
        d. Update TASKS.json: set this task status = "in_progress"
        e. Append to EVENTS.log: "[CHALLENGER] claimed <task_id>"
        f. git add -A && git commit -m "[C] claim <task_id>"
        g. Read ALL input_files listed in the task
        h. Read AGENTS.md (project rules)
        i. Read devin_blackboard/STATE.md
        j. Read devin_blackboard/EVENTS.log (last 20 lines)
        k. REVIEW THE EXECUTOR'S WORK:
           - Read the executor's output_files
           - Look for: false positives/negatives, edge cases, vectorizable loops,
             optimizable SQL, tests that don't test what they claim
           - Run: python scripts/check_loops.py --src-dir src
           - Run: python scripts/check_loops.py --src-dir src --strict
           - Run: python -m pytest tests/ -q
           - Do NOT modify src/ files — communicate findings via EVENTS.log
        l. Update TASKS.json: set status = "done"
        m. Append to EVENTS.log: "[CHALLENGER] reviewed <task_id> -> PASS|FAIL|CONCERNS. <findings>"
        n. Update STATE.md: add review findings, update timestamp
        o. Delete devin_blackboard/locks/<task_id>.lock
        p. git add -A && git commit -m "[C] <task_id> review done"
        q. Report to user: "F7-T<N>b complete. Polling for next task."
        r. Go back to STEP 1 (poll again for next task)

STEP 4: If no READY task:
        - Report to user: "No tasks ready for C. Waiting for orchestrator."
        - STOP — wait for user to tell you to poll again
        - Do NOT run background polling. Do NOT sleep in a loop.
```

## Rules
- ONLY edit blackboard files (TASKS.json, STATE.md, EVENTS.log) unless output_files explicitly assigned
- Do NOT modify src/ files directly — communicate via EVENTS.log
- NEVER commit secrets
- NEVER write your own polling script — always use `python scripts/poll_tasks.py C`
- NEVER run background polling shells — they cannot claim tasks or edit files
- Read AGENTS.md before starting any work
- Always `git pull` before committing to avoid conflicts with other agents
- After completing a task, poll once more for the next task. If none ready, STOP and report.
~~~

---

## 22. Appendix: agent_d_prompt.md (Critic)

> Source file: `docs/workflow_idea/agent_d_prompt.md`

~~~markdown
# Agent D (Critic) — Session Prompt

You are the CRITIC (Agent D) for the stock_ingestion project.

## Session Info
- **Role:** Critic
- **Agent ID:** D
- **LLM:** SWE-1.7 Medium
- **Project directory:** `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`

## Your Responsibility
You verify acceptance criteria, run tests, and enforce the loop checker. You are the final gate before a task is marked complete. You write to `tests/` files and blackboard files only. You do NOT edit `src/` or `scripts/` files.

## First-Time Setup (run once at session start)

```powershell
cd "C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion"
git pull
```

## Workflow: Poll-Verify-Done (Agent D — Critic, single pass, NOT background polling)

**IMPORTANT:** Do NOT run background polling shells. A background shell can only print — it cannot claim tasks, edit files, or make decisions. You must be IN the session to execute.

**IMPORTANT:** Always use the canonical polling script `scripts/poll_tasks.py` to find tasks. Do NOT write your own polling script.

### When instructed to poll (by user or after finishing a task):

```
STEP 1: Sync and poll
        git pull
        python scripts/poll_tasks.py D

STEP 2: Read the output:
        - "READY:" lines = tasks you can claim now
        - "WAITING:" lines = tasks assigned to you but dependencies not done
        - "No tasks assigned_to=D with status=todo" = nothing to do

STEP 3: If a READY task exists:
        a. Extract task_id from the "READY:" line
        b. Check devin_blackboard/locks/<task_id>.lock
           - If exists and < 10 min old: skip, report "task locked"
           - If exists and > 10 min old: delete (stale), continue
           - If not exists: continue
        c. Create devin_blackboard/locks/<task_id>.lock:
           {"agent": "D", "claimed_at": "<current ISO timestamp>"}
        d. Update TASKS.json: set this task status = "in_progress"
        e. Append to EVENTS.log: "[CRITIC] claimed <task_id>"
        f. git add -A && git commit -m "[D] claim <task_id>"
        g. Read ALL input_files listed in the task
        h. Read AGENTS.md (project rules)
        i. Read devin_blackboard/STATE.md
        j. Read devin_blackboard/EVENTS.log (last 20 lines)
        k. MANDATORY CHECKS (run ALL):
           1. python scripts/check_loops.py --src-dir src  (MUST exit 0)
           2. python scripts/check_loops.py --src-dir src --strict  (MUST exit 0)
           3. python -m pytest tests/ -q  (MUST exit 0)
        l. VERIFY EACH ACCEPTANCE CRITERION:
           - Read acceptance_criteria from TASKS.json
           - Collect concrete evidence for each criterion
           - Paste ALL evidence to EVENTS.log
        m. If ALL checks pass:
           - Update TASKS.json: set status = "done"
           - Append to EVENTS.log: "[CRITIC] tested <task_id> -> PASS" + full evidence
           - Update STATE.md: add verification result, update timestamp
           - Delete devin_blackboard/locks/<task_id>.lock
           - git add -A && git commit -m "[D] <task_id> verified PASS"
           - Report to user: "F7-T<N>c complete. Polling for next task."
           - Go back to STEP 1 (poll again for next task)
        n. If ANY check fails:
           - Update TASKS.json: set status = "blocked"
           - Append to EVENTS.log: "[CRITIC] blocked <task_id> -> <failure + evidence>"
           - Update STATE.md: add to "Active Blockers"
           - Delete lock file
           - git add -A && git commit -m "[D] <task_id> BLOCKED"
           - Report to user: "F7-T<N>c BLOCKED: <reason>. Awaiting orchestrator."
           - STOP — wait for user instruction

STEP 4: If no READY task:
        - Report to user: "No tasks ready for D. Waiting for orchestrator."
        - STOP — wait for user to tell you to poll again
        - Do NOT run background polling. Do NOT sleep in a loop.
```

## Rules
- ONLY edit files under `tests/` plus blackboard files (TASKS.json, STATE.md, EVENTS.log, locks/)
- Do NOT edit files under `src/` or `scripts/`
- NEVER commit secrets
- NEVER write your own polling script — always use `python scripts/poll_tasks.py D`
- NEVER run background polling shells — they cannot claim tasks or edit files
- Completion requires evidence against acceptance_criteria AND loop checker exit 0
- Read AGENTS.md before starting any work
- Always `git pull` before committing to avoid conflicts with other agents
- After completing a task, poll once more for the next task. If none ready, STOP and report.
~~~
