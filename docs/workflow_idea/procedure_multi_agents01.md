# Multi-Agent Workflow — Comprehensive Documentation

> **Project:** stock_ingestion — Centralize stock-data ingestion and expose program interfaces.
> **Repository:** `vialliw/stock-ingestion`
> **Local path:** `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`
> **Status:** Phase 1 complete. Phase 2 in progress (Cycles 0-1 done, Cycle 2 ready).
> **Document version:** 3.0 (2026-09-07) — event-driven coordination

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
| Need it free | **Devin CLI native models**: `glm` (GLM-5.2 High) and `swe` (SWE-1.7 Medium) |

### Model selection

Devin CLI provides native models. OpenRouter or external API keys are not needed.

| Short name | Resolves to | Thinking | Used by |
|---|---|---|---|
| `glm` | GLM-5.2 High | High | Orchestrator (A), Challenger (C) |
| `swe` | SWE-1.7 Medium | Medium | Executor (B), Critic (D) |

Role differentiation comes from **system prompts** (the agent prompt files), not just the model. The same model can serve different roles because the prompt defines the behavior.

---

## 2. Prerequisites

1. Repo exists on GitHub: `vialliw/stock-ingestion`
2. Local clone at `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`
3. R2 secrets stored as **GitHub repo secrets** (never committed):
   - `R2_ACCESS_KEY_ID`
   - `R2_ENDPOINT_URL`
   - `R2_SECRET_ACCESS_KEY`
4. Devin CLI authenticated: `devin auth login`
5. Four Devin sessions created, one per agent role, each with the correct model:
   - Session A: `glm` (GLM-5.2 High) — Orchestrator
   - Session B: `swe` (SWE-1.7 Medium) — Executor
   - Session C: `glm` (GLM-5.2 High) — Challenger
   - Session D: `swe` (SWE-1.7 Medium) — Critic

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

## 4. Agent Roles and Session Architecture

### 4.1 Session topology

Four independent Devin sessions, each in its own terminal/window, each with a dedicated LLM. The user manually creates these sessions and pastes the corresponding agent prompt.

| Session | Role | LLM | Prompt file | Responsibility |
|---|---|---|---|---|
| A | Orchestrator (PM) | GLM-5.2 High | `docs/workflow_idea/agent_a_prompt.md` | Plans cycles, writes tasks, monitors progress, adjudicates, approves completion |
| B | Executor | SWE-1.7 Medium | `docs/workflow_idea/agent_b_prompt.md` | Implements features, fixes bugs, writes code in `src/` |
| C | Challenger | GLM-5.2 High | `docs/workflow_idea/agent_c_prompt.md` | Independent review, edge-case analysis, false-positive/negative detection |
| D | Critic | SWE-1.7 Medium | `docs/workflow_idea/agent_d_prompt.md` | Tests, verifies acceptance criteria, regression checks, loop checker enforcement |

### 4.2 Why four separate sessions (not subagents)?

Originally, the orchestrator spawned subagents internally. This had two problems:

1. **Model lock-in:** The orchestrator's internal subagent tool does not expose model selection — spawned subagents use the same model as the orchestrator (GLM-5.2 High), not the intended SWE-1.7 Medium for the executor.
2. **Context coupling:** Internal subagents share the orchestrator's context window, defeating the purpose of context slicing.

Separate sessions solve both: each session runs its own model, has its own context, and communicates only through the blackboard.

### 4.3 Orchestrator's background watcher

The orchestrator (Session A) spawns a **background subagent** to watch for TASKS.json state changes. This is the only subagent the orchestrator spawns — it is read-only and only notifies the orchestrator when a task status changes. The orchestrator's main session stays free to respond to the user.

---

## 5. The Orchestration Loop

### 5.1 Flow diagram

```
User tells Agent B to poll
        │
        ▼
┌────────────────┐
│ Agent B polls  │     git pull + python scripts/poll_tasks.py B
│ (once)         │
└───────┬────────┘
        │ READY task found
        ▼
┌────────────────┐
│ Agent B claims │     lock file + status=in_progress + commit
│ + executes     │     implement + verify + status=done + commit
└───────┬────────┘
        │ B reports done
        ▼
┌────────────────┐
│ Orchestrator   │     background watcher detects status change
│ notified       │     checks if cycle complete
└───────┬────────┘
        │ next task in cycle ready
        ▼
┌────────────────┐
│ User tells     │
│ Agent C to poll│     (challenger reviews B's work)
└───────┬────────┘
        │ C reports done
        ▼
┌────────────────┐
│ User tells     │
│ Agent D to poll│     (critic verifies acceptance criteria)
└───────┬────────┘
        │ D reports done
        ▼
┌────────────────┐
│ Orchestrator   │     writes next cycle tasks to TASKS.json
│ writes next    │     tells user to nudge Agent B
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
- Tell user: "Cycle N complete. Please tell Agent B to poll."

**Step 3 — Spawn background watcher:**
- Spawn a background subagent that polls TASKS.json every 60s
- Watcher returns immediately when any task status changes
- Orchestrator's main session stays free for user interaction

**Step 4 — Handle blockers** (when any task is "blocked"):
- Read STATE.md "Active Blockers" + EVENTS.log evidence
- Investigate, adjudicate, document decision in EVENTS.log
- Set blocked task back to "todo" or create new task
- Tell user: "Blocker resolved. Please tell Agent <X> to poll."

---

## 6. Agent Execution Protocol (B/C/D)

### 6.1 Core principle: poll-once-then-execute-or-stop

Agents do NOT run background polling. They do NOT sleep in loops. They:

1. **Poll once** when instructed (by user or after finishing a task)
2. If a task is READY: **claim → execute → verify → mark done → poll once more**
3. If no task is READY: **STOP and report** — wait for user instruction

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
        a. Check lock file (skip if < 10 min old, delete if stale)
        b. Create lock file: {"agent": "<role>", "claimed_at": "<ISO timestamp>"}
        c. Set status = "in_progress" in TASKS.json
        d. Append to EVENTS.log: "[<ROLE>] claimed <task_id>"
        e. git commit
        f. Read input_files, AGENTS.md, STATE.md, EVENTS.log (last 20 lines)
        g. EXECUTE the task (write to output_files ONLY)
        h. VERIFY (loop checker + pytest must exit 0)
        i. If pass:
           - Set status="done", append evidence to EVENTS.log
           - Update STATE.md, delete lock file, commit
           - Report to user: "<task_id> complete. Polling for next task."
           - Go back to STEP 1 (poll once more)
        j. If fail:
           - Set status="blocked", append failure to EVENTS.log
           - Update STATE.md, delete lock file, commit
           - Report to user: "<task_id> BLOCKED: <reason>"
           - STOP — wait for user instruction

STEP 4: If no READY task:
        - Report: "No tasks ready for <role>. Waiting for orchestrator."
        - STOP — do NOT poll again until user instructs
```

### 6.3 Canonical polling script

All agents use `scripts/poll_tasks.py` — never self-written polling scripts:

```powershell
python scripts/poll_tasks.py B    # Agent B
python scripts/poll_tasks.py C    # Agent C
python scripts/poll_tasks.py D    # Agent D
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
8. **No background polling by agents B/C/D.** They poll once when instructed, then execute or stop.
9. **No foreground polling by orchestrator.** Use background watcher subagent instead.
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

---

## 14. Lessons Learned

### 14.1 Separate sessions > internal subagents

Internal subagents share the orchestrator's model and context. Separate sessions allow each agent to use the appropriate model and maintain its own context window. The tradeoff is that the user must manually coordinate (tell agents when to poll), but this is preferable to the model mismatch problem.

### 14.2 Background shells cannot make decisions

A background shell (`exec` with `timeout: 0`) can run commands and produce output, but it cannot interpret that output, make decisions, edit files, or commit. Any workflow that requires the agent to "poll in the background and act on results" must have the agent in the session, not a background shell.

### 14.3 Foreground sleep blocks user interaction

Any `Start-Sleep` or long-running foreground command in the orchestrator session blocks user messages. The orchestrator must always keep its main session free. Background subagents are the solution.

### 14.4 Canonical scripts prevent configuration drift

When agents write their own polling scripts, they introduce bugs (wrong role, wrong path, wrong logic). A single canonical script with role as a CLI argument eliminates this class of errors.

### 14.5 Explicit "stop and report" prevents infinite loops

Without an explicit "stop and report" instruction, agents may loop indefinitely polling for tasks that don't exist yet. The protocol now explicitly says: if no READY task, STOP and wait for user instruction.

### 14.6 The user is the transport layer

Since agents can't chat with each other directly, and background shells can't make decisions, the user manually tells each agent when to poll. This is the most reliable coordination mechanism given the current tooling. The background watcher reduces the orchestrator's need for user nudging, but agents B/C/D still need the user to tell them when to poll.

---

## 15. Open Questions for Third-Party Review

1. **User as transport layer:** Currently, the user manually tells Agent B/C/D when to poll. Is there a way to automate this without background shells? Could we use a file-based "ping" mechanism (e.g., orchestrator writes to `devin_blackboard/PING_B.md`, Agent B's session watches for it)?

2. **Background watcher reliability:** The background watcher subagent polls every 60s. Is this the right interval? Too fast wastes resources; too slow delays cycle progression. Should it be configurable?

3. **Challenger value:** In practice, the challenger (Agent C) has produced useful findings (loop checker gaps, SQL optimization suggestions) but has not blocked any task. Is the challenger role earning its cost? Should it be merged with the critic?

4. **Sequential vs parallel execution:** Currently, executor → challenger → critic is strictly sequential. Could challenger and critic run in parallel (both review the executor's work independently)? This would speed up cycles but lose the dependency chain.

5. **Lock file race condition:** Two agents could check for a lock file simultaneously, both find it absent, and both create it. The current protocol doesn't use atomic file creation. Should we use `os.open(O_CREAT | O_EXCL)` for atomic lock acquisition?

6. **Git conflict handling:** Multiple agents committing to the same repo can cause merge conflicts. The protocol says "git pull before commit" but doesn't specify conflict resolution. Should the orchestrator be the merge authority?

7. **Task granularity:** Are the current task sizes appropriate? F7-T0 was large (rename + CAST + batch splits + strict checker in one task). Should we split future tasks more finely?

8. **Acceptance criteria verification:** The critic pastes evidence to EVENTS.log, but there's no automated check that ALL criteria were verified. Should we add a checklist mechanism to TASKS.json?

9. **Model selection:** Is SWE-1.7 Medium the right model for the executor, or would High thinking level produce better code? Is GLM-5.2 High appropriate for the challenger, or would a different model provide more diverse perspective?

10. **State recovery:** If an agent crashes mid-task, the lock file remains and the task is stuck in "in_progress". The 10-minute stale-lock rule handles this, but should there be a more explicit recovery procedure?

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
