Here is the detailed enhancement plan, ordered strictly by **applicability and priority**, incorporating all immediate protocol fixes, AST and task-handling guardrails, and Devin platform integrations.

---

## Complete Multi-Agent Workflow Enhancement Plan

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      ENHANCEMENT PLAN ROADMAP BY TIER                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 0: Critical Immediate Fixes (Engine Protocol & Atomic Guardrails)          │
│   • POSIX/Windows Atomic File Locks (`os.O_CREAT | os.O_EXCL`)                   │
│   • Strict Role-Isolated Writes & Structural Schema Enforcers                   │
│   • Standardized JSON Array Acceptance Criteria & Critic Enforcement             │
│   • Stale Lock Reset Strategy & Auto-Recovery Protocol                         │
│                                                                                 │
│ Tier 1: Very High Applicability (Platform State & Execution Engine)             │
│   • 1.1 Devin MCP Servers — State Authority & Event Bus                         │
│   • 1.2 Devin Playbooks — Version-Locked Session Initialization                 │
│                                                                                 │
│ Tier 2: High Applicability (Orchestration & Workflow Automation)                │
│   • 2.1 Devin Session Management — Cross-Session Event Propagation              │
│   • 2.2 Parallel Review Execution & Conditional Challenger Protocol            │
│   • 2.3 Devin Dynamic Workflows — Declarative Cycle Progression                 │
│                                                                                 │
│ Tier 3: Medium Applicability (Routine Automation)                               │
│   • 3.1 Devin Schedules & Orchestrator Watcher Backoff                          │
└─────────────────────────────────────────────────────────────────────────────────┘

```

---

## Tier 0: Critical Immediate Protocol Fixes (Pre-Phase 3)

These enhancements address system reliability, race conditions, and verification blind spots without waiting for external platform integrations.

### 0.1 Atomic File Locking Protocol (`os.O_CREAT | os.O_EXCL`)

* **Current Gap:** Standard file writes in `poll_tasks.py` create a Time-of-Check to Time-of-Use (TOCTOU) race condition when two agents attempt to claim a task simultaneously.


* **How It Works:**
1. Refactor task claiming inside `scripts/poll_tasks.py` using low-level atomic OS flags:
```python
import os

def atomic_claim_lock(lock_path, data_bytes):
    try:
        # O_CREAT | O_EXCL guarantees atomic creation failure if file exists
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'wb') as f:
            f.write(data_bytes)
        return True
    except FileExistsError:
        return False

```


2. If creation fails, `poll_tasks.py` logs a collision event and safely skips the task.




* **Expected Outcome:** Guarantees absolute thread/process safety during lock acquisition across parallel agent instances.



---

### 0.2 Strict Role-Isolated Writes & Write-Authority Enforcers

* **Current Gap:** Concurrent edits to `STATE.md` or `TASKS.json` lead to git merge conflicts and corrupted blackboard state.


* **How It Works:**
1. Standardize write boundaries across sessions:
* **Orchestrator (A):** Sole author of task structures inside `TASKS.json`.


* **Executor (B):** Writes exclusively to assigned output files in `src/` and `tests/`.


* **Challenger (C):** Appends review findings strictly to `EVENTS.log` and `STATE.md`.


* **Critic (D):** Modifies `tests/` and appends execution verification to `EVENTS.log`.




2. Agents mutate task statuses (`todo` $\rightarrow$ `in_progress` $\rightarrow$ `done`) exclusively via `python scripts/poll_tasks.py --claim` or `--done` rather than editing raw JSON blocks manually.




* **Expected Outcome:** Eliminates 95%+ of git merge conflicts across sessions.



---

### 0.3 Standardized Structured Acceptance Criteria (JSON Array Schema)

* **Current Gap:** Unstructured prose in `acceptance_criteria` leads to missing checks or superficial verification by the Critic.


* **How It Works:**
1. Enforce a structured schema for task definitions inside `TASKS.json`:
```json
"acceptance_criteria": [
  {"id": "AC-1", "check": "loop_checker", "command": "python scripts/check_loops.py --src-dir src --strict", "expected_exit": 0},
  {"id": "AC-2", "check": "pytest_suite", "command": "python -m pytest tests/ -q", "expected_exit": 0},
  {"id": "AC-3", "check": "audit_table", "command": "verify_table_schema('ingestion_runs')", "expected_exit": 0}
]

```


2. Require Agent D (Critic) to execute each item in the array and output a matching JSON map to `EVENTS.log` before `poll_tasks.py --done` will accept the completion signal.




* **Expected Outcome:** Ensures complete, machine-verifiable proof of completion for every single task criterion.



---

### 0.4 Stale Lock Recovery & Task Granularity Rules

* **Current Gap:** Crashed sessions leave tasks locked in `in_progress`, while oversized tasks cause execution failures.


* **How It Works:**
1. Add a `--clean-stale` command to `poll_tasks.py` that verifies if an `in_progress` lock is over 10 minutes old. If stale, it removes the lock file, resets the task status to `todo`, and appends an auto-recovery line to `EVENTS.log`.


2. Enforce a strict **Task Granularity Limit** in the Orchestrator prompt: Tasks must be capped at maximum **3 modified files** and **<150 lines of code changed** per task object. Larger tasks must be split into atomic sub-tasks.




* **Expected Outcome:** Prevents pipeline stalls due to session drops and speeds up Executor/Challenger execution cycles.



---

## Tier 1: Very High Applicability (Devin Platform Integration)

Platform feature upgrades that systematically resolve context bloat, race conditions, and session startup variance.

### 1.1 Devin MCP Servers — Central State Authority & Event Bus

* **Objective:** Replace flat file polling and locking with a structured Model Context Protocol (MCP) server.


* **How It Works:**
1. Run a local, lightweight Python MCP server (`mcp_blackboard_server.py`) serving as the sole read/write authority over `devin_blackboard/`.


2. The server exposes typed endpoints to agent sessions:
* `blackboard/get_next_task(role)`: Performs dependency checks and returns available tasks.


* `blackboard/claim_task(task_id, agent)`: Handles atomic lock creation using native OS primitives.


* `blackboard/complete_task(task_id, evidence_map)`: Validates structured acceptance criteria outputs and updates status.


* `blackboard/subscribe()`: Provides an event stream/socket callback to notify Orchestrator A instantly upon status updates.




3. Keep the file-based blackboard (`STATE.md`, `EVENTS.log`) as a git-committed, read-only audit log written asynchronously by the server.




* **Expected Outcome:** Eliminates TOCTOU lock races, prevents file read/write git collisions, and cuts Orchestrator token usage by ~90% by substituting constant file polling with push event notifications.



---

### 1.2 Devin Playbooks — Standardized Session Prompts

* **Objective:** Codify agent prompts (`agent_a_prompt.md` through `agent_d_prompt.md`) into version-controlled Devin Playbooks.


* **How It Works:**
1. Convert system prompts into standard Playbook templates:


```yaml
name: executor-agent
description: "Agent B Executor for stock_ingestion"
model: swe # SWE-1.7 Medium
setup:
  - run: cd $PROJECT_ROOT
  - run: git pull
instructions: |
  Follow the Poll-Execute-Done protocol.
  Always check loop rules via scripts/check_loops.py --strict.

```


2. Parameterize `PROJECT_ROOT` to allow seamless deployment across environments.


3. Tie playbook release versions directly to the `TASKS.json` schema version.




* **Expected Outcome:** Guarantees 100% reproducible session startup, eliminates human copy-paste errors, and ensures model lock-in per role (`swe` vs `glm`).



---

## Tier 2: High Applicability (Workflow & Coordination Enhancements)

Reduces human coordination overhead while preserving safety and optimizing iteration speed.

### 2.1 Devin Session Linking — Cross-Session Signal Propagation

* **Objective:** Enable direct cross-session notification so downstream agents know when to poll without manual human nudging.


* **How It Works:**
1. Leverage native session messaging APIs (`session.notify()`) or a lightweight file-based signal bridge (`SIGNAL.json`).


2. When Agent B successfully executes `blackboard/complete_task`, the system generates an immediate wake-up ping to Session C (Challenger).


3. Agent C receives the signal, performs its single-pass review poll, and shuts down until the next event.


4. Include an `AUTO_MODE=false` flag in `STATE.md` so developers can pause automatic pass-through for complex structural changes.




* **Expected Outcome:** Eliminates ~60% of human transport layer manual nudging while maintaining a safety kill-switch.



---

### 2.2 Parallel Review Execution & Conditional Challenger Protocol

* **Objective:** Accelerate cycle completion times by running reviews in parallel and bypassing redundant Challenger steps for trivial tasks.


* **How It Works:**
1. **Parallel Execution:** Reconfigure dependencies so Challenger C and Critic D run concurrently after Executor B completes its task:


```
             ┌──► Challenger C (Design Review) ──┐
Executor B ──┤                                   ├──► Cycle Complete
             └──► Critic D (Criteria Check)  ───┘

```


2. **Conditional Challenger:** Add a flag `"challenger_required": false` to task definitions in `TASKS.json` for low-risk changes (such as simple config updates or unit test formatting). For complex schema or core calculation tasks, set to `true`.




* **Expected Outcome:** Cuts standard cycle duration by 40–50% for complex tasks and skips unnecessary review cycles on simple tasks.



---

### 2.3 Devin Dynamic Workflows — Declarative Cycle Progression

* **Objective:** Replace manual task creation and dependency tracking with declarative, end-to-end workflow definitions.


* **How It Works:**
1. Define cycle templates in a unified workflow document:


```yaml
workflow: phase2_cycle
steps:
  - name: execute
    role: B
    model: swe
  - name: review
    role: C
    model: glm
    depends_on: [execute]
    condition: task.challenger_required == true
  - name: verify
    role: D
    model: swe
    depends_on: [execute]
on_failure:
  action: halt_and_notify_orchestrator

```


2. Link cycle advancement directly to automated verification pass/fail results.




* **Expected Outcome:** Fully automates task progression and dependency gating across all standard cycles.



---

## Tier 3: Medium Applicability (Routine Maintenance Automation)

Optimizes long-running tasks and reduces background system load.

### 3.1 Devin Schedules & Orchestrator Watcher Exponential Backoff

* **Objective:** Automate routine polling for predictable jobs and prevent token waste in the Orchestrator's background watcher.


* **How It Works:**
1. **Exponential Backoff:** Refactor Orchestrator A's background watcher subagent to scale polling intervals dynamically:


* Active cycle: Poll every 60s.


* Inactive / waiting: Scale up to 120s $\rightarrow$ 300s.




2. **Scheduled Polling:** Configure Devin Schedules for Agent B on routine maintenance runs (e.g., executing night-build regression suites).


3. **Cost Guardrail:** Enforce a hard cap of 1 task execution per scheduled run to prevent runaway token usage during off-hours.




* **Expected Outcome:** Reduces background subagent token burn by ~30–50% during idle periods while providing automated test execution capabilities.



---

## Complete Implementation Sequence

```
Phase 2 (Current)             Phase 3 Cycle 0 - 1           Phase 3 Cycle 2 - 3
┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
│ Implement Tier 0 Fixes   │  │ Deploy Tier 1 Features   │  │ Deploy Tier 2 & 3        │
├──────────────────────────┤  ├──────────────────────────┤  ├──────────────────────────┤
│ • Add Atomic Locks       │  │ • Build Blackboard MCP   │  │ • Enable Session Link    │
│ • Enforce Strict Roles   │─►│   Server                 │─►│ • Enable Parallel C & D  │
│ • Structured Criteria    │  │ • Create Devin Playbooks │  │ • Deploy Dynamic Flow    │
│ • Stale Lock Cleaners    │  │                          │  │ • Implement Backoff      │
└──────────────────────────┘  └──────────────────────────┘  └──────────────────────────┘

```