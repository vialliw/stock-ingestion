# Multi-Agent Workflow Protocol — Event-Driven Coordination

**Status:** ACTIVE
**Date:** 2026-09-07
**Version:** 3.0 — event-driven watcher (no foreground polling)

---

## Overview

Four Devin sessions, each with a dedicated LLM, coordinate through the blackboard files in `devin_blackboard/`. The Orchestrator (Session A) writes tasks and uses a **background watcher subagent** to detect state changes. Agents B/C/D poll once when instructed, execute, and report back.

## Sessions

| Session | Role | LLM | Responsibility |
|---|---|---|---|
| A | Orchestrator (PM) | GLM-5.2 High | Plans, writes tasks, spawns background watcher, reviews evidence, resolves blockers |
| B | Executor | SWE-1.7 Medium | Implements features, fixes bugs, writes code in src/ |
| C | Challenger | GLM-5.2 High | Independent review, alternative implementations, false-positive/negative analysis |
| D | Critic | SWE-1.7 Medium | Tests, verifies acceptance criteria, regression checks, loop checker enforcement |

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
