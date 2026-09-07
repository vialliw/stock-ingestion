# Multi-Agent Workflow Protocol — Polling-Based Coordination

**Status:** ACTIVE
**Date:** 2026-09-07
**Version:** 2.0 — revised for dedicated agent sessions with proper LLMs

---

## Overview

Four Devin sessions, each with a dedicated LLM, coordinate through the blackboard files in `devin_blackboard/`. The Orchestrator (Session A) writes tasks; Agents B/C/D poll for their assigned tasks and execute autonomously.

## Sessions

| Session | Role | LLM | Responsibility |
|---|---|---|---|
| A | Orchestrator (PM) | GLM-5.2 High | Plans, writes tasks to TASKS.json, reviews evidence, approves completion, resolves blockers |
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

## Polling Protocol (Agents B/C/D)

Each agent session runs this loop continuously:

```
LOOP:
  1. Read devin_blackboard/TASKS.json
  2. Find tasks where:
     - assigned_to == my_role (B/C/D)
     - status == "todo"
     - all dependencies (depends_on) have status == "done"
  3. If no matching task:
     - Print "No tasks assigned to me. Waiting 30s..."
     - Sleep 30 seconds
     - Go back to step 1
  4. If matching task found:
     a. Check devin_blackboard/locks/<task_id>.lock
        - If lock file exists AND was created < 10 minutes ago: skip, go to step 1
        - If lock file exists AND was created > 10 minutes ago: delete it (stale lock), continue
        - If no lock file: continue
     b. Create devin_blackboard/locks/<task_id>.lock with contents:
        {"agent": "<B/C/D>", "claimed_at": "<ISO timestamp>"}
     c. Update TASKS.json: set task status = "in_progress"
     d. Append to EVENTS.log: "[<ROLE>] claimed <task_id>"
     e. Commit blackboard changes: git add -A && git commit -m "[<B/C/D>] claim <task_id>"
     f. Read all input_files for the task
     g. Read AGENTS.md for project rules (SQL > vectorized > loop)
     h. Read devin_blackboard/STATE.md and devin_blackboard/EVENTS.log (last 20 lines)
     i. Execute the task (read input_files, write output_files)
     j. Verify (see verification section below)
     k. Update TASKS.json: set task status = "done"
     l. Append to EVENTS.log: "[<ROLE>] completed <task_id> -> <output_files>. <evidence>"
     m. Update STATE.md: add to "What's Done", update timestamp
     n. Delete lock file: devin_blackboard/locks/<task_id>.lock
     o. Commit all changes: git add -A && git commit -m "[<B/C/D>] <task_id> <description>"
     p. Go back to step 1
```

---

## Verification (MANDATORY before setting status="done")

Every agent must run these before marking a task done:

```powershell
# 1. Loop checker (must exit 0)
python scripts/check_loops.py --src-dir src
python scripts/check_loops.py --src-dir src --strict

# 2. Full test suite (must exit 0)
python -m pytest tests/ -q

# 3. Task-specific acceptance criteria (read from TASKS.json)
```

If any check fails:
- Do NOT set status to "done"
- Set status to "blocked"
- File blocker in STATE.md under "Active Blockers"
- Append to EVENTS.log with failure evidence
- Delete lock file

---

## Orchestrator Protocol (Session A)

```
1. Plan the next cycle (write task definitions to TASKS.json)
2. For each task:
   - Set id, title, role, model, status="todo", assigned_to="B"/"C"/"D"
   - Set depends_on (list of task IDs that must be done first)
   - Set input_files, output_files, acceptance_criteria
3. Append to EVENTS.log: "[ORCHESTRATOR] Cycle N tasks written. Agents check TASKS.json."
4. Commit
5. Poll TASKS.json every 60s:
   - Check if all tasks in current cycle have status="done"
   - If any task has status="blocked": review blocker, resolve, set back to "todo"
6. When all cycle tasks are "done":
   - Review evidence in EVENTS.log
   - If acceptable: write next cycle tasks
   - If not acceptable: create BUG tasks or reassign
```

---

## Lock File Protocol

Lock files prevent two sessions from working on the same task.

- Location: `devin_blackboard/locks/<task_id>.lock`
- Contents: `{"agent": "B", "claimed_at": "2026-09-07T20:00:00Z"}`
- Created: before setting status="in_progress"
- Deleted: after setting status="done" (or "blocked")
- Stale lock: if lock file exists AND task status is NOT "in_progress" for >10 minutes, any agent can delete the stale lock and claim the task

---

## Dependency Rules

- A task with `depends_on: ["F7-T0"]` will NOT be picked up until F7-T0 has `status: "done"`
- The polling loop checks all dependencies before claiming
- If a dependency is "blocked", the dependent task stays "todo" (not picked up)

---

## Communication Rules

1. **No agent-to-agent chat.** All communication is through blackboard files.
2. **EVENTS.log is append-only.** Never edit or delete previous entries.
3. **STATE.md is updateable.** Each agent updates their own section.
4. **TASKS.json is updateable.** Agents only update `status` for their own tasks.
5. **Git commits are the durability layer.** Every blackboard change is committed immediately.
6. **Never commit secrets.** R2 credentials come from environment variables, not the repo.
7. **Pull before push.** Always `git pull` before committing to avoid conflicts with other agents.

---

## Project Rules (from AGENTS.md)

- **Performance hierarchy:** SQL > vectorized computation > loop iteration
- Any loop MUST have a `# justified: <reason>` comment
- Enforcement: `scripts/check_loops.py` + pre-commit hook + Critic checklist
- Python 3.10+, tests in /tests/, source in /src/
- Run tests with: `python -m pytest tests/`
