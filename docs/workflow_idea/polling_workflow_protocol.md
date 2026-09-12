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
| `devin_blackboard/TASKS.json` | Task definitions with status + assignment | Orchestrator creates; Agents update status only (via `poll_tasks.py` subcommands) |
| `devin_blackboard/STATE.md` | Current project state, what's done, blockers | All agents update their section |
| `devin_blackboard/EVENTS.log` | Append-only log of all actions | All agents append |
| `devin_blackboard/locks/<task_id>.lock` | Per-task lock files | Created/deleted by `poll_tasks.py --claim` / `--done` / `--blocked` |

---

## Task Lifecycle

```
todo  -->  in_progress  -->  done
                       \->  blocked
```

1. **todo**: Orchestrator writes task to TASKS.json with `status: "todo"`, `assigned_to: "B"` (or "C", "D")
2. **in_progress**: Subagent claims via `python scripts/poll_tasks.py --claim <task_id> <role>` — atomic lock + status + commit in one command
3. **done**: Subagent verifies via `--verify`, then `--done <task_id> <role> --evidence "..."` — sets status, appends evidence, deletes lock, commits
4. **blocked**: Subagent hits a blocker → `--blocked <task_id> <role> --reason "..."`; files blocker in STATE.md

---

## Agent Protocol (B/C/D) — Poll-Execute-Done (single pass)

**CRITICAL:** Agents do NOT run background polling shells. A background shell can only print — it cannot claim tasks, edit files, or make decisions. The agent must be IN the session to execute.

**CRITICAL:** Agents MUST use the canonical polling script `scripts/poll_tasks.py`. Agents MUST NOT write their own polling scripts.

**CRITICAL:** Agents MUST use the subcommands (`--claim`, `--done`, `--blocked`, `--verify`) instead of manually editing TASKS.json. This ensures atomic locks, structured criteria verification, and consistent commit messages.

### poll_tasks.py subcommands

| Command | Purpose |
|---|---|
| `python scripts/poll_tasks.py <role>` | Poll for ready tasks (backward compatible) |
| `python scripts/poll_tasks.py --claim <task_id> <role>` | Atomically claim a task (lock + status + commit) |
| `python scripts/poll_tasks.py --verify <task_id>` | Run all acceptance criteria, output evidence map |
| `python scripts/poll_tasks.py --done <task_id> <role> --evidence <text>` | Mark done (verifies criteria, sets status, deletes lock, commits) |
| `python scripts/poll_tasks.py --blocked <task_id> <role> --reason <text>` | Mark blocked (sets status, deletes lock, commits) |
| `python scripts/poll_tasks.py --clean-stale` | Remove stale locks (>10 min old), reset tasks to todo |

### When spawned by the orchestrator (or after finishing a task):

```
STEP 1: Sync and poll
        git pull
        python scripts/poll_tasks.py <B|C|D>

STEP 2: Read output:
        - "READY:" = claim and execute now
        - "WAITING:" = dependencies not done, stop and report
        - "No tasks" = nothing to do, stop and report

STEP 3: If READY task exists:
        a. CLAIM (atomic lock + status update + commit, all in one command):
           python scripts/poll_tasks.py --claim <task_id> <role>
        b. Read input_files, AGENTS.md, STATE.md, EVENTS.log
        c. EXECUTE the task (write to output_files ONLY)
        d. VERIFY acceptance criteria:
           python scripts/poll_tasks.py --verify <task_id>
        e. If pass, mark done:
           python scripts/poll_tasks.py --done <task_id> <role> --evidence "<summary + evidence>"
           -> Report to the orchestrator in your final message: "<task_id> complete."
           -> Go back to STEP 1 (poll once more for next task)
        f. If fail, mark blocked:
           python scripts/poll_tasks.py --blocked <task_id> <role> --reason "<failure>"
           -> Report to the orchestrator: "<task_id> BLOCKED: <reason>."
           -> STOP — your report is returned to the orchestrator

STEP 4: If no READY task:
        - Report to the orchestrator: "No tasks ready for <role>."
        - STOP — the orchestrator will spawn you again when needed
        - Do NOT run background polling. Do NOT sleep in a loop.
```

---

## Orchestrator Protocol (Agent A) — Local Subagent Spawning

**CRITICAL:** Do NOT run foreground polling loops (`Start-Sleep`). They block user messages. Spawn subagents via `run_subagent`; each call returns the subagent's report when it finishes.

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
- Task stuck "in_progress" → `python scripts/poll_tasks.py --clean-stale`, then Step 3
- Tasks "todo" → Step 3 (spawn next role subagent)

### Step 2: Write next cycle tasks
a. Read `docs/dev_plan/phase2_dev_plan.md` for next cycle spec
b. Write 3 task objects to TASKS.json (executor, challenger, critic)
c. Append to EVENTS.log, update STATE.md, commit
d. Go to Step 3

### Step 3: Spawn the next role subagent (sequential B → C → D)

Call `run_subagent` with `profile="subagent_general"`. The prompt = the full body of the role's prompt file (`docs/workflow_idea/agent_<role>_prompt.md`) + the concrete instruction:

```
cd "C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion"
git pull
python scripts/poll_tasks.py <role>
Claim the READY task via --claim, execute it (output_files ONLY), verify via --verify,
mark done via --done with evidence. Update STATE.md and EVENTS.log. Report back.
```

The call blocks until the subagent finishes and returns its report. Then:
- Task "done" → spawn the next role in sequence (B → C → D)
- Task "blocked" → Step 4
- Subagent failed/crashed without updating status → inspect its report; `--clean-stale` if needed; Step 4 or re-spawn

No watcher is needed in the normal flow — each `run_subagent` call returns a report. For waiting on external state, a `subagent_explore` read-only watcher may observe TASKS.json and report.

### Step 4: Handle blockers
a. Read STATE.md "Active Blockers" + EVENTS.log evidence
b. Investigate, adjudicate, document decision
c. Update TASKS.json (set back to "todo" or create new task)
d. Append to EVENTS.log, update STATE.md, commit
e. Go to Step 3 (spawn the appropriate role subagent)

---

## Optional: Devin Cloud MCP sessions (alternative)

When org quota is available, agents MAY be spawned as remote Devin Cloud sessions instead of local subagents:

- `devin_session_create` with the role's `playbook_id` + `notify_on_response: true` (replaces `run_subagent`)
- `devin_session_gather` to wait for the session to settle (replaces the synchronous return)
- `devin_session_interact` to message/nudge/retrieve agent output (replaces the returned report)

This path requires a `cog_` service-user API key (see `devin_platform_setup.md`) and consumes org ACU quota. If `devin_session_create` returns `out_of_quota`, fall back to the local `run_subagent` default above. The blackboard protocol and `poll_tasks.py` subcommands are identical in both modes — only the spawning mechanism differs.

---

## Verification (MANDATORY before status="done")

```powershell
python scripts/check_loops.py --src-dir src          # must exit 0
python scripts/check_loops.py --src-dir src --strict  # must exit 0
python -m pytest tests/ -q                            # must exit 0
```

`python scripts/poll_tasks.py --verify <task_id>` runs all structured acceptance criteria automatically; `--done` refuses to mark done if any criterion fails. If any check fails: `--blocked <task_id> <role> --reason "..."`, file blocker in STATE.md, append to EVENTS.log.

---

## Lock File Protocol (atomic)

- Location: `devin_blackboard/locks/<task_id>.lock`
- Contents: `{"agent": "B", "claimed_at": "2026-09-07T20:00:00Z"}`
- Created: atomically via `os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)` — eliminates TOCTOU race condition
- Created by: `python scripts/poll_tasks.py --claim <task_id> <role>` (not manual file creation)
- Deleted: after setting status="done" (or "blocked") — handled by `--done` and `--blocked` subcommands
- Stale lock: if lock exists AND `claimed_at` is >10 minutes old, `python scripts/poll_tasks.py --clean-stale` removes it and resets task to `todo`

---

## Communication Rules

1. **No agent-to-agent chat.** All communication through blackboard files.
2. **EVENTS.log is append-only.** Never edit or delete previous entries.
3. **STATE.md is updateable.** Each agent updates their own section.
4. **TASKS.json is updateable.** Agents only update `status` for their own tasks (via `poll_tasks.py` subcommands).
5. **Git commits are the durability layer.** `poll_tasks.py` commits and pushes automatically.
6. **Never commit secrets.** R2 credentials come from environment variables.
7. **Pull before claim.** Always `git pull` before claiming to avoid conflicts.
8. **No background polling by agents B/C/D.** They poll once when spawned, then execute or stop.
9. **No foreground polling by orchestrator.** `run_subagent` calls are synchronous — the orchestrator waits on the subagent's return, not on a polling loop.
10. **Subagent reports are ephemeral.** The blackboard (TASKS.json + EVENTS.log + STATE.md) is the durable record — subagents must write evidence there, not only in their returned message.

---

## Project Rules (from AGENTS.md)

- **Performance hierarchy:** SQL > vectorized computation > loop iteration
- Any loop MUST have a `# justified: <reason>` comment
- Enforcement: `scripts/check_loops.py` + pre-commit hook + Critic checklist
- Python 3.10+, tests in /tests/, source in /src/
- Run tests with: `python -m pytest tests/`
