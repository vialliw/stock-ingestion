# Devin Platform Integration Setup Guide

> This document covers Tiers 1-3 of the workflow enhancement plan.
> Prerequisite: Devin service user API key (`cog_` prefix) from https://app.devin.ai/settings/service-users

---

## Tier 1: Devin MCP Server + Playbooks

### Step 1: Get a Devin API key

1. Go to https://app.devin.ai/settings/service-users
2. Create a service user
3. Generate an API key (starts with `cog_`)
4. Store it securely — do NOT commit it to the repo

### Step 2: Configure Devin MCP server

Add the Devin MCP server to `.devin/config.local.json`:

```json
{
  "permissions": {
    "allow": [
      "Exec(mkdir)",
      "Exec(python)",
      "Exec(git)",
      "Exec(ls)",
      "Exec(PYTEST_EXIT=$LASTEXITCODE)",
      "Exec(Add-Content)"
    ]
  },
  "mcpServers": {
    "devin": {
      "serverUrl": "https://mcp.devin.ai/mcp",
      "headers": {
        "Authorization": "Bearer <YOUR_API_KEY>"
      }
    }
  }
}
```

Replace `<YOUR_API_KEY>` with your `cog_` key.

For enterprise accounts, also add the `X-Org-Id` header:

```json
"headers": {
  "Authorization": "Bearer <YOUR_API_KEY>",
  "X-Org-Id": "<YOUR_ORG_ID>"
}
```

Find your org ID at Settings > Service Users or via `GET /v3/self`.

### Step 3: Verify MCP connection

After updating the config, restart your Devin session. Verify MCP tools are available:

```
The Devin MCP server provides these tools:
- devin_session_create — create sessions with playbook, tags, ACU limit
- devin_session_search — search/filter sessions
- devin_session_interact — send messages, get status, terminate
- devin_session_gather — wait for multiple sessions to finish
- devin_playbook_manage — create/list/update/delete playbooks
- devin_schedule_manage — create/list/update/delete schedules
- devin_knowledge_manage — manage knowledge notes
```

### Step 4: Create 4 Devin Playbooks

Use `devin_playbook_manage` (or ask Devin in a session) to create these playbooks:

#### Playbook 1: stock-ingestion-orchestrator

```
Name: stock-ingestion-orchestrator
Description: Agent A — Orchestrator/PM for stock_ingestion multi-agent workflow
Model: glm (GLM-5.2 High)
Tags: stock-ingestion, orchestrator, agent-a
```

Content: Copy the body of `docs/workflow_idea/agent_a_prompt.md` (everything after the title line).

#### Playbook 2: stock-ingestion-executor

```
Name: stock-ingestion-executor
Description: Agent B — Executor for stock_ingestion multi-agent workflow
Model: swe (SWE-1.7 Medium)
Tags: stock-ingestion, executor, agent-b
```

Content: Copy the body of `docs/workflow_idea/agent_b_prompt.md`.

#### Playbook 3: stock-ingestion-challenger

```
Name: stock-ingestion-challenger
Description: Agent C — Challenger for stock_ingestion multi-agent workflow
Model: glm (GLM-5.2 High)
Tags: stock-ingestion, challenger, agent-c
```

Content: Copy the body of `docs/workflow_idea/agent_c_prompt.md`.

#### Playbook 4: stock-ingestion-critic

```
Name: stock-ingestion-critic
Description: Agent D — Critic for stock_ingestion multi-agent workflow
Model: swe (SWE-1.7 Medium)
Tags: stock-ingestion, critic, agent-d
```

Content: Copy the body of `docs/workflow_idea/agent_d_prompt.md`.

### Step 5: Update orchestrator to use MCP

After playbooks are created, update `agent_a_prompt.md` to replace manual user nudging with MCP session creation:

**Before (current):**
> Tell user: "Cycle N complete. Cycle N+1 tasks written. Please tell Agent B to poll."

**After (with MCP):**
> Create Agent B session:
> ```
> devin_session_create(
>   playbook="stock-ingestion-executor",
>   prompt="Poll for ready tasks: python scripts/poll_tasks.py B. Claim and execute any READY task.",
>   title="Agent B — Cycle N+1",
>   tags=["stock-ingestion", "cycle-N+1", "agent-b"]
> )
> ```
> Then use `devin_session_gather` to wait for B's session to finish.

---

## Tier 2: Dynamic Workflows + Parallel C+D

### Step 1: Enable Dynamic Workflows

If your account is enterprise:
1. Go to Enterprise Settings > Devin
2. Enable "Dynamic workflows"

For non-enterprise accounts, Dynamic Workflows are available by default.

### Step 2: Create the Dynamic Workflow script

The file `scripts/workflow_phase2.py` is included in the repo. It defines:
- Phase "execute": Agent B implements the task
- Phase "review": Agent C reviews + Agent D verifies (in parallel)
- Phase "advance": Orchestrator reviews evidence and advances cycle

To run it, ask Devin in any session:
> "Use a workflow to run scripts/workflow_phase2.py for the stock_ingestion Phase 2 cycle"

Devin will:
1. Write/validate the workflow script
2. Ask for approval (unless auto-approve is on)
3. Run the script which spawns agent sessions on separate VMs
4. Stream progress to the session
5. Resume if interrupted

### Step 3: Update dependency structure for parallel C+D

In TASKS.json, change the critic task's depends_on:
- **Before:** `F7-T<N>c depends_on: ["F7-T<N>", "F7-T<N>b"]`
- **After:** `F7-T<N>c depends_on: ["F7-T<N>"]` (remove dependency on challenger)

This allows challenger and critic to run in parallel after the executor finishes.

For tasks with `challenger_required: false`, the orchestrator skips creating the challenger task entirely.

### Step 4: Session linking

With MCP configured, the orchestrator can:
1. Create Agent B session via `devin_session_create`
2. Wait for B to finish via `devin_session_gather`
3. Create Agent C and D sessions in parallel via `devin_session_create`
4. Wait for both via `devin_session_gather`
5. Review evidence and create next cycle

Add `AUTO_MODE: true|false` to `STATE.md`:
- `true`: orchestrator auto-creates next agent sessions
- `false`: orchestrator pauses and asks user before creating sessions

---

## Tier 3: Schedules + Exponential Backoff

### Step 1: Create scheduled sessions

Use `devin_schedule_manage` (or Devin UI at Settings > Schedules) to create:

#### Nightly regression suite
```
Name: stock-ingestion-nightly-regression
Frequency: Every day at 02:00 (your timezone)
Agent: Devin (standard)
Playbook: stock-ingestion-critic
Prompt: "Run the full test suite: python -m pytest tests/ -q. Report any failures."
ACU limit: 5
```

#### Weekly loop check
```
Name: stock-ingestion-weekly-loop-check
Frequency: Every Monday at 09:00
Agent: Devin (standard)
Playbook: stock-ingestion-critic
Prompt: "Run python scripts/check_loops.py --src-dir src --strict. Report any violations."
ACU limit: 2
```

### Step 2: Exponential backoff for background watcher

If keeping the polling-based watcher (non-Dynamic-Workflow mode), update the watcher subagent prompt:

```
Poll TASKS.json with exponential backoff:
- If any task is in_progress: poll every 60s
- If all tasks are waiting/todo: poll every 120s, then 180s, then 300s (max)
- Reset to 60s immediately when any task status changes
- Return immediately when a task changes status
```

### Step 3: Cost guardrail

For scheduled sessions, set:
- ACU limit per scheduled run: max 5
- 1 task per scheduled run
- Pause schedule if 3 consecutive runs fail

---

## Verification Checklist

### Tier 1
- [ ] Devin MCP server configured in `.devin/config.local.json`
- [ ] `devin mcp list` (or equivalent) shows "devin" server
- [ ] 4 playbooks created (orchestrator, executor, challenger, critic)
- [ ] `devin_playbook_manage` (list) shows all 4 playbooks
- [ ] Agent sessions can be created via `devin_session_create` with playbook reference

### Tier 2
- [ ] Dynamic Workflows enabled (if enterprise)
- [ ] `scripts/workflow_phase2.py` exists and is valid Python
- [ ] Test run: "Use a workflow to run scripts/workflow_phase2.py" succeeds
- [ ] Challenger and Critic run in parallel (verified via session timestamps)
- [ ] `challenger_required: false` tasks skip the challenger phase
- [ ] `AUTO_MODE` flag in STATE.md controls auto-creation

### Tier 3
- [ ] Nightly regression schedule created and active
- [ ] Weekly loop check schedule created and active
- [ ] Background watcher uses exponential backoff (60s → 120s → 300s)
- [ ] ACU limits set on all scheduled sessions
