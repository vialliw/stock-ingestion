# Devin Platform Integration Setup Guide

> **Status: OPTIONAL — cloud alternative.** The DEFAULT workflow spawns agents as **local subagents** via the Devin CLI's built-in `run_subagent` tool (`subagent_general` for write-capable Agents B/C/D, `subagent_explore` for read-only watchers) — no Devin Cloud session, no org quota, no API key, no cost beyond the local session's model usage. See `polling_workflow_protocol.md` and `agent_a_prompt.md` for the default protocol.
>
> Everything below describes the **optional Devin Cloud path** (MCP `devin_session_create` + playbooks + dynamic workflows). Use it only when org ACU quota is available. If `devin_session_create` returns `out_of_quota`, fall back to local `run_subagent` spawning — the blackboard protocol and `poll_tasks.py` subcommands are identical in both modes.
>
> Prerequisite for this optional path: Devin service user API key (`cog_` prefix) from https://app.devin.ai/settings/service-users

---

## Tier 0: Local Subagents (DEFAULT — no setup needed)

The default spawning mechanism needs none of the setup below. The orchestrator calls `run_subagent` with:

| Agent | Profile | Why |
|---|---|---|
| B (Executor), C (Challenger), D (Critic) | `subagent_general` | Write-capable — must edit files and update the blackboard |
| Watcher / read-only probes | `subagent_explore` | Read-only, cheap (SWE-1.6) — observe TASKS.json only |

Local subagents share the orchestrator's workspace and model. They claim tasks via `python scripts/poll_tasks.py --claim <id> <role>`, edit only `output_files`, verify via `--verify`, mark done via `--done <id> <role> --evidence "..."`, and append to `EVENTS.log`. The role prompt files (`agent_b_prompt.md` / `agent_c_prompt.md` / `agent_d_prompt.md`) are written to be executed verbatim by a local subagent — paste the file body into the `run_subagent` prompt plus the concrete poll/claim instruction.

## Tier 1: Devin MCP Server + Playbooks (OPTIONAL cloud alternative)

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

### Step 4: The 4 Devin Playbooks

> **Single source of truth (updated 2026-09-12):** role instructions live ONLY in
> `docs/workflow_idea/agent_{a,b,c,d}_prompt.md`. All four playbooks are thin
> pointers — each says "git pull, read `docs/workflow_idea/agent_<role>_prompt.md`,
> follow it verbatim". This guarantees the cloud playbooks and the local
> `.devin/agents/` subagent profiles can never drift: a revision is a single edit
> to the prompt file and both paths pick it up on the next spawn.
> Do NOT paste prompt bodies into playbooks again.

Existing playbook IDs:

| Role | Playbook | ID | Repo prompt file |
|---|---|---|---|
| Orchestrator | stock-ingestion-orchestrator | `playbook-70ba545fd0be45e180c7c9e06841611e` | `agent_a_prompt.md` |
| Executor | stock-ingestion-executor | `playbook-5ef9c6712be6463eb0f052ae22888886` | `agent_b_prompt.md` |
| Challenger | stock-ingestion-challenger | `playbook-9addf6496feb44bbbd8b44f88a970a25` | `agent_c_prompt.md` |
| Critic | stock-ingestion-critic | `playbook-f0577debe6014f4d9b3276c88fef0ce7` | `agent_d_prompt.md` |

If a playbook ever needs to be recreated, use `devin_playbook_manage`
action=create with title `stock-ingestion-<role>` and a thin-pointer body of the
form: "You are Agent <letter> (<role>) for stock_ingestion, running as a Devin
Cloud session on repo vialliw/stock-ingestion. git pull, then read
`docs/workflow_idea/agent_<letter>_prompt.md` and follow it verbatim. Your role
letter for `scripts/poll_tasks.py` is `<letter>`."

### Step 5: Update orchestrator to use MCP — DONE (now OPTIONAL)

The orchestrator prompt (`docs/workflow_idea/agent_a_prompt.md`) documents this MCP path under "Optional: Devin Cloud MCP sessions". As of 2026-09-12, local `run_subagent` spawning is the default; this section is retained for when org quota is available. The playbook on the Devin platform has been synced.

**What changed:**
- Added "MCP Session Management" section with playbook ID table
- Replaced Step 3 (background watcher subagent) with MCP-based child session spawning:
  - `devin_session_create` with `playbook_id` + `notify_on_response: true`
  - `devin_session_gather` to wait for child sessions to settle
  - `devin_session_interact` to message/nudge/retrieve agent output
- Replaced Step 4 (user-relay nudging) with direct MCP messaging
- Updated Rules section to mandate MCP tools over background subagents
- Added session tracking in STATE.md under "Active Sessions"

**Before (old pattern):**
> Tell user: "Cycle N complete. Cycle N+1 tasks written. Please tell Agent B to poll."

**After (MCP pattern):**
> Create Agent B session via `devin_session_create` with `playbook_id: "playbook-5ef9c6712be6463eb0f052ae22888886"` and `notify_on_response: true`. Wait via `devin_session_gather`. When B settles, spawn C. When C settles, spawn D. When D settles, cycle is complete.

**Playbook IDs (for reference):**
| Role | Playbook ID |
|---|---|
| Orchestrator | `playbook-70ba545fd0be45e180c7c9e06841611e` |
| Executor | `playbook-5ef9c6712be6463eb0f052ae22888886` |
| Challenger | `playbook-9addf6496feb44bbbd8b44f88a970a25` |
| Critic | `playbook-f0577debe6014f4d9b3276c88fef0ce7` |

### Model assignment limitation and decision (Option B)

The MCP `devin_session_create` API does NOT support per-session LLM selection (e.g. SWE-1.7 vs GLM-5.2). It only supports `devin_mode` tiers: `normal`, `fast`, `lite`, `ultra`, `fusion`.

**Three options were considered:**
- **Option A:** Keep manual webapp tabs with correct LLMs, use user as relay — rejected (manual relay was the original bottleneck)
- **Option B:** Use MCP-created sessions with org default model, full programmatic control — **CHOSEN**
- **Option C:** Hybrid (keep tabs for now, switch to MCP later) — not needed

**Decision:** Option B. All agent sessions are created via MCP with the org default model. The playbook provides role identification and workflow instructions. The `LLM` field in each playbook is documentation of the intended model, not an enforced setting.

**Optional `devin_mode` proxy for capability differentiation:**
| Agent | `devin_mode` | Rationale |
|---|---|---|
| B (Executor) | `normal` | Writes code, standard capability |
| C (Challenger) | `ultra` | Deeper reasoning for edge-case analysis |
| D (Critic) | `normal` | Runs checks, standard capability |

Note: `devin_mode` is batch-level only. Use separate `devin_session_create` calls per mode.

---

## Tier 2: Dynamic Workflows (Sequential B → C → D)

### Design decision: Sequential, not parallel

Challenger and Critic run **sequentially** (B → C → D), not in parallel. This ensures the critic sees the challenger's findings before verifying. The tradeoff is speed for thoroughness — chosen because code quality matters more than cycle speed for this project.

### Step 1: Enable Dynamic Workflows

If your account is enterprise:
1. Go to Enterprise Settings > Devin
2. Enable "Dynamic workflows"

For non-enterprise accounts, Dynamic Workflows are available by default.

### Step 2: Dynamic Workflow script

The file `scripts/workflow_phase2.py` is included in the repo. It defines:
- Phase "execute": Agent B implements the task
- Phase "review": Agent C reviews the executor's work
- Phase "verify": Agent D verifies with challenger findings available
- Phase "advance": Orchestrator reviews evidence and advances cycle

The script runs B → C → D sequentially. The critic's prompt includes the challenger's findings so it can focus on issues the challenger identified.

To run it, ask Devin in any session:
> "Use a workflow to run scripts/workflow_phase2.py for the stock_ingestion Phase 2 cycle"

Devin will:
1. Write/validate the workflow script
2. Ask for approval (unless auto-approve is on)
3. Run the script which spawns agent sessions on separate VMs
4. Stream progress to the session
5. Resume if interrupted

### Step 3: AUTO_MODE flag

Add `AUTO_MODE: true|false` to `STATE.md`:
- `true`: orchestrator auto-creates next agent sessions without pausing
- `false`: orchestrator pauses and asks user before creating sessions (default)

---

## Tier 3: Schedules + Exponential Backoff — SKIPPED

### Status: Not implemented (optional)

Tier 3 was evaluated and skipped:

| Item | Status | Reason |
|---|---|---|
| D1: Nightly regression schedule | Not created | Optional — can be added later via Devin UI (Settings > Schedules) or `devin_schedule_manage` MCP tool |
| D2: Weekly loop check schedule | Not created | Optional — same as above |
| D3: Exponential backoff | Obsolete | Replaced by MCP `notify_on_response` push notifications in Tier 1 — no polling means no backoff needed |

### How to create schedules later (if desired)

Use `devin_schedule_manage` MCP tool or Devin UI at Settings > Schedules:

**Nightly regression:**
- Name: stock-ingestion-nightly-regression
- Frequency: Every day at 02:00
- Playbook: stock-ingestion-critic
- Prompt: "Run the full test suite: python -m pytest tests/ -q. Report any failures."
- ACU limit: 5

**Weekly loop check:**
- Name: stock-ingestion-weekly-loop-check
- Frequency: Every Monday at 09:00
- Playbook: stock-ingestion-critic
- Prompt: "Run python scripts/check_loops.py --src-dir src --strict. Report any violations."
- ACU limit: 2

---

## Verification Checklist

### Tier 1
- [x] Devin MCP server configured in `.devin/config.local.json`
- [x] `mcp_list_servers` shows "devin" server
- [x] 4 playbooks created (orchestrator, executor, challenger, critic)
- [x] `devin_playbook_manage` (list) shows all 4 playbooks
- [x] Agent sessions can be created via `devin_session_create` with playbook reference
- [x] Orchestrator playbook updated with MCP Session Management section
- [x] Model limitation documented (Option B — org default model, devin_mode proxy)

### Tier 2
- [ ] Dynamic Workflows enabled (if enterprise) — optional
- [x] `scripts/workflow_phase2.py` exists and is valid Python (sequential B → C → D)
- [ ] Test run: "Use a workflow to run scripts/workflow_phase2.py" — optional, not required
- [x] Sequential mode chosen over parallel (critic sees challenger findings)
- [x] `challenger_required: false` tasks skip the challenger phase (in workflow script)
- [ ] `AUTO_MODE` flag in STATE.md — optional, not yet set
- [x] MCP session linking verified (create → gather → get_messages → sleep)

### Tier 3 — SKIPPED
- [ ] Nightly regression schedule — optional, can be created later
- [ ] Weekly loop check schedule — optional, can be created later
- [x] Exponential backoff — obsolete (replaced by MCP notify_on_response)
- [ ] ACU limits on scheduled sessions — N/A (no schedules created)
