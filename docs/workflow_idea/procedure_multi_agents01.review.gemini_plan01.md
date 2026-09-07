## 4. Enhancement Plan: Devin Platform Integration Features

### Tier 1: Very High Applicability

#### 4.1 Devin MCP Servers (§15.13) — *Primary upgrade path for Phase 3*

**Objective:** Replace the entire file-based polling and locking layer with a structured MCP server that exposes the blackboard as a typed API.

**Current state:** Agents poll via `scripts/poll_tasks.py`, claim via lock files, and report via git commits. This creates race conditions, merge conflicts, and continuous token burn from the orchestrator watcher.

**Target state:** A lightweight MCP server (e.g., `mcp_blackboard_server.py`) running alongside the project, exposing:

| MCP Resource/Tool | Replaces |
|---|---|
| `blackboard/get_next_task(role)` | `scripts/poll_tasks.py` |
| `blackboard/claim_task(task_id, agent)` | Lock file protocol + `TASKS.json` status update |
| `blackboard/complete_task(task_id, evidence)` | Manual `TASKS.json` edit + `EVENTS.log` append |
| `blackboard/subscribe(state_change)` | Orchestrator background watcher |
| `blackboard/get_state()` | Repeated `STATE.md` reads |

**Implementation steps:**

1. **Phase 3, Cycle 0:** Design MCP server schema. Keep file-based blackboard as backing store for audit trail, but make MCP the *write* authority.
2. **Phase 3, Cycle 1:** Implement `claim_task` and `complete_task` as atomic operations using file locking on the server side (not git). The server serializes all blackboard mutations.
3. **Phase 3, Cycle 2:** Add `subscribe` with push notifications (e.g., WebSocket or file-descriptor callback) to replace the orchestrator's polling watcher.
4. **Phase 3, Cycle 3:** Migrate `poll_tasks.py` to be a thin MCP client. Agents still call `python scripts/poll_tasks.py B`, but the script now queries the MCP server instead of parsing JSON files.

**Risk mitigation:** Run MCP server locally on the developer machine, not remotely. If the server crashes, fall back to file-based protocol (graceful degradation).

**Expected impact:** Eliminates 80% of git merge conflicts, removes TOCTOU races, reduces orchestrator token consumption by ~90% (push instead of poll), and enables true event-driven coordination.

---

### Tier 2: High Applicability

#### 4.2 Devin Playbooks (§15.11) — *Standardize agent initialization*

**Objective:** Codify each agent prompt (`agent_a_prompt.md` through `agent_d_prompt.md`) as a version-controlled Devin Playbook, eliminating manual copy-paste during session creation.

**Current state:** The user manually creates four Devin sessions and pastes prompts. This is error-prone and non-reproducible.

**Target state:** One command per role: `devin playbook run orchestrator`, `devin playbook run executor`, etc.

**Implementation steps:**

1. **Immediate (pre-Cycle 2):** Convert the four `agent_<role>_prompt.md` files into Devin Playbook YAML/JSON format.
2. **Include in playbook:** First-time setup commands (`git pull`, `cd <project>`), role-specific rules, and the canonical polling command.
3. **Parameterize:** Replace the hard-coded Windows path with an environment variable or playbook argument (`PROJECT_ROOT`). The playbook should detect or receive the project directory at runtime.
4. **Version-lock:** Tag playbook versions to match blackboard schema versions. If `TASKS.json` schema changes, the playbook version increments.

**Expected impact:** Removes human error in session setup, ensures all agents load identical protocol versions, and makes the workflow reproducible across machines and team members.

---

#### 4.3 Devin Session Linking / Management (§15.14) — *Reduce human coordination burden*

**Objective:** Link the four Devin sessions so that completion signals can propagate without human relay.

**Current state:** When Agent B completes a task, the human must tell Agent C to poll. The orchestrator's background watcher detects the change, but the human is still the transport layer to C and D.

**Target state:** Session linking allows the orchestrator to send a lightweight "wake" signal to Agent C's session when B's task moves to `done`. Agent C then polls once autonomously.

**Implementation steps:**

1. **Phase 3, Cycle 1:** Investigate Devin Session Management API for `session.notify()` or equivalent cross-session messaging.
2. **If native linking unavailable:** Use the file-based `SIGNAL.json` approach (Meta02's suggestion) as a fallback: orchestrator writes `{"wake": ["C"], "reason": "F7-T2 done"}`; Agent C's session has a lightweight file watcher (not an agent loop) that triggers a single poll.
3. **Preserve human override:** Even with linked sessions, require an `AUTO_MODE=false` flag in `STATE.md` that forces human-mediated coordination for complex or risky cycles.

**Expected impact:** Reduces human coordination overhead by ~60% while preserving the safety checkpoint for non-routine tasks.

---

#### 4.4 Devin Dynamic Workflows (§15.15) — *Declarative cycle definition*

**Objective:** Encode the executor → challenger → critic → next cycle progression as a declarative workflow, replacing imperative polling and human nudging.

**Current state:** Cycle progression is implicit in `TASKS.json` `depends_on` fields and enforced by the orchestrator writing new tasks.

**Target state:** A declarative workflow definition that the Devin platform executes:

```yaml
workflow: stock_ingestion_phase2
cycle_template:
  - id: executor
    role: B
    model: swe
    gate: acceptance_criteria_defined
  - id: challenger
    role: C
    model: glm
    depends_on: [executor]
    gate: review_complete
  - id: critic
    role: D
    model: swe
    depends_on: [executor, challenger]
    gate: all_acceptance_criteria_verified
on_blocked:
  action: notify_orchestrator
  escalate_after: 2
```

**Implementation steps:**

1. **Phase 3, Cycle 2:** After MCP server and Playbooks are stable, prototype one Dynamic Workflow for a single cycle (e.g., Cycle 3: Query Timeout).
2. **Validate:** Ensure the workflow correctly handles blocker escalation (task moves to `blocked` → orchestrator notified → human adjudicates).
3. **Migrate:** Convert all remaining Phase 2 cycles to declarative workflows.

**Expected impact:** Eliminates manual task writing by the orchestrator for routine cycles, enforces the dependency chain natively, and gates completion on critic approval automatically.

**Dependency:** This should be implemented *after* MCP and Playbooks. Dynamic Workflows require a stable state-management layer.

---

### Tier 3: Medium Applicability

#### 4.5 Devin Schedules (§15.12) — *Automated routine polling*

**Objective:** Schedule agents B/C/D to automatically poll the blackboard during active development hours, removing the "user tells agent to poll" step for routine tasks.

**Current state:** Human must explicitly tell each agent when to poll.

**Target state:** Agent B's session is scheduled to run `python scripts/poll_tasks.py B` every 5 minutes during 09:00–18:00. If a READY task exists, the agent claims and executes it; if not, it logs "No tasks ready" and exits.

**Implementation steps:**

1. **Phase 3, Cycle 1:** Set up a Devin Schedule for Agent B (Executor) only, as it is the most predictable workload.
2. **Configure:** Schedule should run the Playbook (see 4.2 above) with a `--poll` flag. The agent must follow the existing "poll once, execute or stop" protocol — it should NOT enter a loop.
3. **Safety guardrails:**
   - Max 1 execution per scheduled run (claim → execute → mark done → stop).
   - If task is blocked, schedule logs to `EVENTS.log` and stops — does NOT retry.
   - Orchestrator retains ability to pause schedules via `STATE.md` flag.
4. **Evaluate:** Run for one cycle. Measure token cost vs. human-nudged execution. If cost increase is >20%, defer schedules until Dynamic Workflows are in place.
5. **Expand:** If successful, add schedules for C and D.

**Expected impact:** Reduces human coordination overhead for routine executor tasks. However, schedules introduce a new risk: an agent running autonomously without a human in the loop could consume significant tokens on a misconfigured task. The "Medium" applicability rating from Gemini is correct — this is useful but not critical, and it should be deployed cautiously.

**Dependency:** Should be implemented *after* Playbooks (so scheduled sessions load the correct prompt) and ideally after MCP (so scheduled polling is cheap). If implemented on the current file-based protocol, each scheduled poll still costs a full context window read of `TASKS.json` + `STATE.md` + `EVENTS.log`.

