# Multi-Agent Workflow Procedure — stock_ingestion

> Derived from `meta_01.txt.md` … `meta_06.txt.md` (Meta advice on the Ez2 AIB "打群架" pattern),
> adapted to use **Devin CLI native models** and **custom subagent profiles**.

> Goal: Centralize stock-data ingestion and expose program interfaces, using a multi-agent attack/defense loop instead of a single agent.

This document is the **single procedure we follow to establish the workflow first**, before any feature code is written. Once the workflow below is set up and verified on one trivial task, we use it to build the real ingestion features.

---

## 0. Core ideas (why we do this)

| Problem from Meta advice | Solution we adopt |
|---|---|
| No shared ground truth — `A做完的事，B不知道` | **Blackboard**: `STATE.md` + `TASKS.json` + `EVENTS.log` in repo |
| Heterogeneous capabilities fight | **Role specialization**: Planner / Executor-B / Challenger-C / Critic-D |
| Communication protocol drift | **Strict JSON contract** for tasks and results; no free-form agent chat |
| Need the attack/defense loop | **Generator → Critic → Generator** is the feature, not a bug |
| Cost & context explosion | **Context slicing**: each agent sees only its task + relevant files + last 3 events |
| Need it free | **Devin CLI native models**: `glm` (GLM-5.2 High) and `swe` (SWE-1.7 Medium) are free in Devin Local |

### Key shift from the original Meta advice

The Meta advice (meta_02–meta_05) suggested OpenRouter with `aeternum/swe-1.7` and `z-ai/glm-5.2`. That model ID does not exist on OpenRouter, and Devin CLI does not support custom LLM providers. Instead, Devin CLI provides these models natively and for free:

- **`glm`** short name → resolves to latest GLM (GLM-5.2), thinking level **High**
- **`swe`** short name → resolves to latest SWE (SWE-1.7), thinking level **Medium**

Devin CLI's built-in **custom subagent profiles** (`.devin/agents/*.md`) pin a `model:` per role and run as background subagents. This replaces the manual "open 2 Devin Desktop windows" approach from the Meta advice.

### OpenRouter not needed

OpenRouter models (e.g. `cohere/north-mini-code:free`, `nvidia/nemotron-3.5-lightning:free`) **cannot** be used as subagent brains in Devin CLI — the `model:` field only accepts Devin native model names. All 4 roles use Devin native models. The `API_KEY_SWE_17` and `API_KEY_GLM_52` GitHub secrets are unused for this workflow.

---

## 1. Prerequisites (do once, before any agent runs)

1. Repo exists on GitHub: `vialliw/stock-ingestion` (done).
2. Local clone present at `C:\Users\user\stock-ingestion` (done).
3. R2 secrets stored as **GitHub repo secrets** (verified working via `verify_secrets.yml` workflow):
   - `R2_ACCESS_KEY_ID`
   - `R2_ENDPOINT_URL`
   - `R2_SECRET_ACCESS_KEY`
4. Devin CLI authenticated: `devin auth login` (done). Verify models with `devin models list` — confirm `glm` and `swe` families are visible.

---

## 2. The Blackboard (shared state in the repo)

Every agent reads and writes these files. They are committed to git — git history is the audit trail.

### 2.1 `devin_blackboard/STATE.md`

Canonical source of truth: project goal, file tree, decisions, what's done, active blockers.

### 2.2 `devin_blackboard/TASKS.json`

DAG of tasks. Schema (every task MUST have all fields):

```json
{
  "id": "T1",
  "title": "Short imperative title",
  "role": "executor",
  "model": "swe",
  "status": "todo",
  "depends_on": [],
  "input_files": ["path/that/may/be/read"],
  "output_files": ["path/that/will/be/written"],
  "acceptance_criteria": "Measurable, testable condition",
  "assigned_to": null
}
```

Model field values: `"swe"` for code/test tasks, `"glm"` for reasoning/review tasks.

Status values: `todo` → `in_progress` → `done` → `blocked` → (back to `todo` if Critic fails it).

### 2.3 `devin_blackboard/EVENTS.log`

Append-only. One line per agent action:

```
[INIT] Blackboard created
[<ISO ts>] [<Agent-Role>] <verb> <Tid> -> <files changed | PASS | FAIL | BUG-Tid>
```

### 2.4 `AGENTS.md` (repo root)

Project rules that every Devin session auto-loads. Contains the 5 blackboard protocol rules, project context, and coding conventions. (This replaces the `devin.json` format from the Meta advice — Devin CLI uses `AGENTS.md` for project rules.)

---

## 3. The 4 agent roles (custom subagent profiles)

Defined in `.devin/agents/*.md` with YAML frontmatter. Each pins a `model:` and restricts `allowed-tools:`.

| Role | File | Model | Thinking | Tools | Job |
|---|---|---|---|---|---|
| A — Planner | `.devin/agents/planner.md` | `glm` | High | read, grep, glob, edit, exec | Break goal into DAG in TASKS.json. Never codes. |
| B — Executor | `.devin/agents/executor.md` | `swe` | Medium | read, grep, glob, edit, write, exec | Pick ONE todo, implement only its output_files. |
| C — Challenger | `.devin/agents/challenger.md` | `glm` | High | read, grep, glob, edit, write, exec | Implement differently than B. File BUG reports, don't fix. |
| D — Critic | `.devin/agents/critic.md` | `swe` | Medium | read, grep, glob, edit, write, exec | Write breaking tests in /tests/. Run tests. Fail tasks back to todo. |

### Why C reuses A's model and D reuses B's model

Devin CLI doesn't support OpenRouter models as subagent brains. GLM-5.2 High and SWE-1.7 Medium are free in Devin Local. Role differentiation comes from the **system prompts** (the body of each `.devin/agents/*.md` file), not from the model. This gives us the attack/defense loop at zero cost.

### Thinking levels

Thinking levels are set per session via `Alt+T` (macOS: `Opt+T`). For subagents, the thinking level is determined by the model ID if it includes a level suffix (e.g. `glm-5-2-high`), or by the session default. The short names `glm` and `swe` resolve to the latest version with default thinking — set the level in the main session before spawning subagents if needed.

---

## 4. The run loop (repeat per cycle)

One cycle = Plan → Execute (parallel) → Critic → Fix.

The **main Devin session is the Orchestrator**. It spawns the 4 role agents as custom subagents.

```
                    Main Devin Session (Orchestrator)
                            │
            ┌───────────────┼───────────────┐
            ▼                                │
     ┌────────────┐                          │
     │ A Planner  │  once per feature        │
     │ glm High   │  writes TASKS.json DAG   │
     └─────┬──────┘                          │
           ▼                                 │
     ┌────────────┐   ┌────────────┐         │
     │ B Executor │   │ C Challenger│  parallel background subagents
     │ swe Medium │   │ glm High   │  both pick different todo tasks
     └─────┬──────┘   └──────┬─────┘         │
           └───────┬────────┘                │
                   ▼                         │
            ┌────────────┐                   │
            │  D Critic  │  writes tests,    │
            │ swe Medium │  runs them        │
            └─────┬──────┘                   │
                  ▼                          │
         bugs? ──yes──► re-spawn B/C with BUG-T{id} tasks
                  no                         │
                  ▼                          │
         cycle complete; pick next feature with A
```

### 4.1 Step-by-step for one cycle

1. **Plan** — In the main Devin session, ask:
   > "Plan feature: <feature>. Use the planner subagent to write tasks to /devin_blackboard/TASKS.json."
   
   The Orchestrator spawns a `planner` subagent (foreground). Wait for it to commit the DAG.

2. **Execute in parallel** — In the main Devin session, ask:
   > "Spawn executor and challenger subagents in parallel. Executor: pick the first todo in TASKS.json. Challenger: pick the second todo. Both: read the blackboard first, implement, then update STATE.md and EVENTS.log."
   
   The Orchestrator spawns `executor` (swe) and `challenger` (glm) as **background subagents** — they run in parallel. Both commit to git. Conflicts are **expected and good** — they are the 打群架.

3. **Critic** — After both complete, ask:
   > "Spawn the critic subagent. Review the last 2 commits. Write breaking tests in /tests/."
   
   The Orchestrator spawns `critic` (swe) as a foreground subagent.

4. **Resolve** — If D produced BUG tasks, loop B/C on those BUG-T{id} tasks. If clean, the cycle is done.
5. **Advance** — Pick the next feature with A. Repeat.

### 4.2 Conflict resolution rule

When B and C edit the same file (intentional attack/defense):
- The **Orchestrator** (main session) resolves the merge conflict.
- B re-runs on the merged file and C re-challenges.
- Never let both edit the same file in the same cycle unless the task is explicitly a "duel" task.

---

## 5. Context slicing (cost control)

No agent ever sees the whole project. Each subagent prompt includes ONLY:
- Its role (inherited from the `.devin/agents/<role>.md` profile)
- The specific task object from `TASKS.json`
- The files listed in that task's `input_files` / `output_files`
- The last 3 lines of `EVENTS.log` (not full history)

The Planner is the only role allowed to see the full `TASKS.json` and `STATE.md`.

Subagents do not share conversation history with each other — the blackboard files are their only shared state. This is by design (per Devin CLI subagent docs: "operates in its own conversation chain -- it does not inherit the parent's conversation history").

---

## 6. Verification: the "cart" test before real work

Per Meta advice, do NOT start with the real ingestion feature. First run one full cycle on a **trivial but conflict-prone task** to expose sync bugs:

> Test feature: "A function `store_csv(symbol, date, csv_bytes)` that writes to R2 and returns the object key. Plus a test that re-reads it."

Acceptance: B and C both implement `store_csv`, D writes a test that overwrites the same key twice and checks the second wins. If B and C overwrite each other's STATE.md or EVENTS.log entries, the blackboard protocol is broken — fix the rules in `AGENTS.md` before proceeding.

Only after this trivial cycle passes end-to-end do we plan the real ingestion features.

---

## 7. stock_ingestion feature roadmap (planned, not yet built)

To be broken down by Planner A into TASKS.json. Outline only:

1. R2 client wrapper (uses R2_* secrets from GitHub repo secrets) — `src/r2_store.py`
2. Ingestion fetcher (data source adapters) — `src/fetcher.py`
3. Normalization + schema — `src/normalize.py`
4. Program interface (CLI + library API) — `src/interface.py`, `main.py`
5. Config loader (non-secret) — `config/config.yaml`
6. Critic test suite — `tests/`

Each becomes a DAG of atomic tasks with explicit `input_files` / `output_files` / `acceptance_criteria`.

---

## 8. Checklist to "establish the workflow first"

Use this as the gate before writing any feature code:

- [x] R2 secrets verified in GitHub repo secrets (via `verify_secrets.yml` workflow)
- [x] Devin CLI authenticated (`devin auth login`) and `glm` + `swe` models visible
- [x] `devin_blackboard/STATE.md` committed
- [x] `devin_blackboard/TASKS.json` committed (empty array `[]`)
- [x] `devin_blackboard/EVENTS.log` committed (`[INIT] Blackboard created`)
- [x] `AGENTS.md` committed with blackboard protocol rules
- [x] `.devin/agents/planner.md` committed (model: glm)
- [x] `.devin/agents/executor.md` committed (model: swe)
- [x] `.devin/agents/challenger.md` committed (model: glm)
- [x] `.devin/agents/critic.md` committed (model: swe)
- [x] Run the §6 "cart" test cycle end-to-end and confirm no blackboard corruption
- [x] Confirm `EVENTS.log` shows a clean `[A]→[B]→[C]→[D]` sequence

### Cart test results (2026-09-06)

- **Feature:** `store_csv(symbol, date, csv_bytes) -> int` writing to local DuckDB with APPEND semantics.
- **DAG:** T1 (scaffold) → T2 ∥ T3 (parallel implementations) → T4 (critic tests).
- **Result:** PASS — 22/22 tests passed (11 tests × 2 implementations). No BUG tasks filed.
- **Blackboard integrity:** Concurrent writes from T2 and T3 did not corrupt STATE.md, TASKS.json, or EVENTS.log. Git log is the reliable ordering source.
- **Caveats:**
  1. Timestamp anomaly in EVENTS.log (T2 timestamp out of order vs T1) — cosmetic, use git log for audit ordering.
  2. Weak duel — T2 and T3 produced near-identical implementations. The challenger's ≥50% performance improvement rule may explain this; real divergence will come on more complex tasks.
  3. Added `.gitignore` for `__pycache__/`, `*.duckdb`, `.pytest_cache/`, etc.

Once every box is checked, the workflow is established and we proceed to plan the real ingestion features with Planner A.

**✅ Workflow established. Ready for §7 feature roadmap.**
