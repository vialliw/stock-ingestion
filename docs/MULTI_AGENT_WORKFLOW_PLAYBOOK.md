# Multi-Agent Workflow Playbook

**Purpose:** replicate this project's multi-agent development workflow in any
other repository. Everything below is domain-agnostic — no stock/data
specifics. Copy the listed files, adjust paths, and the workflow runs
identically.

**Model:** one Orchestrator (the main interactive agent session, "Agent A")
coordinates three role subagents — Executor (B), Challenger (C), Critic (D) —
through a file-based blackboard committed to git. Sequential B → C → D per
task: implement → adversarial review → independent acceptance verification.

---

## 1. Roles

| Agent | Role | Spawned by | May edit | Responsibility |
|---|---|---|---|---|
| A | Orchestrator / PM | the main session (not spawned) | blackboard files, docs, task scripts | Plans, registers tasks, spawns B/C/D, reviews evidence, resolves blockers, deploys |
| B | Executor | `run_subagent` (write-capable profile) | only the task's `output_files` + blackboard | Implements code + tests |
| C | Challenger | `run_subagent` (write-capable) | blackboard only | Independent adversarial review — false positives/negatives, edge cases, vacuous tests, fidelity diffs |
| D | Critic | `run_subagent` (write-capable) | blackboard (+ `tests/` if desired) | Final gate — re-runs EVERY acceptance criterion with concrete evidence |
| — | Watcher (optional) | read-only subagent profile | nothing | Observe TASKS.json state; cannot claim |

**Separation is the point:** C and D must not trust B's self-report. D's job is
evidence — command output, file:line citations — not "looks done".

## 2. Directory skeleton to copy

```
<repo>/
├── AGENTS.md                          # project rules (template in §9)
├── devin_blackboard/
│   ├── TASKS.json                     # task DAG — see §4 schema
│   ├── STATE.md                       # canonical project state + per-task summaries
│   ├── EVENTS.log                     # append-only event log (the audit trail)
│   └── locks/                         # atomic claim lock files (<task>.lock)
├── scripts/
│   ├── poll_tasks.py                  # task lifecycle CLI — copy verbatim
│   ├── check_loops.py                 # optional quality gate (see §8)
│   └── pre_commit_check_loops.py      # optional — wired into .git/hooks/pre-commit
├── .devin/agents/
│   ├── executor.md                    # thin profile → points to role prompt
│   ├── challenger.md
│   ├── critic.md
│   └── planner.md                     # optional — task-DAG authoring role
└── docs/workflow_idea/
    ├── agent_b_prompt.md              # canonical role prompts (single source
    ├── agent_c_prompt.md              #   of truth — the .devin profiles only
    ├── agent_d_prompt.md              #   point at these)
    └── polling_workflow_protocol.md   # this protocol, written down
```

Files that are **100% reusable as-is**: `scripts/poll_tasks.py`,
`scripts/check_loops.py`, `scripts/pre_commit_check_loops.py`, the
`.devin/agents/*.md` profiles, and the role prompts (only the project path and
project-specific rules inside them need edits).

## 3. Blackboard contract

### TASKS.json — the task DAG

A JSON array. Each task:

```json
{
  "id": "F1-T1",
  "title": "short imperative title",
  "role": "executor",
  "model": "swe",
  "status": "todo",
  "depends_on": ["F0-T3"],
  "input_files": ["/src/x.py", "path/to/legacy/reference.py"],
  "output_files": ["/src/x.py", "/tests/test_x.py", "/README.md"],
  "acceptance_criteria": "<prose string OR structured list>",
  "assigned_to": "B"
}
```

- `status` lifecycle: **`todo` → `in_progress` → `done` / `blocked`**.
  ⚠ The claim command requires exactly `todo` — don't register with
  `pending` (this project hit that twice).
- `depends_on` gates execution: a task is READY only when all deps are `done`.
- `input_files` / `output_files` enforce scope discipline: the executor may
  only edit `output_files` (+ blackboard). Reviewers verify commit scope
  against this list.
- `acceptance_criteria` has two forms:
  - **Structured list** — `[{id, check, command, expected_exit}]`; the
    `--verify` / `--done` commands execute each `command` and compare exit
    codes automatically, producing a JSON evidence map.
  - **Prose string** — numbered criteria; C/D verify manually with file:line
    evidence. Use prose for "port this logic verbatim" style work, structured
    commands for mechanical gates (tests pass, linters clean).

### STATE.md — canonical state

Sections this project converged on: `Active Blocker`, `Goal`, `Completed`
(one bullet per closed task, with the *why* and verification evidence),
per-task `## <task-id>` sections written by the executor then extended by the
orchestrator with C/D verdicts and deployment notes.

### EVENTS.log — append-only audit trail

Format: `[<ISO-8601 UTC>] [<ROLE>] <what happened>` — one line per event:
`claimed`, `completed <id> -> <evidence>`, `blocked <id> -> <reason>`,
`[SYSTEM] auto-recovered stale lock`, `[CHALLENGER] review APPROVED ...`,
`[CRITIC] verified PASS ...`, `[ORCHESTRATOR] registered/deployed ...`.

**Every agent's first step** is reading STATE.md + EVENTS.log; **last step**
is appending to EVENTS.log. This is the inter-agent communication channel —
agents never talk to each other directly.

### locks/ — atomic claims

`poll_tasks.py --claim` creates `<task>.lock` with `os.O_CREAT|os.O_EXCL`
(race-safe across processes). Locks older than 10 min are stale —
`--clean-stale` removes them and resets the task to `todo` (crash recovery).

## 4. Task lifecycle commands

All agents use the same script — **nobody writes ad-hoc polling scripts**:

```bash
python scripts/poll_tasks.py B                       # list READY/WAITING tasks for role
python scripts/poll_tasks.py --claim F1-T1 B         # atomic: lock + in_progress + event + git commit
python scripts/poll_tasks.py --verify F1-T1          # run structured ACs → evidence map (no status change)
python scripts/poll_tasks.py --done F1-T1 B --evidence "793 tests, lint clean"
python scripts/poll_tasks.py --blocked F1-T1 B --reason "upstream API changed"
python scripts/poll_tasks.py --clean-stale           # recover locks >10 min old
```

`--done` re-runs structured ACs internally and **refuses to close on
failure**. Every mutating command also commits (and pushes) the blackboard —
state is durable even if a session dies mid-task.

## 5. The run loop (what the Orchestrator does)

```
1. PLAN     → user-approved plan; define task id, files, ACs
2. REGISTER → append task to TASKS.json (status "todo"), EVENTS entry, commit
3. SPAWN B  → run_subagent(profile=write-capable, task=<detailed prompt>)
              The prompt names the task id, quotes key requirements, points at
              input_files, and states verification commands. B claims, edits
              only output_files, runs --verify, --done, returns a report.
4. REVIEW   → read B's report + diff; spawn C with an adversarial-review
              prompt (diff ported code vs source, hunt edge cases, check the
              tests actually assert things). C returns APPROVED/BLOCKED.
5. FIX LOOP → blocking findings → resume/spawn B with findings; repeat C.
6. SPAWN D  → final verification prompt: re-run every AC, demand file:line
              evidence, run the gates, check blackboard/commit hygiene.
7. DEPLOY   → orchestrator-only: restarts services, runs live smoke tests,
              verifies real artifacts. Agents never deploy.
8. CLOSE    → backfill any missing blackboard entries (see §7 lessons),
              update STATE.md, commit, report to user.
```

Subagents are **spawned per task** and do a single pass (poll → claim → work →
done → report) — no background polling loops. The orchestrator is the loop.

## 6. Spawn-prompt pattern

Every spawn prompt carries the same skeleton (subagents are stateless — they
see nothing of your session):

1. Identity + repo path + "read STATE.md/EVENTS.log/TASKS.json first".
2. The exact task id to claim via `poll_tasks.py --claim`.
3. Scope: `output_files` only (+ blackboard).
4. Requirements — paste the essentials verbatim; subagents can't see the plan.
5. Role-specific duty: B → implement + verify + done; C → independent review,
   no code edits; D → re-run all ACs with evidence.
6. Boundaries: no live-service restarts, no deploys — orchestrator does those.
7. Report format expected back.

## 7. Lessons learned (bake these into your copy)

- **Status token is `todo`, not `pending`** — register with `todo` or claims
  fail.
- **Reviewers may not log their verdict** — happened on three consecutive
  tasks here; the orchestrator backfills `[CHALLENGER]`/`[CRITIC]` entries
  into EVENTS.log at cycle close. D's prompt now explicitly requires checking
  for this.
- **Never mark done on "tests pass"** — the completion rule is *evidence
  against the acceptance criteria*, verified by an agent that didn't write
  the code.
- **Live smoke catches what unit tests can't** — run a real end-to-end check
  after the C/D verdict (this project's live smoke caught a dtype bug all
  787 tests missed; later smokes caught an unguarded `LN` crashing on newly
  loaded data, a stale running server, and an API retention boundary no spec
  predicted). Deployment is an orchestrator duty, after the cycle.
- **Single pass, then report** — subagents must not background-poll; the
  orchestrator re-spawns them. Polling loops inside subagents just burn
  tokens and queue messages.
- **Commit-scope audits work** — C/D check `git show --stat` against
  `output_files`; scope creep gets caught mechanically.
- **Structured vs prose ACs** — structured `{check, command, expected_exit}`
  lists get automatic `--verify` execution; prose gets manual evidence. A mix
  is fine; decide per task.
- **Keep the blackboard in git** — commits are the durability + ordering
  mechanism; every state mutation is a commit.
- **Shell hygiene for agents** — `python -c` strings with backticks or
  `>`-adjacent quoting produced stray files (`=`, `nul`); `2>nul` under
  Git Bash writes a literal file named `nul` that then broke the poll
  script's `git add -A` commit. Have agents write helper scripts to
  `%TEMP%`/tmp `.py` files instead of heavy inline `-c` quoting, and have
  reviewers check `git status` for strays.
- **Worktree sweeps** — the poll script's `git add -A` sweeps pre-existing
  uncommitted changes into the cycle's commits (a doc rename rode in on an
  executor commit here). Start each task on a clean tree and have reviewers
  audit `git show --stat` for out-of-scope files.
- **TASKS.json schema drift** — an entry registered with the wrong schema
  (`task_id` vs `id`, missing `assigned_to`, `pending` vs `todo`) crashed
  `find_task`/`cmd_claim` mid-cycle; the executor had to normalize it.
  Copy a recent entry verbatim when registering.
- **Match reviewer profile to the job** — the read-only explore profile
  can't run pytest/git; challengers and critics that must execute probes
  need a write-capable profile (`subagent_general`), or log the limitation
  of a static-only review.
- **Route moderate findings back through the loop** — a challenger flagged
  a residual correctness bug post-approval; the fix pattern is a narrow
  follow-up task through the same B→C→D cycle (here: F23-T2), not an
  out-of-band patch.

## 8. Optional quality gate — loop checker

`check_loops.py` (AST scan for unjustified `for` loops in `src/`) +
`.git/hooks/pre-commit` wrapper `pre_commit_check_loops.py`. Enforces the
project's "SQL > vectorized > loop" performance rule and is on the Critic's
mandatory checklist. Drop it if your project doesn't need that rule — the
rest of the workflow is independent of it.

## 9. AGENTS.md — minimal template

```markdown
# Project Rules — <project> (Multi-Agent Blackboard Protocol)

## Blackboard protocol (MANDATORY)
1. FIRST STEP: read devin_blackboard/STATE.md and EVENTS.log.
2. LAST STEP: append your action to EVENTS.log and update STATE.md.
3. Edit ONLY files listed in your task's output_files (+ blackboard).
4. No inter-agent chat — communicate via commits + EVENTS.log.
5. Never commit secrets.
6. If a task seems wrong: file a blocker in STATE.md and stop.

## Orchestrator role
The main session is the Orchestrator (PM), NOT a coding agent. It registers
tasks, spawns B/C/D as local subagents, reviews evidence, resolves blockers,
and deploys. It MUST NOT edit src/ or tests/ — only blackboard files, docs,
and task scripts.

## Spawn failures
If a required subagent cannot be spawned: STOP, record the blocker, ask the
user. Exception — the user must say exactly: "orchestrator may code" /
"orchestrator may implement" / "main devin may code" / "override role
boundaries".

## Completion rule
Never mark complete on "it runs" / "tests pass" alone. Critic D must verify
every acceptance_criteria item with concrete evidence before status=done.

## Coding conventions
<your project's test command, lint command, style rules>
```

## 10. Variants implemented here

- **Local subagents (default)** — `run_subagent` with the write-capable
  profile; shares the orchestrator's workspace; zero extra quota.
- **Devin Cloud sessions (optional)** — `devin_session_create` can run agents
  remotely when org quota is available; same blackboard contract.
- **Dynamic Workflows (optional)** — `scripts/workflow_phase2.py` wraps the
  B→C→D pipeline in declarative, resumable orchestration with structured
  results piped between stages. Sequential by design so D sees C's findings.
- **Planner profile** — `.devin/agents/planner.md` (Agent A variant) can
  author the TASKS.json DAG itself; write-limited to blackboard only.

## Quick start checklist for a new repo

1. Copy `scripts/poll_tasks.py` (+ `check_loops.py`, `pre_commit_check_loops.py`
   if wanted) and `devin_blackboard/` skeleton (`TASKS.json` = `[]`, empty
   STATE.md/EVENTS.log, empty `locks/`).
2. Copy `.devin/agents/*.md` and `docs/workflow_idea/agent_*_prompt.md`;
   edit only the project path/name inside.
3. Write `AGENTS.md` from §9 with your conventions.
4. Install the pre-commit hook if using the loop gate.
5. In your first orchestrator session: register tasks (`status: "todo"`),
   spawn B, run the §5 loop.
