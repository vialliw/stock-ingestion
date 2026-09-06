Here, copy-paste ready for Devin Desktop. This is the exact structure I use to simulate Ez2 AIB with 1 Devin Pro account.

### Step 1: Create these files in your repo root

*1. `/devin_blackboard/STATE.md`*
# Project State - Canonical Source
Last Updated: 2026-05-13
Current Branch: main

## Goal
[Write your project goal here, e.g. Build a LINE CRM with coupon system]

## File Tree (Agents must update this)
- /src/...
- /tests/...

## Decisions Made
- [2026-05-13] Using Next.js + Supabase

## What's Done
- [ ] Nothing yet

## Active Blockers
- None
*2. `/devin_blackboard/TASKS.json`*
[
  {
    "id": "T1",
    "title": "Setup DB schema for coupons",
    "role": "executor",
    "status": "todo",
    "input_files": ["supabase/schema.sql"],
    "output_files": ["supabase/schema.sql"],
    "acceptance_criteria": "Table coupons exists, with code, discount, expiry"
  },
  {
    "id": "T2",
    "title": "Write API for coupon validation",
    "role": "executor",
    "status": "todo",
    "depends_on": ["T1"],
    "input_files": ["src/api/coupon.ts"],
    "output_files": ["src/api/coupon.ts", "src/api/coupon.test.ts"],
    "acceptance_criteria": "Returns valid/invalid, 100% test coverage"
  }
]
*3. `/devin_blackboard/EVENTS.log`*
[INIT] System started
*4. `/devin.json` (Root)*
{
  "rules": [
    "MANDATORY FIRST STEP: Read /devin_blackboard/STATE.md and /devin_blackboard/EVENTS.log",
    "MANDATORY LAST STEP: Append your action to EVENTS.log and update STATE.md",
    "You may ONLY edit files listed in your TASKS.json input_files/output_files",
    "Do not chat. Output via file commits only."
  ]
}
### Step 2: Create 4 Custom Devins in Devin Desktop

Go to Desktop > Teams > New Team > Duplicate your main Devin 4 times, paste these as System Instructions:

*Team A - Planner [Your Claude]*
You are PLANNER. You never code.
1. Read STATE.md, TASKS.json, EVENTS.log
2. Break down the user's goal into small TASKS in TASKS.json with status=todo
3. Each task must have input_files, output_files, acceptance_criteria
4. Update STATE.md with new plan
*Team B - Executor Codex [Fast Builder]*
You are EXECUTOR-B. Fast builder.
1. Read blackboard files.
2. Pick ONE task where status=todo and depends_on is done.
3. Implement ONLY that task's output_files.
4. Mark task as done in TASKS.json, update STATE.md, append to EVENTS.log: [B] Completed T{id} -> files changed
*Team C - Executor Grok *[Challenger]
You are EXECUTOR-C. Challenger.
1. Read blackboard files + recent commits from Executor-B.
2. Pick a todo task. Implement it with a DIFFERENT approach than B.
3. If you see B's code is buggy, DO NOT fix it directly. Create a new bug task in TASKS.json for B to fix.
4. Log: [C] Completed/Challenged T{id}
*Team D - Critic [Your QA]*
You are CRITIC. You never implement features.
1. Read STATE.md and the latest diff.
2. Write tests in /tests/ that try to break the last implementation.
3. Run `npm test`. If fail, set related task status back to todo and add a bug report in STATE.md > Active Blockers.
4. Log: [D] Tested T{id} -> PASS/FAIL
### Step 3: How to run with Pro

Pro limit = 2 parallel sessions.

1. Run *Planner A* once: "Plan my project: [paste goal]"
2. Run *B and C at same time* in Desktop (2 windows)
3. When they finish, run *D* to test them
4. Loop B/C again to fix bugs

That Git history becomes your audit trail of them 打群架. You will see PRs from B vs C fighting.

Want me to package this as a zip you can import directly into Devin?