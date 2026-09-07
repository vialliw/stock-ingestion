Here is detailed feedback on **Section 15** - you asked exactly the right questions. This is where the workflow lives or dies in Phase 2.

### Core workflow questions (1-10)

**1. User as transport layer - file-based ping:**
Yes, file-based ping is better than manual chat. Don't do `PING_B.md` per agent - that's 3 more files to conflict. Do one file: `devin_blackboard/SIGNAL.json` = `{"wake": ["B","C"], "reason": "F7-T2 done"}`. Orchestrator writes it, B/C/D watch it. But the real fix is Devin Schedules (see your Q12) - user should not be router.

**2. Background watcher interval:**
60s is wrong for this workload. Your tasks take 8-20 min for `swe`/`glm` High. Polling at 60s just burns tokens. Set it to:
- Active cycle: 3-5 min
- Idle: 15 min
Make it configurable in `AGENTS.md` env: `POLL_INTERVAL_SEC=180`. And add backoff: if 3 polls = no READY tasks, double interval.

**3. Challenger value - merge with Critic?**
Keep C separate. You said C hasn't blocked any task - that's good data. C's value is not blocking, it's *preventing future tech debt*. Your example: C finding loop checker gaps and SQL optimization. D only checks "does it pass", C checks "will it scale".
Cost: `glm` is expensive though. Two options:
- Keep C but only run C on tasks tagged `needs_challenge=true` in TASKS.json (large/complex tasks)
- Or demote C to `swe` Medium and save High for A only. You get 70% of value for 30% cost.

My recommendation: Keep role, make it conditional.

**4. Sequential vs parallel C+D:**
Don't parallelize yet. Current chain `B -> C -> D` gives you a clean evidence trail: C finds concerns -> D verifies fix. If C and D run in parallel on same B output, they will duplicate work and you get conflicting verdicts in EVENTS.log.
Parallelize later: B completes -> fork C and D both read B's output, but D waits for C's EVENTS.log entry before final PASS. That's `depends_on: ["F7-T1", "F7-T1b"]` for the critic task.

**5. Lock file race:**
You diagnosed it correctly. `os.open(O_CREAT|O_EXCL)` works locally but not across git - lock file doesn't exist remotely until pushed. Git cannot do atomic locks.
Fix: **Status in TASKS.json IS the lock.** Change protocol to:

```python
# atomic claim attempt
pull
if task.status == "todo" and deps done:
   task.status = "in_progress"
   task.claimed_by = "B"
   commit + push
   if push fails -> someone else claimed first, abort
```

Delete `locks/` directory entirely. It gives false security.

**6. Git conflict handling:**
Yes, Orchestrator A must be merge authority. Document it:

- B/C/D: always `git pull --rebase` before commit. If rebase conflict on TASKS.json/EVENTS.log: `git rebase --abort`, pull again, re-apply change.
- Only A is allowed to `git push --force-with-lease` to resolve `STATE.md` conflicts
- Move from one `TASKS.json` to `devin_blackboard/tasks/F7-T2.json` - one file per task = 0 merge conflicts. Your poll script just reads directory.

**7. Task granularity:**
F7-T0 was too large. Rule of thumb you should add to §9: A task should be 1 PR = <300 lines changed = <30 min agent work. If acceptance criteria has 4 bullet points starting with "(1) (2) (3) (4)", split it. Your current F7-T2 "health_check Flight + audit table" is actually 2 tasks.

**8. Acceptance criteria verification:**
Current "paste evidence to EVENTS.log" is not machine-checkable. Add to TASKS.json schema:

```json
"acceptance_criteria": [
  {"id": "AC1", "text": "health_check Flight returns OK", "verified_by": null, "evidence": null}
]
```

Critic must fill `verified_by` + `evidence` path. Then `poll_tasks.py --verify F7-T2` can auto-check all AC have evidence.

**9. Model selection:**
Keep `swe` Medium for B - High would write better code but 2-3x cost, and you have C+D as safety net. That's the right tradeoff. For C, GLM-5.2 High is overkill if you make C conditional (Q3). Test `glm` vs `swe` for challenger on 2 tasks and compare findings - measure value per dollar.

**10. State recovery:**
10-min stale lock is too short for High thinking, too long for Medium. Replace with heartbeat. Agent updates `STATE.md` last_seen every 2 min: `[HEARTBEAT] B F7-T2 alive`. Orchestrator reclaims if no heartbeat for 15 min. Add explicit recovery command: `python scripts/recover.py --task F7-T2 --reason crash` -> sets status back to `todo` + logs event.

### Devin platform features (11-15) - this is your biggest leverage

**11. Devin Playbook:** Yes, use it. Codify each agent prompt as a Playbook v1. Then your "create 4 sessions" becomes 1 command. Playbook CAN reference `TASKS.json` - put `cat devin_blackboard/TASKS.json` in playbook step 1. This is the single best thing you can do to make this reproducible.

**12. Devin Schedules:** This directly solves Q1. Replace user nudging with:
- Schedule B/C/D: every 5 min during 09:00-18:00 BST, run `python scripts/poll_tasks.py <role>`
- Scheduled sessions CAN claim tasks and commit exactly like interactive ones.
- For "stop and report", scheduled session should NOT wait for user - it should log to EVENTS.log and exit. Interactive sessions wait, scheduled sessions proceed autonomously.

**13. Devin MCP:** This would eliminate 80% of your protocol. Ideal API:

```
get_next_task(role="B") -> task
claim_task(id) -> atomic, no lock file needed
report_event(type, msg)
complete_task(id, evidence)
subscribe(task_completed) -> push notification
```

If you can build a small MCP server wrapping the blackboard folder, you can delete `locks/`, `poll_tasks.py`, and the watcher. Highly recommended for Phase 3.

**14. Session Management:** Session linking / handoff would help. Best pattern: Don't link B->C automatically. Instead, when B completes, B writes `SIGNAL.json` with `next_role=C`. Platform watches SIGNAL and wakes C session. Context preservation: Yes, keep sessions warm. Your current "re-read AGENTS.md + STATE.md every poll" is wasteful if session is persistent. Add cache: if STATE.md hash unchanged, skip re-read.

**15. Dynamic Workflows:** This is the end-state. Declarative workflow:

```yaml
workflow: F7-Cycle
steps:
  - role: executor, depends_on: []
  - role: challenger, depends_on: [executor]
  - role: critic, depends_on: [challenger], gate: acceptance_criteria.all_verified
on_blocked: notify_orchestrator
```

This would auto-route based on `assigned_to` and handle `depends_on` natively. Acceptance gate is built-in. Start with Playbook + Schedules (Q11+12), then graduate to Dynamic Workflow when Cycle 3 is stable.

