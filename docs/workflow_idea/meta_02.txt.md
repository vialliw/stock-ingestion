With Devin Pro you can do it, you don't need 5 different companies yet. You can simulate the Ez2 AIB "打群架" pattern using *Devin Desktop as your Orchestrator + Blackboard*.

Devin Pro only gives you 1-3 concurrent Devins, but you can fake 5 roles by running them sequentially. The trick is: *Git is your Blackboard.*

Here is the setup that works:

### 1. Setup your Blackboard in your repo

In your repo root, create 3 files that every Devin MUST read/write:

`/devin_blackboard/`
- `STATE.md` - canonical source of truth. What's done, what's current file tree.
- `TASKS.json` - DAG of tasks
- `EVENTS.log` - append-only: `[timestamp] [Agent-Role] did X -> diff Y`

This solves your problem: `A做完的事，B不知道` -> B now reads `EVENTS.log`

### 2. Create 4 different Devins with different system prompts in Desktop

Devin Desktop > Settings > Custom Instructions > Create new Team.

Don't use the same Devin. Clone it 4 times with different brains:

*Devin-A: Planner (Claude role)*
You are ONLY the planner. You never write final code.
You read STATE.md and break task into small tasks in TASKS.json
Output format: JSON only.
*Devin-B: Executor-1 (Codex role)*
You are Executor. You pick ONE task from TASKS.json with status=todo.
You can only edit files listed in input_files.
After done, you MUST append to EVENTS.log and update STATE.md
You are forbidden to edit other files.
*Devin-C: Executor-2 (Different stack)*
Same as B but: `You prefer alternative implementation. If you see Executor-1's code, try a different library/approach.`

*Devin-D: Critic (Grok role)*
You are ONLY the critic. You never write code, you only write tests in /tests/ and file BUG reports in TASKS.json.
If tests fail, set task status back to todo.
### 3. The workflow loop in Devin Desktop

This is how you run it with Pro limit:

1. Start *Devin-A Planner* session: "Plan feature X, write to http://TASKS.json"
2. After it finishes, start *Devin-B* and *Devin-C* in parallel (Pro allows 2) - each picks a different TODO. This is your攻防.
3. Both will commit. Now Git has conflicts - that's GOOD.
4. Start *Devin-D Critic* - "Run tests, review diff from last 2 commits, create bug tasks"
5. Loop back to B/C to fix bugs.

> Pro tip: In Devin Desktop, check `Auto-create PR for each session`. Then your Blackboard is just the PR list. Devin-D's job is to review PRs from B and C.

### 4. Key Devin Desktop settings for this

- Enable *Knowledge Base* and upload your `STATE.md` as Knowledge so every new Devin auto-reads it
- Turn OFF `Auto context compaction` - you need them to keep http://EVENTS.log
- Use `devin.json` in repo:
{
  "rules": ["Always read /devin_blackboard/STATE.md first", "Always append to EVENTS.log", "Never edit outside your assigned task"]
}
With Pro, you can't run Claude + Codex + Gemini truly parallel like Ez2 AIB, but you can get 80% of the effect: *Git + http://STATE.md + Role prompts = multi-agent.*

You are essentially rebuilding Ez2 AIB's core inside Devin's workspace system.

Want me to write you a ready-to-use `devin_blackboard` template + the 4 system prompts for your Pro account?