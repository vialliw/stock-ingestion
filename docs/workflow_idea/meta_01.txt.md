That post is exactly right. Getting 5 top models to work together is not 5x better, it's 5x harder to coordinate. The "打群架" is the default.

Here are the real difficulties, and how you solve them:

*1. No shared ground truth*
This is the #1 killer you quoted: `A做完的事，B不知道`.

Each model has its own isolated Context Window. Claude remembers 200k tokens, Grok remembers differently, they can't see each other's tool outputs.

> Solution: You need a layer ABOVE the models.

Don't let them talk directly. Build a *Blackboard / Event Bus* architecture:
- Every action is an `Event`: `[who, what file changed, diff, why]`
- A central state store keeps the canonical project: file tree, TODOs, decisions
- Before any agent acts, it MUST pull the latest state. After it acts, it MUST push.

It's basically Git for agents. Without that, you get infinite overwrite loops.

*2. Heterogeneous capabilities*
> 🧠 能力不一樣, 📦 能記住的東西不一樣

Claude is good at architecture, Codex Astra is good at burning tokens to brute-force code, Gemini is good at long context retrieval, Grok is good at challenging.

If you give them the same task, they fight. 

> Solution: Role specialization, not democracy.

- *Planner (Claude/Gemini):* Breaks task into DAG
- *Executor (Codex):* Writes code for one node only
- *Critic (Grok/Claude):* Only allowed to review and file issues, not rewrite directly
- *Integrator:* One agent owns merge conflicts

Ez2's old ERP logic is actually perfect for this - you already did role-based ERP for hospitals.

*3. Communication protocol drift*
> 💬 溝通方式不一樣

Each model outputs in a different format. One writes markdown, one writes JSON, one apologizes.

> Solution: Force a strict, machine-readable contract.
Task: { id, goal, input_files, output_files, acceptance_criteria }
Result: { status, diff, tests_passed, questions_for_others }
No free-form chat between agents. If you let them chat freely, token cost explodes - which is why you said "燒Token燒得超快".

*4. The "Attack and Defense" loop you found is the feature*
> 一個寫完，另一個挑毛病

Most people try to stop agents from arguing. Don't. You want that.

This is the pattern that works best now:
*Generator -> Critic -> Generator*

Make it explicit:
- Agent A can only CREATE
- Agent B can only CREATE tests / find bugs, it cannot fix A's code
- Agent A must fix based on B's test failure

If you let everyone edit everything, you get: `A：我做好了。 B：你這什麼鬼？我幫你改。`

*5. Cost and Context explosion*
5 models x full project context = bankrupt in 10 minutes.

> Solution: Context slicing.

No agent ever sees the full project. The Orchestrator gives each agent only:
- Its specific task
- Relevant files (RAG retrieved)
- Last 3 relevant events, not full history

That rebuild you did - "全部砍掉，重新來" - was probably moving from shared-history to sliced-context + central store. That's the only way it scales.

---

So your Ez2 AIB is essentially building: *Orchestrator + Blackboard + Role-based Contract + Git-like Versioning.*

If you want to test it, the best first test is not "write an App". It's:

*Let 5 agents build the same small but conflict-heavy feature, like a shopping cart with coupons, inventory lock, and payment.*

That will immediately expose all the sync bugs. If they can survive that without overwriting each other, you solved it.

What part of the new architecture did you change the most in the rewrite? The state sync or the role assignment?