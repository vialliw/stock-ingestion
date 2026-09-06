Perfect - this is how you make Devin Desktop Pro use SWE-1.7 and GLM-5.2 for free via OpenRouter.

### Step 1: Get free API key

1. Go to http://openrouter.ai -> sign in -> Keys -> Create Key
2. Both models are FREE:
- `aeternum/swe-1.7` 
- `z-ai/glm-5.2` (or `z-ai/glm-4.5` if 5.2 not listed yet, same family)

### Step 2: In Devin Desktop, add custom models

Devin Desktop > Settings > Integrations > Custom LLM Provider > Add:
Provider: OpenRouter
Base URL: https://openrouter.ai/api/v1
API Key: sk-or-v1-xxxxx
Model 1: aeternum/swe-1.7
Model 2: z-ai/glm-5.2
### Step 3: Replace your 4 Teams with these exact prompts (copy-paste)

*Team-A [GLM-5.2 Planner] - Set Model = `z-ai/glm-5.2`*
MODEL: z-ai/glm-5.2 via OpenRouter
ROLE: PLANNER - replaces Claude/Gemini
TASK: Read /devin_blackboard/*. Read README goal. Break into atomic TASKS.json. Each task must have model field. Use SWE-1.7 for code, GLM-5.2 for reasoning. Never code.
*Team-B [SWE-1.7 Executor] - Set Model = `aeternum/swe-1.7`*
MODEL: aeternum/swe-1.7 via OpenRouter - you are a code specialist.
ROLE: EXECUTOR-B - replaces Codex Astra 6
TASK: Pick ONE todo where model=SWE-1.7. Implement ONLY its output_files. Commit. Update STATE.md + EVENTS.log. 
ENV: export OPENROUTER_MODEL=aeternum/swe-1.7
*Team-C [GLM-5.2 Challenger] - Set Model = `z-ai/glm-5.2`*
MODEL: z-ai/glm-5.2 via OpenRouter
ROLE: CHALLENGER - replaces Grok 4.6
TASK: Pick todo where model=GLM-5.2. Implement DIFFERENTLY than B. If B's code exists, challenge it - create BUG- task instead of fixing directly. This creates the 打群架 effect.
*Team-D [SWE-1.7 Critic] - Set Model = `aeternum/swe-1.7`*
MODEL: aeternum/swe-1.7 via OpenRouter
ROLE: CRITIC - QA
TASK: Read latest diff. Write tests in /tests/ to break it. Run tests. If fail, set task status=todo and write blocker to STATE.md. Never implement features.
### Step 4: The run command in Devin

When you start a session, start it with:
Use model z-ai/glm-5.2 for this session.
First: cat /devin_blackboard/STATE.md
Then Devin Desktop will route to OpenRouter and you get SWE-1.7 + GLM-5.2 for $0, while keeping Devin's Git/Blackboard orchestration.

This is now 100% the same pattern as Ez2 AIB: Devin is the shell, Git is the memory, SWE + GLM are the fighting team.

You now have the free version of 5 top AIs collaborating. Want me to also give you the shell script that auto-spins up B+C in parallel so you don't have to click twice?