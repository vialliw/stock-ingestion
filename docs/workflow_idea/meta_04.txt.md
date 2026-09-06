Sandbox blocked zip creation (only allows data packages), but I have everything ready - just copy this structure into a new GitHub repo and import to Devin Desktop:

*Your Free Model Mapping:*
- `Claude Fable 5.1` (Planner) → *GLM-5.2* - long context, cheap, great at planning
- `Codex Astra 6` (Executor) → *SWE-1.7* - open SWE-Bench SOTA, best free coder
- `Grok 4.6` (Challenger) → *GLM-5.2* - use its aggressive reasoning to attack B's code
- `Gemini 3.8` → *GLM-5.2* (same)
- Critic → *SWE-1.7* (test generation)

*Files to create:*

1. `/devin_blackboard/STATE.md` - paste the template I sent above
2. `/devin_blackboard/TASKS.json` - with `"model": "SWE-1.7"` or `"GLM-5.2"` field
3. `/devin_blackboard/EVENTS.log` - `[INIT] Free Team`
4. `/devin.json` - with rules: read STATE first, write EVENTS last
5. `/teams/` - 4 markdown files = your 4 system prompts for Devin Desktop

*For Devin Desktop to use free models:*
Since Devin Pro defaults to its own model, you need to tell each Team to call the free model via API:

In Team B/C prompt, add:
When you need to code, use tool: `curl https://api.swe17.dev/v1/chat -d ...` 
Or if you have OpenRouter key, set model to `swe-1.7` and `glm-5.2`.
In Devin Desktop > Integrations > Custom Model > Add OpenRouter > swe-1.7 and glm-5.2 are free there.
This way Devin becomes just the orchestrator shell, but the brain inside each session is SWE-1.7 / GLM-5.2 via OpenRouter (both $0 on OpenRouter free tier).

Want me to write you the exact OpenRouter integration command to paste into Devin Desktop so Team B actually uses SWE-1.7 and Team C uses GLM-5.2?