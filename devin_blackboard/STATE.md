# Project State — Canonical Source
Last Updated: 2026-09-06
Current Branch: main

## Goal
Centralize stock-data ingestion and provide program interfaces.
Storage backend: Cloudflare R2 (S3-compatible).

## File Tree (Agents MUST update this after every change)
- /AGENTS.md
- /.devin/agents/planner.md
- /.devin/agents/executor.md
- /.devin/agents/challenger.md
- /.devin/agents/critic.md
- /devin_blackboard/STATE.md
- /devin_blackboard/TASKS.json
- /devin_blackboard/EVENTS.log
- /docs/workflow_idea/procedure_multi_agents01.md
- /src/            (ingestion + interface code — not yet created)
- /tests/          (critic-written tests — not yet created)
- /config/         (runtime config, non-secret — not yet created)

## Decisions Made
- [2026-09-06] Multi-agent workflow adopted (see docs/workflow_idea/procedure_multi_agents01.md)
- [2026-09-06] R2 as object storage; secrets via GitHub repo secrets, never in repo
- [2026-09-06] Using Devin native models: glm (GLM-5.2 High) + swe (SWE-1.7 Medium) — free in Devin Local
- [2026-09-06] OpenRouter not needed; all 4 roles use Devin native models

## What's Done
- [x] Blackboard scaffolding
- [x] Custom subagent profiles created (planner, executor, challenger, critic)
- [x] AGENTS.md project rules committed
- [ ] Cart test cycle (see procedure §6)

## Active Blockers
- None
