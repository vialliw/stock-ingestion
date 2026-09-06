# Project Rules — stock_ingestion (Multi-Agent Blackboard Protocol)

## Blackboard protocol (MANDATORY for all agents)

1. MANDATORY FIRST STEP: Read /devin_blackboard/STATE.md and /devin_blackboard/EVENTS.log.
2. MANDATORY LAST STEP: Append your action to /devin_blackboard/EVENTS.log and update /devin_blackboard/STATE.md.
3. You may ONLY edit files listed in your task's output_files (plus the blackboard files themselves).
4. Do not chat with other agents. Communicate only via file commits and EVENTS.log.
5. Never commit secrets. R2 credentials come from GitHub repo secrets, not the repo.

## Project context

- Goal: Centralize stock-data ingestion and provide program interfaces.
- Storage backend: Cloudflare R2 (S3-compatible). Secrets: R2_ACCESS_KEY_ID, R2_ENDPOINT_URL, R2_SECRET_ACCESS_KEY (GitHub repo secrets).
- Multi-agent workflow: see docs/workflow_idea/procedure_multi_agents01.md
- Agent roles: planner (glm), executor (swe), challenger (glm), critic (swe)
- Blackboard: /devin_blackboard/STATE.md, TASKS.json, EVENTS.log

## Coding conventions

- Python 3.10+
- Tests in /tests/, source in /src/
- Run tests with: python -m pytest tests/
