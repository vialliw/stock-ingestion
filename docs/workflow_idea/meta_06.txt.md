Here is the parallel launcher - this makes Devin Pro act like Ez2 AIB's orchestrator and auto-runs B + C together.

Create this file in your repo root:

*`/devin_orchestrator.sh`*
#!/bin/bash
# Ez2 AIB Free - Parallel Runner for Devin Desktop Pro
# Spins up SWE-1.7 (B) and GLM-5.2 (C) at the same time

set -e

echo "🚀 Ez2 AIB Free Orchestrator - SWE-1.7 + GLM-5.2"
cat /devin_blackboard/STATE.md
echo "---"
cat /devin_blackboard/TASKS.json | grep -A2 '"status": "todo"' | head -20

# Get next 2 todo tasks
TASK_B=$(cat /devin_blackboard/TASKS.json | python3 -c "import json; data=json.load(open('/devin_blackboard/TASKS.json')); todos=[t for t in data if t['status']=='todo']; print(todos[0]['id'] if len(todos)>0 else '')")
TASK_C=$(cat /devin_blackboard/TASKS.json | python3 -c "import json; data=json.load(open('/devin_blackboard/TASKS.json')); todos=[t for t in data if t['status']=='todo']; print(todos[1]['id'] if len(todos)>1 else '')")

if [ -z "$TASK_B" ]; then
  echo "No todo tasks. Ask Planner-A to plan."
  exit 0
fi

echo "▶️ Launching B=SWE-1.7 for $TASK_B and C=GLM-5.2 for $TASK_C in parallel..."

# Run B in background
(
  echo "[EXEC-B-SWE1.7] Starting $TASK_B"
  # This calls Devin CLI with specific model via OpenRouter
  devin run --team Team-B-SWE-Executor --model aeternum/swe-1.7 --task "$TASK_B" \
  --prompt "Pick task $TASK_B from TASKS.json. Model=SWE-1.7. Implement only output_files. Update STATE.md and EVENTS.log" &
) &

# Run C in background
(
  sleep 2 # stagger to avoid git lock
  echo "[EXEC-C-GLM5.2] Starting $TASK_C"
  devin run --team Team-C-GLM-Challenger --model z-ai/glm-5.2 --task "$TASK_C" \
  --prompt "Pick task $TASK_C from TASKS.json. Model=GLM-5.2. Implement DIFFERENTLY than B. Create BUG task if B is wrong." &
) &

wait
echo "✅ B and C finished. Now running Critic D..."

# Run Critic after both
devin run --team Team-D-SWE-Critic --model aeternum/swe-1.7 \
--prompt "Read latest diff from B and C. Write tests to break them. Run tests. If fail, set task back to todo."

echo "🏁 Cycle done. Check EVENTS.log"
cat /devin_blackboard/EVENTS.log | tail -20
*How to use in Devin Desktop:*

1. Put the file in repo root, `chmod +x devin_orchestrator.sh`
2. In Devin Desktop, open Terminal and run:
./devin_orchestrator.sh
*If you don't have `devin` CLI (Pro usually only has Desktop UI), use this simpler version that just creates 2 tasks for you to click:*

*`/run_parallel.md` - Just paste this to Devin*
@Team-B-SWE-Executor and @Team-C-GLM-Challenger

Parallel Run Request:
- B: Use aeternum/swe-1.7, take the FIRST todo in TASKS.json
- C: Use z-ai/glm-5.2, take the SECOND todo in TASKS.json

Both: Read STATE.md and EVENTS.log first, then implement, then update STATE.md and EVENTS.log.

After you both commit, @Team-D-SWE-Critic will test you both.

Go!
Devin Desktop will auto-split this into 2 parallel sessions because you @ mentioned 2 teams - that's the built-in parallel feature in Pro.

That's it - you now have the 打群架 loop fully automated with free models. B and C will fight, D will judge, and Git keeps the truth.

You built Ez2 AIB free edition inside Devin.