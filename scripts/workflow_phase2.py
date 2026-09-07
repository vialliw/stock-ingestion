"""Dynamic Workflow script for stock_ingestion Phase 2 cycles.

This script orchestrates a team of Devin agents through the executor → challenger → critic
pipeline using Devin Dynamic Workflows. It replaces the polling-based protocol with
declarative, resumable orchestration.

Usage:
    Ask Devin in any session:
    "Use a workflow to run scripts/workflow_phase2.py for the stock_ingestion Phase 2 cycle"

Prerequisites:
    - Dynamic Workflows enabled (Settings > Enterprise > Devin for enterprise accounts)
    - TASKS.json with structured acceptance_criteria
    - Agent playbooks created (stock-ingestion-executor, stock-ingestion-challenger, stock-ingestion-critic)

The script:
    1. Reads todo tasks from TASKS.json
    2. For each cycle: runs executor, then parallel challenger + critic
    3. Pipes structured results between stages
    4. Records all results for resumability
"""
import asyncio
import json

# --- Workflow metadata ---
META = {
    "name": "stock-ingestion-phase2",
    "description": "Execute Phase 2 cycles with executor → challenger → critic pipeline",
    "phases": [
        {"title": "execute", "detail": "Agent B implements the task"},
        {"title": "review", "detail": "Agent C reviews and Agent D verifies in parallel"},
        {"title": "advance", "detail": "Orchestrator reviews evidence and advances cycle"},
    ],
}

# --- Schemas for structured agent output ---

EXECUTOR_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string"},
        "files_changed": {"type": "array", "items": {"type": "string"}},
        "tests_passed": {"type": "boolean"},
        "loop_checker_clean": {"type": "boolean"},
        "evidence": {"type": "string"},
        "blockers": {"type": "string"},
    },
    "required": ["task_id", "files_changed", "tests_passed", "evidence"],
}

REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "task_id": {"type": "string"},
        "verdict": {"type": "string", "enum": ["pass", "fail", "concerns"]},
        "findings": {"type": "string"},
        "evidence_map": {"type": "string"},
    },
    "required": ["task_id", "verdict", "findings"],
}

# --- Prompt builders ---

REPO = "github.com/vialliw/stock-ingestion"
PROJECT_DIR = "C:\\Users\\user\\OneDrive\\Documents\\script\\python\\projects\\stock-ingestion"


def build_executor_prompt(task_def):
    """Build the prompt for the executor agent."""
    ac_summary = (
        f"{len(task_def.get('acceptance_criteria', []))} structured criteria"
        if isinstance(task_def.get("acceptance_criteria"), list)
        else task_def.get("acceptance_criteria", "see TASKS.json")
    )
    return (
        f"You are Agent B (Executor) for stock_ingestion.\n"
        f"Project directory: {PROJECT_DIR}\n\n"
        f"Task: {task_def['id']} — {task_def['title']}\n"
        f"Input files: {task_def.get('input_files', [])}\n"
        f"Output files: {task_def.get('output_files', [])}\n"
        f"Acceptance criteria: {ac_summary}\n\n"
        f"Steps:\n"
        f"1. cd to {PROJECT_DIR} and git pull\n"
        f"2. Run: python scripts/poll_tasks.py --claim {task_def['id']} B\n"
        f"3. Read input_files, AGENTS.md, STATE.md, EVENTS.log\n"
        f"4. Implement the task (write to output_files ONLY)\n"
        f"5. Run: python scripts/poll_tasks.py --verify {task_def['id']}\n"
        f"6. If all pass: python scripts/poll_tasks.py --done {task_def['id']} B --evidence \"<summary>\"\n"
        f"7. If any fail: python scripts/poll_tasks.py --blocked {task_def['id']} B --reason \"<reason>\"\n\n"
        f"Follow SQL > vectorized > loop hierarchy. Any loop needs '# justified: <reason>' comment.\n"
        f"Report: task_id, files_changed, tests_passed, loop_checker_clean, evidence, blockers."
    )


def build_challenger_prompt(task_def, exec_result):
    """Build the prompt for the challenger agent."""
    return (
        f"You are Agent C (Challenger) for stock_ingestion.\n"
        f"Project directory: {PROJECT_DIR}\n\n"
        f"Review task: {task_def['id']} — {task_def['title']}\n"
        f"Executor changed: {exec_result.get('files_changed', [])}\n"
        f"Executor evidence: {exec_result.get('evidence', 'N/A')}\n\n"
        f"Steps:\n"
        f"1. cd to {PROJECT_DIR} and git pull\n"
        f"2. Run: python scripts/poll_tasks.py --claim {task_def['id']}b C\n"
        f"3. Read the executor's output_files\n"
        f"4. Look for: false positives/negatives, edge cases, vectorizable loops, optimizable SQL\n"
        f"5. Run: python scripts/poll_tasks.py --verify {task_def['id']}\n"
        f"6. Run: python scripts/poll_tasks.py --done {task_def['id']}b C --evidence \"<findings>\"\n\n"
        f"Do NOT modify src/ files. Communicate findings via EVENTS.log.\n"
        f"Report: task_id, verdict (pass/fail/concerns), findings, evidence_map."
    )


def build_critic_prompt(task_def, exec_result):
    """Build the prompt for the critic agent."""
    return (
        f"You are Agent D (Critic) for stock_ingestion.\n"
        f"Project directory: {PROJECT_DIR}\n\n"
        f"Verify task: {task_def['id']} — {task_def['title']}\n"
        f"Executor changed: {exec_result.get('files_changed', [])}\n"
        f"Executor evidence: {exec_result.get('evidence', 'N/A')}\n\n"
        f"Steps:\n"
        f"1. cd to {PROJECT_DIR} and git pull\n"
        f"2. Run: python scripts/poll_tasks.py --claim {task_def['id']}c D\n"
        f"3. Run: python scripts/poll_tasks.py --verify {task_def['id']}\n"
        f"4. If all pass: python scripts/poll_tasks.py --done {task_def['id']}c D --evidence \"<full evidence map>\"\n"
        f"5. If any fail: python scripts/poll_tasks.py --blocked {task_def['id']}c D --reason \"<failed criteria>\"\n\n"
        f"You are the final gate. Verify each acceptance criterion with concrete evidence.\n"
        f"Report: task_id, verdict (pass/fail), findings, evidence_map."
    )


# --- Cycle execution ---

async def execute_cycle(task_def):
    """Execute one cycle: executor → parallel(challenger, critic) → advance."""
    task_id = task_def["id"]
    challenger_required = task_def.get("challenger_required", True)

    # Phase 1: Executor
    log(f"Starting cycle for {task_id} — {task_def['title']}")
    exec_result = await agent(
        build_executor_prompt(task_def),
        phase="execute",
        schema=EXECUTOR_SCHEMA,
        label=f"execute-{task_id}",
    )

    if not exec_result.get("tests_passed", False):
        log(f"{task_id}: executor failed tests, halting cycle")
        return {"task_id": task_id, "passed": False, "stage": "execute", "result": exec_result}

    # Phase 2: Parallel review + verify
    review_tasks = []

    if challenger_required:
        review_tasks.append(
            agent(
                build_challenger_prompt(task_def, exec_result),
                phase="review",
                schema=REVIEW_SCHEMA,
                label=f"review-{task_id}",
            )
        )

    review_tasks.append(
        agent(
            build_critic_prompt(task_def, exec_result),
            phase="review",
            schema=REVIEW_SCHEMA,
            label=f"verify-{task_id}",
        )
    )

    log(f"{task_id}: running {'challenger + critic' if challenger_required else 'critic'} in parallel")
    reviews = await parallel(review_tasks)

    # Phase 3: Advance
    all_pass = all(r.get("verdict") == "pass" for r in reviews if r is not None)
    has_concerns = any(r.get("verdict") == "concerns" for r in reviews if r is not None)

    if all_pass and not has_concerns:
        log(f"{task_id}: CYCLE COMPLETE — all reviews passed")
    elif has_concerns:
        log(f"{task_id}: CYCLE COMPLETE WITH CONCERNS — reviews passed but with findings to address")
    else:
        log(f"{task_id}: CYCLE BLOCKED — one or more reviews failed")

    return {
        "task_id": task_id,
        "passed": all_pass,
        "has_concerns": has_concerns,
        "executor_result": exec_result,
        "reviews": reviews,
    }


def load_cycle_tasks():
    """Load todo tasks from TASKS.json, grouped by cycle.

    Returns a list of executor tasks (one per cycle) with their
    challenger and critic tasks referenced by ID convention.
    """
    with open("devin_blackboard/TASKS.json", "r", encoding="utf-8") as f:
        tasks = json.load(f)

    # Find executor tasks that are todo and have satisfied dependencies
    done_ids = {t["id"] for t in tasks if t["status"] == "done"}
    cycle_tasks = []
    # justified: filtering in-memory list by multiple conditions; no SQL equivalent
    for t in tasks:
        if (
            t.get("assigned_to") == "B"
            and t["status"] == "todo"
            and t.get("role") == "executor"
        ):
            deps = t.get("depends_on", [])
            if all(d in done_ids for d in deps):
                cycle_tasks.append(t)

    return cycle_tasks


# --- Main workflow ---

async def main():
    await register_workflow(META)

    cycles = load_cycle_tasks()

    if not cycles:
        log("No ready cycles found in TASKS.json. Exiting.")
        return

    log(f"Found {len(cycles)} ready cycle(s)")

    results = []
    # justified: sequential cycle execution — each cycle depends on the previous
    for cycle_task in cycles:
        result = await execute_cycle(cycle_task)
        results.append(result)

        if not result["passed"]:
            log(f"Cycle halted at {result['task_id']} — awaiting orchestrator intervention")
            break

        log(f"Cycle {result['task_id']} complete, advancing to next")

    # Summary
    log("\n=== Workflow Summary ===")
    # justified: formatting summary output from collected results; not vectorizable
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        concerns = " (with concerns)" if r.get("has_concerns") else ""
        log(f"  {r['task_id']}: {status}{concerns}")


if __name__ == "__main__":
    asyncio.run(main())
