"""Polling and task management script for multi-agent workflow.

Usage:
    python poll_tasks.py <role>                    # Poll for ready tasks (backward compatible)
    python poll_tasks.py --claim <task_id> <role>  # Atomically claim a task
    python poll_tasks.py --done <task_id> <role> --evidence <text>  # Mark task done
    python poll_tasks.py --blocked <task_id> <role> --reason <text> # Mark task blocked
    python poll_tasks.py --clean-stale             # Remove stale locks (>10 min old)
    python poll_tasks.py --verify <task_id>        # Run acceptance criteria, output evidence map

Examples:
    python poll_tasks.py B
    python poll_tasks.py --claim F7-T2 B
    python poll_tasks.py --done F7-T2 B --evidence "122/122 tests pass, loop checker clean"
    python poll_tasks.py --blocked F7-T2 B --reason "yfinance API timeout"
    python poll_tasks.py --clean-stale
    python poll_tasks.py --verify F7-T2
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# --- Paths ---
BLACKBOARD = Path("devin_blackboard")
TASKS_PATH = BLACKBOARD / "TASKS.json"
EVENTS_PATH = BLACKBOARD / "EVENTS.log"
STATE_PATH = BLACKBOARD / "STATE.md"
LOCKS_DIR = BLACKBOARD / "locks"
STALE_LOCK_SECONDS = 600  # 10 minutes


# --- Helpers ---

def now_iso():
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def load_tasks():
    """Load TASKS.json and return list of task dicts."""
    with open(TASKS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_tasks(tasks):
    """Write tasks back to TASKS.json."""
    with open(TASKS_PATH, "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2, ensure_ascii=False)
        f.write("\n")


def append_event(text):
    """Append a line to EVENTS.log."""
    with open(EVENTS_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n[{now_iso()}] {text}")


def find_task(tasks, task_id):
    """Find a task by ID. Return the task dict or None."""
    # justified: searching a list by key; no SQL/vectorized equivalent for in-memory list
    for t in tasks:
        if t["id"] == task_id:
            return t
    return None


def lock_path(task_id):
    """Return the lock file path for a task."""
    return LOCKS_DIR / f"{task_id}.lock"


def atomic_claim_lock(task_id, role):
    """Atomically create a lock file using OS-level exclusive creation.

    Returns True if lock acquired, False if lock already exists.
    Uses os.O_CREAT | os.O_EXCL to guarantee atomicity across processes.
    """
    LOCKS_DIR.mkdir(parents=True, exist_ok=True)
    path = str(lock_path(task_id))
    data = json.dumps({"agent": role, "claimed_at": now_iso()}).encode("utf-8")
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        return True
    except FileExistsError:
        return False


def delete_lock(task_id):
    """Delete a lock file if it exists."""
    path = lock_path(task_id)
    if path.exists():
        path.unlink()


def read_lock(task_id):
    """Read lock file contents. Return dict or None."""
    path = lock_path(task_id)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def is_lock_stale(task_id):
    """Check if a lock file is older than STALE_LOCK_SECONDS.

    Returns True if:
    - Lock file exists but is malformed (not valid JSON)
    - Lock file exists but has no claimed_at field
    - Lock file exists and claimed_at is older than STALE_LOCK_SECONDS
    Returns False if:
    - Lock file does not exist
    - Lock file exists and is younger than STALE_LOCK_SECONDS
    """
    path = lock_path(task_id)
    if not path.exists():
        return False
    lock = read_lock(task_id)
    if lock is None:
        return True  # file exists but malformed
    claimed_at = lock.get("claimed_at")
    if not claimed_at:
        return True  # malformed lock = stale
    try:
        claimed_dt = datetime.fromisoformat(claimed_at)
        age = (datetime.now(timezone.utc) - claimed_dt).total_seconds()
        return age > STALE_LOCK_SECONDS
    except (ValueError, TypeError):
        return True  # unparseable timestamp = stale


def update_task_status(tasks, task_id, status):
    """Update a task's status in the tasks list. Returns True if found."""
    # justified: in-memory list mutation by key; no SQL equivalent
    for t in tasks:
        if t["id"] == task_id:
            t["status"] = status
            return True
    return False


def commit_blackboard(message):
    """Stage, commit, and push blackboard files."""
    subprocess.run(["git", "add", "-A"], check=False)
    subprocess.run(["git", "commit", "-m", message], check=False)
    subprocess.run(["git", "push", "origin", "HEAD"], check=False)


def role_label(role):
    """Map role letter to full label for EVENTS.log."""
    labels = {"A": "ORCHESTRATOR", "B": "EXECUTOR", "C": "CHALLENGER", "D": "CRITIC"}
    return labels.get(role, role)


# --- Commands ---

def cmd_poll(role):
    """Poll for ready tasks assigned to a role. Backward compatible with original script."""
    tasks = load_tasks()
    done_ids = {t["id"] for t in tasks if t["status"] == "done"}

    found = False
    for t in tasks:
        if t.get("assigned_to") == role and t["status"] == "todo":
            deps = t.get("depends_on", [])
            deps_ok = all(d in done_ids for d in deps)
            # Check challenger_required flag — if false, skip challenger tasks
            challenger_required = t.get("challenger_required", True)
            if t.get("role") == "challenger" and not challenger_required:
                continue
            status_str = "READY" if deps_ok else "WAITING"
            print(f"{status_str}: {t['id']} assigned_to={t['assigned_to']} deps={deps} deps_ok={deps_ok}")
            if deps_ok:
                print(f"  title: {t['title']}")
                print(f"  input_files: {t.get('input_files', [])}")
                print(f"  output_files: {t.get('output_files', [])}")
                ac = t.get("acceptance_criteria", "")
                if isinstance(ac, list):
                    ac_summary = f"{len(ac)} structured criteria"
                else:
                    ac_summary = str(ac)[:200] + "..." if len(str(ac)) > 200 else str(ac)
                print(f"  acceptance_criteria: {ac_summary}")
            found = True

    if not found:
        print(f"No tasks assigned_to={role} with status=todo.")

    print(f"\n--- Summary ---")
    print(f"done_ids count: {len(done_ids)}")
    for t in tasks:
        if t["status"] == "todo":
            print(f"TODO: {t['id']} assigned_to={t['assigned_to']} role={t['role']}")


def cmd_claim(task_id, role):
    """Atomically claim a task: create lock, set in_progress, append event, commit."""
    tasks = load_tasks()
    task = find_task(tasks, task_id)
    if task is None:
        print(f"ERROR: Task {task_id} not found in TASKS.json")
        sys.exit(1)
    if task.get("assigned_to") != role:
        print(f"ERROR: Task {task_id} is assigned to {task.get('assigned_to')}, not {role}")
        sys.exit(1)
    if task["status"] != "todo":
        print(f"ERROR: Task {task_id} has status={task['status']}, expected 'todo'")
        sys.exit(1)

    # Check dependencies
    done_ids = {t["id"] for t in tasks if t["status"] == "done"}
    deps = task.get("depends_on", [])
    deps_not_done = [d for d in deps if d not in done_ids]
    if deps_not_done:
        print(f"ERROR: Task {task_id} dependencies not satisfied: {deps_not_done}")
        sys.exit(1)

    # Check for existing lock
    existing_lock = read_lock(task_id)
    if existing_lock is not None:
        if is_lock_stale(task_id):
            print(f"WARNING: Stale lock detected for {task_id}, removing...")
            delete_lock(task_id)
        else:
            print(f"ERROR: Task {task_id} is locked by {existing_lock.get('agent')} "
                  f"since {existing_lock.get('claimed_at')}")
            sys.exit(1)

    # Atomic lock acquisition
    if not atomic_claim_lock(task_id, role):
        print(f"ERROR: Race condition — another agent claimed {task_id} simultaneously")
        sys.exit(1)

    # Update status
    update_task_status(tasks, task_id, "in_progress")
    save_tasks(tasks)

    label = role_label(role)
    append_event(f"[{label}] claimed {task_id}")
    commit_blackboard(f"[{role}] claim {task_id}")

    print(f"CLAIMED: {task_id} by Agent {role}")
    print(f"Lock file: {lock_path(task_id)}")
    print(f"Status: in_progress")
    print(f"Next: read input_files, implement in output_files, then run --verify {task_id}")


def cmd_done(task_id, role, evidence):
    """Mark a task done: verify acceptance criteria, set done, append evidence, delete lock, commit."""
    tasks = load_tasks()
    task = find_task(tasks, task_id)
    if task is None:
        print(f"ERROR: Task {task_id} not found")
        sys.exit(1)
    if task.get("assigned_to") != role:
        print(f"ERROR: Task {task_id} is assigned to {task.get('assigned_to')}, not {role}")
        sys.exit(1)
    if task["status"] != "in_progress":
        print(f"ERROR: Task {task_id} has status={task['status']}, expected 'in_progress'")
        sys.exit(1)

    # Verify acceptance criteria if structured
    ac = task.get("acceptance_criteria", "")
    if isinstance(ac, list) and ac:
        print(f"Verifying {len(ac)} acceptance criteria for {task_id}...")
        evidence_map = verify_criteria(ac)
        all_pass = all(item["passed"] for item in evidence_map)
        if not all_pass:
            failed = [item["id"] for item in evidence_map if not item["passed"]]
            print(f"ERROR: Acceptance criteria failed: {failed}")
            print("Use --blocked to mark the task blocked, or fix the issues and re-run --done")
            sys.exit(1)
        print("All acceptance criteria passed.")
        evidence_json = json.dumps(evidence_map, indent=2)
        full_evidence = f"{evidence}\nEvidence map:\n{evidence_json}"
    else:
        full_evidence = evidence

    # Update status
    update_task_status(tasks, task_id, "done")
    save_tasks(tasks)

    label = role_label(role)
    append_event(f"[{label}] completed {task_id} -> {full_evidence}")
    delete_lock(task_id)
    commit_blackboard(f"[{role}] {task_id} done")

    print(f"DONE: {task_id} by Agent {role}")
    print(f"Status: done")
    print(f"Lock deleted: {lock_path(task_id)}")


def cmd_blocked(task_id, role, reason):
    """Mark a task blocked: set blocked, append reason, update STATE.md, delete lock, commit."""
    tasks = load_tasks()
    task = find_task(tasks, task_id)
    if task is None:
        print(f"ERROR: Task {task_id} not found")
        sys.exit(1)
    if task.get("assigned_to") != role:
        print(f"ERROR: Task {task_id} is assigned to {task.get('assigned_to')}, not {role}")
        sys.exit(1)

    update_task_status(tasks, task_id, "blocked")
    save_tasks(tasks)

    label = role_label(role)
    append_event(f"[{label}] blocked {task_id} -> {reason}")
    delete_lock(task_id)
    commit_blackboard(f"[{role}] {task_id} blocked")

    print(f"BLOCKED: {task_id} by Agent {role}")
    print(f"Reason: {reason}")
    print(f"Status: blocked")
    print(f"Lock deleted: {lock_path(task_id)}")
    print(f"Orchestrator must inspect and resolve before re-assigning.")


def cmd_clean_stale():
    """Scan all lock files and remove stale ones (>10 min old). Reset task to todo."""
    if not LOCKS_DIR.exists():
        print("No locks directory found. Nothing to clean.")
        return

    tasks = load_tasks()
    cleaned = 0

    # justified: filesystem iteration over lock files; no SQL/vectorized equivalent
    for lock_file in LOCKS_DIR.glob("*.lock"):
        task_id = lock_file.stem
        if is_lock_stale(task_id):
            lock = read_lock(task_id)
            agent = lock.get("agent", "unknown") if lock else "unknown"
            claimed = lock.get("claimed_at", "unknown") if lock else "unknown"
            delete_lock(task_id)
            update_task_status(tasks, task_id, "todo")
            append_event(
                f"[SYSTEM] auto-recovered stale lock for {task_id} "
                f"(was claimed by {agent} since {claimed}). Task reset to todo."
            )
            print(f"CLEANED: {task_id} (was locked by {agent} since {claimed})")
            cleaned += 1
        else:
            print(f"OK: {task_id} lock is recent, preserving")

    if cleaned > 0:
        save_tasks(tasks)
        commit_blackboard(f"Cleaned {cleaned} stale lock(s)")

    print(f"\nCleaned {cleaned} stale lock(s).")


def verify_criteria(criteria_list):
    """Run each acceptance criterion command and return evidence map.

    Args:
        criteria_list: list of dicts with keys: id, check, command, expected_exit

    Returns:
        list of dicts with keys: id, check, passed, output, expected_exit, actual_exit
    """
    evidence = []
    # justified: running sequential subprocess commands with different configs; not vectorizable
    for ac in criteria_list:
        ac_id = ac.get("id", "unknown")
        check_name = ac.get("check", "unknown")
        command = ac.get("command", "")
        expected_exit = ac.get("expected_exit", 0)

        print(f"  [{ac_id}] {check_name}: {command}")

        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True, timeout=300
            )
            actual_exit = result.returncode
            output = result.stdout + result.stderr
            passed = (actual_exit == expected_exit)
        except subprocess.TimeoutExpired:
            actual_exit = -1
            output = "TIMEOUT after 300s"
            passed = False
        except Exception as e:
            actual_exit = -2
            output = str(e)
            passed = False

        status = "PASS" if passed else "FAIL"
        print(f"    -> {status} (exit {actual_exit}, expected {expected_exit})")

        evidence.append({
            "id": ac_id,
            "check": check_name,
            "command": command,
            "expected_exit": expected_exit,
            "actual_exit": actual_exit,
            "passed": passed,
            "output": output[-500:] if len(output) > 500 else output,
        })

    return evidence


def cmd_verify(task_id):
    """Run acceptance criteria for a task and print evidence map. Does not change status."""
    tasks = load_tasks()
    task = find_task(tasks, task_id)
    if task is None:
        print(f"ERROR: Task {task_id} not found")
        sys.exit(1)

    ac = task.get("acceptance_criteria", "")
    if not isinstance(ac, list) or not ac:
        print(f"Task {task_id} has unstructured acceptance_criteria (prose string).")
        print("Cannot verify programmatically. Manual review required.")
        print(f"Criteria: {ac}")
        sys.exit(0)

    print(f"Verifying {len(ac)} acceptance criteria for {task_id}...\n")
    evidence_map = verify_criteria(ac)

    all_pass = all(item["passed"] for item in evidence_map)
    print(f"\n{'ALL PASS' if all_pass else 'SOME FAILED'}")
    print(f"\nEvidence map (JSON):")
    print(json.dumps(evidence_map, indent=2))

    sys.exit(0 if all_pass else 1)


# --- Main ---

def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    # Subcommand parsing
    if args[0] == "--claim":
        if len(args) < 3:
            print("Usage: python poll_tasks.py --claim <task_id> <role>")
            sys.exit(1)
        cmd_claim(args[1], args[2])

    elif args[0] == "--done":
        if len(args) < 3:
            print("Usage: python poll_tasks.py --done <task_id> <role> [--evidence <text>]")
            sys.exit(1)
        evidence = ""
        if "--evidence" in args:
            idx = args.index("--evidence")
            evidence = " ".join(args[idx + 1:])
        cmd_done(args[1], args[2], evidence)

    elif args[0] == "--blocked":
        if len(args) < 3:
            print("Usage: python poll_tasks.py --blocked <task_id> <role> [--reason <text>]")
            sys.exit(1)
        reason = ""
        if "--reason" in args:
            idx = args.index("--reason")
            reason = " ".join(args[idx + 1:])
        cmd_blocked(args[1], args[2], reason)

    elif args[0] == "--clean-stale":
        cmd_clean_stale()

    elif args[0] == "--verify":
        if len(args) < 2:
            print("Usage: python poll_tasks.py --verify <task_id>")
            sys.exit(1)
        cmd_verify(args[1])

    elif args[0] in ("--help", "-h"):
        print(__doc__)
        sys.exit(0)

    else:
        # Backward compatible: treat first arg as role
        role = args[0].upper()
        if role not in ("A", "B", "C", "D"):
            print(f"Unknown argument: {args[0]}")
            print(__doc__)
            sys.exit(1)
        cmd_poll(role)


if __name__ == "__main__":
    main()
