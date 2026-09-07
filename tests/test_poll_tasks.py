"""Tests for scripts/poll_tasks.py — atomic locks, subcommands, structured criteria."""
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

# Add scripts dir to path for imports
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture
def tmp_blackboard(tmp_path):
    """Create a temporary blackboard directory with TASKS.json and locks/."""
    bb = tmp_path / "devin_blackboard"
    bb.mkdir()
    (bb / "locks").mkdir()
    events = bb / "EVENTS.log"
    events.write_text("[INIT] Blackboard created\n")
    state = bb / "STATE.md"
    state.write_text("# State\n\n## What's Done\n\n## Active Blockers\n")
    return bb


@pytest.fixture
def tmp_tasks(tmp_blackboard):
    """Create TASKS.json with sample tasks."""
    tasks = [
        {
            "id": "TEST-T1",
            "title": "Test executor task",
            "role": "executor",
            "model": "swe",
            "status": "todo",
            "depends_on": [],
            "input_files": [],
            "output_files": [],
            "acceptance_criteria": [
                {"id": "AC-1", "check": "always_pass", "command": "python -c \"exit(0)\"", "expected_exit": 0},
                {"id": "AC-2", "check": "always_fail", "command": "python -c \"exit(1)\"", "expected_exit": 0},
            ],
            "assigned_to": "B",
        },
        {
            "id": "TEST-T2",
            "title": "Test task with deps",
            "role": "challenger",
            "model": "glm",
            "status": "todo",
            "depends_on": ["TEST-T1"],
            "input_files": [],
            "output_files": [],
            "acceptance_criteria": "prose criteria",
            "assigned_to": "C",
        },
        {
            "id": "TEST-T3",
            "title": "Done task",
            "role": "critic",
            "model": "swe",
            "status": "done",
            "depends_on": [],
            "input_files": [],
            "output_files": [],
            "acceptance_criteria": "prose",
            "assigned_to": "D",
        },
    ]
    tasks_path = tmp_blackboard / "TASKS.json"
    tasks_path.write_text(json.dumps(tasks, indent=2))
    return tmp_blackboard


@pytest.fixture
def patch_paths(tmp_tasks, monkeypatch):
    """Patch poll_tasks module to use temp blackboard paths."""
    import poll_tasks

    bb = tmp_tasks
    monkeypatch.setattr(poll_tasks, "BLACKBOARD", bb)
    monkeypatch.setattr(poll_tasks, "TASKS_PATH", bb / "TASKS.json")
    monkeypatch.setattr(poll_tasks, "EVENTS_PATH", bb / "EVENTS.log")
    monkeypatch.setattr(poll_tasks, "STATE_PATH", bb / "STATE.md")
    monkeypatch.setattr(poll_tasks, "LOCKS_DIR", bb / "locks")
    monkeypatch.setattr(poll_tasks, "commit_blackboard", lambda msg: None)
    return poll_tasks


# --- Atomic lock tests ---

class TestAtomicClaimLock:
    def test_atomic_claim_lock_success(self, patch_paths):
        """Lock file created atomically, returns True."""
        result = patch_paths.atomic_claim_lock("TEST-T1", "B")
        assert result is True
        lock_file = patch_paths.LOCKS_DIR / "TEST-T1.lock"
        assert lock_file.exists()
        data = json.loads(lock_file.read_text())
        assert data["agent"] == "B"
        assert "claimed_at" in data

    def test_atomic_claim_lock_collision(self, patch_paths):
        """Second claim on same task fails, returns False."""
        assert patch_paths.atomic_claim_lock("TEST-T1", "B") is True
        assert patch_paths.atomic_claim_lock("TEST-T1", "C") is False

    def test_atomic_claim_lock_different_tasks(self, patch_paths):
        """Different tasks can be locked independently."""
        assert patch_paths.atomic_claim_lock("TEST-T1", "B") is True
        assert patch_paths.atomic_claim_lock("TEST-T2", "C") is True


# --- Stale lock tests ---

class TestStaleLock:
    def test_is_lock_stale_old(self, patch_paths):
        """Lock older than 10 minutes is stale."""
        patch_paths.atomic_claim_lock("TEST-T1", "B")
        lock_file = patch_paths.LOCKS_DIR / "TEST-T1.lock"
        old_time = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        lock_file.write_text(json.dumps({"agent": "B", "claimed_at": old_time}))
        assert patch_paths.is_lock_stale("TEST-T1") is True

    def test_is_lock_stale_recent(self, patch_paths):
        """Lock younger than 10 minutes is not stale."""
        patch_paths.atomic_claim_lock("TEST-T1", "B")
        assert patch_paths.is_lock_stale("TEST-T1") is False

    def test_is_lock_stale_no_lock(self, patch_paths):
        """No lock file means not stale."""
        assert patch_paths.is_lock_stale("TEST-T1") is False

    def test_is_lock_stale_malformed(self, patch_paths):
        """Malformed lock file is treated as stale."""
        lock_file = patch_paths.LOCKS_DIR / "TEST-T1.lock"
        lock_file.write_text("not json")
        assert patch_paths.is_lock_stale("TEST-T1") is True

    def test_clean_stale_removes_old_locks(self, patch_paths):
        """--clean-stale removes locks older than 10 min and resets task to todo."""
        # Claim a task
        patch_paths.atomic_claim_lock("TEST-T1", "B")
        # Make the lock stale
        lock_file = patch_paths.LOCKS_DIR / "TEST-T1.lock"
        old_time = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        lock_file.write_text(json.dumps({"agent": "B", "claimed_at": old_time}))
        # Set task to in_progress
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        tasks[0]["status"] = "in_progress"
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))

        # Clean stale
        patch_paths.cmd_clean_stale()

        # Lock should be gone
        assert not lock_file.exists()
        # Task should be back to todo
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        assert tasks[0]["status"] == "todo"

    def test_clean_stale_preserves_recent_locks(self, patch_paths):
        """--clean-stale does not remove recent locks."""
        patch_paths.atomic_claim_lock("TEST-T1", "B")
        patch_paths.cmd_clean_stale()
        assert (patch_paths.LOCKS_DIR / "TEST-T1.lock").exists()


# --- Claim command tests ---

class TestCmdClaim:
    def test_claim_success(self, patch_paths):
        """--claim sets status to in_progress and creates lock."""
        patch_paths.cmd_claim("TEST-T1", "B")
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        assert tasks[0]["status"] == "in_progress"
        assert (patch_paths.LOCKS_DIR / "TEST-T1.lock").exists()

    def test_claim_wrong_role(self, patch_paths):
        """--claim fails if task assigned to different role."""
        with pytest.raises(SystemExit):
            patch_paths.cmd_claim("TEST-T1", "C")

    def test_claim_wrong_status(self, patch_paths):
        """--claim fails if task is not 'todo'."""
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        tasks[0]["status"] = "done"
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))
        with pytest.raises(SystemExit):
            patch_paths.cmd_claim("TEST-T1", "B")

    def test_claim_deps_not_done(self, patch_paths):
        """--claim fails if dependencies are not done."""
        with pytest.raises(SystemExit):
            patch_paths.cmd_claim("TEST-T2", "C")

    def test_claim_task_not_found(self, patch_paths):
        """--claim fails if task ID doesn't exist."""
        with pytest.raises(SystemExit):
            patch_paths.cmd_claim("NONEXISTENT", "B")


# --- Verify command tests ---

class TestCmdVerify:
    def test_verify_all_pass(self, patch_paths):
        """--verify runs all criteria and exits 0 when all pass."""
        # Make all criteria pass
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        tasks[0]["acceptance_criteria"] = [
            {"id": "AC-1", "check": "pass1", "command": "python -c \"exit(0)\"", "expected_exit": 0},
            {"id": "AC-2", "check": "pass2", "command": "python -c \"exit(0)\"", "expected_exit": 0},
        ]
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))
        # Should exit 0
        with pytest.raises(SystemExit) as exc_info:
            patch_paths.cmd_verify("TEST-T1")
        assert exc_info.value.code == 0

    def test_verify_one_fails(self, patch_paths):
        """--verify exits 1 when a criterion fails."""
        # AC-2 has expected_exit=0 but command exits 1
        with pytest.raises(SystemExit) as exc_info:
            patch_paths.cmd_verify("TEST-T1")
        assert exc_info.value.code == 1

    def test_verify_unstructured_criteria(self, patch_paths):
        """--verify with prose criteria prints manual review message."""
        # TEST-T2 has prose "prose criteria"
        with pytest.raises(SystemExit) as exc_info:
            patch_paths.cmd_verify("TEST-T2")
        assert exc_info.value.code == 0

    def test_verify_task_not_found(self, patch_paths):
        """--verify fails if task doesn't exist."""
        with pytest.raises(SystemExit):
            patch_paths.cmd_verify("NONEXISTENT")


# --- Done command tests ---

class TestCmdDone:
    def test_done_with_all_criteria_pass(self, patch_paths):
        """--done marks done when all criteria pass."""
        # Make all criteria pass
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        tasks[0]["acceptance_criteria"] = [
            {"id": "AC-1", "check": "pass", "command": "python -c \"exit(0)\"", "expected_exit": 0},
        ]
        tasks[0]["status"] = "in_progress"
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))
        # Create a lock
        patch_paths.atomic_claim_lock("TEST-T1", "B")

        patch_paths.cmd_done("TEST-T1", "B", "All tests pass")

        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        assert tasks[0]["status"] == "done"
        assert not (patch_paths.LOCKS_DIR / "TEST-T1.lock").exists()

    def test_done_refuses_if_criteria_fail(self, patch_paths):
        """--done refuses to mark done if criteria fail."""
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        # AC-2 fails (command exits 1, expected 0)
        tasks[0]["status"] = "in_progress"
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))
        patch_paths.atomic_claim_lock("TEST-T1", "B")

        with pytest.raises(SystemExit):
            patch_paths.cmd_done("TEST-T1", "B", "should fail")

        # Task should still be in_progress
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        assert tasks[0]["status"] == "in_progress"

    def test_done_wrong_status(self, patch_paths):
        """--done fails if task is not in_progress."""
        with pytest.raises(SystemExit):
            patch_paths.cmd_done("TEST-T1", "B", "evidence")


# --- Blocked command tests ---

class TestCmdBlocked:
    def test_blocked_sets_status(self, patch_paths):
        """--blocked sets status to blocked and deletes lock."""
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        tasks[0]["status"] = "in_progress"
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))
        patch_paths.atomic_claim_lock("TEST-T1", "B")

        patch_paths.cmd_blocked("TEST-T1", "B", "yfinance timeout")

        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        assert tasks[0]["status"] == "blocked"
        assert not (patch_paths.LOCKS_DIR / "TEST-T1.lock").exists()


# --- Poll command tests (backward compatibility) ---

class TestCmdPoll:
    def test_poll_shows_ready_task(self, patch_paths, capsys):
        """Poll shows READY for tasks with satisfied deps."""
        patch_paths.cmd_poll("B")
        captured = capsys.readouterr()
        assert "READY: TEST-T1" in captured.out

    def test_poll_shows_waiting_task(self, patch_paths, capsys):
        """Poll shows WAITING for tasks with unsatisfied deps."""
        patch_paths.cmd_poll("C")
        captured = capsys.readouterr()
        assert "WAITING: TEST-T2" in captured.out

    def test_poll_no_tasks(self, patch_paths, capsys):
        """Poll shows no tasks for unassigned role."""
        patch_paths.cmd_poll("A")
        captured = capsys.readouterr()
        assert "No tasks assigned_to=A" in captured.out

    def test_poll_challenger_required_false(self, patch_paths, capsys):
        """Poll skips challenger tasks when challenger_required=false."""
        tasks = json.loads(patch_paths.TASKS_PATH.read_text())
        tasks[1]["challenger_required"] = False
        # Make deps satisfied
        tasks[0]["status"] = "done"
        patch_paths.TASKS_PATH.write_text(json.dumps(tasks, indent=2))
        patch_paths.cmd_poll("C")
        captured = capsys.readouterr()
        assert "READY: TEST-T2" not in captured.out


# --- Helper function tests ---

class TestHelpers:
    def test_role_label(self, patch_paths):
        assert patch_paths.role_label("A") == "ORCHESTRATOR"
        assert patch_paths.role_label("B") == "EXECUTOR"
        assert patch_paths.role_label("C") == "CHALLENGER"
        assert patch_paths.role_label("D") == "CRITIC"

    def test_find_task_exists(self, patch_paths):
        task = patch_paths.find_task(
            json.loads(patch_paths.TASKS_PATH.read_text()), "TEST-T1"
        )
        assert task is not None
        assert task["id"] == "TEST-T1"

    def test_find_task_not_exists(self, patch_paths):
        task = patch_paths.find_task(
            json.loads(patch_paths.TASKS_PATH.read_text()), "NONEXISTENT"
        )
        assert task is None

    def test_delete_lock(self, patch_paths):
        """delete_lock removes the lock file."""
        patch_paths.atomic_claim_lock("TEST-T1", "B")
        assert (patch_paths.LOCKS_DIR / "TEST-T1.lock").exists()
        patch_paths.delete_lock("TEST-T1")
        assert not (patch_paths.LOCKS_DIR / "TEST-T1.lock").exists()

    def test_delete_lock_nonexistent(self, patch_paths):
        """delete_lock does not error if lock doesn't exist."""
        patch_paths.delete_lock("NONEXISTENT")  # should not raise
