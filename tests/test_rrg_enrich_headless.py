"""Tests for scripts/rrg_enrich_headless.py — the Phase B wrapper that
spawns a headless `devin -p` session to enrich the weekly RRA report.

All tests are hermetic: state file, summary, lock file and EVENTS.log
live under tmp_path (module constants are monkeypatched), and the
subprocess layer is a fake — no real Devin session is ever spawned.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rrg_enrich_headless as reh  # noqa: E402


# ------------------------------------------------------------------
# Fixtures / fakes
# ------------------------------------------------------------------

PENDING_STATE = {
    "as_of": "2026-11-20",
    "resolved_date": "2026-11-20",
    "prompt_json": "Relative_Rotation_Dashboard_ETFs_2026-11-20_analysis_prompt.json",
    "report_md": "Relative_Rotation_Dashboard_ETFs_2026-11-20_analysis_report.md",
    "report_action": "drafted_needs_enrichment",
    "report_enriched": False,
    "summary_action": "exists",
    "summary_has_date": False,
    "generated_at": "2026-11-23T12:29:37+00:00",
}

ENRICHED_STATE = dict(PENDING_STATE, report_enriched=True,
                      report_action="exists_enriched",
                      summary_has_date=True)


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Redirect every module-level path into tmp_path."""
    docs = tmp_path / "docs" / "rra"
    docs.mkdir(parents=True)
    bb = tmp_path / "devin_blackboard"
    (bb / "locks").mkdir(parents=True)
    events = bb / "EVENTS.log"
    events.write_text("", encoding="utf-8")
    monkeypatch.setattr(reh, "DOCS_RRA", docs)
    monkeypatch.setattr(reh, "STATE_FILE",
                        docs / ".rrg_pipeline_state.json")
    monkeypatch.setattr(reh, "SUMMARY_FILE", docs / "weekly_summary.md")
    monkeypatch.setattr(reh, "EVENTS_LOG", events)
    monkeypatch.setattr(reh, "LOCK_FILE", bb / "locks" / "rrg_enrich.lock")
    return {"docs": docs, "bb": bb, "events": events,
            "summary": docs / "weekly_summary.md",
            "lock": bb / "locks" / "rrg_enrich.lock"}


def _write_state(sandbox, state):
    (sandbox["docs"] / ".rrg_pipeline_state.json").write_text(
        json.dumps(state) if not isinstance(state, str) else state,
        encoding="utf-8")


class _FakeProc:
    def __init__(self, rc=0, hang=False):
        self.pid = 4321
        self._rc = rc
        self._hang = hang
        self.killed = False
        self.wait_timeouts = []

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self._hang:
            raise subprocess.TimeoutExpired("devin", timeout or 0)
        return self._rc

    def kill(self):
        self.killed = True


class _FakePopen:
    """Records argv/cwd and replays a canned exit behavior."""

    def __init__(self, rc=0, hang=False, raises=None):
        self.calls = []
        self.procs = []
        self._rc = rc
        self._hang = hang
        self._raises = raises

    def __call__(self, cmd, cwd=None):
        self.calls.append({"cmd": list(cmd), "cwd": cwd})
        if self._raises is not None:
            raise self._raises
        proc = _FakeProc(rc=self._rc, hang=self._hang)
        self.procs.append(proc)
        return proc


def _patch_popen(monkeypatch, fake):
    monkeypatch.setattr(subprocess, "Popen", fake)


# ------------------------------------------------------------------
# No-op when enriched
# ------------------------------------------------------------------

class TestNoOpWhenEnriched:
    def test_enriched_state_and_covering_summary_noop(
            self, sandbox, monkeypatch, capsys):
        fake = _FakePopen()
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, ENRICHED_STATE)
        sandbox["summary"].write_text(
            "| Date | Weekly summary |\n| 2026-11-20 | metals lead. |\n",
            encoding="utf-8")
        assert reh.run() == 0
        assert fake.calls == []            # no spawn
        assert not sandbox["lock"].exists()  # no lock
        assert "no-op" in capsys.readouterr().out

    def test_report_enriched_but_summary_missing_date_spawns(
            self, sandbox, monkeypatch):
        """Publish-gate mirror: flag alone is not enough — the summary
        must cover resolved_date, else the run is still pending."""
        fake = _FakePopen(rc=0)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, ENRICHED_STATE)
        sandbox["summary"].write_text(
            "| Date | Weekly summary |\n| 2026-11-13 | old row. |\n",
            encoding="utf-8")
        assert reh.run() == 0
        assert len(fake.calls) == 1


# ------------------------------------------------------------------
# Spawn argv correctness
# ------------------------------------------------------------------

class TestSpawnArgs:
    def test_pending_spawn_argv_and_prompt(self, sandbox, monkeypatch):
        fake = _FakePopen(rc=0)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        sandbox["summary"].write_text("no dates here\n", encoding="utf-8")

        assert reh.run() == 0
        assert len(fake.calls) == 1
        call = fake.calls[0]
        cmd = call["cmd"]
        assert cmd[0].endswith("devin.exe") or cmd[0] == "devin"
        assert cmd[1] == "-p"
        assert len(cmd) == 3                    # exactly one -p payload
        assert call["cwd"] == str(REPO_ROOT)

        prompt = cmd[2]
        json_path = sandbox["docs"] / PENDING_STATE["prompt_json"]
        report_path = sandbox["docs"] / PENDING_STATE["report_md"]
        assert str(json_path) in prompt
        assert str(report_path) in prompt
        assert "2026-11-20" in prompt
        # every placeholder filled — no stray braces left over
        assert "{json_path}" not in prompt
        assert "{report_path}" not in prompt
        assert "{resolved_date}" not in prompt
        assert "{" not in prompt and "}" not in prompt
        # contract content
        assert "analyze_rra.py" in prompt
        assert "Weekly read:" in prompt
        assert "**Net call:**" in prompt
        assert "weekly_summary.md" in prompt
        assert "report_index.md" in prompt
        assert "--index" in prompt

    def test_devin_exe_bundled_path_preferred(self, tmp_path, monkeypatch):
        bundled = tmp_path / reh.BUNDLED_DEVIN_REL
        bundled.parent.mkdir(parents=True)
        bundled.write_text("", encoding="utf-8")
        assert reh.find_devin_exe(str(tmp_path)) == str(bundled)

    def test_devin_exe_fallback_to_path(self, tmp_path, monkeypatch):
        import shutil
        monkeypatch.setattr(shutil, "which", lambda name: None)
        # tmp_path has no bundled layout -> falls back to bare 'devin'
        assert reh.find_devin_exe(str(tmp_path)) == "devin"

    def test_events_logged_spawn_pid_exit_markers(
            self, sandbox, monkeypatch):
        fake = _FakePopen(rc=0)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        sandbox["summary"].write_text("x\n", encoding="utf-8")
        assert reh.run() == 0
        text = sandbox["events"].read_text(encoding="utf-8")
        assert "[ENRICH-T1]" in text
        assert "spawn" in text and "pid=4321" in text
        assert "exit code 0" in text
        assert "marker verification" in text
        assert not sandbox["lock"].exists()  # released after the run


# ------------------------------------------------------------------
# Locking / mutual exclusion
# ------------------------------------------------------------------

class TestLocking:
    def _hold_lock(self, sandbox, claimed_at):
        sandbox["lock"].write_text(json.dumps(
            {"agent": "rrg_enrich_headless", "claimed_at": claimed_at}),
            encoding="utf-8")

    def test_held_lock_exits_without_spawn(self, sandbox, monkeypatch):
        fake = _FakePopen()
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        self._hold_lock(sandbox,
                        datetime.now(timezone.utc).isoformat())
        assert reh.run() == 0
        assert fake.calls == []
        assert sandbox["lock"].exists()    # foreign lock left alone
        text = sandbox["events"].read_text(encoding="utf-8")
        assert "skipped" in text

    def test_stale_lock_recovered(self, sandbox, monkeypatch):
        fake = _FakePopen(rc=0)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        stale = (datetime.now(timezone.utc)
                 - timedelta(seconds=reh.STALE_LOCK_SECONDS + 60))
        self._hold_lock(sandbox, stale.isoformat())
        assert reh.run() == 0
        assert len(fake.calls) == 1               # recovered -> spawned
        assert not sandbox["lock"].exists()       # released at the end

    def test_malformed_lock_is_stale(self, sandbox, monkeypatch):
        fake = _FakePopen(rc=0)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        sandbox["lock"].write_text("not json", encoding="utf-8")
        assert reh.run() == 0
        assert len(fake.calls) == 1


# ------------------------------------------------------------------
# State file edge cases
# ------------------------------------------------------------------

class TestStateFile:
    @pytest.mark.parametrize("content", [
        pytest.param(None, id="missing"),
        pytest.param("", id="empty"),
        pytest.param("not json {{", id="corrupt"),
        pytest.param("[1, 2]", id="not-a-dict"),
    ])
    def test_unusable_state_is_noop(self, sandbox, monkeypatch, content):
        fake = _FakePopen()
        _patch_popen(monkeypatch, fake)
        if content is not None:
            _write_state(sandbox, content)
        assert reh.run() == 0
        assert fake.calls == []
        assert not sandbox["lock"].exists()

    def test_pending_state_missing_file_names_no_spawn(
            self, sandbox, monkeypatch):
        """Pending but no prompt_json/report_md -> warning, still 0."""
        fake = _FakePopen()
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, {"report_enriched": False,
                               "resolved_date": "2026-11-20"})
        assert reh.run() == 0
        assert fake.calls == []
        assert "WARNING" in sandbox["events"].read_text(encoding="utf-8")


# ------------------------------------------------------------------
# Dry-run touches nothing
# ------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_pending_prints_one_spawn_touches_nothing(
            self, sandbox, monkeypatch, capsys):
        fake = _FakePopen()
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        sandbox["summary"].write_text("x\n", encoding="utf-8")

        assert reh.run(dry_run=True) == 0
        out = capsys.readouterr().out
        assert fake.calls == []
        assert not sandbox["lock"].exists()
        assert sandbox["events"].read_text(encoding="utf-8") == ""
        # exactly one spawn line, filled with real paths
        assert out.count("-p") == 1
        assert PENDING_STATE["report_md"] in out
        assert "2026-11-20" in out
        assert "{json_path}" not in out

    def test_dry_run_enriched_prints_noop(self, sandbox, monkeypatch,
                                        capsys):
        fake = _FakePopen()
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, ENRICHED_STATE)
        sandbox["summary"].write_text("2026-11-20 row\n", encoding="utf-8")
        assert reh.run(dry_run=True) == 0
        out = capsys.readouterr().out
        assert "no-op" in out
        assert "-p" not in out
        assert sandbox["events"].read_text(encoding="utf-8") == ""


# ------------------------------------------------------------------
# Failure semantics — never blocks the publish pipeline
# ------------------------------------------------------------------

class TestNonBlocking:
    def test_nonzero_session_exit_still_zero(self, sandbox, monkeypatch):
        fake = _FakePopen(rc=3)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        assert reh.run() == 0
        text = sandbox["events"].read_text(encoding="utf-8")
        assert "WARNING" in text and "exit code 3" in text
        assert not sandbox["lock"].exists()

    def test_spawn_oserror_still_zero(self, sandbox, monkeypatch):
        fake = _FakePopen(raises=OSError("devin not found"))
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        assert reh.run() == 0
        assert "spawn FAILED" in sandbox["events"].read_text(
            encoding="utf-8")
        assert not sandbox["lock"].exists()

    def test_timeout_kills_session(self, sandbox, monkeypatch):
        fake = _FakePopen(hang=True)
        _patch_popen(monkeypatch, fake)
        _write_state(sandbox, PENDING_STATE)
        assert reh.run() == 0
        assert fake.procs[0].killed is True
        text = sandbox["events"].read_text(encoding="utf-8")
        assert "killed" in text

    def test_internal_error_swallowed_to_exit_zero(
            self, sandbox, monkeypatch):
        monkeypatch.setattr(reh, "load_state",
                            lambda *a, **k: (_ for _ in ()).throw(
                                RuntimeError("boom")))
        assert reh.main([]) == 0


# ------------------------------------------------------------------
# Prompt template contract
# ------------------------------------------------------------------

class TestPromptTemplate:
    def test_template_covers_skill_contract(self):
        p = reh.ENRICHMENT_PROMPT
        assert "{json_path}" in p and "{report_path}" in p
        assert "{resolved_date}" in p
        # (a) draft only if missing
        assert "analyze_rra.py" in p and "does NOT exist" in p
        # (b) enrichment markers + tables verbatim
        assert "Weekly read:" in p and "Read:" in p
        assert "**Net call:**" in p and "verbatim" in p
        # (c) summary + index
        assert "weekly_summary.md" in p
        assert "report_index.md" in p and "--index" in p
        # (d) scope constraint
        assert "ONLY docs/rra/*.md" in p
        assert "EVENTS.log" in p
        # (e) stop when markers verify
        assert "VERIFY" in p and "STOP" in p

    def test_template_formats_cleanly(self, sandbox):
        state = dict(PENDING_STATE)
        cmd = reh.build_command(state, devin_exe="devin.exe")
        assert cmd is not None and cmd[0] == "devin.exe" and cmd[1] == "-p"
        assert "{" not in cmd[2]
