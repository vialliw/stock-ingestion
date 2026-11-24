"""Tests for scripts/rrg_weekly_pipeline.py — date math, enrichment
gate, filename resolution, and --dry-run behavior.

All tests are hermetic: no network, no DuckDB/Flight, no git.  Gate
logic runs against tmp_path fixtures; dry-run tests prove the pipeline
never reaches the subprocess/clone layer.
"""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rrg_weekly_pipeline as rp  # noqa: E402

sys.path.insert(0, str(REPO_ROOT / ".devin" / "skills" / "summarize_rra"))
import summarize_rra as sr  # noqa: E402


# ------------------------------------------------------------------
# Date math — last_friday
# ------------------------------------------------------------------

class TestLastFriday:
    """last_friday snaps every weekday to the most recent Friday."""

    @pytest.mark.parametrize("today,expected", [
        (date(2026, 11, 16), date(2026, 11, 13)),   # Monday -> prior Fri
        (date(2026, 11, 17), date(2026, 11, 13)),   # Tuesday
        (date(2026, 11, 18), date(2026, 11, 13)),   # Wednesday
        (date(2026, 11, 19), date(2026, 11, 13)),   # Thursday
        (date(2026, 11, 20), date(2026, 11, 20)),   # Friday -> itself
        (date(2026, 11, 21), date(2026, 11, 20)),   # Saturday -> yesterday
        (date(2026, 11, 22), date(2026, 11, 20)),   # Sunday -> Friday
        (date(2026, 1, 1), date(2025, 12, 26)),     # Thursday, year wrap
    ])
    def test_weekday_mapping(self, today, expected):
        assert rp.last_friday(today) == expected
        assert expected.weekday() == 4

    def test_default_uses_today(self):
        got = rp.last_friday()
        assert got <= date.today()
        assert got.weekday() == 4
        assert (date.today() - got).days <= 6


# ------------------------------------------------------------------
# Filename resolution
# ------------------------------------------------------------------

class TestFilenameResolution:
    def test_prompt_to_report_name(self):
        assert rp.prompt_to_report_name(
            "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_prompt.json"
        ) == "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md"

    def test_prompt_to_report_name_matches_dated_report_regex(self):
        """The derived report name must match the dated-report regex so
        summarize_rrg.py picks it up."""
        name = rp.prompt_to_report_name(
            "Relative_Rotation_Dashboard_ETFs_2026-07-04_analysis_prompt.json")
        assert rp.DATED_REPORT_RE.search(name)
        assert rp.report_date_from_name(name) == "2026-07-04"

    def test_prompt_to_report_name_generic_fallback(self):
        assert rp.prompt_to_report_name("foo_prompt_bar.json") == \
            "foo_report_bar.md"

    def test_report_date_from_name(self):
        assert rp.report_date_from_name(
            "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md"
        ) == "2026-11-13"
        assert rp.report_date_from_name(
            "Relative_Rotation_Dashboard_ETFs_analysis_report20261113.md"
        ) is None  # flagship name is not dated-pattern
        assert rp.report_date_from_name("weekly_summary.md") is None

    def test_resolved_date_from_prompt_payload(self, tmp_path):
        p = tmp_path / "Relative_Rotation_Dashboard_ETFs_2026-07-04_analysis_prompt.json"
        p.write_text(json.dumps(
            {"market_snapshot": {"analysis_date": "2026-07-02"}}),
            encoding="utf-8")
        # payload (actual bar date) wins over filename (requested as_of)
        assert rp.resolved_date_from_prompt(p) == "2026-07-02"

    def test_resolved_date_from_prompt_filename_fallback(self, tmp_path):
        p = tmp_path / "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_prompt.json"
        p.write_text("not json", encoding="utf-8")
        assert rp.resolved_date_from_prompt(p) == "2026-11-13"


# ------------------------------------------------------------------
# Enrichment gate
# ------------------------------------------------------------------

ENRICHED = (
    "# RRG Analysis Report — 2026-11-13\n\n"
    "## 1. Quadrant Census\n\n"
    "> Weekly read: metals lead.\n\n"
    "## 7. Actionable Portfolio Tilts\n\n"
    "**Net call:** overweight metals.\n"
)
ENRICHED_READ = ENRICHED.replace("> Weekly read:", "> Read:")
DRAFT = "# RRG Analysis Report — 2026-11-13\n\n## 1. Quadrant Census\n\n(no narrative)\n"
NO_NETCALL = ENRICHED.replace("**Net call:** overweight metals.\n", "")


def _dated(tmp_path: Path, d: str, text: str) -> Path:
    p = tmp_path / f"Relative_Rotation_Dashboard_ETFs_{d}_analysis_report.md"
    p.write_text(text, encoding="utf-8")
    return p


class TestReportGate:
    def test_weekly_read_plus_netcall_passes(self, tmp_path):
        assert rp.report_is_enriched(_dated(tmp_path, "2026-11-13", ENRICHED))

    def test_read_alias_passes(self, tmp_path):
        assert rp.report_is_enriched(_dated(tmp_path, "2026-11-13", ENRICHED_READ))

    def test_draft_blocked(self, tmp_path):
        assert not rp.report_is_enriched(_dated(tmp_path, "2026-11-13", DRAFT))

    def test_missing_netcall_blocked(self, tmp_path):
        assert not rp.report_is_enriched(_dated(tmp_path, "2026-11-13", NO_NETCALL))

    def test_missing_file_blocked(self, tmp_path):
        assert not rp.report_is_enriched(tmp_path / "nope.md")


# Draft placeholders exactly as summarize_rrg.py emits them (unenriched
# draft): 'TODO(agent):' arc line, 'TODO:' row placeholder, and
# '<!-- missing ... -->' markers on partially-enriched rows.
DRAFT_SUMMARY_ARC = (
    "# RRG Weekly Summary Table — ETF Dashboard vs SPY\n\n"
    "2 weekly reports, 2026-11-06 \u2192 2026-11-13 (12-week lookback each).\n"
    "TODO(agent): add the cross-week arc recap here \u2014 regime phases + "
    "inflection dates.\n\n"
    "| Date | Weekly summary |\n"
    "|------|----------------|\n"
    "| 2026-11-06 | metals lead. Net call: overweight metals. |\n"
    "| 2026-11-13 | energy rotates. Net call: hold. |\n"
)
DRAFT_SUMMARY_TODO_ROW = (
    "# RRG Weekly Summary Table\n\n"
    "| Date | Weekly summary |\n"
    "|------|----------------|\n"
    "| 2026-11-06 | metals lead. Net call: overweight metals. |\n"
    "| 2026-11-13 | TODO: no Weekly read / Net call found \u2014 "
    "read the full report. |\n"
)
DRAFT_SUMMARY_MISSING = (
    "# RRG Weekly Summary Table\n\n"
    "| Date | Weekly summary |\n"
    "|------|----------------|\n"
    "| 2026-11-06 | metals lead. |\n"
    "| 2026-11-13 | energy rotates. <!-- missing net_call --> |\n"
)
POLISHED_SUMMARY = (
    "# RRG Weekly Summary Table\n\n"
    "Arc: leadership rotated from metals into energy across two weeks.\n\n"
    "| Date | Weekly summary |\n"
    "|------|----------------|\n"
    "| 2026-11-06 | Metals led while energy lagged. Net call: overweight. |\n"
    "| 2026-11-13 | Energy improved into leading. Net call: hold. |\n"
)


class TestSummaryGate:
    def test_missing_dates_listed(self, tmp_path):
        s = tmp_path / "weekly_summary.md"
        s.write_text("| 2026-11-06 | row |\n", encoding="utf-8")
        assert rp.summary_missing_dates(
            s, ["2026-11-06", "2026-11-13"]) == ["2026-11-13"]

    def test_absent_summary_lists_all(self, tmp_path):
        assert rp.summary_missing_dates(
            tmp_path / "weekly_summary.md", ["2026-11-13"]) == ["2026-11-13"]

    def test_draft_markers_todo_arc(self, tmp_path):
        s = tmp_path / "weekly_summary.md"
        s.write_text(DRAFT_SUMMARY_ARC, encoding="utf-8")
        assert "TODO" in rp.summary_draft_markers(s)

    def test_draft_markers_todo_row(self, tmp_path):
        s = tmp_path / "weekly_summary.md"
        s.write_text(DRAFT_SUMMARY_TODO_ROW, encoding="utf-8")
        assert "TODO" in rp.summary_draft_markers(s)

    def test_draft_markers_missing_marker(self, tmp_path):
        s = tmp_path / "weekly_summary.md"
        s.write_text(DRAFT_SUMMARY_MISSING, encoding="utf-8")
        assert "<!-- missing" in rp.summary_draft_markers(s)

    def test_polished_summary_has_no_markers(self, tmp_path):
        s = tmp_path / "weekly_summary.md"
        s.write_text(POLISHED_SUMMARY, encoding="utf-8")
        assert rp.summary_draft_markers(s) == []

    def test_unreadable_summary_reports_no_markers(self, tmp_path):
        # Unreadable is a missing-dates problem, not a marker problem.
        assert rp.summary_draft_markers(tmp_path / "nope.md") == []


class TestPlanPublish:
    def _seed(self, tmp_path):
        _dated(tmp_path, "2026-11-06", ENRICHED)
        _dated(tmp_path, "2026-11-13", DRAFT)  # unenriched -> skipped
        (tmp_path / "weekly_summary.md").write_text(
            "# Summary\n| 2026-11-06 | row |\n", encoding="utf-8")
        (tmp_path / "Relative_Rotation_Dashboard_ETFs_analysis_report20261113.md") \
            .write_text(ENRICHED, encoding="utf-8")
        (tmp_path / "Relative_Rotation_Dashboard_ETFs_2026-11-06_analysis_prompt.json") \
            .write_text("{}", encoding="utf-8")

    def test_json_never_considered(self, tmp_path):
        self._seed(tmp_path)
        plan = rp.plan_publish(tmp_path)
        assert all(not n.endswith(".json") for n in plan["publish"])
        assert all(not i["name"].endswith(".json") for i in plan["skip"])

    def test_enriched_shipped_draft_skipped(self, tmp_path):
        self._seed(tmp_path)
        plan = rp.plan_publish(tmp_path)
        assert "Relative_Rotation_Dashboard_ETFs_2026-11-06_analysis_report.html" \
            in plan["publish"]  # reports ship as .html
        skipped = {i["name"] for i in plan["skip"]}
        assert "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md" \
            in skipped

    def test_summary_ships_only_when_it_covers_shipped_dates(self, tmp_path):
        self._seed(tmp_path)
        # summary covers 11-06 (the only enriched dated report) -> ships
        plan = rp.plan_publish(tmp_path)
        assert "weekly_summary.md" in plan["publish"]

    def test_summary_blocked_when_shipped_report_date_missing(self, tmp_path):
        self._seed(tmp_path)
        _dated(tmp_path, "2026-11-20", ENRICHED)  # enriched but not in summary
        plan = rp.plan_publish(tmp_path)
        skipped = {i["name"]: i["reason"] for i in plan["skip"]}
        assert "weekly_summary.md" in skipped
        assert "2026-11-20" in skipped["weekly_summary.md"]

    def test_summary_blocked_when_resolved_date_missing(self, tmp_path):
        self._seed(tmp_path)
        plan = rp.plan_publish(tmp_path, resolved_date="2026-11-13")
        skipped = {i["name"] for i in plan["skip"]}
        assert "weekly_summary.md" in skipped

    def test_flagship_uses_marker_gate(self, tmp_path):
        self._seed(tmp_path)
        plan = rp.plan_publish(tmp_path)
        assert "Relative_Rotation_Dashboard_ETFs_analysis_report20261113.html" \
            in plan["publish"]  # flagship also ships as .html

    def test_no_summary_file_means_no_summary_row(self, tmp_path):
        _dated(tmp_path, "2026-11-06", ENRICHED)
        plan = rp.plan_publish(tmp_path)
        assert "weekly_summary.md" not in plan["publish"]
        assert all(i["name"] != "weekly_summary.md" for i in plan["skip"])

    @pytest.mark.parametrize("draft_text", [
        DRAFT_SUMMARY_ARC,        # 'TODO(agent):' arc placeholder
        DRAFT_SUMMARY_TODO_ROW,   # 'TODO:' row placeholder
        DRAFT_SUMMARY_MISSING,    # '<!-- missing ... -->' marker
    ])
    def test_drafted_summary_never_publishable(
            self, tmp_path, draft_text):
        """A summary still carrying draft placeholders is skipped even
        though it names every shipped report's date."""
        _dated(tmp_path, "2026-11-06", ENRICHED)
        _dated(tmp_path, "2026-11-13", ENRICHED)
        (tmp_path / "weekly_summary.md").write_text(
            draft_text, encoding="utf-8")
        plan = rp.plan_publish(tmp_path, resolved_date="2026-11-13")
        assert "weekly_summary.md" not in plan["publish"]
        skipped = {i["name"]: i["reason"] for i in plan["skip"]}
        assert "weekly_summary.md" in skipped
        assert "draft placeholder" in skipped["weekly_summary.md"]

    def test_polished_summary_still_ships(self, tmp_path):
        """The strengthened gate must not block a genuinely enriched
        summary that covers every shipped date."""
        _dated(tmp_path, "2026-11-06", ENRICHED)
        _dated(tmp_path, "2026-11-13", ENRICHED)
        (tmp_path / "weekly_summary.md").write_text(
            POLISHED_SUMMARY, encoding="utf-8")
        plan = rp.plan_publish(tmp_path, resolved_date="2026-11-13")
        assert "weekly_summary.md" in plan["publish"]

    def test_draft_marker_and_missing_date_reasons_combine(
            self, tmp_path):
        """A draft missing a shipped date reports BOTH reasons."""
        _dated(tmp_path, "2026-11-06", ENRICHED)
        _dated(tmp_path, "2026-11-20", ENRICHED)
        (tmp_path / "weekly_summary.md").write_text(
            DRAFT_SUMMARY_ARC, encoding="utf-8")  # lacks 2026-11-20 row
        plan = rp.plan_publish(tmp_path)
        skipped = {i["name"]: i["reason"] for i in plan["skip"]}
        reason = skipped["weekly_summary.md"]
        assert "2026-11-20" in reason and "draft placeholder" in reason


# ------------------------------------------------------------------
# Dry-run — plan only, never subprocesses/clones/pushes
# ------------------------------------------------------------------

class TestDryRun:
    def test_generate_dry_run_prints_plan(self, capsys):
        rp.generate(dry_run=True, today=date(2026, 11, 16))
        out = capsys.readouterr().out
        assert "2026-11-13" in out          # last Friday
        assert "generate_rrg_dashboard" in out
        assert "weekly_summary.md" in out
        assert ".rrg_pipeline_state.json" in out

    def test_generate_dry_run_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(rp, "DOCS_RRG", tmp_path)
        monkeypatch.setattr(rp, "SUMMARY_FILE", tmp_path / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            tmp_path / ".rrg_pipeline_state.json")
        rp.generate(dry_run=True, today=date(2026, 11, 16))
        assert list(tmp_path.iterdir()) == []  # nothing written

    def test_publish_dry_run_never_runs_subprocess(
            self, tmp_path, monkeypatch, capsys):
        _dated(tmp_path, "2026-11-13", ENRICHED)
        monkeypatch.setattr(rp, "DOCS_RRG", tmp_path)
        monkeypatch.setattr(rp, "SUMMARY_FILE", tmp_path / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            tmp_path / ".rrg_pipeline_state.json")

        def _boom(*a, **k):
            raise AssertionError("subprocess invoked during dry-run")

        monkeypatch.setattr(rp.subprocess, "run", _boom)
        rc = rp.publish(dry_run=True)
        out = capsys.readouterr().out
        assert rc == 0
        assert "ls-remote" in out and "clone" in out
        assert "would ship report/Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.html" in out
        assert "would render 1 report .md" in out

    def test_publish_dry_run_makes_no_tmp_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(rp, "DOCS_RRG", tmp_path)
        monkeypatch.setattr(rp, "CLONE_DIR", tmp_path / "clone")
        assert rp.publish(dry_run=True) == 0
        assert not (tmp_path / "clone").exists()

    def test_main_all_dry_run(self, capsys):
        assert rp.main(["--all", "--dry-run"]) == 0
        out = capsys.readouterr().out
        assert "generate_rrg_dashboard" in out and "ls-remote" in out

    def test_main_generate_dry_run(self, capsys):
        assert rp.main(["--generate", "--dry-run"]) == 0
        assert "as_of" in capsys.readouterr().out

    def test_main_publish_dry_run(self, capsys):
        assert rp.main(["--publish", "--dry-run"]) == 0
        assert "dry-run" in capsys.readouterr().out

    def test_main_no_phase_flag_defaults_to_all(self, capsys):
        """No phase flag == --all (both phases run, here as dry-run)."""
        assert rp.main(["--dry-run"]) == 0
        out = capsys.readouterr().out
        assert "generate_rrg_dashboard" in out and "ls-remote" in out


class TestCliFlagConflicts:
    """--all is mutually exclusive with the single-phase flags."""

    @pytest.mark.parametrize("argv", [
        ["--all", "--generate"],
        ["--all", "--publish"],
        ["--all", "--generate", "--publish"],
        ["--all", "--generate", "--dry-run"],
    ])
    def test_all_with_phase_flag_exits_two(self, argv, capsys):
        with pytest.raises(SystemExit) as exc:
            rp.main(argv)
        assert exc.value.code == 2
        assert "--all cannot be combined" in capsys.readouterr().err

    def test_generate_and_publish_together_allowed(self, capsys):
        """--generate --publish (no --all) is explicit 'both' -> runs."""
        assert rp.main(["--generate", "--publish", "--dry-run"]) == 0
        out = capsys.readouterr().out
        assert "generate_rrg_dashboard" in out and "ls-remote" in out


# ------------------------------------------------------------------
# Publish no-op paths (no network needed)
# ------------------------------------------------------------------

class TestPublishGating:
    def test_nothing_publishable_returns_zero_no_clone(
            self, tmp_path, monkeypatch):
        """All drafts -> gate blocks everything -> no clone attempted."""
        _dated(tmp_path, "2026-11-13", DRAFT)
        monkeypatch.setattr(rp, "DOCS_RRG", tmp_path)
        monkeypatch.setattr(rp, "SUMMARY_FILE", tmp_path / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            tmp_path / ".rrg_pipeline_state.json")
        monkeypatch.setattr(rp, "EVENTS_LOG", tmp_path / "EVENTS.log")
        monkeypatch.setattr(rp, "CLONE_DIR", tmp_path / "clone")

        def _boom(*a, **k):
            raise AssertionError("git invoked with nothing publishable")

        monkeypatch.setattr(rp.subprocess, "run", _boom)
        assert rp.publish(dry_run=False) == 0
        assert not (tmp_path / "clone").exists()

    def test_drafted_summary_never_reaches_remote_set(
            self, tmp_path, monkeypatch):
        """End-to-end: a drafted (placeholder-carrying) summary is never
        copied into the remote clone's report/ even when dated reports
        pass the gate and the push path executes."""
        src = tmp_path / "src"
        src.mkdir()
        report_name = ("Relative_Rotation_Dashboard_ETFs_"
                       "2026-11-13_analysis_report.md")
        report_html = report_name[:-3] + ".html"
        (src / report_name).write_text(ENRICHED, encoding="utf-8")
        (src / "weekly_summary.md").write_text(
            DRAFT_SUMMARY_ARC, encoding="utf-8")
        monkeypatch.setattr(rp, "DOCS_RRG", src)
        monkeypatch.setattr(rp, "SUMMARY_FILE", src / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            src / ".rrg_pipeline_state.json")
        monkeypatch.setattr(rp, "EVENTS_LOG", tmp_path / "EVENTS.log")
        monkeypatch.setattr(rp, "CLONE_DIR", tmp_path / "clone")
        monkeypatch.setattr(rp, "toast", lambda *a, **k: None)
        # Publish renders .html via a subprocess; stub it to emit a file.
        monkeypatch.setattr(
            rp, "_render_html",
            lambda md, html: html.write_text("<html/>", encoding="utf-8"))

        copied = []
        real_copy = rp.shutil.copyfile

        def _rec(src_f, dst):
            copied.append(Path(dst).name)
            return real_copy(src_f, dst)

        monkeypatch.setattr(rp.shutil, "copyfile", _rec)

        def _fake_run(cmd, cwd=None, timeout=300):
            if cmd[:2] == ["git", "clone"]:
                Path(cmd[-1]).mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(
                cmd, 0, stdout="", stderr="")

        def _fake_git(clone, *a, timeout=300):
            out = ""
            if a[:2] == ("status", "--porcelain"):
                out = " M report"  # force a diff so commit/push run
            elif a[:1] == ("rev-parse",):
                out = "abc1234"
            return subprocess.CompletedProcess(
                ["git", *a], 0, stdout=out, stderr="")

        monkeypatch.setattr(rp, "_run", _fake_run)  # ls-remote + clone succeed
        monkeypatch.setattr(rp, "_git", _fake_git)

        assert rp.publish(dry_run=False) == 0
        # The enriched report shipped AS .html (the .md never ships);
        # the draft summary did not ship either.
        assert report_html in copied
        assert report_name not in copied
        assert "weekly_summary.md" not in copied


# ------------------------------------------------------------------
# _force_rmtree — read-only debris removal (2026-11-17 incident)
# ------------------------------------------------------------------

class TestForceRmtree:
    """Regression for the 2026-11-17 publish-clone incident: git marks
    .git/objects read-only on Windows, a plain
    shutil.rmtree(ignore_errors=True) silently left the tree behind,
    and the NEXT run's clone died with exit 128 'destination path ...
    is not an empty directory'."""

    def test_readonly_git_style_tree_fully_removed(self, tmp_path):
        clone = tmp_path / "clone"
        obj_dir = clone / ".git" / "objects" / "ab"
        obj_dir.mkdir(parents=True)
        pack = obj_dir / "abcdef1234567890"
        pack.write_bytes(b"\x00" * 32)
        head = clone / ".git" / "HEAD"
        head.write_text("ref: refs/heads/main\n", encoding="utf-8")
        report = clone / "report"
        report.mkdir()
        (report / "x.md").write_text("x\n", encoding="utf-8")
        # git marks pack objects read-only — the exact debris left by a
        # real clone that broke the 09:58 publish.
        for f in (pack, head):
            os.chmod(f, stat.S_IREAD)
        # Prove the debris is actually read-only before removal.
        assert not (os.stat(pack).st_mode & stat.S_IWRITE)

        rp._force_rmtree(clone)

        assert not clone.exists()

    def test_nested_readonly_dirs_and_files(self, tmp_path):
        root = tmp_path / "deep"
        leaf = root / "a" / "b" / "c"
        leaf.mkdir(parents=True)
        for i in range(3):
            f = leaf / f"obj{i}"
            f.write_text(str(i), encoding="utf-8")
            os.chmod(f, stat.S_IREAD)
        os.chmod(leaf, stat.S_IREAD)  # read-only directory entry too

        rp._force_rmtree(root)

        assert not root.exists()

    def test_missing_dir_is_noop(self, tmp_path):
        rp._force_rmtree(tmp_path / "nope")  # must not raise
        rp._force_rmtree(tmp_path / "nope" / "nested")

    def test_normal_tree_removed(self, tmp_path):
        d = tmp_path / "plain"
        (d / "sub").mkdir(parents=True)
        (d / "sub" / "f.txt").write_text("f", encoding="utf-8")
        rp._force_rmtree(d)
        assert not d.exists()

    def test_publish_preclone_wipe_clears_readonly_debris(
            self, tmp_path, monkeypatch):
        """End-to-end: leftover read-only .git debris from a previous
        publish must not make the next clone step see a non-empty dir
        — the pre-clone wipe removes it before `git clone` runs."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "Relative_Rotation_Dashboard_ETFs_"
         "2026-11-13_analysis_report.md").write_text(
            ENRICHED, encoding="utf-8")
        monkeypatch.setattr(rp, "DOCS_RRG", src)
        monkeypatch.setattr(rp, "SUMMARY_FILE", src / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            src / ".rrg_pipeline_state.json")
        monkeypatch.setattr(rp, "EVENTS_LOG", tmp_path / "EVENTS.log")
        monkeypatch.setattr(rp, "toast", lambda *a, **k: None)

        # Simulate debris surviving from the previous run's cleanup.
        clone = tmp_path / "clone"
        debris = clone / ".git" / "objects" / "ab"
        debris.mkdir(parents=True)
        pack = debris / "pack-readonly"
        pack.write_bytes(b"\x00" * 8)
        os.chmod(pack, stat.S_IREAD)
        monkeypatch.setattr(rp, "CLONE_DIR", clone)
        monkeypatch.setattr(
            rp, "_render_html",
            lambda md, html: html.write_text("<html/>", encoding="utf-8"))

        def _fake_run(cmd, cwd=None, timeout=300):
            if cmd[:2] == ["git", "clone"]:
                # A real git clone exits 128 when the destination exists
                # and is non-empty — require the wipe to have cleared it.
                dest = Path(cmd[-1])
                assert not dest.exists() or not any(dest.iterdir()), \
                    "clone target not empty — read-only debris survived"
                dest.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(
                cmd, 0, stdout="", stderr="")

        def _fake_git(clone_dir, *a, timeout=300):
            # Empty porcelain -> "already in sync" early return (no push).
            return subprocess.CompletedProcess(
                ["git", *a], 0, stdout="", stderr="")

        monkeypatch.setattr(rp, "_run", _fake_run)
        monkeypatch.setattr(rp, "_git", _fake_git)

        assert rp.publish(dry_run=False) == 0
        # finally-cleanup removed the fresh clone too.
        assert not clone.exists()


# ------------------------------------------------------------------
# RRG-HTML-T1 — publish-set .md -> .html translation + mirror rules
# ------------------------------------------------------------------

class TestPublishNameFor:
    """publish_name_for maps report .md -> same-stem .html; summary,
    index, and other .md ship unchanged."""

    def test_dated_report_translates_to_html(self):
        assert rp.publish_name_for(
            "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md"
        ) == ("Relative_Rotation_Dashboard_ETFs_"
                  "2026-11-13_analysis_report.html")

    def test_flagship_report_translates_to_html(self):
        assert rp.publish_name_for(
            "Relative_Rotation_Dashboard_ETFs_analysis_report20261113.md"
        ) == ("Relative_Rotation_Dashboard_ETFs_"
                  "analysis_report20261113.html")

    def test_summary_and_index_stay_md(self):
        assert rp.publish_name_for("weekly_summary.md") == "weekly_summary.md"
        assert rp.publish_name_for("report_index.md") == "report_index.md"

    def test_other_md_unchanged(self):
        assert rp.publish_name_for("notes.md") == "notes.md"


class TestPlanPublishHtml:
    """plan_publish returns SHIPPED names: reports -> .html, the rest
    stay .md; skip names stay the local .md."""

    def test_reports_ship_html_summary_and_index_ship_md(self, tmp_path):
        _dated(tmp_path, "2026-11-06", ENRICHED)
        _dated(tmp_path, "2026-11-13", DRAFT)  # fails the gate
        (tmp_path / "weekly_summary.md").write_text(
            POLISHED_SUMMARY, encoding="utf-8")
        (tmp_path / "report_index.md").write_text(
            "# index\n", encoding="utf-8")
        plan = rp.plan_publish(tmp_path)
        assert ("Relative_Rotation_Dashboard_ETFs_"
                "2026-11-06_analysis_report.html") in plan["publish"]
        assert all(not n.endswith("_analysis_report.md")
                   for n in plan["publish"])
        assert "weekly_summary.md" in plan["publish"]
        assert "report_index.md" in plan["publish"]
        skipped = {i["name"] for i in plan["skip"]}
        # skip keeps the LOCAL .md name (the gated-out source).
        assert ("Relative_Rotation_Dashboard_ETFs_"
                "2026-11-13_analysis_report.md") in skipped


# ------------------------------------------------------------------
# Mirror-sync deletion rules + .html rendering on the publish path
# ------------------------------------------------------------------

def _run_publish(tmp_path, monkeypatch, src_files, remote_files,
                 prune=False):
    """Drive publish() with faked git/render; returns (rc, remote_names,
    render_calls).  ``src_files`` seeds DOCS_RRG; ``remote_files`` is
    materialized into the fresh clone's report/ by the fake clone.
    ``remote_names`` is a snapshot of the remote report/ dir taken at
    the post-sync ``git status`` call — the finally-cleanup deletes the
    clone before publish() returns, so post-hoc inspection is impossible.
    ``prune`` is threaded to publish() (dated-artifact deletion opt-in)."""
    src = tmp_path / "src"
    src.mkdir()
    for name, text in src_files.items():
        (src / name).write_text(text, encoding="utf-8")
    monkeypatch.setattr(rp, "DOCS_RRG", src)
    monkeypatch.setattr(rp, "SUMMARY_FILE", src / "weekly_summary.md")
    monkeypatch.setattr(rp, "STATE_FILE",
                        src / ".rrg_pipeline_state.json")
    monkeypatch.setattr(rp, "EVENTS_LOG", tmp_path / "EVENTS.log")
    clone = tmp_path / "clone"
    monkeypatch.setattr(rp, "CLONE_DIR", clone)
    monkeypatch.setattr(rp, "toast", lambda *a, **k: None)

    render_calls = []

    def _fake_render(md_path, html_path):
        render_calls.append((md_path.name, html_path.name))
        html_path.write_text("<html/>", encoding="utf-8")

    monkeypatch.setattr(rp, "_render_html", _fake_render)

    def _fake_run(cmd, cwd=None, timeout=300):
        if cmd[:2] == ["git", "clone"]:
            dest = Path(cmd[-1])
            rdir = dest / "report"
            rdir.mkdir(parents=True, exist_ok=True)
            for name, text in remote_files.items():
                (rdir / name).write_text(text, encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    remote_names: list[str] = []

    def _fake_git(clone_dir, *a, timeout=300):
        out = ""
        if a[:2] == ("status", "--porcelain"):
            remote_names[:] = sorted(
                p.name for p in (clone_dir / "report").iterdir())
            out = " M report"  # force a diff so commit/push run
        elif a[:1] == ("rev-parse",):
            out = "abc1234"
        return subprocess.CompletedProcess(
            ["git", *a], 0, stdout=out, stderr="")

    monkeypatch.setattr(rp, "_run", _fake_run)
    monkeypatch.setattr(rp, "_git", _fake_git)

    rc = rp.publish(dry_run=False, prune=prune)
    return rc, remote_names, render_calls


_R11_06 = "Relative_Rotation_Dashboard_ETFs_2026-11-06_analysis_report.md"
_R11_13 = "Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md"


class TestMirrorSyncDeletion:
    """AC4 deletion semantics on the remote clone's report/ dir."""

    def test_superseded_remote_md_deleted(self, tmp_path, monkeypatch,
                                          capsys):
        """Remote .md whose same-stem .html ships -> deleted."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: ENRICHED},
            remote_files={_R11_13: "old .md copy"})
        assert rc == 0
        assert remote == [_R11_13[:-3] + ".html"]
        assert "superseded" in capsys.readouterr().out

    def test_gate_failed_local_md_protects_remote_md(
            self, tmp_path, monkeypatch):
        """A local .md that failed the gate still protects its remote
        .md — the draft must not delete the last good copy."""
        rc, remote, renders = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: DRAFT,       # fails gate -> protects
                       _R11_06: ENRICHED},   # publishable
            remote_files={_R11_13: "stale remote .md",
                          _R11_06: "old .md"})
        assert rc == 0
        assert _R11_13 in remote                        # protected, kept
        assert _R11_06 not in remote                    # superseded, deleted
        assert _R11_06[:-3] + ".html" in remote
        # only the enriched report was rendered
        assert renders == [(_R11_06, _R11_06[:-3] + ".html")]

    def test_remote_only_md_and_html_deleted(self, tmp_path, monkeypatch):
        """Non-dated remote files with no local source are pruned without
        --prune (dated remote-only artifacts are HELD — see below)."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: ENRICHED},
            remote_files={"stale_notes.md": "x", "orphan_page.html": "x"})
        assert rc == 0
        assert remote == [_R11_13[:-3] + ".html"]

    def test_remote_md_in_publish_set_kept(self, tmp_path, monkeypatch):
        """Synced .md files (weekly_summary/report_index) are neither
        re-deleted nor treated as remote-only."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: ENRICHED,
                       "weekly_summary.md": POLISHED_SUMMARY,
                       "report_index.md": "# index\n"},
            remote_files={"weekly_summary.md": "old",
                          "report_index.md": "old"})
        assert rc == 0
        assert sorted(remote) == sorted(
            ["weekly_summary.md", "report_index.md",
             _R11_13[:-3] + ".html"])


class TestPublishHardening:
    """RRG-HTML-T2: last-good .html parity protection + the --prune
    opt-in for dated-artifact deletion.  (Superseded-.md auto-deletion
    without --prune is covered by test_superseded_remote_md_deleted —
    its remote .md IS a dated name and still deletes.)"""

    _OLD_MD = ("Relative_Rotation_Dashboard_ETFs_"
               "2020-01-01_analysis_report.md")
    _OLD_HTML = _OLD_MD[:-3] + ".html"

    def test_gate_failed_local_md_protects_remote_html(
            self, tmp_path, monkeypatch):
        """Parity: a gate-failed local .md protects its remote same-stem
        .html exactly as it protects its remote .md — a regressed draft
        must not 404 the last good published report."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: DRAFT,       # fails gate -> protects
                       _R11_06: ENRICHED},   # publishable
            remote_files={_R11_13: "stale remote .md",
                          _R11_13[:-3] + ".html": "live .html"})
        assert rc == 0
        assert _R11_13 in remote                  # .md protected (as before)
        assert _R11_13[:-3] + ".html" in remote   # .html now protected too

    def test_dated_remote_only_held_without_prune(
            self, tmp_path, monkeypatch, capsys):
        """Dated remote-only artifacts (.md AND .html) are HELD — left
        on remote, printed as held, counted in the summary."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: ENRICHED},
            remote_files={self._OLD_MD: "x", self._OLD_HTML: "x"})
        assert rc == 0
        assert self._OLD_MD in remote and self._OLD_HTML in remote
        out = capsys.readouterr().out
        assert f"held {self._OLD_MD}" in out
        assert f"held {self._OLD_HTML}" in out
        assert "--prune" in out and "2 held" in out

    def test_dated_remote_only_deleted_with_prune(
            self, tmp_path, monkeypatch, capsys):
        """--prune opts in to deleting dated remote-only artifacts."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: ENRICHED},
            remote_files={self._OLD_MD: "x", self._OLD_HTML: "x"},
            prune=True)
        assert rc == 0
        assert remote == [_R11_13[:-3] + ".html"]
        out = capsys.readouterr().out
        assert f"deleted {self._OLD_MD} (remote-only)" in out
        assert f"deleted {self._OLD_HTML} (remote-only)" in out
        assert "2 remote deleted" in out

    def test_log_event_records_deletions_and_holds(
            self, tmp_path, monkeypatch):
        """Every deletion AND every held-deletion lands in EVENTS.log
        with file name + reason."""
        rc, remote, _ = _run_publish(
            tmp_path, monkeypatch,
            src_files={_R11_13: ENRICHED},
            remote_files={_R11_13: "old .md",        # superseded -> deleted
                          self._OLD_HTML: "x"})      # dated -> held
        assert rc == 0
        log = (tmp_path / "EVENTS.log").read_text(encoding="utf-8")
        assert "publish deleted remote file(s)" in log
        assert f"{_R11_13} (superseded" in log
        assert "publish held dated remote file(s)" in log
        assert f"{self._OLD_HTML} (remote-only)" in log

    def test_dry_run_shows_held_vs_deleted(
            self, tmp_path, monkeypatch, capsys):
        """Dry-run plan output describes both outcomes: non-dated
        deletions happen, dated remote-only artifacts are HELD unless
        --prune."""
        _dated(tmp_path, "2026-11-13", ENRICHED)
        monkeypatch.setattr(rp, "DOCS_RRG", tmp_path)
        monkeypatch.setattr(rp, "SUMMARY_FILE",
                            tmp_path / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            tmp_path / ".rrg_pipeline_state.json")
        assert rp.publish(dry_run=True) == 0
        out = capsys.readouterr().out
        assert "HELD" in out and "--prune" in out and "deleted" in out


class TestRenderHtml:
    """_render_html shells rra_html.py per gated report; failures are
    loud (publish aborts, remote untouched)."""

    def test_invokes_rra_html_script(self, tmp_path, monkeypatch):
        calls = []

        def fake_run(cmd, cwd=None, timeout=300):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        monkeypatch.setattr(rp, "_run", fake_run)
        rp._render_html(tmp_path / "a.md", tmp_path / "a.html")
        cmd = calls[0]
        assert cmd[0] == sys.executable
        assert cmd[1].endswith("rra_html.py")
        assert cmd[2].endswith("a.md") and "--out" in cmd
        assert cmd[cmd.index("--out") + 1].endswith("a.html")

    def test_failure_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            rp, "_run",
            lambda *a, **k: subprocess.CompletedProcess(a[0], 1, "", "boom"))
        with pytest.raises(RuntimeError, match="rra_html"):
            rp._render_html(tmp_path / "a.md", tmp_path / "a.html")

    def test_render_runs_before_clone(self, tmp_path, monkeypatch):
        """Render happens before the network preflight — artifacts are
        local, and a render failure must abort before any clone."""
        order = []
        src = tmp_path / "src"
        src.mkdir()
        (src / _R11_13).write_text(ENRICHED, encoding="utf-8")
        monkeypatch.setattr(rp, "DOCS_RRG", src)
        monkeypatch.setattr(rp, "SUMMARY_FILE", src / "weekly_summary.md")
        monkeypatch.setattr(rp, "STATE_FILE",
                            src / ".rrg_pipeline_state.json")
        monkeypatch.setattr(rp, "EVENTS_LOG", tmp_path / "EVENTS.log")

        def _rec_render(md, html):
            order.append("render")
            html.write_text("<html/>", encoding="utf-8")

        def _rec_run(cmd, cwd=None, timeout=300):
            order.append(cmd[1] if cmd[:2] == ["git", "ls-remote"]
                         else cmd[:2])
            return subprocess.CompletedProcess(cmd, 1, "", "no net")

        monkeypatch.setattr(rp, "_render_html", _rec_render)
        monkeypatch.setattr(rp, "_run", _rec_run)
        assert rp.publish(dry_run=False) == 1  # preflight fails
        assert order == ["render", "ls-remote"]


# ------------------------------------------------------------------
# summarize_rra — absolute Pages links + --index/--out combined run
# ------------------------------------------------------------------

class TestSummarizeIndex:
    def _seed_reports(self, tmp_path):
        for d in ("2026-11-06", "2026-11-13"):
            (tmp_path / f"Relative_Rotation_Dashboard_ETFs_{d}_analysis_report.md") \
                .write_text(ENRICHED, encoding="utf-8")

    def test_index_links_point_at_pages_html(self, tmp_path):
        self._seed_reports(tmp_path)
        items = [sr.extract(p) for p in sr.find_reports(tmp_path)]
        idx = sr.build_index(items)
        base = ("https://vialliw.github.io/"
                "Relative-Rotation-Analysis-Dashboard/report")
        assert (f"[{_R11_06}]({base}/{_R11_06[:-3]}.html)") in idx
        assert (f"[{_R11_13}]({base}/{_R11_13[:-3]}.html)") in idx
        # no relative .md targets remain
        assert f"]({_R11_06})" not in idx

    def test_index_and_out_in_one_invocation(self, tmp_path, monkeypatch):
        """Regression: the old early `return` after --index silently
        dropped a --out passed in the same call."""
        self._seed_reports(tmp_path)
        out_f = tmp_path / "weekly_summary.md"
        idx_f = tmp_path / "report_index.md"
        monkeypatch.setattr(
            sys, "argv",
            ["summarize_rra.py", "--src-dir", str(tmp_path),
             "--out", str(out_f), "--index", str(idx_f)])
        sr.main()
        assert out_f.exists() and idx_f.exists()
        assert "| Date | Weekly summary |" in out_f.read_text(
            encoding="utf-8")
        assert "Report Index" in idx_f.read_text(encoding="utf-8")

    def test_index_alone_still_writes_no_summary(self, tmp_path,
                                                 monkeypatch, capsys):
        """--index alone must not also dump the summary table (the
        pipeline calls --index unconditionally; extra stdout would be
        noise)."""
        self._seed_reports(tmp_path)
        idx_f = tmp_path / "report_index.md"
        monkeypatch.setattr(
            sys, "argv",
            ["summarize_rra.py", "--src-dir", str(tmp_path),
             "--index", str(idx_f)])
        sr.main()
        assert idx_f.exists()
        assert "| Date | Weekly summary |" not in capsys.readouterr().out
