"""Tests for scripts/rrg_weekly_pipeline.py — date math, enrichment
gate, filename resolution, and --dry-run behavior.

All tests are hermetic: no network, no DuckDB/Flight, no git.  Gate
logic runs against tmp_path fixtures; dry-run tests prove the pipeline
never reaches the subprocess/clone layer.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rrg_weekly_pipeline as rp  # noqa: E402


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


class TestSummaryGate:
    def test_missing_dates_listed(self, tmp_path):
        s = tmp_path / "weekly_summary.md"
        s.write_text("| 2026-11-06 | row |\n", encoding="utf-8")
        assert rp.summary_missing_dates(
            s, ["2026-11-06", "2026-11-13"]) == ["2026-11-13"]

    def test_absent_summary_lists_all(self, tmp_path):
        assert rp.summary_missing_dates(
            tmp_path / "weekly_summary.md", ["2026-11-13"]) == ["2026-11-13"]


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
        assert "Relative_Rotation_Dashboard_ETFs_2026-11-06_analysis_report.md" \
            in plan["publish"]
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
        assert "Relative_Rotation_Dashboard_ETFs_analysis_report20261113.md" \
            in plan["publish"]

    def test_no_summary_file_means_no_summary_row(self, tmp_path):
        _dated(tmp_path, "2026-11-06", ENRICHED)
        plan = rp.plan_publish(tmp_path)
        assert "weekly_summary.md" not in plan["publish"]
        assert all(i["name"] != "weekly_summary.md" for i in plan["skip"])


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
        assert "would ship report/Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md" in out

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
