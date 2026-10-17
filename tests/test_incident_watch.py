"""Tests for scripts/incident_watch.py (INC-T1).

The watcher consumes the composite ``data_health`` JSON — tests inject
health dicts straight into ``incident_watch.run_watch`` /
``classify_health`` and write into ``tmp_path`` roots, so no real Flight
server, no real ``C:\\Intel`` incident store, and no real network are
ever touched.  Toast sends go through an injected stub.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
import sys
from pathlib import Path

import pytest

# Add scripts dir to path so incident_watch can be imported (same
# pattern as tests/test_scheduler.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# incident_watch's module header chdirs to the repo root (it is meant to
# be launched by Task Scheduler from any cwd); restore the test
# session's cwd after importing so collection stays hermetic.
_cwd_before_import = os.getcwd()
import incident_watch  # noqa: E402

os.chdir(_cwd_before_import)


_LATEST = "2026-10-09"
_WS, _WE = "2026-08-28", "2026-10-09"
_NOW = dt.datetime(2026, 10, 12, 18, 30, tzinfo=dt.timezone.utc)


def _health() -> dict:
    """A clean (OK) composite health report shaped per doc §1.2 with a
    pending session — tests mutate copies of this."""
    return {
        "health_status": "OK",
        "daily_close": {
            "health_status": "OK",
            "mode": "full",
            "as_of": "2026-10-12T18:30:00-04:00",
            "latest_trading_day": _LATEST,
            "pending_session": "2026-10-12",
            "checks": {
                "staleness": {"stale_count": 0, "stale_tickers": []},
                "coverage": {"missing_count": 0, "configured_count": 1214,
                             "missing_tickers": []},
                "integrity": {"violation_count": 0, "reason_counts": {},
                              "violations": []},
                "duplicates": {"duplicate_count": 0, "duplicates": []},
                "gaps": {"tickers_with_gaps": 0, "total_missing_days": 0,
                         "expected_sessions": 30,
                         "window_start": _WS, "window_end": _WE,
                         "per_ticker": []},
            },
        },
        "fear_greed": {"health_status": "OK"},
        "weekly": {"health_status": "OK"},
        "company_metadata": {"health_status": "OK"},
    }


def _run(root: Path, health, notify=None, probe_error=None) -> dict:
    """run_watch with toasts stubbed out unless a spy is supplied."""
    return incident_watch.run_watch(
        root, health=health, probe_error=probe_error, now=_NOW,
        notify=notify or (lambda *a, **k: None))


def _open_names(root: Path):
    return sorted(p.name for p in (root / "open").iterdir() if p.is_dir())


def _incident(root: Path, state: str, name: str) -> dict:
    return json.loads(
        (root / state / name / "incident.json").read_text(
            encoding="utf-8"))


def _events(root: Path):
    return [json.loads(line) for line in
            (root / "events.jsonl").read_text(
                encoding="utf-8").splitlines() if line.strip()]


def _stale(*tickers_lastdates):
    """Build a staleness check: _stale(("DBRG", "2026-09-29"), ...)."""
    entries = [{"ticker": t, "last_date": d} for t, d in
               tickers_lastdates]
    return {"stale_count": len(entries), "stale_tickers": entries}


def _gap(ticker, missing_days):
    return {"ticker": ticker, "missing_count": len(missing_days),
            "missing_days": list(missing_days)}


# ---------------------------------------------------------------------
# SEV1
# ---------------------------------------------------------------------

class TestSeverity1:
    def test_probe_failure_creates_sev1(self, tmp_path):
        stats = _run(tmp_path, None, probe_error="connection refused")
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert "_sev1_probe_" in names[0]
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity"] == "SEV1"
        assert inc["state"] == "open"
        assert "restart" in inc["suggested_action"].lower()
        assert len(stats["new"]) == 1

    def test_probe_failure_still_writes_heartbeat(self, tmp_path):
        _run(tmp_path, None, probe_error="boom")
        hb = json.loads((tmp_path / "_last_check.json").read_text(
            encoding="utf-8"))
        assert hb["probe"] == "unreachable"
        assert hb["new"] == 1
        assert hb["health_status"] is None

    def test_daily_close_check_failed_is_sev1(self, tmp_path):
        h = _health()
        h["health_status"] = "CHECK_FAILED"
        h["daily_close"]["health_status"] = "CHECK_FAILED"
        h["daily_close"]["error"] = "duckdb explode"
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev1_daily_close_check_failed" in n for n in names)

    def test_section_check_failed_is_sev1(self, tmp_path):
        h = _health()
        h["health_status"] = "CHECK_FAILED"
        h["weekly"] = {"health_status": "CHECK_FAILED",
                       "error": "parity query died"}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev1_weekly_check_failed" in n for n in names)


# ---------------------------------------------------------------------
# SEV2 triggers
# ---------------------------------------------------------------------

class TestSeverity2:
    def test_duplicates_sev2(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["duplicates"] = {
            "duplicate_count": 1,
            "duplicates": [{"ticker": "AAPL", "date": "2026-10-09",
                            "count": 2}],
        }
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_duplicates_AAPL" in n for n in names)

    def test_recent_integrity_violation_sev2(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 1,
            "reason_counts": {"null_or_nonpositive_ohlc": 1},
            "violations": [{"ticker": "WBD", "date": "2026-10-05",
                            "reason": "null_or_nonpositive_ohlc"}],
        }
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_integrity_WBD_2026-10-05" in n for n in names)
        inc = _incident(tmp_path, "open",
                        next(n for n in names if "integrity" in n))
        assert "on_request_load" in inc["suggested_action"]
        # verbatim evidence fragment archived
        rep = json.loads((tmp_path / "open" / inc["incident_key"]
                          / "report.json").read_text(encoding="utf-8"))
        assert rep["ticker"] == "WBD" and rep["date"] == "2026-10-05"

    def test_old_integrity_violation_is_sev3(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 1, "reason_counts": {},
            "violations": [{"ticker": "XYZ", "date": "2019-01-04",
                            "reason": "high_lt_low"}],
        }
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_integrity_XYZ_2019-01-04" in n for n in names)

    def test_stale_count_25_is_sev2_group(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = _stale(
            *[(f"T{i:02d}", "2026-10-08") for i in range(25)])
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        sev2 = [n for n in names if "_sev2_stale_multi" in n]
        assert len(sev2) == 1
        # grouped incident, not 25 folders
        assert not any("_sev3_stale_" in n for n in names)

    def test_gaps_total_30_is_sev2(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 8, "total_missing_days": 35,
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [_gap(f"G{i}", ["2026-10-09"] * (i + 3))
                           for i in range(8)],
        }
        # per_ticker counts sum to >=30 by construction here? make sure
        # the group fires off total_missing_days alone
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_gaps_total" in n for n in names)
        # group ticket covers the run — no per-ticker gap folders
        assert not any("_sev3_gap_" in n for n in names)
        assert len(stats["new"]) == 1

    def test_gap_single_ticker_10_sessions_sev2(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 1, "total_missing_days": 12,
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [_gap("ABCD", [f"2026-09-{d:02d}"
                                         for d in range(20, 30)])],
        }
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_gap_ABCD" in n for n in names)

    def test_coverage_prior_data_is_sev2(self, tmp_path):
        """A configured ticker absent from daily_close that provably
        held rows in an earlier snapshot -> silent loss -> SEV2."""
        prior = _health()
        prior["daily_close"]["checks"]["staleness"] = _stale(
            ("LOST", "2026-10-07"))
        (tmp_path / "_reports").mkdir(parents=True)
        (tmp_path / "_reports" / "2026-10-08.json").write_text(
            json.dumps(prior), encoding="utf-8")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["coverage"] = {
            "missing_count": 1, "configured_count": 1214,
            "missing_tickers": ["LOST"]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_coverage_LOST" in n for n in names)


# ---------------------------------------------------------------------
# SEV3
# ---------------------------------------------------------------------

class TestSeverity3:
    def test_small_stale_set_sev3(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        # 2 sessions stale -> small-stale SEV3 (not in gap list)
        h["daily_close"]["checks"]["staleness"] = _stale(
            ("AAA", "2026-10-07"))
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_stale_AAA" in n for n in names)

    def test_suspected_delisting_action_text(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        # DBRG last data 2026-09-29 vs latest 2026-10-09 -> 8 sessions
        h["daily_close"]["checks"]["staleness"] = _stale(
            ("DBRG", "2026-09-29"))
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("DBRG", ["2026-09-30", "2026-10-01", "2026-10-02",
                          "2026-10-05", "2026-10-06", "2026-10-07",
                          "2026-10-08", "2026-10-09"])]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = 8
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        stale = [n for n in names if "_stale_DBRG" in n]
        assert len(stale) == 1 and "_sev3_" in stale[0]
        inc = _incident(tmp_path, "open", stale[0])
        assert "delisting" in inc["suggested_action"].lower()
        assert "verify" in inc["suggested_action"].lower()
        # the gap entry must not double-ticket the same ticker
        assert not any("_gap_DBRG" in n for n in names)

    def test_stale_ticker_in_gaps_gets_gap_ticket(self, tmp_path):
        """QRVO stale 4 sessions AND in per_ticker -> gap ticket (doc §6)."""
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = _stale(
            ("QRVO", "2026-10-05"))
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", ["2026-10-06", "2026-10-07", "2026-10-08",
                          "2026-10-09"])]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = 4
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_gap_QRVO" in n for n in names)
        assert not any("_stale_QRVO" in n for n in names)

    def test_section_warn_sev3(self, tmp_path):
        h = _health()
        h["health_status"] = "WARN"
        h["weekly"] = {"health_status": "WARN"}
        h["fear_greed"] = {"health_status": "WARN"}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_weekly_warn" in n for n in names)
        assert any("_sev3_fear_greed_warn" in n for n in names)

    def test_coverage_new_ticker_sev3(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["coverage"] = {
            "missing_count": 2, "configured_count": 1214,
            "missing_tickers": ["NEWCO", "FRESH"]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_coverage_NEWCO" in n for n in names)
        assert any("_sev3_coverage_FRESH" in n for n in names)
        assert not any("_sev2_coverage" in n for n in names)


# ---------------------------------------------------------------------
# SEV4 / suppressions
# ---------------------------------------------------------------------

class TestSuppressions:
    def test_builtin_benign_integrity_suppressed(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 2,
            "reason_counts": {"null_or_nonpositive_ohlc": 2},
            "violations": [
                {"ticker": "CL=F", "date": "2020-04-20",
                 "reason": "null_or_nonpositive_ohlc"},
                {"ticker": "^IRX", "date": "2020-03-25",
                 "reason": "null_or_nonpositive_ohlc"},
            ],
        }
        stats = _run(tmp_path, h)
        assert _open_names(tmp_path) == []
        assert stats["suppressed"] >= 2
        ev = _events(tmp_path)
        assert any(e["event"] == "suppressed"
                   and e["check"] == "integrity" for e in ev)

    def test_custom_benign_from_suppressions_json(self, tmp_path):
        (tmp_path / "_config").mkdir(parents=True)
        (tmp_path / "_config" / "suppressions.json").write_text(
            json.dumps({"benign_integrity": [
                {"ticker": "XYZ", "date": "2026-10-05"}]}),
            encoding="utf-8")
        h = _health()
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 1, "reason_counts": {},
            "violations": [{"ticker": "XYZ", "date": "2026-10-05",
                            "reason": "high_lt_low"}]}
        _run(tmp_path, h)
        assert _open_names(tmp_path) == []

    def test_acknowledged_delisted_downgrades(self, tmp_path):
        (tmp_path / "_config").mkdir(parents=True)
        (tmp_path / "_config" / "suppressions.json").write_text(
            json.dumps({"acknowledged_delisted": ["DBRG"]}),
            encoding="utf-8")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = _stale(
            ("DBRG", "2026-09-29"))
        stats = _run(tmp_path, h)
        assert _open_names(tmp_path) == []
        assert stats["suppressed"] >= 1

    def test_pending_session_not_flagged(self, tmp_path):
        stats = _run(tmp_path, _health())
        assert _open_names(tmp_path) == []
        assert stats["suppressed"] >= 1  # the pending-session note
        # ...and no folder mentions the pending session date
        assert not any("2026-10-12" in n for n in _open_names(tmp_path))

    def test_disabled_section_no_ticket(self, tmp_path):
        h = _health()
        h["fear_greed"] = {"health_status": "DISABLED",
                           "reason": "disabled in config"}
        _run(tmp_path, h)
        assert _open_names(tmp_path) == []

    def test_metadata_null_fields_informational(self, tmp_path):
        h = _health()
        h["company_metadata"]["checks"] = {
            "null_fields": {"null_field_rows": 42, "row_count": 1000}}
        _run(tmp_path, h)
        assert _open_names(tmp_path) == []


# ---------------------------------------------------------------------
# Lifecycle: dedup, escalation, auto-resolve, acknowledged
# ---------------------------------------------------------------------

class TestLifecycle:
    def _gap_health(self):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", ["2026-10-06", "2026-10-07", "2026-10-08",
                          "2026-10-09"])]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = 4
        return h

    def test_dedup_second_run_no_new_folder(self, tmp_path):
        h = self._gap_health()
        _run(tmp_path, h)
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert len(names) == 1
        inc = _incident(tmp_path, "open", names[0])
        assert inc["unacked_checks"] == 2
        ev = _events(tmp_path)
        assert [e["event"] for e in ev
                if e["check"] == "gap"] == ["created", "updated"]

    def test_escalation_at_third_check(self, tmp_path):
        h = self._gap_health()
        _run(tmp_path, h)
        _run(tmp_path, h)
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert "_sev2_gap_QRVO" in names[0]
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity"] == "SEV2"
        assert inc["unacked_checks"] == 3
        assert inc["severity_history"][-1]["from"] == "SEV3"
        assert inc["severity_history"][-1]["to"] == "SEV2"
        assert stats["toasts"]  # escalation re-pages

    def test_auto_resolve_moves_to_resolved(self, tmp_path):
        h = self._gap_health()
        _run(tmp_path, h)
        stats = _run(tmp_path, _health())
        assert _open_names(tmp_path) == []
        resolved = [p.name for p in (tmp_path / "resolved").iterdir()
                    if p.is_dir()]
        assert len(resolved) == 1
        assert "_sev3_gap_QRVO" in resolved[0]
        inc = _incident(tmp_path, "resolved", resolved[0])
        assert inc["state"] == "resolved"
        assert stats["resolved"] == resolved
        ev = _events(tmp_path)
        assert ev[-1]["event"] == "resolved"

    def test_acknowledged_updates_last_seen_no_unacked(self, tmp_path):
        h = self._gap_health()
        _run(tmp_path, h)
        name = _open_names(tmp_path)[0]
        # operator moves it to acknowledged\
        (tmp_path / "acknowledged" / name).parent.mkdir(exist_ok=True)
        os.rename(tmp_path / "open" / name,
                  tmp_path / "acknowledged" / name)
        _run(tmp_path, h)
        inc = _incident(tmp_path, "acknowledged", name)
        assert inc["unacked_checks"] == 1  # not incremented in ack
        assert inc["state"] == "acknowledged"  # synced to folder location
        assert _open_names(tmp_path) == []
        # and it still auto-resolves once the finding clears
        _run(tmp_path, _health())
        assert (tmp_path / "resolved" / name).is_dir()

    def test_folder_name_sanitized(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 1, "reason_counts": {},
            "violations": [{"ticker": "CL=F", "date": "2026-10-05",
                            "reason": "null_or_nonpositive_ohlc"}]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_integrity_CL_F_2026-10-05" in n
                   for n in names)


# ---------------------------------------------------------------------
# Heartbeat, events feed, report archive
# ---------------------------------------------------------------------

class TestStoreArtifacts:
    def test_heartbeat_written_on_success(self, tmp_path):
        h = self_health = _health()
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", ["2026-10-09"])]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = 1
        _run(tmp_path, h)
        hb = json.loads((tmp_path / "_last_check.json").read_text(
            encoding="utf-8"))
        assert hb["probe"] == "ok"
        assert hb["health_status"] == "OK"
        assert hb["new"] == 1
        assert "ts" in hb

    def test_report_snapshot_archived(self, tmp_path):
        _run(tmp_path, _health())
        reports = list((tmp_path / "_reports").glob("*.json"))
        assert len(reports) == 1
        snap = json.loads(reports[0].read_text(encoding="utf-8"))
        assert snap["health_status"] == "OK"

    def test_second_same_day_snapshot_not_clobbered(self, tmp_path):
        _run(tmp_path, _health())
        _run(tmp_path, _health())
        assert len(list((tmp_path / "_reports").glob("*.json"))) == 2

    def test_events_jsonl_created_per_transition(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["duplicates"] = {
            "duplicate_count": 1,
            "duplicates": [{"ticker": "AAPL", "date": "2026-10-09"}]}
        _run(tmp_path, h)
        _run(tmp_path, _health())
        events = [e["event"] for e in _events(tmp_path)]
        assert "created" in events and "resolved" in events


# ---------------------------------------------------------------------
# Toast (notify) behavior
# ---------------------------------------------------------------------

class TestNotify:
    def test_toast_only_on_new_sev12(self, tmp_path):
        calls = []
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["duplicates"] = {
            "duplicate_count": 1,
            "duplicates": [{"ticker": "AAPL", "date": "2026-10-09"}]}
        _run(tmp_path, h, notify=lambda *a: calls.append(a))
        assert len(calls) == 1 and calls[0][0] == 2
        # repeat run -> update only, no re-toast below escalation cadence
        _run(tmp_path, h, notify=lambda *a: calls.append(a))
        assert len(calls) == 1

    def test_no_toast_for_sev3(self, tmp_path):
        calls = []
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", ["2026-10-09"])]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = 1
        _run(tmp_path, h, notify=lambda *a: calls.append(a))
        assert calls == []

    def test_toast_on_probe_failure_sev1(self, tmp_path):
        calls = []
        _run(tmp_path, None, probe_error="down",
             notify=lambda *a: calls.append(a))
        assert len(calls) == 1 and calls[0][0] == 1


# ---------------------------------------------------------------------
# Self-test + CLI
# ---------------------------------------------------------------------

class TestSelfTest:
    def test_self_test_exits_0_and_materializes(self, tmp_path,
                                                monkeypatch):
        monkeypatch.setattr(incident_watch.tempfile, "mkdtemp",
                            lambda **kw: str(tmp_path / "selftest"))
        # real notify would spawn powershell — keep offline & silent
        monkeypatch.setattr(incident_watch, "_notify",
                            lambda *a, **k: None)
        code = incident_watch.main(["--self-test"])
        assert code == 0
        root = tmp_path / "selftest"
        names = _open_names(root)
        # doc §6 fixture: 1xSEV2 (WBD integrity) + DBRG stale + 4 gaps
        assert any("_sev2_integrity_WBD_2026-10-05" in n for n in names)
        assert any("_sev3_stale_DBRG" in n for n in names)
        assert any("_sev3_gap_QRVO" in n for n in names)
        assert (root / "_last_check.json").is_file()
        hb = json.loads((root / "_last_check.json").read_text(
            encoding="utf-8"))
        assert hb["probe"] == "ok" and hb["health_status"] == "WARN"


class TestClassifyDirect:
    """classify_health must accept plain dicts — no Flight needed."""

    def test_clean_report_no_findings(self):
        findings = incident_watch.classify_health(_health())
        assert [f for f in findings if not f.suppressed] == []

    def test_probe_failure_finding(self):
        findings = incident_watch.classify_health(
            None, probe_error="timeout")
        assert len(findings) == 1
        assert findings[0].severity == 1
        assert findings[0].check == "probe"


# ---------------------------------------------------------------------
# INC-T1b regression tests — one test per challenger finding B1/B2/B3
# and N1/N2/N3/N4 (see devin_blackboard/EVENTS.log [CHALLENGER] entry).
# ---------------------------------------------------------------------

class TestSeverityUpgradeB1:
    """B1: a recurring finding re-classified WORSE must route through
    the rename + severity_history + toast path — no silent min() merge.
    Challenger probe: gap 9 -> 10 sessions on check 2."""

    def _gap_health(self, days):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", list(days))]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = \
            len(days)
        return h

    def test_upgrade_renames_folder_and_pages(self, tmp_path):
        calls = []
        # check 1: 9 missing sessions -> SEV3 gap ticket
        _run(tmp_path, self._gap_health(
            [f"2026-10-0{d}" for d in range(1, 10)]),
            notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        assert any("_sev3_gap_QRVO" in n for n in names)
        assert calls == []  # SEV3 does not page

        # check 2: worsens to 10 sessions -> SEV2 on recurrence
        stats = _run(tmp_path, self._gap_health(
            [f"2026-09-{d:02d}" for d in range(20, 30)]),
            notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        # same incident, renamed — never a second folder
        assert len(names) == 1
        assert "_sev2_gap_QRVO" in names[0]
        assert not any("_sev3_" in n for n in names)
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity"] == "SEV2"
        assert inc["incident_key"] == names[0]
        assert inc["unacked_checks"] == 2
        hist = inc["severity_history"]
        assert hist[-1]["from"] == "SEV3"
        assert hist[-1]["to"] == "SEV2"
        assert "re-classified" in hist[-1]["reason"]
        # doc §5: a NEW SEV2 condition pages
        assert any(c[0] == 2 for c in calls)
        ev = _events(tmp_path)
        # 'escalated' audit record written (suppressed SEV4 events may
        # trail it — pending_session is logged after SEV-sorted findings)
        assert "escalated" in [e["event"] for e in ev]
        assert len(stats["new"]) == 0
        assert stats["updated"] == names

    def test_downgrade_never_silently_lowers_severity(self, tmp_path):
        """A SEV2 gap healing to SEV3 size stays SEV2 while open —
        the operator owns the ack/downgrade decision."""
        _run(tmp_path, self._gap_health(
            [f"2026-09-{d:02d}" for d in range(20, 30)]))
        stats = _run(tmp_path, self._gap_health(["2026-10-09"]))
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert "_sev2_gap_QRVO" in names[0]
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity"] == "SEV2"
        assert stats["toasts"] == []


class TestMalformedPayloadsB2:
    """B2: non-dict check/section payloads must never crash classify —
    they emit SEV2 anomaly findings and the SEV1 CHECK_FAILED scan
    still runs over whatever shape arrived."""

    def test_non_dict_subcheck_and_check_failed(self, tmp_path):
        """Challenger probe: {'staleness': 'query failed'} under a
        CHECK_FAILED section — previously exit 1 + zero folders."""
        h = _health()
        h["health_status"] = "CHECK_FAILED"
        h["daily_close"]["health_status"] = "CHECK_FAILED"
        h["daily_close"]["error"] = "duckdb explode"
        h["daily_close"]["checks"]["staleness"] = "query failed"
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev1_daily_close_check_failed" in n
                   for n in names)
        assert any("_sev2_staleness_malformed_payload" in n
                   for n in names)
        assert stats["new"]

    def test_non_dict_checks_container_no_crash(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"] = "query exploded"
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_daily_close_malformed_checks" in n
                   for n in names)
        assert stats["new"]

    def test_non_dict_section_payload_no_crash(self, tmp_path):
        h = _health()
        h["weekly"] = "boom"
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_weekly_malformed_payload" in n
                   for n in names)
        assert stats["new"]

    def test_non_dict_daily_close_section_no_crash(self, tmp_path):
        h = _health()
        h["daily_close"] = "the whole section is a string"
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_daily_close_malformed_payload" in n
                   for n in names)
        assert stats["new"]


class TestNonNumericCountsB3:
    """B3: unparseable count fields -> SEV2 anomaly finding + safe
    fallback, never ValueError/exit 1."""

    def test_non_numeric_stale_count_still_classifies(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = {
            "stale_count": "many",
            "stale_tickers": [{"ticker": "AAA",
                               "last_date": "2026-10-08"}]}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_staleness_unparseable_stale_count" in n
                   for n in names)
        # the listed ticker is still classified on its own merits
        assert any("_stale_AAA" in n for n in names)
        assert stats["new"]

    def test_non_numeric_gap_counts_no_crash(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 1, "total_missing_days": "bogus",
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [{"ticker": "ABCD", "missing_count": "lots",
                            "missing_days": []}]}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("unparseable" in n for n in names)
        assert stats["new"]

    def test_non_numeric_duplicate_count(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["duplicates"] = {
            "duplicate_count": "NaN-ish", "duplicates": []}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_duplicates_unparseable_duplicate_count" in n
                   for n in names)
        assert stats["new"]


class TestCountOnlySignalsN1:
    """N1: counts that exceed the listed tickers (truncated report)
    still produce findings instead of being dropped."""

    def test_stale_count_only_sev2(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = {
            "stale_count": 25, "stale_tickers": []}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_stale_count25" in n for n in names)
        assert stats["new"]

    def test_stale_count_beyond_listed_tickers(self, tmp_path):
        """stale_count 30 with only 3 listed entries still pages — the
        unlisted 27 cannot be trusted away."""
        h = _health()
        h["daily_close"]["checks"]["staleness"] = {
            "stale_count": 30,
            "stale_tickers": [{"ticker": "AAA",
                               "last_date": "2026-10-08"}]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_stale_" in n for n in names)

    def test_coverage_count_only_sev3(self, tmp_path):
        """missing_count 1-10 with an empty ticker list -> SEV3
        count-only finding (prior data unprovable)."""
        h = _health()
        h["daily_close"]["checks"]["coverage"] = {
            "missing_count": 5, "configured_count": 1214,
            "missing_tickers": []}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_coverage_count5" in n for n in names)

    def test_coverage_count_only_sev2_broad(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["coverage"] = {
            "missing_count": 15, "configured_count": 1214,
            "missing_tickers": []}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_coverage_count15" in n for n in names)


class TestSameRunDedupN2:
    """N2: two findings with the same identity in ONE run -> one
    folder, never a `_2` phantom that can never resolve."""

    def test_identical_violations_one_folder(self, tmp_path):
        h = _health()
        v = {"ticker": "WBD", "date": "2026-10-05",
             "reason": "null_or_nonpositive_ohlc"}
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 2,
            "reason_counts": {"null_or_nonpositive_ohlc": 2},
            "violations": [dict(v), dict(v)]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert not any(n.endswith("_2") for n in names)
        # and the single folder still resolves normally
        stats = _run(tmp_path, _health())
        assert len(stats["resolved"]) == 1

    def test_duplicate_findings_direct(self, tmp_path):
        """_apply_findings dedups by identity even off the happy
        classify path (two constructed _Finding objects)."""
        f = lambda sev: incident_watch._Finding(  # noqa: E731
            check="gap", detail="QRVO", severity=sev, summary="dup",
            evidence={}, suggested_action="x")
        stats = incident_watch._apply_findings(
            tmp_path, [f(3), f(2)], "20261012", "2026-10-12T18:30:00")
        assert len(stats["new"]) == 1
        # worst severity wins (findings arrive sorted SEV1-first)
        assert "_sev3_gap_QRVO" in stats["new"][0]


class TestTopLevelCheckFailedN3:
    """N3: top-level health_status == CHECK_FAILED -> SEV1 even when
    every section reports OK."""

    def test_composite_check_failed_sev1(self, tmp_path):
        h = _health()
        h["health_status"] = "CHECK_FAILED"
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev1_composite_check_failed" in n for n in names)
        inc = _incident(
            tmp_path, "open",
            next(n for n in names if "composite" in n))
        assert inc["severity"] == "SEV1"
        assert stats["toasts"]  # SEV1 pages


class TestViolationDatesN4:
    """N4: unknown/unparseable/missing violation dates are uniformly
    SEV3 (conservative); only a provably-in-window date is SEV2."""

    def test_none_and_garbage_dates_both_sev3(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 3, "reason_counts": {},
            "violations": [
                {"ticker": "NNN", "date": None,
                 "reason": "high_lt_low"},
                {"ticker": "GGG", "date": "not-a-date",
                 "reason": "high_lt_low"},
                {"ticker": "OKK", "date": "2026-10-05",
                 "reason": "high_lt_low"}]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_integrity_NNN_None" in n for n in names)
        # 'not-a-date' — _sanitize keeps hyphens
        assert any("_sev3_integrity_GGG_not-a-date" in n
                   for n in names)
        assert any("_sev2_integrity_OKK_2026-10-05" in n
                   for n in names)
        assert not any("_sev2_integrity_NNN" in n for n in names)
        assert not any("_sev2_integrity_GGG" in n for n in names)

    def test_unparseable_window_bounds_fail_conservative(self, tmp_path):
        """Garbage window bounds can no longer be used to prove a
        violation is 'recent' -> SEV3."""
        h = _health()
        h["daily_close"]["checks"]["gaps"]["window_start"] = "junk"
        h["daily_close"]["checks"]["gaps"]["window_end"] = "junk"
        h["daily_close"]["checks"]["integrity"] = {
            "violation_count": 1, "reason_counts": {},
            "violations": [{"ticker": "WBD", "date": "2026-10-05",
                            "reason": "high_lt_low"}]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev3_integrity_WBD_2026-10-05" in n
                   for n in names)


# ---------------------------------------------------------------------
# INC-T1c regression tests — NB1 (non-finite counts -> anomaly, never
# OverflowError), NB2 (same-run re-classification + escalation cadence
# must not double-toast), NB3 (bool counts -> anomaly), NB4 (legacy
# folder/json severity drift self-heals on update).
# ---------------------------------------------------------------------

class TestNonFiniteCountsNB1:
    """NB1: json.loads natively accepts Infinity/-Infinity/1e999 — a
    non-finite count reaching classify must become a SEV2 'unparseable'
    anomaly, not an OverflowError that exits 1 with zero folders."""

    def test_inf_stale_count_anomaly_no_crash(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = {
            "stale_count": float("inf"),
            "stale_tickers": [{"ticker": "AAA",
                               "last_date": "2026-10-08"}]}
        stats = _run(tmp_path, h)   # would exit 1 before NB1 fix
        names = _open_names(tmp_path)
        assert any("unparseable_stale_count" in n for n in names)
        # the listed ticker still classifies on its own merits
        assert any("_stale_AAA" in n for n in names)
        assert stats["new"]

    def test_negative_inf_total_missing_days(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 1, "total_missing_days": float("-inf"),
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [{"ticker": "ABCD", "missing_count": 2,
                            "missing_days": ["2026-10-08",
                                             "2026-10-09"]}]}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("unparseable_total_missing_days" in n
                   for n in names)
        # the per-ticker entry still produces its own ticket
        assert any("_gap_ABCD" in n for n in names)
        assert stats["new"]

    def test_1e999_reaches_classify_as_inf(self, tmp_path):
        """json.loads('1e999') overflows to float inf — the exact
        malformed-payload path the challenger probed."""
        parsed = json.loads(
            '{"stale_count": 1e999, "stale_tickers": []}')
        assert parsed["stale_count"] == float("inf")
        h = _health()
        h["daily_close"]["checks"]["staleness"] = parsed
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("unparseable_stale_count" in n for n in names)
        assert stats["new"]

    def test_safe_int_never_raises_on_edge_values(self):
        """_safe_int is used on operator-touchable incident.json fields
        — ±inf/nan/bool must all degrade to the default."""
        for v in (float("inf"), float("-inf"), float("nan"),
                  True, False, "Infinity", "-Infinity", "1e999",
                  "nan", None):
            assert incident_watch._safe_int(v, 7) == 7
        # real counts still parse
        assert incident_watch._safe_int(42, 7) == 42
        assert incident_watch._safe_int("42", 7) == 42
        assert incident_watch._safe_int(4.9, 7) == 4


class TestBoolCountsNB3:
    """NB3: bool is an int subclass — JSON true/false must NOT silently
    parse as 1/0; treat as the unparseable anomaly it is."""

    def test_bool_stale_count_is_anomaly(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["staleness"] = {
            "stale_count": True, "stale_tickers": []}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("unparseable_stale_count" in n for n in names)
        assert stats["new"]

    def test_bool_coverage_missing_count_is_anomaly(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["coverage"] = {
            "missing_count": False, "configured_count": 1214,
            "missing_tickers": []}
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("unparseable_missing_count" in n for n in names)
        assert stats["new"]

    def test_bool_per_ticker_gap_count_is_anomaly(self, tmp_path):
        h = _health()
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            {"ticker": "ABCD", "missing_count": False,
             "missing_days": []}]
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("unparseable_missing_count" in n for n in names)
        # the bogus per-ticker count must not mint a gap ticket
        assert not any("_gap_ABCD" in n for n in names)
        assert stats["new"]


class TestSameRunEscalationDedupNB2:
    """NB2: a re-classification that lands on an unacked-cadence run
    produced 2 toasts + 2 history entries (re-classified + re-page)
    for ONE severity change — now deduped to exactly one of each."""

    def _gap_health(self, days):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", list(days))]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = \
            len(days)
        return h

    def test_reclassify_on_cadence_run_single_toast(self, tmp_path):
        calls = []
        nine = [f"2026-10-0{d}" for d in range(1, 10)]
        ten = [f"2026-09-{d:02d}" for d in range(20, 30)]
        _run(tmp_path, self._gap_health(nine),
             notify=lambda *a: calls.append(a))
        _run(tmp_path, self._gap_health(nine),
             notify=lambda *a: calls.append(a))
        assert calls == []
        # 3rd unacked check (escalation cadence) AND re-classified
        # 9 -> 10 sessions (SEV3 -> SEV2) in the same run.
        stats = _run(tmp_path, self._gap_health(ten),
                     notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert "_sev2_gap_QRVO" in names[0]
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity"] == "SEV2"
        assert inc["unacked_checks"] == 3
        # exactly ONE history entry and ONE toast for the whole run
        assert len(inc["severity_history"]) == 1
        assert "re-classified" in inc["severity_history"][0]["reason"]
        assert len(calls) == 1 and calls[0][0] == 2
        assert len(stats["toasts"]) == 1

    def test_plain_cadence_escalation_still_pages(self, tmp_path):
        """Guard against over-dedup: escalation WITHOUT re-classification
        (same severity, unacked hits 3) still escalates + toasts."""
        calls = []
        four = ["2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09"]
        _run(tmp_path, self._gap_health(four),
             notify=lambda *a: calls.append(a))
        _run(tmp_path, self._gap_health(four),
             notify=lambda *a: calls.append(a))
        stats = _run(tmp_path, self._gap_health(four),
                     notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert "_sev2_gap_QRVO" in names[0]
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity_history"][-1]["reason"].startswith(
            "unacknowledged")
        assert len(calls) == 1 and calls[0][0] == 2
        assert len(stats["toasts"]) == 1

    def test_sev1_repager_still_fires_when_not_reclassified(self,
                                                          tmp_path):
        """SEV1 stays SEV1 at cadence -> the re-page path must remain
        (escalated=False, so `not escalated` still lets it through)."""
        calls = []
        for _ in range(3):
            _run(tmp_path, None, probe_error="down",
                 notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        assert len(names) == 1
        inc = _incident(tmp_path, "open", names[0])
        repages = [h for h in inc["severity_history"]
                   if "re-page" in h["reason"]]
        assert len(repages) == 1
        # new SEV1 toast + cadence re-page toast
        assert len(calls) == 2


class TestLegacySeverityDriftNB4:
    """NB4: folders damaged by the pre-B1 silent merge (incident.json
    severity drifted from the folder's sevN token) self-heal on the
    next update — the stored json value is authoritative."""

    def test_drifted_folder_renamed_to_json_severity(self, tmp_path):
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"]["per_ticker"] = [
            _gap("QRVO", ["2026-10-06", "2026-10-07", "2026-10-08",
                          "2026-10-09"])]
        h["daily_close"]["checks"]["gaps"]["tickers_with_gaps"] = 1
        h["daily_close"]["checks"]["gaps"]["total_missing_days"] = 4
        _run(tmp_path, h)
        name = _open_names(tmp_path)[0]
        assert "_sev3_gap_QRVO" in name
        # simulate pre-B1 damage: json silently says SEV2, folder sev3
        p = tmp_path / "open" / name / "incident.json"
        data = json.loads(p.read_text(encoding="utf-8"))
        data["severity"] = "SEV2"
        p.write_text(json.dumps(data), encoding="utf-8")
        # next run: finding classifies SEV3 again — the 'better' path
        # never downgrades, and no rename trigger fired before NB4;
        # the reconcile step must still fix the folder name.
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert len(names) == 1
        assert "_sev2_gap_QRVO" in names[0]
        inc = _incident(tmp_path, "open", names[0])
        assert inc["severity"] == "SEV2"
        assert inc["incident_key"] == names[0]
        assert stats["updated"] == names


# ---------------------------------------------------------------------
# INC-T5 regression tests — acknowledged_delisted tickers are suppressed
# per-ticker; their days must ALSO be excluded from the aggregate SEV2
# thresholds (gaps total_missing_days >= 30, single-ticker >=10, and
# staleness stale_count >= 25) while remaining visible in the suppressed
# accounting.  Live defect 2026-10-17: DBRG+QRVO+WBD (acknowledged
# delisted) contributed 31 of 33 total_missing_days -> false SEV2 page.
# ---------------------------------------------------------------------

def _days(start: int, n: int) -> list:
    """n consecutive '2026-09-DD' date strings for _gap() builders."""
    return [f"2026-09-{d:02d}" for d in range(start, start + n)]


def _write_delisted(root: Path, *tickers) -> None:
    (root / "_config").mkdir(parents=True, exist_ok=True)
    (root / "_config" / "suppressions.json").write_text(
        json.dumps({"acknowledged_delisted": list(tickers)}),
        encoding="utf-8")


class TestDelistedAggregateExclusion:
    def test_delisted_gap_days_excluded_from_total(self, tmp_path):
        """Live repro: 31 delisted + 2 live days = 33 reported -> the
        effective total is 2, below the >=30 group SEV2; live tickers
        still ticket at SEV3 on their own merits."""
        _write_delisted(tmp_path, "DBRG", "QRVO", "WBD")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 5, "total_missing_days": 33,
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [
                _gap("DBRG", _days(10, 15)),
                _gap("QRVO", _days(10, 12)),
                _gap("WBD", ["2026-10-06", "2026-10-07",
                             "2026-10-08", "2026-10-09"]),
                _gap("ITEQ", ["2026-10-09"]),
                _gap("LESL", ["2026-10-09"]),
            ]}
        calls = []
        stats = _run(tmp_path, h, notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        assert not any("_sev2_gaps_total" in n for n in names)
        assert calls == []  # no page
        # live tickers ticket on their own merits
        assert any("_sev3_gap_ITEQ" in n for n in names)
        assert any("_sev3_gap_LESL" in n for n in names)
        # delisted tickers produce no folders at all
        assert not any("_gap_DBRG" in n for n in names)
        assert not any("_gap_QRVO" in n for n in names)
        assert not any("_gap_WBD" in n for n in names)
        # ...but their days are visible in suppressed accounting
        assert stats["suppressed_detail"]["gap"]["count"] == 3
        assert stats["suppressed_detail"]["gap"]["missing_days"] == 31
        hb = json.loads((tmp_path / "_last_check.json").read_text(
            encoding="utf-8"))
        assert hb["suppressed_detail"]["gap"]["count"] == 3
        assert hb["suppressed_detail"]["gap"]["missing_days"] == 31

    def test_delisted_single_ticker_10plus_stays_suppressed(
            self, tmp_path):
        """The single-ticker >=10-sessions SEV2 trigger must not fire
        for an acknowledged-delisted ticker even when it dominates the
        report."""
        _write_delisted(tmp_path, "DBRG")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 1, "total_missing_days": 15,
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [_gap("DBRG", _days(10, 15))]}
        stats = _run(tmp_path, h)
        assert _open_names(tmp_path) == []
        assert stats["suppressed_detail"]["gap"]["count"] == 1
        assert stats["suppressed_detail"]["gap"]["missing_days"] == 15

    def test_delisted_stale_excluded_from_stale_count(self, tmp_path):
        """25 stale tickers but 3 acknowledged delisted -> effective
        count 22 < 25 -> no group SEV2; live tickers still ticket."""
        _write_delisted(tmp_path, "OLD1", "OLD2", "OLD3")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = _stale(
            ("OLD1", "2026-09-29"), ("OLD2", "2026-09-29"),
            ("OLD3", "2026-09-29"),
            *[(f"T{i:02d}", "2026-10-08") for i in range(22)])
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert not any("_sev2_stale" in n for n in names)
        assert stats["suppressed_detail"]["stale"]["count"] == 3
        # the 22 live stale tickers still classify individually
        assert any("_sev3_stale_T00" in n for n in names)

    def test_group_sev2_still_fires_on_live_days(self, tmp_path):
        """Non-delisted totals unchanged: 31 live + 2 delisted days = 33
        reported -> effective 31 >= 30 -> group SEV2 that lists only
        live tickers and still pages."""
        _write_delisted(tmp_path, "DBRG")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 3, "total_missing_days": 33,
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [
                _gap("DBRG", ["2026-10-08", "2026-10-09"]),
                _gap("ABCD", _days(5, 16)),
                _gap("EFGH", _days(5, 15)),
            ]}
        calls = []
        stats = _run(tmp_path, h, notify=lambda *a: calls.append(a))
        names = _open_names(tmp_path)
        sev2 = [n for n in names if "_sev2_gaps_total" in n]
        assert len(sev2) == 1
        assert not any("_gap_DBRG" in n for n in names)
        inc = _incident(tmp_path, "open", sev2[0])
        assert "31 missing day(s)" in inc["evidence_summary"]
        assert calls and calls[0][0] == 2  # real SEV2 still pages
        assert stats["suppressed_detail"]["gap"]["missing_days"] == 2

    def test_truncated_per_ticker_still_pages(self, tmp_path):
        """Conservative fallback: total_missing_days exceeds the days we
        can attribute to LISTED delisted tickers — the unattributable
        remainder stays counted and still pages."""
        _write_delisted(tmp_path, "DBRG")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["gaps"] = {
            "tickers_with_gaps": 9, "total_missing_days": 35,
            "expected_sessions": 30,
            "window_start": _WS, "window_end": _WE,
            "per_ticker": [_gap("DBRG", _days(10, 5))]}
        # effective = 35 - 5 = 30 >= 30 -> group SEV2 (count fallback)
        stats = _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_gaps_total" in n for n in names)
        assert stats["suppressed_detail"]["gap"]["missing_days"] == 5

    def test_stale_count_only_unlisted_still_pages(self, tmp_path):
        """Truncated stale list: unlisted stale tickers cannot be
        attributed to the delisted set — they stay counted, so 26
        reported minus the 1 listed delisted = 25 >= 25 -> SEV2."""
        _write_delisted(tmp_path, "OLD1")
        h = _health()
        h["daily_close"]["health_status"] = "WARN"
        h["daily_close"]["checks"]["staleness"] = {
            "stale_count": 26,
            "stale_tickers": [{"ticker": "OLD1",
                               "last_date": "2026-09-29"}]}
        _run(tmp_path, h)
        names = _open_names(tmp_path)
        assert any("_sev2_stale_count25" in n for n in names)
