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
