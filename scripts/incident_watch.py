"""Incident watcher — classify ``data_health`` JSON into incident folders.

Scheduled after the daily load (INC-T2 registers the watch task).  Each
run calls the Flight ``data_health`` action, classifies the composite
health report per ``docs/incident_management_requirements.md`` §2.1
severity rules, and materializes one folder per incident in the
centralized store (doc §7)::

    <root>/open/<YYYYMMDD>_sev<N>_<check>_<detail>/
        incident.json   {incident_key, severity, check, first_seen,
                         last_seen, unacked_checks, severity_history[],
                         state, suggested_action, evidence_summary}
        report.json     verbatim offending JSON fragment
        TICKET.md       human summary + copy-paste remediation

Dedup: the folder name IS the incident key — a repeat finding updates
``last_seen``/``unacked_checks`` on the open ticket, never a second
folder.  Findings absent from the latest report auto-resolve (folder
moves ``open/`` -> ``resolved/``).  An incident unacknowledged for
``_ESCALATE_EVERY_N_CHECKS`` checks bumps one severity level (SEV3 ->
SEV2, folder renamed, ``severity_history`` appended); SEV1/SEV2 at the
same cadence re-page via toast.  A recurring finding re-classified at
a WORSE severity takes the same rename/history/toast path — severity
never flips silently (INC-T1b).  Every create/update/escalate/resolve/
suppress appends one JSON line to ``<root>/events.jsonl``.

Toast notification (doc §5): NEW SEV1/SEV2 folders (and SEV1/SEV2
re-pages) invoke ``scripts/notify_done.ps1`` — the registered-AUMID
toast script; a plain "PowerShell" AppID is silently dropped on Win11.
Toast failures never fail the run.  SEV3/SEV4 surface only in the stdout
digest + ``_last_check.json``.

Store root resolution (doc §7.2): ``<repo>/incidents`` when that
junction exists (it points into ``C:\\Intel\\incidents\\stock-ingestion``);
otherwise the central path directly; ``--incidents-root`` overrides both
(used by tests).

Usage::

    python scripts/incident_watch.py [--gap-days N] [--incidents-root P]
    python scripts/incident_watch.py --self-test   # offline synthetic run

Exit codes: ``0`` = a classification cycle completed — including a probe
failure, which raises a SEV1 incident; ``1`` = internal crash (a
best-effort heartbeat is still written first).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def _ensure_stdio_fallbacks() -> None:
    """Redirect ``sys.stdout``/``sys.stderr`` to the null device if None.

    A console-less Windows Task Scheduler process starts with both
    streams set to ``None``; any library touching them then crashes.
    Idempotent — real streams are left untouched (same guard as
    ``scripts/run_flight_server.py``).
    """
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w", encoding="utf-8")


# Patch missing stdio as early as possible — before the third-party
# imports below.
_ensure_stdio_fallbacks()

# Ensure src/ is importable regardless of the task's working directory
# (same cwd-safe header as scripts/daily_load_once.py).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(_REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
_NOTIFY_PS1 = os.path.join(_REPO_ROOT, "scripts", "notify_done.ps1")
_LOCAL_INCIDENTS_LINK = os.path.join(_REPO_ROOT, "incidents")
_DEFAULT_CENTRAL_ROOT = r"C:\Intel\incidents\stock-ingestion"
_FALLBACK_LOG = os.path.join(_REPO_ROOT, "logs", "incident_watch_error.log")

import pyarrow.flight as flight  # noqa: E402

from src.config import PipelineConfig  # noqa: E402

logger = logging.getLogger(__name__)

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_JSON_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

# Flight call bounds — same values as scripts/data_health_report.py.
_FLIGHT_CONNECT_TIMEOUT_S = 10
_FLIGHT_ACTION_TIMEOUT_S = 120

# Doc §2.1 thresholds.
_STALE_SEV2_THRESHOLD = 25        # ~2% of the ~1214 configured tickers
_COVERAGE_SEV3_MAX = 10           # missing_count 1-10 -> SEV3
_GAP_TOTAL_SEV2_DAYS = 30         # total missing days in one run -> SEV2
_GAP_TICKER_SEV2_DAYS = 10        # one ticker missing >=10 sessions -> SEV2
_DELISTING_STALE_SESSIONS = 5     # stale >=5 sessions, no integrity -> delisting?
_ESCALATE_EVERY_N_CHECKS = 3      # doc §2.2: >=3 consecutive unacked checks

_FOLDER_NAME_MAX = 80
_TOAST_TIMEOUT_S = 30
# Fallback "recent" horizon for integrity violations when the health
# report carries no gaps window bounds (~30 sessions ~ 60 calendar days).
_INTEGRITY_RECENT_DAYS = 60

_STATE_DIRS = ("open", "acknowledged", "resolved")
_STORE_DIRS = _STATE_DIRS + ("_reports", "_config")
_SECTION_NAMES = ("fear_greed", "weekly", "company_metadata")
_FOLDER_PREFIX_RE = re.compile(r"^(\d{8})_sev(\d)_")
_SANITIZE_RE = re.compile(r"[^0-9A-Za-z_-]+")


class _Finding:
    """One classified finding -> at most one incident folder.

    ``suppressed=True`` marks SEV4/benign items: they are logged,
    counted in the heartbeat and appended to ``events.jsonl``, but never
    materialize a folder (doc §2.1: SEV4 = log only, no ticket).
    """

    __slots__ = (
        "check", "detail", "severity", "summary", "evidence",
        "suggested_action", "suppressed",
    )

    def __init__(self, check, detail, severity, summary, evidence,
                 suggested_action, suppressed=False):
        self.check = check
        self.detail = detail
        self.severity = severity
        self.summary = summary
        self.evidence = evidence
        self.suggested_action = suggested_action
        self.suppressed = suppressed

    @property
    def identity(self) -> str:
        """Stable dedup identity: check + detail — deliberately WITHOUT
        severity or date so an escalation rename and repeat runs keep
        matching the same folder (doc §7.3)."""
        return _sanitize(f"{self.check}_{self.detail}")


def _sanitize(text: str) -> str:
    """Make a folder-name-safe component (``CL=F`` -> ``CL_F``,
    ``^IRX`` -> ``_IRX``)."""
    return _SANITIZE_RE.sub("_", str(text))


def _folder_name(yyyymmdd: str, severity: int, identity: str) -> str:
    return f"{yyyymmdd}_sev{severity}_{identity}"[:_FOLDER_NAME_MAX]


def _multi_detail(prefix: str, items) -> str:
    """Detail for a multi-ticker incident: ``multi<N>_<hash6>`` — the
    hash of the sorted member set keeps the incident key stable across
    runs while the membership is unchanged (doc §2.2)."""
    items = [str(i) for i in items]
    digest = hashlib.sha1(
        ",".join(sorted(items)).encode("utf-8")
    ).hexdigest()[:6]
    return f"{prefix}{len(items)}_{digest}"


def _sessions_between(last_date, latest) -> "int | None":
    """Business-day sessions strictly after ``last_date`` up to and
    including ``latest`` — approximates 'stale sessions' for the
    suspected-delisting rule (NYSE holiday nuance is irrelevant at the
    >=5 threshold).  ``None`` when either date is missing/unparseable.
    """
    if not last_date or not latest:
        return None
    try:
        d0 = _dt.date.fromisoformat(str(last_date)[:10])
        d1 = _dt.date.fromisoformat(str(latest)[:10])
    except ValueError:
        return None
    if d1 <= d0:
        return 0
    # Count weekdays in (last_date, latest] — numpy's busday would also
    # work but a bounded weekday count needs no trading calendar.
    days = (d1 - d0).days
    weeks, rem = divmod(days, 7)
    count = weeks * 5
    d = d0
    # justified: rem <= 6 iterations — weekday counting cannot be
    # vectorized without pulling in a trading-calendar dependency.
    for _ in range(rem):
        d += _dt.timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return count


def _in_window(date, window_start, window_end, latest) -> bool:
    """SEV2 gate for integrity violations (doc §2.1 'recent corrupt
    row').  True only when the violation's date parses AND is provably
    recent — inside the gaps window when both bounds parse, else within
    ``_INTEGRITY_RECENT_DAYS`` of a parseable ``latest_trading_day``.

    INC-T1b N4 — consistent conservative policy: a violation with an
    unknown/unparseable/missing date, or a window/latest bound that
    cannot be parsed, fails -> SEV3 ('historical / cannot prove
    recent').  A corrupt row we cannot date is a localized data-quality
    issue, not proof that the loader just accepted bad data, so it
    tickets without paging.  Previously a garbage date failed loud
    (SEV2) while ``None`` failed quiet (SEV3)."""
    try:
        d = _dt.date.fromisoformat(str(date)[:10])
    except (TypeError, ValueError):
        return False
    if window_start and window_end:
        try:
            ws = _dt.date.fromisoformat(str(window_start)[:10])
            we = _dt.date.fromisoformat(str(window_end)[:10])
        except (TypeError, ValueError):
            return False
        return ws <= d <= we
    if latest:
        try:
            lt = _dt.date.fromisoformat(str(latest)[:10])
        except (TypeError, ValueError):
            return False
        return d >= lt - _dt.timedelta(days=_INTEGRITY_RECENT_DAYS)
    return False


# ---------------------------------------------------------------------
# Payload coercion helpers (INC-T1b B2/B3) — the health report is
# machine-generated but its shape is a contract, not a guarantee: any
# field may arrive as an error string or other non-conforming type.
# Every coercion below degrades to a safe empty value AND, where the
# payload was actually present-but-wrong, emits a SEV2 anomaly finding
# ('can't count = can't trust -> escalate as finding, not silence').
# ---------------------------------------------------------------------

def _as_dict(value) -> dict:
    """Coerce a payload to ``dict`` — anything else degrades to ``{}``
    so a malformed check can never crash classification."""
    return value if isinstance(value, dict) else {}


def _as_list(value) -> list:
    """Coerce a report list field to ``list`` — a bare string becomes a
    one-item list (probably a single ticker), dicts/other scalars
    degrade to ``[]`` rather than iterating garbage."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, str):
        return [value] if value else []
    return []


def _malformed_finding(check: str, detail: str, raw) -> "_Finding":
    """SEV2 anomaly when a report payload has an unusable shape —
    'can't count = can't trust -> escalate as finding, not silence'
    (INC-T1b B2/B3).  SEV2 pages because a check we cannot evaluate is
    a hole in monitoring coverage."""
    return _Finding(
        check=check, detail=detail, severity=2,
        summary=(f"{check}: {detail} arrived as "
                 f"{type(raw).__name__} ({str(raw)[:80]!r}) — the "
                 "check cannot be evaluated reliably"),
        evidence={check: {detail: raw}},
        suggested_action=(
            "Inspect scripts/data_health_report.py --json output — a "
            "check returned a malformed/non-numeric payload (usually "
            "an error string); fix the check and re-run."))


def _check_dict(checks: dict, name: str, findings: list) -> dict:
    """``checks[name]`` as a dict; a non-``None`` non-dict payload emits
    a SEV2 anomaly finding instead of crashing classify (INC-T1b B2)."""
    raw = checks.get(name)
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    findings.append(_malformed_finding(name, "malformed_payload", raw))
    return {}


def _parse_count(value) -> "int | None":
    """Coerce a count-like value to ``int`` — ``None`` when unusable,
    never raises (INC-T1c NB1/NB3).

    ``None`` covers everything that is not a usable count: non-numeric
    strings, bools (``int`` subclasses — JSON ``true``/``false`` is not
    a count, INC-T1c NB3), and non-finite floats/strings.  ``json.loads``
    natively accepts ``Infinity``/``-Infinity``/``1e999``, which arrive
    as ``float('inf')``; ``int(inf)`` raises ``OverflowError`` — outside
    the ``TypeError``/``ValueError`` net that crashed classify (NB1).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return int(f)


def _safe_int(value, default: int = 0) -> int:
    """``int()`` that never raises — helper for counters whose failure
    is already covered elsewhere (unacked_checks, informational rows).
    Bools and non-finite values degrade to ``default`` (INC-T1c NB1)."""
    n = _parse_count(value)
    return default if n is None else n


def _count_field(payload: dict, field: str, check: str,
                 findings: list, default: int = 0) -> int:
    """Parse a numeric count field without ever raising (INC-T1b B3,
    INC-T1c NB1/NB3).

    An unparseable-but-present count — non-numeric strings, bools, or
    non-finite ``inf``/``nan``/``1e999`` values — emits a SEV2 anomaly
    finding (can't count = can't trust) and falls back to ``default``
    (the caller passes ``len()`` of whatever detail list did arrive)."""
    raw = payload.get(field)
    if raw is None:
        return default
    n = _parse_count(raw)
    if n is None:
        findings.append(_malformed_finding(
            check, f"unparseable_{field}", raw))
        return default
    return n


def _severity_num(value, default: int = 4) -> int:
    """Parse ``'SEV2'`` / ``2`` / ``'2'`` -> int in 1..4; junk ->
    ``default`` (stored incident.json is operator-touchable)."""
    s = str(value).strip()
    if s.upper().startswith("SEV"):
        s = s[3:]
    try:
        n = int(s)
    except (TypeError, ValueError):
        return default
    return n if 1 <= n <= 4 else default


def _refetch_action(tickers: str) -> str:
    """Suggested action text for stale/gap/coverage findings (doc §4):
    an idempotent MERGE-upsert re-fetch of the missing window."""
    return (
        "Re-fetch the missing window — idempotent MERGE-upsert: "
        f"daily_load {{\"tickers\": [\"{tickers}\"]}} or "
        "on_request_load for the missing date range."
    )
_DELISTING_ACTION = (
    "Suspected delisting/corporate action: verify ticker status "
    "(stale >=5 sessions with no integrity error).  If confirmed "
    "delisted, add the ticker to acknowledged_delisted in "
    "_config/suppressions.json to downgrade to SEV4."
)


def _builtin_benign_integrity() -> set:
    """Doc §3 known-benign integrity rows: CL=F negative WTI settle
    (2020-04-20/21) and ^IRX zero T-bill yield (2020-03-18..30)."""
    pairs = {("CL=F", "2020-04-20"), ("CL=F", "2020-04-21")}
    d0 = _dt.date(2020, 3, 18)
    pairs |= {
        ("^IRX", str(d0 + _dt.timedelta(days=i))) for i in range(13)
    }
    return pairs


def _load_suppressions(root: Path) -> "tuple[set, set]":
    """Merge built-in benign rows with ``<root>/_config/suppressions.json``.

    File shape: ``{"benign_integrity": [{"ticker","date"}, ...] or
    [[ticker, date], ...], "acknowledged_delisted": ["TICK", ...]}``.
    A missing file is fine; an unparseable file logs a warning and falls
    back to the built-in list only.
    """
    benign = _builtin_benign_integrity()
    ack = set()
    path = root / "_config" / "suppressions.json"
    if not path.is_file():
        return benign, ack
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not parse %s: %s", path, exc)
        return benign, ack
    # justified: per-item shape validation of a small operator-edited
    # config list — first bad entry should be skipped, not crash the run.
    for item in data.get("benign_integrity", []) or []:
        if isinstance(item, dict):
            pair = (item.get("ticker"), item.get("date"))
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            pair = (item[0], item[1])
        else:
            continue
        if pair[0] and pair[1]:
            benign.add((str(pair[0]), str(pair[1])))
    # justified: same small-config per-item normalization as above.
    for t in data.get("acknowledged_delisted", []) or []:
        if t:
            ack.add(str(t))
    return benign, ack


def _prior_evidence_tickers(root: Path) -> set:
    """Tickers proven to have held rows in an earlier ``_reports``
    snapshot — the 'previously had data' signal for the doc §2.1
    coverage rule (silent loss -> SEV2; never-seen ticker -> SEV3)."""
    seen = set()
    reports_dir = root / "_reports"
    if not reports_dir.is_dir():
        return seen
    # justified: one iteration per archived health snapshot; each file
    # must be parsed individually — no vectorized filesystem API exists.
    for path in sorted(reports_dir.glob("*.json")):
        try:
            h = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(h, dict):
            continue
        daily = h.get("daily_close")
        if not isinstance(daily, dict):
            daily = h
        checks = _as_dict((daily or {}).get("checks"))
        lists = (
            _as_dict(checks.get("staleness")).get("stale_tickers"),
            _as_dict(checks.get("gaps")).get("per_ticker"),
            _as_dict(checks.get("integrity")).get("violations"),
            _as_dict(checks.get("duplicates")).get("duplicates"),
            _as_dict(checks.get("raw_close")).get("per_ticker"),
        )
        # justified: fixed 4-element tuple of evidence lists.
        for lst in lists:
            # justified: per-entry dict validation of report JSON.
            for e in _as_list(lst):
                if isinstance(e, dict) and e.get("ticker"):
                    seen.add(str(e["ticker"]))
                elif isinstance(e, str):
                    seen.add(e)
    return seen


def classify_health(health, *, probe_error=None, benign=None,
                    ack_delisted=frozenset(), prior_data=frozenset()):
    """Classify a data_health report into findings (doc §2.1).

    ``health`` is the composite report dict, or ``None`` when the probe
    failed (Flight unreachable / non-JSON) — that alone is a SEV1.
    Accepts a plain dict so tests never touch the Flight server.
    Returns a list of ``_Finding`` sorted SEV1-first; SEV4-level items
    come back with ``suppressed=True``.
    """
    benign = _builtin_benign_integrity() | set(benign or ())
    ack = set(ack_delisted or ())
    prior_data = set(prior_data or ())
    findings = []

    if not isinstance(health, dict):
        findings.append(_Finding(
            check="probe", detail="unreachable", severity=1,
            summary=("data_health probe failed — Flight server "
                     "unreachable or non-JSON payload "
                     f"({probe_error or 'no payload'})"),
            evidence={"probe_error": probe_error, "health": health},
            suggested_action=(
                "Restart the StockIngestion-FlightServer scheduled task "
                "per the ops runbook (scripts/register_flight_service.ps1), "
                "then re-run scripts/data_health_report.py --json."),
        ))
        return findings

    daily = health.get("daily_close")
    if not isinstance(daily, dict):
        if daily is not None:
            # non-dict daily_close section — blind payload, anomaly
            findings.append(_malformed_finding(
                "daily_close", "malformed_payload", daily))
        daily = health  # pre-F17 servers: single-level report schema
    raw_checks = daily.get("checks")
    if raw_checks is None:
        checks = {}
    elif isinstance(raw_checks, dict):
        checks = raw_checks
    else:
        findings.append(_malformed_finding(
            "daily_close", "malformed_checks", raw_checks))
        checks = {}
    latest = daily.get("latest_trading_day")
    gaps = _check_dict(checks, "gaps", findings)
    ws, we = gaps.get("window_start"), gaps.get("window_end")
    gap_entries = {
        e["ticker"]: e for e in _as_list(gaps.get("per_ticker"))
        if isinstance(e, dict) and e.get("ticker")
    }

    # ---- SEV1: any section whose check itself failed ----
    # INC-T1b N3 — a top-level CHECK_FAILED blinds monitoring even when
    # every section still reports OK (doc §2.1 'monitoring is blind').
    if health.get("health_status") == "CHECK_FAILED":
        findings.append(_Finding(
            check="composite", detail="check_failed", severity=1,
            summary=("composite health_status == CHECK_FAILED: "
                     f"{health.get('error', 'unknown error')}"),
            evidence={"health_status": health.get("health_status"),
                      "error": health.get("error")},
            suggested_action=(
                "The health check crashed — investigate the evidence "
                "error (DuckDB file/Flight server), then re-run "
                "scripts/data_health_report.py --json."),
        ))
    sections = {"daily_close": daily}
    for name in _SECTION_NAMES:  # justified: constant 3-tuple, exempt
        sec = health.get(name)
        if sec is None:
            continue
        if isinstance(sec, dict):
            sections[name] = sec
        else:
            # non-dict section — cannot read its health_status;
            # finding-worthy anomaly, never a crash (INC-T1b B2).
            findings.append(_malformed_finding(
                name, "malformed_payload", sec))
    for name, sec in sections.items():  # .items() — exempt iterator
        if sec.get("health_status") == "CHECK_FAILED":
            findings.append(_Finding(
                check=name, detail="check_failed", severity=1,
                summary=(f"{name} health check itself failed: "
                         f"{sec.get('error', 'unknown error')}"),
                evidence={name: sec},
                suggested_action=(
                    "The health check crashed — investigate the evidence "
                    "error (DuckDB file/Flight server), then re-run "
                    "scripts/data_health_report.py --json."),
            ))

    # ---- staleness ----
    # A stale ticker missing >=5 sessions is a suspected delisting/halt
    # (SEV3 'verify ticker status'); fewer sessions are ticketed by the
    # gaps check when present there, else as a small-stale SEV3.
    delisting = set()
    st = _check_dict(checks, "staleness", findings)
    stale_entries = [
        e for e in _as_list(st.get("stale_tickers"))
        if isinstance(e, dict) and e.get("ticker")
    ]
    stale_count = max(
        _count_field(st, "stale_count", "staleness", findings),
        len(stale_entries))
    active = []
    # justified: per-ticker classification — each entry maps to its own
    # incident decision (ack-suppress / delisting / gap deferral).
    for e in stale_entries:
        if str(e["ticker"]) in ack:
            findings.append(_Finding(
                check="stale", detail=e["ticker"], severity=4,
                summary=(f"{e['ticker']} stale since {e.get('last_date')} "
                         "— acknowledged delisted"),
                evidence=e, suggested_action="none", suppressed=True))
        else:
            active.append(e)
    # INC-T5 — acknowledged-delisted tickers are already suppressed
    # per-ticker; they must not feed the >=25 aggregate either.  Only
    # LISTED delisted entries are subtracted: when stale_tickers is
    # truncated the unlisted remainder stays counted (conservative).
    eff_stale_count = stale_count - (len(stale_entries) - len(active))
    # INC-T1b N1 — page on the COUNT even when the ticker list is
    # empty/truncated (`..._stale_count<N>` fallback key).  The guard
    # only drops the SEV2 when every LISTED ticker was ack-suppressed
    # AND the count adds nothing beyond them.
    if eff_stale_count >= _STALE_SEV2_THRESHOLD and (
            active or stale_count > len(stale_entries)):
        tickers = [e["ticker"] for e in active]
        detail = (_multi_detail("multi", tickers) if tickers
                  else f"count{eff_stale_count}")
        findings.append(_Finding(
            check="stale", detail=detail,
            severity=2,
            summary=(f"{eff_stale_count} tickers stale vs latest "
                     f"session {latest} — broad ingestion failure"),
            evidence=st,
            suggested_action=(
                "Broad staleness — re-run the daily load "
                "(daily_load {\"force\": true}); if it persists check "
                "the upstream feed and server logs."),
        ))
    else:
        # justified: per-ticker incident decision — see above.
        for e in active:
            t = str(e["ticker"])
            n = _sessions_between(e.get("last_date"), latest)
            if n is not None and n >= _DELISTING_STALE_SESSIONS:
                delisting.add(t)
                findings.append(_Finding(
                    check="stale", detail=t, severity=3,
                    summary=(f"{t} stale {n} sessions vs latest session "
                             f"{latest} (last data {e.get('last_date')}) "
                             "— suspected delisting/halt"),
                    evidence=e,
                    suggested_action=_DELISTING_ACTION,
                ))
            elif t not in gap_entries:
                findings.append(_Finding(
                    check="stale", detail=t, severity=3,
                    summary=(f"{t} stale vs latest session {latest} "
                             f"(last data {e.get('last_date')})"),
                    evidence=e,
                    suggested_action=_refetch_action(t),
                ))
            # else: the gaps section below raises this ticker's ticket.

    # ---- coverage ----
    # missing_count > 0 for tickers that previously had data -> SEV2
    # (silent loss); never-seen absences -> SEV3 per ticker; a broad
    # absence (>10) is SEV2 regardless — it cannot be all-new tickers.
    cov = _check_dict(checks, "coverage", findings)
    missing = [str(t) for t in _as_list(cov.get("missing_tickers")) if t]
    missing_count = max(
        _count_field(cov, "missing_count", "coverage", findings),
        len(missing))
    if missing_count > _COVERAGE_SEV3_MAX:
        findings.append(_Finding(
            check="coverage",
            # INC-T1b N1 — count-only fallback when the list is
            # truncated/empty (`..._coverage_count<N>` key).
            detail=(_multi_detail("multi", missing) if missing
                    else f"count{missing_count}"),
            severity=2,
            summary=(f"{missing_count} of "
                     f"{cov.get('configured_count', '?')} configured "
                     "tickers absent from daily_close"),
            evidence=cov,
            suggested_action=_refetch_action(
                '", "'.join(missing[:10])),
        ))
    else:
        # INC-T1b N1 — the count exceeds the tickers actually listed
        # (truncated report): a count-only absence we cannot attribute
        # to prior data -> SEV3, same as a never-seen absence.
        if missing_count > len(missing):
            findings.append(_Finding(
                check="coverage", detail=f"count{missing_count}",
                severity=3,
                summary=(f"{missing_count} configured ticker(s) absent "
                         "from daily_close — ticker list truncated/"
                         "unavailable in the report"),
                evidence=cov,
                suggested_action=(
                    "Re-run scripts/data_health_report.py --json for "
                    "the ticker list, then daily_load/on_request_load "
                    "for the missing window.")))
        # justified: per-ticker incident decision — see staleness.
        for t in missing:
            if t in ack:
                findings.append(_Finding(
                    check="coverage", detail=t, severity=4,
                    summary=(f"{t} absent from daily_close — "
                             "acknowledged delisted"),
                    evidence={"ticker": t}, suggested_action="none",
                    suppressed=True))
            elif t in prior_data:
                findings.append(_Finding(
                    check="coverage", detail=t, severity=2,
                    summary=(f"{t} configured but absent from "
                             "daily_close — previously held rows "
                             "(silent ticker loss)"),
                    evidence={"ticker": t, "coverage": cov},
                    suggested_action=_refetch_action(t),
                ))
            else:
                findings.append(_Finding(
                    check="coverage", detail=t, severity=3,
                    summary=(f"{t} configured but absent from "
                             "daily_close (possibly newly configured)"),
                    evidence={"ticker": t, "coverage": cov},
                    suggested_action=_refetch_action(t),
                ))

    # ---- integrity (after known-benign suppression, doc §3) ----
    integ = _check_dict(checks, "integrity", findings)
    # justified: per-violation suppression+window classification — each
    # row is an independent incident candidate.
    for v in _as_list(integ.get("violations")):
        if not isinstance(v, dict):
            continue
        t, d, reason = v.get("ticker"), v.get("date"), v.get("reason")
        if (str(t), str(d)) in benign:
            findings.append(_Finding(
                check="integrity", detail=f"{t}_{d}", severity=4,
                summary=f"{t} {d}: {reason} — known-benign (suppressed)",
                evidence=v, suggested_action="none", suppressed=True))
            continue
        if _in_window(d, ws, we, latest):
            findings.append(_Finding(
                check="integrity", detail=f"{t}_{d}", severity=2,
                summary=(f"{t} {d}: {reason} — recent corrupt row "
                         "accepted by the loader"),
                evidence=v,
                suggested_action=(
                    "Targeted re-fetch for that ticker/date via "
                    "on_request_load — the loader accepted a bad row."),
            ))
        else:
            findings.append(_Finding(
                check="integrity", detail=f"{t}_{d}", severity=3,
                summary=(f"{t} {d}: {reason} — historical corrupt row "
                         "outside the trailing window"),
                evidence=v,
                suggested_action=(
                    "Historical corruption — review, then targeted "
                    "on_request_load re-fetch for that ticker/date."),
            ))

    # ---- duplicates ----
    dup = _check_dict(checks, "duplicates", findings)
    dup_count = _count_field(
        dup, "duplicate_count", "duplicates", findings)
    if dup_count > 0:
        items = _as_list(dup.get("duplicates"))
        if len(items) == 1 and isinstance(items[0], dict):
            detail = str(items[0].get("ticker") or items[0])
        elif items:
            detail = _multi_detail("multi", items)
        else:
            # INC-T1b N1 — count-only fallback (truncated list).
            detail = f"count{dup_count}"
        findings.append(_Finding(
            check="duplicates", detail=detail, severity=2,
            summary=(f"{dup_count} duplicate (Ticker, Date) key(s) — "
                     "violates the PK contract"),
            evidence=dup,
            suggested_action=(
                "Investigate the upsert path — duplicate (Ticker, Date) "
                "keys should be impossible."),
        ))

    # ---- raw_close (HRAW-T1) ----
    # Aggregate per ticker; never one folder per row.  Recent invalid
    # rows in the gap window are SEV2; historical are SEV3.
    # acknowledged-delisted tickers downgrade to SEV4 suppressed.  Any
    # invalid_count not covered by the bounded per_ticker list is a
    # conservative count-level SEV3 incident.  Malformed payloads are
    # handled by _check_dict / _count_field as SEV2 anomalies.
    raw = _check_dict(checks, "raw_close", findings)
    raw_invalid = _count_field(
        raw, "invalid_count", "raw_close", findings)
    if raw_invalid > 0:
        raw_entries = {
            e["ticker"]: e for e in _as_list(raw.get("per_ticker"))
            if isinstance(e, dict) and e.get("ticker")
        }
        accounted = 0
        # justified: per-ticker classification — each aggregate maps to
        # one incident decision (ack-suppress / recent SEV2 / SEV3).
        for t, e in raw_entries.items():  # .items() — exempt
            cnt = _count_field(e, "invalid_count", "raw_close", findings)
            if cnt <= 0:
                continue
            accounted += cnt
            first = e.get("first_date")
            last = e.get("last_date")
            ev = {
                "ticker": t, "invalid_count": cnt,
                "first_date": first, "last_date": last,
            }
            if str(t) in ack:
                findings.append(_Finding(
                    check="raw_close", detail=t, severity=4,
                    summary=(f"{t}: {cnt} invalid RawClose row(s) — "
                             "acknowledged delisted"),
                    evidence=ev, suggested_action="none",
                    suppressed=True))
                continue
            if _in_window(last, ws, we, latest):
                findings.append(_Finding(
                    check="raw_close", detail=t, severity=2,
                    summary=(f"{t}: {cnt} invalid RawClose row(s) "
                             f"ending {last} (recent) — reload required"),
                    evidence=ev,
                    suggested_action=_refetch_action(t)))
            else:
                findings.append(_Finding(
                    check="raw_close", detail=t, severity=3,
                    summary=(f"{t}: {cnt} invalid RawClose row(s) "
                             f"{first}..{last} — historical"),
                    evidence=ev,
                    suggested_action=_refetch_action(t)))
        if raw_invalid > accounted:
            unattrib = raw_invalid - accounted
            findings.append(_Finding(
                check="raw_close", detail=f"count{unattrib}",
                severity=3,
                summary=(f"{unattrib} invalid RawClose row(s) not "
                         "attributable to listed tickers — report "
                         "truncated or ticker list unavailable"),
                evidence={"invalid_count": raw_invalid,
                          "accounted": accounted, "raw_close": raw},
                suggested_action=(
                    "Inspect scripts/data_health_report.py --json for "
                    "the full raw_close detail, then on_request_load "
                    "for affected tickers.")))

    # ---- gaps ----
    # total_missing_days >= 30 -> one group SEV2 covering the run; else
    # per-ticker tickets (>=10 sessions -> SEV2, smaller -> SEV3).
    # INC-T5 — acknowledged-delisted tickers are suppressed per-ticker;
    # their missing days must not feed the AGGREGATE threshold or the
    # single-ticker >=10 trigger either (live defect 2026-10-17: 31
    # delisted days pushed total_missing_days to 33 -> false SEV2 page).
    # Only LISTED delisted days are subtracted: a truncated per_ticker
    # list leaves unattributable days counted (conservative).
    total_missing = _count_field(
        gaps, "total_missing_days", "gaps", findings)
    gap_mc = {}
    ack_gap_days = 0
    # justified: each entry's missing_count is resolved once here (an
    # unparseable count emits exactly ONE anomaly finding) and reused by
    # both the aggregate exclusion and the per-ticker loop below.
    for t, e in gap_entries.items():  # .items() — exempt
        mc = _count_field(e, "missing_count", "gaps", findings)
        gap_mc[t] = mc
        if str(t) in ack:
            ack_gap_days += max(mc, 0)
            findings.append(_Finding(
                check="gap", detail=t, severity=4,
                summary=(f"{t} missing {e.get('missing_count')} "
                         "session(s) — acknowledged delisted"),
                evidence=e, suggested_action="none",
                suppressed=True))
    eff_missing = max(total_missing - ack_gap_days, 0)
    if eff_missing >= _GAP_TOTAL_SEV2_DAYS:
        findings.append(_Finding(
            check="gaps",
            detail=_multi_detail(
                "total",
                sorted(t for t in gap_entries if str(t) not in ack)
                or [f"{eff_missing}d"]),
            severity=2,
            summary=(f"{eff_missing} missing day(s) across "
                     f"{gaps.get('tickers_with_gaps', '?')} tickers in "
                     f"window {ws}..{we}"),
            evidence=gaps,
            suggested_action=(
                "Broad/persistent gap — re-run the daily load "
                "(daily_load {\"force\": true}); persistent per-ticker "
                "gaps -> on_request_load for the window."),
        ))
    else:
        # justified: per-ticker incident decision — see staleness.
        for t, e in gap_entries.items():  # .items() — exempt
            if str(t) in ack:
                continue  # suppressed above — delisted days excluded
            if str(t) in delisting:
                continue  # already ticketed as suspected delisting
            mc = gap_mc[t]
            if mc <= 0:
                continue
            sev = 2 if mc >= _GAP_TICKER_SEV2_DAYS else 3
            findings.append(_Finding(
                check="gap", detail=t, severity=sev,
                summary=(f"{t} missing {mc} session(s) in window "
                         f"{ws}..{we}"),
                evidence=e,
                suggested_action=_refetch_action(t),
            ))

    # ---- section WARN / DISABLED + informational notes ----
    for name in _SECTION_NAMES:  # justified: constant 3-tuple, exempt
        sec = health.get(name)
        if not isinstance(sec, dict):
            continue
        status = sec.get("health_status")
        if status == "WARN":
            findings.append(_Finding(
                check=name, detail="warn", severity=3,
                summary=f"{name} section reported WARN",
                evidence={name: sec},
                suggested_action=(
                    f"Inspect the {name} section of "
                    "scripts/data_health_report.py --json; verify "
                    "next-run self-recovery."),
            ))
        elif status == "DISABLED":
            findings.append(_Finding(
                check=name, detail="disabled", severity=4,
                summary=f"{name} section DISABLED (optional)",
                evidence={name: status}, suggested_action="none",
                suppressed=True))
    md = health.get("company_metadata")
    if isinstance(md, dict):
        nf = _as_dict(_as_dict(md.get("checks")).get("null_fields"))
        if _safe_int(nf.get("null_field_rows"), 0) > 0:
            findings.append(_Finding(
                check="company_metadata", detail="null_fields", severity=4,
                summary=(f"company_metadata: {nf.get('null_field_rows')} "
                         "null-field row(s) — informational by design"),
                evidence=nf, suggested_action="none", suppressed=True))
    pending = daily.get("pending_session")
    if pending:
        findings.append(_Finding(
            check="pending_session", detail=str(pending), severity=4,
            summary=(f"pending session {pending} still open / within "
                     "grace — not counted as missing"),
            evidence={"pending_session": pending},
            suggested_action="none", suppressed=True))

    findings.sort(key=lambda f: f.severity)
    return findings


# ---------------------------------------------------------------------
# Store I/O
# ---------------------------------------------------------------------

def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=1, default=str) + "\n",
                    encoding="utf-8")


def _append_event(root: Path, event: dict) -> None:
    """Append one JSONL audit record to ``<root>/events.jsonl`` (doc §5)."""
    with open(root / "events.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, default=str) + "\n")


def _identity_from_name(name: str) -> str:
    """Fallback identity for a folder lacking a readable incident.json:
    strip the ``YYYYMMDD_sevN_`` prefix."""
    return _FOLDER_PREFIX_RE.sub("", name)


def _scan_active(root: Path) -> dict:
    """identity -> {state, dir, data} for open/ + acknowledged/."""
    found = {}
    # justified: fixed 2-element lifecycle-state tuple (exempt literal).
    for state in ("open", "acknowledged"):
        sdir = root / state
        if not sdir.is_dir():
            continue
        # justified: one iteration per incident folder — dedup is
        # inherently per-directory; no vectorized filesystem API exists.
        for child in sorted(sdir.iterdir()):
            if not child.is_dir():
                continue
            data = _read_json(child / "incident.json") or {}
            identity = data.get("identity") or _identity_from_name(
                child.name)
            found[identity] = {"state": state, "dir": child, "data": data}
    return found


def _unique_dir(path: Path) -> Path:
    """Return ``path`` or ``path_2``/``path_3``… so a resolved folder is
    never overwritten when an incident key recurs."""
    if not path.exists():
        return path
    n = 2
    # justified: tiny bounded suffix walk for name collisions — the
    # common case returns on the first check above.
    while path.with_name(f"{path.name}_{n}").exists():
        n += 1
    return path.with_name(f"{path.name}_{n}")


def _rename_severity(inc_dir: Path, sev_num: int) -> Path:
    """Rename an incident folder after a severity change: keep the
    first-seen date prefix, swap the ``sevN`` token (doc §7.3).  No-op
    when the folder name lacks the standard prefix or the severity
    token is already correct (guards against a phantom ``_2`` dir)."""
    new_name = _FOLDER_PREFIX_RE.sub(
        lambda m: f"{m.group(1)}_sev{sev_num}_", inc_dir.name)
    if new_name == inc_dir.name:
        return inc_dir
    new_dir = _unique_dir(inc_dir.with_name(new_name))
    inc_dir.rename(new_dir)
    return new_dir


def _ticket_md(data: dict) -> str:
    """Render TICKET.md — human summary + copy-paste remediation (doc §4)."""
    hist = "".join(
        f"  - {h.get('ts')}: {h.get('from')} -> {h.get('to')}"
        + (f" ({h.get('reason')})" if h.get("reason") else "")
        for h in data.get("severity_history", [])
    )
    hist = hist or "  - none"
    return (
        f"# {data.get('severity')} incident — {data.get('check')}\n\n"
        f"- Incident key: `{data.get('incident_key')}`\n"
        f"- Severity: {data.get('severity')}\n"
        f"- Check: {data.get('check')}\n"
        f"- State: {data.get('state')}\n"
        f"- First seen: {data.get('first_seen')}\n"
        f"- Last seen: {data.get('last_seen')}\n"
        f"- Unacknowledged checks: {data.get('unacked_checks')}\n\n"
        f"## Summary\n\n{data.get('evidence_summary')}\n\n"
        f"## Suggested action\n\n{data.get('suggested_action')}\n\n"
        f"## Evidence\n\nSee `report.json` (verbatim health-JSON "
        f"fragment).\n\n## Severity history\n\n{hist}\n"
    )


def _write_incident(inc_dir: Path, data: dict, evidence) -> None:
    inc_dir.mkdir(parents=True, exist_ok=True)
    _write_json(inc_dir / "incident.json", data)
    _write_json(inc_dir / "report.json", evidence)
    (inc_dir / "TICKET.md").write_text(_ticket_md(data),
                                     encoding="utf-8")


def _apply_findings(root: Path, findings, yyyymmdd: str,
                    now_iso: str) -> dict:
    """Materialize findings into the store (doc §7.3 lifecycle).

    Returns stats: ``new``/``updated``/``resolved`` folder-name lists,
    ``suppressed`` count, ``suppressed_detail`` — a per-check
    ``{check: {"count": N, "missing_days": M}}`` breakdown so excluded
    acknowledged-delisted gap days stay visible (INC-T5) — and
    ``toasts``: (severity, summary) pairs the caller should page on.
    """
    existing = _scan_active(root)
    seen = set()
    stats = {"new": [], "updated": [], "resolved": [],
             "suppressed": 0, "suppressed_detail": {}, "toasts": []}

    # justified: each finding maps to a distinct incident folder on disk
    # — per-finding filesystem materialization has no SQL/vectorized
    # equivalent; findings are O(incidents).
    for f in findings:
        if f.suppressed or f.severity >= 4:
            stats["suppressed"] += 1
            det = stats["suppressed_detail"].setdefault(
                f.check, {"count": 0})
            det["count"] += 1
            # Aggregate numeric evidence for the suppressed breakdown:
            # gap entries expose missing_days; raw_close exposes invalid_count.
            extra = {}
            if isinstance(f.evidence, dict):
                mdays = _parse_count(f.evidence.get("missing_count"))
                if mdays:
                    det["missing_days"] = (
                        det.get("missing_days", 0) + mdays)
                    extra["missing_days"] = mdays
                icount = _parse_count(f.evidence.get("invalid_count"))
                if icount:
                    det["invalid_count"] = (
                        det.get("invalid_count", 0) + icount)
                    extra["invalid_count"] = icount
            _append_event(root, {
                "ts": now_iso, "event": "suppressed",
                "incident_key": None, "identity": f.identity,
                "severity": f"SEV{f.severity}", "check": f.check,
                "summary": f.summary,
                **extra})
            continue
        # INC-T1b N2 — same-identity findings in ONE run must not mint
        # a `_2` phantom folder: `existing` is a pre-run snapshot, so
        # `seen` (kept severity-sorted, worst first) is the dedup key
        # for anything already materialized this run.
        if f.identity in seen:
            logger.info(
                "duplicate finding identity %s in one run — keeping "
                "the first (worst-severity) instance", f.identity)
            continue
        seen.add(f.identity)
        rec = existing.get(f.identity)
        if rec is None:
            dest = _unique_dir(root / "open"
                               / _folder_name(yyyymmdd, f.severity,
                                              f.identity))
            data = {
                "incident_key": dest.name, "identity": f.identity,
                "severity": f"SEV{f.severity}", "check": f.check,
                "first_seen": now_iso, "last_seen": now_iso,
                "unacked_checks": 1, "severity_history": [],
                "state": "open",
                "suggested_action": f.suggested_action,
                "evidence_summary": f.summary,
            }
            _write_incident(dest, data, f.evidence)
            stats["new"].append(dest.name)
            _append_event(root, {
                "ts": now_iso, "event": "created",
                "incident_key": dest.name, "identity": f.identity,
                "severity": f"SEV{f.severity}", "check": f.check,
                "summary": f.summary})
            if f.severity <= 2:
                stats["toasts"].append((f.severity, f.summary))
            continue

        data = rec["data"]
        inc_dir = rec["dir"]
        data.setdefault("identity", f.identity)
        data["state"] = rec["state"]  # open vs acknowledged by location
        data["last_seen"] = now_iso
        data["suggested_action"] = f.suggested_action
        data["evidence_summary"] = f.summary
        stored_sev = _severity_num(data.get("severity"))
        sev_num = stored_sev
        escalated = False
        # INC-T1b B1 — a recurring finding re-classified WORSE than the
        # stored severity is a real severity change: route it through
        # the same severity_history + folder-rename + SEV1/2-toast path
        # as time-based escalation instead of silently editing
        # incident.json (doc §5 toast + §7.3 rename contract).  A
        # finding re-classified *better* never auto-downgrades an
        # open/acknowledged ticket — the operator still owns the ack.
        if f.severity < stored_sev:
            data.setdefault("severity_history", []).append({
                "ts": now_iso, "from": f"SEV{stored_sev}",
                "to": f"SEV{f.severity}",
                "reason": "finding re-classified at higher severity"})
            sev_num = f.severity
            inc_dir = _rename_severity(inc_dir, sev_num)
            escalated = True
            if sev_num <= 2:
                stats["toasts"].append(
                    (sev_num, f.summary + " (escalated)"))
        if rec["state"] == "open":
            data["unacked_checks"] = _safe_int(
                data.get("unacked_checks"), 0) + 1
            if (data["unacked_checks"] % _ESCALATE_EVERY_N_CHECKS == 0):
                if sev_num > 2:
                    old = f"SEV{sev_num}"
                    sev_num -= 1
                    data.setdefault("severity_history", []).append({
                        "ts": now_iso, "from": old, "to": f"SEV{sev_num}",
                        "reason": "unacknowledged "
                                  f"{data['unacked_checks']} checks"})
                    # Folder rename: keep the first-seen date prefix,
                    # swap the severity token (doc §7.3).
                    inc_dir = _rename_severity(inc_dir, sev_num)
                    escalated = True
                    stats["toasts"].append(
                        (sev_num, f.summary + " (escalated)"))
                elif not escalated:
                    # INC-T1c NB2 — when the re-classification block
                    # above already recorded severity_history and paged
                    # THIS run, a same-run "re-page" would emit a second
                    # toast + history entry for one severity change.
                    data.setdefault("severity_history", []).append({
                        "ts": now_iso, "from": f"SEV{sev_num}",
                        "to": f"SEV{sev_num}",
                        "reason": "re-page: unacknowledged "
                                  f"{data['unacked_checks']} checks"})
                    stats["toasts"].append(
                        (sev_num, f.summary + " (re-page)"))
        data["severity"] = f"SEV{sev_num}"
        # INC-T1c NB4 — self-heal pre-B1 damage: folders whose sevN
        # token drifted from incident.json's severity (the old silent
        # min() merge) are renamed to the now-authoritative severity on
        # the next update.  _rename_severity no-ops when the token is
        # already correct (or the name lacks the standard prefix).
        inc_dir = _rename_severity(inc_dir, sev_num)
        data["incident_key"] = inc_dir.name
        _write_incident(inc_dir, data, f.evidence)
        stats["updated"].append(inc_dir.name)
        _append_event(root, {
            "ts": now_iso,
            "event": "escalated" if escalated else "updated",
            "incident_key": inc_dir.name, "identity": f.identity,
            "severity": data.get("severity"), "check": f.check,
            "summary": f.summary})

    # ---- auto-resolve: active keys absent from this run's findings ----
    for identity, rec in existing.items():  # .items() — exempt
        if identity in seen:
            continue
        data = rec["data"]
        data["state"] = "resolved"
        data["resolved_ts"] = now_iso
        dest = _unique_dir(root / "resolved" / rec["dir"].name)
        shutil.move(str(rec["dir"]), str(dest))
        _write_incident(dest, data, _read_json(dest / "report.json"))
        stats["resolved"].append(dest.name)
        _append_event(root, {
            "ts": now_iso, "event": "resolved",
            "incident_key": dest.name, "identity": identity,
            "severity": data.get("severity"), "check": data.get("check"),
            "summary": "finding absent from latest report — auto-resolved"})
    return stats


def _report_yyyymmdd(health, now: _dt.datetime) -> str:
    """Date prefix for new incident folders — the report's as_of date,
    else today (probe failure has no as_of)."""
    daily = (health or {}).get("daily_close") if isinstance(
        health, dict) else None
    as_of = (daily or health or {}).get("as_of") if isinstance(
        daily or health, dict) else None
    if as_of:
        try:
            return str(as_of)[:10].replace("-", "")
        except Exception:
            pass
    return now.strftime("%Y%m%d")


def _archive_report(root: Path, health, probe_error,
                    now: _dt.datetime) -> None:
    """Snapshot the raw report to ``_reports/YYYY-MM-DD[_HHMMSS].json``
    (audit trail + prior-data evidence for the coverage rule)."""
    reports = root / "_reports"
    payload = health if isinstance(health, dict) else {
        "probe_error": probe_error, "health": health}
    dest = reports / f"{now:%Y-%m-%d}.json"
    if dest.exists():
        dest = reports / f"{now:%Y-%m-%d_%H%M%S}.json"
        n = 2
        # justified: bounded suffix walk for same-second collisions.
        while dest.exists():
            dest = reports / f"{now:%Y-%m-%d_%H%M%S}_{n}.json"
            n += 1
    _write_json(dest, payload)


def _write_heartbeat(root: Path, probe: str, health_status,
                     stats: dict) -> None:
    """Write ``<root>/_last_check.json`` — the watcher liveness signal
    (doc §7.3: open/ empty + fresh heartbeat = healthy)."""
    _write_json(root / "_last_check.json", {
        "ts": _dt.datetime.now().astimezone().isoformat(
            timespec="seconds"),
        "probe": probe,
        "health_status": health_status,
        "new": len(stats.get("new", [])),
        "updated": len(stats.get("updated", [])),
        "resolved": len(stats.get("resolved", [])),
        "suppressed": stats.get("suppressed", 0),
        # additive (INC-T5): per-check suppressed breakdown, incl. the
        # acknowledged-delisted gap days excluded from the aggregate.
        "suppressed_detail": stats.get("suppressed_detail", {}),
    })


def _notify(severity: int, summary: str) -> None:
    """Best-effort Windows toast via scripts/notify_done.ps1 (doc §5).

    Uses the registered-AUMID script — a plain "PowerShell" AppID is
    silently dropped on Win11.  Any failure is logged, never raised.
    """
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-File", _NOTIFY_PS1,
             "-Title", f"Stock ingestion - SEV{severity} incident",
             "-Message", f"INCIDENT SEV{severity}: {summary}"],
            timeout=_TOAST_TIMEOUT_S, capture_output=True)
    except Exception as exc:
        logger.warning("toast notification failed (non-fatal): %s", exc)


def run_watch(root, health=None, *, probe_error=None, now=None,
              notify=None) -> dict:
    """One full watch cycle against store ``root``; returns stats.

    ``health`` is the composite data_health dict (``None`` = probe
    failure -> SEV1).  ``notify`` defaults to the real toast — tests
    inject a stub.  Always writes the heartbeat and report snapshot.
    """
    root = Path(root)
    now = now or _dt.datetime.now().astimezone()
    now_iso = now.isoformat(timespec="seconds")
    # justified: constant tuple of required subdirs (exempt literal).
    for d in _STORE_DIRS:
        (root / d).mkdir(parents=True, exist_ok=True)

    benign, ack_delisted = _load_suppressions(root)
    prior_data = _prior_evidence_tickers(root)
    findings = classify_health(
        health, probe_error=probe_error, benign=benign,
        ack_delisted=ack_delisted, prior_data=prior_data)
    stats = _apply_findings(
        root, findings, _report_yyyymmdd(health, now), now_iso)
    _archive_report(root, health, probe_error, now)
    _write_heartbeat(
        root,
        probe="ok" if isinstance(health, dict) else "unreachable",
        health_status=health.get("health_status") if isinstance(
            health, dict) else None,
        stats=stats)
    sender = notify if notify is not None else _notify
    # justified: at most one toast per new/escalated incident — paging
    # is inherently per-incident and cannot be batched.
    for severity, summary in stats["toasts"]:
        try:
            sender(severity, summary)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("toast sender raised (non-fatal): %s", exc)
    return stats


# ---------------------------------------------------------------------
# Live probe + entrypoint
# ---------------------------------------------------------------------

def _fetch_health(config: PipelineConfig, gap_days: int) -> dict:
    """Call the ``data_health`` action and return the health dict —
    same client pattern as scripts/data_health_report._fetch_health."""
    client = flight.FlightClient(
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    client.wait_for_available(timeout=_FLIGHT_CONNECT_TIMEOUT_S)
    action = flight.Action(
        "data_health", json.dumps({"gap_days": gap_days}).encode("utf-8")
    )
    results = list(
        client.do_action(
            action,
            options=flight.FlightCallOptions(
                timeout=_FLIGHT_ACTION_TIMEOUT_S))
    )
    payload = json.loads(results[0].body.to_pybytes().decode("utf-8"))
    return payload["health"]


def _self_test_health() -> dict:
    """Synthetic WARN fixture mirroring doc §6 (live run 2026-10-12):
    5 stale tickers incl. DBRG 8 sessions, per-ticker gaps, one
    recent-date integrity violation plus the two known-benign rows."""
    return {
        "health_status": "WARN",
        "daily_close": {
            "health_status": "WARN", "mode": "full",
            "as_of": "2026-10-12T18:30:00-04:00",
            "latest_trading_day": "2026-10-09",
            "pending_session": "2026-10-12",
            "checks": {
                "staleness": {
                    "stale_count": 5,
                    "stale_tickers": [
                        {"ticker": "DBRG", "last_date": "2026-09-29"},
                        {"ticker": "QRVO", "last_date": "2026-10-05"},
                        {"ticker": "WBD", "last_date": "2026-10-05"},
                        {"ticker": "LESL", "last_date": "2026-10-08"},
                        {"ticker": "TCBI", "last_date": "2026-10-08"},
                    ],
                },
                "coverage": {"missing_count": 0, "configured_count": 1214,
                             "missing_tickers": []},
                "integrity": {
                    "violation_count": 3,
                    "reason_counts": {"null_or_nonpositive_ohlc": 3},
                    "violations": [
                        {"ticker": "WBD", "date": "2026-10-05",
                         "reason": "null_or_nonpositive_ohlc"},
                        {"ticker": "CL=F", "date": "2020-04-20",
                         "reason": "null_or_nonpositive_ohlc"},
                        {"ticker": "^IRX", "date": "2020-03-25",
                         "reason": "null_or_nonpositive_ohlc"},
                    ],
                },
                "duplicates": {"duplicate_count": 0, "duplicates": []},
                "gaps": {
                    "tickers_with_gaps": 5, "total_missing_days": 18,
                    "expected_sessions": 30,
                    "window_start": "2026-08-28",
                    "window_end": "2026-10-09",
                    "per_ticker": [
                        {"ticker": "DBRG", "missing_count": 8,
                         "missing_days": ["2026-09-30", "2026-10-01",
                                          "2026-10-02", "2026-10-05",
                                          "2026-10-06", "2026-10-07",
                                          "2026-10-08", "2026-10-09"]},
                        {"ticker": "QRVO", "missing_count": 4,
                         "missing_days": ["2026-10-06", "2026-10-07",
                                          "2026-10-08", "2026-10-09"]},
                        {"ticker": "WBD", "missing_count": 4,
                         "missing_days": ["2026-10-06", "2026-10-07",
                                          "2026-10-08", "2026-10-09"]},
                        {"ticker": "LESL", "missing_count": 1,
                         "missing_days": ["2026-10-09"]},
                        {"ticker": "TCBI", "missing_count": 1,
                         "missing_days": ["2026-10-09"]},
                    ],
                },
            },
        },
        "fear_greed": {"health_status": "OK"},
        "weekly": {"health_status": "OK"},
        "company_metadata": {"health_status": "OK"},
    }


def _configure_logging(config: PipelineConfig) -> None:
    """Log to ``cfg.logging.log_file`` honoring ``log_format`` — same
    pattern as scripts/daily_load_once._configure_logging."""
    from pythonjsonlogger import json as jsonlogger

    log_file = config.logging.log_file
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    if config.logging.log_format == "json":
        formatter: logging.Formatter = jsonlogger.JsonFormatter(
            _JSON_FORMAT)
    else:
        formatter = logging.Formatter(_TEXT_FORMAT)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(formatter)
    root.addHandler(handler)


def _configure_fallback_logging() -> None:
    """Last-resort logging when the config cannot be loaded — writes a
    repo-local file plus stderr."""
    os.makedirs(os.path.dirname(_FALLBACK_LOG), exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    file_handler = logging.FileHandler(_FALLBACK_LOG, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(stream_handler)


def _resolve_root(arg: "str | None", self_test: bool) -> Path:
    """Incident store root: --self-test -> fresh temp dir;
    --incidents-root override; else <repo>/incidents when the junction
    exists; else the central store path (doc §7.1)."""
    if self_test:
        return Path(tempfile.mkdtemp(prefix="incident_watch_selftest_"))
    if arg:
        return Path(arg)
    if os.path.exists(_LOCAL_INCIDENTS_LINK):
        return Path(_LOCAL_INCIDENTS_LINK)
    return Path(_DEFAULT_CENTRAL_ROOT)


def _print_summary(root: Path, stats: dict, health,
                   probe_error) -> None:
    """Short stdout summary for the Task Scheduler log + SEV3/4 digest."""
    status = health.get("health_status") if isinstance(health, dict) \
        else "PROBE_FAILED"
    print(f"incident_watch: health_status={status} "
          f"probe={'ok' if isinstance(health, dict) else 'unreachable'} "
          f"root={root}")
    print(f"  new={len(stats['new'])} updated={len(stats['updated'])} "
          f"resolved={len(stats['resolved'])} "
          f"suppressed={stats['suppressed']}")
    if stats.get("suppressed_detail"):
        print("  suppressed_detail="
              + json.dumps(stats["suppressed_detail"], sort_keys=True,
                           default=str))
    # justified: one stdout line per store transition — O(incidents).
    for name in stats["new"]:
        print(f"  + NEW {name}")
    for name in stats["updated"]:
        print(f"  ~ updated {name}")
    for name in stats["resolved"]:
        print(f"  - RESOLVED {name}")
    if stats["toasts"]:
        print(f"  toasts sent: {len(stats['toasts'])}")


def main(argv=None) -> int:
    """Run one incident-watch cycle; return the process exit code."""
    parser = argparse.ArgumentParser(
        description="Incident watcher: classify the data_health report "
                    "into incident folders under the central store.")
    parser.add_argument("--gap-days", type=int, default=30, metavar="N",
                        help="trailing sessions scanned for per-ticker "
                             "gaps (default 30)")
    parser.add_argument("--incidents-root", default=None, metavar="PATH",
                        help="incident store root override (default: "
                             "<repo>/incidents junction or "
                             "C:\\Intel\\incidents\\stock-ingestion)")
    parser.add_argument("--self-test", action="store_true",
                        help="offline run: bundled synthetic WARN "
                             "fixture into a fresh temp dir; exit 0")
    args = parser.parse_args(argv)

    config = None
    if not args.self_test:
        # Self-test must be fully offline — skip config load and file
        # logging so nothing touches cfg.logging.log_file (C:\Intel).
        try:
            config = PipelineConfig.from_yaml(_CONFIG_PATH)
            _configure_logging(config)
        except Exception:
            _configure_fallback_logging()
            logger.exception(
                "incident_watch startup: config/logging setup failed; "
                "using fallback log.")

    root = _resolve_root(args.incidents_root, args.self_test)
    if args.self_test:
        health, probe_error = _self_test_health(), None
        logger.info("self-test: synthetic fixture -> %s", root)
    elif config is None:
        health, probe_error = None, "config unavailable"
    else:
        try:
            health = _fetch_health(config, args.gap_days)
            probe_error = None
        except Exception as exc:
            health, probe_error = None, str(exc)
            logger.error("data_health probe failed: %s", exc)

    try:
        stats = run_watch(root, health=health, probe_error=probe_error)
    except Exception:
        logger.exception(
            "incident_watch crashed during the watch cycle.")
        # Best-effort heartbeat: handlers must still see the watcher ran.
        try:
            root = Path(root)
            # justified: constant tuple (exempt literal).
            for d in _STORE_DIRS:
                (root / d).mkdir(parents=True, exist_ok=True)
            _write_heartbeat(
                root, probe="error",
                health_status=health.get("health_status") if isinstance(
                    health, dict) else None,
                stats={"new": [], "updated": [], "resolved": [],
                       "suppressed": 0})
        except Exception:
            pass
        return 1

    _print_summary(Path(root), stats, health, probe_error)
    return 0


if __name__ == "__main__":
    # Idempotent re-check in case an embedding host nulled the streams.
    _ensure_stdio_fallbacks()
    sys.exit(main())
