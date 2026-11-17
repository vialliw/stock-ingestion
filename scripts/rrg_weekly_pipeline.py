"""rrg_weekly_pipeline.py — deterministic halves of the weekly RRA
report workflow (design: docs/enhancements/rrg_weekly_automation_design.md).

Phase A ``--generate``
    Resolve ``as_of`` = last Friday, call
    ``rrg_dashboard.generate_rrg_dashboard(config, "ETFs", as_of=as_of,
    json_only=True, output_dir="docs/rra")``, draft the dated analysis
    report via ``.devin/skills/analyze_rra/analyze_rra.py`` ONLY when the
    report file is absent, create ``docs/rra/weekly_summary.md`` via
    ``.devin/skills/summarize_rra/summarize_rra.py`` ONLY when absent,
    and write ``docs/rra/.rrg_pipeline_state.json``.

Phase B — manual enrichment in a Devin session (unchanged; this script
    never writes ``> Read:`` / ``**Net call:**`` narrative).

Phase C ``--publish``
    Enrichment gate (dated report: ``> Weekly read:`` or ``> Read:``
    AND ``**Net call:**``; weekly_summary.md must contain the resolved
    date of every dated report being shipped AND carry no draft
    placeholders — no ``TODO`` tokens, no ``<!-- missing`` markers),
    preflight
    ``git ls-remote``, clone into ``.tmp/rrg-dash``, mirror-sync
    ``docs/rra/*.md`` -> ``report/`` (json never synced), commit as
    ``devin-ai-integration[bot]``, push only on diff, cleanup, EVENTS.log
    entry, toast via ``scripts/notify_done.ps1`` only when published.

Usage:
    python scripts/rrg_weekly_pipeline.py --all        (default; cannot
                                                       combine with
                                                       --generate/--publish)
    python scripts/rrg_weekly_pipeline.py --generate
    python scripts/rrg_weekly_pipeline.py --publish
    python scripts/rrg_weekly_pipeline.py --dry-run    (plan only; never
                                                       clones or pushes)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = Path(__file__).resolve().parent
for _p in (str(_REPO_ROOT), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

DOCS_RRG = _REPO_ROOT / "docs" / "rra"
STATE_FILE = DOCS_RRG / ".rrg_pipeline_state.json"
ANALYZE_SCRIPT = _REPO_ROOT / ".devin" / "skills" / "analyze_rra" / "analyze_rra.py"
SUMMARIZE_SCRIPT = _REPO_ROOT / ".devin" / "skills" / "summarize_rra" / "summarize_rra.py"
SUMMARY_FILE = DOCS_RRG / "weekly_summary.md"
INDEX_FILE = DOCS_RRG / "report_index.md"
CLONE_DIR = _REPO_ROOT / ".tmp" / "rra-dash"
EVENTS_LOG = _REPO_ROOT / "devin_blackboard" / "EVENTS.log"
NOTIFY_SCRIPT = _SCRIPTS_DIR / "notify_done.ps1"
CONFIG_PATH = _REPO_ROOT / "src" / "config.yaml"

REMOTE_URL = "https://github.com/vialliw/Relative-Rotation-Analysis-Dashboard"
GIT_AUTHOR_NAME = "devin-ai-integration[bot]"
GIT_AUTHOR_EMAIL = "158243242+devin-ai-integration[bot]@users.noreply.github.com"
ETF_PREFIX = "Relative_Rotation_Dashboard_ETFs"
PRESET_NAME = "ETFs"

# Dated artifacts produced for a resolved week, e.g.
# Relative_Rotation_Dashboard_ETFs_2026-11-13_analysis_report.md
DATED_REPORT_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_analysis_report\.md$")
PROMPT_RE = re.compile(r"_analysis_prompt\.json$")
WEEKLY_READ_RE = re.compile(r"^>\s*(?:Weekly read|Read):", re.MULTILINE)
NETCALL_RE = re.compile(r"\*\*Net call:\*\*")
# Draft placeholders emitted by summarize_rra.py: the 'TODO(agent):' arc
# line, 'TODO:' row placeholders, and '<!-- missing <field> -->' markers.
TODO_RE = re.compile(r"\bTODO\b")
MISSING_MARKER_RE = re.compile(r"<!--\s*missing")


# ------------------------------------------------------------------
# Pure helpers (unit-tested)
# ------------------------------------------------------------------

def last_friday(today: Optional[date] = None) -> date:
    """Return the most recent Friday on or before ``today``.

    Friday itself maps to itself; Sat/Sun roll back 1/2 days; Mon-Thu
    roll back 3-6 days.  A missed Friday run (StartWhenAvailable on
    Saturday) still resolves to that Friday's bar.
    """
    if today is None:
        today = date.today()
    return today - timedelta(days=(today.weekday() - 4) % 7)


def prompt_to_report_name(name: str) -> str:
    """Map an analysis-prompt filename to its report filename.

    ``..._analysis_prompt.json`` -> ``..._analysis_report.md``; generic
    fallback swaps any ``prompt`` stem segment and the suffix.
    """
    if PROMPT_RE.search(name):
        return PROMPT_RE.sub("_analysis_report.md", name)
    stem = Path(name).stem.replace("prompt", "report")
    return stem + ".md"


def report_date_from_name(name: str) -> Optional[str]:
    """Extract the YYYY-MM-DD week date from a dated report filename."""
    m = DATED_REPORT_RE.search(name)
    return m.group(1) if m else None


def resolved_date_from_prompt(json_path: Path) -> Optional[str]:
    """Resolved weekly-bar date from a prompt JSON payload.

    ``market_snapshot.analysis_date`` is authoritative (it is the actual
    bar date, e.g. Thursday of a holiday-shortened week).  Falls back to
    the date embedded in the filename when the payload is unreadable.
    """
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        value = payload.get("market_snapshot", {}).get("analysis_date")
        if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return value
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        pass
    m = re.search(r"_(\d{4}-\d{2}-\d{2})_analysis_prompt\.json$", json_path.name)
    return m.group(1) if m else None


def report_is_enriched(path: Path) -> bool:
    """Enrichment gate for a dated report: weekly-read + net-call markers."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return bool(WEEKLY_READ_RE.search(text)) and bool(NETCALL_RE.search(text))


def summary_missing_dates(summary_path: Path, dates: list[str]) -> list[str]:
    """Dates the summary file fails to mention (empty list = covers all)."""
    try:
        text = summary_path.read_text(encoding="utf-8")
    except OSError:
        return list(dates)
    return [d for d in dates if d not in text]


def summary_draft_markers(summary_path: Path) -> list[str]:
    """Draft placeholders found in the summary (empty list = polished).

    A freshly drafted weekly_summary.md (raw summarize_rra.py output
    before the manual enrich pass) carries 'TODO(agent):' on the arc
    line, 'TODO:' row placeholders, and/or '<!-- missing ... -->'
    markers.  Any of them means the file must never be published.
    """
    try:
        text = summary_path.read_text(encoding="utf-8")
    except OSError:
        return []  # unreadable is handled by the missing-dates gate
    found = []
    if TODO_RE.search(text):
        found.append("TODO")
    if MISSING_MARKER_RE.search(text):
        found.append("<!-- missing")
    return found


def plan_publish(src_dir: Path,
                 resolved_date: Optional[str] = None) -> dict:
    """Classify every ``src_dir/*.md`` against the enrichment gate.

    Returns ``{"publish": [...], "skip": [{"name","reason"}, ...]}``.
    ``.json`` files are never considered (only ``*.md`` is globbed).

    - dated report -> needs ``> Weekly read:``/``> Read:`` + ``**Net call:**``
    - weekly_summary.md -> needs every shipped dated report's resolved
      date (and the pipeline's ``resolved_date`` when provided) in its
      text AND must contain no draft placeholders (``TODO`` tokens,
      ``<!-- missing`` markers) — a drafted summary never ships
    - report_index.md -> always publishable (deterministic, no markers)
    - other .md (e.g. the flagship report) -> same marker gate as dated
    """
    publish: list[str] = []
    skip: list[dict] = []
    dated_publish_dates: list[str] = []
    summary_name = SUMMARY_FILE.name

    md_files = sorted(src_dir.glob("*.md"))
    for path in md_files:
        name = path.name
        if name == summary_name:
            continue  # gated below once dated set is known
        if name == INDEX_FILE.name:
            publish.append(name)  # deterministic index — always shippable
            continue
        if report_is_enriched(path):
            publish.append(name)
            d = report_date_from_name(name)
            if d:
                dated_publish_dates.append(d)
        else:
            skip.append({"name": name,
                         "reason": "missing '> Weekly read:'/'> Read:' "
                                   "or '**Net call:**' markers"})

    summary_path = src_dir / summary_name
    if summary_path.exists():
        required = list(dated_publish_dates)
        if resolved_date and resolved_date not in required:
            required.append(resolved_date)
        missing = summary_missing_dates(summary_path, required)
        markers = summary_draft_markers(summary_path)
        reasons = []
        if missing:
            reasons.append("summary missing resolved date(s): "
                           + ", ".join(missing))
        if markers:
            reasons.append("summary contains draft placeholder(s): "
                           + ", ".join(markers))
        if reasons:
            skip.append({"name": summary_name,
                         "reason": "; ".join(reasons)})
        else:
            publish.append(summary_name)
    return {"publish": sorted(publish), "skip": skip}


# ------------------------------------------------------------------
# Infrastructure
# ------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(text: str) -> None:
    """Append one line to devin_blackboard/EVENTS.log (same format as
    scripts/poll_tasks.py)."""
    try:
        EVENTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with EVENTS_LOG.open("a", encoding="utf-8") as f:
            f.write(f"\n[{_now_iso()}] {text}")
    except OSError as exc:
        print(f"WARNING: could not append EVENTS.log: {exc}", file=sys.stderr)


def toast(message: str, title: str = "RRA weekly pipeline") -> None:
    """Windows toast via scripts/notify_done.ps1 (registered AUMID).

    Failures are reported on stderr but never raise — a notification
    problem must not fail the pipeline.
    """
    if not NOTIFY_SCRIPT.exists():
        print(f"WARNING: notify script missing: {NOTIFY_SCRIPT}",
              file=sys.stderr)
        return
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-File", str(NOTIFY_SCRIPT),
             "-Title", title, "-Message", message],
            capture_output=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        print(f"WARNING: toast failed: {exc}", file=sys.stderr)


def _run(cmd: list[str], cwd: Optional[Path] = None,
         timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None,
                          capture_output=True, text=True, timeout=timeout)


def _git(clone: Path, *args: str, timeout: int = 300) -> subprocess.CompletedProcess:
    return _run(["git", "-C", str(clone), *args], timeout=timeout)


def _force_rmtree(path: Path) -> None:
    """Best-effort recursive delete that survives Windows read-only files.

    ``git clone`` marks ``.git/objects/*`` read-only, and a plain
    ``shutil.rmtree(..., ignore_errors=True)`` then silently leaves the
    tree behind — debris in ``.tmp/rra-dash`` makes the NEXT run's clone
    fail (exit 128, "already exists and is not an empty directory").
    The ``onexc`` hook (Python 3.12+) clears the read-only attribute and
    retries the failed operation; residual failures are reported on
    stderr (the wipe stays best-effort, but is no longer silent).
    """
    if not path.exists():
        return

    def _writable_retry(func, p, exc):
        try:
            os.chmod(p, stat.S_IWRITE)
            func(p)
        except OSError:
            pass  # best-effort: rmtree keeps going on the rest

    try:
        shutil.rmtree(path, onexc=_writable_retry)
    except Exception as exc:  # noqa: BLE001 — cleanup must never crash the pipeline
        print(f"WARNING: could not fully remove {path}: {exc}",
              file=sys.stderr)


# ------------------------------------------------------------------
# Phase A — generate
# ------------------------------------------------------------------

def generate(dry_run: bool = False,
             today: Optional[date] = None) -> dict:
    """Phase A: JSON + draft report + draft summary + state file."""
    as_of = last_friday(today)
    prompt_name = f"{ETF_PREFIX}_{as_of.isoformat()}_analysis_prompt.json"
    report_name = prompt_to_report_name(prompt_name)
    prompt_path = DOCS_RRG / prompt_name
    report_path = DOCS_RRG / report_name

    plan = {
        "as_of": as_of.isoformat(),
        "prompt_json": str(prompt_path),
        "report_md": str(report_path),
        "summary_md": str(SUMMARY_FILE),
        "state_file": str(STATE_FILE),
    }

    if dry_run:
        print(f"[dry-run] as_of (last Friday) = {as_of.isoformat()}")
        print(f"[dry-run] would call generate_rrg_dashboard(config, "
              f"{PRESET_NAME!r}, as_of={as_of.isoformat()}, json_only=True, "
              f"output_dir='docs/rra') -> {prompt_name}")
        if report_path.exists():
            state = "exists " + ("(enriched — leave untouched)"
                                 if report_is_enriched(report_path)
                                 else "(draft — needs enrichment, leave untouched)")
        else:
            state = "absent — would run analyze_rra.py draft"
        print(f"[dry-run] report {report_name}: {state}")
        print(f"[dry-run] weekly_summary.md: "
              f"{'exists — leave untouched' if SUMMARY_FILE.exists() else 'absent — would run summarize_rra.py --out'}")
        print(f"[dry-run] would write state file {STATE_FILE.name}")
        print(f"[dry-run] would regenerate {INDEX_FILE.name} "
              "(unconditional — deterministic index)")
        print("[dry-run] no files written, no subprocesses run")
        return plan

    # Real run — heavy imports deferred so dry-run/tests stay light.
    from src.config import PipelineConfig  # noqa: E402
    from rrg_dashboard import generate_rrg_dashboard  # noqa: E402

    cfg = PipelineConfig.from_yaml(str(CONFIG_PATH))
    result = generate_rrg_dashboard(cfg, PRESET_NAME, as_of=as_of,
                                    json_only=True, output_dir=str(DOCS_RRG))
    json_path = Path(result["json"])
    # The returned json_path is authoritative (the generator may name the
    # file differently than the naive expectation); derive the report path
    # from it, not from the precomputed name.
    report_path = json_path.parent / prompt_to_report_name(json_path.name)
    resolved = resolved_date_from_prompt(json_path) or as_of.isoformat()

    # Report: draft only when absent; never overwrite an existing file
    # (the enrich pass or an in-flight draft owns it).
    if report_path.exists():
        report_action = ("exists_enriched" if report_is_enriched(report_path)
                         else "exists_draft")
    else:
        proc = _run([sys.executable, str(ANALYZE_SCRIPT), str(json_path),
                     "--out", str(report_path)], cwd=_REPO_ROOT)
        if proc.returncode != 0:
            raise RuntimeError(
                f"analyze_rra.py failed (exit {proc.returncode}): "
                f"{(proc.stderr or "").strip()[:400]}")
        report_action = "drafted_needs_enrichment"

    # weekly_summary.md: create only when absent — the enrich pass owns it.
    if SUMMARY_FILE.exists():
        summary_action = "exists"
    else:
        proc = _run([sys.executable, str(SUMMARIZE_SCRIPT),
                     "--src-dir", str(DOCS_RRG),
                     "--out", str(SUMMARY_FILE)], cwd=_REPO_ROOT)
        if proc.returncode != 0:
            raise RuntimeError(
                f"summarize_rra.py failed (exit {proc.returncode}): "
                f"{(proc.stderr or "").strip()[:400]}")
        summary_action = "drafted"

    # report_index.md: ALWAYS regenerated — it must reflect additions and
    # deletions immediately; output is deterministic (no enrichment step).
    proc = _run([sys.executable, str(SUMMARIZE_SCRIPT),
                 "--src-dir", str(DOCS_RRG),
                 "--index", str(INDEX_FILE)], cwd=_REPO_ROOT)
    if proc.returncode != 0:
        raise RuntimeError(
            f"summarize_rra.py --index failed (exit {proc.returncode}): "
            f"{(proc.stderr or '').strip()[:400]}")

    enriched = report_is_enriched(report_path) if report_path.exists() else False
    missing = summary_missing_dates(SUMMARY_FILE, [resolved])
    state = {
        "as_of": as_of.isoformat(),
        "resolved_date": resolved,
        "prompt_json": json_path.name,
        "report_md": report_path.name,
        "report_action": report_action,
        "report_enriched": enriched,
        "summary_action": summary_action,
        "summary_has_date": not missing,
        "generated_at": _now_iso(),
    }
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n",
                          encoding="utf-8")
    print(f"generate: as_of={as_of} resolved={resolved} "
          f"report={report_action} summary={summary_action}")
    print(f"state -> {STATE_FILE}")
    plan.update(state)
    return plan


# ------------------------------------------------------------------
# Phase C — publish
# ------------------------------------------------------------------

def _state_resolved_date() -> Optional[str]:
    """resolved_date from the last --generate state file, if present."""
    try:
        value = json.loads(STATE_FILE.read_text(encoding="utf-8")) \
            .get("resolved_date")
        return value if isinstance(value, str) else None
    except (OSError, json.JSONDecodeError):
        return None


def publish(dry_run: bool = False) -> int:
    """Phase C: gate -> preflight -> clone -> mirror-sync -> push."""
    resolved = _state_resolved_date()
    plan = plan_publish(DOCS_RRG, resolved_date=resolved)

    print(f"publish gate: {len(plan['publish'])} publishable, "
          f"{len(plan['skip'])} skipped")
    for item in plan["skip"]:
        print(f"  skip {item['name']}: {item['reason']}")

    if dry_run:
        for name in plan["publish"]:
            print(f"[dry-run] would ship report/{name}")
        print("[dry-run] would preflight: git ls-remote " + REMOTE_URL)
        print(f"[dry-run] would wipe+clone {REMOTE_URL} -> {CLONE_DIR}")
        print("[dry-run] would mirror-sync docs/rra/*.md -> report/ "
              "(.json never synced; remote-only .md deleted), "
              "commit as devin-ai-integration[bot], push only on diff")
        print("[dry-run] no clone, no push, no files changed")
        return 0

    if not plan["publish"]:
        print("publish: nothing publishable — remote untouched")
        log_event("[RRG-WEEKLY] publish: nothing passed the enrichment "
                  "gate; remote untouched")
        return 0

    # Preflight: auth/connectivity check before touching the clone.
    proc = _run(["git", "ls-remote", REMOTE_URL, "HEAD"], timeout=120)
    if proc.returncode != 0:
        msg = (f"git ls-remote {REMOTE_URL} failed "
               f"(exit {proc.returncode}): {(proc.stderr or "").strip()[:300]}")
        print(f"publish FAILED: {msg}", file=sys.stderr)
        log_event(f"[RRG-WEEKLY] publish preflight FAILED -> {msg}")
        toast(f"RRA publish preflight failed: {msg[:120]}")
        return 1

    # Fresh clone each run — a dirty/leftover .tmp/rrg-dash is wiped,
    # including read-only .git/objects debris a plain rmtree can't remove.
    if CLONE_DIR.exists():
        _force_rmtree(CLONE_DIR)
    CLONE_DIR.parent.mkdir(parents=True, exist_ok=True)
    proc = _run(["git", "clone", "--depth", "1", REMOTE_URL,
                 str(CLONE_DIR)], timeout=600)
    if proc.returncode != 0:
        msg = f"git clone failed (exit {proc.returncode}): {(proc.stderr or "").strip()[:300]}"
        print(f"publish FAILED: {msg}", file=sys.stderr)
        log_event(f"[RRG-WEEKLY] publish clone FAILED -> {msg}")
        toast(f"RRA publish clone failed: {msg[:120]}")
        return 1

    pushed = 0
    commit_sha = ""
    try:
        report_dir = CLONE_DIR / "report"
        report_dir.mkdir(exist_ok=True)

        # Mirror-sync: copy publishable files in; delete remote-only .md
        # (a local file that failed the gate is NOT remote-only — it
        # stays on the remote rather than being deleted).
        for name in plan["publish"]:
            shutil.copyfile(DOCS_RRG / name, report_dir / name)
        local_names = {p.name for p in DOCS_RRG.glob("*.md")}
        for remote_md in report_dir.glob("*.md"):
            if remote_md.name not in local_names:
                remote_md.unlink()
                print(f"  deleted remote-only {remote_md.name}")

        _git(CLONE_DIR, "add", "-A", "report")
        status = _git(CLONE_DIR, "status", "--porcelain", "report")
        if not (status.stdout or "").strip():
            print("publish: remote report/ already in sync — no diff, "
                  "no commit")
            log_event("[RRG-WEEKLY] publish: no diff after sync; "
                      "remote untouched")
            return 0

        msg_date = resolved or date.today().isoformat()
        commit = _git(
            CLONE_DIR,
            "-c", f"user.name={GIT_AUTHOR_NAME}",
            "-c", f"user.email={GIT_AUTHOR_EMAIL}",
            "commit", "-m",
            f"Weekly RRA report sync ({msg_date}) — {len(plan['publish'])} file(s)")
        if commit.returncode != 0:
            raise RuntimeError(
                f"git commit failed: {(commit.stderr or "").strip()[:300]}")

        push = _git(CLONE_DIR, "push", "origin", "HEAD", timeout=300)
        if push.returncode != 0:
            msg = f"git push failed (exit {push.returncode}): {(push.stderr or "").strip()[:300]}"
            print(f"publish FAILED: {msg}", file=sys.stderr)
            log_event(f"[RRG-WEEKLY] publish push FAILED -> {msg}")
            toast(f"RRA publish push failed: {msg[:120]}")
            return 1

        commit_sha = (_git(CLONE_DIR, "rev-parse", "--short", "HEAD")
                      .stdout or "").strip()
        pushed = len(plan["publish"])
        print(f"publish: pushed {pushed} file(s) -> report/ "
              f"(commit {commit_sha})")
        log_event(f"[RRG-WEEKLY] published {pushed} file(s) -> "
                  f"{REMOTE_URL}/report (commit {commit_sha}); "
                  f"skipped {len(plan['skip'])} unenriched")
        toast(f"RRA weekly publish: {pushed} file(s) -> report/ "
              f"({commit_sha})")
        return 0
    finally:
        _force_rmtree(CLONE_DIR)


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--generate", action="store_true",
                    help="Phase A only: JSON + drafts + state file")
    ap.add_argument("--publish", action="store_true",
                    help="Phase C only: gate + mirror-sync + push")
    ap.add_argument("--all", action="store_true",
                    help="generate then publish (default)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print intended actions only; never writes, "
                         "clones, or pushes")
    args = ap.parse_args(argv)

    # --all already means "generate then publish"; combining it with a
    # single-phase flag is contradictory -> explicit error (exit 2).
    if args.all and (args.generate or args.publish):
        ap.error("--all cannot be combined with --generate/--publish")

    # --all (or no phase flag) runs both; a single phase flag is exclusive.
    explicit = args.generate or args.publish
    do_generate = args.generate or args.all or not explicit
    do_publish = args.publish or args.all or not explicit

    try:
        if do_generate:
            generate(dry_run=args.dry_run)
        if do_publish:
            return publish(dry_run=args.dry_run)
        return 0
    except Exception as exc:  # noqa: BLE001 — pipeline must exit !=0, not traceback-crash silently
        print(f"rrg_weekly_pipeline FAILED: {exc}", file=sys.stderr)
        log_event(f"[RRG-WEEKLY] pipeline FAILED -> {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
