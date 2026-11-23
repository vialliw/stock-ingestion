"""rrg_enrich_headless.py — Phase B of the weekly RRA report pipeline:
automate the enrichment step via a headless local Devin session
(design: docs/enhancements/rrg_weekly_automation_design.md).

Reads ``docs/rra/.rrg_pipeline_state.json`` (written by
``scripts/rrg_weekly_pipeline.py --generate``).  When the report is
still a draft (``report_enriched`` flag false AND the file itself lacks
the ``> Weekly read:``/``**Net call:**`` markers — the flag can lag a
manual enrichment) OR ``weekly_summary.md`` does not yet cover
``resolved_date`` — the same criteria the Phase C publish gate applies —
this wrapper spawns ONE headless session:

    devin.exe -p "<ENRICHMENT PROMPT>"

via the bundled Devin CLI at
``%LOCALAPPDATA%\\Programs\\Devin\\resources\\app\\extensions\\windsurf\\devin\\bin\\devin.exe``
(fallback: ``devin`` on PATH), cwd = repo root.  The session enriches
the report markers (``> Weekly read:``/``> Read:`` + ``**Net call:**``)
and updates ``weekly_summary.md`` / ``report_index.md`` so the next
Phase C publish trigger can ship them.

Safety semantics:

- Mutual exclusion: ``devin_blackboard/locks/rrg_enrich.lock``
  (``O_CREAT | O_EXCL``); a held lock exits WITHOUT spawning.  Stale
  locks (>10 min, malformed, or missing ``claimed_at``) are recovered —
  same convention as ``scripts/poll_tasks.py``.
- Timeout: the session is killed after 30 minutes.
- NEVER blocks the pipeline: non-zero session exit, spawn failure,
  timeout, or a wrapper-internal error only produce a logged warning;
  this script always exits 0.  The publish gate holds drafts either way.
- ``--dry-run`` prints the resolved no-op or the single filled spawn
  command and touches nothing (no lock, no spawn, no EVENTS.log append).

Scheduled by ``scripts/register_rrg_enrich.ps1`` as
``StockIngestion-RRGEnrich`` (Fri 22:45 + Sat/Sun 08:30).

Usage:
    python scripts/rrg_enrich_headless.py            # no-op or spawn
    python scripts/rrg_enrich_headless.py --dry-run  # plan only
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parent.parent

DOCS_RRA = _REPO_ROOT / "docs" / "rra"
STATE_FILE = DOCS_RRA / ".rrg_pipeline_state.json"
SUMMARY_FILE = DOCS_RRA / "weekly_summary.md"
EVENTS_LOG = _REPO_ROOT / "devin_blackboard" / "EVENTS.log"
LOCK_FILE = _REPO_ROOT / "devin_blackboard" / "locks" / "rrg_enrich.lock"

ENRICH_TIMEOUT_S = 30 * 60          # kill the session after 30 minutes
STALE_LOCK_SECONDS = 600            # poll_tasks.py convention: >10 min

# Bundled Devin CLI relative to %LOCALAPPDATA% (see AC: headless `devin -p`).
BUNDLED_DEVIN_REL = Path(
    "Programs") / "Devin" / "resources" / "app" / "extensions" / \
    "windsurf" / "devin" / "bin" / "devin.exe"

# Publish-gate criteria mirrored from scripts/rrg_weekly_pipeline.py so
# this wrapper stays self-contained (stdlib-only, no sibling import).
WEEKLY_READ_RE = re.compile(r"^>\s*(?:Weekly read|Read):", re.MULTILINE)
NETCALL_RE = re.compile(r"\*\*Net call:\*\*")
TODO_RE = re.compile(r"\bTODO\b")
MISSING_MARKER_RE = re.compile(r"<!--\s*missing")
DATED_REPORT_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_analysis_report\.md$")

# Prompt handed to `devin -p`.  Placeholders {json_path}, {report_path},
# {resolved_date} are filled from the state file.  Keep it free of other
# braces so str.format cannot leave stray placeholders.
ENRICHMENT_PROMPT = """\
You are a non-interactive headless session inside the stock_ingestion
repo (working directory = repo root).  Perform Phase B (enrichment) of
the weekly RRA report pipeline for the resolved week {resolved_date}.

Inputs (from docs/rra/.rrg_pipeline_state.json):
  - analysis prompt JSON: {json_path}
  - analysis report file: {report_path}
  - resolved week date:   {resolved_date}

Do exactly this, then stop:

1. DRAFT (only if missing).  If the report file does NOT exist, generate
   the rule-based draft first:
     python .devin/skills/analyze_rra/analyze_rra.py "{json_path}" --out "{report_path}"
   If the report already exists (draft or enriched), skip this step —
   never regenerate over an existing report.

2. ENRICH the report per .devin/skills/analyze_rra/SKILL.md:
   - a `> Weekly read:` (dated series) or `> Read:` blockquote at the END
     of each of sections 1-6 (after the section's table, before the next
     heading);
   - a `**Net call:** ...` bold line at the end of section 7;
   - keep every rule-generated table verbatim; fix only names the rules
     misbucketed; write the narrative per the skill's per-section
     guidance and gotchas.

3. UPDATE docs/rra/weekly_summary.md per
   .devin/skills/summarize_rra/SKILL.md:
   - `python .devin/skills/summarize_rra/summarize_rra.py --src-dir docs/rra --digest`
     gives the source material;
   - write a NEW enriched row for {resolved_date}: 2-3 sentences covering
     regime character/breadth, key movers or signals (ticker names and
     velocities verbatim), and the call;
   - refresh the arc-recap line so it names every regime phase in order
     including this week; leave no `TODO` or `<!-- missing` placeholders;
   - regenerate the deterministic index:
     python .devin/skills/summarize_rra/summarize_rra.py --src-dir docs/rra --index docs/rra/report_index.md

4. VERIFY, then STOP.  Both markers must be present before you finish:
   - the report file contains a `> Weekly read:`/`> Read:` line AND a
     `**Net call:**` line;
   - docs/rra/weekly_summary.md contains the date {resolved_date}.
   If a step cannot be completed, leave partial files in place (the
   publish gate will hold drafts), append one line to
   devin_blackboard/EVENTS.log describing what failed, and stop — do not
   retry in a loop.

Constraints:
- Edit ONLY docs/rra/*.md files plus append to
  devin_blackboard/EVENTS.log (tag your line [ENRICH-T1] or [SKILL]).
  Do NOT modify src/, tests/, scripts/, config files, or any .json input.
- Do NOT git commit, push, or run the publish phase.
- Do NOT ask questions; there is no human on this session.
"""


# ------------------------------------------------------------------
# Pure helpers (unit-tested)
# ------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state(path: Path) -> Optional[dict]:
    """State file contents, or None when missing/empty/corrupt/not-a-dict."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def resolved_date_of(state: dict) -> str:
    """resolved_date from state; falls back to the report filename date."""
    value = state.get("resolved_date")
    if isinstance(value, str) and value:
        return value
    m = DATED_REPORT_RE.search(str(state.get("report_md", "")))
    return m.group(1) if m else ""


def report_is_enriched(path: Path) -> bool:
    """Publish-gate marker check for a dated report (mirrors pipeline)."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return bool(WEEKLY_READ_RE.search(text)) and bool(NETCALL_RE.search(text))


def summary_missing_dates(summary_path: Path, dates: list) -> list:
    """Dates the summary fails to mention (mirrors pipeline gate)."""
    try:
        text = summary_path.read_text(encoding="utf-8")
    except OSError:
        return list(dates)
    return [d for d in dates if d not in text]


def summary_draft_markers(summary_path: Path) -> list:
    """Draft placeholders in the summary (mirrors pipeline gate)."""
    try:
        text = summary_path.read_text(encoding="utf-8")
    except OSError:
        return []
    found = []
    if TODO_RE.search(text):
        found.append("TODO")
    if MISSING_MARKER_RE.search(text):
        found.append("<!-- missing")
    return found


def enrichment_complete(state: dict, summary_path: Path) -> bool:
    """No-op check mirroring the Phase C publish gate.

    Complete when (a) the report is enriched by ANY evidence — the
    ``report_enriched`` state flag OR the actual on-disk markers
    (``> Weekly read:``/``> Read:`` + ``**Net call:**``, the same
    regexes the publish gate applies) — AND (b) ``weekly_summary.md``
    mentions the resolved week date.  Checking the file itself covers
    the stale-flag case: a report enriched in a manual session leaves
    ``report_enriched=false`` until the next --generate, and must not
    trigger a wasted headless session.
    """
    resolved = resolved_date_of(state)
    if not resolved:
        return False
    report_path = DOCS_RRA / str(state.get("report_md", ""))
    report_done = bool(state.get("report_enriched")) \
        or report_is_enriched(report_path)
    if not report_done:
        return False
    return not summary_missing_dates(summary_path, [resolved])


def find_devin_exe(localappdata: Optional[str] = None) -> str:
    """Bundled Devin CLI under %LOCALAPPDATA%, else 'devin' on PATH.

    The returned string is used as argv[0]; when neither exists the bare
    name 'devin' is returned and the spawn simply fails — a logged
    warning, never a crash.
    """
    base = localappdata if localappdata is not None \
        else os.environ.get("LOCALAPPDATA", "")
    if base:
        bundled = Path(base) / BUNDLED_DEVIN_REL
        if bundled.is_file():
            return str(bundled)
    return shutil.which("devin") or "devin"


def build_command(state: dict, devin_exe: Optional[str] = None) -> Optional[list]:
    """[devin, '-p', filled_prompt] or None when state lacks file names."""
    prompt_json = state.get("prompt_json")
    report_md = state.get("report_md")
    if not prompt_json or not report_md:
        return None
    prompt = ENRICHMENT_PROMPT.format(
        json_path=DOCS_RRA / prompt_json,
        report_path=DOCS_RRA / report_md,
        resolved_date=resolved_date_of(state),
    )
    return [devin_exe or find_devin_exe(), "-p", prompt]


def verify_markers(state: dict) -> dict:
    """Post-run publish-gate verification against the actual artifacts."""
    resolved = resolved_date_of(state)
    report_path = DOCS_RRA / str(state.get("report_md", ""))
    return {
        "report_markers": report_is_enriched(report_path),
        "summary_has_date": (not summary_missing_dates(SUMMARY_FILE, [resolved])
                             if resolved else False),
        "summary_draft_markers": summary_draft_markers(SUMMARY_FILE),
    }


# ------------------------------------------------------------------
# Infrastructure
# ------------------------------------------------------------------

def log_event(text: str) -> None:
    """Append one line to devin_blackboard/EVENTS.log (poll_tasks format)."""
    try:
        EVENTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with EVENTS_LOG.open("a", encoding="utf-8") as f:
            f.write(f"\n[{_now_iso()}] {text}")
    except OSError as exc:
        print(f"WARNING: could not append EVENTS.log: {exc}",
              file=sys.stderr)


def _lock_is_stale(path: Path) -> bool:
    """poll_tasks convention: malformed, missing claimed_at, or >10 min."""
    if not path.exists():
        return False
    try:
        lock = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return True  # exists but malformed = stale
    claimed_at = lock.get("claimed_at") if isinstance(lock, dict) else None
    if not claimed_at:
        return True
    try:
        claimed_dt = datetime.fromisoformat(claimed_at)
        if claimed_dt.tzinfo is None:
            claimed_dt = claimed_dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - claimed_dt).total_seconds()
        return age > STALE_LOCK_SECONDS
    except (ValueError, TypeError):
        return True  # unparseable timestamp = stale


def _acquire_lock(path: Path) -> bool:
    """Atomic O_CREAT|O_EXCL lock; recovers stale locks per poll_tasks."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return False
    for _attempt in range(2):
        # justified: bounded two-try acquire — first try, then one retry
        # after removing a stale lock; not a data-iteration loop.
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if _attempt == 0 and _lock_is_stale(path):
                try:
                    path.unlink()
                except OSError:
                    return False
                continue
            return False
        with os.fdopen(fd, "wb") as f:
            f.write(json.dumps({"agent": "rrg_enrich_headless",
                                "claimed_at": _now_iso()}).encode("utf-8"))
        return True
    return False


def _release_lock(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _spawn_and_wait(cmd: list, timeout: int = ENRICH_TIMEOUT_S) -> int:
    """Spawn the headless session; return its exit code.

    Returns -1 when the spawn itself fails, -2 when the session was
    killed after ``timeout`` seconds.  Never raises.
    """
    try:
        proc = subprocess.Popen(cmd, cwd=str(_REPO_ROOT))
    except OSError as exc:
        log_event(f"[ENRICH-T1] WARNING spawn FAILED -> {exc}")
        print(f"enrich: could not spawn {cmd[0]}: {exc}", file=sys.stderr)
        return -1
    log_event(f"[ENRICH-T1] spawned headless session pid={proc.pid} "
              f"cmd={Path(str(cmd[0])).name} -p <prompt> "
              f"cwd={_REPO_ROOT}")
    print(f"enrich: spawned pid={proc.pid}; waiting up to {timeout}s")
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        log_event(f"[ENRICH-T1] WARNING session pid={proc.pid} killed "
                  f"after {timeout}s timeout")
        print(f"enrich: session killed after {timeout}s timeout",
              file=sys.stderr)
        return -2


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def run(dry_run: bool = False) -> int:
    """No-op, spawn-and-wait, or dry-run plan.  Always returns 0."""
    state = load_state(STATE_FILE)
    if state is None:
        msg = (f"state file missing or unreadable: {STATE_FILE.name} — "
               "nothing to enrich")
        print(f"[dry-run] no-op: {msg}" if dry_run else f"enrich: {msg}")
        if not dry_run:
            log_event(f"[ENRICH-T1] no-op: {msg}")
        return 0

    resolved = resolved_date_of(state)
    if enrichment_complete(state, SUMMARY_FILE):
        msg = (f"no-op: report already enriched (flag or markers) and "
               f"weekly_summary.md already covers {resolved}")
        print(f"[dry-run] {msg}" if dry_run else f"enrich: {msg}")
        return 0

    cmd = build_command(state)
    if cmd is None:
        msg = ("state pending but lacks prompt_json/report_md — "
               "cannot build the enrichment prompt")
        print(f"[dry-run] WARNING: {msg}" if dry_run
              else f"enrich: WARNING {msg}", file=sys.stderr)
        if not dry_run:
            log_event(f"[ENRICH-T1] WARNING {msg}")
        return 0

    if dry_run:
        covers = not summary_missing_dates(SUMMARY_FILE, [resolved]) \
            if resolved else False
        print(f"[dry-run] state PENDING (report_enriched="
              f"{state.get('report_enriched')}, summary covers "
              f"{resolved or '?'}: {covers})")
        print(f"[dry-run] would spawn exactly 1 headless session, "
              f"cwd={_REPO_ROOT}:")
        print(f"  {subprocess.list2cmdline(cmd)}")
        print("[dry-run] no lock taken, nothing spawned, "
              "EVENTS.log untouched")
        return 0

    if not _acquire_lock(LOCK_FILE):
        msg = (f"another enrich run holds {LOCK_FILE.name} — "
               "exiting without spawn")
        print(f"enrich: {msg}")
        log_event(f"[ENRICH-T1] skipped: {msg}")
        return 0
    try:
        log_event(f"[ENRICH-T1] state pending -> spawning headless "
                  f"devin -p session (resolved_date={resolved or '?'})")
        rc = _spawn_and_wait(cmd)
        if rc == 0:
            log_event(f"[ENRICH-T1] session exit code 0")
        else:
            log_event(f"[ENRICH-T1] WARNING session exit code {rc} — "
                      "enrichment may be incomplete; publish gate still "
                      "applies")
            print(f"enrich: WARNING session exit code {rc}",
                  file=sys.stderr)
        markers = verify_markers(state)
        log_event(f"[ENRICH-T1] post-run marker verification: "
                  f"report_markers={markers['report_markers']} "
                  f"summary_has_date={markers['summary_has_date']} "
                  f"summary_draft_markers={markers['summary_draft_markers']}")
        if not markers["report_markers"] or not markers["summary_has_date"]:
            print("enrich: post-run markers still missing — publish "
                  "gate will hold the draft", file=sys.stderr)
        return 0
    finally:
        _release_lock(LOCK_FILE)


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="print the resolved no-op or the single filled "
                         "spawn command; touches nothing")
    args = ap.parse_args(argv)
    try:
        return run(dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001 — must never block the pipeline
        print(f"rrg_enrich_headless WARNING: wrapper error: {exc}",
              file=sys.stderr)
        try:
            log_event(f"[ENRICH-T1] WARNING wrapper error -> {exc}")
        except Exception:  # noqa: BLE001
            pass
        return 0


if __name__ == "__main__":
    sys.exit(main())
