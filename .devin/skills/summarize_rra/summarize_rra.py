"""summarize_rra.py — digest extractor + summary-table drafter for the
dated weekly RRA analysis reports in docs/rra/.

Input : docs/rra/*_YYYY-MM-DD_analysis_report.md
        (dated reports produced by the analyze_rra skill; each carries a
         '> Weekly read:' line under section 1 and a '**Net call:**'
         line under section 7 per the analyze_rra enrichment contract)
Output: a draft docs/rra/weekly_summary.md — title, arc-recap placeholder
        and a | Date | Weekly summary | table (ascending). The invoking
        agent rewrites each row into 2-3 polished sentences and fills the
        arc line before saving.

Usage:
    python .devin/skills/summarize_rra/summarize_rra.py --digest
    python .devin/skills/summarize_rra/summarize_rra.py --out docs/rra/weekly_summary.md
    python .devin/skills/summarize_rra/summarize_rra.py --src-dir docs/rra
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPORT_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_analysis_report\.md$")
READ_RE = re.compile(r"^>\s*(?:Weekly read|Read):\s*(.*)$")
# GitHub Pages base of the publish target — the pipeline ships each dated
# report as <stem>.html under report/ (same convention the remote repo's
# action_update_index.py uses), so index links point at the .html pages.
PAGES_BASE = ("https://vialliw.github.io/"
              "Relative-Rotation-Analysis-Dashboard/report")
NETCALL_RE = re.compile(r"^\*\*Net call:\*\*\s*(.*)$")
CENSUS_RE = re.compile(r"\*\*(LEADING|IMPROVING|WEAKENING|LAGGING):\s*(\d+)\*\*")


def find_reports(src: Path) -> list[Path]:
    return sorted(p for p in src.glob("*_analysis_report.md")
                  if REPORT_RE.search(p.name))


def extract(path: Path) -> dict:
    weekly_read = net_call = None
    census = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if weekly_read is None and (m := READ_RE.match(line)):
            weekly_read = m.group(1).strip()
        if net_call is None and (m := NETCALL_RE.match(line)):
            net_call = m.group(1).strip()
        for q, n in CENSUS_RE.findall(line):
            census[q] = int(n)
    return {
        "date": REPORT_RE.search(path.name).group(1),
        "file": path.name,
        "weekly_read": weekly_read,
        "net_call": net_call,
        "census": census,
        "complete": bool(weekly_read and net_call),
    }


def build_table(items: list[dict]) -> str:
    L = [
        "# RRA Weekly Summary Table — ETF Dashboard vs SPY",
        "",
        f"{len(items)} weekly reports, {items[0]['date']} → {items[-1]['date']} "
        f"(12-week lookback each).",
        "TODO(agent): add the cross-week arc recap here — regime phases + "
        "inflection dates.",
        "",
        "| Date | Weekly summary |",
        "|------|----------------|",
    ]
    for it in items:
        cell = " ".join(x for x in (it["weekly_read"],
                        f"Net call: {it['net_call']}" if it["net_call"] else None)
                        if x)
        if not cell:
            cell = "TODO: no Weekly read / Net call found — read the full report."
        elif not it["complete"]:
            missing = "weekly_read" if not it["weekly_read"] else "net_call"
            cell += f" <!-- missing {missing} -->"
        L.append(f"| {it['date']} | {cell.replace('|', '/')} |")
    return "\n".join(L) + "\n"


def build_index(items: list[dict]) -> str:
    """Deterministic report_index.md — every dated report, one row."""
    L = [
        "# RRA Report Index — ETF Dashboard vs SPY",
        "",
        f"{len(items)} weekly reports, {items[0]['date']} → {items[-1]['date']} "
        f"(12-week lookback each). Regenerated on every pipeline run — do not "
        f"edit by hand.",
        "",
        "| Date | Report | Lead | Impr | Weak | Lag |",
        "|------|--------|------|------|------|-----|",
    ]
    for it in items:
        c = it["census"]
        html_name = it["file"][:-3] + ".html"  # .md -> published .html
        L.append(f"| {it['date']} | [{it['file']}]({PAGES_BASE}/{html_name}) | "
                 f"{c.get('LEADING', '-')} | {c.get('IMPROVING', '-')} | "
                 f"{c.get('WEAKENING', '-')} | {c.get('LAGGING', '-')} |")
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src-dir", type=Path, default=Path("docs/rra"),
                    help="directory of dated analysis reports (default: docs/rra)")
    ap.add_argument("--out", type=Path,
                    help="write draft summary table here (default: stdout)")
    ap.add_argument("--index", type=Path, metavar="OUT",
                    help="write a deterministic report index (report_index.md) "
                         "here — fully generated, no enrichment step")
    ap.add_argument("--digest", action="store_true",
                    help="print per-date extracted source material as JSON")
    args = ap.parse_args()

    reports = find_reports(args.src_dir)
    if not reports:
        raise SystemExit(f"{args.src_dir}: no dated *_YYYY-MM-DD_analysis_report.md files")
    items = [extract(p) for p in reports]

    # --digest / --index / --out are independent outputs — any subset may
    # be combined in one invocation (the old early `return` after --index
    # silently dropped a --out passed alongside it).
    acted = False

    if args.digest:
        acted = True
        print(json.dumps(items, indent=2))
        missing = [it["date"] for it in items if not it["complete"]]
        if missing:
            print(f"incomplete enrichment: {', '.join(missing)}", file=sys.stderr)

    if args.index:
        acted = True
        index = build_index(items)
        args.index.parent.mkdir(parents=True, exist_ok=True)
        args.index.write_text(index, encoding="utf-8")
        print(f"wrote {args.index} ({len(index)} bytes, {len(items)} rows)",
              file=sys.stderr)

    if args.out:
        acted = True
        table = build_table(items)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(table, encoding="utf-8")
        print(f"wrote {args.out} ({len(table)} bytes, {len(items)} rows)",
              file=sys.stderr)

    if not acted:
        # No output flag at all -> draft table to stdout (original default).
        print(build_table(items), end="")


if __name__ == "__main__":
    main()
