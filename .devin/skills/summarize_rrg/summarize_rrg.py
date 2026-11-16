"""summarize_rrg.py — digest extractor + summary-table drafter for the
dated weekly RRG analysis reports in docs/rrg/.

Input : docs/rrg/*_YYYY-MM-DD_analysis_report.md
        (dated reports produced by the analyze_rrg skill; each carries a
         '> Weekly read:' line under section 1 and a '**Net call:**'
         line under section 7 per the analyze_rrg enrichment contract)
Output: a draft docs/rrg/weekly_summary.md — title, arc-recap placeholder
        and a | Date | Weekly summary | table (ascending). The invoking
        agent rewrites each row into 2-3 polished sentences and fills the
        arc line before saving.

Usage:
    python .devin/skills/summarize_rrg/summarize_rrg.py --digest
    python .devin/skills/summarize_rrg/summarize_rrg.py --out docs/rrg/weekly_summary.md
    python .devin/skills/summarize_rrg/summarize_rrg.py --src-dir docs/rrg
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPORT_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_analysis_report\.md$")
READ_RE = re.compile(r"^>\s*(?:Weekly read|Read):\s*(.*)$")
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
        "# RRG Weekly Summary Table — ETF Dashboard vs SPY",
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


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src-dir", type=Path, default=Path("docs/rrg"),
                    help="directory of dated analysis reports (default: docs/rrg)")
    ap.add_argument("--out", type=Path,
                    help="write draft summary table here (default: stdout)")
    ap.add_argument("--digest", action="store_true",
                    help="print per-date extracted source material as JSON")
    args = ap.parse_args()

    reports = find_reports(args.src_dir)
    if not reports:
        raise SystemExit(f"{args.src_dir}: no dated *_YYYY-MM-DD_analysis_report.md files")
    items = [extract(p) for p in reports]

    if args.digest:
        print(json.dumps(items, indent=2))
        missing = [it["date"] for it in items if not it["complete"]]
        if missing:
            print(f"incomplete enrichment: {', '.join(missing)}", file=sys.stderr)
        return

    table = build_table(items)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(table, encoding="utf-8")
        print(f"wrote {args.out} ({len(table)} bytes, {len(items)} rows)",
              file=sys.stderr)
    else:
        print(table, end="")


if __name__ == "__main__":
    main()
