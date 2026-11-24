"""rra_html.py — styled/sortable HTML renderer for the v4 RRA analysis
reports (docs/rra/*_analysis_report*.md).

Generalized port of the approved one-off renderer .tmp/make_html_v4.py
(approved output: .tmp/rra_report_2026-11-20.html). The .md stays the
internal source of truth (draft -> enrich on .md); the .html produced
here is the publish artifact the weekly pipeline
(scripts/rrg_weekly_pipeline.py) ships to GitHub Pages — see
docs/enhancements/rrg_weekly_automation_design.md.

Transformations (v4 conventions):
- section 5 Quadrant Crossings bullet groups -> ONE table per ###
  group (Ticker|Crossing|Signal|Status|Heading|dRatio|dMom|Landed|Past|
  Persist); rows render green when the crossing lands on ->LEADING,
  red on ->LAGGING
- section 8 tilt bullets -> one table (Tilt|Detail|Count|Tickers);
  Overweight row green, Avoid row red
- prose keyword bolding (quadrants / statuses / directions /
  transitions) — code spans and existing bold are left alone
- census-grid cells get green/red classes derived dynamically from
  ``LEADING (n)`` / ``LAGGING (n)`` cell text (no hardcoded counts)
- the 2x2 census grid gets class="grid"; every other table gets
  class="sortable" (numeric-aware click-toggle asc/desc, ▲/▼ marker)

Usage:
    python rra_html.py <report.md>                  # html -> stdout
    python rra_html.py <report.md> --out <report.html>
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import mistune

# ------------------------------------------------------------------
# Section 5 — crossing bullet groups -> markdown tables
# ------------------------------------------------------------------

_S5_RE = re.compile(r"(## 5\. Quadrant Crossings\n)(.*?)(?=## 6\. )", re.S)
_S5_TRANS = re.compile(r"\*\*([A-Z]+->[A-Z]+)\*\* — (.*)")
_S5_ENTRY = re.compile(
    r"^- `([A-Z0-9]+)` (CONFIRMED|PROVISIONAL_CROSSING|UNCHANGED) — (.*)$")
_S5_ARROW = re.compile(r"[↗↘↙↖]|FLAT")
_S5_DELTAS = (
    re.compile(r"Δratio ([+-]?\d+\.\d+), Δmom ([+-]?\d+\.\d+)"),
    re.compile(r"Δ([+-]?\d+\.\d+)/([+-]?\d+\.\d+)"),
)
_S5_LANDED = re.compile(r"\(landed ([^)]+)\)")
_S5_PAST = re.compile(r"([\d.]+) past boundary")
_S5_PERSIST = re.compile(r"TEMPORARY|PENDING|CONFIRMED")

_S5_HEADER = [
    "| Ticker | Crossing | Signal | Status | Heading | ΔRatio | ΔMom | "
    "Landed | Past | Persist |",
    # separator row must match the 10 header columns exactly or mistune
    # refuses to parse the block as a table
    "|---|---|---|---|---|---|---|---|---|---|",
]


def _crossings_to_tables(src: str) -> str:
    """Rewrite §5 bullet entries as one markdown table per ### group."""
    s5 = _S5_RE.search(src)
    if s5 is None:
        return src  # non-v4 input — leave untouched

    out, rows = [], []
    cur_trans = cur_label = None

    def flush() -> None:
        if rows:
            out.extend([*_S5_HEADER, *rows, ""])
            rows.clear()

    # justified: line-by-line state machine over the §5 body — regex
    # alone cannot track the current ### group / transition header.
    for ln in s5.group(2).split("\n"):
        if ln.startswith("### "):
            flush()
            out.append(ln)
        elif ln.startswith("**") and "->" in ln:
            m = _S5_TRANS.match(ln)
            if m:  # transition header — folded into Crossing/Signal cols
                cur_trans, cur_label = m.group(1), m.group(2)
            else:
                flush()
                out.append(ln)
        elif (m := _S5_ENTRY.match(ln)):
            tk, st, rest = m.groups()
            arrow = (_S5_ARROW.search(rest) or [None])[0] or "—"
            dd = _S5_DELTAS[0].search(rest) or _S5_DELTAS[1].search(rest)
            dr, dm = dd.groups() if dd else ("—", "—")
            landed = past = "—"
            lm = _S5_LANDED.search(rest)
            if lm:
                inner = lm.group(1)
                past = (_S5_PAST.search(inner) or [None, None])[1] or "—"
                landed = (re.sub(
                    r",?\s*(only )?[\d.]+ past boundary.*$", "", inner)
                    .strip() or inner)
            if "whipsaw" in rest and past != "—":
                past += " ⚠"
            ps = _S5_PERSIST.findall(rest)
            pers = ps[-1] if ps else "—"
            rows.append(f"| `{tk}` | {cur_trans} | {cur_label} | {st} | "
                        f"{arrow} | {dr} | {dm} | {landed} | {past} | "
                        f"{pers} |")
        elif ln.strip() == "":
            out.append(ln)
        else:
            flush()
            out.append(ln)
    flush()
    return src[:s5.start(2)] + "\n".join(out) + src[s5.end(2):]


# ------------------------------------------------------------------
# Section 8 — tilt bullets -> a single markdown table
# ------------------------------------------------------------------

_TILTS_RE = re.compile(
    r"(## 8\. Actionable Portfolio Tilts\n\n)(.*?)(?=\n\*\*Net call|\Z)",
    re.S)
_TILT_ROW = re.compile(r"^- \*\*(.+?)\*\*(?: — (.+?))? \((\d+)\): (.*)$")


def _tilts_to_table(src: str) -> str:
    """Rewrite the §8 tilt bullet list as a markdown table."""
    def _sub(m: re.Match) -> str:
        lines_out = ["| Tilt | Detail | Count | Tickers |",
                     "|---|---|---|---|"]
        # justified: each bullet line is classified independently — a
        # TILT match becomes a row, anything else passes through.
        for ln in m.group(2).split("\n"):
            t = _TILT_ROW.match(ln)
            if t:
                b, d, n, tkrs = t.groups()
                lines_out.append(f"| **{b}** | {d or '—'} | {n} | {tkrs} |")
            else:
                lines_out.append(ln)
        return m.group(1) + "\n".join(lines_out) + "\n"

    return _TILTS_RE.sub(_sub, src)


# ------------------------------------------------------------------
# Prose keyword bolding (code spans + existing bold left alone)
# ------------------------------------------------------------------

_KEYWORDS = (
    "LEADING|IMPROVING|WEAKENING|LAGGING|CONFIRMED|PROVISIONAL_CROSSING|"
    "PROVISIONAL|TEMPORARY|PENDING|ACCELERATING|DECELERATING|UNCHANGED|FLAT")
_TRANSITION = re.compile(r"[A-Z]+->[A-Z]+")


def _boldify(line: str) -> str:
    """Bold keyword occurrences in a prose line.

    Code spans (`` `...` ``) and existing ``**bold**`` are stashed so
    their contents are never re-bolded; plain ``A->B`` transitions are
    stashed then re-emitted bolded.
    """
    stash: list[str] = []

    def keep(m: re.Match) -> str:
        stash.append(m.group(0))
        return f"\x00{len(stash) - 1}\x00"

    line = re.sub(r"`[^`]*`|\*\*[^*]+\*\*", keep, line)
    line = re.sub(rf"\b({_TRANSITION.pattern})\b", keep, line)
    stash = [f"**{s}**" if _TRANSITION.fullmatch(s) else s
             for s in stash]
    line = re.sub(rf"\b({_KEYWORDS})\b", r"**\1**", line)
    return re.sub("\x00(\\d+)\x00",
                  lambda m: stash[int(m.group(1))], line)


def _boldify_prose(src: str) -> str:
    """Boldify prose lines only — tables, headings, html untouched."""
    return "\n".join(
        _boldify(ln)
        if (ln.strip() and not ln.lstrip().startswith(("|", "#", "<")))
        else ln
        for ln in src.split("\n"))


# ------------------------------------------------------------------
# HTML post-processing — classes + table roles
# ------------------------------------------------------------------

def _postprocess_html(body: str) -> str:
    # Census-grid cells: green LEADING / red LAGGING — the count inside
    # the parentheses is matched dynamically (any n).
    body = re.sub(r"<td>(<strong>LEADING \(\d+\)</strong>)",
                  r'<td class="q-lead">\1', body)
    body = re.sub(r"<td>(<strong>LAGGING \(\d+\)</strong>)",
                  r'<td class="q-lag">\1', body)
    # §5 rows: green when the crossing lands on ->LEADING, red ->LAGGING.
    body = re.sub(
        r"<tr>(\s*<td><code>[^<]+</code></td>\s*<td>[A-Z]+-&gt;LEADING</td>)",
        r'<tr class="x-lead">\1', body)
    body = re.sub(
        r"<tr>(\s*<td><code>[^<]+</code></td>\s*<td>[A-Z]+-&gt;LAGGING</td>)",
        r'<tr class="x-lag">\1', body)
    # §8 rows: green Overweight / red Avoid.
    body = re.sub(r"<tr>(\s*<td><strong>Overweight</strong></td>)",
                  r'<tr class="x-lead">\1', body)
    body = re.sub(r"<tr>(\s*<td><strong>Avoid</strong></td>)",
                  r'<tr class="x-lag">\1', body)
    # Table roles: the 2x2 census grid (header mentions RS-Momentum) is
    # layout -> class="grid"; every other table is sortable.
    parts = body.split("<table>")
    out = [parts[0]]
    # justified: per-table classification — the census grid is identified
    # by its header row, not by position.
    for chunk in parts[1:]:
        first_row = chunk.split("</tr>", 1)[0]
        cls = "grid" if "RS-Momentum" in first_row else "sortable"
        out.append(f'<table class="{cls}">{chunk}')
    return "".join(out)


# ------------------------------------------------------------------
# Page template
# ------------------------------------------------------------------

_PAGE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>{title}</title>
<style>
  body {{ font-family:'Segoe UI',Calibri,sans-serif; max-width:1180px; margin:32px auto; padding:0 24px; color:#1a1a1a; line-height:1.55; font-size:15px; }}
  h1 {{ font-size:26px; border-bottom:3px solid #2c5f8a; padding-bottom:8px; }}
  h2 {{ font-size:20px; color:#2c5f8a; margin-top:36px; border-bottom:1px solid #cdd7e0; padding-bottom:4px; }}
  h3 {{ font-size:16px; color:#444; }}
  table {{ border-collapse:collapse; width:100%; margin:14px 0; font-size:12.5px; }}
  th,td {{ border:1px solid #d0d7de; padding:6px 10px; text-align:left; vertical-align:top; }}
  th {{ background:#f0f4f8; }}
  tr:nth-child(even) td {{ background:#fafbfc; }}
  tr.x-lead td {{ background:#d4edda; }}
  tr.x-lag td {{ background:#f8d7da; }}
  table.grid td.q-lead {{ background:#d4edda; }}
  table.grid td.q-lag  {{ background:#f8d7da; }}
  table.sortable th {{ cursor:pointer; user-select:none; }}
  table.sortable th:hover {{ background:#dce6ef; }}
  table.sortable th.sorted-asc::after  {{ content:" ▲"; font-size:10px; color:#2c5f8a; }}
  table.sortable th.sorted-desc::after {{ content:" ▼"; font-size:10px; color:#2c5f8a; }}
  code {{ background:#eef2f6; padding:1px 5px; border-radius:3px; font-size:12.5px; }}
  blockquote {{ border-left:4px solid #2c5f8a; background:#f4f8fb; margin:14px 0; padding:8px 16px; color:#333; }}
  blockquote p {{ margin:4px 0; }}
</style></head><body>
{body}
<script>
document.querySelectorAll('table.sortable th').forEach(function(th){{
  th.addEventListener('click', function(){{
    var table=th.closest('table'), idx=[...th.parentNode.children].indexOf(th);
    var asc=!th.classList.contains('sorted-asc');
    table.querySelectorAll('th').forEach(function(h){{h.classList.remove('sorted-asc','sorted-desc');}});
    th.classList.add(asc?'sorted-asc':'sorted-desc');
    var rows=[...table.tBodies[0].rows];
    rows.sort(function(a,b){{
      var x=a.cells[idx].textContent.trim(), y=b.cells[idx].textContent.trim();
      var nx=parseFloat(x), ny=parseFloat(y);
      var cmp=(!isNaN(nx)&&!isNaN(ny))?nx-ny:x.localeCompare(y);
      return asc?cmp:-cmp;
    }});
    rows.forEach(function(r){{table.tBodies[0].appendChild(r);}});
  }});
}});
</script>
</body></html>"""


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_COMPACT_DATE_RE = re.compile(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)")


def title_from_filename(name: str) -> str:
    """Derive the page title from a report filename.

    ``..._YYYY-MM-DD_analysis_report.md`` (and the compact-date flagship
    ``..._analysis_reportYYYYMMDD.md``) -> ``RRA Analysis Report —
    YYYY-MM-DD``; anything else falls back to the file stem.
    """
    m = _DATE_RE.search(name) or _COMPACT_DATE_RE.search(name)
    if m:
        return "RRA Analysis Report — " + "-".join(m.groups())
    return "RRA Analysis Report — " + Path(name).stem


def render(md_text: str, title: str = "RRA Analysis Report") -> str:
    """Render a v4 report .md body to the styled standalone HTML page."""
    src = md_text.replace("\r\n", "\n")
    src = _crossings_to_tables(src)
    src = _tilts_to_table(src)
    src = _boldify_prose(src)
    body = _postprocess_html(mistune.html(src))
    return _PAGE.format(title=title, body=body)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", type=Path, help="report .md file")
    ap.add_argument("--out", type=Path,
                    help="write .html here (default: stdout)")
    ap.add_argument("--title",
                    help="override <title> (default: derived from the "
                         "input filename's date)")
    args = ap.parse_args(argv)

    md_text = args.input.read_text(encoding="utf-8")
    html = render(md_text,
                  args.title or title_from_filename(args.input.name))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(html, encoding="utf-8")
        print(f"wrote {args.out} ({len(html)} bytes)", file=sys.stderr)
    else:
        sys.stdout.write(html)


if __name__ == "__main__":
    main()
