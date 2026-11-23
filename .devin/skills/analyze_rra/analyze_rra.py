"""analyze_rra.py — deterministic stats extractor + report drafter for
Relative Rotation Graph (RRA) analysis-prompt JSON files.

Input : docs/rra/*_analysis_prompt*.json
        (system_prompt, analysis_query, instructions, market_snapshot{
         analysis_date, benchmark, lookback_weeks, ticker_data{...}})
Output: a complete draft markdown report (all standardized sections with
        computed tables and rule-based tilt buckets). The invoking agent
        reviews/refines the narrative, then saves to docs/rra/.

Usage:
    python .devin/skills/analyze_rra/analyze_rra.py <input.json>
    python .devin/skills/analyze_rra/analyze_rra.py <input.json> --out <report.md>
    python .devin/skills/analyze_rra/analyze_rra.py <input.json> --stats-only
"""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

VEL_HIGH = 5.0          # velocity_1w threshold for "high-velocity"
VEL_DEAD = 1.0          # velocity_1w below this = dead money
CONSISTENT = 75.0       # consistency_score threshold for "consistent leader"
LAGGED_OUT = 8          # lag_weeks_3m at/above this = structural laggard
TOP_N = 10              # rows shown in ranked tables

# Quadrant-crossing bias taxonomy (see SKILL.md). Diagonal jumps and any
# unlisted transition are discontinuous — treat with suspicion.
CROSSING_BIAS = {
    "LEADING->IMPROVING":   "regime break — bearish shock",
    "WEAKENING->LAGGING":   "breakdown confirmed — bearish",
    "IMPROVING->LAGGING":   "failed improvement — bearish",
    "LEADING->WEAKENING":   "normal aging — trim into strength",
    "IMPROVING->LEADING":   "rotation confirmed — bullish",
    "LAGGING->IMPROVING":   "bottoming — early bullish",
    "LAGGING->WEAKENING":   "hard right break — squeeze, don't chase",
    "WEAKENING->LEADING":   "re-acceleration — bullish",
}
CROSSING_DEFAULT = "discontinuous — treat with suspicion"

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})_analysis_prompt.*\.json$")


def load(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "market_snapshot" not in data or "ticker_data" not in data["market_snapshot"]:
        raise SystemExit(f"{path}: missing market_snapshot.ticker_data")
    return data


def _quadrant_of(r, m):
    """Same >=100/<100 boundary logic as rrg_dashboard.gen_ai_prompt."""
    if r >= 100 and m >= 100:
        return "LEADING"
    if r >= 100:
        return "WEAKENING"
    if m < 100:
        return "LAGGING"
    return "IMPROVING"


def _heading(dr, dm):
    """Compass heading from week-over-week deltas; a zero axis counts
    as positive (same >=0 convention as gen_ai_prompt)."""
    if dr >= 0 and dm >= 0:
        return "NE"
    if dr >= 0:
        return "SE"
    if dm >= 0:
        return "NW"
    return "SW"


def find_prior_prompt(path: Path) -> Path | None:
    """The latest ``*YYYY-MM-DD_analysis_prompt*.json`` in the same
    directory strictly older than ``path`` (by filename date), else
    None — used to derive trajectory semantics on legacy payloads."""
    m = DATE_RE.search(path.name)
    if not m:
        return None
    cur = m.group(1)
    best_d, best_p = "", None
    for p in sorted(path.parent.glob("*_analysis_prompt*.json")):
        if p.resolve() == path.resolve():
            continue
        mm = DATE_RE.search(p.name)
        if mm and cur > mm.group(1) > best_d:
            best_d, best_p = mm.group(1), p
    return best_p


def prior_snapshot(path: Path) -> tuple[dict | None, Path | None]:
    """(market_snapshot, path) of the prior dated prompt file, or
    (None, None). Never raises — legacy fallback must degrade
    gracefully on missing/corrupt files."""
    p = find_prior_prompt(path)
    if p is None:
        return None, None
    try:
        return json.loads(p.read_text(encoding="utf-8")).get("market_snapshot"), p
    except (OSError, ValueError):
        return None, None


def rows(snap: dict, prev_snap: dict | None = None) -> list[dict]:
    prev_data = (prev_snap or {}).get("ticker_data", {})
    out = []
    for tk, d in snap["ticker_data"].items():
        cp, tr, ps = d.get("current_position", {}), d.get("trajectory", {}), d.get("persistence", {})
        lead, lag = ps.get("lead_weeks_3m", 0) or 0, ps.get("lag_weeks_3m", 0) or 0
        prev_r = tr.get("prev_rs_ratio")
        prev_m = tr.get("prev_rs_momentum")
        dr = tr.get("delta_rs_ratio")
        dm = tr.get("delta_rs_momentum")
        heading = tr.get("heading")
        prev_quad = tr.get("prev_quadrant")
        qchange = tr.get("quadrant_change")
        # Legacy payload: derive the trajectory semantics by diffing the
        # prior dated prompt file's current_position for this ticker.
        if heading is None and prev_data:
            pe = prev_data.get(tk, {}).get("current_position", {})
            pr, pm = pe.get("rs_ratio"), pe.get("rs_momentum")
            cr, cm = cp.get("rs_ratio"), cp.get("rs_momentum")
            if None not in (pr, pm, cr, cm):
                prev_r, prev_m, dr, dm = pr, pm, round(cr - pr, 2), round(cm - pm, 2)
                heading = _heading(dr, dm)
                prev_quad = (pe.get("quadrant") or "").upper() or _quadrant_of(pr, pm)
                qchange = ("UNCHANGED" if prev_quad == (cp.get("quadrant") or "").upper()
                           else f"{prev_quad}->{(cp.get('quadrant') or '').upper()}")
        out.append({
            "ticker": tk,
            "rs_ratio": cp.get("rs_ratio"),
            "rs_momentum": cp.get("rs_momentum"),
            "quadrant": (cp.get("quadrant") or "").upper(),
            "angle_deg": cp.get("angle_deg"),
            "velocity_1w": tr.get("velocity_1w", 0.0) or 0.0,
            "vel_dir": (tr.get("velocity_direction") or "").upper(),
            "prev_rs_ratio": prev_r,
            "prev_rs_momentum": prev_m,
            "delta_rs_ratio": dr,
            "delta_rs_momentum": dm,
            "heading": heading,
            "prev_quadrant": prev_quad,
            "quadrant_change": qchange,
            "lead_w": lead,
            "lag_w": lag,
            "consistency": ps.get("consistency_score", 0.0) or 0.0,
            "history_w": lead + lag,
        })
    return out


def fmt(v, nd=1):
    return "n/a" if v is None else f"{v:.{nd}f}"


def table(rs, cols=("ticker", "quadrant", "rs_ratio", "rs_momentum",
                    "velocity_1w", "vel_dir", "lead_w", "lag_w", "consistency")):
    hdr = {"ticker": "Ticker", "quadrant": "Quadrant", "rs_ratio": "RS-Ratio",
           "rs_momentum": "RS-Mom", "velocity_1w": "Vel 1w", "vel_dir": "Dir",
           "heading": "Hdg", "lead_w": "Lead wks", "lag_w": "Lag wks",
           "consistency": "Consist %"}
    lines = ["| " + " | ".join(hdr[c] for c in cols) + " |",
             "|" + "---|" * len(cols)]
    for r in rs:
        lines.append("| " + " | ".join(
            fmt(r[c], 2) if c in ("rs_ratio", "rs_momentum") else
            fmt(r[c]) if c in ("velocity_1w", "consistency") else
            str(r[c] or "—") if c == "heading" else
            str(r[c]) for c in cols) + " |")
    return lines


def build_report(data: dict, src: Path) -> str:
    snap = data["market_snapshot"]
    # Trajectory semantics (heading/deltas/crossings) are native in new
    # payloads; on legacy JSONs they are derived by diffing the prior
    # dated prompt file when one exists, else omitted gracefully.
    native_semantics = all(
        (d.get("trajectory") or {}).get("heading")
        for d in snap["ticker_data"].values())
    prev_snap, prev_path = prior_snapshot(src)
    rs = rows(snap, None if native_semantics else prev_snap)
    has_heading = any(r["heading"] for r in rs)
    asof, bench = snap.get("analysis_date", "unknown"), snap.get("benchmark", "?")
    lookback = snap.get("lookback_weeks", "?")
    instr = data.get("instructions", {})

    census = Counter(r["quadrant"] for r in rs)
    leaders = sorted([r for r in rs if r["quadrant"] == "LEADING"],
                     key=lambda r: (-r["consistency"], -r["velocity_1w"]))
    consistent = [r for r in leaders if r["consistency"] >= CONSISTENT]
    improvers = sorted([r for r in rs if r["quadrant"] == "IMPROVING"],
                       key=lambda r: -r["velocity_1w"])[:TOP_N]
    weakening = sorted([r for r in rs if r["quadrant"] == "WEAKENING"],
                       key=lambda r: -r["velocity_1w"])
    decel_leaders = sorted([r for r in leaders if r["vel_dir"] == "DECELERATING"],
                           key=lambda r: -r["velocity_1w"])[:TOP_N]
    accel = sorted(rs, key=lambda r: -r["velocity_1w"])[:TOP_N]
    dead = sorted([r for r in rs if r["velocity_1w"] < VEL_DEAD],
                  key=lambda r: r["velocity_1w"])
    laggards = sorted([r for r in rs if r["quadrant"] == "LAGGING"],
                      key=lambda r: -r["lag_w"])[:TOP_N]
    # lead+lag < lookback is NORMAL (weeks in IMPROVING/WEAKENING are uncounted);
    # only lead+lag <= 2 means almost no extreme-quadrant history.
    short_hist = [r for r in rs if r["history_w"] <= 2]

    # Rule-based tilt buckets, mutually exclusive in priority order
    # (reduce > trim > avoid > overweight > watch). Agent reviews before saving.
    # An SW heading (both deltas negative — moving toward LAGGING) bars a
    # ticker from Overweight regardless of quadrant label.
    reduce_ = [r["ticker"] for r in weakening]
    trim = [r["ticker"] for r in decel_leaders if r["velocity_1w"] >= VEL_HIGH]
    taken = set(reduce_) | set(trim)
    avoid = [r["ticker"] for r in rs if r["quadrant"] == "LAGGING" and r["lag_w"] >= LAGGED_OUT
             and r["ticker"] not in taken]
    taken |= set(avoid)
    ow = [r["ticker"] for r in rs if r["ticker"] not in taken
          and (r["heading"] or "") != "SW" and (
          (r["quadrant"] == "LEADING" and r["vel_dir"] == "ACCELERATING")
          or (r["quadrant"] == "IMPROVING" and r["vel_dir"] == "ACCELERATING" and r["velocity_1w"] >= VEL_HIGH)
          or (r["consistency"] >= CONSISTENT and r["quadrant"] in ("LEADING", "IMPROVING")))]
    taken |= set(ow)
    watch = [r["ticker"] for r in rs if r["ticker"] not in taken
             and r["quadrant"] == "IMPROVING" and r["rs_momentum"] and
             r["rs_momentum"] >= 110 and (r["rs_ratio"] or 100) < 100]

    L = []
    L.append(f"# RRA Analysis Report — {asof}")
    L.append("")
    L.append(f"- **Data as-of:** {asof}")
    L.append(f"- **Benchmark:** {bench} | **Lookback:** {lookback} weeks | **Universe:** {len(rs)} tickers")
    L.append(f"- **Source:** `{src.name}`")
    if data.get("analysis_query"):
        # Sanitize legacy payloads: the trademarked "RRG" abbreviation is
        # rendered as "RRA" in all report text (project convention).
        L.append(f"- **Query:** {data['analysis_query'].replace('RRG', 'RRA')}")
    rules = instr.get("interpretation_rules")
    if rules:
        L.append(f"- **Quadrant rules:** " + "; ".join(f"{k.replace('_quadrant','')}: {v}" for k, v in rules.items()))
    L.append("")

    L.append("## 1. Quadrant Census")
    L.append("")
    L.append(" | ".join(f"**{q}: {census.get(q, 0)}**" for q in
             ("LEADING", "IMPROVING", "WEAKENING", "LAGGING")))
    L.append("")

    # Hdg column appears in the per-ticker tables whenever trajectory
    # semantics are available (native fields or prior-file diff).
    main_cols = ("ticker", "quadrant", "rs_ratio", "rs_momentum",
                 "velocity_1w", "vel_dir", "heading", "lead_w", "lag_w",
                 "consistency") if has_heading else (
                 "ticker", "quadrant", "rs_ratio", "rs_momentum",
                 "velocity_1w", "vel_dir", "lead_w", "lag_w", "consistency")

    L.append("## 2. Leadership Regime")
    L.append("")
    L.append(f"Consistent leaders (consistency ≥ {CONSISTENT:.0f}%), ranked by consistency then velocity:")
    L.append("")
    L += table(consistent[:TOP_N], cols=main_cols)
    L.append("")

    L.append("## 3. Improving Trends")
    L.append("")
    L.append(f"Top {TOP_N} IMPROVING tickers by 1-week velocity:")
    L.append("")
    L += table(improvers, cols=main_cols)
    L.append("")

    L.append("## 4. Weakening Momentum")
    L.append("")
    L.append("WEAKENING quadrant:")
    L.append("")
    L += table(weakening, cols=main_cols) if weakening else ["*(none)*"]
    L.append("")
    L.append(f"Decelerating LEADING tickers (distribution watch), top {TOP_N} by velocity:")
    L.append("")
    L += table(decel_leaders, cols=main_cols)
    L.append("")

    L.append("## 5. Velocity Analysis")
    L.append("")
    L.append(f"Fastest movers (top {TOP_N} by |velocity_1w|):")
    L.append("")
    L += table(accel, cols=("ticker", "quadrant", "velocity_1w", "vel_dir", "heading", "rs_momentum")
               if has_heading else
               ("ticker", "quadrant", "velocity_1w", "vel_dir", "rs_momentum"))
    L.append("")
    if dead:
        L.append(f"Dead money (velocity < {VEL_DEAD}): " + ", ".join(
            f"`{r['ticker']}` {r['velocity_1w']:.2f}" for r in dead))
        L.append("")

    # Quadrant Crossings — every ticker whose week-over-week quadrant
    # label changed, annotated with the crossing-taxonomy bias label.
    crossings = sorted(
        (r for r in rs
         if r["quadrant_change"] and r["quadrant_change"] != "UNCHANGED"),
        key=lambda r: r["ticker"])
    if crossings:
        L.append("### Quadrant Crossings")
        L.append("")
        for r in crossings:
            bias = CROSSING_BIAS.get(r["quadrant_change"], CROSSING_DEFAULT)
            L.append(f"- `{r['ticker']}` {r['quadrant_change']} — {bias} "
                     f"(heading {r['heading'] or '—'}, "
                     f"Δratio {fmt(r['delta_rs_ratio'], 2)}, "
                     f"Δmom {fmt(r['delta_rs_momentum'], 2)})")
        L.append("")

    L.append("## 6. Structural Laggards")
    L.append("")
    L.append(f"Top {TOP_N} LAGGING by lag-weeks:")
    L.append("")
    L += table(laggards, cols=("ticker", "rs_ratio", "rs_momentum", "velocity_1w", "lag_w", "consistency"))
    L.append("")

    L.append("## 7. Actionable Portfolio Tilts")
    L.append("")
    L.append(f"- **Overweight** ({len(ow)}): " + ", ".join(f"`{t}`" for t in sorted(ow)))
    L.append(f"- **Hold-but-trim** — decelerating leaders ({len(trim)}): " + ", ".join(f"`{t}`" for t in sorted(trim)))
    L.append(f"- **Reduce** — weakening ({len(reduce_)}): " + (", ".join(f"`{t}`" for t in sorted(reduce_)) or "none"))
    L.append(f"- **Avoid** — structural laggards ({len(avoid)}): " + ", ".join(f"`{t}`" for t in sorted(avoid)))
    L.append(f"- **Watch** — improving, RS-mom ≥ 110 but RS-ratio < 100 ({len(watch)}): " +
             (", ".join(f"`{t}`" for t in sorted(watch)) or "none"))
    L.append("")

    L.append("## 8. Data Caveats")
    L.append("")
    L.append(f"- `consistency_score` = lead_weeks_3m / lookback ({lookback}) — share of the "
             "window spent in LEADING. lead+lag < lookback is normal (IMPROVING/WEAKENING "
             "weeks are uncounted); it is NOT insufficient history.")
    if short_hist:
        L.append("- Minimal extreme-quadrant history (lead+lag ≤ 2 — score can't distinguish "
                 "\"new to universe\" from \"always middling\"): " + ", ".join(
                     f"`{r['ticker']}` ({r['lead_w']}/{r['lag_w']})"
                     for r in sorted(short_hist, key=lambda r: r["ticker"])))
    if prev_path and not native_semantics:
        L.append(f"- `heading`/`delta_*`/`prev_*`/`quadrant_change` were derived by "
                 f"diffing `{prev_path.name}` — the input is a legacy payload without "
                 "trajectory semantics.")
    elif not has_heading:
        L.append("- Trajectory semantics (heading/deltas/crossings) unavailable: "
                 "legacy payload and no prior dated prompt file in this directory.")
    L.append("- `angle_deg` convention is not defined in the prompt; do not infer rotation "
             "direction from it alone — use quadrant + `heading` (or, on legacy payloads "
             "without a prior file, quadrant + velocity_direction).")
    L.append("- Tilt buckets are rule-generated; review before acting.")
    L.append("")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", type=Path, help="analysis-prompt JSON file")
    ap.add_argument("--out", type=Path, help="write report here (default: stdout)")
    ap.add_argument("--stats-only", action="store_true",
                    help="print quadrant census + counts only, no report")
    args = ap.parse_args()

    data = load(args.input)
    if args.stats_only:
        rs = rows(data["market_snapshot"])
        print(json.dumps({"asof": data["market_snapshot"].get("analysis_date"),
                          "universe": len(rs),
                          "census": dict(Counter(r["quadrant"] for r in rs))}, indent=2))
        return

    report = build_report(data, args.input)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(report, encoding="utf-8")
        print(f"wrote {args.out} ({len(report)} bytes)", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
