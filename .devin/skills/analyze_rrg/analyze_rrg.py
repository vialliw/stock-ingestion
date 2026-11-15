"""analyze_rrg.py — deterministic stats extractor + report drafter for
Relative Rotation Graph (RRG) analysis-prompt JSON files.

Input : docs/rrg/*_analysis_prompt*.json
        (system_prompt, analysis_query, instructions, market_snapshot{
         analysis_date, benchmark, lookback_weeks, ticker_data{...}})
Output: a complete draft markdown report (all standardized sections with
        computed tables and rule-based tilt buckets). The invoking agent
        reviews/refines the narrative, then saves to docs/rrg/.

Usage:
    python .devin/skills/analyze_rrg/analyze_rrg.py <input.json>
    python .devin/skills/analyze_rrg/analyze_rrg.py <input.json> --out <report.md>
    python .devin/skills/analyze_rrg/analyze_rrg.py <input.json> --stats-only
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

VEL_HIGH = 5.0          # velocity_1w threshold for "high-velocity"
VEL_DEAD = 1.0          # velocity_1w below this = dead money
CONSISTENT = 75.0       # consistency_score threshold for "consistent leader"
LAGGED_OUT = 8          # lag_weeks_3m at/above this = structural laggard
TOP_N = 10              # rows shown in ranked tables


def load(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "market_snapshot" not in data or "ticker_data" not in data["market_snapshot"]:
        raise SystemExit(f"{path}: missing market_snapshot.ticker_data")
    return data


def rows(snap: dict) -> list[dict]:
    out = []
    for tk, d in snap["ticker_data"].items():
        cp, tr, ps = d.get("current_position", {}), d.get("trajectory", {}), d.get("persistence", {})
        lead, lag = ps.get("lead_weeks_3m", 0) or 0, ps.get("lag_weeks_3m", 0) or 0
        out.append({
            "ticker": tk,
            "rs_ratio": cp.get("rs_ratio"),
            "rs_momentum": cp.get("rs_momentum"),
            "quadrant": (cp.get("quadrant") or "").upper(),
            "angle_deg": cp.get("angle_deg"),
            "velocity_1w": tr.get("velocity_1w", 0.0) or 0.0,
            "vel_dir": (tr.get("velocity_direction") or "").upper(),
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
           "lead_w": "Lead wks", "lag_w": "Lag wks", "consistency": "Consist %"}
    lines = ["| " + " | ".join(hdr[c] for c in cols) + " |",
             "|" + "---|" * len(cols)]
    for r in rs:
        lines.append("| " + " | ".join(
            fmt(r[c], 2) if c in ("rs_ratio", "rs_momentum") else
            fmt(r[c]) if c in ("velocity_1w", "consistency") else
            str(r[c]) for c in cols) + " |")
    return lines


def build_report(data: dict, src: Path) -> str:
    snap = data["market_snapshot"]
    rs = rows(snap)
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
    reduce_ = [r["ticker"] for r in weakening]
    trim = [r["ticker"] for r in decel_leaders if r["velocity_1w"] >= VEL_HIGH]
    taken = set(reduce_) | set(trim)
    avoid = [r["ticker"] for r in rs if r["quadrant"] == "LAGGING" and r["lag_w"] >= LAGGED_OUT
             and r["ticker"] not in taken]
    taken |= set(avoid)
    ow = [r["ticker"] for r in rs if r["ticker"] not in taken and (
          (r["quadrant"] == "LEADING" and r["vel_dir"] == "ACCELERATING")
          or (r["quadrant"] == "IMPROVING" and r["vel_dir"] == "ACCELERATING" and r["velocity_1w"] >= VEL_HIGH)
          or (r["consistency"] >= CONSISTENT and r["quadrant"] in ("LEADING", "IMPROVING")))]
    taken |= set(ow)
    watch = [r["ticker"] for r in rs if r["ticker"] not in taken
             and r["quadrant"] == "IMPROVING" and r["rs_momentum"] and
             r["rs_momentum"] >= 110 and (r["rs_ratio"] or 100) < 100]

    L = []
    L.append(f"# RRG Analysis Report — {asof}")
    L.append("")
    L.append(f"- **Data as-of:** {asof}")
    L.append(f"- **Benchmark:** {bench} | **Lookback:** {lookback} weeks | **Universe:** {len(rs)} tickers")
    L.append(f"- **Source:** `{src.name}`")
    if data.get("analysis_query"):
        L.append(f"- **Query:** {data['analysis_query']}")
    rules = instr.get("interpretation_rules")
    if rules:
        L.append(f"- **Quadrant rules:** " + "; ".join(f"{k.replace('_quadrant','')}: {v}" for k, v in rules.items()))
    L.append("")

    L.append("## 1. Quadrant Census")
    L.append("")
    L.append(" | ".join(f"**{q}: {census.get(q, 0)}**" for q in
             ("LEADING", "IMPROVING", "WEAKENING", "LAGGING")))
    L.append("")

    L.append("## 2. Leadership Regime")
    L.append("")
    L.append(f"Consistent leaders (consistency ≥ {CONSISTENT:.0f}%), ranked by consistency then velocity:")
    L.append("")
    L += table(consistent[:TOP_N])
    L.append("")

    L.append("## 3. Improving Trends")
    L.append("")
    L.append(f"Top {TOP_N} IMPROVING tickers by 1-week velocity:")
    L.append("")
    L += table(improvers)
    L.append("")

    L.append("## 4. Weakening Momentum")
    L.append("")
    L.append("WEAKENING quadrant:")
    L.append("")
    L += table(weakening) if weakening else ["*(none)*"]
    L.append("")
    L.append(f"Decelerating LEADING tickers (distribution watch), top {TOP_N} by velocity:")
    L.append("")
    L += table(decel_leaders)
    L.append("")

    L.append("## 5. Velocity Analysis")
    L.append("")
    L.append(f"Fastest movers (top {TOP_N} by |velocity_1w|):")
    L.append("")
    L += table(accel, cols=("ticker", "quadrant", "velocity_1w", "vel_dir", "rs_momentum"))
    L.append("")
    if dead:
        L.append(f"Dead money (velocity < {VEL_DEAD}): " + ", ".join(
            f"`{r['ticker']}` {r['velocity_1w']:.2f}" for r in dead))
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
    L.append("- `angle_deg` convention is not defined in the prompt; do not infer rotation "
             "direction from it alone — use quadrant + velocity_direction.")
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
