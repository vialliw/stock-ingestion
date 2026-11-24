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
    python .devin/skills/analyze_rra/analyze_rra.py --html <report.md> --html-out <report.html>
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
DEADBAND = 1.0          # pts past the crossed boundary for CONFIRMED crossing
FLAT_TOL = 0.05         # |delta| below this on BOTH axes -> FLAT heading

# Heading compass -> arrow glyph, applied everywhere headings render.
# FLAT renders as the literal word (no glyph).
ARROW = {"NE": "↗", "SE": "↘", "SW": "↙", "NW": "↖", "FLAT": "FLAT"}

# Census-grid theme buckets for the 2x2 RRG grid (96-ticker ETF universe).
# Order is the display order inside each cell; unmapped tickers fall back
# to "Other". Tickers are emitted alphabetically within each theme bullet.
THEME_MAP = [
    ("Energy/commodity",             "USO COMT DBC BCD FTGC IEO XOP VDE FCG XLE CANE NANR SOYB CORN WEAT XES UNG".split()),
    ("Metals/hard-asset",            "GDX SLV GLD PLTM PALL XME COPX SLX CPER DBB REMX LIT BATT SMOG".split()),
    ("Healthcare",                   "XLV VHT IBB IHI XBI XPH".split()),
    ("Defensive/rate-sensitive",     "TLT XLU XLP HYG JNK SGOV ITB".split()),
    ("Value/dividend/consumer",      "VTV VOE IWD VBR XRT XLY XLB".split()),
    ("Financials/REITs",             "XLF IYF KRE IAI SCHH IYR".split()),
    ("Growth/mega-cap tech",         "QQQ SMH XLK XSW CIBR CLOU ARKK IWF AWAY VBK VOT XLC HERO DIA IWM".split()),
    ("Clean energy",                 "QCLN TAN PBW ICLN".split()),
    ("EM/Intl",                      "EEM EWY EWJ EWA EWZ FXI INDA VNM EWC EFA VXUS VGK".split()),
    ("Transports/infra/industrials", "IYT JETS XTN XLI XAR GRID IFRA".split()),
]
TK2THEME = {t: name for name, tks in THEME_MAP for t in tks}

# Which boundary (axis = 100) each adjacency crosses. Diagonals cross both.
_RATIO_CROSSED = {frozenset(p) for p in (("LEADING", "IMPROVING"),
                                         ("WEAKENING", "LAGGING"))}
_MOM_CROSSED = {frozenset(p) for p in (("LEADING", "WEAKENING"),
                                       ("IMPROVING", "LAGGING"))}
_DIAGONAL = {frozenset(p) for p in (("LEADING", "LAGGING"),
                                    ("IMPROVING", "WEAKENING"))}

# Probabilistic crossing labels (v4 taxonomy — see SKILL.md). Diagonals and
# any unlisted transition get the discontinuous default.
CROSSING_LABEL = {
    "LEADING->IMPROVING": "potential regime break / RS breakdown warning",
    "IMPROVING->LAGGING": "failed recovery / bearish continuation",
    "LAGGING->WEAKENING": "possible squeeze / counter-trend rally — not a buy signal",
    "WEAKENING->LEADING": "possible re-acceleration — verify follow-through",
    "IMPROVING->LEADING": "possible rotation into leadership — confirm next week",
    "LEADING->WEAKENING": "potential distribution / early reversal — watch",
    "WEAKENING->LAGGING": "potential bearish continuation — confirm persistence",
    "LAGGING->IMPROVING": "early recovery attempt — unconfirmed",
}
CROSSING_DEFAULT = "discontinuous regime change — investigate"

# Presentation order inside ## 5. Quadrant Crossings.
CCW_ORDER = ["LEADING->IMPROVING", "IMPROVING->LAGGING",
             "LAGGING->WEAKENING", "WEAKENING->LEADING"]
CW_ORDER = ["IMPROVING->LEADING", "LEADING->WEAKENING",
            "WEAKENING->LAGGING", "LAGGING->IMPROVING"]
DIAG_ORDER = ["LEADING->LAGGING", "LAGGING->LEADING",
              "IMPROVING->WEAKENING", "WEAKENING->IMPROVING"]

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
    """Compass heading from week-over-week deltas. FLAT is checked FIRST
    (|delta| < FLAT_TOL on both axes); then a zero axis counts as positive
    (same >=0 convention as gen_ai_prompt)."""
    if abs(dr) < FLAT_TOL and abs(dm) < FLAT_TOL:
        return "FLAT"
    if dr >= 0 and dm >= 0:
        return "NE"
    if dr >= 0:
        return "SE"
    if dm >= 0:
        return "NW"
    return "SW"


def _arrow(h):
    """Display glyph for a heading value (FLAT renders as the word)."""
    return ARROW.get(h or "", "—")


def _crossed_axes(quadrant_change):
    """The boundary(ies) a PREV->CURR transition crossed: 'ratio' for
    LEADING<->IMPROVING / WEAKENING<->LAGGING, 'momentum' for
    LEADING<->WEAKENING / IMPROVING<->LAGGING, both for diagonals.
    Unlisted pairs are evaluated on both axes (conservative)."""
    prev, _, cur = quadrant_change.partition("->")
    pair = frozenset((prev, cur))
    if pair in _RATIO_CROSSED:
        return ("ratio",)
    if pair in _MOM_CROSSED:
        return ("momentum",)
    return ("ratio", "momentum")


def _crossing_status(r):
    """UNCHANGED when no transition; CONFIRMED when every crossed axis
    landed >= DEADBAND past 100, else PROVISIONAL_CROSSING."""
    qc = r.get("quadrant_change")
    if not qc or qc == "UNCHANGED" or "->" not in qc:
        return "UNCHANGED"
    for ax in _crossed_axes(qc):
        v = r["rs_ratio"] if ax == "ratio" else r["rs_momentum"]
        if v is None or abs(v - 100) < DEADBAND:
            return "PROVISIONAL_CROSSING"
    return "CONFIRMED"


def prior_quadrant_series(path: Path) -> list[dict]:
    """{ticker: quadrant} maps of every dated prompt file in the same dir
    strictly older than `path`, newest first — for the
    quadrant_persistence walk-back. Never raises."""
    m = DATE_RE.search(path.name)
    if not m:
        return []
    cur = m.group(1)
    series = []
    for p in sorted(path.parent.glob("*_analysis_prompt*.json")):
        if p.resolve() == path.resolve():
            continue
        mm = DATE_RE.search(p.name)
        if not mm or mm.group(1) >= cur:
            continue
        try:
            snap = json.loads(p.read_text(encoding="utf-8")).get("market_snapshot") or {}
        except (OSError, ValueError):
            continue
        series.append((mm.group(1), {
            tk: (e.get("current_position") or {}).get("quadrant", "").upper()
            for tk, e in (snap.get("ticker_data") or {}).items()}))
    series.sort(key=lambda t: t[0], reverse=True)
    return [qmap for _, qmap in series]


def _persistence(r, series):
    """TEMPORARY (1wk) / PENDING (2wk) / CONFIRMED (>=3wk) — consecutive
    weeks occupying the CURRENT quadrant, walking back prior files."""
    n = 1
    for qmap in series:
        if qmap.get(r["ticker"]) != r["quadrant"]:
            break
        n += 1
    return "TEMPORARY" if n == 1 else "PENDING" if n == 2 else "CONFIRMED"


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
        # FLAT is a v4 heading state the native JSON field predates: when
        # deltas are within tolerance on both axes it overrides whatever
        # heading the payload (or derivation) produced.
        if heading is not None and dr is not None and dm is not None \
                and abs(dr) < FLAT_TOL and abs(dm) < FLAT_TOL:
            heading = "FLAT"
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


def fmt(v, nd=1, sign=False):
    if v is None:
        return "n/a"
    return f"{v:+.{nd}f}" if sign else f"{v:.{nd}f}"


def table(rs, cols=("ticker", "quadrant", "rs_ratio", "rs_momentum",
                    "velocity_1w", "vel_dir", "lead_w", "lag_w", "consistency")):
    hdr = {"ticker": "Ticker", "quadrant": "Quadrant", "rs_ratio": "RS-Ratio",
           "rs_momentum": "RS-Mom", "velocity_1w": "Vel 1w", "vel_dir": "Dir",
           "heading": "Heading", "persist": "Persist", "lead_w": "Lead wks",
           "lag_w": "Lag wks", "consistency": "Consist %"}
    lines = ["| " + " | ".join(hdr[c] for c in cols) + " |",
             "|" + "---|" * len(cols)]
    for r in rs:
        lines.append("| " + " | ".join(
            fmt(r[c], 2) if c in ("rs_ratio", "rs_momentum") else
            fmt(r[c]) if c in ("velocity_1w", "consistency") else
            _arrow(r[c]) if c == "heading" else
            str(r[c] or "—") if c == "persist" else
            str(r[c]) for c in cols) + " |")
    return lines


def _census_cell(quadrant, rs):
    """One 2x2-grid cell: '**QUADRANT (n)**' followed by '<br>• Theme:
    `TKR`↙ ...' bullets grouping members via THEME_MAP (alphabetical
    inside each bullet; unmapped tickers fall back to 'Other')."""
    by_t = {r["ticker"]: r for r in rs if r["quadrant"] == quadrant}
    cell = f"**{quadrant} ({len(by_t)})**"
    seen = set()
    for theme, tks in THEME_MAP:
        here = sorted(set(tks) & by_t.keys())
        if not here:
            continue
        seen.update(here)
        cell += (f"<br>• {theme}: " + " ".join(
            f"`{t}`{ARROW.get(by_t[t]['heading'] or '', '')}" for t in here))
    rest = sorted(set(by_t) - seen)
    if rest:
        cell += ("<br>• Other: " + " ".join(
            f"`{t}`{ARROW.get(by_t[t]['heading'] or '', '')}" for t in rest))
    return cell


def _crossing_line(r):
    """One §5 entry: '- `TKR` STATUS — ↙, Δratio -6.89, Δmom -2.07
    (landed ratio 97.59, 2.41 past boundary); quadrant_persistence X'.
    Provisional entries get 'only' + '— whipsaw watch'."""
    st = r["crossing_status"]
    prov = st == "PROVISIONAL_CROSSING"
    axes = _crossed_axes(r["quadrant_change"])
    if axes == ("ratio",):
        dist = abs((r["rs_ratio"] or 100) - 100)
        land = f"landed ratio {fmt(r['rs_ratio'], 2)}"
    elif axes == ("momentum",):
        dist = abs((r["rs_momentum"] or 100) - 100)
        land = f"landed mom {fmt(r['rs_momentum'], 2)}"
    else:  # diagonal — both axes evaluated independently
        dr_d = abs((r["rs_ratio"] or 100) - 100)
        dm_d = abs((r["rs_momentum"] or 100) - 100)
        land = (f"landed ratio {fmt(r['rs_ratio'], 2)}, "
                f"mom {fmt(r['rs_momentum'], 2)}")
        dist = f"{dr_d:.2f}/{dm_d:.2f}"
    if prov:
        land += f", only {dist if isinstance(dist, str) else f'{dist:.2f}'} past boundary — whipsaw watch"
    else:
        land += f", {dist if isinstance(dist, str) else f'{dist:.2f}'} past boundary"
    return (f"- `{r['ticker']}` {st} — {_arrow(r['heading'])}, "
            f"Δratio {fmt(r['delta_rs_ratio'], 2, sign=True)}, "
            f"Δmom {fmt(r['delta_rs_momentum'], 2, sign=True)} "
            f"({land}); quadrant_persistence {r.get('persist') or '—'}")


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
    # quadrant_persistence (TEMPORARY/PENDING/CONFIRMED occupancy) + the
    # geometric crossing_status are derived per ticker from the dated
    # file series — independent of where the heading fields came from.
    series = prior_quadrant_series(src)
    # justified: per-row field assignment needs the ticker index into the
    # walk-back series; not expressible as a transformation of rs alone.
    for r in rs:
        r["persist"] = _persistence(r, series)
        r["crossing_status"] = _crossing_status(r)
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
    # Traditional 2x2 RRG grid — RS-Momentum rows (>=100 top) x RS-Ratio
    # columns (>=100 right). Each cell: '**QUADRANT (n)**' + per-theme
    # bullets; every ticker carries its heading arrow.
    L.append("| RS-Momentum ↓ / RS-Ratio → | **< 100** | **≥ 100** |")
    L.append("|---|---|---|")
    L.append("| **≥ 100** | " + _census_cell("IMPROVING", rs) + " | "
             + _census_cell("LEADING", rs) + " |")
    L.append("| **< 100** | " + _census_cell("LAGGING", rs) + " | "
             + _census_cell("WEAKENING", rs) + " |")
    L.append("")

    # Heading + Persist columns appear in the per-ticker tables whenever
    # trajectory semantics are available (native fields or prior-file diff).
    main_cols = ("ticker", "quadrant", "rs_ratio", "rs_momentum",
                 "velocity_1w", "vel_dir", "heading", "persist", "lead_w",
                 "lag_w", "consistency") if has_heading else (
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

    # ## 5. Quadrant Crossings — standalone section (v4). Every ticker
    # whose week-over-week quadrant label changed, grouped by transition
    # type: counter-clockwise (abrupt reversals) then clockwise (normal
    # rotation) then diagonals (discontinuous), each under a bold
    # '**A->B** — probabilistic label' header.
    crossings = sorted(
        (r for r in rs
         if r["quadrant_change"] and r["quadrant_change"] != "UNCHANGED"),
        key=lambda r: r["ticker"])
    L.append("## 5. Quadrant Crossings")
    L.append("")
    if not has_heading:
        L.append("*Trajectory semantics unavailable (legacy payload, no prior "
                 "dated prompt file) — crossing analysis omitted.*")
        L.append("")
    elif not crossings:
        L.append("No quadrant crossings this week.")
        L.append("")
    else:
        conf = sum(1 for r in crossings if r["crossing_status"] == "CONFIRMED")
        prov = len(crossings) - conf
        by_type = Counter(r["quadrant_change"] for r in crossings)
        types_txt = ", ".join(f"{n}x {t}" for t, n in
                              sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0])))
        diag_n = sum(n for t, n in by_type.items()
                     if frozenset(t.split("->")) in _DIAGONAL)
        diag_txt = (f"{diag_n} diagonal/discontinuous transition"
                    + ("s." if diag_n != 1 else ".")
                    if diag_n else "No diagonal/discontinuous transitions.")
        L.append(f"{len(crossings)} crossings this week: **{conf} confirmed / "
                 f"{prov} provisional** (geometric deadband {DEADBAND:.1f} pt past "
                 f"the crossed boundary). By type: {types_txt}. {diag_txt}")
        L.append("")
        known = set(CCW_ORDER) | set(CW_ORDER) | set(DIAG_ORDER)
        groups = (("### Counter-clockwise — abrupt reversals",
                   CCW_ORDER + sorted(set(by_type) - known)),
                  ("### Clockwise — normal rotation", CW_ORDER),
                  ("### Diagonal — discontinuous", DIAG_ORDER))
        for title, order in groups:
            present = [t for t in order if by_type.get(t)]
            if not present:
                continue
            L.append(title)
            L.append("")
            for t in present:
                L.append(f"**{t}** — "
                         + CROSSING_LABEL.get(t, CROSSING_DEFAULT))
                L.append("")
                # confirmed first, then provisional; velocity desc inside each
                for r in sorted((r for r in crossings if r["quadrant_change"] == t),
                                key=lambda r: (r["crossing_status"] != "CONFIRMED",
                                               -r["velocity_1w"])):
                    L.append(_crossing_line(r))
                L.append("")

    L.append("## 6. Velocity Analysis")
    L.append("")
    L.append(f"Fastest movers (top {TOP_N} by |velocity_1w|):")
    L.append("")
    L += table(accel, cols=("ticker", "quadrant", "velocity_1w", "vel_dir",
                            "heading", "persist", "rs_momentum")
               if has_heading else
               ("ticker", "quadrant", "velocity_1w", "vel_dir", "rs_momentum"))
    L.append("")
    if dead:
        L.append(f"Dead money (velocity < {VEL_DEAD}): " + ", ".join(
            f"`{r['ticker']}` {r['velocity_1w']:.2f}" for r in dead))
        L.append("")

    L.append("## 7. Structural Laggards")
    L.append("")
    L.append(f"Top {TOP_N} LAGGING by lag-weeks:")
    L.append("")
    L += table(laggards, cols=("ticker", "rs_ratio", "rs_momentum", "velocity_1w", "lag_w", "consistency"))
    L.append("")

    L.append("## 8. Actionable Portfolio Tilts")
    L.append("")
    L.append(f"- **Overweight** ({len(ow)}): " + ", ".join(f"`{t}`" for t in sorted(ow)))
    L.append(f"- **Hold-but-trim** — decelerating leaders ({len(trim)}): " + ", ".join(f"`{t}`" for t in sorted(trim)))
    L.append(f"- **Reduce** — weakening ({len(reduce_)}): " + (", ".join(f"`{t}`" for t in sorted(reduce_)) or "none"))
    L.append(f"- **Avoid** — structural laggards ({len(avoid)}): " + ", ".join(f"`{t}`" for t in sorted(avoid)))
    L.append(f"- **Watch** — improving, RS-mom ≥ 110 but RS-ratio < 100 ({len(watch)}): " +
             (", ".join(f"`{t}`" for t in sorted(watch)) or "none"))
    L.append("")

    L.append("## 9. Data Caveats")
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
    if has_heading:
        flat_n = sum(1 for r in rs if r["heading"] == "FLAT")
        L.append("- `crossing_status` = geometric deadband on the actually-crossed axis "
                 "(ratio for L<->I & W<->L; momentum for L<->W & I<->L; diagonals both) "
                 "— PROVISIONAL_CROSSING within 1.0 pt of 100, CONFIRMED beyond; "
                 "whipsaw-verdict needs a second week. `quadrant_persistence` = "
                 "consecutive weeks occupying the current quadrant "
                 "(TEMPORARY/PENDING/CONFIRMED) — occupancy duration only, NOT crossing "
                 "validity; `heading` FLAT reserved for |Δ|<0.05 on both axes "
                 f"({flat_n} occurrences this week).")
    L.append("- Tilt buckets are rule-generated; review before acting.")
    L.append("")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", type=Path, nargs="?",
                    help="analysis-prompt JSON file (not needed with --html)")
    ap.add_argument("--out", type=Path, help="write report here (default: stdout)")
    ap.add_argument("--stats-only", action="store_true",
                    help="print quadrant census + counts only, no report")
    ap.add_argument("--html", type=Path, metavar="REPORT_MD",
                    help="render an existing report .md to styled HTML "
                         "via rra_html.py and exit — no JSON needed")
    ap.add_argument("--html-out", type=Path, metavar="REPORT_HTML",
                    help="write the --html output here (default: stdout)")
    args = ap.parse_args()

    if args.html:
        # Render-only path: report .md -> publish .html, no re-extraction.
        # rra_html needs mistune; import lazily so extraction stays stdlib-only.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import rra_html  # noqa: E402
        html = rra_html.render(
            args.html.read_text(encoding="utf-8"),
            rra_html.title_from_filename(args.html.name))
        if args.html_out:
            args.html_out.parent.mkdir(parents=True, exist_ok=True)
            args.html_out.write_text(html, encoding="utf-8")
            print(f"wrote {args.html_out} ({len(html)} bytes)",
                  file=sys.stderr)
        else:
            sys.stdout.write(html)
        return

    if args.input is None:
        ap.error("input JSON file required (or use --html <report.md>)")

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
