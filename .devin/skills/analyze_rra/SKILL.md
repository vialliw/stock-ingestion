# analyze_rra

Analyze a Relative Rotation Graph (RRA) analysis-prompt JSON file and
produce a standardized markdown report saved under `docs/rra/`. Use when
the user asks to analyze an RRA/relative-rotation JSON (e.g.
`docs/rra/*_analysis_prompt*.json`), or a new dated RRA prompt file
appears and a report is needed.

Repo: `B:\projects\stock-ingestion`

## Input contract

The JSON is a self-contained LLM prompt:

- `system_prompt` — analyst persona to adopt for the narrative.
- `analysis_query` — what to identify (leaders, improvers, decay, tilts).
- `instructions.focus_priority` — ordered themes; `interpretation_rules` —
  quadrant meanings; `response_style` — tone.
- `market_snapshot` — `analysis_date`, `benchmark`, `lookback_weeks`,
  `ticker_data{ticker: {current_position{rs_ratio, rs_momentum, quadrant,
  angle_deg}, trajectory{velocity_1w, velocity_direction,
  prev_rs_ratio, prev_rs_momentum, delta_rs_ratio, delta_rs_momentum,
  heading, prev_quadrant, quadrant_change},
  persistence{lead_weeks_3m, lag_weeks_3m, consistency_score}}}`.

The trajectory-semantics keys (`prev_*`, `delta_*`, `heading`,
`prev_quadrant`, `quadrant_change`) were added by RRG-TRAJ-T1 — payloads
generated before that lack them. The extractor then derives them by
diffing the prior `*YYYY-MM-DD_analysis_prompt.json` in the same
directory, or omits the columns/block when none exists.

## Standardized report sections

If the prompt's `instructions` do not specify a report structure, use
this fixed section order (the generator script emits exactly this):

1. Header — data as-of date, benchmark, lookback, universe size, source file
2. Quadrant Census — counts per quadrant
3. Leadership Regime — consistent leaders (consistency ≥ 75)
4. Improving Trends — top IMPROVING by 1-week velocity
5. Weakening Momentum — WEAKENING quadrant + decelerating LEADING
6. Velocity Analysis — fastest movers, dead money (< 1.0), plus a
   `### Quadrant Crossings` sub-block listing every week-over-week
   quadrant transition with a bias label (omitted when trajectory
   semantics are unavailable)
7. Structural Laggards — top LAGGING by lag-weeks
8. Actionable Portfolio Tilts — overweight / hold-but-trim / reduce /
   avoid / watch (rule-generated draft buckets — review before saving)
9. Data Caveats — short-history tickers, consistency_score semantics,
   undefined angle_deg convention

If `instructions.focus_priority` exists, it governs narrative emphasis
inside those sections, not the section order.

Per-ticker tables carry a `Hdg` column when trajectory semantics are
available. Heading is the compass direction of the week-over-week move,
from sign(`delta_rs_ratio`), sign(`delta_rs_momentum`) (a zero axis
counts as positive — deterministic convention):

| Hdg | Deltas | Meaning |
|-----|--------|---------|
| NE  | ratio +, mom +   | rotating up-right — **toward LEADING** |
| SE  | ratio +, mom −   | ratio still rising but **momentum rolling over** — decelerating into WEAKENING risk |
| NW  | ratio −, mom +   | **momentum-only improvement** — ratio still eroding, not yet a leader |
| SW  | ratio −, mom −   | rotating down-left — **toward LAGGING**; never bullish |

### Quadrant-crossing taxonomy

`quadrant_change` is `UNCHANGED` or `PREV->CURR`. Bias labels emitted in
the §5 crossings block:

| Crossing | Bias |
|----------|------|
| LEADING->IMPROVING   | regime break — bearish shock |
| WEAKENING->LAGGING   | breakdown confirmed — bearish |
| IMPROVING->LAGGING   | failed improvement — bearish |
| LEADING->WEAKENING   | normal aging — trim into strength |
| IMPROVING->LEADING   | rotation confirmed — bullish |
| LAGGING->IMPROVING   | bottoming — early bullish |
| LAGGING->WEAKENING   | hard right break — squeeze, don't chase |
| WEAKENING->LEADING   | re-acceleration — bullish |
| diagonal jumps (IMPROVING->WEAKENING, LAGGING->LEADING, …) | discontinuous — treat with suspicion |

## Procedure

1. **Generate the draft** — run the bundled extractor (stdlib only):
   ```
   python .devin/skills/analyze_rra/analyze_rra.py <input.json> --out docs/rra/<report>.md
   ```
   Report filename: same stem as the input with `prompt`→`report`
   (e.g. `..._analysis_prompt20261113.json` →
   `..._analysis_report20261113.md`). Quick sanity check first with
   `--stats-only`.
2. **Review + enrich** — read the draft; fix anything the rules
   misbucketed (e.g. a high-consistency LEADER newly in WEAKENING belongs
   in §4 commentary), then add the analytical narrative per
   `response_style`. Keep tables verbatim. Enrichment is REQUIRED, not
   optional — see the contract below.
3. **Save** — the report lands in `docs/rra/`; do not modify the input
   JSON. Log the run to `devin_blackboard/EVENTS.log` per AGENTS.md.

## Enrichment contract (REQUIRED)

Every report — single runs and batch backfills alike — gets the same
narrative structure; there is no "compact" mode:

- `> Read: ...` blockquote at the END of each of §1–§6 (after the
  section's table, before the next heading).
- `**Net call:** ...` bold line at the end of §7, after the bullet list.

Per-section content:

- **§1 Census** — regime character in one line (balanced vs lopsided,
  where leadership/laggards cluster). In a dated series, label the week
  in the arc (e.g. "inflection week", "post-squeeze digestion") and use
  `> Weekly read:` instead of `> Read:`.
- **§2 Leadership** — WHO the consistent leaders are as a group (sector/
  asset theme), streak count changes vs prior weeks, anomalies.
- **§3 Improving** — the dominant improving theme with named tickers +
  velocities; bench depth (count) and whether a successor regime exists.
- **§4 Weakening** — regime breaks (high-consistency names newly in
  WEAKENING get called out first), then the decelerating-leader cluster
  theme. "Trim into strength" style guidance when a whole complex
  decelerates.
- **§5 Velocity** — what the fastest accelerators vs decelerators say as
  groups (velocity polarity = where capital is going vs leaving);
  counter-trend squeezes flagged as sells, not buys.
- **§6 Laggards** — the 2–4 thematic buckets inside the lag-list; these
  are the funding source for the leadership bid.
- **§7 Net call** — 2–3 sentences: the one rotation action, the core
  hold, and the trigger that would change the call.

Hard narrative rules (these exist because a real report misread a
LEADING->IMPROVING, SW-heading ticker as "accelerating back toward
LEADING" — velocity_direction measures speed, not direction):

1. Never write "accelerating", "improving", or "rotating toward
   LEADING" about a ticker unless `delta_rs_ratio > 0`. Check the `Hdg`
   column / `trajectory.heading` — only NE and NW headings have a
   positive-momentum component, and only NE/NW/SE move right on ratio.
2. An SW-heading ticker (both deltas negative — moving toward LAGGING)
   must NOT appear in the §7 Overweight bucket regardless of quadrant
   label or consistency score. The generator enforces this when heading
   is available; on legacy reports produced before the fields existed,
   check the crossings block / prior week manually.

For batch/backfill runs: extract a per-week digest first (census,
12wk-leaders, top accelerating improvers, top decelerating leaders,
WEAKENING list, top laggards by lag_weeks, fastest movers, dead money) —
`analyze_rra.py` already prints most of this; a small `json.load` loop
over the prompt files fills the rest — then write every report's reads
from that digest plus cross-week context (streak gains/losses, quadrant
flips of named tickers, prior-week velocity comparisons).

## Rules / gotchas

- `consistency_score` = `lead_weeks_3m / lookback_weeks` — share of the
  window spent LEADING. `lead + lag < lookback` is normal (IMPROVING /
  WEAKENING weeks are uncounted); only `lead + lag ≤ 2` means minimal
  extreme-quadrant history (flagged in §8).
- `velocity_direction` ACCELERATING/DECELERATING is change in
  `velocity_1w` magnitude, NOT direction of quadrant travel — a ticker
  can accelerate while deepening into LAGGING. Use `heading` /
  `delta_rs_ratio` / `delta_rs_momentum` (or the prior-file diff on
  legacy payloads) for direction; `velocity_direction` alone must never
  justify a "moving toward X" claim.
- `angle_deg` sign convention is undefined in these prompts; never
  interpret rotation direction from angle alone.
- Pure analysis task — no `src/`/`tests/` changes; orchestrator may run
  it directly (no B/C/D cycle needed).
