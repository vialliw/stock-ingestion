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

The v4 fields `crossing_status` and `quadrant_persistence` are ALWAYS
derived by the extractor (they are not JSON fields): `crossing_status`
from the landed position vs the crossed boundary, `quadrant_persistence`
by walking back consecutive weeks in the dated file series.

## Standardized report sections

If the prompt's `instructions` do not specify a report structure, use
this fixed section order (the generator script emits exactly this):

1. Header — data as-of date, benchmark, lookback, universe size, source file
2. Quadrant Census — counts per quadrant + the 2x2 RRG grid (below)
3. Leadership Regime — consistent leaders (consistency ≥ 75)
4. Improving Trends — top IMPROVING by 1-week velocity
5. Weakening Momentum — WEAKENING quadrant + decelerating LEADING
6. **Quadrant Crossings** — standalone section (v4): every week-over-week
   quadrant transition grouped by type (counter-clockwise abrupt
   reversals, then clockwise normal rotation, then diagonals), each entry
   carrying crossing_status + deltas + landed-past-boundary +
   quadrant_persistence. Renders "unavailable" note when trajectory
   semantics cannot be obtained (legacy payload, no prior file).
7. Velocity Analysis — fastest movers, dead money (< 1.0)
8. Structural Laggards — top LAGGING by lag-weeks
9. Actionable Portfolio Tilts — overweight / hold-but-trim / reduce /
   avoid / watch (rule-generated draft buckets — review before saving)
10. Data Caveats — short-history tickers, consistency_score semantics,
    undefined angle_deg convention, crossing_status/persistence/FLAT doc

(The emitted `## N.` headers number these 1–9 after the title header
block: §1 Census … §5 Crossings, §6 Velocity, §7 Laggards, §8 Tilts,
§9 Caveats.)

If `instructions.focus_priority` exists, it governs narrative emphasis
inside those sections, not the section order.

### Quadrant Census — the 2x2 grid

§1 emits the bold counts line, then a traditional RRG grid:

```
| RS-Momentum ↓ / RS-Ratio → | **< 100** | **≥ 100** |
|---|---|---|
| **≥ 100** | IMPROVING cell | LEADING cell |
| **< 100** | LAGGING cell   | WEAKENING cell |
```

Each cell is `**QUADRANT (n)**` followed by `<br>• ThemeName:
`TKR`↙ …` bullets grouping members via THEME_MAP (Energy/commodity,
Metals/hard-asset, Healthcare, Defensive/rate-sensitive,
Value/dividend/consumer, Financials/REITs, Growth/mega-cap tech,
Clean energy, EM/Intl, Transports/infra/industrials; unmapped → "Other").
Tickers are alphabetical inside each bullet and each is suffixed with its
heading arrow. Every universe ticker appears exactly once.

### Heading arrows

Per-ticker tables carry `Heading` (and `Persist`) columns when
trajectory semantics are available. Heading is the compass direction of
the week-over-week move, from sign(`delta_rs_ratio`),
sign(`delta_rs_momentum`) — rendered as an arrow EVERYWHERE it appears
(table cells, census-grid suffixes, crossing entries):

| Arrow | Heading | Deltas | Meaning |
|-------|---------|--------|---------|
| ↗ | NE | ratio +, mom + | rotating up-right — **toward LEADING** |
| ↘ | SE | ratio +, mom − | ratio still rising but **momentum rolling over** — decelerating into WEAKENING risk |
| ↖ | NW | ratio −, mom + | **momentum-only improvement** — ratio still eroding, not yet a leader |
| ↙ | SW | ratio −, mom − | rotating down-left — **toward LAGGING**; never bullish |
| FLAT | FLAT | \|Δratio\| < 0.05 AND \|Δmom\| < 0.05 | no meaningful drift — checked BEFORE the arrow map (0/2,688 real ticker-weeks to date) |

Heading = travel direction in ratio/momentum space, NOT rotation
direction; heading, speed, quadrant transition, crossing_status, and
quadrant_persistence are independent fields — no auto-bullish/bearish
from heading alone (quadrant + distance-to-boundary required).

### Quadrant-crossing taxonomy (v4 — probabilistic labels)

`quadrant_change` is `UNCHANGED` or `PREV->CURR`. §5 groups transitions
counter-clockwise (abrupt reversals) → clockwise (normal rotation) →
diagonal (discontinuous); each group header carries a probabilistic
label:

| Crossing | Label |
|----------|-------|
| LEADING->IMPROVING   | potential regime break / RS breakdown warning |
| IMPROVING->LAGGING   | failed recovery / bearish continuation |
| LAGGING->WEAKENING   | possible squeeze / counter-trend rally — not a buy signal |
| WEAKENING->LEADING   | possible re-acceleration — verify follow-through |
| IMPROVING->LEADING   | possible rotation into leadership — confirm next week |
| LEADING->WEAKENING   | potential distribution / early reversal — watch |
| WEAKENING->LAGGING   | potential bearish continuation — confirm persistence |
| LAGGING->IMPROVING   | early recovery attempt — unconfirmed |
| diagonals (LEADING<->LAGGING, IMPROVING<->WEAKENING) | discontinuous regime change — investigate |

### Confirmation fields (v4)

- `crossing_status` (geometric, this week): `UNCHANGED` |
  `PROVISIONAL_CROSSING` | `CONFIRMED`. Evaluated on the actually-crossed
  axis — ratio=100 for LEADING<->IMPROVING & WEAKENING<->LAGGING,
  momentum=100 for LEADING<->WEAKENING & IMPROVING<->LAGGING, BOTH axes
  independently for diagonals (provisional if EITHER landed within the
  1.0-pt deadband of 100; CONFIRMED when every crossed axis landed ≥ 1.0
  pt beyond). Provisional entries carry "— whipsaw watch".
- `quadrant_persistence` (temporal): `TEMPORARY` (1 wk) | `PENDING`
  (2 wks) | `CONFIRMED` (≥3 wks) = consecutive weeks occupying the
  CURRENT quadrant, walked back over the dated file series. It measures
  **quadrant occupancy duration only — not the persistence, direction,
  or strength of the original crossing**. A ticker can sit CONFIRMED in
  a quadrant while deteriorating within it; read it alongside `heading`
  — a CONFIRMED-occupancy ticker with a counter-thesis heading is a
  stale-inhabitant flag, not a confirmed crossing. All new crossings
  start TEMPORARY; a PROVISIONAL crossing that holds into a second week
  upgrades persistence but keeps the whipsaw caveat in narrative.

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
narrative structure; there is no "compact" mode. One sentence per `>`
line:

- `> Weekly read: ...` blockquote in §1 placed BEFORE the 2x2 grid
  (after the bold counts line, before the `| RS-Momentum ↓ ...` table).
- `> Read: ...` blockquote at the END of each of §2–§7 (after the
  section's table/crossings block, before the next heading).
- `**Net call:** ...` bold line at the end of §8, after the bullet list.

Per-section content:

- **§1 Census** (`> Weekly read:`) — regime character (balanced vs
  lopsided, where leadership/laggards cluster); label the week in the
  arc (e.g. "inflection week", "post-squeeze digestion"); may span
  several one-sentence `>` lines.
- **§2 Leadership** — WHO the consistent leaders are as a group (sector/
  asset theme), streak count changes vs prior weeks, anomalies.
- **§3 Improving** — the dominant improving theme with named tickers +
  velocities; bench depth (count) and whether a successor regime exists.
- **§4 Weakening** — regime breaks (high-consistency names newly in
  WEAKENING get called out first), then the decelerating-leader cluster
  theme. "Trim into strength" style guidance when a whole complex
  decelerates.
- **§5 Quadrant Crossings** — the two flows (confirmed vs provisional),
  which transition groups matter (defensive rotation vs regime breaks
  vs squeezes), and the whipsaw-verdict framing for provisional names
  (they get their verdict on next week's print).
- **§6 Velocity** — what the fastest accelerators vs decelerators say as
  groups (velocity polarity = where capital is going vs leaving);
  counter-trend squeezes flagged as sells, not buys.
- **§7 Laggards** — the 2–4 thematic buckets inside the lag-list; these
  are the funding source for the leadership bid.
- **§8 Net call** — 2–3 sentences: the one rotation action, the core
  hold, and the trigger that would change the call.

Hard narrative rules (these exist because a real report misread a
LEADING->IMPROVING, ↙-heading ticker as "accelerating back toward
LEADING" — velocity_direction measures speed, not direction):

1. Never write "accelerating", "improving", or "rotating toward
   LEADING" about a ticker unless `delta_rs_ratio > 0`. Check the
   `Heading` column / `trajectory.heading` — only ↗ and ↖ headings have
   a positive-momentum component, and only ↗/↖/↘ move right on ratio.
2. A ↙-heading (SW) ticker — both deltas negative, moving toward
   LAGGING — must NOT appear in the §8 Overweight bucket regardless of
   quadrant label or consistency score. The generator enforces this when
   heading is available; on legacy reports produced before the fields
   existed, check the crossings section / prior week manually.

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
  extreme-quadrant history (flagged in §9).
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
