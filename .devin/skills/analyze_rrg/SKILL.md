# analyze_rrg

Analyze a Relative Rotation Graph (RRG) analysis-prompt JSON file and
produce a standardized markdown report saved under `docs/rrg/`. Use when
the user asks to analyze an RRG/relative-rotation JSON (e.g.
`docs/rrg/*_analysis_prompt*.json`), or a new dated RRG prompt file
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
  angle_deg}, trajectory{velocity_1w, velocity_direction},
  persistence{lead_weeks_3m, lag_weeks_3m, consistency_score}}}`.

## Standardized report sections

If the prompt's `instructions` do not specify a report structure, use
this fixed section order (the generator script emits exactly this):

1. Header — data as-of date, benchmark, lookback, universe size, source file
2. Quadrant Census — counts per quadrant
3. Leadership Regime — consistent leaders (consistency ≥ 75)
4. Improving Trends — top IMPROVING by 1-week velocity
5. Weakening Momentum — WEAKENING quadrant + decelerating LEADING
6. Velocity Analysis — fastest movers, dead money (< 1.0)
7. Structural Laggards — top LAGGING by lag-weeks
8. Actionable Portfolio Tilts — overweight / hold-but-trim / reduce /
   avoid / watch (rule-generated draft buckets — review before saving)
9. Data Caveats — short-history tickers, consistency_score semantics,
   undefined angle_deg convention

If `instructions.focus_priority` exists, it governs narrative emphasis
inside those sections, not the section order.

## Procedure

1. **Generate the draft** — run the bundled extractor (stdlib only):
   ```
   python .devin/skills/analyze_rrg/analyze_rrg.py <input.json> --out docs/rrg/<report>.md
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
3. **Save** — the report lands in `docs/rrg/`; do not modify the input
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

For batch/backfill runs: extract a per-week digest first (census,
12wk-leaders, top accelerating improvers, top decelerating leaders,
WEAKENING list, top laggards by lag_weeks, fastest movers, dead money) —
`analyze_rrg.py` already prints most of this; a small `json.load` loop
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
  can accelerate while deepening into LAGGING.
- `angle_deg` sign convention is undefined in these prompts; never
  interpret rotation direction from angle alone.
- Pure analysis task — no `src/`/`tests/` changes; orchestrator may run
  it directly (no B/C/D cycle needed).
