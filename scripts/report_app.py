"""Interactive Streamlit report app with five tabs — the three weekly
reports first, then the two daily ones:

* **"Weekly — Combined Regimes"**: the combined weekly report from
  ``docs/report_idea/report_weekly_trends.md`` — all four non-Normal
  volume regimes among the top-K most-liquid names, ordered by regime
  priority (Strong Acceleration, Acceleration, Strong Deceleration,
  Deceleration) then ``rvol`` descending.  The source md is internally
  inconsistent — its SQL ranks Deceleration above Strong Deceleration
  while its own interpretation guide's recommended reading order puts
  Strong Deceleration third; the guide wins (PM-approved deviation).
* **"Weekly — Acceleration"**: the weekly acceleration candidates from
  ``docs/report_idea/report_weekly_trend_accelerating.md`` — only the
  two acceleration regimes, ordered by ``rvol`` descending.
* **"Weekly — Deceleration"**: the weekly deceleration candidates from
  ``docs/report_idea/report_weekly_trend_decelerating.md`` — only the
  two deceleration regimes, ordered by ``rvol`` ascending (quietest
  names first).
* **"Daily — Top 25 by Dollar Volume"**: the top-N tickers ranked by
  latest (most recent trading day) dollar volume, enriched with the
  RVOL / regime edge metrics (latest dollar volume, relative volume,
  pct change, volume regime classification, share of total average
  dollar volume).  The query is the one inside
  ``docs/report_idea/report_Acceleration Candidates (RVOL significantly
  above norm).md`` — note the two daily md filenames are *swapped* vs
  their contents; tabs are labelled by intent, not filename.
* **"Daily — Acceleration Candidates"**: filters ``rvol >= threshold``
  among the top-K most-liquid names by latest dollar volume, ordered
  by ``rvol`` descending.  The query is the one inside
  ``docs/report_idea/report_Top 25 by Approximate Dollar Volume.md``.

The weekly tabs aggregate ``daily_close`` into weekly bars
(``DATE_TRUNC('week', Date)``, first-Open/last-Close via
``ARRAY_AGG``, ``SUM(Volume)``, ``SUM(trade_money)``, and a
``LAG``-based weekly pct change) and average over the last N *weeks*
per ticker.  Note the weekly deceleration defaults differ from the
daily ones: ``strong_decel=0.50`` / ``decel=0.70`` (the daily tabs use
0.6 / 0.8).

Guides: a top-level "About / How to read these reports" expander under
the app title renders the generic sections of
``docs/report_idea/report_basic.md`` (``BASIC_GUIDE`` — verbatim
except for 3 permanently-stale session-dated lines dropped per PM
approval in F7-T27; the session-specific data tables / named
candidates / "Key Insights" are excluded), and every tab renders its
source md's interpretation guide verbatim in an "Interpretation
guide" expander (``WEEKLY_*_GUIDE`` for the weekly tabs,
``DAILY_*_GUIDE`` for the daily ones).

Every tab's bar chart is drawn through
:func:`_render_ordered_bar_chart` — an Altair bar chart with
``sort=None`` on the nominal ``Ticker`` axis (falling back to
``st.vega_lite_chart`` with an equivalent spec if Altair is ever
unavailable) — so the bars follow the dataframe's row order, i.e. the
query's ``ORDER BY`` (the table's default order), instead of
``st.bar_chart``'s alphabetical category sort.

All interactive widget values are numeric (or drawn from the fixed
``VOLUME_REGIMES`` whitelist), so they are safely formatted into the
SQL text — no quoting/escaping concerns.

Data access mirrors ``scripts/report_top25_active.py``: the running
Arrow Flight service is tried first (``do_get`` with a JSON
``{"sql": ...}`` ticket on
``grpc://{server.flight_host}:{server.flight_port}``, a 5-second
``wait_for_available`` probe, and a 120-second ``FlightCallOptions``
call bound), and any Flight-path failure falls back to a direct
*read-only* DuckDB connection.  When both fail the tab shows
``st.error``.

Two hardening behaviours on top of that:

* **Caching.**  The UI path fetches through an ``st.cache_data``
  wrapper keyed on ``(sql, flight_location, db_path)`` with a TTL of
  ``_CACHE_TTL_S`` seconds, so a widget rerun that leaves the SQL text
  unchanged does not re-query the Flight server / DuckDB.  Each tab
  also has a "Refresh data" button that clears the cache explicitly.
* **Inverted-threshold warnings.**  The generated ``CASE`` checks the
  *strong* RVOL bounds first, so thresholds only behave as labelled
  when ``strong_accel >= accel`` and ``strong_decel <= decel``.
  ``find_threshold_inversions`` reports each misordering and the UI
  surfaces it as a non-blocking ``st.warning`` on both tabs (the tab
  still renders — warn, don't refuse).

The module is importable and unit-testable *without* the Streamlit
runtime: the SQL builder functions (:func:`build_top25_sql`,
:func:`build_accel_sql`, :func:`build_weekly_combined_sql`,
:func:`build_weekly_accel_sql`, :func:`build_weekly_decel_sql`), the
threshold helper
(:func:`find_threshold_inversions`), the data fetch
(:func:`fetch_dataframe`), the xlsx serializer
(:func:`dataframe_to_xlsx_bytes`) and the bar-chart spec builders
(:func:`_ordered_bar_spec`, :func:`_build_ordered_bar_chart`) are plain
functions; ``streamlit`` (and ``altair``) is only imported inside the
UI code paths — the
``st.cache_data`` wrapper is likewise built lazily via
``_build_cached_fetch(st)`` / ``_get_cached_fetch(st)`` so tests can
exercise the cache-key derivation with a fake ``st``.

Launch with:

    streamlit run scripts/report_app.py
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from typing import List, Optional, Sequence

# Ensure src/ is importable regardless of the caller's working
# directory (mirrors scripts/daily_load_once.py /
# scripts/report_top25_active.py).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")

import duckdb
import pandas as pd
import pyarrow.flight as flight

from src.config import PipelineConfig

logger = logging.getLogger(__name__)

# Short probe answering "is the Flight service up?" — kept small so a
# stopped service does not stall the app before the direct read-only
# fallback takes over (mirrors report_top25_active).
_FLIGHT_CONNECT_TIMEOUT_S = 5

# Bound on the do_get call itself so a wedged-but-connected server
# cannot hang the app forever.  120 s mirrors the server's own
# query_timeout_seconds default (src/config.py ServerConfig).
_FLIGHT_GET_TIMEOUT_S = 120

# Padding added to each auto-sized xlsx column so content is not flush
# against the cell edge in Excel.
_WIDTH_PADDING = 2

# TTL for the st.cache_data wrapper: bounds how long a widget rerun may
# serve data fetched before the underlying table changed.  Five minutes
# keeps interactive tweaking snappy while still picking up fresh data
# after a daily load; the "Refresh data" buttons clear the cache early.
_CACHE_TTL_S = 300

# ------------------------------------------------------------------
# Report parameter defaults (tunable via the Streamlit widgets).
# ------------------------------------------------------------------
DEFAULT_LOOKBACK = 30          # trailing trading periods per ticker
DEFAULT_LIMIT = 25             # top-N rows for tab A
DEFAULT_STRONG_ACCEL = 2.5     # rvol >= -> 'Strong Acceleration'
DEFAULT_ACCEL = 1.8            # rvol >= -> 'Acceleration'
DEFAULT_DECEL = 0.8            # rvol <= -> 'Deceleration'
DEFAULT_STRONG_DECEL = 0.6     # rvol <= -> 'Strong Deceleration'
DEFAULT_TOP_K = 50             # liquidity filter for tab B

# Weekly deceleration defaults.  The weekly specs use tighter decel
# bounds than the daily report (0.50 / 0.70 vs the daily 0.6 / 0.8),
# so they get their own constants rather than sharing DEFAULT_DECEL /
# DEFAULT_STRONG_DECEL.  The weekly acceleration bounds (2.5 / 1.8),
# the 30-period lookback (now measured in weeks) and the top-K
# liquidity bound (50) intentionally reuse the daily constants.
DEFAULT_WEEKLY_STRONG_DECEL = 0.50  # weekly rvol <= -> 'Strong Deceleration'
DEFAULT_WEEKLY_DECEL = 0.70         # weekly rvol <= -> 'Deceleration'

# Fixed whitelist of volume_regime labels the SQL CASE can emit.  The
# tab-A multiselect is restricted to these values and the builder
# validates against them before formatting into SQL text.
VOLUME_REGIMES = (
    "Strong Acceleration",
    "Acceleration",
    "Normal",
    "Deceleration",
    "Strong Deceleration",
)

_TAB_A_SHEET = "Top 25 by Dollar Volume"
_TAB_A_FILENAME = "top25_by_dollar_volume.xlsx"
_TAB_B_SHEET = "Acceleration Candidates"
_TAB_B_FILENAME = "acceleration_candidates.xlsx"
_TAB_WC_SHEET = "Weekly Combined Regimes"
_TAB_WC_FILENAME = "weekly_combined_regimes.xlsx"
_TAB_WA_SHEET = "Weekly Acceleration"
_TAB_WA_FILENAME = "weekly_acceleration_candidates.xlsx"
_TAB_WD_SHEET = "Weekly Deceleration"
_TAB_WD_FILENAME = "weekly_deceleration_candidates.xlsx"


# ------------------------------------------------------------------
# SQL builders (pure functions — unit-testable without Streamlit).
# ------------------------------------------------------------------


def _require_number(name: str, value) -> float:
    """Return ``value`` as a float after rejecting non-numerics.

    Widget values are formatted directly into the SQL text, so only
    real numbers may pass — ``bool`` and non-numeric types raise
    ``ValueError``/``TypeError`` here instead of reaching the query.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    return value


def _regime_in_clause(regimes: Optional[Sequence[str]]) -> str:
    """Build an optional ``c.volume_regime IN (...)`` WHERE clause.

    Every label is validated against the fixed ``VOLUME_REGIMES``
    whitelist so only known-safe literals reach the SQL text.  ``None``
    or an empty selection means "no filter" (all regimes shown).
    """
    if not regimes:
        return ""
    unknown = [r for r in regimes if r not in VOLUME_REGIMES]
    if unknown:
        raise ValueError(
            f"Unknown volume_regime value(s): {unknown!r}; "
            f"must be drawn from {list(VOLUME_REGIMES)!r}"
        )
    labels = ", ".join(f"'{r}'" for r in regimes)
    return f"WHERE c.volume_regime IN ({labels})\n"


def build_top25_sql(
    lookback: int = DEFAULT_LOOKBACK,
    limit: int = DEFAULT_LIMIT,
    strong_accel: float = DEFAULT_STRONG_ACCEL,
    accel: float = DEFAULT_ACCEL,
    decel: float = DEFAULT_DECEL,
    strong_decel: float = DEFAULT_STRONG_DECEL,
    regimes: Optional[Sequence[str]] = None,
) -> str:
    """Tab A query — top ``limit`` tickers by latest dollar volume plus
    the RVOL / regime edge metrics.

    Adapted from ``docs/report_idea/report_Acceleration Candidates
    (RVOL significantly above norm).md`` (the file whose *name* says
    "Acceleration Candidates" but whose content is the top-25 report).

    ``lookback`` sets the trailing window used for the per-ticker
    averages; ``strong_accel``/``accel``/``decel``/``strong_decel`` are
    the RVOL regime thresholds; ``regimes`` optionally restricts the
    output to selected ``volume_regime`` labels.
    """
    lookback = int(_require_number("lookback", lookback))
    limit = int(_require_number("limit", limit))
    strong_accel = float(_require_number("strong_accel", strong_accel))
    accel = float(_require_number("accel", accel))
    decel = float(_require_number("decel", decel))
    strong_decel = float(_require_number("strong_decel", strong_decel))
    regime_where = _regime_in_clause(regimes)

    return f"""
WITH trade_money AS (
  SELECT
    Ticker,
    Date,
    Open,
    High,
    Low,
    "Close",
    Volume,
    ((Open + High + Low + "Close") / 4.0) * Volume AS trade_money,
    (("Close" - LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date))
      / NULLIF(LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date), 0)) * 100 AS pct_change
  FROM daily_close
),

-- Last N trading periods per ticker (for averages)
last_n_periods AS (
  SELECT
    Ticker,
    Date,
    trade_money,
    Volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS period_rank
  FROM trade_money
),

avg_metrics AS (
  SELECT
    Ticker,
    AVG(trade_money) AS avg_trade_money,
    AVG(Volume)      AS avg_volume
  FROM last_n_periods
  WHERE period_rank <= {lookback}
  GROUP BY Ticker
),

latest_day AS (
  SELECT
    t.Ticker,
    t.Date AS latest_date,
    t."Close" AS latest_close,
    t.Volume AS latest_volume,
    t.trade_money AS latest_trade_money,
    t.pct_change AS latest_pct_change,
    ROW_NUMBER() OVER (PARTITION BY t.Ticker ORDER BY t.Date DESC) AS rn
  FROM trade_money t
),

combined AS (
  SELECT
    l.Ticker,
    l.latest_date,
    l.latest_close,
    ROUND(l.latest_pct_change, 2) AS pct_change,
    l.latest_volume,
    a.avg_volume,
    ROUND(l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0), 2) AS rvol,
    l.latest_trade_money,
    a.avg_trade_money,
    ROUND(l.latest_trade_money * 1.0 / NULLIF(a.avg_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {strong_accel} THEN 'Strong Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {accel} THEN 'Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) <= {strong_decel} THEN 'Strong Deceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) <= {decel} THEN 'Deceleration'
      ELSE 'Normal'
    END AS volume_regime
  FROM latest_day l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

total_avg AS (
  SELECT SUM(avg_trade_money) AS total_avg_trade_money
  FROM avg_metrics
)

SELECT
  c.Ticker,
  c.latest_date,
  c.latest_close,
  c.pct_change,
  c.latest_volume,
  c.avg_volume,
  c.rvol,
  ROUND(c.latest_trade_money, 0) AS latest_dollar_volume,
  ROUND(c.avg_trade_money, 0) AS avg_dollar_volume,
  c.relative_dollar_vol,
  c.volume_regime,
  ROUND((c.avg_trade_money / t.total_avg_trade_money) * 100, 2) AS pct_of_total_avg_dollar
FROM combined c
CROSS JOIN total_avg t
{regime_where}ORDER BY c.latest_trade_money DESC
LIMIT {limit}
"""


def build_accel_sql(
    lookback: int = DEFAULT_LOOKBACK,
    accel: float = DEFAULT_ACCEL,
    strong_accel: float = DEFAULT_STRONG_ACCEL,
    top_k: int = DEFAULT_TOP_K,
) -> str:
    """Tab B query — acceleration candidates only.

    Adapted from ``docs/report_idea/report_Top 25 by Approximate Dollar
    Volume.md`` (the file whose *name* says "Top 25" but whose content
    is the acceleration-candidates screen): ranks every ticker by
    latest dollar volume, keeps the ``top_k`` most-liquid names, then
    keeps only ``rvol >= accel`` (labelled 'Acceleration', or 'Strong
    Acceleration' when ``rvol >= strong_accel``), ordered by ``rvol``
    descending.
    """
    lookback = int(_require_number("lookback", lookback))
    accel = float(_require_number("accel", accel))
    strong_accel = float(_require_number("strong_accel", strong_accel))
    top_k = int(_require_number("top_k", top_k))

    return f"""
WITH trade_money AS (
  SELECT
    Ticker,
    Date,
    Open,
    High,
    Low,
    "Close",
    Volume,
    ((Open + High + Low + "Close") / 4.0) * Volume AS trade_money,
    (("Close" - LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date))
      / NULLIF(LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date), 0)) * 100 AS pct_change
  FROM daily_close
),

last_n_periods AS (
  SELECT
    Ticker,
    Date,
    trade_money,
    Volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS period_rank
  FROM trade_money
),

avg_metrics AS (
  SELECT
    Ticker,
    AVG(trade_money) AS avg_trade_money,
    AVG(Volume)      AS avg_volume
  FROM last_n_periods
  WHERE period_rank <= {lookback}
  GROUP BY Ticker
),

latest_day AS (
  SELECT
    t.Ticker,
    t.Date AS latest_date,
    t."Close" AS latest_close,
    t.Volume AS latest_volume,
    t.trade_money AS latest_trade_money,
    t.pct_change AS latest_pct_change,
    ROW_NUMBER() OVER (PARTITION BY t.Ticker ORDER BY t.Date DESC) AS rn
  FROM trade_money t
),

combined AS (
  SELECT
    l.Ticker,
    l.latest_date,
    l.latest_close,
    ROUND(l.latest_pct_change, 2) AS pct_change,
    l.latest_volume,
    a.avg_volume,
    ROUND(l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0), 2) AS rvol,
    l.latest_trade_money,
    a.avg_trade_money,
    ROUND(l.latest_trade_money * 1.0 / NULLIF(a.avg_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {strong_accel} THEN 'Strong Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {accel} THEN 'Acceleration'
      ELSE 'Other'
    END AS volume_regime
  FROM latest_day l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

-- Rank all stocks by latest dollar volume first (to keep only liquid names)
ranked_by_dollar AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY latest_trade_money DESC) AS dollar_rank
  FROM combined
)

SELECT
  Ticker,
  latest_date,
  latest_close,
  pct_change,
  latest_volume,
  avg_volume,
  rvol,
  ROUND(latest_trade_money, 0) AS latest_dollar_volume,
  ROUND(avg_trade_money, 0) AS avg_dollar_volume,
  relative_dollar_vol,
  volume_regime
FROM ranked_by_dollar
WHERE volume_regime IN ('Acceleration', 'Strong Acceleration')
  AND dollar_rank <= {top_k}
ORDER BY rvol DESC, latest_trade_money DESC
"""


# ------------------------------------------------------------------
# Weekly interpretation guides — verbatim text from the source
# docs/report_idea/report_weekly*.md files, rendered in each weekly
# tab via st.markdown inside an st.expander.
# ------------------------------------------------------------------

# docs/report_idea/report_weekly_trends.md — "Quick Interpretation
# Guide (Combined Report)".
WEEKLY_COMBINED_GUIDE = """\
### Quick Interpretation Guide (Combined Report)

| volume_regime          | Priority | What it usually means                          | Action Bias                     |
|------------------------|----------|------------------------------------------------|---------------------------------|
| **Strong Acceleration**| Highest  | Explosive volume expansion                     | Momentum / Breakout focus      |
| **Acceleration**       | High     | Clearly elevated participation                 | Trend-following                |
| **Deceleration**       | Medium   | Noticeably quieter than normal                 | Caution / Range / Wait         |
| **Strong Deceleration**| High     | Volume has dried up significantly              | Exhaustion or quiet setup      |

**Recommended reading order:**
1. Strong Acceleration names first
2. Then regular Acceleration
3. Then Strong Deceleration
4. Finally regular Deceleration

This single query gives you the complete weekly volume-regime picture among the most liquid stocks every weekend."""

# docs/report_idea/report_weekly_trend_accelerating.md —
# "Interpretation Guide – Weekly Acceleration Candidates".
WEEKLY_ACCEL_GUIDE = """\
### Interpretation Guide – Weekly Acceleration Candidates

| Signal | Meaning | Typical Trading Implication |
|--------|---------|-----------------------------|
| **Strong Acceleration** (RVOL ≥ 2.5) | Volume expanded dramatically vs the previous 30 weeks | Highest conviction. Often marks the start or acceleration of a multi-week trend (accumulation or distribution). Highest priority. |
| **Acceleration** (RVOL 1.8 – 2.49) | Clearly elevated weekly volume | Solid confirmation of institutional interest. Good for swing trades in the direction of the weekly price change. |
| **High RVOL + Rising Price** | Volume supporting the uptrend | Bullish continuation / breakout candidate. |
| **High RVOL + Falling Price** | Heavy selling on expanding volume | Distribution or breakdown. Caution / potential short candidate. |
| **High RVOL + Flat Price** | Quiet accumulation or absorption | Watch for upcoming directional move. |
| **Top of the list (highest RVOL among top 50 $ volume)** | Liquidity + extreme expansion | Best risk/reward setups – avoid thin names automatically. |

#### How to use the weekly report
1. Run it every weekend (after Friday close).
2. Focus first on **Strong Acceleration** names inside the top 50.
3. Cross-check with the daily version for timing (weekly tells you *what* is trending, daily helps with *when* to enter).
4. Best timeframes: 1–4 week swing trades.
5. Combine with price structure (weekly breakout, pullback to moving average, etc.) for higher probability.

#### Suggested thresholds you can tweak
- More selective → change `1.8` to `2.0` or `2.2`
- More signals → lower to `1.6`
- Longer lookback → change `30` to `40` or `52` weeks"""

# docs/report_idea/report_weekly_trend_decelerating.md —
# "Interpretation Guide – Weekly Deceleration Candidates".
WEEKLY_DECEL_GUIDE = """\
### Interpretation Guide – Weekly Deceleration Candidates

| Signal | Meaning | Typical Trading Implication |
|--------|---------|-----------------------------|
| **Strong Deceleration** (RVOL ≤ 0.50) | Volume dried up dramatically vs the previous 30 weeks | Highest conviction quietness. Can signal exhaustion of a prior trend, lack of interest, or quiet accumulation/distribution. |
| **Deceleration** (RVOL 0.51 – 0.70) | Clearly below-average weekly volume | Reduced participation. Trend may be losing steam or the stock is consolidating. |
| **Low RVOL + Rising Price** | Price rising on declining volume | Often a warning of weakening upside momentum (potential topping process). |
| **Low RVOL + Falling Price** | Price falling on low volume | Can be a healthy pullback or lack of selling pressure (less bearish than high-volume decline). |
| **Low RVOL + Flat Price** | Very quiet consolidation | Watch for the eventual breakout – low volume ranges often precede expansion. |
| **Lowest RVOL among top 50 $ volume** | Liquid name that has gone unusually quiet | Highest quality “sleeping giant” or exhaustion candidates. |

#### How to use the weekly Deceleration report
1. Run together with the Acceleration report every weekend.
2. Strong Deceleration in a previously strong name can mark the end of a multi-week move.
3. Low volume + price holding up is often more constructive than low volume + price breaking down.
4. Best used for mean-reversion ideas, range trades, or waiting for volume to return before entering.
5. Pair with the daily version to see if volume is starting to pick up again inside the quiet weekly regime.

#### Suggested threshold tweaks
- Stricter → change `0.70` to `0.60` and `0.50` to `0.40`
- More signals → raise to `0.80` / `0.60`"""


# ------------------------------------------------------------------
# Top-level + daily interpretation guides — verbatim text from the
# source docs/report_idea/*.md files.  BASIC_GUIDE renders in a
# top-level "About / How to read these reports" expander under the
# app title; the DAILY_*_GUIDE constants render inside their tabs via
# st.expander("Interpretation guide") + st.markdown, exactly like the
# weekly guides above.
# ------------------------------------------------------------------

# docs/report_idea/report_basic.md — the GENERIC sections only:
# title block + "Purpose & Trading Edge" + "Recommended Usage for
# Edge" + "Methodology Notes" (+ the closing paragraph).  The
# session-specific content — the Sep-11 top-25 data table, the named
# acceleration/deceleration candidates and "Key Insights & Potential
# Trading Edges" — is deliberately excluded.  F7-T27 (PM-approved
# verbatim exception): 3 permanently-stale lines inside the generic
# sections are also deliberately dropped — the "Session: September
# 11, 2026 ..." title-block line, the "Data sources: MarketBeat,
# MarketGrep, ..." paragraph (names sources this app does not use),
# and the "- Markets closed Sep 13 ..." methodology bullet.
BASIC_GUIDE = """\
**Volume Acceleration/Deceleration Edge Report**  
**Focus: Top 25 Active Stocks by Dollar Volume**  

### Purpose & Trading Edge
This report screens the highest **dollar-volume** names (true institutional liquidity leaders) for **volume accelerating or decelerating much faster than normal**.  

**Core idea**:  
- Absolute volume alone is noisy.  
- **Relative Volume (RVOL = today’s volume / average volume)** reveals when flow is genuinely abnormal.  
- Extreme RVOL in high-dollar-volume stocks often precedes or confirms institutional repositioning, breakouts, earnings reactions, or exhaustion.  
- Combine with price action for edges: high-RVOL + strong price = momentum continuation/breakout; high-RVOL + weak price = distribution or reversal risk; unusually low RVOL in a big-name mover can signal lack of conviction.

**Primary filters used**:
- Ranked by dollar volume (Price × Volume).
- RVOL thresholds: Acceleration > 1.8–2.0× (stronger > 2.5×); Deceleration < 0.7×.
- Preference for liquid names (avoid pure micro-cap noise unless dollar volume is meaningful).
- Price change context for directional bias.

### Recommended Usage for Edge
- **Daily process**: Pull top 25–50 dollar volume → calculate RVOL → flag >2× (or >1.8× with strong price) and <0.7×.
- **Confirmation**: Require price structure (break of range, volume climax, etc.) + preferably options flow or news catalyst.
- **Risk management**: Size by dollar volume/liquidity. High-RVOL names can gap; use wider stops or options.
- **Timeframe**: Best for swing (1–5 days) or next-session open reaction. Intraday, monitor for volume tapering (deceleration after acceleration = potential exhaustion).

### Methodology Notes
- Dollar volume prioritizes institutional relevance over pure share volume.
- RVOL uses recent average volume (typically 20–65 day or 3-month depending on source).
- Report is descriptive — not recommendations. Always cross-check live data, news, and your own risk parameters.

This framework systematically surfaces the stocks where volume behavior is *most abnormal* relative to the highest-liquidity names — exactly the condition that historically produces the cleanest short-term trading edges."""

# docs/report_idea/report_Acceleration Candidates (RVOL significantly
# above norm).md — "How to use / interpret the output" + "Recommended
# thresholds" + "Optional enhancements" (the source md for the
# "Daily — Top 25 by Dollar Volume" tab's query).
DAILY_TOP25_GUIDE = """\
### How to use / interpret the output

| Column | Meaning |
|--------|---------|
| `latest_dollar_volume` | Today’s dollar volume (the ranking key for “Top 25 Active”) |
| `rvol` | Relative Volume (today’s volume / 30-day average volume) — **core edge metric** |
| `relative_dollar_vol` | Same idea but on dollar volume |
| `volume_regime` | Auto-classified: Strong Acceleration / Acceleration / Normal / Deceleration / Strong Deceleration |
| `pct_of_total_avg_dollar` | Same as your original report (share of average dollar volume) |

### Recommended thresholds (you can adjust in the `CASE`)
- **Strong Acceleration**: RVOL ≥ 2.5
- **Acceleration**: RVOL ≥ 1.8
- **Deceleration**: RVOL ≤ 0.8
- **Strong Deceleration**: RVOL ≤ 0.6

### Optional enhancements you can add easily

1. **Filter only Acceleration / Strong Acceleration** (for the edge watchlist):
```sql
-- add at the end before ORDER BY
WHERE c.volume_regime IN ('Acceleration', 'Strong Acceleration')
```

2. **Add more price context** (e.g. distance from 20-day high):
```sql
-- inside trade_money CTE or a new CTE
MAX("Close") OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high_20d
```

3. **Change lookback** from 30 to 20 periods: just change `period_rank <= 30` to `<= 20`."""

# docs/report_idea/report_Top 25 by Approximate Dollar Volume.md —
# "Key points of this query" + the closing tweak paragraph (the source
# md for the "Daily — Acceleration Candidates" tab's query).
DAILY_ACCEL_GUIDE = """\
### Key points of this query

- **Only Acceleration Candidates**: Filters strictly to `rvol >= 1.8` (and highlights `>= 2.5` as Strong Acceleration).
- **Top 50 liquidity filter**: First ranks *all* stocks by latest dollar volume, then keeps only those in the top 50 before applying the RVOL filter. This cleanly excludes thin-volume stocks.
- Keeps the same style and CTEs as your original base report.
- Ordered by strongest RVOL first (best acceleration signal), then by dollar volume.

You can tighten the threshold easily (e.g. change `1.8` to `2.0` or `2.5` if you only want the strongest signals)."""


# ------------------------------------------------------------------
# Weekly SQL builders — the three docs/report_idea/report_weekly*.md
# queries.  All share the same weekly-bar prologue (daily_base ->
# weekly_bars -> weekly_with_change -> last_n_weeks -> avg_metrics ->
# latest_week); each builder then adds its own combined CASE, the
# dollar_rank liquidity filter, and its own ordering.
# ------------------------------------------------------------------


def _weekly_cte_prefix(lookback: int) -> str:
    """Return the shared weekly CTE prologue (through ``latest_week``).

    ``lookback`` bounds the trailing number of *weeks* averaged in
    ``avg_metrics`` — it is a validated int by the time it gets here,
    so it is safe to format into the SQL text.
    """
    return f"""
WITH daily_base AS (
  SELECT
    Ticker,
    Date,
    Open,
    High,
    Low,
    "Close",
    Volume,
    ((Open + High + Low + "Close") / 4.0) * Volume AS trade_money
  FROM daily_close
),

-- Aggregate into weekly bars (weeks start Monday via DATE_TRUNC)
weekly_bars AS (
  SELECT
    Ticker,
    DATE_TRUNC('week', Date) AS week_start,
    MIN(Date) AS week_start_date,
    MAX(Date) AS week_end_date,

    (ARRAY_AGG(Open ORDER BY Date ASC))[1] AS week_open,
    MAX(High) AS week_high,
    MIN(Low) AS week_low,
    (ARRAY_AGG("Close" ORDER BY Date DESC))[1] AS week_close,

    SUM(Volume) AS week_volume,
    SUM(trade_money) AS week_trade_money
  FROM daily_base
  GROUP BY Ticker, DATE_TRUNC('week', Date)
),

-- Add weekly % change
weekly_with_change AS (
  SELECT
    *,
    ((week_close - LAG(week_close) OVER (PARTITION BY Ticker ORDER BY week_start))
      / NULLIF(LAG(week_close) OVER (PARTITION BY Ticker ORDER BY week_start), 0)) * 100 AS week_pct_change
  FROM weekly_bars
),

-- Last N weeks per ticker
last_n_weeks AS (
  SELECT
    Ticker,
    week_start,
    week_trade_money,
    week_volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY week_start DESC) AS week_rank
  FROM weekly_with_change
),

-- Average metrics over the last N weeks
avg_metrics AS (
  SELECT
    Ticker,
    AVG(week_trade_money) AS avg_week_trade_money,
    AVG(week_volume) AS avg_week_volume
  FROM last_n_weeks
  WHERE week_rank <= {lookback}
  GROUP BY Ticker
),

-- Latest completed week
latest_week AS (
  SELECT
    w.*,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY week_start DESC) AS rn
  FROM weekly_with_change w
)
"""


def build_weekly_combined_sql(
    lookback: int = DEFAULT_LOOKBACK,
    top_k: int = DEFAULT_TOP_K,
    strong_accel: float = DEFAULT_STRONG_ACCEL,
    accel: float = DEFAULT_ACCEL,
    decel: float = DEFAULT_WEEKLY_DECEL,
    strong_decel: float = DEFAULT_WEEKLY_STRONG_DECEL,
) -> str:
    """Weekly combined report — all four non-Normal regimes among the
    ``top_k`` most-liquid names, regime-priority ordering.

    Adapted from ``docs/report_idea/report_weekly_trends.md``:
    ``lookback`` trailing weeks feed the averages; the CASE labels all
    four regimes; the final SELECT filters out 'Normal' and orders by
    regime priority (Strong Acceleration, Acceleration, Strong
    Deceleration, Deceleration) then ``rvol`` descending.
    """
    lookback = int(_require_number("lookback", lookback))
    top_k = int(_require_number("top_k", top_k))
    strong_accel = float(_require_number("strong_accel", strong_accel))
    accel = float(_require_number("accel", accel))
    decel = float(_require_number("decel", decel))
    strong_decel = float(_require_number("strong_decel", strong_decel))

    # NOTE — deliberate deviation from the source md: its own SQL ranks
    # the ORDER BY priorities Deceleration=3 / Strong Deceleration=4,
    # but the same md's interpretation guide gives the recommended
    # reading order as Strong Acceleration -> Acceleration ->
    # Strong Deceleration -> Deceleration.  PM approved matching the
    # guide, so Strong Deceleration ranks third below.
    return (
        _weekly_cte_prefix(lookback)
        + f""",
combined AS (
  SELECT
    l.Ticker,
    l.week_start,
    l.week_end_date AS latest_week_end,
    l.week_close AS latest_close,
    ROUND(l.week_pct_change, 2) AS pct_change,
    l.week_volume AS latest_volume,
    a.avg_week_volume,
    ROUND(l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0), 2) AS rvol,
    l.week_trade_money AS latest_dollar_volume,
    a.avg_week_trade_money,
    ROUND(l.week_trade_money * 1.0 / NULLIF(a.avg_week_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) >= {strong_accel} THEN 'Strong Acceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) >= {accel} THEN 'Acceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= {strong_decel} THEN 'Strong Deceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= {decel} THEN 'Deceleration'
      ELSE 'Normal'
    END AS volume_regime
  FROM latest_week l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY latest_dollar_volume DESC) AS dollar_rank
  FROM combined
)

SELECT
  Ticker,
  week_start,
  latest_week_end,
  latest_close,
  pct_change,
  latest_volume,
  avg_week_volume,
  rvol,
  ROUND(latest_dollar_volume, 0) AS latest_dollar_volume,
  ROUND(avg_week_trade_money, 0) AS avg_dollar_volume,
  relative_dollar_vol,
  volume_regime
FROM ranked
WHERE volume_regime IN (
    'Strong Acceleration',
    'Acceleration',
    'Strong Deceleration',
    'Deceleration'
  )
  AND dollar_rank <= {top_k}
ORDER BY
  -- Priority follows the md's "Recommended reading order" guide
  -- (Strong Deceleration before Deceleration), deliberately
  -- deviating from the md's own SQL which had them swapped.
  CASE volume_regime
    WHEN 'Strong Acceleration' THEN 1
    WHEN 'Acceleration' THEN 2
    WHEN 'Strong Deceleration' THEN 3
    WHEN 'Deceleration' THEN 4
  END,
  rvol DESC
"""
    )


def build_weekly_accel_sql(
    lookback: int = DEFAULT_LOOKBACK,
    accel: float = DEFAULT_ACCEL,
    strong_accel: float = DEFAULT_STRONG_ACCEL,
    top_k: int = DEFAULT_TOP_K,
) -> str:
    """Weekly acceleration candidates — the two acceleration regimes
    among the ``top_k`` most-liquid names, ``rvol`` descending.

    Adapted from
    ``docs/report_idea/report_weekly_trend_accelerating.md``.
    """
    lookback = int(_require_number("lookback", lookback))
    accel = float(_require_number("accel", accel))
    strong_accel = float(_require_number("strong_accel", strong_accel))
    top_k = int(_require_number("top_k", top_k))

    return (
        _weekly_cte_prefix(lookback)
        + f""",
combined AS (
  SELECT
    l.Ticker,
    l.week_start,
    l.week_end_date AS latest_week_end,
    l.week_close AS latest_close,
    ROUND(l.week_pct_change, 2) AS pct_change,
    l.week_volume AS latest_volume,
    a.avg_week_volume,
    ROUND(l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0), 2) AS rvol,
    l.week_trade_money AS latest_dollar_volume,
    a.avg_week_trade_money,
    ROUND(l.week_trade_money * 1.0 / NULLIF(a.avg_week_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) >= {strong_accel} THEN 'Strong Acceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) >= {accel} THEN 'Acceleration'
      ELSE 'Other'
    END AS volume_regime
  FROM latest_week l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY latest_dollar_volume DESC) AS dollar_rank
  FROM combined
)

SELECT
  Ticker,
  week_start,
  latest_week_end,
  latest_close,
  pct_change,
  latest_volume,
  avg_week_volume,
  rvol,
  ROUND(latest_dollar_volume, 0) AS latest_dollar_volume,
  ROUND(avg_week_trade_money, 0) AS avg_dollar_volume,
  relative_dollar_vol,
  volume_regime
FROM ranked
WHERE volume_regime IN ('Acceleration', 'Strong Acceleration')
  AND dollar_rank <= {top_k}
ORDER BY rvol DESC, latest_dollar_volume DESC
"""
    )


def build_weekly_decel_sql(
    lookback: int = DEFAULT_LOOKBACK,
    decel: float = DEFAULT_WEEKLY_DECEL,
    strong_decel: float = DEFAULT_WEEKLY_STRONG_DECEL,
    top_k: int = DEFAULT_TOP_K,
) -> str:
    """Weekly deceleration candidates — the two deceleration regimes
    among the ``top_k`` most-liquid names, ``rvol`` ascending
    (quietest names first).

    Adapted from
    ``docs/report_idea/report_weekly_trend_decelerating.md``.  Note the
    weekly decel defaults (``strong_decel=0.50``, ``decel=0.70``) are
    tighter than the daily ones.
    """
    lookback = int(_require_number("lookback", lookback))
    decel = float(_require_number("decel", decel))
    strong_decel = float(_require_number("strong_decel", strong_decel))
    top_k = int(_require_number("top_k", top_k))

    return (
        _weekly_cte_prefix(lookback)
        + f""",
combined AS (
  SELECT
    l.Ticker,
    l.week_start,
    l.week_end_date AS latest_week_end,
    l.week_close AS latest_close,
    ROUND(l.week_pct_change, 2) AS pct_change,
    l.week_volume AS latest_volume,
    a.avg_week_volume,
    ROUND(l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0), 2) AS rvol,
    l.week_trade_money AS latest_dollar_volume,
    a.avg_week_trade_money,
    ROUND(l.week_trade_money * 1.0 / NULLIF(a.avg_week_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= {strong_decel} THEN 'Strong Deceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= {decel} THEN 'Deceleration'
      ELSE 'Other'
    END AS volume_regime
  FROM latest_week l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY latest_dollar_volume DESC) AS dollar_rank
  FROM combined
)

SELECT
  Ticker,
  week_start,
  latest_week_end,
  latest_close,
  pct_change,
  latest_volume,
  avg_week_volume,
  rvol,
  ROUND(latest_dollar_volume, 0) AS latest_dollar_volume,
  ROUND(avg_week_trade_money, 0) AS avg_dollar_volume,
  relative_dollar_vol,
  volume_regime
FROM ranked
WHERE volume_regime IN ('Deceleration', 'Strong Deceleration')
  AND dollar_rank <= {top_k}
ORDER BY rvol ASC, latest_dollar_volume DESC
"""
    )


# ------------------------------------------------------------------
# Threshold sanity helper — pure function, testable without Streamlit.
# ------------------------------------------------------------------


def find_threshold_inversions(
    accel: Optional[float] = None,
    strong_accel: Optional[float] = None,
    decel: Optional[float] = None,
    strong_decel: Optional[float] = None,
) -> List[str]:
    """Return human-readable descriptions of inverted RVOL thresholds.

    The generated ``CASE`` expressions check the *strong* bounds first,
    so the regime labels only behave as labelled when
    ``strong_accel >= accel`` and ``strong_decel <= decel``.  With an
    inverted pair the query silently mislabels rows — e.g.
    ``accel=2.0, strong_accel=1.0`` labels every ``rvol >= 1.0`` name
    'Strong Acceleration' (never plain 'Acceleration'), and on tab B
    the effective filter becomes ``rvol >= strong_accel`` because the
    WHERE clause filters on the regime *label*, not on ``rvol >=
    accel`` directly.  The decel pair is the mirror image.

    Each ``None`` pair is skipped — tab B only uses the acceleration
    pair.  Returns an empty list when nothing is inverted; the caller
    renders one non-blocking ``st.warning`` per returned string.
    """
    inversions: List[str] = []
    if (
        accel is not None
        and strong_accel is not None
        and strong_accel < accel
    ):
        inversions.append(
            f"Inverted thresholds: 'Strong Acceleration' "
            f"(rvol >= {strong_accel:g}) is set BELOW 'Acceleration' "
            f"(rvol >= {accel:g}). The SQL checks the strong bound "
            f"first, so every row with rvol >= {strong_accel:g} is "
            f"labelled 'Strong Acceleration' and the effective "
            f"acceleration floor is {strong_accel:g}, not {accel:g}. "
            f"Set 'Strong Acceleration' >= 'Acceleration'."
        )
    if (
        decel is not None
        and strong_decel is not None
        and strong_decel > decel
    ):
        inversions.append(
            f"Inverted thresholds: 'Strong Deceleration' "
            f"(rvol <= {strong_decel:g}) is set ABOVE 'Deceleration' "
            f"(rvol <= {decel:g}). The SQL checks the strong bound "
            f"first, so every row with rvol <= {strong_decel:g} is "
            f"labelled 'Strong Deceleration' and the effective "
            f"deceleration bound is {strong_decel:g}, not {decel:g}. "
            f"Set 'Strong Deceleration' <= 'Deceleration'."
        )
    return inversions


# ------------------------------------------------------------------
# Data access — Flight first, direct read-only DuckDB fallback.
# ------------------------------------------------------------------


def _query_via_flight(location: str, sql: str) -> pd.DataFrame:
    """Run ``sql`` through the Flight service's ``do_get``.

    Sends a JSON ``{"sql": ...}`` ticket — the same contract the
    Flight server's ``do_get`` implements — after a short
    ``wait_for_available`` probe.  The call is bounded by
    ``_FLIGHT_GET_TIMEOUT_S`` so a wedged server cannot hang the app.
    Raises on any failure; the caller decides whether to fall back.
    """
    client = flight.FlightClient(location)
    client.wait_for_available(timeout=_FLIGHT_CONNECT_TIMEOUT_S)
    ticket = flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))
    options = flight.FlightCallOptions(timeout=_FLIGHT_GET_TIMEOUT_S)
    reader = client.do_get(ticket, options=options)
    return reader.read_all().to_pandas()


def _query_via_duckdb(db_path: str, sql: str) -> pd.DataFrame:
    """Run ``sql`` on a direct *read-only* DuckDB connection.

    Only usable when no process holds the database write lock (i.e.
    the Flight service / ingestion daemon is not running).
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(sql).df()
    finally:
        conn.close()


def _fetch_dataframe_uncached(
    sql: str, location: str, db_path: str
) -> Optional[pd.DataFrame]:
    """Fetch report rows: Flight first, direct read-only DB second.

    ``location`` is the ``grpc://host:port`` Flight endpoint and
    ``db_path`` the fallback DuckDB file — kept as plain strings (not
    the config object) so the ``st.cache_data`` cache key stays small
    and deterministic.  Returns ``None`` when both paths fail (each
    failure is logged); the UI turns that into ``st.error``.
    """
    try:
        return _query_via_flight(location, sql)
    except Exception as exc:
        logger.warning(
            "Flight server at %s unreachable or query failed (%s); "
            "falling back to a direct read-only DuckDB query.",
            location,
            exc,
        )

    try:
        return _query_via_duckdb(db_path, sql)
    except Exception:
        logger.exception(
            "Direct read-only DuckDB query also failed "
            "(write lock held or database unavailable)."
        )
        return None


def fetch_dataframe(
    sql: str, config: PipelineConfig
) -> Optional[pd.DataFrame]:
    """Uncached fetch for callers outside the Streamlit UI path.

    Same Flight-first / read-only-fallback logic as the cached UI path,
    minus ``st.cache_data`` — kept public so tests and any non-Streamlit
    caller can exercise the fetch without the cache layer.
    """
    location = (
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    return _fetch_dataframe_uncached(sql, location, config.db_path)


# ------------------------------------------------------------------
# Query-result caching (st.cache_data, built lazily in the UI path).
# ------------------------------------------------------------------

# Per-process holder for the lazily-built cached fetch.  ``None`` until
# the first UI run builds it — keeps ``import report_app`` free of any
# streamlit dependency.
_CACHED_FETCH = None


def _build_cached_fetch(st):
    """Return an ``st.cache_data``-wrapped fetch keyed on the SQL text.

    The wrapper's cache key is ``(sql, location, db_path)``: identical
    SQL against the same targets is served from memory for up to
    ``_CACHE_TTL_S`` seconds, so widget reruns that leave the generated
    SQL unchanged do not re-query the Flight server / DuckDB.  ``st``
    is passed in (never imported at module level) so tests can supply a
    fake ``cache_data`` and exercise the key derivation directly.
    """

    @st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
    def _cached(
        sql: str, location: str, db_path: str
    ) -> Optional[pd.DataFrame]:
        return _fetch_dataframe_uncached(sql, location, db_path)

    return _cached


def _get_cached_fetch(st):
    """Return the process-wide cached fetch, building it on first use.

    The ``st.cache_data`` wrapper must be created once and reused
    across reruns — re-decorating on every rerun would build a fresh,
    empty cache each time and never hit.  ``.clear()`` on the returned
    callable empties the cache (wired to the "Refresh data" buttons).
    """
    global _CACHED_FETCH
    if _CACHED_FETCH is None:
        _CACHED_FETCH = _build_cached_fetch(st)
    return _CACHED_FETCH


def cached_fetch_dataframe(
    st, sql: str, config: PipelineConfig
) -> Optional[pd.DataFrame]:
    """``fetch_dataframe`` through the ``st.cache_data`` layer.

    The cache key derives from the SQL text plus the connection
    targets, so a widget rerun that leaves the SQL unchanged is served
    from cache; any changed widget value produces a different SQL
    string and therefore a cache miss.  Results are cached for
    ``_CACHE_TTL_S`` seconds; a ``None`` result (both paths failed) is
    cached too — a transient Flight blip will not re-probe on every
    rerun, and the "Refresh data" button clears it early.
    """
    location = (
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    return _get_cached_fetch(st)(sql, location, config.db_path)


# ------------------------------------------------------------------
# xlsx export (in-memory, for st.download_button).
# ------------------------------------------------------------------


def dataframe_to_xlsx_bytes(
    df: pd.DataFrame, sheet_name: str = "Report"
) -> bytes:
    """Serialize ``df`` to xlsx bytes with auto-adjusted column widths.

    Each column's width is set to the maximum rendered content length
    (header included) plus ``_WIDTH_PADDING`` — same convention as
    ``scripts/report_top25_active.py``.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]
        # justified: openpyxl exposes no vectorized API for per-column
        # width sizing — each column's cells must be measured one
        # column at a time (small report sheets, negligible cost).
        for column_cells in worksheet.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in column_cells
            )
            letter = column_cells[0].column_letter
            worksheet.column_dimensions[letter].width = (
                max_len + _WIDTH_PADDING
            )
    return buffer.getvalue()


# ------------------------------------------------------------------
# Streamlit UI (streamlit is imported lazily so this module stays
# importable and testable without the Streamlit runtime).
# ------------------------------------------------------------------


def _ordered_bar_spec(df: pd.DataFrame, y_col: str) -> dict:
    """Return a self-contained Vega-Lite spec for a bar chart whose
    bars keep ``df`` row order.

    ``"sort": null`` on the nominal ``Ticker`` axis is the key part:
    without it Vega-Lite sorts categories alphabetically, which would
    scramble the query's ``ORDER BY`` (the table's default order).
    The data values are embedded so the spec can be handed straight to
    ``st.vega_lite_chart``.
    """
    return {
        "mark": "bar",
        "data": {
            "values": df[["Ticker", y_col]].to_dict(orient="records")
        },
        "encoding": {
            "x": {"field": "Ticker", "type": "nominal", "sort": None},
            "y": {"field": y_col, "type": "quantitative"},
        },
    }


def _build_ordered_bar_chart(df: pd.DataFrame, y_col: str):
    """Return an ``altair.Chart`` bar chart preserving ``df`` row order.

    ``sort=None`` on the nominal x axis serializes to ``"sort": null``,
    so the bars follow the SQL ``ORDER BY`` (the dataframe's row
    order) rather than Vega-Lite's alphabetical category default.
    ``altair`` is imported lazily — it ships with streamlit, but this
    module stays importable without it (tests may exercise
    :func:`_ordered_bar_spec` instead).
    """
    import altair as alt

    return (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("Ticker:N", sort=None),
            y=alt.Y(f"{y_col}:Q"),
        )
    )


def _render_ordered_bar_chart(st, df: pd.DataFrame, y_col: str) -> None:
    """Render ``df`` as a bar chart in the query's ``ORDER BY`` order.

    ``st.bar_chart`` cannot express this — its nominal x axis always
    sorts alphabetically — so the chart goes through
    ``st.altair_chart`` (Altair ships with streamlit; no extra
    dependency), falling back to ``st.vega_lite_chart`` with the
    equivalent spec if Altair is ever missing.  ``st`` is passed in so
    tests can exercise the helper without the Streamlit runtime.
    """
    try:
        chart = _build_ordered_bar_chart(df, y_col)
    except ImportError:
        st.vega_lite_chart(_ordered_bar_spec(df, y_col), width="stretch")
        return
    st.altair_chart(chart, width="stretch")


def _render_top25_tab(st, config: PipelineConfig) -> None:
    """Tab A — Top 25 by Dollar Volume (+ RVOL/regime edge metrics)."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback periods",
                min_value=2,
                max_value=500,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="a_lookback",
            )
            limit = st.number_input(
                "Top-N limit",
                min_value=1,
                max_value=500,
                value=DEFAULT_LIMIT,
                step=1,
                key="a_limit",
            )
        with col2:
            strong_accel = st.number_input(
                "Strong Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_STRONG_ACCEL,
                step=0.1,
                key="a_strong_accel",
            )
            accel = st.number_input(
                "Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_ACCEL,
                step=0.1,
                key="a_accel",
            )
        with col3:
            decel = st.number_input(
                "Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_DECEL,
                step=0.1,
                key="a_decel",
            )
            strong_decel = st.number_input(
                "Strong Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_STRONG_DECEL,
                step=0.1,
                key="a_strong_decel",
            )
        regimes = st.multiselect(
            "Volume regime filter (empty = all)",
            options=list(VOLUME_REGIMES),
            default=list(VOLUME_REGIMES),
            key="a_regimes",
        )
        if st.button("Refresh data", key="a_refresh"):
            _get_cached_fetch(st).clear()

    # Non-blocking warning: an inverted pair silently mislabels rows
    # (the CASE checks the strong bound first), so surface it but still
    # render the tab.
    # justified: at most two warning strings — one per inverted
    # threshold pair; a fixed tiny set, not data iteration.
    for msg in find_threshold_inversions(
        accel=accel,
        strong_accel=strong_accel,
        decel=decel,
        strong_decel=strong_decel,
    ):
        st.warning(msg)

    try:
        sql = build_top25_sql(
            lookback=lookback,
            limit=limit,
            strong_accel=strong_accel,
            accel=accel,
            decel=decel,
            strong_decel=strong_decel,
            regimes=regimes,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = cached_fetch_dataframe(st, sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "latest_dollar_volume" in df.columns:
        _render_ordered_bar_chart(st, df, "latest_dollar_volume")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_A_SHEET),
        file_name=_TAB_A_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="a_download",
    )
    with st.expander("Interpretation guide"):
        st.markdown(DAILY_TOP25_GUIDE)


def _render_accel_tab(st, config: PipelineConfig) -> None:
    """Tab B — Acceleration Candidates (rvol >= threshold among the
    top-K most-liquid names, ordered by rvol descending)."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback periods",
                min_value=2,
                max_value=500,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="b_lookback",
            )
            top_k = st.number_input(
                "Liquidity top-K",
                min_value=1,
                max_value=2000,
                value=DEFAULT_TOP_K,
                step=1,
                key="b_top_k",
            )
        with col2:
            accel = st.number_input(
                "Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_ACCEL,
                step=0.1,
                key="b_accel",
            )
        with col3:
            strong_accel = st.number_input(
                "Strong Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_STRONG_ACCEL,
                step=0.1,
                key="b_strong_accel",
            )
        if st.button("Refresh data", key="b_refresh"):
            _get_cached_fetch(st).clear()

    # Non-blocking warning: with strong_accel < accel the regime-label
    # filter silently becomes rvol >= strong_accel — surface it but
    # still render the tab.
    # justified: at most one warning string for this tab's pair; a
    # fixed tiny set, not data iteration.
    for msg in find_threshold_inversions(
        accel=accel, strong_accel=strong_accel
    ):
        st.warning(msg)

    try:
        sql = build_accel_sql(
            lookback=lookback,
            accel=accel,
            strong_accel=strong_accel,
            top_k=top_k,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = cached_fetch_dataframe(st, sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "rvol" in df.columns:
        _render_ordered_bar_chart(st, df, "rvol")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_B_SHEET),
        file_name=_TAB_B_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="b_download",
    )
    with st.expander("Interpretation guide"):
        st.markdown(DAILY_ACCEL_GUIDE)


def _render_weekly_combined_tab(st, config: PipelineConfig) -> None:
    """Weekly — Combined Regimes: all four non-Normal weekly volume
    regimes among the top-K most-liquid names, regime-priority
    ordering (Strong Acceleration first, Deceleration last)."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback weeks",
                min_value=2,
                max_value=260,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="wc_lookback",
            )
            top_k = st.number_input(
                "Liquidity top-K",
                min_value=1,
                max_value=2000,
                value=DEFAULT_TOP_K,
                step=1,
                key="wc_top_k",
            )
        with col2:
            strong_accel = st.number_input(
                "Strong Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_STRONG_ACCEL,
                step=0.1,
                key="wc_strong_accel",
            )
            accel = st.number_input(
                "Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_ACCEL,
                step=0.1,
                key="wc_accel",
            )
        with col3:
            decel = st.number_input(
                "Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_WEEKLY_DECEL,
                step=0.1,
                key="wc_decel",
            )
            strong_decel = st.number_input(
                "Strong Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_WEEKLY_STRONG_DECEL,
                step=0.1,
                key="wc_strong_decel",
            )
        if st.button("Refresh data", key="wc_refresh"):
            _get_cached_fetch(st).clear()

    # Non-blocking warning: an inverted pair silently mislabels rows
    # (the CASE checks the strong bound first), so surface it but still
    # render the tab.
    # justified: at most two warning strings — one per inverted
    # threshold pair; a fixed tiny set, not data iteration.
    for msg in find_threshold_inversions(
        accel=accel,
        strong_accel=strong_accel,
        decel=decel,
        strong_decel=strong_decel,
    ):
        st.warning(msg)

    try:
        sql = build_weekly_combined_sql(
            lookback=lookback,
            top_k=top_k,
            strong_accel=strong_accel,
            accel=accel,
            decel=decel,
            strong_decel=strong_decel,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = cached_fetch_dataframe(st, sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "rvol" in df.columns:
        _render_ordered_bar_chart(st, df, "rvol")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_WC_SHEET),
        file_name=_TAB_WC_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="wc_download",
    )
    with st.expander("Interpretation guide"):
        st.markdown(WEEKLY_COMBINED_GUIDE)


def _render_weekly_accel_tab(st, config: PipelineConfig) -> None:
    """Weekly — Acceleration: weekly acceleration candidates among the
    top-K most-liquid names, ordered by rvol descending."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback weeks",
                min_value=2,
                max_value=260,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="wa_lookback",
            )
            top_k = st.number_input(
                "Liquidity top-K",
                min_value=1,
                max_value=2000,
                value=DEFAULT_TOP_K,
                step=1,
                key="wa_top_k",
            )
        with col2:
            accel = st.number_input(
                "Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_ACCEL,
                step=0.1,
                key="wa_accel",
            )
        with col3:
            strong_accel = st.number_input(
                "Strong Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_STRONG_ACCEL,
                step=0.1,
                key="wa_strong_accel",
            )
        if st.button("Refresh data", key="wa_refresh"):
            _get_cached_fetch(st).clear()

    # Non-blocking warning: with strong_accel < accel the regime-label
    # filter silently becomes rvol >= strong_accel — surface it but
    # still render the tab.
    # justified: at most one warning string for this tab's pair; a
    # fixed tiny set, not data iteration.
    for msg in find_threshold_inversions(
        accel=accel, strong_accel=strong_accel
    ):
        st.warning(msg)

    try:
        sql = build_weekly_accel_sql(
            lookback=lookback,
            accel=accel,
            strong_accel=strong_accel,
            top_k=top_k,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = cached_fetch_dataframe(st, sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "rvol" in df.columns:
        _render_ordered_bar_chart(st, df, "rvol")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_WA_SHEET),
        file_name=_TAB_WA_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="wa_download",
    )
    with st.expander("Interpretation guide"):
        st.markdown(WEEKLY_ACCEL_GUIDE)


def _render_weekly_decel_tab(st, config: PipelineConfig) -> None:
    """Weekly — Deceleration: weekly deceleration candidates among the
    top-K most-liquid names, ordered by rvol ascending (quietest
    names first).  Weekly decel defaults are 0.50 / 0.70 — tighter
    than the daily 0.6 / 0.8."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback weeks",
                min_value=2,
                max_value=260,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="wd_lookback",
            )
            top_k = st.number_input(
                "Liquidity top-K",
                min_value=1,
                max_value=2000,
                value=DEFAULT_TOP_K,
                step=1,
                key="wd_top_k",
            )
        with col2:
            decel = st.number_input(
                "Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_WEEKLY_DECEL,
                step=0.1,
                key="wd_decel",
            )
        with col3:
            strong_decel = st.number_input(
                "Strong Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_WEEKLY_STRONG_DECEL,
                step=0.1,
                key="wd_strong_decel",
            )
        if st.button("Refresh data", key="wd_refresh"):
            _get_cached_fetch(st).clear()

    # Non-blocking warning: with strong_decel > decel the regime-label
    # filter silently becomes rvol <= strong_decel — surface it but
    # still render the tab.
    # justified: at most one warning string for this tab's pair; a
    # fixed tiny set, not data iteration.
    for msg in find_threshold_inversions(
        decel=decel, strong_decel=strong_decel
    ):
        st.warning(msg)

    try:
        sql = build_weekly_decel_sql(
            lookback=lookback,
            decel=decel,
            strong_decel=strong_decel,
            top_k=top_k,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = cached_fetch_dataframe(st, sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "rvol" in df.columns:
        _render_ordered_bar_chart(st, df, "rvol")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_WD_SHEET),
        file_name=_TAB_WD_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="wd_download",
    )
    with st.expander("Interpretation guide"):
        st.markdown(WEEKLY_DECEL_GUIDE)


def main() -> None:
    """Streamlit entrypoint — run via ``streamlit run scripts/report_app.py``."""
    import streamlit as st

    st.set_page_config(page_title="Stock Reports", layout="wide")
    st.title("Stock Reports")
    with st.expander("About / How to read these reports"):
        st.markdown(BASIC_GUIDE)

    try:
        config = PipelineConfig.from_yaml(_CONFIG_PATH)
    except Exception as exc:
        st.error(f"Failed to load configuration ({_CONFIG_PATH}): {exc}")
        return

    (
        tab_weekly_combined,
        tab_weekly_accel,
        tab_weekly_decel,
        tab_top25,
        tab_accel,
    ) = st.tabs(
        [
            "Weekly — Combined Regimes",
            "Weekly — Acceleration",
            "Weekly — Deceleration",
            "Daily — Top 25 by Dollar Volume",
            "Daily — Acceleration Candidates",
        ]
    )
    with tab_weekly_combined:
        _render_weekly_combined_tab(st, config)
    with tab_weekly_accel:
        _render_weekly_accel_tab(st, config)
    with tab_weekly_decel:
        _render_weekly_decel_tab(st, config)
    with tab_top25:
        _render_top25_tab(st, config)
    with tab_accel:
        _render_accel_tab(st, config)


# ``streamlit run`` executes this file as __main__, while unit tests
# import it as a plain module — the guard keeps imports side-effect
# free (no widgets rendered, no data fetched during test collection).
if __name__ == "__main__":
    main()
