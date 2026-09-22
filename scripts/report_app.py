"""Interactive Streamlit report app with nine tabs — the three weekly
reports first, then the two daily ones, then Market Breadth, then
Relative Rotation (RRG), then Fear & Greed, then Correlation:

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
* **"Market Breadth"**: the 5-panel market breadth dashboard ported
  from the legacy ``breadth_builder02.py`` (SQL, EMA seeding,
  indicator formulas, figure construction and interpretation guide
  live verbatim in ``scripts/breadth_dashboard.py``) — SPY
  candlestick + 50-day EMA, % above SMA50/SMA200, net advances &
  volume ratio, and new highs vs. new lows.  ``plotly`` is imported
  lazily inside the tab path so this module stays importable without
  it.  The trailing-years widget defaults to
  ``reports.market_breadth_years`` (0 = all data).
* **"Relative Rotation (RRG)"**: the animated Relative Rotation Graph
  dashboard ported from the legacy ``plot_rrg_chart_gemini10b.py``
  (RRG presets, weekly_close query, RS-Ratio/RS-Momentum math, spline
  trails and the LLM analysis-prompt JSON live verbatim in
  ``scripts/rrg_dashboard.py``, lazily imported — it in turn
  lazy-imports ``plotly``/``scipy``).  Reads the persisted
  ``weekly_close`` table.  A preset ``st.selectbox`` picks one of the
  seven configurations (six legacy + the ``Mag7`` project addition)
  or a ``Custom`` ad-hoc ticker list (free-text comma-separated
  symbols, regex-validated before they reach the query); period /
  trail / lookback-years / benchmark widgets default to the preset's
  values (period=14 / trail=3 / 3 years / SPY for Custom).
* **"Fear & Greed"**: the CNN Fear & Greed dashboard over the
  ``fear_greed_history`` table (F18-T1 — strict MVP of the user's
  dashboard proposal).  All SQL / window slicing / figure construction
  lives in ``scripts/fear_greed_dashboard.py`` (lazily imported — it in
  turn lazy-imports ``plotly``): a headline 0-100 gauge with the five
  rating bands and a delta vs the previous trading day, an
  ``fg_score`` history line over five semi-transparent rating-band
  zones (optional MA20/MA50 overlay plus an optional SPY close overlay
  on a secondary right-side axis, inner-joined onto the F&G trading
  days), and a rating heatmap covering
  the nine ``*_rating`` sub-indicator columns (ordinal-mapped cells,
  raw value + rating in the hover).  A window radio slices the frame
  post-query in pandas — ``1M/3M/6M/YTD/1Y/2Y/Max``, default ``1Y``;
  ``YTD`` anchors to Jan 1 of the latest year present *in the data*.
* **"Correlation"**: the multi-asset correlation dashboard from
  ``docs/enhancements/20260922a.md`` over the existing ``daily_close``
  table (F22-T1) — five series (^TNX 10Y yield, CL=F WTI crude, ^IRX
  13-week T-bill proxy, SPY, SMH) pivoted to a wide Date x label frame
  and inner-aligned via ``dropna`` (CL=F trades on equity holidays).
  All SQL / pandas shaping / figure construction lives in
  ``scripts/correlation_dashboard.py`` (lazily imported — it in turn
  lazy-imports ``plotly``): side-by-side levels vs returns correlation
  heatmaps, a base-100 normalized overlay, a three-row multi-panel
  overlay (WTI vs TNX / SPY+SMH vs TNX / IRX+TNX), rolling-window
  return correlations with a dashed y=0 line, and a scatter with OLS
  trendline + Pearson corr in the title.  Each chart is followed by a
  "How to read this chart" expander rendering the matching
  ``correlation_dashboard.CORR_GUIDE`` entry; a data-table expander
  closes the tab with the wide frame + a CSV download button.  A
  window radio slices post-query (``1M/3M/6M/YTD/1Y/2Y/Max``, default
  ``Max``); configured tickers absent from ``daily_close`` are dropped
  with a warning — never a crash.

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
``DAILY_*_GUIDE`` for the daily ones, and
``fear_greed_dashboard.FEAR_GREED_GUIDE`` — adapted from
``docs/enhancements/20260920b.md`` — for the Fear & Greed tab; the
Correlation tab instead renders a per-chart
``correlation_dashboard.CORR_GUIDE`` entry in a "How to read this
chart" expander after each of its five charts).

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


def _render_market_breadth_tab(st, config: PipelineConfig) -> None:
    """Market Breadth — the 5-panel breadth dashboard ported from the
    legacy ``breadth_builder02.py``.

    All SQL / indicator math / figure construction lives in
    ``scripts/breadth_dashboard.py`` (imported lazily — it in turn
    lazy-imports ``plotly`` inside ``build_breadth_figure``, so this
    module stays importable without plotly).  Both queries go through
    :func:`cached_fetch_dataframe` like every other tab; the
    trailing-years cutoff is applied *post-query* in pandas because
    the SMA_200 / 252-day lookback windows need pre-cutoff history.
    """
    import breadth_dashboard

    with st.expander("Parameters", expanded=True):
        years = st.number_input(
            "Years of history (0 = all data)",
            min_value=0,
            max_value=30,
            value=config.reports.market_breadth_years,
            step=1,
            key="mb_years",
        )
        if st.button("Refresh data", key="mb_refresh"):
            _get_cached_fetch(st).clear()

    breadth_df = cached_fetch_dataframe(
        st, breadth_dashboard.build_breadth_sql(), config
    )
    if breadth_df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return
    spy_df = cached_fetch_dataframe(
        st, breadth_dashboard.build_spy_sql(), config
    )
    if spy_df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(
        f"{len(breadth_df)} breadth row(s), {len(spy_df)} SPY row(s)"
    )

    # Same guards as the legacy generator.
    if breadth_df.empty:
        st.error(
            "No breadth data returned from query. Check table name "
            "and columns."
        )
        return
    if spy_df.empty:
        st.error(
            "No SPY data found. Ensure 'SPY' ticker exists in "
            "daily_close table."
        )
        return

    breadth_df, spy_df = breadth_dashboard.filter_years(
        breadth_df, spy_df, int(years)
    )
    spy_df["EMA_50"] = breadth_dashboard.calculate_ema(
        spy_df["Close"], 50
    )
    breadth_df = breadth_dashboard.compute_indicators(breadth_df)
    fig = breadth_dashboard.build_breadth_figure(
        breadth_df, spy_df, source=config.database.duckdb_filename
    )
    st.plotly_chart(fig, width="stretch")
    with st.expander("Interpretation guide"):
        # The verbatim guide is an HTML grid — st.html embeds it
        # directly; fall back to unsafe markdown on older streamlit.
        if hasattr(st, "html"):
            st.html(breadth_dashboard.BREADTH_GUIDE_HTML)
        else:
            st.markdown(
                breadth_dashboard.BREADTH_GUIDE_HTML,
                unsafe_allow_html=True,
            )


def _render_rrg_tab(st, config: PipelineConfig) -> None:
    """Relative Rotation (RRG) — the animated RRG dashboard ported
    from the legacy ``plot_rrg_chart_gemini10b.py``.

    All presets / SQL / RRG math / figure construction / LLM payload
    building lives in ``scripts/rrg_dashboard.py`` (imported lazily —
    it in turn lazy-imports ``plotly`` and ``scipy``, so this module
    stays importable without them).  The query goes through
    :func:`cached_fetch_dataframe` like every other tab and reads the
    persisted ``weekly_close`` table.

    The preset selectbox offers the seven configured universes plus a
    ``Custom`` ad-hoc option (free-text comma-separated tickers; the
    widgets fall back to period=14 / trail=3 / lookback=3y /
    benchmark=SPY defaults).  Every symbol — preset, custom or
    benchmark — is regex-validated inside ``build_rrg_sql`` before it
    reaches the query text, and requested tickers absent from the
    fetched data are dropped with an ``st.warning``.
    """
    import rrg_dashboard

    with st.expander("Parameters", expanded=True):
        preset_name = st.selectbox(
            "RRG preset",
            # "Custom" is LAST so the presets keep their order.
            options=list(rrg_dashboard.RRG_PRESETS) + ["Custom"],
            index=0,
            key="rrg_preset",
        )
        # ``None`` means the ad-hoc "Custom" selection — the widgets
        # fall back to the shared defaults and a free-text input
        # supplies the ticker list.
        preset = rrg_dashboard.RRG_PRESETS.get(preset_name)
        defaults = preset or {
            "period": 14,
            "trail": 3,
            "lookback_years": 3,
            "benchmark": "SPY",
        }
        col1, col2 = st.columns(2)
        with col1:
            period = st.number_input(
                "RRG period",
                min_value=1,
                max_value=104,
                value=int(defaults["period"]),
                step=1,
                key="rrg_period",
            )
            trail = st.number_input(
                "Trail length (weeks)",
                min_value=1,
                max_value=52,
                value=int(defaults["trail"]),
                step=1,
                key="rrg_trail",
            )
        with col2:
            lookback_years = st.number_input(
                "Lookback years",
                min_value=1,
                max_value=30,
                value=int(defaults["lookback_years"]),
                step=1,
                key="rrg_years",
            )
            benchmark = st.text_input(
                "Benchmark ticker",
                value=defaults["benchmark"],
                key="rrg_benchmark",
            )
        custom_raw = ""
        if preset is None:
            custom_raw = st.text_input(
                "Tickers (comma-separated)",
                placeholder="AAPL,MSFT,NVDA",
                key="rrg_custom_tickers",
                help=(
                    "Free-text ad-hoc universe — symbols are "
                    "uppercased, deduplicated and regex-validated "
                    "(letters, digits, '.' and '-' only) before they "
                    "reach the query."
                ),
            )
        if st.button("Refresh data", key="rrg_refresh"):
            _get_cached_fetch(st).clear()

    if preset is not None:
        tickers = preset["tickers"].split(",")
    else:
        # Custom: split on commas -> strip -> uppercase -> drop
        # empties -> dedupe preserving first-seen order
        # (dict.fromkeys keeps insertion order).
        tickers = list(
            dict.fromkeys(
                t
                for t in (s.strip().upper() for s in custom_raw.split(","))
                if t
            )
        )
        if not tickers:
            st.error(
                "Enter at least one ticker (comma-separated, e.g. "
                "AAPL,MSFT,NVDA) to run a Custom RRG."
            )
            return

    try:
        sql = rrg_dashboard.build_rrg_sql(
            tickers, benchmark, int(lookback_years)
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
    if df.empty:
        st.error(
            "No weekly_close data returned for this selection — check "
            "the tickers/benchmark and that the weekly layer has been "
            "built (weekly_refresh action or scripts/refresh_weekly.py)."
        )
        return

    df_pivot = rrg_dashboard.pivot_closes(df)
    try:
        rrg = rrg_dashboard.compute_rrg(
            df_pivot, tickers, benchmark, int(period)
        )
    except ValueError as exc:
        st.error(str(exc))
        return
    # Surface the library's printed drop-warning in the UI: requested
    # tickers absent from the fetched data do not survive compute_rrg.
    dropped = [t for t in tickers if t not in rrg["tickers"]]
    if dropped:
        st.warning(
            f"{len(dropped)} requested ticker(s) absent from the "
            f"fetched weekly_close data — dropped: "
            f"{', '.join(dropped)}"
        )
    if len(rrg["dates"]) <= int(trail):
        st.error(
            f"Only {len(rrg['dates'])} fully-valid weekly bar(s) "
            f"survive the RRG warmup — the trail length "
            f"({int(trail)}) plus the warmup need more history "
            f"(increase lookback years or reduce period/trail)."
        )
        return

    fig = rrg_dashboard.build_rrg_figure(
        rrg["clean_ratio"],
        rrg["clean_momo"],
        df_pivot,
        rrg["tickers"],
        benchmark,
        int(trail),
    )
    st.plotly_chart(fig, width="stretch")

    # Latest-position stats — same table the figure's stats row shows
    # (Ticker / Ratio / Momentum / Velocity, sorted by Ratio desc).
    v_vec = (
        (rrg["clean_ratio"].iloc[-1] - rrg["clean_ratio"].iloc[-2]) ** 2
        + (rrg["clean_momo"].iloc[-1] - rrg["clean_momo"].iloc[-2]) ** 2
    ) ** 0.5
    stats = pd.DataFrame(
        {
            "Ticker": rrg["tickers"],
            "RS-Ratio": rrg["clean_ratio"].iloc[-1].values,
            "RS-Momentum": rrg["clean_momo"].iloc[-1].values,
            "Velocity": v_vec.values,
        }
    ).sort_values("RS-Ratio", ascending=False)
    st.dataframe(stats, width="stretch")

    # LLM analysis-prompt payload — same JSON the standalone script
    # writes to {prefix}_analysis_prompt.json.
    payload = rrg_dashboard.gen_ai_prompt(
        rrg["clean_ratio"], rrg["clean_momo"], benchmark
    )
    with st.expander("LLM analysis prompt (JSON payload)"):
        st.json(payload)
    st.download_button(
        label="Download analysis prompt (JSON)",
        data=payload,
        file_name=(
            f"{preset['prefix']}_analysis_prompt.json"
            if preset is not None
            else "RRG_Custom_analysis_prompt.json"
        ),
        mime="application/json",
        key="rrg_download",
    )


def _render_fear_greed_tab(st, config: PipelineConfig) -> None:
    """Fear & Greed — headline gauge, band-shaded ``fg_score`` history
    and a sub-indicator rating heatmap over ``fear_greed_history``
    (F18-T1 — strict MVP of the user's dashboard proposal).

    All SQL / window slicing / figure construction lives in
    ``scripts/fear_greed_dashboard.py`` (imported lazily — it in turn
    lazy-imports ``plotly`` inside the figure builders, so this module
    stays importable without plotly).  Both queries — the F&G history
    and the SPY closes for the history chart's secondary-axis overlay
    (F19-T1) — go through :func:`cached_fetch_dataframe` like every
    other tab; the window preset slices the fetched frames post-query
    in pandas via ``filter_window`` / a date-bounds slice.  A failed or
    empty SPY fetch degrades to a caption — it never breaks the tab.
    """
    import fear_greed_dashboard

    with st.expander("Parameters", expanded=True):
        presets = list(fear_greed_dashboard.WINDOW_PRESETS)
        window = st.radio(
            "Window",
            options=presets,
            index=presets.index(fear_greed_dashboard.DEFAULT_WINDOW),
            horizontal=True,
            key="fg_window",
        )
        if st.button("Refresh data", key="fg_refresh"):
            _get_cached_fetch(st).clear()

    df = cached_fetch_dataframe(
        st, fear_greed_dashboard.build_fear_greed_sql(), config
    )
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return
    if df.empty:
        st.error(
            "No fear_greed_history data returned — the table is empty "
            "or has not been loaded yet (run the fear_greed_refresh "
            "Flight action or scripts/refresh_fear_greed.py)."
        )
        return

    df = fear_greed_dashboard.normalize_dates(df)

    # Second fetch — SPY closes from the earliest F&G date onward, so
    # the frame covers the "Max" window and every preset can slice it
    # post-query (fetched BEFORE the window slice below).  A failed or
    # empty SPY fetch degrades to a caption near the history chart.
    spy_df = cached_fetch_dataframe(
        st,
        fear_greed_dashboard.build_spy_sql(df["date"].min()),
        config,
    )

    df = fear_greed_dashboard.filter_window(df, window)
    if df.empty:
        st.error(
            f"No fear_greed_history rows inside the {window} window — "
            "pick a wider preset."
        )
        return

    st.caption(f"{len(df)} row(s)")

    # Headline gauge — delta vs the previous trading day (the same row
    # when the window holds a single bar, i.e. delta 0).
    latest = df.iloc[-1]
    latest_score = float(latest["fg_score"])
    prev_score = (
        float(df.iloc[-2]["fg_score"]) if len(df) >= 2 else latest_score
    )
    st.plotly_chart(
        fear_greed_dashboard.build_gauge_figure(
            latest_score, prev_score, latest["fg_rating"]
        ),
        width="stretch",
    )

    # Change caption — 1d/5d/21d deltas, each only shown when the
    # windowed history is long enough to contain that lookback.
    parts = [f"As of {latest['date'].date()}"]
    if len(df) >= 2:
        parts.append(f"1d {latest_score - prev_score:+.1f}")
    if len(df) >= 6:
        parts.append(
            f"5d {latest_score - float(df.iloc[-6]['fg_score']):+.1f}"
        )
    if len(df) >= 22:
        parts.append(
            f"21d {latest_score - float(df.iloc[-22]['fg_score']):+.1f}"
        )
    st.caption(" · ".join(parts))

    show_ma = st.checkbox("Show MA20/MA50", value=False, key="fg_ma")
    overlay_spy = st.checkbox(
        "Overlay SPY price", value=True, key="fg_spy"
    )
    spy_window = None
    if (
        spy_df is None
        or spy_df.empty
        or not {"Date", "Close"} <= set(spy_df.columns)
    ):
        st.caption(
            "SPY overlay unavailable — no SPY daily_close rows "
            "returned for the Fear & Greed date range."
        )
    elif overlay_spy:
        # Window-slice SPY to the history chart's window (the fetch
        # covered the full F&G range).  ``Date`` arrives object-dtype
        # via Flight — normalize before the bounds comparison.
        spy_window = spy_df.copy()
        spy_window["Date"] = pd.to_datetime(spy_window["Date"])
        spy_window = spy_window[
            (spy_window["Date"] >= df["date"].min())
            & (spy_window["Date"] <= df["date"].max())
        ]
    st.plotly_chart(
        fear_greed_dashboard.build_history_figure(
            df, show_ma, spy_df=spy_window
        ),
        width="stretch",
    )
    st.plotly_chart(
        fear_greed_dashboard.build_heatmap_figure(df), width="stretch"
    )
    with st.expander("Interpretation guide"):
        st.markdown(fear_greed_dashboard.FEAR_GREED_GUIDE)


def _render_correlation_tab(st, config: PipelineConfig) -> None:
    """Correlation — the multi-asset correlation dashboard over
    ``daily_close`` (F22-T1 — the five panels of
    ``docs/enhancements/20260922a.md``).

    All SQL / wide-frame shaping / figure construction lives in
    ``scripts/correlation_dashboard.py`` (imported lazily — it in turn
    lazy-imports ``plotly`` inside the figure builders, so this module
    stays importable without plotly).  The single
    (Date, Ticker, Close) query goes through
    :func:`cached_fetch_dataframe` like every other tab; the fetched
    frame is pivoted to a Date x display-label wide frame and
    ``dropna`` inner-aligned (CL=F trades on equity-market holidays),
    then window-sliced post-query in pandas via ``filter_window``.

    The long frame is filtered to the SELECTED tickers before
    coverage/alignment (F23-T2 — challenger n1): ``to_wide``'s
    ``dropna`` inner-aligns over every column it is given, so
    aligning over all five fetched series would let a DESELECTED
    short-history ticker silently collapse the window.  A configured
    asset whose column is absent from the wide frame
    (e.g. ``^TNX`` not yet loaded) is dropped with a visible
    ``st.warning`` naming the missing labels — the survivors still
    render, never a crash.  A second ``st.warning`` fires when the
    selected series' pre-alignment coverage differs (per
    :func:`correlation_dashboard.series_coverage` on the
    selected-only long frame), naming the limiting series and its
    range.  The "Returns basis"
    radio (Daily | Log returns) drives the right-hand heatmap, the
    rolling chart and the scatter; the left heatmap is always Levels.
    Sections with fewer than two usable aligned rows render
    ``st.info`` instead of a figure — the guide expanders still render.
    """
    import correlation_dashboard as corr_dash

    all_labels = list(corr_dash.CORR_TICKERS.values())
    pair_options = [
        corr_dash.pair_label(p) for p in corr_dash.DEFAULT_ROLLING_PAIRS
    ]

    with st.expander("Parameters", expanded=True):
        presets = list(corr_dash.WINDOW_PRESETS)
        window = st.radio(
            "Window",
            options=presets,
            index=presets.index(corr_dash.DEFAULT_WINDOW),
            horizontal=True,
            key="corr_window",
        )
        roll_window = st.number_input(
            "Rolling window (trading days)",
            min_value=corr_dash.ROLLING_WINDOW_MIN,
            max_value=corr_dash.ROLLING_WINDOW_MAX,
            value=corr_dash.DEFAULT_ROLLING_WINDOW,
            step=1,
            key="corr_rollwin",
        )
        ret_basis = st.radio(
            "Returns basis",
            options=list(corr_dash.RET_BASIS_OPTIONS),
            index=list(corr_dash.RET_BASIS_OPTIONS).index(
                corr_dash.DEFAULT_RET_BASIS
            ),
            horizontal=True,
            key="corr_retbasis",
        )
        assets = st.multiselect(
            "Assets",
            options=all_labels,
            default=all_labels,
            key="corr_assets",
        )
        selected_pair_labels = st.multiselect(
            "Rolling-correlation pairs",
            options=pair_options,
            default=pair_options,
            key="corr_pairs",
        )
        scatter_x = st.selectbox(
            "Scatter X",
            options=all_labels,
            index=all_labels.index("WTI") if "WTI" in all_labels else 0,
            key="corr_scatter_x",
        )
        scatter_y = st.selectbox(
            "Scatter Y",
            options=all_labels,
            index=(
                all_labels.index("TNX_10Y")
                if "TNX_10Y" in all_labels
                else 0
            ),
            key="corr_scatter_y",
        )
        if st.button("Refresh data", key="corr_refresh"):
            _get_cached_fetch(st).clear()

    # Min-2-assets guard — a correlation matrix needs at least two
    # series; warn and bail before hitting the database.
    if len(assets) < 2:
        st.warning(
            "Select at least two assets — a correlation needs a pair."
        )
        return

    df = cached_fetch_dataframe(st, corr_dash.build_corr_sql(), config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return
    if df.empty:
        st.error(
            "No daily_close data returned for the correlation tickers "
            "(^TNX, CL=F, ^IRX, SPY, SMH) — the table is empty or the "
            "macro series have not been loaded yet."
        )
        return

    # Restrict the long frame to the SELECTED tickers BEFORE the
    # coverage check and to_wide's dropna inner-alignment (F23-T2 —
    # challenger n1): aligning over every fetched series would let a
    # deselected short-history ticker collapse the aligned window
    # anyway, and the coverage warning would name series that are not
    # even plotted.  Labels map back to raw tickers via the inverse of
    # CORR_TICKERS; an unknown label simply contributes no ticker and
    # is reported by the missing-column warning below.
    label_to_ticker = {l: t for t, l in corr_dash.CORR_TICKERS.items()}
    selected_raw = [
        label_to_ticker[l] for l in assets if l in label_to_ticker
    ]
    df = df[df["Ticker"].isin(selected_raw)]

    # Coverage check BEFORE to_wide's dropna inner-alignment: if the
    # selected series do not share the same (first, last) valid range,
    # warn and name the limiting (shortest-span) series — the aligned
    # window is silently shortened to its coverage otherwise.
    coverage = corr_dash.series_coverage(df)
    sel_cov = {l: r for l, r in coverage.items() if l in assets}
    if len(set(sel_cov.values())) > 1:
        limit_label, (cov_first, cov_last) = min(
            sel_cov.items(), key=lambda kv: kv[1][1] - kv[1][0]
        )
        st.warning(
            f"{limit_label} only covers "
            f"{cov_first:%Y-%m-%d} → {cov_last:%Y-%m-%d} — "
            "aligned window shortened."
        )

    wide = corr_dash.to_wide(df)

    # Drop configured-but-absent series with a visible warning (a
    # ticker that has not been backfilled yet must not crash the tab).
    missing_selected = [l for l in assets if l not in wide.columns]
    if missing_selected:
        st.warning(
            "No daily_close rows for: "
            + ", ".join(missing_selected)
            + " — excluded from every chart (run a backfill to load "
            "them)."
        )
    selected = [l for l in assets if l in wide.columns]
    if len(selected) < 2:
        st.warning(
            "Fewer than two selected assets have data — nothing to "
            "correlate yet."
        )
        return
    wide = wide[selected]

    wide = corr_dash.filter_window(wide, window)
    if wide.empty:
        st.error(
            f"No aligned correlation rows inside the {window} window "
            "— pick a wider preset."
        )
        return

    st.caption(
        f"{len(wide)} aligned trading day(s) · {len(selected)} series"
    )
    ret_kind = corr_dash.RET_BASIS_TO_KIND.get(ret_basis, "daily")

    def _guide(key: str) -> None:
        with st.expander("How to read this chart"):
            st.markdown(corr_dash.CORR_GUIDE[key])

    # 1) Heatmaps — levels (left, always) vs the chosen returns basis
    # (right).  None -> insufficient aligned rows -> st.info.
    heatmaps = corr_dash.build_heatmap_figures(wide, ret_kind)
    if heatmaps is None:
        st.info(
            "Insufficient aligned data for the correlation heatmaps "
            "— need at least two aligned rows."
        )
    else:
        levels_fig, returns_fig = heatmaps
        col_levels, col_returns = st.columns(2)
        with col_levels:
            st.plotly_chart(levels_fig, width="stretch")
        with col_returns:
            st.plotly_chart(returns_fig, width="stretch")
    _guide("heatmap")

    # 2) Normalized base=100 overlay.
    st.plotly_chart(
        corr_dash.build_normalized_figure(wide), width="stretch"
    )
    _guide("normalized")

    # 3) Multi-panel overlay (dual-axis rows + shared-% row).
    st.plotly_chart(
        corr_dash.build_multipanel_figure(wide), width="stretch"
    )
    _guide("multipanel")

    # 4) Rolling correlations of the chosen returns basis for the
    # chosen pairs.  The effective window is clamped to the available
    # returns history — caption it when the clamp engaged.
    pairs = [
        corr_dash.parse_pair_label(text)
        for text in selected_pair_labels
    ]
    pairs = [
        (a, b)
        for a, b in pairs
        if a in wide.columns and b in wide.columns
    ]
    if pairs:
        rolling_fig, effective_window = corr_dash.build_rolling_figure(
            wide, roll_window, pairs, ret_kind
        )
        if effective_window < int(roll_window):
            st.caption(
                f"Rolling window clamped to {effective_window} "
                f"trading days — only {len(wide)} aligned rows in "
                "the selected window."
            )
        st.plotly_chart(rolling_fig, width="stretch")
    else:
        st.info(
            "No rolling-correlation pairs selected with data — pick "
            "at least one pair."
        )
    _guide("rolling")

    # 5) Scatter + OLS trendline for the chosen pair on the chosen
    # returns basis.  None -> insufficient aligned rows -> st.info.
    if scatter_x in wide.columns and scatter_y in wide.columns:
        scatter_fig = corr_dash.build_scatter_figure(
            wide, scatter_x, scatter_y, ret_kind
        )
        if scatter_fig is None:
            st.info(
                "Insufficient aligned data for the scatter — need "
                "at least two aligned rows for the pair."
            )
        else:
            st.plotly_chart(scatter_fig, width="stretch")
    else:
        st.info(
            "The selected scatter pair is not both loaded — pick two "
            "series with data."
        )
    _guide("scatter")

    # Data table + CSV download of the windowed wide frame.
    with st.expander("Data table (aligned window)"):
        st.dataframe(wide, width="stretch")
        st.download_button(
            label="Download as CSV",
            data=wide.to_csv().encode("utf-8"),
            file_name="correlation_window.csv",
            mime="text/csv",
            key="corr_download",
        )


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
        tab_breadth,
        tab_rrg,
        tab_fear_greed,
        tab_correlation,
    ) = st.tabs(
        [
            "Weekly — Combined Regimes",
            "Weekly — Acceleration",
            "Weekly — Deceleration",
            "Daily — Top 25 by Dollar Volume",
            "Daily — Acceleration Candidates",
            "Market Breadth",
            "Relative Rotation (RRG)",
            "Fear & Greed",
            "Correlation",
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
    with tab_breadth:
        _render_market_breadth_tab(st, config)
    with tab_rrg:
        _render_rrg_tab(st, config)
    with tab_fear_greed:
        _render_fear_greed_tab(st, config)
    with tab_correlation:
        _render_correlation_tab(st, config)


# ``streamlit run`` executes this file as __main__, while unit tests
# import it as a plain module — the guard keeps imports side-effect
# free (no widgets rendered, no data fetched during test collection).
if __name__ == "__main__":
    main()
