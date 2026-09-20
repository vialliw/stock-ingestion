"""
=============================================================================
FEAR & GREED DASHBOARD — report_app tab module (F18-T1)
=============================================================================

All SQL / pandas shaping / figure construction for the "Fear & Greed"
tab in ``scripts/report_app.py`` lives here — same layering as
``scripts/breadth_dashboard.py`` and ``scripts/rrg_dashboard.py``:
``report_app`` imports this module lazily inside
``_render_fear_greed_tab`` and this module in turn lazy-imports
``plotly.graph_objects`` inside the figure builders, so both modules
stay importable without plotly/streamlit.

The data source is the ``fear_greed_history`` table (F16-T1 — CNN Fear
& Greed history written by the ``fear_greed_refresh`` Flight action or
``scripts/refresh_fear_greed.py``): one row per date with the composite
``fg_score`` (0-100) / ``fg_rating`` plus 9 sub-indicator
``<key> DOUBLE`` / ``<key>_rating VARCHAR`` pairs
(``src/db.py::FEAR_GREED_SUB_INDICATORS``).

The tab is the strict MVP of the user's dashboard proposal:

1. A headline gauge — latest ``fg_score`` on a 0-100 dial with the five
   rating bands as gauge steps and a delta vs the previous trading day.
2. A ``fg_score`` history chart — line + light fill on a fixed 0-100
   domain with five semi-transparent horizontal rating-band zones, an
   optional MA20/MA50 overlay, and an optional SPY close overlay on a
   secondary (right-side) y axis (F19-T1).
3. A rating heatmap — one row per sub-indicator, cells colored by the
   textual rating mapped to a 0-4 ordinal scale, hover showing the raw
   metric value + rating.

Window presets (1M/3M/6M/YTD/1Y/2Y/Max) slice the frame POST-query in
pandas — the whole ~1.5K-row table is fetched once via the shared
``cached_fetch_dataframe`` path (same precedent as
``breadth_dashboard.filter_years``).

Two deliberate chart choices:

* The ``date`` column arrives from Arrow Flight as object-dtype
  ``datetime.date`` (the F13 lesson) — :func:`normalize_dates` runs
  ``pd.to_datetime`` before any Timestamp ops.
* X axes are **category** axes over ``YYYY-MM-DD`` date strings, so the
  axis contains trading days only — no weekend/holiday gaps and no
  interpolation.  Raw sub-indicator metric values are never plotted on
  a numeric axis (their units differ wildly); they only appear in the
  heatmap hover text.

The optional SPY overlay is fed by a second query —
:func:`build_spy_sql` pulls ``Date``/``Close`` rows for ``SPY`` from
``daily_close`` starting at the earliest F&G date, and
:func:`build_history_figure` inner-aligns those closes onto the
figure's trading-day category axis (SPY dates absent from the F&G
window drop; F&G dates without a SPY close render as a gap).

The tab's bottom-of-page "Interpretation guide" expander renders the
module-level ``FEAR_GREED_GUIDE`` markdown constant (F20-T1 — adapted
from ``docs/enhancements/20260920b.md``: the screenshot-specific
example readings removed/genericized, the sub-indicator count fixed
to the nine rows the heatmap actually renders, and a note added for
the optional SPY price-axis overlay).

Version: 1.2 (F20-T1)
=============================================================================
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional, Tuple

# Ensure the repo root (for ``src.db`` constants) is on sys.path
# regardless of the caller's working directory — mirrors the header in
# scripts/breadth_dashboard.py / scripts/rrg_dashboard.py.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

import pandas as pd

from src.db import FEAR_GREED_SUB_INDICATORS

# ------------------------------------------------------------------
# Rating palette / bands — CNN's five labels ordered fear -> greed.
# ------------------------------------------------------------------

# Textual rating -> ordinal for the heatmap's z matrix.  NULL or
# out-of-vocabulary ratings map to ``None`` (a gap cell).
RATING_ORDER: Dict[str, int] = {
    "extreme fear": 0,
    "fear": 1,
    "neutral": 2,
    "greed": 3,
    "extreme greed": 4,
}

# Five-band palette, ordered to match RATING_ORDER (index == ordinal).
RATING_COLORS: List[str] = [
    "#d62728",  # extreme fear — red
    "#ff7f0e",  # fear — orange
    "#7f7f7f",  # neutral — gray
    "#9ed36a",  # greed — light green
    "#2ca02c",  # extreme greed — green
]

# fg_score band edges used by BOTH the gauge steps and the history
# hrect zones — CNN's published breakpoints (25/45/55/75).
BAND_BOUNDS: List[int] = [0, 25, 45, 55, 75, 100]

# The 9 sub-indicator columns, in table order, paired with the display
# label shown on the heatmap's y axis.
INDICATORS: Tuple[Tuple[str, str], ...] = (
    ("stock_price_strength", "Stock Price Strength"),
    ("stock_price_breadth", "Stock Price Breadth"),
    ("market_momentum_sp500", "Momentum (SPX)"),
    ("market_momentum_sp125", "Momentum (125d MA)"),
    ("market_volatility_vix", "Volatility (VIX)"),
    ("market_volatility_vix_50", "VIX 50d MA"),
    ("put_call_options", "Put/Call"),
    ("junk_bond_demand", "Junk Bond Demand"),
    ("safe_haven_demand", "Safe Haven Demand"),
)

# Window presets offered by the tab's radio — "Max" is a passthrough
# (no cutoff).  ``YTD`` anchors to Jan 1 of the latest year present IN
# THE DATA, not the calendar year — a stale table must not produce an
# empty window.
WINDOW_PRESETS: Tuple[str, ...] = ("1M", "3M", "6M", "YTD", "1Y", "2Y", "Max")
DEFAULT_WINDOW = "1Y"

# Calendar-offset equivalents for the month/year presets (trading-day
# slicing happens implicitly — the frame only holds trading days).
_WINDOW_OFFSETS: Dict[str, pd.DateOffset] = {
    "1M": pd.DateOffset(months=1),
    "3M": pd.DateOffset(months=3),
    "6M": pd.DateOffset(months=6),
    "1Y": pd.DateOffset(years=1),
    "2Y": pd.DateOffset(years=2),
}

# Stepped colorscale for the heatmap: with zmin=0/zmax=4 each rating
# ordinal k lands inside the [k/4, (k+1)/4) band, so the five palette
# colors anchor exactly to z = 0..4.
_HEATMAP_COLORSCALE: List[list] = [
    [step / 5.0 + edge, color]
    for step, color in enumerate(RATING_COLORS)
    for edge in (0.0, 0.2)
]

# ------------------------------------------------------------------
# Interpretation guide — rendered in the tab's bottom-of-page
# st.expander("Interpretation guide") via st.markdown (F20-T1), the
# same idiom as the daily/weekly *_GUIDE constants in report_app.
#
# Adapted from docs/enhancements/20260920b.md — NOT verbatim:
#   * every "the screenshot" reference and the embedded example
#     readings were removed or genericized (the app shows live data);
#   * the source says "seven underlying drivers/components" but its
#     own table — and this heatmap — render NINE rows (the two MA
#     companions included), so the count wording says nine;
#   * a bullet was added under the line-chart section for the
#     optional SPY close overlay (F19-T1).
# ------------------------------------------------------------------

FEAR_GREED_GUIDE = """\
### Interpretation Guide: Fear & Greed Dashboard (Three Charts)

This tab shows CNN's Fear & Greed Index and its nine underlying sub-indicators. The three charts work together: the gauge gives the current snapshot, the line chart shows how the overall index has evolved, and the colored strips reveal which components are driving the sentiment.

#### 1. Gauge (Current Fear & Greed Index)
**What it shows**
A semi-circular dial of the latest `fg_score` (0–100).

- **0–25** (dark red): Extreme Fear
- **25–45** (orange): Fear
- **45–55** (gray): Neutral
- **55–75** (light green): Greed
- **75–100** (dark green): Extreme Greed

**Reading the number**
- The score lands in one of the five zones above — e.g. a reading of 29.1 sits firmly in the **Fear** zone.
- The small ▲/▼ delta marker next to the number is the point change versus the previous trading day.

**How to use it**
This is the single best "at-a-glance" number. Readings below ~25 historically coincide with high anxiety and often better medium-term buying opportunities; readings above ~75 signal complacency and elevated risk of pullbacks. The needle and color make the current regime unmistakable.

#### 2. Line Chart (Fear & Greed Score History)
**What it shows**
Daily `fg_score` over the selected window.

- Blue line = the index itself.
- Background horizontal bands use the same five-zone colors as the gauge (green at the top for greed, gray for neutral, darker tones lower for fear).
- The optional MA20/MA50 checkbox overlays moving averages of the index.
- The optional "Overlay SPY price" checkbox adds the SPY close as a dark-orange line on a **separate right-side price axis** — do not compare its price scale against the 0–100 index axis; it is there for shape/timing comparison only.

**How to read it**
- Watch whether the line is rising or falling through the colored zones.
- Prolonged stays in the lower (fear) bands often mark capitulation periods; prolonged stays in the upper (greed) bands mark euphoria.
- Sharp drops usually correspond to market stress events.
- The index line is the only series that legitimately lives on a 0–100 scale; do not compare its absolute level with the raw numbers of the sub-indicators — or with the SPY price on the secondary axis.

**Practical tip**
Use the window presets (1M / 3M / 6M / YTD / 1Y / 2Y / Max) to switch between short-term noise and multi-year context. The axis shows trading days only, so weekends and holidays do not create artificial gaps.

#### 3. Colored Strips (Sub-Indicator Regime Heatmap)
**What it shows**
The nine sub-indicator rows that feed the overall index — the components CNN aggregates, plus the two moving-average companions — each shown as a horizontal color bar across the same time window:

| Row                        | Meaning                                      |
|----------------------------|----------------------------------------------|
| Stock Price Strength       | % of stocks near 52-week highs vs lows      |
| Stock Price Breadth        | Volume-based market breadth (McClellan-style)|
| Momentum (SPX)             | S&P 500 vs its own recent trend             |
| Momentum (125d MA)         | S&P 500 vs its 125-day moving average       |
| Volatility (VIX)           | Current VIX level                           |
| VIX 50d MA                 | VIX's own 50-day average                    |
| Put/Call                   | Equity put/call ratio                       |
| Junk Bond Demand           | High-yield vs investment-grade spread proxy |
| Safe Haven Demand          | Stocks vs bonds relative performance        |

**Color meaning (same palette everywhere)**
- Red / dark red → Extreme Fear or Fear
- Gray → Neutral
- Light green / dark green → Greed or Extreme Greed

**How to interpret**
- When most strips are red/orange, fear is broad-based (classic risk-off).
- When most strips are green, greed is broad-based.
- Mixed colors show internal divergence (e.g., strong momentum but elevated volatility).
- The strips use the CNN rating bands, **not** the raw numeric values — the raw metric value and its rating appear in each cell's hover. This avoids the dangerous mistake of plotting VIX, put/call ratios, and S&P levels on the same axis.

**Key rule**
Never treat the absolute height or shade intensity of these strips as comparable numeric magnitudes. They are categorical regime labels only.

#### Putting the Three Charts Together
1. Start with the **gauge** → "Where are we right now?"
2. Look at the **line chart** → "How did we get here, and is the move accelerating or reversing?"
3. Scan the **strips** → "Is this fear/greed concentrated in one or two drivers, or is it across the board?"

#### Quick Caveats
- The overall score is a composite; individual strips can disagree with it.
- Raw underlying numbers (VIX level, put/call ratio, etc.) live on completely different scales and are intentionally **not** plotted on the line chart — they only appear in the heatmap hover.
- Colors are discrete regime labels, not continuous intensities.
- Data cover trading days only, from mid-2020 onward.

Use the gauge for the headline, the line for context and timing, and the strips for diagnostic detail."""


# ------------------------------------------------------------------
# SQL builders — fetch the whole table once; window-slice in pandas.
# ------------------------------------------------------------------


def build_fear_greed_sql() -> str:
    """SELECT every display column from ``fear_greed_history``.

    Pulls ``date``, ``fg_score``, ``fg_rating`` and each sub-indicator's
    raw-value + rating pair — everything except ``ingested_at`` (an
    audit column, not needed for display).  ~1.5K rows total, so no
    WHERE clause: the window preset is applied post-query by
    :func:`filter_window` (same precedent as
    ``breadth_dashboard.filter_years`` — pre-cutoff rows would also be
    needed if a trailing window ever feeds a warm-up).
    """
    cols = [("date", "fg_score", "fg_rating")] + [
        (key, f"{key}_rating") for key in FEAR_GREED_SUB_INDICATORS
    ]
    select_list = ", ".join(col for pair in cols for col in pair)
    return (
        f"SELECT {select_list} FROM fear_greed_history ORDER BY date"
    )


def build_spy_sql(start_date) -> str:
    """SPY closes for the history chart's secondary-axis overlay.

    ``start_date`` is a ``datetime.date`` / ``Timestamp`` / ISO string
    — the earliest F&G date, so the fetched frame covers the ``Max``
    window and every preset can slice it post-query.  Single-quote
    literal interpolation mirrors the module's established SQL-string
    convention (see :func:`build_fear_greed_sql`); ``start_date`` is
    normalized to ``YYYY-MM-DD`` via ``pd.Timestamp`` before it reaches
    the text.
    """
    start = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    return (
        "SELECT Date, Close FROM daily_close "
        f"WHERE Ticker='SPY' AND Date >= '{start}' "
        "ORDER BY Date"
    )


# ------------------------------------------------------------------
# Pandas shaping helpers.
# ------------------------------------------------------------------


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with ``date`` as datetime64.

    Arrow Flight's ``do_get().to_pandas()`` delivers the DuckDB DATE
    column as object-dtype ``datetime.date`` objects (the F13 dtype
    lesson) — ``pd.to_datetime`` is required before any Timestamp ops
    (``.dt`` accessors, DateOffset subtraction, comparisons).
    """
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    return out


def filter_window(df: pd.DataFrame, preset: str) -> pd.DataFrame:
    """Slice ``df`` to the trailing window named by ``preset``.

    ``df`` must already carry a datetime64 ``date`` column (run
    :func:`normalize_dates` first).  ``Max`` and an empty frame are
    passthroughs.  ``YTD`` keeps rows on/after Jan 1 of the latest year
    present in the data (not the calendar year — a stale table would
    otherwise produce an empty window).  Raises ``ValueError`` on an
    unknown preset.
    """
    if preset not in WINDOW_PRESETS:
        raise ValueError(
            f"Unknown window preset {preset!r} — expected one of "
            f"{WINDOW_PRESETS}"
        )
    if preset == "Max" or df.empty:
        return df.copy()
    max_date = df["date"].max()
    if preset == "YTD":
        cutoff = pd.Timestamp(year=max_date.year, month=1, day=1)
    else:
        cutoff = max_date - _WINDOW_OFFSETS[preset]
    return df[df["date"] >= cutoff].copy()


def rating_ordinal(rating) -> Optional[int]:
    """Map a rating string to its 0-4 ordinal (``RATING_ORDER``).

    ``None``/``NaN``/unknown strings return ``None`` — the heatmap
    renders those cells as gaps rather than miscoloring them.
    """
    if rating is None or (isinstance(rating, float) and pd.isna(rating)):
        return None
    return RATING_ORDER.get(str(rating).strip().lower())


def _category_dates(df: pd.DataFrame) -> List[str]:
    """``YYYY-MM-DD`` strings for a trading-day-only category x axis."""
    return df["date"].dt.strftime("%Y-%m-%d").tolist()


# ------------------------------------------------------------------
# Figure builders — plotly is imported lazily inside each one so this
# module (and therefore report_app) stays importable without plotly.
# ------------------------------------------------------------------


def build_gauge_figure(latest_score, prev_score, rating):
    """Headline 0-100 gauge: score + delta vs the previous trading day.

    The five rating bands render as gauge ``steps`` colored with
    ``RATING_COLORS`` at the ``BAND_BOUNDS`` edges; the rating label
    goes in the title.
    """
    import plotly.graph_objects as go

    steps = [
        {"range": [BAND_BOUNDS[i], BAND_BOUNDS[i + 1]], "color": color}
        for i, color in enumerate(RATING_COLORS)
    ]
    title = "Fear & Greed Index"
    if rating is not None and not (isinstance(rating, float) and pd.isna(rating)):
        title = f"Fear & Greed Index — {str(rating).title()}"
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=float(latest_score),
            delta={"reference": float(prev_score)},
            title={"text": title},
            gauge={
                "axis": {"range": [0, 100]},
                "steps": steps,
                "bar": {"color": "#1f1f1f"},
            },
        )
    )
    fig.update_layout(height=300, margin=dict(l=40, r=40, t=60, b=20))
    return fig


def build_history_figure(
    df: pd.DataFrame,
    show_ma: bool = False,
    spy_df: Optional[pd.DataFrame] = None,
):
    """``fg_score`` history: line + light fill over five band zones.

    The frame must already be window-sliced and ``normalize_dates``-ed.
    X is a category axis of ``YYYY-MM-DD`` strings (trading days only —
    no weekend/holiday gaps); y is fixed to [0, 100].  The five
    semi-transparent ``hrect`` band shapes use the palette in
    ``RATING_COLORS`` order at the ``BAND_BOUNDS`` edges.  ``show_ma``
    overlays MA20/MA50 line traces (computed on the sliced window).
    Hover shows date | score | rating.

    ``spy_df`` (optional, F19-T1) is a ``daily_close`` frame with
    ``Date``/``Close`` columns — ``Date`` arrives object-dtype via
    Flight (the F13 lesson) and is normalized with ``pd.to_datetime``.
    A non-empty frame adds a dark-orange SPY line on a secondary
    right-side y axis: the closes are LEFT-merged onto this figure's
    category date strings, so the x axis stays exactly the F&G trading
    days — SPY dates absent from the F&G window drop out and F&G dates
    missing a SPY close render as a gap (NaN).  ``None``, an empty
    frame, or a frame missing the expected columns produces the same
    figure as before — no ``yaxis2``, no error.
    """
    import plotly.graph_objects as go

    x = _category_dates(df)
    fig = go.Figure()

    # Five rating-band zones behind the line (hrect -> layout shapes).
    for i, color in enumerate(RATING_COLORS):
        # justified: five constant band rects — each needs its own
        # bound pair; there is no vectorized add_hrect API (a constant
        # 5-iteration loop, negligible).
        fig.add_hrect(
            y0=BAND_BOUNDS[i],
            y1=BAND_BOUNDS[i + 1],
            fillcolor=color,
            opacity=0.12,
            line_width=0,
            layer="below",
        )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=df["fg_score"],
            mode="lines",
            name="Fear & Greed",
            line={"color": "#1f77b4", "width": 1.6},
            fill="tozeroy",
            fillcolor="rgba(31, 119, 180, 0.15)",
            customdata=df["fg_rating"],
            hovertemplate=(
                "Date: %{x}<br>Score: %{y:.1f}<br>"
                "Rating: %{customdata}<extra></extra>"
            ),
        )
    )

    if show_ma:
        ma20 = df["fg_score"].rolling(20).mean()
        ma50 = df["fg_score"].rolling(50).mean()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=ma20,
                mode="lines",
                name="MA20",
                line={"color": "#9467bd", "width": 1.2},
                hovertemplate="MA20: %{y:.1f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=ma50,
                mode="lines",
                name="MA50",
                line={"color": "#e377c2", "width": 1.2},
                hovertemplate="MA50: %{y:.1f}<extra></extra>",
            )
        )

    spy_added = (
        spy_df is not None
        and not spy_df.empty
        and {"Date", "Close"} <= set(spy_df.columns)
    )
    if spy_added:
        # Inner-align SPY onto the figure's category dates: a LEFT
        # merge keyed on the YYYY-MM-DD strings keeps the x axis
        # exactly the F&G trading days — SPY dates absent from the F&G
        # window drop out of the join, and F&G dates with no SPY close
        # produce NaN (a gap in the line), never a crash.
        spy = spy_df.copy()
        spy["Date"] = pd.to_datetime(spy["Date"])
        spy["_cat"] = spy["Date"].dt.strftime("%Y-%m-%d")
        aligned = pd.DataFrame({"_cat": x}).merge(
            spy.drop_duplicates("_cat")[["_cat", "Close"]],
            on="_cat",
            how="left",
        )
        fig.add_trace(
            go.Scatter(
                x=aligned["_cat"],
                y=aligned["Close"],
                mode="lines",
                name="SPY",
                yaxis="y2",
                line=dict(color="#FF8C00", width=1.5),
                hovertemplate=(
                    "Date: %{x}<br>SPY Close: %{y:.2f}<extra></extra>"
                ),
            )
        )

    fig.update_yaxes(range=[0, 100], title_text="Fear & Greed score")
    fig.update_xaxes(type="category", title_text=None)
    fig.update_layout(
        height=420,
        margin=dict(l=40, r=40, t=30, b=40),
        hovermode="x unified",
    )
    if spy_added:
        # AFTER update_yaxes — that call touches every y axis and
        # would otherwise clobber yaxis2's title/range.
        fig.update_layout(
            yaxis2=dict(
                overlaying="y",
                side="right",
                title="SPY",
                showgrid=False,
            )
        )
    return fig


def build_heatmap_figure(df: pd.DataFrame):
    """Rating heatmap: 9 sub-indicator rows x trading-day columns.

    ``z`` is the ordinal matrix — each ``<key>_rating`` string mapped
    through :func:`rating_ordinal` (NULL/unknown -> ``None`` gap).  The
    discrete stepped colorscale anchors the five ``RATING_COLORS`` to
    z = 0..4 (``zmin=0``/``zmax=4``).  ``customdata`` carries each
    cell's ``(raw value, rating)`` pair so the hover reads
    ``value · rating`` — the raw metric values are intentionally never
    plotted on a numeric axis.  Same category-date x axis as the
    history chart.
    """
    import plotly.graph_objects as go

    x = _category_dates(df)
    labels = [label for _, label in INDICATORS]

    z: List[List[Optional[float]]] = []
    custom: List[List[Tuple[object, str]]] = []
    for key, _label in INDICATORS:
        # justified: per-indicator row extraction — 9 constant rows;
        # the per-cell ordinal/customdata mapping is vectorized inside
        # each row via .map()/zip (no inner Python loop).
        rating_col = f"{key}_rating"
        ratings = df[rating_col] if rating_col in df.columns else pd.Series(
            [None] * len(df), index=df.index
        )
        values = df[key] if key in df.columns else pd.Series(
            [float("nan")] * len(df), index=df.index
        )
        z.append(ratings.map(rating_ordinal).tolist())
        custom.append(
            [
                (
                    float(v) if pd.notna(v) else "",
                    r if isinstance(r, str) else "",
                )
                for v, r in zip(values.tolist(), ratings.tolist())
            ]
        )

    fig = go.Figure(
        go.Heatmap(
            z=z,
            x=x,
            y=labels,
            zmin=0,
            zmax=4,
            colorscale=_HEATMAP_COLORSCALE,
            customdata=custom,
            hovertemplate=(
                "%{y}<br>Date: %{x}<br>"
                "%{customdata[0]} · %{customdata[1]}<extra></extra>"
            ),
            colorbar={
                "tickmode": "array",
                "tickvals": [0, 1, 2, 3, 4],
                "ticktext": [
                    "Extreme fear",
                    "Fear",
                    "Neutral",
                    "Greed",
                    "Extreme greed",
                ],
            },
        )
    )
    fig.update_xaxes(type="category", title_text=None)
    fig.update_layout(
        height=380,
        margin=dict(l=140, r=40, t=30, b=40),
        yaxis={"autorange": "reversed"},
    )
    return fig
