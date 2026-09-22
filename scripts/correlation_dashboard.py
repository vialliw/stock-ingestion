"""
=============================================================================
MULTI-ASSET CORRELATION DASHBOARD — report_app tab module (F22-T1)
=============================================================================

All SQL / pandas shaping / figure construction for the "Correlation"
tab in ``scripts/report_app.py`` lives here — same layering as
``scripts/fear_greed_dashboard.py``: ``report_app`` imports this
module lazily inside ``_render_correlation_tab`` and this module in
turn lazy-imports ``plotly.graph_objects`` /
``plotly.subplots.make_subplots`` inside the figure builders, so both
modules stay importable without plotly/streamlit.

The data source is the existing ``daily_close`` table — the same
(Date, Ticker, Close) long layout the other tabs query.  The five
series are the macro/equity panel from
``docs/enhancements/20260922a tech spec.md``:

==========  ==========  ===========================================
Ticker      Label       Series
==========  ==========  ===========================================
``^TNX``    TNX_10Y     CBOE 10-Year Treasury yield index (already %)
``CL=F``    WTI         Front-month WTI crude futures ($/bbl)
``^IRX``    IRX_3M      13-week T-bill yield (% — discount-rate proxy)
``SPY``     SPY         SPDR S&P 500 ETF ($)
``SMH``     SMH         VanEck Semiconductor ETF ($)
==========  ==========  ===========================================

Yahoo already returns ``^TNX``/``^IRX`` in percent — no transform is
applied.  ``CL=F`` trades on equity-market holidays, so the wide
pivot is ``dropna``-ed (inner-aligned dates only — a WTI-only date
drops out rather than fabricating equity closes).

The tab renders the five panels of ``docs/enhancements/20260922a.md``:

1. Two correlation heatmaps side-by-side (``st.columns(2)``): levels
   on the left, the selected return type on the right.
2. A normalized base=100 overlay of all five series.
3. A three-row multi-panel overlay (WTI vs TNX / SPY+SMH vs TNX /
   IRX+TNX on one percent axis).
4. Rolling-window correlations of daily returns for the selected
   pairs with a dashed y=0 reference line.
5. A scatter of a user-selected pair with an OLS trendline and the
   Pearson correlation in the title.

Window presets (1M/3M/6M/YTD/1Y/2Y/Max) slice the wide frame
POST-query in pandas — the whole ~1.7K-row-per-series history is
fetched once via the shared ``cached_fetch_dataframe`` path (same
precedent as ``fear_greed_dashboard.filter_window``).

Two deliberate data choices (mirroring the F13/F18 lessons):

* The ``Date`` column arrives from Arrow Flight as object-dtype
  ``datetime.date`` — :func:`normalize_dates` (also called inside
  :func:`to_wide`) runs ``pd.to_datetime`` before any Timestamp ops.
* X axes are **category** axes over ``YYYY-MM-DD`` date strings, so
  the axis contains aligned trading days only — no weekend/holiday
  gaps and no interpolation.

Each chart section in the tab is followed by a compact
``st.expander("How to read this chart")`` rendering the matching
``CORR_GUIDE`` markdown entry — adapted from
``docs/enhancements/20260922a.md`` and
``docs/enhancements/20260922a interpretation guide.txt`` (genericized:
no screenshot references, per-chart caveats kept).

Version: 1.0 (F22-T1)
=============================================================================
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional, Tuple

# Ensure the repo root (for ``src.*`` imports if ever needed) is on
# sys.path regardless of the caller's working directory — mirrors the
# header in scripts/fear_greed_dashboard.py.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

import numpy as np
import pandas as pd

# ------------------------------------------------------------------
# Series panel — ordered ticker -> display label mapping.  The order
# fixes the heatmap axis order, the normalized-chart legend order and
# the asset multi-select option order.
# ------------------------------------------------------------------

CORR_TICKERS: Dict[str, str] = {
    "^TNX": "TNX_10Y",
    "CL=F": "WTI",
    "^IRX": "IRX_3M",
    "SPY": "SPY",
    "SMH": "SMH",
}

# The six pairs from docs/enhancements/20260922a.md shown by default
# in the rolling-correlation multi-select.
DEFAULT_ROLLING_PAIRS: Tuple[Tuple[str, str], ...] = (
    ("WTI", "TNX_10Y"),
    ("SPY", "TNX_10Y"),
    ("SMH", "TNX_10Y"),
    ("SPY", "WTI"),
    ("SMH", "WTI"),
    ("SMH", "SPY"),
)

# Correlation-type radio labels -> returns() kind.  "Levels" is a
# passthrough so the same mapping feeds both the right-hand heatmap
# and the rolling chart.
CORR_TYPE_OPTIONS: Tuple[str, ...] = ("Levels", "Daily Returns", "Log Returns")
DEFAULT_CORR_TYPE = "Daily Returns"
CORR_TYPE_TO_KIND: Dict[str, str] = {
    "Levels": "levels",
    "Daily Returns": "daily",
    "Log Returns": "log",
}
_RET_KIND_TITLES: Dict[str, str] = {
    "levels": "Levels",
    "daily": "Daily Returns",
    "log": "Log Returns",
}

# Window presets offered by the tab's radio — "Max" is a passthrough
# (no cutoff).  ``YTD`` anchors to Jan 1 of the latest year present IN
# THE DATA, not the calendar year — a stale table must not produce an
# empty window.  (Same convention as fear_greed_dashboard.)
WINDOW_PRESETS: Tuple[str, ...] = ("1M", "3M", "6M", "YTD", "1Y", "2Y", "Max")
DEFAULT_WINDOW = "Max"

# Calendar-offset equivalents for the month/year presets (trading-day
# slicing happens implicitly — the wide frame only holds aligned
# trading days after to_wide's dropna).
_WINDOW_OFFSETS: Dict[str, pd.DateOffset] = {
    "1M": pd.DateOffset(months=1),
    "3M": pd.DateOffset(months=3),
    "6M": pd.DateOffset(months=6),
    "1Y": pd.DateOffset(years=1),
    "2Y": pd.DateOffset(years=2),
}

# Rolling-window widget bounds (trading days) — the tech spec's
# default is 60 with a 10-252 range.
DEFAULT_ROLLING_WINDOW = 60
ROLLING_WINDOW_MIN = 10
ROLLING_WINDOW_MAX = 252

# Diverging colorscale for the correlation heatmaps — red positive,
# blue negative, anchored to the fixed [-1, 1] domain.
_CORR_COLORSCALE = "RdBu_r"

# ------------------------------------------------------------------
# Per-chart interpretation guide — each chart section in the tab is
# followed by st.expander("How to read this chart") + st.markdown of
# the matching entry below.
#
# Adapted from docs/enhancements/20260922a.md and
# docs/enhancements/20260922a interpretation guide.txt — NOT verbatim:
# the example readings were genericized (the app shows live data, not
# a fixed 2020-2026 sample) and no screenshot references remain.
# ------------------------------------------------------------------

CORR_GUIDE: Dict[str, str] = {
    "heatmap": """\
**Correlation heatmaps — levels vs returns.** Each cell is the Pearson
correlation between two series over the current window: red = positive,
blue = negative, white ≈ 0 (fixed −1…+1 scale).

- The **left** matrix correlates raw *levels* — prices and yields
  together.  Levels correlations run high whenever series share a
  multi-year trend (e.g., equities and yields both rising through a
  post-recovery rate-hike cycle); they mostly tell you the series
  drifted in the same direction, not that they co-move day to day.
- The **right** matrix correlates *changes* (daily or log returns, per
  the Correlation-type radio).  Returns correlations are almost always
  much lower — daily moves are noisy — and are the more honest measure
  of short-term co-movement.
- Always sanity-check a high levels correlation against its returns
  counterpart before reading a relationship into it.""",
    "normalized": """\
**Normalized chart — every series rebased to 100** at the start of the
selected window, so relative performance is directly comparable across
different units (dollars vs percent yields).

- Divergence between SPY and SMH shows semiconductor over/under-
  performance; SMH is the more rate-sensitive (longer-duration growth)
  of the two, so it tends to amplify broad-market moves.
- A steadily climbing TNX_10Y / IRX_3M line marks a rising-rate regime —
  remember these lines are *yields rebased*, not prices.
- Rebase happens at the window start: change the window and the whole
  picture re-anchors, so compare shapes, not absolute heights, across
  windows.""",
    "multipanel": """\
**Multi-panel overlay — raw levels on paired axes.**

- **Row 1:** WTI crude ($/bbl, left) vs the 10-year yield (%, right) —
  the oil–rates relationship, which is loose most of the time but
  tightens sharply during inflation/geopolitical episodes.
- **Row 2:** SPY and SMH ($, left) vs the 10-year yield (%, right) —
  watch whether equity rallies coincide with rising yields
  (growth-driven) or falling yields (risk-off / easing-driven).
- **Row 3:** IRX_3M and TNX_10Y share one percent axis — the gap
  between them is the 3M–10Y curve slope; IRX above TNX signals an
  inverted curve.
- **Caveat:** `^IRX` (the 13-week T-bill yield) is only a *proxy* for
  the policy/discount rate — Yahoo carries no clean Fed Discount Rate
  or Fed Funds series (those live on FRED as `DISCOUNT`/`FEDFUNDS`).
  It tracks policy rates closely but is not the administered rate
  itself.  Dual-axis panels are for shape/timing comparison only —
  never compare a left-axis price level against a right-axis yield.""",
    "rolling": """\
**Rolling correlation — a sliding window** (the Rolling-window input,
default 60 trading days) of daily-return correlations for each
selected pair.  The dashed zero line separates positive from negative
co-movement regimes.

- This is the **regime-shift** detector: pairs that sit near zero for
  years can spike hard during stress episodes — e.g., oil–10Y yield
  correlation has jumped above ~0.9 on short windows during
  geopolitical supply shocks.  A spike that fades quickly is an event;
  one that persists is a regime change.
- Equity–yield pairs (SPY/SMH vs TNX_10Y) flipping negative mark
  risk-off phases where stocks fall as yields rise — or flight-to-
  quality phases where both move opposite ways.
- Shorter windows react faster but are noisier; longer windows smooth
  real but short-lived episodes away.  SMH–yield readings tend to be
  sharper than SPY–yield: semiconductors' long-duration growth cash
  flows make them more sensitive to discount-rate moves, even though
  SMH still tracks SPY closely overall.""",
    "scatter": """\
**Scatter — one point per aligned trading day** for the selected pair
(levels), with an OLS regression line and the full-window Pearson
correlation in the title.

- A tight cloud around the trendline = a stable linear relationship;
  a fan-shaped or clustered cloud = correlation driven by a few
  episodes rather than a steady link.
- Because the axes are *levels*, a high |r| can be dominated by a
  shared multi-year trend — cross-check the same pair on the returns
  heatmap before concluding the series co-move.
- **General caveats:** correlation is not causation — both series may
  be driven by a third factor (growth expectations, inflation, Fed
  policy).  Every number on this tab is sample-window sensitive:
  correlations over a 2020-style recovery, a hiking cycle, and a
  calm drift can differ wildly, so always state the window when
  quoting a correlation.""",
}

# ------------------------------------------------------------------
# SQL builder — fetch the five series once; window-slice in pandas.
# ------------------------------------------------------------------


def build_corr_sql() -> str:
    """SELECT (Date, Ticker, Close) rows for the five panel tickers.

    No WHERE clause on Date — the window preset is applied post-query
    by :func:`filter_window` (pre-cutoff rows would also be needed if
    a trailing window ever feeds a warm-up).  Ticker literals come
    from the fixed ``CORR_TICKERS`` whitelist, never user input.
    """
    tickers = ", ".join(f"'{t}'" for t in CORR_TICKERS)
    return (
        "SELECT Date, Ticker, Close FROM daily_close "
        f"WHERE Ticker IN ({tickers}) ORDER BY Date"
    )


# ------------------------------------------------------------------
# Pandas shaping helpers.
# ------------------------------------------------------------------


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` with ``Date`` as datetime64.

    Arrow Flight's ``do_get().to_pandas()`` delivers the DuckDB DATE
    column as object-dtype ``datetime.date`` objects (the F13 dtype
    lesson) — ``pd.to_datetime`` is required before any Timestamp ops
    or pivot indexing.
    """
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"])
    return out


def to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot the long (Date, Ticker, Close) frame to a wide frame.

    Date is normalized first (object-dtype ``datetime.date`` arrives
    via Flight), then pivoted to a ``DatetimeIndex`` x one column per
    display label (``CORR_TICKERS`` order — only columns actually
    present appear, so a not-yet-loaded ticker simply drops out), then
    ``dropna`` inner-aligns the calendars: ``CL=F`` trades on
    equity-market holidays, so without the dropna those dates would
    carry NaN equity closes.  Rows are sorted ascending by Date.
    """
    long = normalize_dates(df)
    wide = long.pivot(index="Date", columns="Ticker", values="Close")
    wide = wide.rename(columns=CORR_TICKERS)
    labels = [label for label in CORR_TICKERS.values() if label in wide.columns]
    wide = wide[labels].sort_index().dropna()
    wide.index.name = "Date"
    return wide


def returns(wide: pd.DataFrame, kind: str) -> pd.DataFrame:
    """Return the per-series change frame for ``kind``.

    ``kind`` is one of ``"levels"`` (passthrough copy — lets the
    corr-type radio feed the heatmap/rolling builders uniformly),
    ``"daily"`` (simple ``pct_change``) or ``"log"`` (first difference
    of ``log(Close)`` — undefined where a level is <= 0, e.g. the
    negative WTI print of Apr 2020; those cells stay NaN and pairwise
    correlation simply skips them).  The all-NaN first row produced by
    the diff is dropped via ``dropna(how="all")`` — rows with partial
    data are kept.  Raises ``ValueError`` on an unknown kind.
    """
    if kind == "levels":
        return wide.copy()
    if kind == "daily":
        return wide.pct_change().dropna(how="all")
    if kind == "log":
        return np.log(wide).diff().dropna(how="all")
    raise ValueError(
        f"Unknown returns kind {kind!r} — expected 'levels', 'daily' or 'log'"
    )


def filter_window(wide: pd.DataFrame, preset: str) -> pd.DataFrame:
    """Slice a wide frame to the trailing window named by ``preset``.

    ``wide`` must carry a ``DatetimeIndex`` (the output of
    :func:`to_wide`).  ``Max`` and an empty frame are passthroughs.
    ``YTD`` keeps rows on/after Jan 1 of the latest year present in
    the data (not the calendar year — a stale table would otherwise
    produce an empty window).  Raises ``ValueError`` on an unknown
    preset.
    """
    if preset not in WINDOW_PRESETS:
        raise ValueError(
            f"Unknown window preset {preset!r} — expected one of "
            f"{WINDOW_PRESETS}"
        )
    if preset == "Max" or wide.empty:
        return wide.copy()
    max_date = wide.index.max()
    if preset == "YTD":
        cutoff = pd.Timestamp(year=max_date.year, month=1, day=1)
    else:
        cutoff = max_date - _WINDOW_OFFSETS[preset]
    return wide[wide.index >= cutoff].copy()


def pair_label(pair: Tuple[str, str]) -> str:
    """``("WTI", "TNX_10Y")`` -> ``"WTI vs TNX_10Y"`` (widget label)."""
    return f"{pair[0]} vs {pair[1]}"


def parse_pair_label(text: str) -> Tuple[str, str]:
    """Inverse of :func:`pair_label` — split on the first ``" vs "``."""
    left, _sep, right = text.partition(" vs ")
    return (left.strip(), right.strip())


def _category_dates(index: pd.Index) -> List[str]:
    """``YYYY-MM-DD`` strings for a trading-day-only category x axis."""
    return pd.DatetimeIndex(index).strftime("%Y-%m-%d").tolist()


# ------------------------------------------------------------------
# Figure builders — plotly is imported lazily inside each one so this
# module (and therefore report_app) stays importable without plotly.
# ------------------------------------------------------------------


def _corr_heatmap_fig(corr: pd.DataFrame, title: str):
    """One annotated -1..+1 diverging heatmap for a corr matrix."""
    import plotly.graph_objects as go

    labels = list(corr.columns)
    fig = go.Figure(
        go.Heatmap(
            z=corr.values,
            x=labels,
            y=labels,
            zmin=-1,
            zmax=1,
            colorscale=_CORR_COLORSCALE,
            text=np.round(corr.values, 2),
            texttemplate="%{text:.2f}",
            hovertemplate=(
                "%{y} vs %{x}<br>corr: %{z:.3f}<extra></extra>"
            ),
            colorbar={"title": "corr"},
        )
    )
    fig.update_layout(
        title=title,
        height=420,
        margin=dict(l=80, r=40, t=60, b=60),
        yaxis={"autorange": "reversed"},
    )
    return fig


def build_heatmap_figures(wide: pd.DataFrame, ret_kind: str = "daily"):
    """(levels_fig, returns_fig) — two annotated correlation heatmaps.

    ``wide`` is the windowed wide frame (display-label columns).
    ``levels_fig`` correlates the raw levels; ``returns_fig``
    correlates ``returns(wide, ret_kind)`` (``"daily"``/``"log"`` — or
    ``"levels"``, which deliberately reproduces the left panel when
    the corr-type radio is on Levels).  Both are ``go.Heatmap``
    figures on the fixed [-1, 1] diverging scale so the tab can render
    them side-by-side in ``st.columns(2)``.
    """
    levels_fig = _corr_heatmap_fig(wide.corr(), "Correlation of Levels")
    ret_title = _RET_KIND_TITLES.get(ret_kind, ret_kind)
    returns_fig = _corr_heatmap_fig(
        returns(wide, ret_kind).corr(),
        f"Correlation of {ret_title}",
    )
    return levels_fig, returns_fig


def build_normalized_figure(wide: pd.DataFrame):
    """All series rebased to 100 at the window start (5 lines max).

    ``wide / wide.iloc[0] * 100`` — relative performance comparable
    across units.  X is a category axis of ``YYYY-MM-DD`` strings
    (aligned trading days only — no weekend/holiday gaps).  A series
    whose first value is NaN would poison the division, but
    :func:`to_wide` already dropna-ed the frame.
    """
    import plotly.graph_objects as go

    norm = wide / wide.iloc[0] * 100.0
    x = _category_dates(norm.index)
    fig = go.Figure()
    for label in norm.columns:
        # justified: one Scatter trace per series — a constant <=5
        # iteration loop over column names; plotly has no vectorized
        # multi-series constructor from a DataFrame here.
        fig.add_trace(
            go.Scatter(
                x=x,
                y=norm[label],
                mode="lines",
                name=label,
                hovertemplate=(
                    f"{label}: %{{y:.1f}}<extra></extra>"
                ),
            )
        )
    fig.update_xaxes(type="category", title_text=None)
    fig.update_yaxes(title_text="Normalized (window start = 100)")
    fig.update_layout(
        title="Normalized Price / Yield (base = 100)",
        height=420,
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode="x unified",
    )
    return fig


def build_multipanel_figure(wide: pd.DataFrame):
    """Three stacked panels: WTI vs TNX | SPY+SMH vs TNX | IRX+TNX.

    Rows 1-2 use a secondary (right-side) y axis for the percent yield
    against the left-axis dollar series; row 3 keeps IRX_3M and
    TNX_10Y on a single shared percent axis (both are yields — the gap
    between them is the 3M-10Y curve slope).  Series absent from
    ``wide`` are skipped per-panel rather than failing — a partially
    loaded panel still renders.  Category x axis, shared across rows.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.07,
        specs=[
            [{"secondary_y": True}],
            [{"secondary_y": True}],
            [{"secondary_y": False}],
        ],
        subplot_titles=(
            "WTI ($/bbl) vs TNX_10Y (%)",
            "SPY & SMH ($) vs TNX_10Y (%)",
            "IRX_3M vs TNX_10Y (% — shared axis)",
        ),
    )
    x = _category_dates(wide.index)

    def _add(label: str, row: int, secondary: bool) -> None:
        if label not in wide.columns:
            return
        fig.add_trace(
            go.Scatter(
                x=x,
                y=wide[label],
                mode="lines",
                name=label,
                legendgroup=label,
                hovertemplate=(
                    f"{label}: %{{y:.2f}}<extra></extra>"
                ),
            ),
            row=row,
            col=1,
            secondary_y=secondary,
        )

    _add("WTI", 1, secondary=False)
    _add("TNX_10Y", 1, secondary=True)
    _add("SPY", 2, secondary=False)
    _add("SMH", 2, secondary=False)
    _add("TNX_10Y", 2, secondary=True)
    _add("IRX_3M", 3, secondary=False)
    _add("TNX_10Y", 3, secondary=False)

    fig.update_xaxes(type="category", title_text=None)
    fig.update_yaxes(title_text="WTI ($/bbl)", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="TNX_10Y (%)", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Price ($)", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="TNX_10Y (%)", row=2, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Yield (%)", row=3, col=1)
    fig.update_layout(
        title="Multi-Panel Overlay",
        height=760,
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode="x unified",
    )
    return fig


def build_rolling_figure(
    wide: pd.DataFrame,
    window: int = DEFAULT_ROLLING_WINDOW,
    pairs: Optional[List[Tuple[str, str]]] = None,
):
    """Rolling ``window``-day correlation of daily returns, per pair.

    ``pairs`` are display-label tuples (``DEFAULT_ROLLING_PAIRS``
    order when ``None``); pairs referencing a label absent from
    ``wide`` are skipped.  Each pair gets one line of
    ``rets[a].rolling(window).corr(rets[b])`` on the daily-returns
    frame; a dashed y=0 hline separates positive from negative
    co-movement regimes.  The first ``window`` points of each line are
    NaN and render as a leading gap.
    """
    import plotly.graph_objects as go

    if pairs is None:
        pairs = list(DEFAULT_ROLLING_PAIRS)
    rets = returns(wide, "daily")
    x = _category_dates(rets.index)
    fig = go.Figure()
    for a, b in pairs:
        # justified: one rolling-corr trace per selected pair — a
        # small constant set (<=6 by default); each pandas call is
        # already vectorized internally.
        if a not in rets.columns or b not in rets.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=x,
                y=rets[a].rolling(int(window)).corr(rets[b]),
                mode="lines",
                name=pair_label((a, b)),
                hovertemplate=(
                    f"{a} vs {b}: %{{y:.2f}}<extra></extra>"
                ),
            )
        )
    fig.add_hline(y=0, line_dash="dash", line_color="#888888")
    fig.update_xaxes(type="category", title_text=None)
    fig.update_yaxes(
        title_text=f"{int(window)}-day rolling corr (daily returns)",
        range=[-1.05, 1.05],
    )
    fig.update_layout(
        title=f"Rolling {int(window)}-Day Correlations of Daily Returns",
        height=420,
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode="x unified",
    )
    return fig


def build_scatter_figure(
    wide: pd.DataFrame, x_label: str, y_label: str
):
    """Levels scatter of ``x_label`` vs ``y_label`` + OLS trendline.

    One marker per aligned trading day; the trendline is the
    least-squares fit (``np.polyfit`` degree 1) and the Pearson
    correlation appears in the title.  Levels are used — the axes are
    the raw units of each series — so a high |r| can be trend-driven
    (cross-check the returns heatmap; the guide says as much).
    Raises ``KeyError`` if either label is absent from ``wide``.
    """
    import plotly.graph_objects as go

    xs = wide[x_label]
    ys = wide[y_label]
    corr = float(xs.corr(ys))
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="markers",
            name=pair_label((x_label, y_label)),
            marker={"size": 5, "opacity": 0.5},
            hovertemplate=(
                f"{x_label}: %{{x:.2f}}<br>"
                f"{y_label}: %{{y:.2f}}<extra></extra>"
            ),
        )
    )
    slope, intercept = np.polyfit(xs.to_numpy(), ys.to_numpy(), 1)
    x_line = np.array([float(xs.min()), float(xs.max())])
    fig.add_trace(
        go.Scatter(
            x=x_line,
            y=slope * x_line + intercept,
            mode="lines",
            name="OLS trendline",
            line={"color": "#d62728", "dash": "dash"},
            hovertemplate="trendline<extra></extra>",
        )
    )
    fig.update_xaxes(title_text=x_label)
    fig.update_yaxes(title_text=y_label)
    fig.update_layout(
        title=f"{x_label} vs {y_label} — Pearson corr {corr:.2f}",
        height=480,
        margin=dict(l=40, r=40, t=60, b=40),
    )
    return fig
