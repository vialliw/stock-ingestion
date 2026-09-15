"""
=============================================================================
MARKET BREADTH DASHBOARD GENERATOR (stock-ingestion port)
=============================================================================

Flight-sourced port of the legacy
``C:\\Intel\\python\\work\\MarketBreath\\breadth_builder02.py`` (v2.1).
The SQL strings, EMA seeding, indicator formulas, figure construction
and interpretation-guide HTML are copied VERBATIM — the only change is
the data source: instead of ``duckdb.connect(db_path)`` directly, both
queries go through the shared fetch path in ``scripts/report_app.py``
(``fetch_dataframe`` — Arrow Flight ``do_get`` JSON ``{"sql"}`` ticket
first, direct *read-only* DuckDB fallback).  This module never opens a
writable DuckDB connection.

Generates an interactive HTML dashboard with 5 market breadth subcharts:
1. SPY Candlestick + 50-Day EMA
2. % Stocks Above 50-Day SMA
3. % Stocks Above 200-Day SMA
4. Net Advances & Volume Ratio
5. New Highs vs. New Lows (252-Day Lookback)

The ``years`` window is applied POST-query in pandas (the SMA_200 /
252-day lookback windows need pre-cutoff history — pushing the cutoff
into the WHERE clause would change semantics).  ``years=0`` means
"all data" (the legacy ``years=None`` passthrough); the default comes
from ``reports.market_breadth_years`` in ``src/config.yaml``.

Output is always ``Stock_Ingestion_Market_Breadth.html`` (fixed name,
not DB-stem-derived like the legacy) written to
``reports.report_dir``.

Usage:
    python scripts/breadth_dashboard.py [years]

    from breadth_dashboard import generate_breadth_dashboard
    html_path = generate_breadth_dashboard(config, years=3)

The module is importable without plotly: ``plotly.graph_objects`` /
``make_subplots`` are imported lazily inside
:func:`build_breadth_figure` only (same pattern as the lazy ``altair``
import in ``scripts/report_app.py``).

Author: Market Breath Dashboard Tool (ported by stock-ingestion F11-T1)
Version: 2.1 (Fixed bar visibility + all previous fixes)
=============================================================================
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional, Tuple

# Ensure the repo root (for ``src.config``) and this script's own
# directory (for the ``report_app`` sibling import) are on sys.path
# regardless of the caller's working directory — mirrors the header in
# scripts/daily_load_once.py / scripts/report_app.py.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

import report_app
from src.config import PipelineConfig

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")

# Fixed output filename — deliberately NOT derived from the DB stem
# like the legacy ``market_breadth_dashboard_{db_path.stem}.html``.
OUTPUT_FILENAME = "Stock_Ingestion_Market_Breadth.html"

# Interpretation-guide HTML block — VERBATIM from the legacy
# breadth_builder02.py (injected before </body> in the generated file
# and rendered in the report_app "Market Breadth" tab's expander).
BREADTH_GUIDE_HTML = """
    <div style="margin-top:40px; padding:25px; background:rgba(30,30,40,0.95); border-radius:10px; font-family:'Segoe UI',Arial,sans-serif; color:#e0e0e0; max-width:1280px; margin-left:auto; margin-right:auto; border:1px solid #444;">
        <h3 style="color:#00CCFF; margin-top:0; border-bottom:2px solid #00CCFF; padding-bottom:10px;">📖 Complete Interpretation Guide</h3>
        
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-bottom:20px;">
            <div style="padding:15px; background:rgba(50,50,70,0.6); border-radius:8px;">
                <h4 style="color:#00CCFF; margin:0 0 10px 0;">1️⃣ SPY Candlestick + 50-Day EMA</h4>
                <ul style="margin:0; padding-left:20px; font-size:13px; line-height:1.6;">
                    <li><strong>🔵 Up candle</strong>: Close > Open (bullish day)</li>
                    <li><strong>🔴 Down candle</strong>: Close < Open (bearish day)</li>
                    <li><strong>🟡 Gold dotted line</strong>: 50-Day EMA (dynamic support/resistance)</li>
                    <li><strong>✅ Bullish</strong>: Price above rising EMA</li>
                    <li><strong>❌ Bearish</strong>: Price below falling EMA</li>
                    <li><strong>⚠️ Watch</strong>: Price crossing EMA repeatedly = choppy market</li>
                </ul>
            </div>
            
            <div style="padding:15px; background:rgba(50,50,70,0.6); border-radius:8px;">
                <h4 style="color:#00FF99; margin:0 0 10px 0;">2️⃣ % Stocks Above 50-Day SMA</h4>
                <ul style="margin:0; padding-left:20px; font-size:13px; line-height:1.6;">
                    <li><strong>Green area</strong>: Short-term trend participation</li>
                    <li><strong>>80%</strong>: Overbought → watch for pullback</li>
                    <li><strong>50%</strong>: Neutral equilibrium line</li>
                    <li><strong><20%</strong>: Oversold → potential bounce zone</li>
                    <li><strong>✅ Confirmation</strong>: Rising with SPY = healthy trend</li>
                    <li><strong>⚠️ Divergence</strong>: SPY up but % falling = narrow leadership</li>
                </ul>
            </div>
            
            <div style="padding:15px; background:rgba(50,50,70,0.6); border-radius:8px;">
                <h4 style="color:#9B59B6; margin:0 0 10px 0;">3️⃣ % Stocks Above 200-Day SMA</h4>
                <ul style="margin:0; padding-left:20px; font-size:13px; line-height:1.6;">
                    <li><strong>Purple area</strong>: Long-term trend participation</li>
                    <li><strong>>80%</strong>: Extended bull market (caution)</li>
                    <li><strong>50%</strong>: Long-term neutral</li>
                    <li><strong><20%</strong>: Deep bear market (opportunity)</li>
                    <li><strong>✅ Bull market</strong>: Sustained >50% with upward slope</li>
                    <li><strong>🔄 Regime change</strong>: Crosses 50% line = potential trend shift</li>
                </ul>
            </div>
            
            <div style="padding:15px; background:rgba(50,50,70,0.6); border-radius:8px;">
                <h4 style="color:#F39C12; margin:0 0 10px 0;">4️⃣ Net Advances & Volume Ratio</h4>
                <ul style="margin:0; padding-left:20px; font-size:13px; line-height:1.6;">
                    <li><strong>🔵/ Bars</strong>: Net stocks rising vs. falling</li>
                    <li><strong>🟠 Orange line</strong>: Volume Ratio (Adv/Dec)</li>
                    <li><strong>Ratio >1.2</strong>: Strong buying conviction</li>
                    <li><strong>Ratio <0.8</strong>: Strong selling conviction</li>
                    <li><strong>✅ High conviction</strong>: Tall bars + extreme ratio in same direction</li>
                    <li><strong>⚠️ Low conviction</strong>: Small bars + ratio near 1.0</li>
                </ul>
            </div>
        </div>
        
        <div style="padding:15px; background:rgba(50,50,70,0.6); border-radius:8px; margin-bottom:20px;">
            <h4 style="color:#FFA15A; margin:0 0 10px 0;">5️⃣ New Highs vs. New Lows (252-Day Lookback)</h4>
            <ul style="margin:0; padding-left:20px; font-size:13px; line-height:1.6;">
                <li><strong>🟢 Green bars</strong>: More 1-year highs than lows (bullish)</li>
                <li><strong>🔴 Red bars</strong>: More 1-year lows than highs (bearish)</li>
                <li><strong>⚪ White line</strong>: Zero equilibrium (equal highs/lows)</li>
                <li><strong>🟠 Dashed line</strong>: NH/NL Ratio (right axis)</li>
                <li><strong>✅ Healthy bull</strong>: Sustained positive net new highs</li>
                <li><strong>⚠️ Warning divergence</strong>: SPY making new highs but net new highs declining</li>
                <li><strong>🔄 Reversal signal</strong>: Extreme negative bars + sudden flip positive = capitulation bottom</li>
                <li><strong>💡 Pro tip</strong>: New highs/lows lead price by 1-3 months at major turning points</li>
            </ul>
        </div>
        
        <div style="padding:15px; background:rgba(40,60,80,0.7); border-left:4px solid #00CCFF; border-radius:0 8px 8px 0;">
            <h4 style="color:#00CCFF; margin:0 0 10px 0;">🎯 Combined Signal Framework</h4>
            <table style="width:100%; border-collapse:collapse; font-size:13px;">
                <tr style="border-bottom:1px solid #555;">
                    <th style="text-align:left; padding:8px;">Signal Strength</th>
                    <th style="text-align:left; padding:8px;">Criteria</th>
                    <th style="text-align:left; padding:8px;">Action</th>
                </tr>
                <tr>
                    <td style="padding:8px; color:#00FF99;"><strong>🟢 HIGH (A+)</strong></td>
                    <td style="padding:8px;">SPY > EMA + %SMA50>50% + Net Adv +200 + NH-NL positive</td>
                    <td style="padding:8px;">✅ Add longs, raise stops</td>
                </tr>
                <tr>
                    <td style="padding:8px; color:#F39C12;"><strong>🟡 MEDIUM (B)</strong></td>
                    <td style="padding:8px;">3 of 4 indicators align, 1 neutral</td>
                    <td style="padding:8px;">🟡 Hold positions, wait for confirmation</td>
                </tr>
                <tr>
                    <td style="padding:8px; color:#FF4136;"><strong>🔴 LOW / AVOID (C)</strong></td>
                    <td style="padding:8px;">Indicators conflict or all neutral</td>
                    <td style="padding:8px;">❌ Reduce size, stay cash, wait</td>
                </tr>
                <tr>
                    <td style="padding:8px; color:#FFA15A;"><strong>⚠️ REVERSAL WATCH</strong></td>
                    <td style="padding:8px;">Extreme NH-NL + Volume Ratio spike + %SMA <20% or >80%</td>
                    <td style="padding:8px;">🔄 Prepare for counter-trend move</td>
                </tr>
            </table>
        </div>
        
        <p style="margin-top:20px; font-size:12px; color:#888; border-top:1px solid #444; padding-top:15px;">
            💡 <strong>Final Pro Tips:</strong><br>
            • Wait for 3+ indicators to align before acting on signals<br>
            • Breadth leads price: use divergences as early warnings, not timing triggers<br>
            • Context matters: signals near key support/resistance carry more weight<br>
            • This dashboard shows <em>probability</em>, not certainty—always use stop losses
        </p>
    </div>
    """


# ------------------------------------------------------------------
# SQL builders — VERBATIM from the legacy breadth_builder02.py.
# ------------------------------------------------------------------


def build_breadth_sql() -> str:
    """Breadth metrics for the universe (excluding SPY) — legacy query
    verbatim: ``Ticker != 'SPY'`` exclusion, ``LAG(Close)`` per ticker,
    SMA_50/SMA_200 windows, and the 252-day High/Low lookback that
    EXCLUDES the current row (``1 PRECEDING`` as the window end)."""
    return """
    WITH price_lag AS (
        SELECT 
            Date, Ticker, Open, High, Low, Close, Volume,
            LAG(Close) OVER (PARTITION BY Ticker ORDER BY Date) as Prev_Close
        FROM daily_close
        WHERE Ticker != 'SPY'
    ),
    daily_metrics AS (
        SELECT 
            Date, Ticker, Close, Volume, Prev_Close, High, Low,
            CASE 
                WHEN Close > Prev_Close THEN 1 
                WHEN Close < Prev_Close THEN -1 
                ELSE 0 
            END as AD_Score,
            AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as SMA_50,
            AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as SMA_200,
            -- FIX: Lookback EXCLUDES current day (1 PRECEDING as end)
            MAX(High) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING) as High_252d,
            MIN(Low) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING) as Low_252d
        FROM price_lag
    ),
    new_highs_lows AS (
        SELECT 
            Date,
            SUM(CASE WHEN High > High_252d AND High_252d IS NOT NULL THEN 1 ELSE 0 END) as New_Highs,
            SUM(CASE WHEN Low < Low_252d AND Low_252d IS NOT NULL THEN 1 ELSE 0 END) as New_Lows
        FROM daily_metrics
        WHERE Prev_Close IS NOT NULL
        GROUP BY Date
    ),
    breadth_agg AS (
        SELECT 
            m.Date,
            COUNT(*) as Total_Stocks,
            SUM(CASE WHEN m.AD_Score > 0 THEN 1 ELSE 0 END) as Advances,
            SUM(CASE WHEN m.AD_Score < 0 THEN 1 ELSE 0 END) as Declines,
            SUM(CASE WHEN m.Close > m.SMA_50 THEN 1 ELSE 0 END) as Above_SMA_50,
            SUM(CASE WHEN m.Close > m.SMA_200 THEN 1 ELSE 0 END) as Above_SMA_200,
            SUM(m.Volume * CASE WHEN m.AD_Score > 0 THEN 1 ELSE 0 END) as Adv_Volume,
            SUM(m.Volume * CASE WHEN m.AD_Score < 0 THEN 1 ELSE 0 END) as Dec_Volume,
            COALESCE(nh.New_Highs, 0) as New_Highs,
            COALESCE(nh.New_Lows, 0) as New_Lows
        FROM daily_metrics m
        LEFT JOIN new_highs_lows nh ON m.Date = nh.Date
        WHERE m.Prev_Close IS NOT NULL
        GROUP BY m.Date, nh.New_Highs, nh.New_Lows
        ORDER BY m.Date
    )
    SELECT * FROM breadth_agg
    """


def build_spy_sql() -> str:
    """SPY OHLC data for the candlestick + EMA — legacy query
    verbatim."""
    return """
    SELECT Date, Open, High, Low, Close, Volume
    FROM daily_close
    WHERE Ticker = 'SPY'
    ORDER BY Date
    """


# ------------------------------------------------------------------
# Indicator math — VERBATIM from the legacy breadth_builder02.py.
# ------------------------------------------------------------------


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average.

    LEGACY seeding semantics kept verbatim — do NOT replace with
    ``pd.Series.ewm``: the first ``period`` values are seeded with the
    SMA of those values, then the recursive EMA runs from index
    ``period`` onward (a different warmup than ``ewm`` produces).
    """
    multiplier = 2 / (period + 1)
    ema = series.copy()
    ema.iloc[:period] = series.iloc[:period].mean()
    # justified: recursive EMA — each value depends on the previous
    # EMA value, so it cannot be vectorized (the range() form is
    # check_loops-exempt; comment kept for consistency anyway).
    for i in range(period, len(series)):
        ema.iloc[i] = (series.iloc[i] * multiplier) + (ema.iloc[i-1] * (1 - multiplier))
    return ema


def compute_indicators(breadth_df: pd.DataFrame) -> pd.DataFrame:
    """Add the six derived breadth columns — legacy formulas verbatim:
    ADL cumsum, Pct_Above_SMA50/200, Volume_Ratio with the
    ``replace(0, 1)`` zero-division guard, Net_New_Highs, and the
    NH_NL_Ratio guard.  Returns the (copied) frame for chaining.
    """
    breadth_df = breadth_df.copy()
    breadth_df['ADL'] = (breadth_df['Advances'] - breadth_df['Declines']).cumsum()
    breadth_df['Pct_Above_SMA50'] = (breadth_df['Above_SMA_50'] / breadth_df['Total_Stocks']) * 100
    breadth_df['Pct_Above_SMA200'] = (breadth_df['Above_SMA_200'] / breadth_df['Total_Stocks']) * 100
    breadth_df['Volume_Ratio'] = breadth_df['Adv_Volume'] / breadth_df['Dec_Volume'].replace(0, 1)
    breadth_df['Net_New_Highs'] = breadth_df['New_Highs'] - breadth_df['New_Lows']
    breadth_df['NH_NL_Ratio'] = breadth_df['New_Highs'] / breadth_df['New_Lows'].replace(0, 1)
    return breadth_df


def filter_years(
    breadth_df: pd.DataFrame,
    spy_df: pd.DataFrame,
    years: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the legacy trailing-N-years cutoff (POST-query, in pandas).

    ``years=None`` or ``years=0`` is a passthrough (all data); a
    positive value keeps only rows on/after ``max_date - DateOffset(
    years=years)`` where ``max_date`` is the latest date across BOTH
    frames — the legacy semantics verbatim.  Both frames always leave
    with a datetime ``Date`` column, matching the legacy "Ensure
    datetime type" step.
    """
    breadth_df = breadth_df.copy()
    spy_df = spy_df.copy()
    if years is not None and years > 0:
        breadth_df['Date'] = pd.to_datetime(breadth_df['Date'])
        spy_df['Date'] = pd.to_datetime(spy_df['Date'])

        max_date = max(breadth_df['Date'].max(), spy_df['Date'].max())
        cutoff_date = max_date - pd.DateOffset(years=years)

        breadth_df = breadth_df[breadth_df['Date'] >= cutoff_date].copy()
        spy_df = spy_df[spy_df['Date'] >= cutoff_date].copy()
        print(f"📅 Filtered to last {years} year(s): {cutoff_date.date()} to {max_date.date()}")

    # Ensure datetime type
    if not pd.api.types.is_datetime64_any_dtype(breadth_df['Date']):
        breadth_df['Date'] = pd.to_datetime(breadth_df['Date'])
    if not pd.api.types.is_datetime64_any_dtype(spy_df['Date']):
        spy_df['Date'] = pd.to_datetime(spy_df['Date'])
    return breadth_df, spy_df


# ------------------------------------------------------------------
# Figure construction — VERBATIM from the legacy breadth_builder02.py
# (subplot titles, colors, row heights, hlines, bar fixes, layout).
# ------------------------------------------------------------------


def build_breadth_figure(
    breadth_df: pd.DataFrame,
    spy_df: pd.DataFrame,
    source: str = "daily_close",
):
    """Build the 5-subplot plotly figure (legacy construction verbatim).

    ``source`` only fills the dashboard title (the legacy used
    ``db_path.name``; the port is source-agnostic so the caller passes
    a label — ``generate_breadth_dashboard`` uses the configured DB
    filename).  ``plotly`` is imported lazily so this module stays
    importable without it.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=5, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.035,
        subplot_titles=(
            "SPY Price Candlestick + 50-Day EMA",
            "% Stocks Above 50-Day SMA",
            "% Stocks Above 200-Day SMA",
            "Net Advances & Volume Ratio",
            "New Highs vs. New Lows (252-Day Lookback)"
        ),
        row_heights=[0.35, 0.15, 0.15, 0.15, 0.20]
    )

    # --- Row 1: SPY Candlestick + 50-Day EMA ---
    fig.add_trace(go.Candlestick(
        x=spy_df['Date'], open=spy_df['Open'], high=spy_df['High'],
        low=spy_df['Low'], close=spy_df['Close'], name='SPY',
        increasing_line_color='#00CCFF', decreasing_line_color='#FF4136'
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=spy_df['Date'], y=spy_df['EMA_50'], name='50-Day EMA',
        line=dict(color='#FFD700', width=1.5, dash='dot'), hoverinfo='skip'
    ), row=1, col=1)

    # --- Row 2: % Above SMA50 ---
    fig.add_trace(go.Scatter(
        x=breadth_df['Date'], y=breadth_df['Pct_Above_SMA50'],
        name='% > SMA50', fill='tozeroy', line=dict(color='#00FF99')
    ), row=2, col=1)
    fig.add_hline(y=50, line_dash="dash", line_color="gray", row=2, col=1)
    fig.add_hline(y=80, line_dash="dot", line_color="red", row=2, col=1)
    fig.add_hline(y=20, line_dash="dot", line_color="green", row=2, col=1)

    # --- Row 3: % Above SMA200 ---
    fig.add_trace(go.Scatter(
        x=breadth_df['Date'], y=breadth_df['Pct_Above_SMA200'],
        name='% > SMA200', fill='tozeroy', line=dict(color='#9B59B6')
    ), row=3, col=1)
    fig.add_hline(y=50, line_dash="dash", line_color="gray", row=3, col=1)
    fig.add_hline(y=80, line_dash="dot", line_color="red", row=3, col=1)
    fig.add_hline(y=20, line_dash="dot", line_color="green", row=3, col=1)

    # --- Row 4: Net Advances Bars & Volume Ratio ---
    net_advance = breadth_df['Advances'] - breadth_df['Declines']
    fig.add_trace(go.Bar(
        x=breadth_df['Date'], y=net_advance, name='Net Advances',
        marker_color=net_advance.apply(lambda x: '#00CCFF' if x > 0 else '#FF4136'),
        opacity=0.85
    ), row=4, col=1)

    fig.add_trace(go.Scatter(
        x=breadth_df['Date'], y=breadth_df['Volume_Ratio'],
        name='Vol Ratio (Adv/Dec)', line=dict(color='#F39C12', width=1.5), yaxis='y4'
    ), row=4, col=1)

    # --- Row 5: New Highs vs New Lows (FIXED BAR VISIBILITY) ---
    fig.add_trace(go.Bar(
        x=breadth_df['Date'],
        y=breadth_df['Net_New_Highs'],
        name='Net New Highs',
        marker_color=breadth_df['Net_New_Highs'].apply(
            lambda x: '#00FF99' if x > 0 else '#FF4136'
        ),
        opacity=1.0,      # ← Full opacity for visibility
        width=2.0,        # ← FIXED: Explicit bar width in days
        marker=dict(
            line=dict(width=0)  # ← No border lines
        )
    ), row=5, col=1)

    fig.add_hline(y=0, line_dash="solid", line_color="white", line_width=1, row=5, col=1)

    # Add ratio on secondary axis WITHOUT scaling
    if breadth_df['NH_NL_Ratio'].notna().any():
        fig.add_trace(go.Scatter(
            x=breadth_df['Date'],
            y=breadth_df['NH_NL_Ratio'],
            name='NH/NL Ratio',
            line=dict(color='#FFA15A', width=1.5, dash='dash'),
            yaxis='y5'
        ), row=5, col=1)

    fig.update_layout(
        height=1300,
        width=1280,
        title_text=f"📊 Market Breadth Dashboard | Source: {source}",
        title_x=0.5,
        template="plotly_dark",
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False,
        bargap=0.1,        # ← FIXED: Gap between bars for visibility
        bargroupgap=0.0    # ← FIXED: Gap between bars in same group
    )

    # Update Y-Axis Titles
    fig.update_yaxes(title_text="SPY Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="% of Universe", row=2, col=1, range=[0, 100])
    fig.update_yaxes(title_text="% of Universe", row=3, col=1, range=[0, 100])
    fig.update_yaxes(title_text="Net Count", row=4, col=1)
    fig.update_yaxes(title_text="Vol Ratio", side="right", overlaying="y4", row=4, col=1)
    fig.update_yaxes(title_text="Net New Highs", row=5, col=1)
    fig.update_yaxes(title_text="NH/NL Ratio", side="right", overlaying="y5", row=5, col=1)

    # Candlestick-specific styling
    fig.update_xaxes(row=1, col=1, rangeslider_visible=False)

    return fig


# ------------------------------------------------------------------
# Orchestration — same order as the legacy generate_breadth_dashboard,
# with the DuckDB connect swapped for the shared Flight-first fetch.
# ------------------------------------------------------------------


def generate_breadth_dashboard(
    config: PipelineConfig,
    years: Optional[int] = None,
    output_dir: Optional[str] = None,
) -> str:
    """Generate ``Stock_Ingestion_Market_Breadth.html`` and return its path.

    Args:
        config: ``PipelineConfig`` — Flight endpoint, DB path (read-only
            fallback only) and ``reports`` settings come from here.
        years: Trailing-years window.  ``None`` falls back to
            ``config.reports.market_breadth_years``; ``0`` means "all
            data" (the legacy ``years=None`` passthrough).  The cutoff
            is applied post-query so the SMA_200 / 252-day lookback
            windows keep their pre-cutoff history.
        output_dir: Target directory; defaults to
            ``config.reports.report_dir``.

    Returns:
        str: Path to the generated HTML file.

    Raises:
        RuntimeError: If a query fails on both the Flight path and the
            read-only DuckDB fallback.
        ValueError: If no breadth data returned or SPY is missing.
    """
    out_dir = (
        Path(output_dir)
        if output_dir is not None
        else Path(config.reports.report_dir)
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / OUTPUT_FILENAME

    if years is None:
        years = config.reports.market_breadth_years

    print(f"📁 Output: {output_path}")

    # Execute queries — Flight do_get first, direct READ-ONLY DuckDB
    # fallback (never a writable connection); a ``None`` return means
    # both paths failed.
    breadth_df = report_app.fetch_dataframe(build_breadth_sql(), config)
    if breadth_df is None:
        raise RuntimeError(
            "Error querying breadth metrics: Flight server unreachable "
            "and direct read-only DuckDB fallback failed."
        )
    spy_df = report_app.fetch_dataframe(build_spy_sql(), config)
    if spy_df is None:
        raise RuntimeError(
            "Error querying SPY data: Flight server unreachable and "
            "direct read-only DuckDB fallback failed."
        )

    if breadth_df.empty:
        raise ValueError("No breadth data returned from query. Check table name and columns.")
    if spy_df.empty:
        raise ValueError("No SPY data found. Ensure 'SPY' ticker exists in daily_close table.")

    print(f"✅ Breadth data: {len(breadth_df)} rows")
    print(f"✅ SPY data: {len(spy_df)} rows")

    # Filter by years if specified (post-query — see module docstring).
    breadth_df, spy_df = filter_years(breadth_df, spy_df, years)

    # 50-day EMA for SPY (legacy seeding — SMA of first `period` values,
    # then recursive).
    spy_df['EMA_50'] = calculate_ema(spy_df['Close'], 50)

    # Breadth indicators (legacy formulas verbatim).
    breadth_df = compute_indicators(breadth_df)

    # Debug output
    print(f"📊 Net New Highs range: {breadth_df['Net_New_Highs'].min():.0f} to {breadth_df['Net_New_Highs'].max():.0f}")
    print(f"📊 NH/NL Ratio range: {breadth_df['NH_NL_Ratio'].min():.2f} to {breadth_df['NH_NL_Ratio'].max():.2f}")
    print(f"📊 Total bars to plot: {len(breadth_df)}")
    print(f"📊 New Highs/Lows sample (last 5 rows):")
    print(breadth_df[['Date', 'New_Highs', 'New_Lows', 'Net_New_Highs', 'NH_NL_Ratio']].tail(5))

    # Build the 5-subplot dashboard (legacy construction verbatim).
    fig = build_breadth_figure(
        breadth_df, spy_df, source=config.database.duckdb_filename
    )

    html_content = fig.to_html(include_plotlyjs=True, full_html=True)

    # Inject interpretation guide before closing body tag
    if '</body>' in html_content:
        html_content = html_content.replace('</body>', f'{BREADTH_GUIDE_HTML}\n</body>')

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"✅ Dashboard generated successfully!")
    print(f"📂 Open in browser: {output_path}")

    return str(output_path)


# ============================================================================
# MAIN EXECUTION (when run directly)
# ============================================================================
def main(argv: Optional[list] = None) -> int:
    """CLI entrypoint.

    Accepts one optional positional argument: the trailing-years
    override (e.g. ``python scripts/breadth_dashboard.py 5``; ``0`` =
    all data).  With no argument the configured
    ``reports.market_breadth_years`` default applies.
    """
    args = sys.argv[1:] if argv is None else list(argv)
    years_param = int(args[0]) if args else None

    try:
        config = PipelineConfig.from_yaml(_CONFIG_PATH)
    except Exception as e:
        print(f"\n❌ Error loading config ({_CONFIG_PATH}): {e}")
        return 1

    try:
        html_path = generate_breadth_dashboard(config, years=years_param)
        print(f"\n🎉 Done! Open {html_path} in your browser to view the dashboard.")
        return 0
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
