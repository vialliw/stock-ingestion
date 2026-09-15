"""
=============================================================================
RELATIVE ROTATION GRAPH (RRG) DASHBOARD GENERATOR (stock-ingestion port)
=============================================================================

Flight-sourced port of the legacy
``projects\\others\\plot_rrg_chart_gemini10b.py``.  The SQL semantics,
RRG math (rs / rs_ratio / rs_momentum / valid_mask / dynamic axis
limit), the 4-row animated figure construction (spline trails +
diamond heads, benchmark bars, stats table, static guide table,
per-date frames + slider + play/pause, quadrant rects + annotations,
1500x1500), the ``gen_ai_prompt`` JSON payload builder and the
``export_rotation_summary`` text export are copied VERBATIM — the
only changes are:

* the data source: instead of ``duckdb.connect(db_path)`` directly,
  the ``weekly_close`` query goes through the shared fetch path in
  ``scripts/report_app.py`` (``fetch_dataframe`` — Arrow Flight
  ``do_get`` JSON ``{"sql"}`` ticket first, direct *read-only*
  DuckDB fallback).  This module never opens a writable DuckDB
  connection.
* the ``Ticker IN (...)`` list is built with a safe quoted join
  (``'A','B'``) instead of the legacy ``tuple(all_t)`` repr — the
  tuple form produces the fragile ``('X',)`` singleton syntax and
  embeds raw, unvalidated symbol text.
* tickers absent from the fetched data are dropped with a printed
  warning (the legacy would raise ``KeyError``); a missing benchmark
  column raises ``ValueError``.

Requires the persisted ``weekly_close`` table (F12-T1 — rebuilt by
the ``weekly_refresh`` Flight action, the automatic weekly bridge on
loads, or ``scripts/refresh_weekly.py``).

Six presets mirror the legacy ``__main__`` block verbatim (``ETFs``,
``sector1``, ``sector2``, ``sector3``, ``Semi``, ``AI`` — all
``period=14``, ``trail=3``) and write
``{report_dir}/{prefix}.html`` plus
``{prefix}_analysis_prompt.json``.

Usage:
    python scripts/rrg_dashboard.py                    # all 6 presets
    python scripts/rrg_dashboard.py --dashboard sector1
    python scripts/rrg_dashboard.py --dashboard Semi --trail 5 --years 1

    from rrg_dashboard import generate_rrg_dashboard
    paths = generate_rrg_dashboard(config, "ETFs")

The module is importable without plotly or scipy:
``plotly.graph_objects`` / ``make_subplots`` are imported lazily
inside :func:`build_rrg_figure` and ``scipy.interpolate`` inside
:func:`get_spline` (same pattern as the lazy imports in
``scripts/breadth_dashboard.py`` / ``scripts/report_app.py``).

Author: Relative Rotation Dashboard Tool (ported by stock-ingestion F13-T1)
=============================================================================
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Sequence

# Ensure the repo root (for ``src.config``) and this script's own
# directory (for the ``report_app`` sibling import) are on sys.path
# regardless of the caller's working directory — mirrors the header in
# scripts/daily_load_once.py / scripts/breadth_dashboard.py.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd

import report_app
from src.config import PipelineConfig

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")

# Ticker symbols allowed into the SQL IN-list: uppercase letters,
# digits, dot and dash only (e.g. "BRK.B", "BF-B").  Anything else is
# rejected before it can reach the query text.
_TICKER_RE = re.compile(r"^[A-Z0-9.\-]+$")

# The six legacy __main__ configurations, verbatim: same ticker
# strings (comma-separated, exactly as the legacy ``ticker_list``
# arguments), benchmarks, lookback depths, period=14, trail=3 and
# output prefixes.
RRG_PRESETS: Dict[str, dict] = {
    "ETFs": {
        "tickers": (
            "AMLP,ARKK,AWAY,BATT,BCD,CANE,CIBR,CLOU,COMT,COPX,CORN,"
            "CPER,DBB,DBC,DIA,EFA,EWA,EWC,EWZ,FCG,FTGC,GDX,GLD,GRID,"
            "HERO,HYG,IAI,IBB,ICLN,IEO,IFRA,IHI,INDA,ITB,ITEQ,IYF,IYT,"
            "JETS,JNK,KRE,LIT,NANR,PALL,PBW,PLTM,QCLN,REMX,SCHH,SGOV,"
            "SLV,SLX,SMH,SMOG,SOYB,TAN,UNG,USO,VBK,VBR,VDE,VHT,VNM,"
            "VOE,VOT,VT,VTV,WEAT,XAR,XBI,XES,XLB,XLC,XLF,XLI,XLK,XLP,"
            "XLRE,XLU,XLV,XLY,XME,XPH,XRT,XSW,XTN"
        ),
        "benchmark": "SPY",
        "lookback_years": 1,
        "period": 14,
        "trail": 3,
        "prefix": "Relative_Rotation_Dashboard_ETFs",
    },
    "sector1": {
        "tickers": (
            "VTV,IYT,XBI,XLB,XLC,XLE,XLI,XLK,XLP,XLRE,XLU,XLY,XME,GDX,"
            "XPH,SMH,CIBR"
        ),
        "benchmark": "SPY",
        "lookback_years": 3,
        "period": 14,
        "trail": 3,
        "prefix": "Relative_Rotation_Dashboard_sector1",
    },
    "sector2": {
        "tickers": (
            "VTV,IYT,XBI,XLB,XLC,XLE,XLI,XLK,XLP,XLRE,XLU,XLY,XME,GDX,"
            "XPH,SMH,SPY"
        ),
        "benchmark": "SGOV",
        "lookback_years": 3,
        "period": 14,
        "trail": 3,
        "prefix": "Relative_Rotation_Dashboard_sector2",
    },
    "sector3": {
        "tickers": "XLK,IWM,DIA,TLT,GLD,SLV,SGOV",
        "benchmark": "SPY",
        "lookback_years": 3,
        "period": 14,
        "trail": 3,
        "prefix": "Relative_Rotation_Dashboard_sector3",
    },
    "Semi": {
        "tickers": (
            "ADI,ALAB,ALGM,AMAT,AMD,ASML,ASX,AVGO,CDNS,CRDO,ENTG,INTC,"
            "KLAC,LRCX,LSCC,MCHP,MPWR,MRVL,MTSI,MU,NVDA,NVMI,NXPI,OLED,"
            "ON,POWI,QCOM,QRVO,SLAB,SNPS,STM,SWKS,TER,TSM,TXN"
        ),
        "benchmark": "SMH",
        "lookback_years": 3,
        "period": 14,
        "trail": 3,
        "prefix": "Relative_Rotation_Dashboard_Semi",
    },
    "AI": {
        "tickers": (
            "XLK,NVDA,TSM,ARM,MSFT,GOOGL,AMZN,ORCL,AVGO,ANET,PLTR,META,"
            "PEG,PWR,VRT,VST,NRG,CEG,TLN,CIEN,CRDO,CLS,FIX,WCC,HPE,SMCI,"
            "ETN,CRWV,NBIS,COHR,LITE,NOK,RIOT,SMH,INTC,AMD,CORZ,HIVE,"
            "IREN,HUT,CLSK"
        ),
        "benchmark": "XLK",
        "lookback_years": 3,
        "period": 14,
        "trail": 3,
        "prefix": "Relative_Rotation_Dashboard_AI",
    },
}


# ------------------------------------------------------------------
# SQL builder — legacy query semantics (the tuple-repr IN list is
# replaced by a validated quoted join).
# ------------------------------------------------------------------


def build_rrg_sql(
    tickers: Sequence[str], benchmark: str, lookback_years: int
) -> str:
    """Weekly-close query for one RRG preset — legacy semantics.

    Emits ``SELECT Date, Ticker, "Close" FROM weekly_close WHERE
    Ticker IN (...) AND Date >= '<today - years*365d>' ORDER BY Date
    ASC`` — the legacy query verbatim except that the ticker list is a
    safe quoted join (``'A','B'``) instead of ``tuple(all_t)`` repr.
    Every symbol (tickers *and* benchmark — the benchmark is always
    appended to the IN list, as in the legacy ``all_t``) must match
    ``^[A-Z0-9.\\-]+$`` or ``ValueError`` is raised.
    """
    if isinstance(lookback_years, bool) or int(lookback_years) != lookback_years:
        raise TypeError(
            f"lookback_years must be an integer, got {lookback_years!r}"
        )
    lookback_years = int(lookback_years)
    if lookback_years < 1:
        raise ValueError(f"lookback_years must be >= 1, got {lookback_years}")

    all_t = list(tickers) + [benchmark]
    # justified: fixed-size symbol universe (a preset config list, not
    # table rows) — every symbol must be regex-validated before it is
    # quoted into the SQL text.
    for t in all_t:
        if not isinstance(t, str) or not _TICKER_RE.match(t):
            raise ValueError(
                f"Invalid ticker symbol for the SQL IN-list: {t!r}"
            )

    lookback_start = (
        datetime.today() - timedelta(days=lookback_years * 365)
    ).strftime("%Y-%m-%d")
    in_list = ",".join(f"'{t}'" for t in all_t)
    return (
        f'SELECT Date, Ticker, "Close" FROM weekly_close '
        f"WHERE Ticker IN ({in_list}) AND Date >= '{lookback_start}' "
        f"ORDER BY Date ASC"
    )


def pivot_closes(df: pd.DataFrame) -> pd.DataFrame:
    """Wide close matrix — legacy verbatim:
    ``pivot(index='Date', columns='Ticker', values='Close').ffill()``.

    Data-source normalization (NOT a logic change): the legacy read via
    duckdb ``.df()`` which converts DATE columns to ``datetime64``;
    the Arrow Flight path delivers ``datetime.date`` objects (object
    dtype).  Coerce ``Date`` to ``datetime64`` first so the pivot index
    is a ``DatetimeIndex`` — downstream code (``gen_ai_prompt``,
    ``export_rotation_summary``, frame names/slider labels, xaxis2
    range) assumes ``Timestamp`` semantics such as ``.date()``.
    """
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    return df.pivot(index="Date", columns="Ticker", values="Close").ffill()


# ------------------------------------------------------------------
# RRG math — VERBATIM from the legacy plot_rrg_chart_gemini10b.py
# (approved deviation: tickers absent from the fetched data are
# dropped with a warning instead of raising KeyError; a missing
# benchmark column raises ValueError).
# ------------------------------------------------------------------


def compute_rrg(
    df_pivot: pd.DataFrame,
    tickers: Sequence[str],
    benchmark: str,
    period: int,
) -> dict:
    """Compute the RRG series — legacy math verbatim.

    Returns a dict with ``rs``, ``rs_ratio``, ``rs_momentum``,
    ``valid_mask``, ``clean_ratio``, ``clean_momo``, ``dates``,
    ``limit`` and ``tickers`` (the resolved ticker list after the
    missing-ticker drop).
    """
    if benchmark not in df_pivot.columns:
        raise ValueError(
            f"Benchmark ticker {benchmark!r} is not present in the "
            f"fetched weekly_close data."
        )
    # DEVIATION (approved): the legacy indexes df_pivot[tickers]
    # directly and would KeyError on an absent ticker; the port drops
    # absent tickers with a printed warning so a missing symbol cannot
    # sink the whole dashboard.
    missing = [t for t in tickers if t not in df_pivot.columns]
    if missing:
        print(
            f"⚠️ {len(missing)} ticker(s) absent from the fetched "
            f"weekly_close data — dropped: {missing}"
        )
    tickers = [t for t in tickers if t in df_pivot.columns]
    if not tickers:
        raise ValueError(
            "None of the requested tickers are present in the fetched "
            "weekly_close data."
        )

    # 2. RRG Calculations (verbatim)
    rs = df_pivot[tickers].div(df_pivot[benchmark], axis=0)
    rs_ratio = 100 + (rs.pct_change(period).rolling(3).mean() * 100)
    rs_momentum = 100 + (rs_ratio.pct_change(period).rolling(3).mean() * 100)

    valid_mask = rs_ratio.notna().all(axis=1) & rs_momentum.notna().all(axis=1)
    clean_ratio = rs_ratio[valid_mask]
    clean_momo = rs_momentum[valid_mask]
    dates = clean_ratio.index.tolist()

    # 3. Dynamic Scaling (verbatim)
    all_vals = np.concatenate([clean_ratio.values, clean_momo.values])
    all_vals = all_vals[np.isfinite(all_vals)]
    max_dev = max(abs(all_vals.max() - 100), abs(all_vals.min() - 100)) / 0.9
    limit = [100 - max_dev, 100 + max_dev]

    return {
        "rs": rs,
        "rs_ratio": rs_ratio,
        "rs_momentum": rs_momentum,
        "valid_mask": valid_mask,
        "clean_ratio": clean_ratio,
        "clean_momo": clean_momo,
        "dates": dates,
        "limit": limit,
        "tickers": tickers,
    }


def get_spline(x, y):
    """Smooth a trail segment — legacy verbatim (scipy quadratic
    ``make_interp_spline`` resampled to 25 points; fewer than 3 finite
    points pass through unsplined).  ``scipy`` is imported lazily so
    this module stays importable without it.
    """
    from scipy.interpolate import make_interp_spline

    mask = np.isfinite(x) & np.isfinite(y)
    xf, yf = x[mask], y[mask]
    if len(xf) < 3:
        return xf, yf
    t = np.linspace(0, 1, len(xf))
    t_new = np.linspace(0, 1, 25)
    return make_interp_spline(t, xf, k=2)(t_new), make_interp_spline(t, yf, k=2)(
        t_new
    )


# ------------------------------------------------------------------
# Figure construction — VERBATIM from the legacy
# plot_rrg_chart_gemini10b.py (4-row subplot, 13-color cycle, spline
# trails + diamond heads, benchmark bars, stats table, static guide
# table, per-date frames + slider + play/pause, quadrant rects +
# annotations, 1500x1500).  ``dates`` and ``limit`` are recomputed
# here with the same verbatim expressions the legacy ran inline.
# ------------------------------------------------------------------


def build_rrg_figure(
    clean_ratio: pd.DataFrame,
    clean_momo: pd.DataFrame,
    df_pivot: pd.DataFrame,
    tickers: Sequence[str],
    benchmark: str,
    trail: int,
):
    """Build the animated 4-row RRG figure (legacy construction
    verbatim).  ``plotly`` is imported lazily so this module stays
    importable without it."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # 13 Distinct Colors (verbatim)
    colors = [
        '#FF0000', '#FF007F', '#FF7F00', '#FF00FF', '#FFFF00',
        '#7FFF00', '#00FF00', '#00FF7F', '#00FFFF', '#007FFF',
        '#0000FF', '#4B0082', '#8B00FF'
    ]

    dates = clean_ratio.index.tolist()

    # Dynamic scaling — same verbatim expressions as compute_rrg.
    all_vals = np.concatenate([clean_ratio.values, clean_momo.values])
    all_vals = all_vals[np.isfinite(all_vals)]
    max_dev = max(abs(all_vals.max() - 100), abs(all_vals.min() - 100)) / 0.9
    limit = [100 - max_dev, 100 + max_dev]

    # 4. Create Subplots (verbatim)
    fig = make_subplots(
        rows=4, cols=1,
        row_heights=[0.50, 0.09, 0.15, 0.26],
        vertical_spacing=0.05,
        specs=[[{"type": "xy"}], [{"type": "xy"}], [{"type": "table"}], [{"type": "table"}]]
    )

    # Register Base Traces (verbatim)
    # justified: one scatter trace per ticker — plotly has no vectorized
    # multi-series registration, and the 13-color cycle must be applied
    # per trace (enumerate is also check_loops-exempt).
    for idx, t in enumerate(tickers):
        # FIX: MODULO APPLIED TO HANDLE UNLIMITED TICKERS
        color = colors[idx % len(colors)]
        fig.add_trace(go.Scatter(
            x=[], y=[], mode='lines+markers', name=t,
            line=dict(width=2, color=color),
            marker=dict(size=6, opacity=0.5, color=color),
            hovertemplate=f"<b>{t}</b><br>Ratio: %{{x:.2f}}<br>Momentum: %{{y:.2f}}<extra></extra>"
        ), row=1, col=1)

    # justified: per-ticker diamond head marker trace — same
    # registration constraint as the trail traces above.
    for idx, t in enumerate(tickers):
        # FIX: MODULO APPLIED TO HANDLE UNLIMITED TICKERS
        color = colors[idx % len(colors)]
        fig.add_trace(go.Scatter(
            x=[], y=[], mode='markers+text', text=[t],
            marker=dict(size=12, symbol='diamond', color=color),
            textposition="top center", showlegend=False,
            hovertemplate=f"<b>{t}</b><br>Ratio: %{{x:.2f}}<br>Momentum: %{{y:.2f}}<extra></extra>"
        ), row=1, col=1)

    fig.add_trace(go.Bar(x=dates, y=df_pivot.loc[dates, benchmark], marker_color='darkblue', showlegend=False), row=2, col=1)
    fig.add_trace(go.Bar(x=[], y=[], marker_color='orange', showlegend=False), row=2, col=1)

    # Stats Table (verbatim)
    fig.add_trace(go.Table(
        header=dict(values=['Ticker', 'Ratio', 'Momentum', 'Velocity'], fill_color='#1f1f1f', font=dict(color='white')),
        cells=dict(values=[[], [], [], []])
    ), row=3, col=1)

    # Trader Guide (Static) (verbatim)
    guide_data = [
        ['<b>LEADING</b>', '<b>WEAKENING</b>', '<b>LAGGING</b>', '<b>IMPROVING</b>', '<b>UP (↑)</b>', '<b>DOWN (↓)</b>', '<b>RIGHT (→)</b>', '<b>LEFT (←)</b>'],
        ['Strong Outperformance', 'Exhaustion Phase', 'Deep Underperformance', 'Early Recovery', 'Momentum Gaining', 'Momentum Fading', 'Outperforming Benchmark, RS ↑', 'Underperforming Benchmark, RS ↓'],
        ['<b>BUY / HOLD</b>', '<b>TRIM / PROTECT</b>', '<b>SELL / AVOID</b>', '<b>WATCH / SPECULATE</b>', 'Trend Accelerating', 'Trend Decelerating', 'Outperforming', 'Underperforming']
    ]
    fig.add_trace(go.Table(
        header=dict(values=['Quadrant/Direction', 'Market Context', 'Trader Action'], fill_color='#2c3e50', font=dict(color='white')),
        cells=dict(values=guide_data, fill_color='#f8f9fa')
    ), row=4, col=1)

    # 5. Animation Frames (verbatim)
    frames = []
    # justified: one go.Frame object must be constructed per animation
    # step — no vectorized API (range is also check_loops-exempt).
    for i in range(trail, len(dates)):
        curr_date = dates[i]
        tail_slice = slice(i - trail + 1, i + 1)
        frame_data = []

        # Update RRG Lines
        # justified: per-ticker trail trace inside the frame — each
        # series needs its own spline + styled Scatter object.
        for idx, t in enumerate(tickers):
            # FIX: MODULO APPLIED
            color = colors[idx % len(colors)]
            xr, yr = clean_ratio[t].iloc[tail_slice].values, clean_momo[t].iloc[tail_slice].values
            xs, ys = get_spline(xr, yr)
            frame_data.append(go.Scatter(
                x=xs, y=ys, line=dict(color=color),
                marker=dict(color=color, opacity=[1 if j % 6 == 0 else 0 for j in range(len(xs))]),
                hovertemplate=f"<b>{t}</b><br>Ratio: %{{x:.2f}}<br>Momo: %{{y:.2f}}<extra></extra>"
            ))

        # Update RRG Heads
        # justified: per-ticker head marker inside the frame — same
        # per-series Scatter requirement.
        for idx, t in enumerate(tickers):
            # FIX: MODULO APPLIED
            color = colors[idx % len(colors)]
            frame_data.append(go.Scatter(
                x=[clean_ratio[t].iloc[i]], y=[clean_momo[t].iloc[i]],
                marker=dict(color=color, symbol='diamond'),
                hovertemplate=f"<b>{t}</b><br>Ratio: %{{x:.2f}}<br>Momentum: %{{y:.2f}}<extra></extra>"
            ))

        frame_data.append(go.Bar(x=dates, y=df_pivot.loc[dates, benchmark]))
        frame_data.append(go.Bar(x=dates[tail_slice], y=df_pivot.loc[dates[tail_slice], benchmark]))

        # Table Data Update
        v_vec = np.sqrt((clean_ratio.iloc[i] - clean_ratio.iloc[i - 1]) ** 2 + (clean_momo.iloc[i] - clean_momo.iloc[i - 1]) ** 2)
        stats = pd.DataFrame({'Ticker': tickers, 'Ratio': clean_ratio.iloc[i].values, 'Momo': clean_momo.iloc[i].values, 'Vel': v_vec.values}).sort_values('Ratio', ascending=False)
        frame_data.append(go.Table(cells=dict(values=[stats['Ticker'].tolist(), stats['Ratio'].round(2).tolist(), stats['Momo'].round(2).tolist(), stats['Vel'].round(2).tolist()])))
        frame_data.append(go.Table(cells=dict(values=guide_data)))

        frames.append(go.Frame(data=frame_data, name=str(curr_date)))

    # 6. Layout (verbatim)
    fig.update_layout(
        title=dict(text=f"<b>Relative Rotation Analysis Dashboard (Benchmark: {benchmark})</b>", x=0.5, font=dict(size=24)),
        width=1500, height=1500,
        xaxis1=dict(range=limit, zeroline=True, zerolinecolor='black', title="RS-Ratio (Trend)",
                    dtick=10, gridcolor='#BDBDBD', gridwidth=1),
        yaxis1=dict(range=limit, zeroline=True, zerolinecolor='black', title="RS-Momentum",
                    dtick=10, gridcolor='#BDBDBD', gridwidth=1),
        xaxis2=dict(range=[dates[0], dates[-1]]),
        yaxis2=dict(visible=False),
        template="plotly_white",
        updatemenus=[{
            "type": "buttons", "x": 0.06, "y": 0.99,
            "buttons": [
                {"label": "▶ Play", "method": "animate", "args": [None, {"frame": {"duration": 100, "redraw": True}}]},
                {"label": "⏸ Pause", "method": "animate", "args": [[None], {"mode": "immediate"}]}
            ]
        }],
        sliders=[{
            "active": len(frames) - 1,
            "x": 0.06, "y": 0.66, "len": 0.92,
            "steps": [{"args": [[f.name], {"mode": "immediate"}], "label": f.name.split(' ')[0], "method": "animate"} for f in frames]
        }]
    )

    # Background Quadrants (verbatim)
    quads = [("rgba(0,255,0,0.08)", 100, limit[1], 100, limit[1], "LEADING"),
             ("rgba(255,165,0,0.08)", 100, limit[1], limit[0], 100, "WEAKENING"),
             ("rgba(255,0,0,0.08)", limit[0], 100, limit[0], 100, "LAGGING"),
             ("rgba(0,0,255,0.08)", limit[0], 100, 100, limit[1], "IMPROVING")]

    # justified: fixed 4-element constant list — one rect shape + one
    # text annotation per quadrant (constant-literal iteration is also
    # check_loops-exempt).
    for clr, x0, x1, y0, y1, txt in quads:
        fig.add_shape(type="rect", x0=x0, y0=y0, x1=x1, y1=y1, fillcolor=clr, line_width=0, layer="below", row=1, col=1)
        fig.add_annotation(x=(x0 + x1) / 2, y=(y0 + y1) / 2, text=txt, showarrow=False, font=dict(size=40, color="grey"), opacity=0.3, row=1, col=1)

    fig.frames = frames
    return fig


# ------------------------------------------------------------------
# LLM payload + text export — VERBATIM from the legacy
# plot_rrg_chart_gemini10b.py.
# ------------------------------------------------------------------


def gen_ai_prompt(clean_ratio, clean_momo, benchmark="SPY"):
    """
    Constructs a structured JSON payload containing RRG technical data,
    persistence metrics, and institutional-grade system instructions
    for LLM-based market analysis.  (Legacy verbatim; the closure-
    captured ``benchmark_name`` became a parameter.)
    """
    latest_date = clean_ratio.index[-1]
    tickers = clean_ratio.columns.tolist()
    lookback = 12  # Consistent with 3-month institutional reporting

    # 1. Build the Data Snapshot
    ticker_analysis = {}
    # justified: per-ticker dict assembly — each entry mixes scalar
    # reductions and a nested structure, not a frame transform.
    for t in tickers:
        # Current and Previous values for trajectory
        r_curr, r_prev = clean_ratio[t].iloc[-1], clean_ratio[t].iloc[-2]
        m_curr, m_prev = clean_momo[t].iloc[-1], clean_momo[t].iloc[-2]

        # Velocity and Angle (Heading)
        velocity = np.sqrt((r_curr - r_prev) ** 2 + (m_curr - m_prev) ** 2)
        angle = np.degrees(np.arctan2((m_curr - m_prev), (r_curr - r_prev)))

        # Quadrant Logic
        if r_curr >= 100 and m_curr >= 100:
            quad = "LEADING"
        elif r_curr >= 100 and m_curr < 100:
            quad = "WEAKENING"
        elif r_curr < 100 and m_curr < 100:
            quad = "LAGGING"
        else:
            quad = "IMPROVING"

        # Persistence (Historical Consistency)
        hist_r = clean_ratio[t].iloc[-lookback:]
        hist_m = clean_momo[t].iloc[-lookback:]
        lead_wks = int(((hist_r >= 100) & (hist_m >= 100)).sum())
        lag_wks = int(((hist_r < 100) & (hist_m < 100)).sum())

        ticker_analysis[t] = {
            "current_position": {
                "rs_ratio": round(r_curr, 2),
                "rs_momentum": round(m_curr, 2),
                "quadrant": quad,
                "angle_deg": round(angle, 1)
            },
            "trajectory": {
                "velocity_1w": round(velocity, 2),
                "velocity_direction": "ACCELERATING" if velocity > np.sqrt((clean_ratio[t].iloc[-2] - clean_ratio[t].iloc[-3]) ** 2 + (clean_momo[t].iloc[-2] - clean_momo[t].iloc[-3]) ** 2) else "DECELERATING"
            },
            "persistence": {
                "lead_weeks_3m": lead_wks,
                "lag_weeks_3m": lag_wks,
                "consistency_score": round((lead_wks / lookback) * 100, 1)
            }
        }

    # 2. Define the Institutional Prompt Wrapper
    full_payload = {
        "system_prompt": "You are an institutional market technician specializing in relative rotation analysis, sector rotation, and momentum regimes.",
        "analysis_query": "Analyze the RRG data to identify consistent leaders, high-velocity improvers, decaying momentum, and actionable portfolio tilts.",
        "instructions": {
            "focus_priority": ["leadership_regime", "improving_trends", "weakening_momentum", "velocity_analysis"],
            "interpretation_rules": {
                "lead_quadrant": "Strong RS and momentum - overweight",
                "improving_quadrant": "Building momentum - tactical entry watch",
                "weakening_quadrant": "Fading momentum - reduce exposure",
                "lagging_quadrant": "Weak RS and momentum - avoid"
            },
            "response_style": "institutional technical concise actionable"
        },
        "market_snapshot": {
            "analysis_date": str(latest_date.date()),
            "benchmark": benchmark,
            "lookback_weeks": lookback,
            "ticker_data": ticker_analysis
        }
    }

    return json.dumps(full_payload, indent=2)


def export_rotation_summary(clean_ratio, clean_momo, output_path, dashboard_prefix):
    """
    Summarizes 12 weeks of RRG trajectory into a minimalist
    pipe-delimited format for efficient LLM context injection.
    (Ported verbatim — the closure-captured ``output_path`` /
    ``dashboard_prefix`` became parameters.  Deliberately NOT invoked:
    the legacy call site is commented out upstream, and the same
    parity is kept in :func:`generate_rrg_dashboard`.)
    """
    latest_date = clean_ratio.index[-1].date()
    tickers = clean_ratio.columns
    lookback = 12  # Approximately 3 months of weekly data

    summary = [f"RRG_SUMMARY|DATE:{latest_date}|LOOKBACK:12W"]
    summary.append("TICKER|QUAD|RATIO|MOMENTUM|VELOCITY_1WEEK|ANGLE|LEAD_WEEKS|LAG_WEEKS")

    # justified: one pipe-delimited line per ticker — per-column string
    # assembly, not a vectorizable frame transform.
    for t in tickers:
        # Latest values for velocity and heading
        r_curr, r_prev = clean_ratio[t].iloc[-1], clean_ratio[t].iloc[-2]
        m_curr, m_prev = clean_momo[t].iloc[-1], clean_momo[t].iloc[-2]

        velocity = np.sqrt((r_curr - r_prev) ** 2 + (m_curr - m_prev) ** 2)
        angle = np.degrees(np.arctan2((m_curr - m_prev), (r_curr - r_prev)))

        # Current Quadrant Label
        if r_curr >= 100 and m_curr >= 100:
            quad = "LEAD"
        elif r_curr >= 100 and m_curr < 100:
            quad = "WEAK"
        elif r_curr < 100 and m_curr < 100:
            quad = "LAG"
        else:
            quad = "IMPR"

        # Historical Consistency (3-Month Lookback)
        hist_r = clean_ratio[t].iloc[-lookback:]
        hist_m = clean_momo[t].iloc[-lookback:]

        lead_wks = ((hist_r >= 100) & (hist_m >= 100)).sum()
        lag_wks = ((hist_r < 100) & (hist_m < 100)).sum()

        summary.append(f"{t}|{quad}|{r_curr:.2f}|{m_curr:.2f}|{velocity:.2f}|{angle:.0f}|{lead_wks}|{lag_wks}")

    SUMMARY_TEXT = "\n".join(summary)
    PROMPT = "Act as a Market Technician. Analyze these 12 weeks ofRRG stats. Identify consistent leaders (high LEAD_WEEKS), high-velocity improvers, and decaying momentum in WEAK/LAG; Summarize the groups belonging to Leadership Regime, Secondary Strength, Improving, and Avoid / Underweight; Suggest Actionable Technician’s Framework (Core overweight, Tactical adds, Fade / hedge, Risk signal); Rank best LEAD entries vs late-cycle exhaustion risks; Translate into portfolio tilt recommendations (risk-on vs defensive)."

    with open(f'{output_path}{os.sep}{dashboard_prefix}.txt', "w") as f:
        f.write(f"{PROMPT}\n\n{SUMMARY_TEXT}")
    return SUMMARY_TEXT


# ------------------------------------------------------------------
# Orchestration — same order as the legacy __main__ calls, with the
# DuckDB connect swapped for the shared Flight-first fetch.
# ------------------------------------------------------------------


def generate_rrg_dashboard(
    config: PipelineConfig,
    preset_name: str,
    period: Optional[int] = None,
    trail: Optional[int] = None,
    lookback_years: Optional[int] = None,
    benchmark: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """Generate one RRG dashboard preset; return its artifact paths.

    Args:
        config: ``PipelineConfig`` — Flight endpoint, DB path
            (read-only fallback only) and ``reports.report_dir``.
        preset_name: Key into :data:`RRG_PRESETS`.
        period / trail / lookback_years / benchmark: ``None`` falls
            back to the preset's values (14 / 3 / preset / preset).
        output_dir: Target directory; defaults to
            ``config.reports.report_dir``.

    Returns:
        dict: ``{"html": ..., "json": ...}`` — paths of the written
        ``{prefix}.html`` and ``{prefix}_analysis_prompt.json``.

    Raises:
        ValueError: Unknown preset, no data returned, missing
            benchmark, or no requested tickers present.
        RuntimeError: The query failed on both the Flight path and the
            read-only DuckDB fallback.
    """
    if preset_name not in RRG_PRESETS:
        raise ValueError(
            f"Unknown RRG preset {preset_name!r}; expected one of "
            f"{sorted(RRG_PRESETS)}"
        )
    preset = RRG_PRESETS[preset_name]

    period = preset["period"] if period is None else period
    trail = preset["trail"] if trail is None else trail
    lookback_years = (
        preset["lookback_years"] if lookback_years is None else lookback_years
    )
    benchmark = preset["benchmark"] if benchmark is None else benchmark
    prefix = preset["prefix"]
    tickers = preset["tickers"].split(",")

    out_dir = (
        Path(output_dir)
        if output_dir is not None
        else Path(config.reports.report_dir)
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / f"{prefix}.html"
    json_path = out_dir / f"{prefix}_analysis_prompt.json"

    print(f"📁 Output: {html_path}")

    # Fetch — Flight do_get first, direct READ-ONLY DuckDB fallback
    # (never a writable connection); ``None`` means both paths failed.
    df = report_app.fetch_dataframe(
        build_rrg_sql(tickers, benchmark, lookback_years), config
    )
    if df is None:
        raise RuntimeError(
            "Error querying weekly_close: Flight server unreachable "
            "and direct read-only DuckDB fallback failed."
        )
    if df.empty:
        raise ValueError(
            f"No weekly_close data returned for preset "
            f"{preset_name!r} (benchmark {benchmark!r}, "
            f"{lookback_years}y lookback)."
        )

    print(f"✅ weekly_close data: {len(df)} rows")

    df_pivot = pivot_closes(df)
    rrg = compute_rrg(df_pivot, tickers, benchmark, period)
    fig = build_rrg_figure(
        rrg["clean_ratio"],
        rrg["clean_momo"],
        df_pivot,
        rrg["tickers"],
        benchmark,
        trail,
    )

    fig.write_html(
        str(html_path),
        full_html=True,
        include_plotlyjs="cdn",
        auto_open=False,
    )
    # Legacy parity — the upstream call is commented out too:
    # summary_text = export_rotation_summary(rrg["clean_ratio"], rrg["clean_momo"], str(out_dir), prefix)

    # Generate the JSON payload
    json_payload = gen_ai_prompt(rrg["clean_ratio"], rrg["clean_momo"], benchmark)
    with open(json_path, "w") as f:
        f.write(json_payload)
    print(f"JSON analysis file generated at: {json_path}")
    print(f"✅ Dashboard generated successfully!")
    print(f"📂 Open in browser: {html_path}")

    return {"html": str(html_path), "json": str(json_path)}


# ============================================================================
# MAIN EXECUTION (when run directly)
# ============================================================================
def main(argv: Optional[list] = None) -> int:
    """CLI entrypoint — generates one preset or ALL six.

    ``--dashboard NAME`` selects a single preset; when absent every
    preset in :data:`RRG_PRESETS` runs.  ``--period``, ``--trail``,
    ``--years`` and ``--benchmark`` override the preset values.
    Per-preset failures are logged and the batch continues; the exit
    code is 1 only when *every* selected preset failed.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Generate Relative Rotation (RRG) dashboard(s) from the "
            "persisted weekly_close table (Flight-first, read-only "
            "DuckDB fallback)."
        )
    )
    parser.add_argument(
        "--dashboard",
        metavar="NAME",
        default=None,
        help=(
            "RRG preset to run (default: all six). One of: "
            + ", ".join(RRG_PRESETS)
        ),
    )
    parser.add_argument(
        "--period",
        type=int,
        default=None,
        help="RRG period override (preset default: 14).",
    )
    parser.add_argument(
        "--trail",
        type=int,
        default=None,
        help="Trail length override (preset default: 3).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=None,
        help="Lookback-years override (preset default: 1 or 3).",
    )
    parser.add_argument(
        "--benchmark",
        default=None,
        help="Benchmark ticker override (preset default).",
    )
    args = parser.parse_args(argv)

    if args.dashboard is not None and args.dashboard not in RRG_PRESETS:
        print(
            f"\n❌ Unknown dashboard preset {args.dashboard!r}; "
            f"expected one of: {', '.join(RRG_PRESETS)}"
        )
        return 1

    try:
        config = PipelineConfig.from_yaml(_CONFIG_PATH)
    except Exception as e:
        print(f"\n❌ Error loading config ({_CONFIG_PATH}): {e}")
        return 1

    names = [args.dashboard] if args.dashboard else list(RRG_PRESETS)
    succeeded = []
    failed = []
    # justified: preset batch — each dashboard is an independent
    # generate call over a small fixed config list; a failure is
    # logged and the batch continues.
    for name in names:
        print(f"\n=== Generating RRG dashboard: {name} ===")
        try:
            paths = generate_rrg_dashboard(
                config,
                name,
                period=args.period,
                trail=args.trail,
                lookback_years=args.years,
                benchmark=args.benchmark,
            )
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            failed.append(name)
            continue
        succeeded.append(name)
        print(f"✅ {name}: {paths['html']}")

    print(
        f"\n{len(succeeded)}/{len(names)} preset(s) generated"
        + (f"; failed: {', '.join(failed)}" if failed else "")
    )
    if not succeeded:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
