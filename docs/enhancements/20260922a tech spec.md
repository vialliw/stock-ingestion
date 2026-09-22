**Technical Specification**  
**Title:** Interactive Multi-Asset Correlation Dashboard  
**Version:** 1.0  
**Date:** 2026-09-22  
**Scope:** Visualization and analysis of correlations between `^TNX`, WTI (`CL=F`), short-rate proxy (`^IRX`), `SPY`, and `SMH`.

---

### 1. Objectives
- Ingest historical daily market data into DuckDB.
- Provide an interactive Streamlit dashboard for exploring:
  - Price/yield levels and normalized series
  - Full-period and rolling correlations (user-selectable window, default 60 days)
  - Multi-panel overlays and scatter plots
- Support dynamic window selection, date-range filtering, and pair selection.

### 2. Data Sources & Tickers
| Series              | Yahoo Ticker | Description                          | Notes |
|---------------------|--------------|--------------------------------------|-------|
| 10Y Treasury Yield  | `^TNX`      | CBOE 10-Year Treasury Yield Index    | Levels in % |
| WTI Crude           | `CL=F`      | Front-month WTI futures              | USD/bbl |
| S&P 500             | `SPY`       | SPDR S&P 500 ETF                     | USD |
| Semiconductors      | `SMH`       | VanEck Semiconductor ETF             | USD |
| Short-rate proxy    | `^IRX`      | 13-week T-bill yield                 | % (closest clean Yahoo proxy for policy/discount rates) |

**Ingestion frequency:** Daily (or on-demand refresh).  
**History:** From 2020-01-01 (or earliest available) to present.  
**Primary source:** Yahoo Finance via `yfinance` (or equivalent). Fallback: CSV export / FRED for rates if needed.

### 3. Data Ingestion & Storage (DuckDB)

#### 3.1 Schema
```sql
-- Raw OHLCV + adjusted close (keep only Close for this app)
CREATE TABLE IF NOT EXISTS market_prices (
    date        DATE PRIMARY KEY,
    ticker      VARCHAR,
    close       DOUBLE,
    volume      BIGINT,          -- optional
    ingested_at TIMESTAMP DEFAULT current_timestamp
);

-- Or wide format for convenience
CREATE TABLE IF NOT EXISTS market_wide (
    date        DATE PRIMARY KEY,
    tnx_10y     DOUBLE,   -- ^TNX
    wti         DOUBLE,   -- CL=F
    irx_3m      DOUBLE,   -- ^IRX
    spy         DOUBLE,
    smh         DOUBLE,
    ingested_at TIMESTAMP DEFAULT current_timestamp
);
```

#### 3.2 Ingestion Pipeline (Python)
- Use `yfinance` (or `pandas_datareader`) to download.
- Clean: forward-fill missing closes only within trading days; drop rows with any NaN after alignment.
- Handle known anomalies (e.g., WTI negative print in Apr 2020).
- Upsert into DuckDB (idempotent by date).
- Optional: store daily returns and log-returns as computed columns or views.

**Example skeleton:**
```python
import yfinance as yf
import duckdb
import pandas as pd

tickers = {"^TNX": "tnx_10y", "CL=F": "wti", "^IRX": "irx_3m", "SPY": "spy", "SMH": "smh"}
# download → align → clean → write to DuckDB
con = duckdb.connect("market_data.duckdb")
con.execute("CREATE OR REPLACE TABLE market_wide AS SELECT * FROM df")
```

**Refresh strategy:** Full reload on demand or incremental (last N days) via Streamlit button / scheduled job.

### 4. Application Architecture (Streamlit)

```
streamlit_app/
├── app.py                 # Main entry
├── data/
│   └── market_data.duckdb
├── utils/
│   ├── data_loader.py     # DuckDB queries + caching
│   ├── metrics.py         # Correlation, rolling, normalize
│   └── charts.py          # Plotly / Altair / Matplotlib helpers
└── requirements.txt
```

#### 4.1 Key Features
- **Sidebar controls**
  - Date range selector (default: full history)
  - Rolling window slider / number input (default = 60 trading days, range 10–252)
  - Asset multi-select (default: all five)
  - Correlation type: Levels | Daily Returns | Log Returns
  - Chart type toggles (heatmap, normalized, multi-panel, rolling, scatter)
  - “Refresh Data” button

- **Main panels** (tabs or vertical sections)
  1. **Overview Heatmaps** – static full-period + current-window correlation matrices
  2. **Normalized Prices** – base-100 line chart
  3. **Multi-Panel Overlays** – dual-axis (WTI vs TNX, SPY/SMH vs TNX, IRX vs TNX)
  4. **Rolling Correlation** – interactive line chart of selected pairs over the chosen window
  5. **Scatter Matrix / Pairwise** – selectable pairs with regression line + correlation annotation
  6. **Data Table** – downloadable filtered series

#### 4.2 Computation Requirements
- All calculations performed in-memory after DuckDB query (or via DuckDB SQL where efficient).
- Rolling correlation: `pandas.Series.rolling(window).corr()` or DuckDB window functions.
- Normalization: `(price / price.iloc[0]) * 100`
- Missing data: drop or interpolate only after user confirmation.
- Cache: `@st.cache_data` for expensive queries and rolling computations.

### 5. Non-Functional Requirements
- **Performance:** Full history (~1.7k rows × 5 series) must load and recompute rolling metrics in < 2 s on modest hardware.
- **Interactivity:** Window change or date filter triggers immediate re-render of affected charts.
- **Reproducibility:** Pin library versions; store ingestion timestamp.
- **Extensibility:** Easy addition of new tickers (update mapping + schema).
- **Deployment:** Local or Streamlit Community Cloud / Docker. DuckDB file can be committed or downloaded on first run.

### 6. Technology Stack
| Component          | Choice                  | Rationale |
|--------------------|-------------------------|-----------|
| Database           | DuckDB                  | Fast analytical queries, zero-config, pandas-friendly |
| Ingestion          | yfinance + pandas       | Simple Yahoo access |
| Frontend           | Streamlit               | Rapid interactive dashboards |
| Charting           | Plotly (preferred) or Altair | Native Streamlit interactivity, dual-axis, hover |
| Correlation engine | pandas / numpy          | Mature rolling stats |
| Caching            | st.cache_data           | Responsive UI |

### 7. Deliverables
1. DuckDB schema + ingestion script.
2. Streamlit application (`app.py` + utils).
3. `requirements.txt` (duckdb, streamlit, yfinance, pandas, plotly, numpy, …).
4. README with run instructions and data-refresh notes.
5. Optional: unit tests for correlation math and data cleaning.

### 8. Future Enhancements (out of scope for v1)
- Real-time / intraday data
- Additional rate series from FRED (true Discount Rate, EFFR)
- Regime detection / correlation clustering
- Export of rolling correlation time series
- Multi-user auth / saved views

---

This specification is ready for implementation. The existing static charts (heatmap, normalized, multi-panel, rolling, scatters) map 1:1 onto the interactive Streamlit panels with a user-controlled window (default 60 days).