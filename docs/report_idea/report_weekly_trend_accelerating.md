```sql
/* =====================================================
   WEEKLY Acceleration Candidates Report
   - Only stocks with significantly elevated Relative Volume
   - Limited to the top 50 most liquid names (by weekly dollar volume)
   - Built on your original style
   ===================================================== */

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

-- Aggregate into weekly bars (Monday start – adjust if your engine uses different week start)
weekly_bars AS (
  SELECT 
    Ticker,
    DATE_TRUNC('week', Date) AS week_start,          -- change to DATE_TRUNC('week', Date + INTERVAL '1 day') if you prefer Sunday start
    MIN(Date) AS week_start_date,
    MAX(Date) AS week_end_date,
    
    -- OHLC
    (ARRAY_AGG(Open ORDER BY Date ASC))[1] AS week_open,
    MAX(High) AS week_high,
    MIN(Low) AS week_low,
    (ARRAY_AGG("Close" ORDER BY Date DESC))[1] AS week_close,
    
    -- Volume & Dollar Volume
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

-- Last 30 weeks per ticker
last_30_weeks AS (
  SELECT 
    Ticker,
    week_start,
    week_trade_money,
    week_volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY week_start DESC) AS week_rank
  FROM weekly_with_change
),

-- Average metrics over last 30 weeks
avg_metrics AS (
  SELECT 
    Ticker,
    AVG(week_trade_money) AS avg_week_trade_money,
    AVG(week_volume) AS avg_week_volume
  FROM last_30_weeks
  WHERE week_rank <= 30
  GROUP BY Ticker
),

-- Latest completed week
latest_week AS (
  SELECT 
    w.*,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY week_start DESC) AS rn
  FROM weekly_with_change w
),

-- Combine latest week + averages
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
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) >= 2.5 THEN 'Strong Acceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) >= 1.8 THEN 'Acceleration'
      ELSE 'Other'
    END AS volume_regime
  FROM latest_week l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

-- Rank by latest weekly dollar volume (liquidity filter)
ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY latest_dollar_volume DESC) AS dollar_rank
  FROM combined
)

-- Final output: only Acceleration candidates within the top 50 liquid names
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
  AND dollar_rank <= 50
ORDER BY rvol DESC, latest_dollar_volume DESC;
```

---

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
- Longer lookback → change `30` to `40` or `52` weeks

