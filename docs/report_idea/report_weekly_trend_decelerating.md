```sql
/* =====================================================
   WEEKLY Deceleration Candidates Report
   - Only stocks with significantly depressed Relative Volume
   - Limited to the top 50 most liquid names (by weekly dollar volume)
   - Built on the same style as the Acceleration version
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

-- Aggregate into weekly bars
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
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= 0.50 THEN 'Strong Deceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= 0.70 THEN 'Deceleration'
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

-- Final output: only Deceleration candidates within the top 50 liquid names
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
  AND dollar_rank <= 50
ORDER BY rvol ASC, latest_dollar_volume DESC;   -- lowest RVOL first
```

---

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
- More signals → raise to `0.80` / `0.60`
