```sql
/* =====================================================
   WEEKLY Combined Acceleration + Deceleration Report
   - Shows both high-RVOL (Acceleration) and low-RVOL (Deceleration)
   - Limited to the top 50 most liquid names only
   - Single clean result set with clear regime flag
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

weekly_with_change AS (
  SELECT 
    *,
    ((week_close - LAG(week_close) OVER (PARTITION BY Ticker ORDER BY week_start)) 
      / NULLIF(LAG(week_close) OVER (PARTITION BY Ticker ORDER BY week_start), 0)) * 100 AS week_pct_change
  FROM weekly_bars
),

last_30_weeks AS (
  SELECT 
    Ticker,
    week_start,
    week_trade_money,
    week_volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY week_start DESC) AS week_rank
  FROM weekly_with_change
),

avg_metrics AS (
  SELECT 
    Ticker,
    AVG(week_trade_money) AS avg_week_trade_money,
    AVG(week_volume) AS avg_week_volume
  FROM last_30_weeks
  WHERE week_rank <= 30
  GROUP BY Ticker
),

latest_week AS (
  SELECT 
    w.*,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY week_start DESC) AS rn
  FROM weekly_with_change w
),

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
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= 0.50 THEN 'Strong Deceleration'
      WHEN l.week_volume * 1.0 / NULLIF(a.avg_week_volume, 0) <= 0.70 THEN 'Deceleration'
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
    'Deceleration', 
    'Strong Deceleration'
  )
  AND dollar_rank <= 50
ORDER BY 
  CASE volume_regime
    WHEN 'Strong Acceleration' THEN 1
    WHEN 'Acceleration' THEN 2
    WHEN 'Deceleration' THEN 3
    WHEN 'Strong Deceleration' THEN 4
  END,
  rvol DESC;   -- within Acceleration: highest RVOL first
               -- within Deceleration: will still show higher RVOL first,
               -- you can change to ASC if you prefer lowest RVOL first for Decel
```

---

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

This single query gives you the complete weekly volume-regime picture among the most liquid stocks every weekend.