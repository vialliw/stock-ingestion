Here is a practical SQL version of the **Volume Acceleration / Deceleration Edge Report**, built directly on top of your base query style.

It uses the same `trade_money` definition, 30-period lookback, and ranking logic, then adds the key edge metrics (latest dollar volume, Relative Volume, price change, and acceleration/deceleration flags).

```sql
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

-- Last 30 trading periods per ticker (for average)
last_30_periods AS (
  SELECT 
    Ticker,
    Date,
    trade_money,
    Volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS period_rank
  FROM trade_money
),

-- Average trade_money & average volume over last 30 periods
avg_metrics AS (
  SELECT 
    Ticker,
    AVG(trade_money) AS avg_trade_money,
    AVG(Volume)      AS avg_volume
  FROM last_30_periods
  WHERE period_rank <= 30
  GROUP BY Ticker
),

-- Latest trading day metrics
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

-- Combine latest + averages + relative metrics
combined AS (
  SELECT 
    l.Ticker,
    l.latest_date,
    l.latest_close,
    ROUND(l.latest_pct_change, 2) AS pct_change,
    l.latest_volume,
    a.avg_volume,
    ROUND(l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0), 2) AS rvol,          -- Relative Volume
    l.latest_trade_money,
    a.avg_trade_money,
    ROUND(l.latest_trade_money * 1.0 / NULLIF(a.avg_trade_money, 0), 2) AS relative_dollar_vol,
    
    -- Acceleration / Deceleration flags (tunable thresholds)
    CASE 
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= 2.5 THEN 'Strong Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= 1.8 THEN 'Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) <= 0.6 THEN 'Strong Deceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) <= 0.8 THEN 'Deceleration'
      ELSE 'Normal'
    END AS volume_regime
  FROM latest_day l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

-- Total average trade money (for percentage of total, same as your base report)
total_avg AS (
  SELECT SUM(avg_trade_money) AS total_avg_trade_money
  FROM avg_metrics
)

-- Final report: Top 25 by latest dollar volume + edge metrics
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
  
  -- Same percentage logic as your base report (based on avg)
  ROUND((c.avg_trade_money / t.total_avg_trade_money) * 100, 2) AS pct_of_total_avg_dollar
FROM combined c
CROSS JOIN total_avg t
ORDER BY c.latest_trade_money DESC
LIMIT 25;
```

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

3. **Change lookback** from 30 to 20 periods: just change `period_rank <= 30` to `<= 20`.

