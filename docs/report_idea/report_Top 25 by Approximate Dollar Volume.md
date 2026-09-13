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

last_30_periods AS (
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
  FROM last_30_periods
  WHERE period_rank <= 30
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
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= 2.5 THEN 'Strong Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= 1.8 THEN 'Acceleration'
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

-- Final: Acceleration Candidates only, limited to top 50 by dollar volume
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
  AND dollar_rank <= 50          -- only the most liquid 50 names
ORDER BY rvol DESC, latest_trade_money DESC;
```

### Key points of this query

- **Only Acceleration Candidates**: Filters strictly to `rvol >= 1.8` (and highlights `>= 2.5` as Strong Acceleration).
- **Top 50 liquidity filter**: First ranks *all* stocks by latest dollar volume, then keeps only those in the top 50 before applying the RVOL filter. This cleanly excludes thin-volume stocks.
- Keeps the same style and CTEs as your original base report.
- Ordered by strongest RVOL first (best acceleration signal), then by dollar volume.

You can tighten the threshold easily (e.g. change `1.8` to `2.0` or `2.5` if you only want the strongest signals).