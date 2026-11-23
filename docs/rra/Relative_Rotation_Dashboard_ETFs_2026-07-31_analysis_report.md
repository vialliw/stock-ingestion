# RRA Analysis Report — 2026-07-31

- **Data as-of:** 2026-07-31
- **Benchmark:** SPY | **Lookback:** 12 weeks | **Universe:** 96 tickers
- **Source:** `Relative_Rotation_Dashboard_ETFs_2026-07-31_analysis_prompt.json`
- **Query:** Analyze the RRA data to identify consistent leaders, high-velocity improvers, decaying momentum, and actionable portfolio tilts.
- **Quadrant rules:** lead: Strong RS and momentum - overweight; improving: Building momentum - tactical entry watch; weakening: Fading momentum - reduce exposure; lagging: Weak RS and momentum - avoid

## 1. Quadrant Census

**LEADING: 17** | **IMPROVING: 6** | **WEAKENING: 12** | **LAGGING: 61**

## 2. Leadership Regime

Consistent leaders (consistency ≥ 75%), ranked by consistency then velocity:

| Ticker | Quadrant | RS-Ratio | RS-Mom | Vel 1w | Dir | Hdg | Lead wks | Lag wks | Consist % |
|---|---|---|---|---|---|---|---|---|---|
| SMH | LEADING | 111.26 | 104.11 | 14.3 | ACCELERATING | SW | 12 | 0 | 100.0 |
| CLOU | LEADING | 119.64 | 134.39 | 10.6 | ACCELERATING | NE | 12 | 0 | 100.0 |
| XLK | LEADING | 108.73 | 112.73 | 7.1 | ACCELERATING | SW | 12 | 0 | 100.0 |
| CIBR | LEADING | 132.21 | 146.73 | 4.8 | ACCELERATING | NE | 12 | 0 | 100.0 |
| QQQ | LEADING | 101.32 | 103.93 | 4.3 | ACCELERATING | SW | 12 | 0 | 100.0 |

## 3. Improving Trends

Top 10 IMPROVING tickers by 1-week velocity:

| Ticker | Quadrant | RS-Ratio | RS-Mom | Vel 1w | Dir | Hdg | Lead wks | Lag wks | Consist % |
|---|---|---|---|---|---|---|---|---|---|
| ARKK | IMPROVING | 91.57 | 106.66 | 6.8 | DECELERATING | SW | 4 | 0 | 33.3 |
| IHI | IMPROVING | 90.17 | 101.02 | 6.4 | ACCELERATING | NE | 0 | 11 | 0.0 |
| IWF | IMPROVING | 95.55 | 101.95 | 3.1 | ACCELERATING | SW | 4 | 0 | 33.3 |
| XRT | IMPROVING | 98.29 | 101.44 | 1.9 | ACCELERATING | NE | 0 | 8 | 0.0 |
| ITB | IMPROVING | 93.35 | 100.05 | 1.6 | DECELERATING | SW | 0 | 8 | 0.0 |
| VOT | IMPROVING | 99.43 | 103.74 | 0.8 | DECELERATING | SW | 4 | 0 | 33.3 |

## 4. Weakening Momentum

WEAKENING quadrant:

| Ticker | Quadrant | RS-Ratio | RS-Mom | Vel 1w | Dir | Hdg | Lead wks | Lag wks | Consist % |
|---|---|---|---|---|---|---|---|---|---|
| IEO | WEAKENING | 102.81 | 71.34 | 12.6 | DECELERATING | NE | 2 | 7 | 16.7 |
| EWY | WEAKENING | 102.09 | 82.23 | 12.4 | ACCELERATING | SW | 2 | 0 | 16.7 |
| AMLP | WEAKENING | 101.58 | 85.24 | 9.0 | ACCELERATING | NE | 0 | 9 | 0.0 |
| WEAT | WEAKENING | 103.71 | 88.32 | 8.1 | DECELERATING | NE | 3 | 6 | 25.0 |
| VOE | WEAKENING | 100.82 | 92.72 | 3.9 | ACCELERATING | NE | 0 | 11 | 0.0 |
| VTV | WEAKENING | 102.32 | 95.43 | 3.8 | ACCELERATING | NE | 0 | 10 | 0.0 |
| IWD | WEAKENING | 104.37 | 97.90 | 3.2 | ACCELERATING | NE | 0 | 6 | 0.0 |
| SCHH | WEAKENING | 101.07 | 91.41 | 2.4 | ACCELERATING | NE | 1 | 9 | 8.3 |
| VBR | WEAKENING | 100.73 | 94.66 | 2.3 | ACCELERATING | NE | 0 | 10 | 0.0 |
| KRE | WEAKENING | 103.88 | 98.33 | 2.3 | ACCELERATING | NE | 2 | 5 | 16.7 |
| IWM | WEAKENING | 101.62 | 98.13 | 1.4 | DECELERATING | SW | 4 | 3 | 33.3 |
| IYT | WEAKENING | 102.74 | 99.73 | 0.7 | DECELERATING | SW | 2 | 5 | 16.7 |

Decelerating LEADING tickers (distribution watch), top 10 by velocity:

| Ticker | Quadrant | RS-Ratio | RS-Mom | Vel 1w | Dir | Hdg | Lead wks | Lag wks | Consist % |
|---|---|---|---|---|---|---|---|---|---|
| JETS | LEADING | 108.43 | 119.69 | 3.1 | DECELERATING | SE | 6 | 5 | 50.0 |
| AWAY | LEADING | 100.39 | 122.43 | 2.8 | DECELERATING | SE | 4 | 2 | 33.3 |
| XBI | LEADING | 106.05 | 100.61 | 2.4 | DECELERATING | SW | 5 | 6 | 41.7 |

## 5. Velocity Analysis

Fastest movers (top 10 by |velocity_1w|):

| Ticker | Quadrant | Vel 1w | Dir | Hdg | RS-Mom |
|---|---|---|---|---|---|
| SMH | LEADING | 14.3 | ACCELERATING | SW | 104.11 |
| IEO | WEAKENING | 12.6 | DECELERATING | NE | 71.34 |
| EWY | WEAKENING | 12.4 | ACCELERATING | SW | 82.23 |
| XOP | LAGGING | 12.4 | DECELERATING | NE | 67.02 |
| XLE | LAGGING | 11.6 | DECELERATING | NE | 71.58 |
| CANE | LAGGING | 11.5 | ACCELERATING | NE | 92.71 |
| VDE | LAGGING | 11.3 | DECELERATING | NE | 70.66 |
| USO | LAGGING | 10.7 | DECELERATING | NE | 48.43 |
| CLOU | LEADING | 10.6 | ACCELERATING | NE | 134.39 |
| FCG | LAGGING | 10.3 | DECELERATING | NE | 65.46 |

Dead money (velocity < 1.0): `XME` 0.41, `IYT` 0.66, `VXUS` 0.78, `VOT` 0.79

### Quadrant Crossings

- `AMLP` LAGGING->WEAKENING — hard right break — squeeze, don't chase (heading NE, Δratio 4.90, Δmom 7.55)
- `AWAY` IMPROVING->LEADING — rotation confirmed — bullish (heading SE, Δratio 2.23, Δmom -1.69)
- `DIA` LAGGING->LEADING — discontinuous — treat with suspicion (heading NE, Δratio 1.19, Δmom 1.04)
- `IBB` WEAKENING->LEADING — re-acceleration — bullish (heading NE, Δratio 2.15, Δmom 1.29)
- `IEO` LAGGING->WEAKENING — hard right break — squeeze, don't chase (heading NE, Δratio 7.66, Δmom 10.03)
- `IHI` LAGGING->IMPROVING — bottoming — early bullish (heading NE, Δratio 3.75, Δmom 5.16)
- `INDA` IMPROVING->LAGGING — failed improvement — bearish (heading SE, Δratio 1.39, Δmom -0.50)
- `SCHH` LAGGING->WEAKENING — hard right break — squeeze, don't chase (heading NE, Δratio 1.92, Δmom 1.50)
- `VBR` LAGGING->WEAKENING — hard right break — squeeze, don't chase (heading NE, Δratio 1.69, Δmom 1.60)
- `VOE` LAGGING->WEAKENING — hard right break — squeeze, don't chase (heading NE, Δratio 2.39, Δmom 3.02)
- `XTN` WEAKENING->LAGGING — breakdown confirmed — bearish (heading SW, Δratio -1.82, Δmom -4.92)

## 6. Structural Laggards

Top 10 LAGGING by lag-weeks:

| Ticker | RS-Ratio | RS-Mom | Vel 1w | Lag wks | Consist % |
|---|---|---|---|---|---|
| COPX | 85.89 | 77.54 | 2.1 | 12 | 0.0 |
| DBB | 94.06 | 87.37 | 2.9 | 12 | 0.0 |
| EFA | 96.88 | 91.34 | 2.5 | 12 | 0.0 |
| EWC | 97.11 | 90.77 | 2.6 | 12 | 0.0 |
| EWZ | 83.59 | 65.42 | 2.1 | 12 | 0.0 |
| FXI | 90.14 | 92.41 | 6.5 | 12 | 0.0 |
| GDX | 70.72 | 64.63 | 3.0 | 12 | 0.0 |
| GLD | 79.51 | 72.63 | 2.5 | 12 | 0.0 |
| HERO | 89.26 | 99.57 | 4.0 | 12 | 0.0 |
| HYG | 94.45 | 90.69 | 4.4 | 12 | 0.0 |

## 7. Actionable Portfolio Tilts

- **Overweight** (12): `CIBR`, `CLOU`, `DIA`, `IAI`, `IBB`, `IHI`, `IYF`, `VHT`, `XLF`, `XLV`, `XPH`, `XSW`
- **Hold-but-trim** — decelerating leaders (0): 
- **Reduce** — weakening (12): `AMLP`, `EWY`, `IEO`, `IWD`, `IWM`, `IYT`, `KRE`, `SCHH`, `VBR`, `VOE`, `VTV`, `WEAT`
- **Avoid** — structural laggards (46): `BATT`, `BCD`, `COPX`, `CORN`, `CPER`, `DBB`, `EFA`, `EWA`, `EWC`, `EWJ`, `EWZ`, `FCG`, `FXI`, `GDX`, `GLD`, `HERO`, `HYG`, `IFRA`, `INDA`, `IYR`, `JNK`, `NANR`, `PALL`, `PLTM`, `REMX`, `SGOV`, `SLV`, `SMOG`, `SOYB`, `TLT`, `UNG`, `VDE`, `VGK`, `VNM`, `VXUS`, `XAR`, `XES`, `XLB`, `XLC`, `XLE`, `XLI`, `XLP`, `XLU`, `XLY`, `XME`, `XOP`
- **Watch** — improving, RS-mom ≥ 110 but RS-ratio < 100 (0): none

## 8. Data Caveats

- `consistency_score` = lead_weeks_3m / lookback (12) — share of the window spent in LEADING. lead+lag < lookback is normal (IMPROVING/WEAKENING weeks are uncounted); it is NOT insufficient history.
- Minimal extreme-quadrant history (lead+lag ≤ 2 — score can't distinguish "new to universe" from "always middling"): `EWY` (2/0)
- `angle_deg` convention is not defined in the prompt; do not infer rotation direction from it alone — use quadrant + `heading` (or, on legacy payloads without a prior file, quadrant + velocity_direction).
- Tilt buckets are rule-generated; review before acting.
