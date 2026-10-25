# finviz_classification_update

Compare the latest Finviz sector/industry CSV against the stored
`company_metadata` (yfinance) and push Finviz values into the DB via the
`metadata_load` `overrides` payload. Use when the quarterly
`StockIngestion-FinvizClassify` run produces a new CSV, when the user asks
to sync/correct classifications, or after spotting sector/industry drift.

Repo: `C:\Users\user\OneDrive\Documents\script\python\projects\stock-ingestion`
Service: Arrow Flight on `127.0.0.1:8815` (task `Flight Server`).
Report dir: `C:\Intel\parquet` — CSVs `finviz_sector_industry_YYYYMMDD.csv`.

## Contract (SEC-T3)

`metadata_load` accepts `overrides`: a list of dicts
`{ticker, company_name?, sector?, industry?, quote_type?}` with
**merge semantics** — fields absent from a dict preserve the stored value;
explicit `null` counts as absent. Overrides apply AFTER any fetch loop and
win over it. Malformed payloads are rejected before any write. Report
fields: `overrides_applied`, `overrides_report` `[{ticker, applied_fields,
created}]`.

## Procedure

1. **Pick the input CSV** — newest `finviz_sector_industry_*.csv` in
   `C:\Intel\parquet` (created by `python scripts/classify_tickers_finviz.py`
   — run it first if none or stale).
2. **Diff** — join `company_metadata` (`quote_type='EQUITY'` rows with a
   stored `sector` or `industry`) to the CSV on `ticker`; collect rows
   where `sector` or `industry` differ. Skip rows Finviz didn't classify
   (delisted: currently DBRG, LESL, QRVO, WBD — expected, not errors).
3. **Apply** — send each differing ticker as an override carrying ONLY the
   differing fields (`sector` and/or `industry`) — merge semantics keep
   `company_name`/`quote_type` untouched:
   ```python
   import json, pyarrow.flight as fl
   c = fl.FlightClient("grpc://localhost:8815")
   payload = {"tickers": [], "overrides": [
       {"ticker": "GPN", "sector": "Technology",
        "industry": "Software - Infrastructure"},
   ]}
   rep = json.loads(list(c.do_action(
       fl.Action("metadata_load", json.dumps(payload).encode())
   ))[0].body.to_pybytes())
   print(rep["status"], rep.get("overrides_applied"), rep.get("overrides_report"))
   ```
4. **Verify** — re-query the changed tickers; every override must read
   back the Finviz value, untouched fields unchanged, and `data_health`
   must not flag the write.

## Reference diff snippet

```python
import json, glob, pandas as pd, pyarrow.flight as fl
c = fl.FlightClient("grpc://localhost:8815")
def q(sql):
    return c.do_get(fl.Ticket(json.dumps({"sql": sql}).encode())).read_all().to_pandas()
fv = pd.read_csv(sorted(glob.glob(r"C:\Intel\parquet\finviz_sector_industry_*.csv"))[-1])
yf = q("SELECT ticker, sector, industry FROM company_metadata WHERE quote_type='EQUITY'")
m = yf.merge(fv, on="ticker", suffixes=("_yf", "_fv"))
diffs = m[m.sector_fv.notna() & ((m.sector_yf != m.sector_fv) | (m.industry_yf != m.industry_fv))]
overrides = [
    {**({"sector": r.sector_fv} if r.sector_yf != r.sector_fv else {}),
     **({"industry": r.industry_fv} if r.industry_yf != r.industry_fv else {}),
     "ticker": r.ticker}
    for r in diffs.itertuples()
]
```

To apply only REAL disagreements and skip naming-convention deltas,
normalize first (e.g. map `Financial Services`→`Financial` before diffing):
`m.sector_yf = m.sector_yf.replace({"Financial Services": "Financial"})` —
that collapses the ~162-ticker naming block to the handful of true
disagreements (3 as of 2026-10-25).

## Notes

- Known baseline (2026-10-25): Finviz vs yfinance agreed on ~99.7% of
  1,114 joint tickers; the `Financial Services`(yf) vs `Financial`(fv)
  sector naming convention accounts for the bulk of any sector diff —
  **decide whether to adopt Finviz naming or keep Yahoo's before mass
  overrides**. Per-ticker overrides are the safe default.
- `overrides` never triggers yfinance fetches on its own
  (`tickers: []` is the clean call).
- Overrides for tickers not in `company_metadata` INSERT new rows
  (`created: true` in the report) — sanity-check ticker spellings.
