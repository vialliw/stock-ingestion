"""Generate grok03B-style volume-profile HTML charts AS OF a cutoff date.

Reuses VolumeProfileVisualizer_grok03B.py (import + monkeypatch) but sources
OHLCV from the stock_ingestion DuckDB via the Flight service instead of
yfinance, truncated at --cutoff.

Usage:
    python scripts/vp_asof.py AVGO,AMD,MRVL,MU,WDC --cutoff 2026-06-19
    python scripts/vp_asof.py SMH --cutoff 2026-03-06 --months 6 --bins 20
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow.flight as fl

VP_DIR = r"C:/Users/user/OneDrive/Documents/script/python/projects/volume_profile"
sys.path.insert(0, VP_DIR)
import VolumeProfileVisualizer_grok03B as vpv  # noqa: E402


def fetch(tickers, start, cutoff):
    c = fl.FlightClient("grpc://localhost:8815")
    inlist = ",".join(f"'{t}'" for t in tickers)
    sql = (
        "SELECT Date, Ticker, Open, High, Low, Close, Volume FROM daily_close "
        f"WHERE Ticker IN ({inlist}) AND Date >= '{start}' AND Date <= '{cutoff}' "
        "ORDER BY Ticker, Date"
    )
    df = c.do_get(fl.Ticket(json.dumps({"sql": sql}).encode())).read_all().to_pandas()
    if df.empty:
        raise SystemExit(f"No rows for {tickers} in {start}..{cutoff}")
    parts = {
        t: df[df.Ticker == t]
        .set_index(pd.to_datetime(df[df.Ticker == t].Date))[
            ["Open", "High", "Low", "Close", "Volume"]
        ]
        for t in tickers
    }
    wide = pd.concat(parts, axis=1).swaplevel(0, 1, axis=1).sort_index(axis=1)
    return wide, df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", help="comma-separated, e.g. AVGO,AMD,MRVL,MU,WDC")
    ap.add_argument("--cutoff", required=True, help="YYYY-MM-DD, data through this date")
    ap.add_argument("--months", type=int, default=6, help="lookback months")
    ap.add_argument("--bins", type=int, default=20)
    ap.add_argument("--out", default="R:/spiderhive0")
    args = ap.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    cutoff = pd.Timestamp(args.cutoff)
    start = (cutoff - pd.DateOffset(months=args.months)).date().isoformat()
    tag = "asof" + args.cutoff.replace("-", "")

    out_dir = args.out
    try:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        probe = Path(out_dir) / ".write_test"
        probe.write_text("")
        probe.unlink()
    except OSError:
        out_dir = "c:/intel/parquet/tech_analysis"
        print(f"{args.out} not writable, falling back to {out_dir}")

    wide, df = fetch(tickers, start, args.cutoff)
    last = df.Date.max()
    print(f"Data thru {last} ({args.cutoff} cutoff), window {start}+, n_days={df.Date.nunique()}")

    vpv.get_filename = lambda ticker, months: f"{ticker}_VolumeProfile_{months}M_{tag}.html"
    viz = vpv.VolumeProfileVisualizer(tickers=",".join(tickers), no_of_months=args.months, num_bins=args.bins)
    # single ticker: their code wants a flat df, not MultiIndex
    viz.data = wide.xs(tickers[0], level=1, axis=1) if len(tickers) == 1 else wide

    saved = viz.plot_chart(output_folder=out_dir)
    for path in saved:
        html = Path(path).read_text(encoding="utf-8")
        html = html.replace(f"({args.months} Months) |", f"({args.months}M lookback, data thru {args.cutoff}) |")
        Path(path).write_text(html, encoding="utf-8")

    for t in tickers:
        p = viz._process_single_ticker(t)
        print(f"{t}: POC={p['poc_price']:.2f}  close range={p['df'].Close.min():.2f}-{p['df'].Close.max():.2f}")
    print("\n".join(str(s) for s in saved))


if __name__ == "__main__":
    main()
