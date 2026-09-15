@echo off
rem Double-clickable gap verification: runs the data_health report,
rem then probes each flagged ticker against yfinance to classify each
rem missing day as 'download_gap' (Yahoo has the data — repairable via
rem on_request_load) or 'halt_or_suspension' (ticker did not trade).
rem
rem Optional: edit to pass --gap-days N, --max-probes N, or --json.

cd /d "%~dp0\.."
python scripts\data_health_verify.py %*
echo.
pause
