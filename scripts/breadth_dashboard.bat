@echo off
rem Double-clickable market breadth dashboard generator.
rem Writes Stock_Ingestion_Market_Breadth.html to reports.report_dir
rem (config.yaml) via the running Flight server, with a read-only
rem DuckDB fallback when the server is down.
rem
rem Optional: pass a trailing-years override, e.g.
rem   breadth_dashboard.bat 5      (0 = all history)
rem With no argument the configured reports.market_breadth_years applies.

cd /d "%~dp0\.."
python scripts\breadth_dashboard.py %*
echo.
pause
