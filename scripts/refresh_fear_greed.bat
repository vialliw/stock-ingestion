@echo off
rem Double-clickable CNN Fear & Greed refresh.
rem Sends the "fear_greed_refresh" action to the running Flight server,
rem which upserts the composite score and all 9 sub-indicators into
rem fear_greed_history. The server stays the single DuckDB writer --
rem this never opens the DB directly.
rem
rem Optional first argument overrides fear_greed.start_date (YYYY-MM-DD).

cd /d "%~dp0\.."
python scripts\refresh_fear_greed.py %*
echo.
pause
