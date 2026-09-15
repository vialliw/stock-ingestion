@echo off
rem Double-clickable company_metadata backfill.
rem Sends the "metadata_load" action to the running Flight server and
rem prints a summary (fetched / skipped / failed / aborted). The server
rem stays the single DuckDB writer -- this never opens the DB directly.
rem
rem Optional: pass flags through, e.g.
rem     load_company_metadata.bat --refresh
rem     load_company_metadata.bat --tickers AAPL,MSFT --refresh
rem     load_company_metadata.bat --limit 5 --delay 1.0

cd /d "%~dp0\.."
python scripts\load_company_metadata.py %*
echo.
pause
