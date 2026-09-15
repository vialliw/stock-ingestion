@echo off
rem Double-clickable daily_close data-health report.
rem Sends the "data_health" action to the running Flight server and
rem prints the full report (staleness / coverage / integrity /
rem duplicates / per-ticker gaps over the last 30 sessions).
rem
rem Optional: edit the line below to pass --gap-days N or --json.

cd /d "%~dp0\.."
python scripts\data_health_report.py %*
echo.
pause
