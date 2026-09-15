@echo off
rem Double-clickable weekly-layer refresh.
rem Sends the "weekly_refresh" action to the running Flight server,
rem which rebuilds weekly_close (Friday-dated) and weekly_indicators
rem from daily_close. The server stays the single DuckDB writer --
rem this never opens the DB directly.
rem
rem No arguments needed (any extras are passed through harmlessly).

cd /d "%~dp0\.."
python scripts\refresh_weekly.py %*
echo.
pause
