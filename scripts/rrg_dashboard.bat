@echo off
rem Double-clickable RRG dashboard generator.
rem Writes Relative_Rotation_Dashboard_<preset>.html plus
rem <prefix>_analysis_prompt.json to reports.report_dir (config.yaml)
rem via the running Flight server, with a read-only DuckDB fallback
rem when the server is down. Reads the persisted weekly_close table
rem (built by weekly_refresh / scripts\refresh_weekly.py).
rem
rem Optional flags pass straight through, e.g.
rem   rrg_dashboard.bat --dashboard sector1 --years 5
rem With no --dashboard flag ALL six presets are generated.

cd /d "%~dp0\.."
python scripts\rrg_dashboard.py %*
echo.
pause
