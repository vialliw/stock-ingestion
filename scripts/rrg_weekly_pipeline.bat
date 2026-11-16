@echo off
rem Convenience wrapper for the weekly RRG pipeline
rem (scripts/rrg_weekly_pipeline.py --all: generate + publish).
rem The StockIngestion-RRGWeekly scheduled task runs the .py directly
rem under pythonw.exe (windowless); this .bat is for manual/interactive
rem runs. Extra args are passed through (e.g. --dry-run, --publish).

cd /d "%~dp0\.."
python scripts\rrg_weekly_pipeline.py --all %*
