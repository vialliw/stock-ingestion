<#
.SYNOPSIS
    Registers (or removes) a Windows Scheduled Task that runs the
    stock-ingestion daily load once per day with the default config.

.DESCRIPTION
    The task action runs scripts\daily_load_once.py with the resolved
    python interpreter.  That script loads src/config.yaml and calls
    PipelineScheduler.run_once() — the same daily-load path used by the
    APScheduler daemon (NYSE market check, DuckDB lock retry,
    run_daily_load, optional R2 sync).

    Idempotent: re-running overwrites the existing task (schtasks /f).

.PARAMETER TaskName
    Scheduled task name. Default: StockIngestion-DailyLoad

.PARAMETER Time
    Daily start time, HH:mm 24h in the local machine timezone.
    Default: scheduler.schedule_time from src/config.yaml.

.PARAMETER Unregister
    Delete the scheduled task instead of creating it.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1 -Time 16:30

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1 -Unregister
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'StockIngestion-DailyLoad',
    [string]$Time = '',
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'daily_load_once.py'

if ($Unregister) {
    schtasks.exe /delete /tn $TaskName /f
    exit $LASTEXITCODE
}

if (-not (Test-Path $runner)) {
    Write-Error "Runner script not found: $runner"
}

# Default to the configured scheduler time from src/config.yaml.
if (-not $Time) {
    $configPath = Join-Path $repoRoot 'src\config.yaml'
    $matched = Select-String -Path $configPath -Pattern 'schedule_time:\s*[''"]?(\d{1,2}:\d{2})'
    $Time = if ($matched) { $matched.Matches[0].Groups[1].Value } else { '16:30' }
}

# Resolve the python interpreter to an absolute path so the task does
# not depend on the PATH of whatever context Task Scheduler runs in.
$pythonExe = (Get-Command python -ErrorAction Stop).Source

$action = "`"$pythonExe`" `"$runner`""

# /sc DAILY fires every calendar day; the engine's NYSE calendar check
# skips weekends and market holidays.  /f makes this idempotent.
schtasks.exe /create /tn $TaskName /tr $action /sc DAILY /st $Time /f
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "Scheduled task '$TaskName' registered: daily at $Time."
    Write-Host "  Action: $action"
    Write-Host "  Remove: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1 -Unregister"
}
exit $exitCode
