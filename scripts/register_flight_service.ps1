<#
.SYNOPSIS
    Registers (or removes) a Windows Scheduled Task that runs the
    stock-ingestion Arrow Flight server as a service-like AtStartup task.

.DESCRIPTION
    The task action runs scripts\run_flight_server.py with the resolved
    python interpreter.  That wrapper anchors the repo root on sys.path,
    makes it the working directory, and calls src.main.main() — which
    loads src/config.yaml, opens the DuckDB database read-write, and
    serves Arrow Flight on server.flight_host:server.flight_port.

    Service-like settings: the task fires at system boot (AtStartup
    trigger), runs as SYSTEM whether or not a user is logged on, has no
    execution time limit, restarts automatically on failure (3 retries,
    1 minute apart), and ignores new launch requests while an instance
    is already running (MultipleInstances = IgnoreNew).  Layered on top
    of that, run_flight_server.py itself retries the DuckDB open for up
    to ~10 minutes when another process holds the write lock (e.g. a
    daily_load_once.py direct fallback load), so a long lock hold cannot
    starve all three task restarts (F7-T18 / challenger finding C1).

    Idempotent: re-running removes any existing registration and
    re-registers the task.  Registration requires elevation — a task
    that runs as SYSTEM must be created by an administrator.

    IMPORTANT: while this service task is registered, do NOT also run
    src/scheduler_main.py — the Flight server holds the DuckDB write
    lock for its whole lifetime and must stay the single writer.  The
    daily scheduled load (StockIngestion-DailyLoad ->
    scripts/daily_load_once.py) routes through the running service via
    the 'daily_load' Flight action and only opens DuckDB directly when
    the service is unreachable.

.PARAMETER TaskName
    Scheduled task name. Default: StockIngestion-FlightServer

.PARAMETER Unregister
    Delete the scheduled task instead of creating it.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_flight_service.ps1

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_flight_service.ps1 -Unregister
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'StockIngestion-FlightServer',
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'run_flight_server.py'

if ($Unregister) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Scheduled task '$TaskName' removed."
    } else {
        Write-Host "Scheduled task '$TaskName' is not registered."
    }
    exit 0
}

if (-not (Test-Path $runner)) {
    Write-Error "Runner script not found: $runner"
}

# Resolve the python interpreter to an absolute path so the task does
# not depend on the PATH of whatever context Task Scheduler runs in.
$pythonExe = (Get-Command python -ErrorAction Stop).Source

# The wrapper chdirs to the repo root itself; -WorkingDirectory is set
# anyway so the process starts with a sane cwd.
$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$runner`"" `
    -WorkingDirectory $repoRoot

$trigger = New-ScheduledTaskTrigger -AtStartup

# No execution time limit (PT0S) — the server is a long-running daemon.
# Restart on failure: 3 attempts, 1 minute apart.  IgnoreNew keeps a
# second instance from fighting over the DuckDB write lock.
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

# Run whether or not a user is logged on (service-like).  SYSTEM can
# read/write the local DuckDB file and reach the network; note that
# user-scoped environment variables (e.g. R2_* set for a user) are not
# visible to SYSTEM — set them at machine scope if r2.enabled is true.
$principal = New-ScheduledTaskPrincipal `
    -UserId 'SYSTEM' `
    -LogonType ServiceAccount `
    -RunLevel Highest

# Idempotent re-registration: drop any existing task first.
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal | Out-Null

Write-Host "Scheduled task '$TaskName' registered: runs at system startup as SYSTEM."
Write-Host "  Action: `"$pythonExe`" `"$runner`""
Write-Host "  Remove: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_flight_service.ps1 -Unregister"
exit 0
