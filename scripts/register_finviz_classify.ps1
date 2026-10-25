<#
.SYNOPSIS
    Registers (or removes) a Windows Scheduled Task that runs the
    quarterly Finviz sector/industry classification.

.DESCRIPTION
    The task action runs scripts\classify_tickers_finviz.py with the
    resolved python interpreter and the repo root as the working
    directory.  That script classifies every EQUITY ticker in
    company_metadata into Finviz's sector/industry taxonomy and writes
    finviz_sector_industry_YYYYMMDD.csv + finviz_sector_summary_YYYYMMDD.csv
    under the configured reports directory (no DuckDB writes).  A
    typical run takes ~10-15 minutes.

    Schedule: QUARTERLY — day 1 of January, April, July, and October at
    the configured start time (default 08:00 machine-local).
    New-ScheduledTaskTrigger cannot express a quarter-of-year schedule,
    so the task is created via schtasks.exe
    (/sc MONTHLY /d 1 /m JAN,APR,JUL,OCT) and then tightened with
    ScheduledTasks cmdlets (action working directory + settings).  The
    start time is MACHINE-LOCAL — Task Scheduler triggers always take
    machine-local time (F7-T13 lesson: this machine is not ET).

    Idempotent: schtasks /f replaces any existing registration, so
    re-running re-registers cleanly.  Registration requires elevation —
    a task that runs as SYSTEM must be created by an administrator.

.PARAMETER TaskName
    Scheduled task name. Default: StockIngestion-FinvizClassify

.PARAMETER Time
    Start time, HH:mm 24h in the MACHINE-LOCAL timezone.
    Default: 08:00.

.PARAMETER UserPrincipal
    Register the task under the currently-running user account
    (interactive logon, limited run level) instead of the default
    SYSTEM/ServiceAccount/Highest principal.  The task is then visible
    in Task Scheduler without elevation, but runs only while that user
    is logged on (StartWhenAvailable still catches missed runs).

.PARAMETER Unregister
    Delete the scheduled task instead of creating it.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_finviz_classify.ps1

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_finviz_classify.ps1 -Time 09:30 -UserPrincipal

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_finviz_classify.ps1 -Unregister
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'StockIngestion-FinvizClassify',
    [string]$Time = '08:00',
    [switch]$UserPrincipal,
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'classify_tickers_finviz.py'

if ($Unregister) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Scheduled task '$TaskName' removed."
    } else {
        Write-Host "Scheduled task '$TaskName' is not registered."
    }
    exit 0
}

try {
    if (-not (Test-Path $runner)) {
        Write-Error "Runner script not found: $runner"
    }

    # Validate the HH:mm start time before handing it to schtasks —
    # triggers take machine-local time.
    $triggerAt = [DateTime]::ParseExact(
        $Time, 'HH:mm', [System.Globalization.CultureInfo]::InvariantCulture)

    # Resolve the python interpreter to an absolute path so the task
    # does not depend on the PATH of whatever context Task Scheduler
    # runs in.
    $pythonExe = (Get-Command python -ErrorAction Stop).Source

    # ---------------------------------------------------------------
    # 1. Create the quarterly trigger via schtasks.exe — the only
    #    supported way to express "day 1 of JAN,APR,JUL,OCT" from the
    #    command line (New-ScheduledTaskTrigger has no quarter-of-year
    #    trigger; /sc MONTHLY + /m month-list is the documented
    #    schtasks equivalent).  /f makes re-registration idempotent.
    # ---------------------------------------------------------------
    $schtasksArgs = @(
        '/create', '/f',
        '/tn', $TaskName,
        '/tr', "`"$pythonExe`" `"$runner`"",
        '/sc', 'MONTHLY',
        '/d', '1',
        '/m', 'JAN,APR,JUL,OCT',
        '/st', $Time
    )
    if ($UserPrincipal) {
        $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        # /it = interactive token (runs only while the user is logged
        # on; no password stored).  LIMITED run level matches the
        # register_incident_watch.ps1 -UserPrincipal convention.
        $schtasksArgs += @('/ru', $currentUser, '/it', '/rl', 'LIMITED')
    } else {
        $schtasksArgs += @('/ru', 'SYSTEM', '/rl', 'HIGHEST')
    }
    & schtasks.exe @schtasksArgs | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "schtasks /create failed (exit $LASTEXITCODE). Registration as SYSTEM requires elevation."
    }

    # ---------------------------------------------------------------
    # 2. Tighten the task with ScheduledTasks cmdlets: proper action
    #    working directory + bounded execution settings.  Set-ScheduledTask
    #    preserves the schtasks-created quarterly trigger.
    # ---------------------------------------------------------------
    $action = New-ScheduledTaskAction `
        -Execute $pythonExe `
        -Argument "`"$runner`"" `
        -WorkingDirectory $repoRoot

    # Bounded 2-hour limit — a classification run takes ~10-15 min for
    # ~1,100 tickers.  IgnoreNew drops a second launch while one is
    # running; StartWhenAvailable catches a run missed while the
    # machine was off.
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
        -MultipleInstances IgnoreNew `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries

    Set-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Settings $settings | Out-Null

    $task = Get-ScheduledTask -TaskName $TaskName

    Write-Host ""
    Write-Host "=== register_finviz_classify summary ==="
    if ($UserPrincipal) {
        $principalDesc = "as $currentUser (Interactive/Limited)"
    } else {
        $principalDesc = 'as SYSTEM'
    }
    Write-Host "  task:     '$TaskName' registered — day 1 of JAN,APR,JUL,OCT at $Time machine-local, $principalDesc."
    Write-Host "  action:   `"$pythonExe`" `"$runner`""
    Write-Host "  workdir:  $repoRoot"
    Write-Host "  limit:    $($task.Settings.ExecutionTimeLimit) execution time limit; StartWhenAvailable=$($task.Settings.StartWhenAvailable)"
    Write-Host "  remove:   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_finviz_classify.ps1 -Unregister"
    exit 0
} catch {
    Write-Error "register_finviz_classify failed: $_"
    exit 1
}
