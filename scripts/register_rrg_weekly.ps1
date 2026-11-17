<#
.SYNOPSIS
    Registers (or removes) the StockIngestion-RRGWeekly scheduled task
    that runs the weekly RRG report pipeline.

.DESCRIPTION
    The task action runs scripts\rrg_weekly_pipeline.py --all with
    pythonw.exe (the windowless interpreter next to python.exe, when
    present) so no console window pops up in the interactive session —
    same convention as StockInestion-DailyLoad.  Phase A generates the
    Friday-dated prompt JSON + draft report + draft summary; Phase C
    mirror-publishes enriched docs/rra/*.md to
    vialliw/Relative-Rotation-Analysis-Dashboard:report/
    (docs/enhancements/rrg_weekly_automation_design.md).

    Triggers (machine-local time):

      * Friday 22:15 — 45-minute buffer after the 21:30 machine-local
        StockInestion-DailyLoad trigger (the load ends with
        _weekly_bridge -> run_weekly_refresh() rebuilding weekly_close,
        so the pipeline must run after it).
      * Saturday 09:00 and Sunday 09:00 — cheap retry ticks: --all is
        idempotent, so weekend runs are no-ops unless the user enriched
        a draft, in which case the publish phase ships it without
        waiting a week.

    StartWhenAvailable catches a missed Friday run on next boot; the
    as_of resolution still lands on the correct last-Friday bar.

    IMPORTANT — trigger model: this task uses a multi-trigger
    registration (Register-ScheduledTask -Trigger <array of weekly
    triggers>), NOT schtasks /sc DAILY.  Re-running this script
    re-registers the same three triggers (idempotent); do not replace
    them with a daily schedule — a daily trigger would re-run the
    generation phase on weekdays where it is not wanted.  This avoids
    the documented register_daily_task.ps1 caveat (its live task is
    weekly Mon-Fri while the script still registers /sc DAILY).

    Idempotent: re-running drops the existing task and registers it
    again with these settings.

.PARAMETER TaskName
    Scheduled task name. Default: StockIngestion-RRGWeekly

.PARAMETER FridayTime
    Friday trigger start time, HH:mm 24h machine-local.
    Default: 22:15 (45 minutes after the DailyLoad trigger).

.PARAMETER WeekendTime
    Saturday/Sunday trigger start time, HH:mm 24h machine-local.
    Default: 09:00.

.PARAMETER Unregister
    Delete the scheduled task instead of creating it.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_weekly.ps1

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_weekly.ps1 -FridayTime 22:30 -WeekendTime 10:00

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_weekly.ps1 -Unregister
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'StockIngestion-RRGWeekly',
    [string]$FridayTime = '22:15',
    [string]$WeekendTime = '09:00',
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'rrg_weekly_pipeline.py'

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

# Machine-local times (ScheduledTasks triggers are machine-local).
$fridayAt = [DateTime]::ParseExact($FridayTime, 'HH:mm', [System.Globalization.CultureInfo]::InvariantCulture)
$weekendAt = [DateTime]::ParseExact($WeekendTime, 'HH:mm', [System.Globalization.CultureInfo]::InvariantCulture)

# Resolve the python interpreter to an absolute path so the task does
# not depend on the PATH of whatever context Task Scheduler runs in.
# Prefer pythonw.exe (the windowless interpreter sitting next to
# python.exe) so the scheduled run does not pop a console window into
# the interactive session; the pipeline prints progress lines but every
# observable effect goes to files/logs, so no console output is needed.
# NOTE: the scheduled action runs the .py directly — a .bat cannot be
# passed to pythonw.exe, and running the .bat via cmd would reintroduce
# the console window this convention exists to avoid.  The companion
# scripts\rrg_weekly_pipeline.bat is only a manual-run wrapper.
$pythonExe = (Get-Command python -ErrorAction Stop).Source
$pythonwExe = Join-Path (Split-Path -Parent $pythonExe) 'pythonw.exe'
if (Test-Path $pythonwExe) {
    $pythonExe = $pythonwExe
}

$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$runner`" --all" `
    -WorkingDirectory $repoRoot

# Three weekly triggers on one task: Fri 22:15 (post-load generation +
# publish) and Sat/Sun 09:00 (idempotent publish retries for weekend
# enrichment).  This must stay a multi-trigger registration — NOT
# schtasks /sc DAILY (see .DESCRIPTION).
$triggers = @(
    (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday   -At $fridayAt)
    (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At $weekendAt)
    (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday   -At $weekendAt)
)

# Bounded 2-hour limit — generation waits on the Flight server and the
# publish phase clones+pulls a remote repo; both are quick but the cap
# keeps a wedged run from hanging forever.  IgnoreNew drops a second
# launch while one is running; StartWhenAvailable catches a Friday run
# missed while the machine was off.
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

# Per design: user / Interactive / Limited — the pipeline writes only
# user-owned paths (docs/rra, .tmp, devin_blackboard) and pushes via the
# user's git credentials; it must be visible in Task Scheduler without
# elevation and needs no admin rights.  Runs only while that user is
# logged on (StartWhenAvailable still catches missed runs).
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited

# Idempotent re-registration: drop any existing task first.
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Principal $principal | Out-Null

Write-Host ""
Write-Host "=== register_rrg_weekly summary ==="
Write-Host "  task:     '$TaskName' registered"
Write-Host "  triggers: Fri $FridayTime, Sat $WeekendTime, Sun $WeekendTime (weekly, machine-local)"
Write-Host "  settings: StartWhenAvailable, IgnoreNew, 2h limit, batteries allowed"
Write-Host "  principal: $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) (Interactive/Limited)"
Write-Host "  action:   `"$pythonExe`" `"$runner`" --all"
Write-Host "  workdir:  $repoRoot"
Write-Host "  remove:   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_weekly.ps1 -Unregister"
exit 0
