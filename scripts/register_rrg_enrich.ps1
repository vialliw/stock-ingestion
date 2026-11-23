<#
.SYNOPSIS
    Registers (or removes) the StockIngestion-RRGEnrich scheduled task
    that automates Phase B (enrichment) of the weekly RRG report
    pipeline via a headless local Devin session.

.DESCRIPTION
    The task action runs scripts\rrg_enrich_headless.py with pythonw.exe
    (the windowless interpreter next to python.exe, when present) so no
    console window pops up in the interactive session — same convention
    as StockIngestion-RRGWeekly / StockInestion-DailyLoad.  The wrapper
    reads docs\rra\.rrg_pipeline_state.json; when the weekly report is
    still a draft or weekly_summary.md does not cover the resolved date,
    it spawns ONE headless `devin -p` session that enriches the report
    ("> Weekly read:"/"**Net call:**" markers) and refreshes
    weekly_summary.md + report_index.md so the next Phase C publish
    trigger can ship them.  Enrichment failure is non-blocking — the
    wrapper always exits 0 and the publish gate holds drafts
    (docs/enhancements/rrg_weekly_automation_design.md).

    Triggers (machine-local time):

      * Friday 22:45 — 30 minutes after the 22:15
        StockIngestion-RRGWeekly trigger (Phase A generation + a first
        publish attempt are done by then, so the state file is in place).
      * Saturday 08:30 and Sunday 08:30 — 30 minutes BEFORE the 09:00
        RRGWeekly publish retries, so an enrichment run started here
        lands its markers in time for the same-morning publish instead
        of waiting for the next trigger.

    StartWhenAvailable catches a missed Friday run on next boot; the
    wrapper's no-op check makes repeat runs cheap (they exit without
    spawning once the markers verify).

    IMPORTANT — trigger model: this task uses a multi-trigger
    registration (Register-ScheduledTask -Trigger <array of weekly
    triggers>), NOT schtasks /sc DAILY.  Re-running this script
    re-registers the same three triggers (idempotent); do not replace
    them with a daily schedule — a daily trigger would spawn headless
    sessions on weekdays where none are wanted.  This avoids the
    documented register_daily_task.ps1 caveat (its live task is weekly
    Mon-Fri while the script still registers /sc DAILY).

    Idempotent: re-running drops the existing task and registers it
    again with these settings.

.PARAMETER TaskName
    Scheduled task name. Default: StockIngestion-RRGEnrich

.PARAMETER FridayTime
    Friday trigger start time, HH:mm 24h machine-local.
    Default: 22:45 (30 minutes after the RRGWeekly trigger).

.PARAMETER WeekendTime
    Saturday/Sunday trigger start time, HH:mm 24h machine-local.
    Default: 08:30 (30 minutes before the RRGWeekly publish retries).

.PARAMETER Unregister
    Delete the scheduled task instead of creating it.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_enrich.ps1

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_enrich.ps1 -FridayTime 23:00 -WeekendTime 08:00

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_enrich.ps1 -Unregister
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'StockIngestion-RRGEnrich',
    [string]$FridayTime = '22:45',
    [string]$WeekendTime = '08:30',
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'rrg_enrich_headless.py'

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
# the interactive session; the wrapper prints progress lines but every
# observable effect goes to files/logs, so no console output is needed.
$pythonExe = (Get-Command python -ErrorAction Stop).Source
$pythonwExe = Join-Path (Split-Path -Parent $pythonExe) 'pythonw.exe'
if (Test-Path $pythonwExe) {
    $pythonExe = $pythonwExe
}

$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$runner`"" `
    -WorkingDirectory $repoRoot

# Three weekly triggers on one task: Fri 22:45 (post-generation
# enrichment) and Sat/Sun 08:30 (enrich before the 09:00 publish
# retries).  This must stay a multi-trigger registration — NOT
# schtasks /sc DAILY (see .DESCRIPTION).
$triggers = @(
    (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday   -At $fridayAt)
    (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At $weekendAt)
    (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday   -At $weekendAt)
)

# Bounded 1-hour limit — the wrapper kills its headless session after
# 30 minutes, so a wedged wrapper itself still cannot hang forever.
# IgnoreNew drops a second launch while one is running (the wrapper's
# own lock file is a second layer of mutual exclusion);
# StartWhenAvailable catches a Friday run missed while the machine was
# off.
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

# Per design: user / Interactive / Limited — the wrapper writes only
# user-owned paths (docs/rra, devin_blackboard) and the headless session
# edits only docs/rra/*.md; it must be visible in Task Scheduler without
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
Write-Host "=== register_rrg_enrich summary ==="
Write-Host "  task:     '$TaskName' registered"
Write-Host "  triggers: Fri $FridayTime, Sat $WeekendTime, Sun $WeekendTime (weekly, machine-local)"
Write-Host "  settings: StartWhenAvailable, IgnoreNew, 1h limit, batteries allowed"
Write-Host "  principal: $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) (Interactive/Limited)"
Write-Host "  action:   `"$pythonExe`" `"$runner`""
Write-Host "  workdir:  $repoRoot"
Write-Host "  remove:   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_rrg_enrich.ps1 -Unregister"
exit 0
