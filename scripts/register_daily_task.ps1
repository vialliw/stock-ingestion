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
    Default: scheduler.schedule_time from src/config.yaml, converted
    from scheduler.timezone (e.g. America/New_York) to the machine's
    local timezone because schtasks /st interprets /st as local time.

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
# scheduler.schedule_time is expressed in scheduler.timezone (e.g.
# America/New_York), but schtasks /st interprets the start time in the
# MACHINE-LOCAL timezone.  Convert at registration so the task fires at
# the intended market time regardless of the machine's locale.
if (-not $Time) {
    $configPath = Join-Path $repoRoot 'src\config.yaml'
    $timeMatch = Select-String -Path $configPath -Pattern 'schedule_time:\s*[''"]?(\d{1,2}:\d{2})'
    $tzMatch   = Select-String -Path $configPath -Pattern '^\s*timezone:\s*[''"]?([A-Za-z0-9_+\-/]+)'
    $configTime = if ($timeMatch) { $timeMatch.Matches[0].Groups[1].Value } else { '16:30' }
    $configTzId = if ($tzMatch) { $tzMatch.Matches[0].Groups[1].Value } else { 'America/New_York' }

    # Resolve the configured timezone id.  .NET 6+ (PowerShell 7) resolves
    # IANA ids like 'America/New_York' directly; Windows PowerShell 5.1
    # (.NET Framework) needs the IANA -> Windows-id fallback map.
    $sourceTz = $null
    try {
        $sourceTz = [System.TimeZoneInfo]::FindSystemTimeZoneById($configTzId)
    } catch {
        $ianaToWindows = @{
            'America/New_York'    = 'Eastern Standard Time'
            'America/Chicago'     = 'Central Standard Time'
            'America/Denver'      = 'Mountain Standard Time'
            'America/Phoenix'     = 'US Mountain Standard Time'
            'America/Los_Angeles' = 'Pacific Standard Time'
            'America/Anchorage'   = 'Alaskan Standard Time'
            'Pacific/Honolulu'    = 'Hawaiian Standard Time'
            'Etc/UTC'             = 'UTC'
            'UTC'                 = 'UTC'
        }
        if ($ianaToWindows.ContainsKey($configTzId)) {
            $sourceTz = [System.TimeZoneInfo]::FindSystemTimeZoneById($ianaToWindows[$configTzId])
        }
    }
    if (-not $sourceTz) {
        Write-Error ("Cannot resolve scheduler.timezone '$configTzId' to a system timezone. " +
                     "Pass -Time HH:mm explicitly in machine-local time.")
    }

    $localTz = [System.TimeZoneInfo]::Local

    # Anchor the conversion to TODAY in the source timezone so the correct
    # UTC offset (including DST) applies.
    $sourceNow = [System.TimeZoneInfo]::ConvertTime(
        [DateTime]::UtcNow, [System.TimeZoneInfo]::Utc, $sourceTz)
    $timeParts = $configTime -split ':'
    $sourceDateTime = $sourceNow.Date.AddHours([int]$timeParts[0]).AddMinutes([int]$timeParts[1])

    if ($sourceTz.IsInvalidTime($sourceDateTime)) {
        Write-Warning "schedule_time $configTime falls inside a DST gap in '$configTzId' today; the converted local time may be off by one hour."
    } elseif ($sourceTz.IsAmbiguousTime($sourceDateTime)) {
        Write-Warning "schedule_time $configTime is ambiguous in '$configTzId' today (DST fall-back); the converted local time uses the standard-time interpretation."
    }

    $localDateTime = [System.TimeZoneInfo]::ConvertTime($sourceDateTime, $sourceTz, $localTz)
    if ($localDateTime.Date -ne $sourceDateTime.Date) {
        Write-Warning ("Converted start time lands on local date $($localDateTime.ToString('yyyy-MM-dd')) " +
                       "(source date $($sourceDateTime.ToString('yyyy-MM-dd'))); the task will fire on the local calendar day.")
    }
    $Time = $localDateTime.ToString('HH:mm')
    Write-Host "schedule_time $configTime ($configTzId) -> $Time machine-local ($($localTz.Id))."
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
