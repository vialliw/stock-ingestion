<#
.SYNOPSIS
    Registers (or removes) a Windows Scheduled Task that runs the
    incident health watcher once per day, and provisions the central
    incident store + repo junction it writes to.

.DESCRIPTION
    The task action runs scripts\incident_watch.py with the resolved
    python interpreter.  That script pulls the data_health report from
    the running Flight service, classifies findings into SEV1-SEV4
    incident folders under the central store, dedups/escalates
    recurring findings, auto-resolves cleared ones, appends to
    events.jsonl, and writes the _last_check.json heartbeat
    (docs/incident_management_requirements.md).

    Provisioning is idempotent and also runs standalone via
    -ProvisionOnly (e.g. as a deploy step without registration):

      * C:\Intel\incidents\stock-ingestion\ with subdirs
        open, acknowledged, resolved, _reports, _config
      * _config\suppressions.json seeded when absent (CL=F 2020-04-20 /
        2020-04-21 benign integrity rows, empty acknowledged_delisted)
      * _project.json seeded when absent (doc section 7.1)
      * junction <repo>\incidents -> C:\Intel\incidents\stock-ingestion
        via cmd /c mklink /J — a pre-existing REAL directory is never
        clobbered (the script stops instead)

    Schedule: daily at 18:30 America/New_York — after the 16:14 ET
    daily load plus grace.  The ET time is converted to machine-local
    at registration because ScheduledTasks triggers take machine-local
    time (F7-T13 lesson: this machine is not ET).

    Idempotent: re-running leaves existing store contents alone and
    re-registers the task.  Registration requires elevation — a task
    that runs as SYSTEM must be created by an administrator.

.PARAMETER TaskName
    Scheduled task name. Default: StockIngestion-HealthWatch

.PARAMETER Time
    Daily start time, HH:mm 24h in the MACHINE-LOCAL timezone.
    Default: 18:30 America/New_York converted to machine-local.
    Pass an explicit value to bypass the timezone conversion.

.PARAMETER StoreRoot
    Central incident store path.
    Default: C:\Intel\incidents\stock-ingestion

.PARAMETER ProvisionOnly
    Provision the incident store + junction only; do not touch the
    scheduled task (useful as an orchestrator deploy step).

.PARAMETER UserPrincipal
    Register the task under the currently-running user account
    (LogonType Interactive, RunLevel Limited) instead of the default
    SYSTEM/ServiceAccount/Highest principal.  The task is then visible
    in Task Scheduler without elevation, but runs only while that user
    is logged on (StartWhenAvailable still catches missed runs).

.PARAMETER Unregister
    Delete the scheduled task instead of creating it.  The store and
    the junction are left in place.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -ProvisionOnly

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -UserPrincipal

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -Unregister
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'StockIngestion-HealthWatch',
    [string]$Time = '',
    [string]$StoreRoot = 'C:\Intel\incidents\stock-ingestion',
    [switch]$ProvisionOnly,
    [switch]$UserPrincipal,
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'incident_watch.py'
$junction = Join-Path $repoRoot 'incidents'

if ($Unregister) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Scheduled task '$TaskName' removed (store and junction left in place)."
    } else {
        Write-Host "Scheduled task '$TaskName' is not registered."
    }
    exit 0
}

$created = New-Object System.Collections.ArrayList

try {
    # ---------------------------------------------------------------
    # 1. Provision the central incident store (doc section 7.1).
    # ---------------------------------------------------------------
    $storeSubDirs = @('open', 'acknowledged', 'resolved', '_reports', '_config')
    foreach ($dir in @($StoreRoot) + ($storeSubDirs | ForEach-Object { Join-Path $StoreRoot $_ })) {
        if (-not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            [void]$created.Add("dir      $dir")
        }
    }

    # Seed known-benign suppressions (doc section 3) only when absent —
    # operator edits must never be overwritten.
    $suppPath = Join-Path $StoreRoot '_config\suppressions.json'
    if (-not (Test-Path -LiteralPath $suppPath)) {
        $suppJson = '{"benign_integrity":[{"ticker":"CL=F","date":"2020-04-20"},{"ticker":"CL=F","date":"2020-04-21"}],"acknowledged_delisted":[]}'
        [System.IO.File]::WriteAllText($suppPath, $suppJson)
        [void]$created.Add("file     $suppPath")
    }

    # Seed the project marker (doc section 7.1) only when absent.
    $projPath = Join-Path $StoreRoot '_project.json'
    if (-not (Test-Path -LiteralPath $projPath)) {
        $projJson = @{
            name      = 'stock-ingestion'
            repo_path = $repoRoot
            source    = 'data_health'
            tz        = 'America/New_York'
        } | ConvertTo-Json -Compress
        [System.IO.File]::WriteAllText($projPath, $projJson)
        [void]$created.Add("file     $projPath")
    }

    # ---------------------------------------------------------------
    # 2. Junction <repo>\incidents -> central store (doc section 7.2).
    #    Never clobber a real directory.
    # ---------------------------------------------------------------
    $storeRootNorm = $StoreRoot.TrimEnd('\')
    $junctionItem = Get-Item -LiteralPath $junction -Force -ErrorAction SilentlyContinue
    if ($junctionItem) {
        $isLink = [bool]($junctionItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
        if (-not $isLink) {
            Write-Error ("'$junction' already exists as a real directory/file; " +
                         "refusing to clobber it. Move or remove it, then re-run.")
        }
        $target = (@($junctionItem.Target) -join ';').TrimEnd('\')
        if ($target -ne $storeRootNorm) {
            Write-Error ("'$junction' is already a junction but points to " +
                         "'$target', not '$storeRootNorm'. Remove it manually " +
                         "(rmdir — never del /s) and re-run.")
        }
        Write-Host "junction already present: $junction -> $target"
    } else {
        & cmd.exe /c mklink /J "$junction" "$StoreRoot" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "mklink /J failed (exit $LASTEXITCODE): $junction -> $StoreRoot"
        }
        [void]$created.Add("junction $junction -> $storeRootNorm")
    }

    # Verify the junction resolves through the link into the store.
    if (-not (Test-Path -LiteralPath (Join-Path $junction 'open'))) {
        Write-Error "junction '$junction' does not resolve into the provisioned store."
    }

    # ---------------------------------------------------------------
    # 3. ProvisionOnly — stop before touching the scheduled task.
    # ---------------------------------------------------------------
    if ($ProvisionOnly) {
        Write-Host ""
        Write-Host "=== register_incident_watch -ProvisionOnly summary ==="
        foreach ($entry in $created) { Write-Host "  created: $entry" }
        if ($created.Count -eq 0) {
            Write-Host "  (store tree, suppressions, and junction already existed)"
        }
        Write-Host "  store root: $StoreRoot"
        Write-Host "  junction:   $junction -> $storeRootNorm"
        exit 0
    }

    # ---------------------------------------------------------------
    # 4. Register the scheduled task (ScheduledTasks cmdlets, same
    #    conventions as register_flight_service.ps1).
    # ---------------------------------------------------------------
    if (-not (Test-Path $runner)) {
        Write-Error "Runner script not found: $runner"
    }

    # Default watch time: 18:30 America/New_York converted to
    # machine-local — ScheduledTasks triggers take machine-local time
    # (same IANA -> Windows fallback as register_daily_task.ps1).
    $watchTzId = 'America/New_York'
    $watchTimeEt = '18:30'
    if (-not $Time) {
        $sourceTz = $null
        try {
            $sourceTz = [System.TimeZoneInfo]::FindSystemTimeZoneById($watchTzId)
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
            if ($ianaToWindows.ContainsKey($watchTzId)) {
                $sourceTz = [System.TimeZoneInfo]::FindSystemTimeZoneById($ianaToWindows[$watchTzId])
            }
        }
        if (-not $sourceTz) {
            Write-Error ("Cannot resolve watch timezone '$watchTzId' to a system timezone. " +
                         "Pass -Time HH:mm explicitly in machine-local time.")
        }

        $localTz = [System.TimeZoneInfo]::Local

        # Anchor the conversion to TODAY in the source timezone so the
        # correct UTC offset (including DST) applies.
        $sourceNow = [System.TimeZoneInfo]::ConvertTime(
            [DateTime]::UtcNow, [System.TimeZoneInfo]::Utc, $sourceTz)
        $timeParts = $watchTimeEt -split ':'
        $sourceDateTime = $sourceNow.Date.AddHours([int]$timeParts[0]).AddMinutes([int]$timeParts[1])

        if ($sourceTz.IsInvalidTime($sourceDateTime)) {
            Write-Warning "watch time $watchTimeEt falls inside a DST gap in '$watchTzId' today; the converted local time may be off by one hour."
        } elseif ($sourceTz.IsAmbiguousTime($sourceDateTime)) {
            Write-Warning "watch time $watchTimeEt is ambiguous in '$watchTzId' today (DST fall-back); the converted local time uses the standard-time interpretation."
        }

        $localDateTime = [System.TimeZoneInfo]::ConvertTime($sourceDateTime, $sourceTz, $localTz)
        if ($localDateTime.Date -ne $sourceDateTime.Date) {
            Write-Warning ("Converted start time lands on local date $($localDateTime.ToString('yyyy-MM-dd')) " +
                           "(source date $($sourceDateTime.ToString('yyyy-MM-dd'))); the task will fire on the local calendar day.")
        }
        $Time = $localDateTime.ToString('HH:mm')
        Write-Host "watch time $watchTimeEt ($watchTzId) -> $Time machine-local ($($localTz.Id))."
    }

    # Resolve the python interpreter to an absolute path so the task
    # does not depend on the PATH of whatever context Task Scheduler
    # runs in.
    $pythonExe = (Get-Command python -ErrorAction Stop).Source

    # incident_watch.py anchors the repo root itself; -WorkingDirectory
    # is set anyway so the process starts with a sane cwd.
    $action = New-ScheduledTaskAction `
        -Execute $pythonExe `
        -Argument "`"$runner`"" `
        -WorkingDirectory $repoRoot

    $triggerAt = [DateTime]::ParseExact($Time, 'HH:mm', [System.Globalization.CultureInfo]::InvariantCulture)
    $trigger = New-ScheduledTaskTrigger -Daily -At $triggerAt

    # Bounded 30-minute limit — the watcher is a one-shot probe, not a
    # daemon.  IgnoreNew drops a second launch while one is running;
    # StartWhenAvailable catches a run missed while the machine was off.
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
        -MultipleInstances IgnoreNew `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries

    # Run whether or not a user is logged on.  SYSTEM can read the
    # local DuckDB-backed health endpoint and write C:\Intel.
    # -UserPrincipal instead registers under the current account —
    # visible in Task Scheduler without elevation, but runs only
    # while that user is logged on.  Limited (not Highest) is enough:
    # the watcher only reads the Flight gRPC port and writes a
    # user-owned store; StartWhenAvailable covers missed runs.
    if ($UserPrincipal) {
        $principal = New-ScheduledTaskPrincipal `
            -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
            -LogonType Interactive `
            -RunLevel Limited
    } else {
        $principal = New-ScheduledTaskPrincipal `
            -UserId 'SYSTEM' `
            -LogonType ServiceAccount `
            -RunLevel Highest
    }

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

    Write-Host ""
    Write-Host "=== register_incident_watch summary ==="
    foreach ($entry in $created) { Write-Host "  created: $entry" }
    if ($created.Count -eq 0) {
        Write-Host "  (store tree, suppressions, and junction already existed)"
    }
    Write-Host "  store root: $StoreRoot"
    Write-Host "  junction:   $junction -> $storeRootNorm"
    if ($UserPrincipal) {
        $principalDesc = "as $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) (Interactive/Limited)"
    } else {
        $principalDesc = 'as SYSTEM'
    }
    Write-Host "  task:       '$TaskName' registered — daily at $Time machine-local (default $watchTimeEt $watchTzId), $principalDesc."
    Write-Host "  action:     `"$pythonExe`" `"$runner`""
    Write-Host "  remove:     powershell -NoProfile -ExecutionPolicy Bypass -File scripts\register_incident_watch.ps1 -Unregister"
    exit 0
} catch {
    Write-Error "register_incident_watch failed: $_"
    exit 1
}
