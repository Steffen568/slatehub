# SlateHub — Windows Task Scheduler Setup
# Right-click this file → "Run with PowerShell" (as Administrator)
# Creates all scheduled tasks with Wake-to-Run enabled on key jobs.

$ScriptPath = Join-Path $PSScriptRoot "refresh_all.py"
$Py         = "py"
$PyArgs     = "-3.12 `"$ScriptPath`""

function New-SlateTask {
    param(
        [string]$Name,
        [string]$Mode,
        [object]$Trigger,
        [bool]$Wake = $true
    )

    $action   = New-ScheduledTaskAction -Execute $Py -Argument "$PyArgs $Mode" -WorkingDirectory $PSScriptRoot
    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun:$Wake `
        -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
        -MultipleInstances IgnoreNew

    $task = Register-ScheduledTask `
        -TaskName   "SlateHub\$Name" `
        -Action     $action `
        -Trigger    $Trigger `
        -Settings   $settings `
        -RunLevel   Highest `
        -Force

    $status = if ($task) { "[OK]" } else { "[FAILED]" }
    Write-Host "$status $Name"
}

Write-Host ""
Write-Host "Creating SlateHub scheduled tasks..."
Write-Host ""

# ── QUICK REFRESH — every 15 minutes, NO wake ─────────────────────────────────
# Runs when PC is already on. Waking every 15 min all day would kill battery.
$quickTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration ([TimeSpan]::MaxValue)
New-SlateTask -Name "Quick Refresh (every 15 min)" -Mode "--quick" -Trigger $quickTrigger -Wake $false

# ── DAILY STATS — 7:00 AM, WAKE ───────────────────────────────────────────────
$statsTrigger = New-ScheduledTaskTrigger -Daily -At "07:00"
New-SlateTask -Name "Daily Stats (7:00 AM)" -Mode "--stats" -Trigger $statsTrigger

# ── DAILY SPLITS — 7:30 AM, WAKE ──────────────────────────────────────────────
$splitsTrigger = New-ScheduledTaskTrigger -Daily -At "07:30"
New-SlateTask -Name "Daily Splits (7:30 AM)" -Mode "--splits" -Trigger $splitsTrigger

# ── MORNING — 9:00 AM, WAKE ───────────────────────────────────────────────────
$morningTrigger = New-ScheduledTaskTrigger -Daily -At "09:00"
New-SlateTask -Name "Morning Refresh (9:00 AM)" -Mode "--morning" -Trigger $morningTrigger

# ── POST-GAME — 11:30 PM, WAKE ────────────────────────────────────────────────
$postgameTrigger = New-ScheduledTaskTrigger -Daily -At "23:30"
New-SlateTask -Name "Post-Game Refresh (11:30 PM)" -Mode "--postgame" -Trigger $postgameTrigger

Write-Host ""
Write-Host "======================================================="
Write-Host "  Done. Task summary:"
Write-Host ""
Write-Host "  Every 15 min   Quick: schedule + lineups + weather"
Write-Host "                 (runs when PC is on — no wake)"
Write-Host "  7:00 AM  WAKE  Daily stats"
Write-Host "  7:30 AM  WAKE  Daily splits"
Write-Host "  9:00 AM  WAKE  Morning: bullpen, game logs, DK"
Write-Host "  11:30 PM WAKE  Post-game: bullpen + game logs"
Write-Host ""
Write-Host "  To verify: Win+R -> taskschd.msc -> SlateHub folder"
Write-Host "  To run manually: right-click any task -> Run"
Write-Host "======================================================="
Write-Host ""
Read-Host "Press Enter to close"
