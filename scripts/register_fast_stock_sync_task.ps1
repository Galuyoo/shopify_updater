$ErrorActionPreference = "Stop"

$TaskName = "StoreFeeder Fast Stock Sync"
$ProjectRoot = "C:\shopify_updater_amazon_work"
$ScriptPath = Join-Path $ProjectRoot "scripts\run_fast_stock_sync.ps1"
$StartAt = (Get-Date).AddMinutes(5)

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"" `
    -WorkingDirectory $ProjectRoot

$Trigger = New-ScheduledTaskTrigger `
    -Once `
    -At $StartAt `
    -RepetitionInterval (New-TimeSpan -Minutes 30) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Runs StoreFeeder fast supplier stock sync every 30 minutes." `
    -Force | Out-Null

Write-Host "Registered scheduled task: $TaskName"
Get-ScheduledTask -TaskName $TaskName
Get-ScheduledTaskInfo -TaskName $TaskName
