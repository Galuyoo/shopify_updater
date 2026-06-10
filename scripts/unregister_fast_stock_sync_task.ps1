$ErrorActionPreference = "Stop"

$TaskName = "StoreFeeder Fast Stock Sync"

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Write-Host "Unregistered scheduled task: $TaskName"
